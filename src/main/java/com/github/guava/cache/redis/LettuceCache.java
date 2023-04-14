package com.github.guava.cache.redis;

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author ThinkDiffW
 */
public class LettuceCache<K, V> extends AbstractLoadingCache<K, V> implements LoadingCache<K, V> {
    static final Logger logger = Logger.getLogger(LettuceCache.class.getName());

    private final StatefulRedisConnection<byte[], byte[]> connection;

    private final Serializer keySerializer;

    private final Serializer valueSerializer;

    private final byte[] keyPrefix;

    private final int expiration;

    private final CacheLoader<K, V> loader;

    public LettuceCache(StatefulRedisConnection<byte[], byte[]> connection, Serializer keySerializer,
                        Serializer valueSerializer, byte[] keyPrefix, int expiration) {
        this(connection, keySerializer, valueSerializer, keyPrefix, expiration, null);
    }

    public LettuceCache(StatefulRedisConnection<byte[], byte[]> connection, Serializer keySerializer, Serializer valueSerializer,
                        byte[] keyPrefix, int expiration, CacheLoader<K, V> loader) {
        this.connection = connection;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyPrefix = keyPrefix;
        this.expiration = expiration;
        this.loader = loader;
    }

    public V getIfPresent(Object o) {
        byte[] reply = connection.sync().get(Bytes.concat(keyPrefix, keySerializer.serialize(o)));
        return Optional.ofNullable(reply).map(valueSerializer::<V>deserialize).orElse(null);
    }


    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        try {
            V value = this.getIfPresent(key);
            if (value == null) {
                value = valueLoader.call();
                if (value == null) {
                    throw new CacheLoader.InvalidCacheLoadException("valueLoader must not return null, key=" + key);
                } else {
                    this.put(key, value);
                }
            }
            return value;
        } catch (Throwable e) {
            convertAndThrow(e);
            // never execute
            return null;
        }
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        List<byte[]> keyBytesList = StreamSupport.stream(keys.spliterator(), false).map(key -> Bytes.concat(keyPrefix, keySerializer.serialize(key))).collect(Collectors.toList());
        List<KeyValue<byte[], byte[]>> list = connection.sync().mget(Iterables.toArray(keyBytesList, byte[].class));
        Map<K, V> map = new LinkedHashMap<>();
        for (KeyValue<byte[], byte[]> keyValue : list) {
            if (keyValue.hasValue()) {
                byte[] originKeyBytes = Arrays.copyOfRange(keyValue.getKey(), keyPrefix.length, keyValue.getKey().length);
                map.put(keySerializer.deserialize(originKeyBytes), valueSerializer.deserialize(keyValue.getValue()));
            }
        }
        return ImmutableMap.copyOf(map);
    }

    @Override
    public void put(K key, V value) {
        byte[] keyBytes = Bytes.concat(keyPrefix, keySerializer.serialize(key));
        byte[] valueBytes = valueSerializer.serialize(value);

        RedisCommands<byte[], byte[]> commands = connection.sync();
        if (expiration > 0) {
            commands.setex(keyBytes, expiration, valueBytes);
        } else {
            commands.set(keyBytes, valueBytes);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<byte[], byte[]> bytesMap = new HashMap<>(m.size());
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            bytesMap.put(Bytes.concat(keyPrefix, keySerializer.serialize(entry.getKey())), valueSerializer.serialize(entry.getValue()));
        }

        if (expiration > 0) {
            connection.sync().mset(bytesMap);
            RedisReactiveCommands<byte[], byte[]> cmd = connection.reactive();
            Flux.fromIterable(bytesMap.keySet()).flatMap(x -> cmd.expire(x, expiration)).collectList().block();
        } else {
            connection.sync().mset(bytesMap);
        }
    }

    @Override
    public void invalidate(Object key) {
        connection.sync().del(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        Set<byte[]> keyBytes = new LinkedHashSet<>();
        for (Object key : keys) {
            keyBytes.add(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
        }
        connection.sync().del(Iterables.toArray(keyBytes, byte[].class));
    }

    @Override
    public V get(final K key) throws ExecutionException {
        return this.get(key, () -> loader.load(key));
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys) throws ExecutionException {
        Map<K, V> result = Maps.newLinkedHashMap(this.getAllPresent(keys));

        Set<K> keysToLoad = Sets.newLinkedHashSet(keys);
        keysToLoad.removeAll(result.keySet());
        if (!keysToLoad.isEmpty()) {
            try {
                Map<K, V> newEntries = loader.loadAll(keysToLoad);
                if (newEntries == null) {
                    throw new CacheLoader.InvalidCacheLoadException(loader + " returned null map from loadAll");
                }
                this.putAll(newEntries);

                for (K key : keysToLoad) {
                    V value = newEntries.get(key);
                    if (value == null) {
                        throw new CacheLoader.InvalidCacheLoadException("loadAll failed to return a value for " + key);
                    }
                    result.put(key, value);
                }
            } catch (CacheLoader.UnsupportedLoadingOperationException e) {
                Map<K, V> newEntries = new LinkedHashMap<>();
                boolean nullsPresent = false;
                Throwable t = null;
                for (K key : keysToLoad) {
                    try {
                        V value = loader.load(key);
                        if (value == null) {
                            // delay failure until non-null entries are stored
                            nullsPresent = true;
                        } else {
                            newEntries.put(key, value);
                        }
                    } catch (Throwable tt) {
                        t = tt;
                        break;
                    }
                }
                this.putAll(newEntries);

                if (nullsPresent) {
                    throw new CacheLoader.InvalidCacheLoadException(loader + " returned null keys or values from loadAll");
                } else if (t != null) {
                    convertAndThrow(t);
                } else {
                    result.putAll(newEntries);
                }
            } catch (Throwable e) {
                convertAndThrow(e);
            }
        }

        return ImmutableMap.copyOf(result);
    }

    @Override
    public void refresh(K key) {
        try {
            V value = loader.load(key);
            this.put(key, value);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception thrown during refresh", e);
        }
    }

    private static void convertAndThrow(Throwable t) throws ExecutionException {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new ExecutionException(t);
        } else if (t instanceof RuntimeException) {
            throw new UncheckedExecutionException(t);
        } else if (t instanceof Exception) {
            throw new ExecutionException(t);
        } else {
            throw new ExecutionError((Error) t);
        }
    }
}
