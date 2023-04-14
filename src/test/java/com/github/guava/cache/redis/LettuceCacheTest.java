package com.github.guava.cache.redis;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class LettuceCacheTest {

    private LettuceCache<String, String> lettuceCache;
    private RedisClient redisClient;

    @Before
    public void setUp() throws Exception {
        redisClient = RedisClient.create("redis://123456@127.0.0.1:6379/0");
        StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);
        StringSerializer stringSerializer = new StringSerializer();
        JdkSerializer jdkSerializer = new JdkSerializer();
        lettuceCache = new LettuceCache<>(connection, stringSerializer, jdkSerializer, "pre_".getBytes(StandardCharsets.UTF_8), 120, new CacheLoader<String, String>() {
            @Override
            public String load(String key) {
                return key + "1";
            }
        });
        String test = lettuceCache.get("test");
        System.out.println("test = " + test);
    }

    @After
    public void tearDown() {
        if (redisClient != null) {
            redisClient.close();
            System.out.println("close successful");
        }
    }

    @Test
    public void getIfPresent() {
        String test = lettuceCache.getIfPresent("test");
        Assert.assertEquals("test1", test);
    }

    @Test
    public void get() throws ExecutionException {
        String test = lettuceCache.get("test1", () -> "test11");
        Assert.assertEquals("test11", test);
    }

    @Test
    public void getAllPresent() {
        ImmutableMap<String, String> allPresent = lettuceCache.getAllPresent(Arrays.asList("test", "test1"));
        System.out.println("allPresent = " + allPresent);
        Assert.assertTrue(allPresent.size() > 0);
    }

    @Test
    public void put() {
        lettuceCache.put("test2", "test21");
    }

    @Test
    public void putAll() {
        lettuceCache.putAll(Collections.singletonMap("test3", "test31"));
    }

    @Test
    public void invalidate() {
        lettuceCache.invalidate("test");
    }

    @Test
    public void invalidateAll() {
        lettuceCache.invalidateAll();
    }

    @Test
    public void testGet() throws ExecutionException {
        String test4 = lettuceCache.get("test4");
        Assert.assertEquals("test41", test4);
    }

    @Test
    public void getAll() throws ExecutionException {
        ImmutableMap<String, String> map = lettuceCache.getAll(Arrays.asList("test", "test1"));
        Assert.assertTrue(map.size() > 0);
    }

    @Test
    public void refresh() {
        lettuceCache.refresh("test");
    }
}