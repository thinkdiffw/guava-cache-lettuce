# guava-cache-lettuce
Implement guava cache interface backed by redis (lettuce or jedis). Guava provide memory cache implementation. We provide redis cache implementation.

Please read [guava caches explained](https://github.com/google/guava/wiki/CachesExplained) first.

## Howto
```
<dependency>
    <groupId>io.github.thinkdiffw</groupId>
    <artifactId>guava-cache-lettuce</artifactId>
    <version>0.0.4</version>
</dependency>
```
### From a [CacheLoader](http://google.github.io/guava/releases/snapshot/api/docs/com/google/common/cache/CacheLoader.html)
```
LettuceCache<Key, Graph> redisCache = new LettuceCache<>(
  connection,
  keySerializer,
  valueSerializer,
  keyPrefix,
  expiration,
  new CacheLoader<Key, Graph>() {
    public Graph load(Key key) throws AnyException {
      return createExpensiveGraph(key);
    }
  }
);
...

try {
  return redisCache.get(key);
} catch (ExecutionException e) {
  throw new OtherException(e.getCause());
}
```
### From a <a href='http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Callable.html'><code>Callable</code></a>
```
LettuceCache<Key, Graph> redisCache = new LettuceCache<>(
  connection,
  keySerializer,
  valueSerializer,
  keyPrefix,
  expiration
);
...

try {
  // If the key wasn't in the "easy to compute" group, we need to do things the hard way.
  redisCache.get(key, new Callable<Value>() {
    @Override
    public Value call() throws AnyException {
      return doThingsTheHardWay(key);
    }
  });
} catch (ExecutionException e) {
  throw new OtherException(e.getCause());
}
```
