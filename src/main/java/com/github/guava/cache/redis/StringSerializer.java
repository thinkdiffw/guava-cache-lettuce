package com.github.guava.cache.redis;

import java.nio.charset.StandardCharsets;

/**
 * @author ThinkDiffW
 */
public class StringSerializer implements Serializer {
    @Override
    public byte[] serialize(Object obj) {
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T deserialize(byte[] objectData) {
        return (T) new String(objectData, StandardCharsets.UTF_8);
    }
}
