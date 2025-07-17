package com.example.redisClone;

public class RedisStoreObject {
    @SuppressWarnings("unused")
    String value;
    @SuppressWarnings("unused")
    long expiration;

    public RedisStoreObject(String value) {
        this(value, Long.MAX_VALUE); 
    }

    public RedisStoreObject(String value, long expiration) {
        this.value = value;
        this.expiration = expiration;
    }
}
