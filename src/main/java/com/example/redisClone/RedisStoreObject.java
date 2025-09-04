package com.example.redisClone;

/**
 * A wrapper object for values stored in the Redis map.
 * It holds both the string value and its expiration timestamp.
 */
public class RedisStoreObject {
    String value;
    long expiration;

    /**
     * Constructor for a value that does not expire.
     * @param value The string value to store.
     */
    public RedisStoreObject(String value) {
        // Use Long.MAX_VALUE to signify that this key never expires.
        this(value, Long.MAX_VALUE);
    }

    /**
     * Constructor for a value with a specific expiration time.
     * @param value The string value to store.
     * @param expiration The expiration time as a Unix timestamp in milliseconds.
     */
    public RedisStoreObject(String value, long expiration) {
        this.value = value;
        this.expiration = expiration;
    }
}