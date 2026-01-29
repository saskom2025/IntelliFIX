package org.intellifix.redis.config;

import redis.clients.jedis.JedisPooled;

public class RedisClientConfig {

    private static final String HOST = "localhost";
    private static final int PORT = 18130;
    private static final String USER = "default";
    private static final String PASSWORD = "pwd";

    public static JedisPooled jedis() {
        return new JedisPooled(HOST, PORT, USER, PASSWORD);
    }
}
