package org.intellifix;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

public class App {
    private static final String STREAM_KEY = "fix-stream";
    private static final String GROUP_NAME = "fix-group";
    private static final String CONSUMER_NAME = "consumer_1";

    public static void main(String[] args) {
        System.out.println("#### Main method ####");
        String redisUri = "localhost";

        try (JedisPooled jedis = new JedisPooled(redisUri)) {
            System.out.println("Connected to Redis Cloud...");
            Thread.ofVirtual().start(() -> consumeMessages(jedis));
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void consumeMessages(JedisPooled jedis) {
        System.out.println("Starting to consume messages...");

        while (!Thread.interrupted()) {
            // Read messages from the group
            // ">" means: read messages that have never been delivered to other consumers in the group.
            // block(0) makes the call wait indefinitely until a new message arrives.
            List<Map.Entry<String, List<StreamEntry>>> results = jedis.xreadGroup(
                    GROUP_NAME,
                    CONSUMER_NAME,
                    XReadGroupParams.xReadGroupParams().block(0).count(10),
                    Map.of(STREAM_KEY, StreamEntryID.UNRECEIVED_ENTRY)
            );

            if (results != null) {
                for (Map.Entry<String, List<StreamEntry>> streamEntry : results) {
                    for (StreamEntry message : streamEntry.getValue()) {
                        processMessage(jedis, message);
                    }
                }
            }
        }
    }

    private static void processMessage(JedisPooled jedis, StreamEntry message) {
        System.out.println("Processing Message ID: " + message.getID());
        System.out.println("Payload: " + message.getFields());
        jedis.xack(STREAM_KEY, GROUP_NAME, message.getID());
    }
}
