package org.intellifix.redis;

import org.intellifix.redis.base.MessagePublisher;

import org.intellifix.common.ExecutionState;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;

@Slf4j
public class RedisMessagePublisher implements MessagePublisher {
    private static final String LOGS_DIR = "log";
    private static final String LOG_FILE_NAME = "redis_stream.log";
    private static final DateTimeFormatter LOG_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final long SESSION_GAP_MS = 5000;

    @Override
    public void publishMessage(String message) {
        // TODO logic to publish message to Redis
        log.info("To Redis:" + message);
        saveToFile(message);
    }

    private void saveToFile(String message) {
        try {
            Path logsPath = Paths.get(LOGS_DIR);
            if (!Files.exists(logsPath)) {
                Files.createDirectories(logsPath);
            }

            Path filePath = logsPath.resolve(LOG_FILE_NAME);

            if (ExecutionState.isFirstMessage.getAndSet(false)) {
                if (Files.exists(filePath)) {
                    FileTime lastModified = Files.getLastModifiedTime(filePath);
                    long idleTime = Duration.between(lastModified.toInstant(), Instant.now()).toMillis();
                    if (idleTime > SESSION_GAP_MS) {
                        Files.write(filePath, System.lineSeparator().getBytes(StandardCharsets.UTF_8),
                                StandardOpenOption.APPEND);
                    }
                }
            }

            String timestamp = LocalDateTime.now().format(LOG_TIME_FORMATTER);
            String logEntry = String.format("[%s] %s%n", timestamp, message);

            Files.write(filePath, logEntry.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("Message saved to file: {}", filePath.toAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to save message to file", e);
        }
    }
}
