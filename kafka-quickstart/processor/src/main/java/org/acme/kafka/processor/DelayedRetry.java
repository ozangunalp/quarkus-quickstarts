package org.acme.kafka.processor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@ApplicationScoped
public class DelayedRetry {

    public static final String RETRY_COUNT = "retry-count";
    public static final String RETRY_ORIGINAL_TIMESTAMP = "retry_original_timestamp";

    @ConfigProperty(name = "mp.messaging.incoming.requests_retry.topics")
    String topics;

    @Incoming("requests_retry")
    @Outgoing("requests_retry_retried")
    public Multi<Message<?>> retry(Multi<Message<?>> retried) {
        return retried.onItem().transformToUni(msg -> {
            Message<?> retryMessage = getRetryMessage(msg, topics);
            Duration between = getDelay(msg);
            if (between.isNegative()) {
                return Uni.createFrom().item(retryMessage);
            } else {
                System.out.println("Retrying " + msg.getPayload() + " in " + between.toSeconds() + " seconds.");
                return Uni.createFrom().item(retryMessage).onItem().delayIt().by(between);
            }
        }).concatenate(false);
    }

    private static Message<?> getRetryMessage(Message<?> retried, String topics) {
        ConsumerRecord<?, ?> record = record(retried);
        incrementRetryHeader(record.headers());
        String nextRetryTopic = getNextRetryTopic(record.topic(), topics);
        return nextRetryTopic != null ? retried.withNack(t -> retried.nack(t,
                Metadata.of(OutgoingKafkaRecordMetadata.builder()
                        .withTopic(nextRetryTopic)
                        .build()))) : retried;
    }

    private static Duration getDelay(Message<?> retried) {
        ConsumerRecord<?, ?> record = record(retried);
        int delay = getDelayFromTopic(record.topic());
        Instant originalTimestamp = getOrSetOriginalRecordTimestamp(record.headers(), record.timestamp());
        return Duration.between(Instant.now(), originalTimestamp.plus(delay, ChronoUnit.MILLIS));
    }
    
    private static ConsumerRecord<?, ?> record(Message<?> msg) {
        return msg.getMetadata(IncomingKafkaRecordMetadata.class)
                .map(IncomingKafkaRecordMetadata::getRecord)
                .orElseThrow();
    }

    private static String getNextRetryTopic(String topic, String topics) {
        Iterator<String> iterator = Arrays.stream(topics.split(",")).iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (next.equals(topic)) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
            }
        }
        return null;
    }

    public static int getRetryHeader(Headers headers) {
        Header retry = headers.lastHeader(RETRY_COUNT);
        return retry == null ? 0 : Integer.parseInt(new String(retry.value()));
    }

    public static long getTimestampHeader(Headers headers, long timestamp) {
        Header retry = headers.lastHeader(RETRY_ORIGINAL_TIMESTAMP);
        return retry == null ? timestamp : Long.parseLong(new String(retry.value()));
    }

    private static void incrementRetryHeader(Headers headers) {
        int count = getRetryHeader(headers) + 1;
        headers.add(RETRY_COUNT, Integer.toString(count).getBytes(StandardCharsets.UTF_8));
    }

    private static Instant getOrSetOriginalRecordTimestamp(Headers headers, long timestamp) {
        Header retry = headers.lastHeader(RETRY_ORIGINAL_TIMESTAMP);
        if (retry == null) {
            headers.add(RETRY_ORIGINAL_TIMESTAMP, Long.toString(timestamp).getBytes(StandardCharsets.UTF_8));
            return Instant.ofEpochMilli(timestamp);
        } else {
            long timestampHeader = getTimestampHeader(headers, timestamp);
            return Instant.ofEpochMilli(timestampHeader);
        }
    }

    private static int getDelayFromTopic(String topicName) {
        return Integer.parseInt(topicName.substring(topicName.lastIndexOf("_") + 1));
    }
}
