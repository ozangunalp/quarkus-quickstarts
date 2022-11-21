package org.acme.kafka.checkpoint;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.acme.kafka.checkpoint.model.Aggregation;
import org.junit.jupiter.api.Test;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class AggregatorTest {

    @InjectKafkaCompanion
    KafkaCompanion companion;
    
    @Test
    void testAggregation() {
        companion.registerSerde(Aggregation.class, new ObjectMapperSerde<>(Aggregation.class));
        try (ConsumerTask<Integer, Aggregation> consumerRecords = companion.consume(Integer.class, Aggregation.class)
                .fromTopics(Aggregator.TEMPERATURES_AGGREGATED_TOPIC)) {
            await().until(() -> consumerRecords.count() > 1);
        }
    }
}