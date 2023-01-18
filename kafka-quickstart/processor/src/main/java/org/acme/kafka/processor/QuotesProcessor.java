package org.acme.kafka.processor;

import static org.acme.kafka.processor.DelayedRetry.getRetryHeader;

import java.util.Random;

import javax.enterprise.context.ApplicationScoped;

import org.acme.kafka.model.Quote;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class QuotesProcessor {

    private Random random = new Random();

    @Incoming("requests")
    @Incoming("requests_retry_retried")
    @Outgoing("quotes")
    @Blocking
    public Quote process(ConsumerRecord<String, String> quoteRequest) throws InterruptedException {
        // simulate some hard working task
        String payload = quoteRequest.value();
        int retryHeader = getRetryHeader(quoteRequest.headers());
        System.out.println("Processing record " + quoteRequest.offset() + " " + retryHeader);
        int price = random.nextInt(100);
        if (retryHeader < 2 && price > 50) {
            throw new IllegalStateException("attempt " + retryHeader);
        }
        Thread.sleep(200);
        return new Quote(payload, price);
    }

}
