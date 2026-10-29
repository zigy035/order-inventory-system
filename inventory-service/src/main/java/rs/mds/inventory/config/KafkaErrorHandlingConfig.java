package rs.mds.inventory.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import rs.mds.inventory.messaging.InvalidOrderEventException;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, InventoryProperties properties) {
        String dltTopic = properties.kafka().topics().ordersDlt();

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterTemplate(producerFactory),
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff(properties.kafka().retry()));
        errorHandler.addNotRetryableExceptions(InvalidOrderEventException.class);
        errorHandler.setRetryListeners(new RetryListener() {

            @Override
            public void failedDelivery(@NonNull ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Processing failed (attempt {}): topic={} partition={} offset={} cause={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), rootCauseMessage(ex));
            }

            @Override
            public void recovered(@NonNull ConsumerRecord<?, ?> record, Exception ex) {
                log.error("Record sent to DLT {}: topic={} partition={} offset={} cause={}",
                        dltTopic, record.topic(), record.partition(), record.offset(), rootCauseMessage(ex));
            }

            @Override
            public void recoveryFailed(@NonNull ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("Failed to send record to DLT {}, it will be redelivered: topic={} partition={} offset={} cause={}",
                        dltTopic, record.topic(), record.partition(), record.offset(), rootCauseMessage(failure));
            }
        });
        return errorHandler;
    }

    private static ExponentialBackOffWithMaxRetries backOff(InventoryProperties.Retry retry) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxAttempts() - 1);
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return backOff;
    }

    private static String rootCauseMessage(Throwable ex) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(ex);
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    /**
     * The DLT receives two kinds of values: raw bytes (when JSON could not be deserialized)
     * and OrderCreatedEvent objects (when the content was invalid). Each needs its own serializer.
     */
    private static KafkaTemplate<String, Object> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> valueSerializers = new LinkedHashMap<>();
        valueSerializers.put(byte[].class, new ByteArraySerializer());
        valueSerializers.put(Object.class, new JacksonJsonSerializer<>().noTypeInfo());

        DefaultKafkaProducerFactory<String, Object> dltProducerFactory = new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(valueSerializers, true));

        return new KafkaTemplate<>(dltProducerFactory);
    }
}
