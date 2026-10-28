package rs.mds.order.messaging;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import rs.mds.order.config.OrderKafkaProperties;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final OrderKafkaProperties properties;

    public OrderEventPublisher(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
                               OrderKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publish(OrderCreatedEvent event) {
        String topic = properties.topics().orders();
        try {
            SendResult<String, OrderCreatedEvent> result = kafkaTemplate
                    .send(topic, event.itemId(), event)
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);

            RecordMetadata metadata = result.getRecordMetadata();
            log.info("Published {} orderId={} eventId={} topic={} partition={} offset={}",
                    event.eventType(), event.orderId(), event.eventId(),
                    metadata.topic(), metadata.partition(), metadata.offset());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublishException(event.orderId(), e);
        } catch (ExecutionException | TimeoutException | KafkaException e) {
            throw new OrderPublishException(event.orderId(), e);
        }
    }
}
