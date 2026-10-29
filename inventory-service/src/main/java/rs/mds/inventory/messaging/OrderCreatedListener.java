package rs.mds.inventory.messaging;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import rs.mds.inventory.domain.InventoryService;
import rs.mds.inventory.domain.Reservation;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final InventoryService inventoryService;
    private final Validator validator;

    public OrderCreatedListener(InventoryService inventoryService, Validator validator) {
        this.inventoryService = inventoryService;
        this.validator = validator;
    }

    @KafkaListener(id = "order-created-listener", idIsGroup = false, topics = "${app.kafka.topics.orders}")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent event = record.value();
        validate(event);
        warnIfKeyDoesNotMatchItem(record, event);

        InventoryService.Result result =
                inventoryService.reserve(event.orderId(), event.itemId(), event.quantity());
        Reservation reservation = result.reservation();

        if (result.duplicate()) {
            log.info("Duplicate OrderCreated ignored: orderId={} eventId={} originalStatus={} partition={} offset={}",
                    event.orderId(), event.eventId(), reservation.status(), record.partition(), record.offset());
        } else {
            log.info("Processed OrderCreated: orderId={} itemId={} quantity={} status={} eventId={} partition={} offset={}",
                    event.orderId(), event.itemId(), event.quantity(), reservation.status(),
                    event.eventId(), record.partition(), record.offset());
        }
    }

    private void validate(OrderCreatedEvent event) {
        if (event == null) {
            throw new InvalidOrderEventException("Record has no value (empty payload or tombstone)");
        }

        Set<ConstraintViolation<OrderCreatedEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new InvalidOrderEventException("Invalid OrderCreated event: " + details);
        }
    }

    private void warnIfKeyDoesNotMatchItem(ConsumerRecord<String, OrderCreatedEvent> record, OrderCreatedEvent event) {
        if (!Objects.equals(record.key(), event.itemId())) {
            log.warn("Record key does not match itemId (per-item ordering not guaranteed): key={} itemId={} orderId={}",
                    record.key(), event.itemId(), event.orderId());
        }
    }
}
