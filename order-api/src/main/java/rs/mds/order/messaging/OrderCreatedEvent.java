package rs.mds.order.messaging;

import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String orderId,
        String itemId,
        int quantity
) {
    public static final String EVENT_TYPE = "OrderCreated";
    public static final int SCHEMA_VERSION = 1;

    public static OrderCreatedEvent of(String orderId, String itemId, int quantity) {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                EVENT_TYPE,
                SCHEMA_VERSION,
                Instant.now(),
                orderId,
                itemId,
                quantity);
    }
}
