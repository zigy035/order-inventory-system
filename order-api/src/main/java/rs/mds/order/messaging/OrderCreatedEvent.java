package rs.mds.order.messaging;

import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        Instant occurredAt,
        String orderId,
        String itemId,
        int quantity
) {
    public static OrderCreatedEvent of(String orderId, String itemId, int quantity) {
        return new OrderCreatedEvent(UUID.randomUUID(), Instant.now(), orderId, itemId, quantity);
    }
}
