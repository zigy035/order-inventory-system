package rs.mds.inventory.domain;

import java.time.Instant;
import java.util.Objects;

public record Reservation(
        String orderId,
        String itemId,
        int quantity,
        ReservationStatus status,
        Instant processedAt
) {

    public Reservation {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(itemId, "itemId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(processedAt, "processedAt must not be null");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, was " + quantity);
        }
    }

    public static Reservation reserved(String orderId, String itemId, int quantity, Instant processedAt) {
        return new Reservation(orderId, itemId, quantity, ReservationStatus.RESERVED, processedAt);
    }

    public static Reservation rejected(String orderId, String itemId, int quantity,
                                       ReservationStatus reason, Instant processedAt) {
        if (reason.isReserved()) {
            throw new IllegalArgumentException("Rejection reason must not be RESERVED");
        }
        return new Reservation(orderId, itemId, quantity, reason, processedAt);
    }
}
