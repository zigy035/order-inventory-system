package rs.mds.inventory.domain;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory inventory with idempotent, thread-safe stock reservation.
 * Framework-free on purpose: contains only business rules, no Kafka or Spring dependencies.
 */
public class InventoryService {

    private final Map<String, AtomicInteger> stock;
    private final ConcurrentMap<String, Reservation> reservationsByOrderId = new ConcurrentHashMap<>();
    private final Clock clock;

    public InventoryService(Map<String, Integer> initialStock, Clock clock) {
        Objects.requireNonNull(initialStock, "initialStock must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");

        Map<String, AtomicInteger> stock = new HashMap<>();
        initialStock.forEach((itemId, quantity) -> {
            if (quantity == null || quantity < 0) {
                throw new IllegalArgumentException(
                        "Initial stock for item '" + itemId + "' must be >= 0, was " + quantity);
            }
            stock.put(itemId, new AtomicInteger(quantity));
        });
        this.stock = stock;
    }

    /**
     * Processes an order exactly once per orderId. Repeated calls with the same orderId
     * return the original outcome without touching stock again.
     */
    public Result reserve(String orderId, String itemId, int quantity) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(itemId, "itemId must not be null");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, was " + quantity);
        }

        AtomicBoolean processedNow = new AtomicBoolean(false);
        Reservation reservation = reservationsByOrderId.computeIfAbsent(orderId, id -> {
            processedNow.set(true);
            return process(id, itemId, quantity);
        });
        return new Result(reservation, !processedNow.get());
    }

    public Optional<Reservation> findReservation(String orderId) {
        return Optional.ofNullable(reservationsByOrderId.get(orderId));
    }

    public List<Reservation> reservations() {
        return reservationsByOrderId.values().stream()
                .sorted(Comparator.comparing(Reservation::processedAt))
                .toList();
    }

    public Map<String, Integer> stockLevels() {
        Map<String, Integer> snapshot = new TreeMap<>();
        stock.forEach((itemId, available) -> snapshot.put(itemId, available.get()));
        return snapshot;
    }

    private Reservation process(String orderId, String itemId, int quantity) {
        Instant now = clock.instant();

        AtomicInteger available = stock.get(itemId);
        if (available == null) {
            return Reservation.rejected(orderId, itemId, quantity, ReservationStatus.REJECTED_UNKNOWN_ITEM, now);
        }
        if (tryDecrement(available, quantity)) {
            return Reservation.reserved(orderId, itemId, quantity, now);
        }
        return Reservation.rejected(orderId, itemId, quantity, ReservationStatus.REJECTED_INSUFFICIENT_STOCK, now);
    }

    /** Lock-free "decrement only if enough stock" using compare-and-set. */
    private static boolean tryDecrement(AtomicInteger available, int quantity) {
        int current;
        do {
            current = available.get();
            if (current < quantity) {
                return false;
            }
        } while (!available.compareAndSet(current, current - quantity));
        return true;
    }

    /**
     * @param reservation the outcome for the order (new or previously recorded)
     * @param duplicate   true if the order had already been processed and this call changed nothing
     */
    public record Result(Reservation reservation, boolean duplicate) {
    }
}
