package rs.mds.inventory.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InventoryServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-28T12:00:00Z");
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void reservesStockWhenEnoughIsAvailable() {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 10));

        // when
        InventoryService.Result result = inventory.reserve("order-1", "item-1", 3);

        // then
        assertThat(result.duplicate()).isFalse();
        assertThat(result.reservation())
                .isEqualTo(Reservation.reserved("order-1", "item-1", 3, NOW));
        assertThat(inventory.stockLevels()).containsEntry("item-1", 7);
        assertThat(inventory.findReservation("order-1")).contains(result.reservation());
    }

    @Test
    void rejectsOrderWhenStockIsInsufficientAndLeavesStockUnchanged() {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 2));

        // when
        InventoryService.Result result = inventory.reserve("order-1", "item-1", 3);

        // then
        assertThat(result.duplicate()).isFalse();
        assertThat(result.reservation()).isEqualTo(Reservation.rejected(
                "order-1", "item-1", 3, ReservationStatus.REJECTED_INSUFFICIENT_STOCK, NOW));
        assertThat(inventory.stockLevels()).containsEntry("item-1", 2);
        assertThat(inventory.findReservation("order-1")).contains(result.reservation());
    }

    @Test
    void reservesWhenRequestedQuantityEqualsAvailableStock() {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 5));

        // when
        InventoryService.Result result = inventory.reserve("order-1", "item-1", 5);

        // then
        assertThat(result.reservation().status()).isEqualTo(ReservationStatus.RESERVED);
        assertThat(inventory.stockLevels()).containsEntry("item-1", 0);
    }

    @Test
    void rejectsOrderForUnknownItemWithoutAffectingExistingStock() {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 10));

        // when
        InventoryService.Result result = inventory.reserve("order-1", "item-unknown", 1);

        // then
        assertThat(result.reservation()).isEqualTo(Reservation.rejected(
                "order-1", "item-unknown", 1, ReservationStatus.REJECTED_UNKNOWN_ITEM, NOW));
        assertThat(inventory.stockLevels()).containsExactly(Map.entry("item-1", 10));
    }

    @Test
    void processesSameOrderOnlyOnceWhenDeliveredTwice() {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 10));
        InventoryService.Result first = inventory.reserve("order-1", "item-1", 3);

        // when
        InventoryService.Result second = inventory.reserve("order-1", "item-1", 3);

        // then
        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.reservation()).isSameAs(first.reservation());
        assertThat(inventory.stockLevels()).containsEntry("item-1", 7);
        assertThat(inventory.reservations()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -5})
    void rejectsNonPositiveQuantityWithoutChangingState(int invalidQuantity) {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 10));

        // when / then
        assertThatThrownBy(() -> inventory.reserve("order-1", "item-1", invalidQuantity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quantity must be positive");

        assertThat(inventory.stockLevels()).containsEntry("item-1", 10);
        assertThat(inventory.findReservation("order-1")).isEmpty();
    }

    @Test
    void rejectsNegativeInitialStock() {
        assertThatThrownBy(() -> inventoryWith(Map.of("item-1", -1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("item-1");
    }

    @Test
    void rejectsNullInitialStockValue() {
        Map<String, Integer> initialStock = new HashMap<>();
        initialStock.put("item-1", null);

        assertThatThrownBy(() -> inventoryWith(initialStock))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("item-1");
    }

    @Test
    void neverOversellsStockUnderConcurrentOrders() throws Exception {
        // given
        int initialStock = 100;
        int orderCount = 1000;
        InventoryService inventory = inventoryWith(Map.of("item-1", initialStock));

        // when
        List<InventoryService.Result> results = runConcurrently(orderCount,
                i -> inventory.reserve("order-" + i, "item-1", 1));

        // then
        long reserved = results.stream()
                .filter(r -> r.reservation().status().isReserved())
                .count();
        long rejected = results.stream()
                .filter(r -> r.reservation().status() == ReservationStatus.REJECTED_INSUFFICIENT_STOCK)
                .count();

        assertThat(reserved).isEqualTo(initialStock);
        assertThat(rejected).isEqualTo(orderCount - initialStock);
        assertThat(inventory.stockLevels()).containsEntry("item-1", 0);
        assertThat(inventory.reservations()).hasSize(orderCount);
    }

    @Test
    void processesSameOrderOnlyOnceUnderConcurrentDelivery() throws Exception {
        // given
        InventoryService inventory = inventoryWith(Map.of("item-1", 10));
        int deliveryCount = 50;

        // when
        List<InventoryService.Result> results = runConcurrently(deliveryCount,
                i -> inventory.reserve("order-1", "item-1", 3));

        // then
        long firstProcessings = results.stream().filter(r -> !r.duplicate()).count();
        Reservation original = results.getFirst().reservation();

        assertThat(firstProcessings).isEqualTo(1);
        assertThat(results).allSatisfy(r -> assertThat(r.reservation()).isSameAs(original));
        assertThat(inventory.stockLevels()).containsEntry("item-1", 7);
        assertThat(inventory.reservations()).hasSize(1);
    }

    private InventoryService inventoryWith(Map<String, Integer> initialStock) {
        return new InventoryService(initialStock, clock);
    }

    private static List<InventoryService.Result> runConcurrently(
            int taskCount,
            IntFunction<InventoryService.Result> task) throws Exception {

        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<InventoryService.Result>> futures = new ArrayList<>(taskCount);

        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            for (int i = 0; i < taskCount; i++) {
                int taskIndex = i;
                futures.add(executor.submit(() -> {
                    startGate.await();
                    return task.apply(taskIndex);
                }));
            }
            startGate.countDown();
        }

        List<InventoryService.Result> results = new ArrayList<>(taskCount);
        for (Future<InventoryService.Result> future : futures) {
            results.add(future.get());
        }
        return results;
    }
}
