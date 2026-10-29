package rs.mds.inventory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import rs.mds.inventory.config.InventoryProperties;
import rs.mds.inventory.domain.InventoryService;
import rs.mds.inventory.domain.ReservationStatus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "app.kafka.retry.initial-interval=50ms",
        "app.kafka.retry.max-interval=200ms"
})
@Import(TestcontainersConfiguration.class)
class InventoryIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockitoSpyBean
    private InventoryService inventoryService;

    @Autowired
    private InventoryProperties properties;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void reservesStockForValidOrderEvent() throws Exception {
        // given
        String orderId = uniqueOrderId();

        // when
        sendToOrdersTopic("item-1", orderJson(orderId, "item-1", 2));

        // then
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(inventoryService.findReservation(orderId))
                        .hasValueSatisfying(r -> assertThat(r.status()).isEqualTo(ReservationStatus.RESERVED)));
    }

    @Test
    void routesUndeserializableMessageToDltWithOriginalPayload() throws Exception {
        // given
        String key = "it-poison-" + UUID.randomUUID();
        String payload = "this is not json";

        // when
        sendToOrdersTopic(key, payload);

        // then
        ConsumerRecord<String, byte[]> dltRecord = awaitDltRecord(r -> key.equals(r.key()));

        assertThat(new String(dltRecord.value(), StandardCharsets.UTF_8)).isEqualTo(payload);
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_ORIGINAL_TOPIC))
                .isEqualTo(properties.kafka().topics().orders());
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo(DeserializationException.class.getName());
    }

    @Test
    void retriesTransientFailureAndThenProcessesOrder() throws Exception {
        // given
        String orderId = uniqueOrderId();
        doThrow(new IllegalStateException("Simulated transient failure"))
                .doCallRealMethod()
                .when(inventoryService).reserve(eq(orderId), anyString(), anyInt());

        // when
        sendToOrdersTopic("item-1", orderJson(orderId, "item-1", 1));

        // then
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(inventoryService.findReservation(orderId))
                        .hasValueSatisfying(r -> assertThat(r.status()).isEqualTo(ReservationStatus.RESERVED)));
        verify(inventoryService, times(2)).reserve(eq(orderId), anyString(), anyInt());
    }

    @Test
    void sendsOrderToDltAfterRetriesAreExhausted() throws Exception {
        // given
        String orderId = uniqueOrderId();
        doThrow(new IllegalStateException("Simulated persistent failure"))
                .when(inventoryService).reserve(eq(orderId), anyString(), anyInt());

        // when
        sendToOrdersTopic("item-1", orderJson(orderId, "item-1", 1));

        // then
        ConsumerRecord<String, byte[]> dltRecord = awaitDltRecord(
                r -> new String(r.value(), StandardCharsets.UTF_8).contains(orderId));

        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(IllegalStateException.class.getName());
        verify(inventoryService, times(properties.kafka().retry().maxAttempts()))
                .reserve(eq(orderId), anyString(), anyInt());
        assertThat(inventoryService.findReservation(orderId)).isEmpty();
    }

    // ---------- helpers ----------

    private void sendToOrdersTopic(String key, String value) throws Exception {
        kafkaTemplate.send(properties.kafka().topics().orders(), key, value).get(5, TimeUnit.SECONDS);
    }

    private ConsumerRecord<String, byte[]> awaitDltRecord(Predicate<ConsumerRecord<String, byte[]>> matcher) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-dlt-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(properties.kafka().topics().ordersDlt()));
            AtomicReference<ConsumerRecord<String, byte[]>> found = new AtomicReference<>();

            await().atMost(TIMEOUT).pollInSameThread().until(() -> {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (matcher.test(record)) {
                        found.set(record);
                        return true;
                    }
                }
                return false;
            });
            return found.get();
        }
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String orderJson(String orderId, String itemId, int quantity) {
        return """
                {"eventId":"%s","orderId":"%s","itemId":"%s","quantity":%d}
                """.formatted(UUID.randomUUID(), orderId, itemId, quantity);
    }

    private static String uniqueOrderId() {
        return "it-" + UUID.randomUUID();
    }
}
