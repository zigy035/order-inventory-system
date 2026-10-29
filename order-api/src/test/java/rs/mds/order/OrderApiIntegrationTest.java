package rs.mds.order;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import rs.mds.order.config.OrderKafkaProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class OrderApiIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private OrderKafkaProperties properties;

    @Test
    void acceptsOrderAndPublishesEventKeyedByItemId() throws Exception {
        // given
        String orderId = "it-" + UUID.randomUUID();

        // when
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"orderId":"%s","itemId":"item-1","quantity":2}
                                """.formatted(orderId)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value(orderId))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        // then
        ConsumerRecord<String, String> record = awaitOrderRecord(orderId);
        String json = record.value();

        assertThat(record.key()).isEqualTo("item-1");
        assertThat(JsonPath.<String>read(json, "$.orderId")).isEqualTo(orderId);
        assertThat(JsonPath.<String>read(json, "$.itemId")).isEqualTo("item-1");
        assertThat(JsonPath.<Integer>read(json, "$.quantity")).isEqualTo(2);
        assertThat(JsonPath.<String>read(json, "$.eventId")).isNotBlank();
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    void rejectsInvalidOrderWithValidationErrors() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"orderId":"","itemId":"item-1","quantity":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.orderId").exists())
                .andExpect(jsonPath("$.errors.quantity").exists());
    }

    private ConsumerRecord<String, String> awaitOrderRecord(String orderId) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-orders-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(properties.topics().orders()));
            AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();

            await().atMost(TIMEOUT).pollInSameThread().until(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.value().contains(orderId)) {
                        found.set(record);
                        return true;
                    }
                }
                return false;
            });
            return found.get();
        }
    }
}
