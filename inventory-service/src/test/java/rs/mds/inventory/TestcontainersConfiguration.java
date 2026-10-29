package rs.mds.inventory;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import rs.mds.inventory.config.InventoryProperties;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	private static final int PARTITIONS = 3;

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));
	}

	@Bean
	NewTopic ordersTopic(InventoryProperties properties) {
		return TopicBuilder.name(properties.kafka().topics().orders()).partitions(PARTITIONS).replicas(1).build();
	}

	@Bean
	NewTopic ordersDltTopic(InventoryProperties properties) {
		return TopicBuilder.name(properties.kafka().topics().ordersDlt()).partitions(PARTITIONS).replicas(1).build();
	}
}
