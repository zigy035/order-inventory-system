package rs.mds.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import rs.mds.inventory.domain.InventoryService;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
public class InventoryConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    InventoryService inventoryService(InventoryProperties properties, Clock clock) {
        return new InventoryService(properties.initialStock(), clock);
    }
}
