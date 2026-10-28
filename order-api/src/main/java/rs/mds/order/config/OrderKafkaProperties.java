package rs.mds.order.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record OrderKafkaProperties(
        @Valid @NotNull Topics topics,
        @NotNull Duration publishTimeout
) {
    public record Topics(@NotBlank String orders) {
    }
}
