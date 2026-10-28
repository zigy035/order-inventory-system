package rs.mds.inventory.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "app")
public record InventoryProperties(
        @Valid @NotNull Kafka kafka,
        @NotEmpty Map<@NotBlank String, @NotNull @PositiveOrZero Integer> initialStock
) {

    public record Kafka(
            @Valid @NotNull Topics topics,
            @Valid @NotNull Retry retry
    ) {
    }

    public record Topics(
            @NotBlank String orders,
            @NotBlank String ordersDlt
    ) {
    }

    public record Retry(
            @Min(1) int maxAttempts,
            @NotNull Duration initialInterval,
            @Min(1) double multiplier,
            @NotNull Duration maxInterval
    ) {
    }
}
