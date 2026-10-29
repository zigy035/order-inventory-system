package rs.mds.inventory.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.UUID;

/**
 * Consumer-side representation of the OrderCreated event.
 * Intentionally a separate class from the producer's; the services share only the JSON contract.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedEvent(
        @NotNull UUID eventId,
        Instant occurredAt,
        @NotBlank @Size(max = 64) String orderId,
        @NotBlank @Size(max = 64) String itemId,
        @NotNull @Positive Integer quantity
) {
}
