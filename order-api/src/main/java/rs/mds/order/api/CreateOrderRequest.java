package rs.mds.order.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank @Size(max = 64) String orderId,
        @NotBlank @Size(max = 64) String itemId,
        @NotNull @Positive @Max(10_000) Integer quantity
) {
}
