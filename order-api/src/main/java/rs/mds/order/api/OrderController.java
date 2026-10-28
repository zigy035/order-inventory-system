package rs.mds.order.api;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import rs.mds.order.service.OrderService;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderAcceptedResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        orderService.placeOrder(request.orderId(), request.itemId(), request.quantity());
        return ResponseEntity.accepted().body(OrderAcceptedResponse.accepted(request.orderId()));
    }
}
