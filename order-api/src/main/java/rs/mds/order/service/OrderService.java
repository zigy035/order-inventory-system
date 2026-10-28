package rs.mds.order.service;

import org.springframework.stereotype.Service;
import rs.mds.order.messaging.OrderCreatedEvent;
import rs.mds.order.messaging.OrderEventPublisher;

@Service
public class OrderService {

    private final OrderEventPublisher eventPublisher;

    public OrderService(OrderEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void placeOrder(String orderId, String itemId, int quantity) {
        eventPublisher.publish(OrderCreatedEvent.of(orderId, itemId, quantity));
    }
}
