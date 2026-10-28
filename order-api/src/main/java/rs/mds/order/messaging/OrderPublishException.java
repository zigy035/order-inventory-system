package rs.mds.order.messaging;

public class OrderPublishException extends RuntimeException {

    private final String orderId;

    public OrderPublishException(String orderId, Throwable cause) {
        super("Failed to publish event for orderId=" + orderId, cause);
        this.orderId = orderId;
    }

    public String getOrderId() {
        return orderId;
    }
}
