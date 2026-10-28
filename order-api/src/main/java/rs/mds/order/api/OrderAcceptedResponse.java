package rs.mds.order.api;

public record OrderAcceptedResponse(String orderId, String status) {

    public static OrderAcceptedResponse accepted(String orderId) {
        return new OrderAcceptedResponse(orderId, "ACCEPTED");
    }
}
