package rs.mds.inventory.domain;

public enum ReservationStatus {

    /** Stock was available and has been reserved for the order. */
    RESERVED,

    /** The requested quantity exceeds the currently available stock. */
    REJECTED_INSUFFICIENT_STOCK,

    /** The item does not exist in the inventory. */
    REJECTED_UNKNOWN_ITEM;

    public boolean isReserved() {
        return this == RESERVED;
    }
}
