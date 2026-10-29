package rs.mds.inventory.messaging;

/**
 * Thrown when an incoming event cannot be processed because of its content
 * (missing fields or invalid values).
 * Retrying will never succeed, so the error handler routes it straight to the DLT.
 */
public class InvalidOrderEventException extends RuntimeException {

    public InvalidOrderEventException(String message) {
        super(message);
    }
}
