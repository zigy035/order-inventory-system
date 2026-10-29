package rs.mds.inventory.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import rs.mds.inventory.domain.InventoryService;
import rs.mds.inventory.domain.Reservation;

import java.util.List;
import java.util.Map;

@RestController
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/inventory")
    public Map<String, Integer> stockLevels() {
        return inventoryService.stockLevels();
    }

    @GetMapping("/reservations")
    public List<Reservation> reservations() {
        return inventoryService.reservations();
    }

    @GetMapping("/reservations/{orderId}")
    public Reservation reservation(@PathVariable String orderId) {
        return inventoryService.findReservation(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "No processed order with id '" + orderId + "'. It may not have been processed yet."));
    }
}
