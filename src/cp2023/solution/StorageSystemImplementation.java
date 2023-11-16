/*
 * University of Warsaw
 * Concurrent Programming Course 2023/2024
 * Java Assignment
 *
 * Authors: Karol Szwed (ks430171@students.mimuw.edu.pl)
 */
package cp2023.solution;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import cp2023.base.*;
import cp2023.exceptions.*;

public class StorageSystemImplementation implements StorageSystem {

    // Enum for transfer type - ADD/REMOVE/MOVE.
    public enum TransferType {
        ADD, REMOVE, MOVE
    }

    // Mutex for operating on a transfer.
    // Only prepare() and perform() methods will be run in parallel.
    private final Semaphore transferOperation = new Semaphore(1, true);

    private final HashMap<DeviceId, Integer> deviceTotalSlots; // Capacity of each device.
    private HashMap<ComponentId, DeviceId> componentPlacement; // Current placement of each component.
    private HashMap<DeviceId, Integer> deviceTakenSlots; // Number of slots taken up by components on each device.

    // Set of pairs (component, boolean) - true if component is transferred, false otherwise.
    private HashMap<ComponentId, Boolean> isComponentTransferred;

    private TransfersGraph graph; // Directed graph of MOVE transfers.


    public StorageSystemImplementation(HashMap<DeviceId, Integer> deviceTotalSlots,
                                       HashMap<ComponentId, DeviceId> componentPlacement) {
        this.deviceTotalSlots = deviceTotalSlots;
        this.componentPlacement = componentPlacement;

        // Initialize isComponentTransferred map - at the beginning all components are not transferred.
        this.isComponentTransferred = new HashMap<>();
        for (ComponentId component : componentPlacement.keySet()) {
            this.isComponentTransferred.put(component, false);
        }

        // Initialize deviceTakenSlots map using componentPlacement map.
        this.deviceTakenSlots = new HashMap<>();
        for (Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()) {
            DeviceId device = entry.getValue();
            if (deviceTakenSlots.containsKey(device)) {
                deviceTakenSlots.put(device, deviceTakenSlots.get(device) + 1);
            } else {
                deviceTakenSlots.put(device, 1);
            }
        }

        // Initialize graph of transfers.
        this.graph = new TransfersGraph(new LinkedList<>(deviceTotalSlots.keySet()));
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        // Check for IllegalTransferType - not an ADD/REMOVE/MOVE operation.
        if (transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() == null ) {
            throw new IllegalTransferType(transfer.getComponentId());
        }

        // Assign a transfer type.
        TransferType transferType = assignTransferType(transfer);

        try {
            transferOperation.acquire();
        } catch (InterruptedException e) {
            // Exception thrown per project specification.
            throw new RuntimeException("panic: unexpected thread interruption");
        }


        // Check if source device exists.
        DeviceId source = transfer.getSourceDeviceId();
        if (source != null && !deviceTotalSlots.containsKey(source)) {
            throw new DeviceDoesNotExist(source);
        }

        // Check if destination device exists.
        DeviceId destination = transfer.getDestinationDeviceId();
        if (destination != null && !deviceTotalSlots.containsKey(destination)) {
            throw new DeviceDoesNotExist(destination);
        }

        // Check if component exists on the destination device.
        ComponentId component = transfer.getComponentId();
        if (transferType == TransferType.ADD && componentPlacement.get(component) == destination) {
            assert destination != null; // TransferType must be ADD
            throw new ComponentAlreadyExists(component, destination);
        }

        // Check if component is moved from device to the same device.
        if (transferType == TransferType.MOVE && source == destination) {
            assert source != null; // TransferType must be MOVE
            throw new ComponentDoesNotNeedTransfer(component, source);
        }

        // Check if component exists on the source device for MOVE or REMOVE operations.
        if ((transferType == TransferType.MOVE || transferType == TransferType.REMOVE)
                && componentPlacement.get(component) != source) {
            assert source != null; // TransferType must be MOVE or REMOVE
            throw new ComponentDoesNotExist(component, source);
        }

        // Check if component is already being transferred.
        if (isComponentTransferred.get(component)) {
            throw new ComponentIsBeingOperatedOn(component);
        }

        // End of checking the arguments of componentTransfer and basic legality of the operation on a component.

        // Look for a cycle withing graph of transfers.

    }

    /*
     * Assigns a transfer type to a component transfer.
     * INPUT: ComponentTransfer object.
     * FUNCTION: Checks if the transfer is an ADD/REMOVE/MOVE operation using getSourceDeviceID() and
     * getDestinationDeviceID() methods.
     * OUTPUT: TransferType enum - ADD/REMOVE/MOVE.
     */
    private TransferType assignTransferType(ComponentTransfer transfer) {
        // IllegalTransferType is checked for in execute() method.
        if (transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() != null) {
            return TransferType.ADD;
        } else if (transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() == null) {
            return TransferType.REMOVE;
        } else {
            return TransferType.MOVE;
        }
    }
}
