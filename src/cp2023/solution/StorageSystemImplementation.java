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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import cp2023.base.*;
import cp2023.exceptions.*;

public class StorageSystemImplementation implements StorageSystem {

    // Enum for transfer type - ADD/REMOVE/MOVE.
    public enum TransferType {
        ADD, REMOVE, MOVE
    }

    // Enum for transfer phase - LEGAL/PREPARE_STARTS/PREPARE_ENDED/PERFORM_ENDED.
    public enum TransferPhase {
        // LEGAL - transfer did not throw any exceptions.
        LEGAL, PREPARE_STARTS, PREPARE_ENDED, PERFORM_ENDED
    }

    // Mutex for operating on a transfer.
    // Only prepare() and perform() methods will be run in parallel.
    private final Semaphore transferOperation = new Semaphore(1, true);

    private final HashMap<DeviceId, Integer> deviceTotalSlots; // Capacity of each device.
    private HashMap<ComponentId, DeviceId> componentPlacement; // Current placement of each component.
    private ConcurrentHashMap<DeviceId, AtomicInteger> deviceTakenSlots; // Number of slots taken up by components on each device.

    // Set of pairs (component, boolean) - true if component is transferred, false otherwise.
    private HashMap<ComponentId, Boolean> isComponentTransferred;

    private ConcurrentHashMap<ComponentTransfer, TransferPhase> componentTransferPhase; // Phase of each component transfer.

    // Queues for transfers waiting for space on each device.
    private ConcurrentHashMap<DeviceId, ConcurrentLinkedQueue<ComponentTransfer>> deviceQueues;

    /*
     * transferPhaseLatches hashmap will be updated when component transfer is inserted into the deviceQueue,
     * so transfers that do not have to wait for free space on destination device will never be put into this hashmap.
     *
     * ArrayList of latches for each ComponentTransfer will be of size 2.
     * First latch is released, when component transfer can call prepare().
     * Second latch is release, when component transfer can call perform().
     */
    private ConcurrentHashMap<ComponentTransfer, ArrayList<CountDownLatch>> transferPhaseLatches;

    private TransfersGraph graph; // Directed graph of MOVE transfers.


    public StorageSystemImplementation(HashMap<DeviceId, Integer> deviceTotalSlots,
                                       HashMap<ComponentId, DeviceId> componentPlacement) {
        this.deviceTotalSlots = deviceTotalSlots;
        this.componentPlacement = componentPlacement;
        this.componentTransferPhase = new ConcurrentHashMap<>();
        this.transferPhaseLatches = new ConcurrentHashMap<>();

        // Initialize isComponentTransferred map - at the beginning all components are not transferred.
        this.isComponentTransferred = new HashMap<>();
        for (ComponentId component : componentPlacement.keySet()) {
            this.isComponentTransferred.put(component, false);
        }

        // Initialize deviceTakenSlots map using componentPlacement map.
        this.deviceTakenSlots = new ConcurrentHashMap<>();
        for (Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()) {
            DeviceId device = entry.getValue();
            if (deviceTakenSlots.containsKey(device)) {
                deviceTakenSlots.get(device).incrementAndGet();
            } else {
                AtomicInteger slots = new AtomicInteger(1);
                deviceTakenSlots.put(device, slots);
            }
        }

        // Initialize deviceQueues
        this.deviceQueues = new ConcurrentHashMap<>();
        for (DeviceId device : deviceTotalSlots.keySet()) {
            deviceQueues.put(device, new ConcurrentLinkedQueue<>());
        }

        // Initialize graph of transfers.
        this.graph = new TransfersGraph(new LinkedList<>(deviceTotalSlots.keySet()));
    }

    private void acquire_semaphore(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            // Exception thrown per project specification.
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    private void init_transferPhaseLatch(ComponentTransfer transfer) {
        ArrayList<CountDownLatch> latches = new ArrayList<>();
        latches.add(0, new CountDownLatch(1)); // prepare() latch
        latches.add(1, new CountDownLatch(1)); // perform latch

        transferPhaseLatches.put(transfer, latches);
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        // Check for IllegalTransferType - not an ADD/REMOVE/MOVE operation.
        if (transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() == null ) {
            throw new IllegalTransferType(transfer.getComponentId());
        }

        // Assign a transfer type.
        TransferType transferType = assignTransferType(transfer);

        acquire_semaphore(transferOperation); // Acquire the mutex.

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

        // Update isComponentTransferred map.
        isComponentTransferred.put(component, true);

        // Update componentTransferPhase map - transfer is currently LEGAL.
        componentTransferPhase.put(transfer, TransferPhase.LEGAL);

        // End of checking the arguments of componentTransfer and basic legality of the operation on a component.

        // REMOVE transfer if its legal, it is performed immediately. (It is always allowed.)
        if (transferType == TransferType.REMOVE) {
            transferOperation.release(); // Release the mutex.

            componentTransferPhase.put(transfer, TransferPhase.PREPARE_STARTS);
            transfer.prepare();
            componentTransferPhase.put(transfer, TransferPhase.PREPARE_ENDED);
            transfer.perform();
            componentTransferPhase.put(transfer, TransferPhase.PERFORM_ENDED);

            acquire_semaphore(transferOperation); // Acquire the mutex.

            // Component is removed from the system
            componentPlacement.remove(component);
            deviceTakenSlots.get(source).decrementAndGet(); // Decrement number of slots taken up by components.
            isComponentTransferred.remove(component);
            componentTransferPhase.remove(transfer);

            transferOperation.release(); // Release the mutex.

            return; // REMOVE transfer is finished.
        }

        // If there is free space on the destination device, ADD/MOVE transfer starts.
        if (deviceTakenSlots.get(destination).get() < deviceTotalSlots.get(destination)) {
            transferOperation.release(); // Release the mutex.

            componentTransferPhase.put(transfer, TransferPhase.PREPARE_STARTS);
            transfer.prepare();
            componentTransferPhase.put(transfer, TransferPhase.PREPARE_ENDED);
            transfer.perform();
            componentTransferPhase.put(transfer, TransferPhase.PERFORM_ENDED);

            acquire_semaphore(transferOperation); // Acquire the mutex.

            if (transferType == TransferType.MOVE) {
                // Update data structures for source device.
                componentPlacement.remove(component);
                deviceTakenSlots.get(source).decrementAndGet();
                graph.removeEdge(transfer); // TODO: Check behaviour, if transfer was never waiting for execution
                                            // that is, what's the behaviour for transfers never added to the graph
            }

            // Steps that are identical for MOVE and ADD

            componentPlacement.put(component, destination);
            deviceTakenSlots.get(destination).incrementAndGet();
            isComponentTransferred.put(component, false);
            componentTransferPhase.remove(transfer);

            transferOperation.release(); // Release the mutex.

            return; // ADD or MOVE transfer is finished - case of enough space on destination device.
        }

        // Now we have to write code for executing transfer, when there is not enough space on the destination device
        // and transfer type is ADD/MOVE

        // Add transfer to the waiting queue of the destination device

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
