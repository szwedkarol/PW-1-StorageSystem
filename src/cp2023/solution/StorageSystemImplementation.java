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

    // TODO: Likely deprecated - to be (safely) deleted
    // Enum for transfer phase - LEGAL/PREPARE_STARTS/PREPARE_ENDED/PERFORM_ENDED.
    public enum TransferPhase {
        // LEGAL - transfer did not throw any exceptions.
        LEGAL, PREPARE_STARTS, PREPARE_ENDED, PERFORM_ENDED
    }

    // TODO: Rename to TransferPhase after removing deprecated enum
    public enum LatchPhase {
        PREPARE, PERFORM
    }

    // Mutex for operating on a transfer.
    // Only prepare() and perform() methods will be run in parallel.
    private final Semaphore transferOperation = new Semaphore(1, true);

    private final HashMap<DeviceId, Integer> deviceTotalSlots; // Capacity of each device.
    private final HashMap<ComponentId, DeviceId> componentPlacement; // Current placement of each component.
    private final ConcurrentHashMap<DeviceId, AtomicInteger> deviceTakenSlots; // Number of slots taken up by components on each device.

    // Set of pairs (component, boolean) - true if component is transferred, false otherwise.
    private final HashMap<ComponentId, Boolean> isComponentTransferred;

    // Queues for transfers waiting for space on each device.
    private final ConcurrentHashMap<DeviceId, ConcurrentLinkedQueue<ComponentTransfer>> deviceQueues;

    // "You cannot call prepare() on the place being freed by transfer X and then call perform() on the place of transfer Y."
    // Key has to wait for transfer Value to finish its phases (prepare()/perform()).
    private final ConcurrentHashMap<ComponentTransfer, ComponentTransfer> waitsFor;

    /*
     * transferPhaseLatches hashmap will be updated when component transfer is inserted into the deviceQueue,
     * so transfers that do not have to wait for free space on destination device will never be put into this hashmap.
     *
     * EnumMap of latches for each ComponentTransfer will be of size 2.
     * PREPARE latch is released, when component transfer can call prepare().
     * PERFORM latch is release, when component transfer can call perform().
     */
    private final ConcurrentHashMap<ComponentTransfer, EnumMap<LatchPhase, CountDownLatch>> transferPhaseLatches;

    private final TransfersGraph graph; // Directed graph of MOVE transfers.


    public StorageSystemImplementation(HashMap<DeviceId, Integer> deviceTotalSlots,
                                       HashMap<ComponentId, DeviceId> componentPlacement) {
        this.deviceTotalSlots = deviceTotalSlots;
        this.componentPlacement = componentPlacement;
        this.transferPhaseLatches = new ConcurrentHashMap<>();
        this.waitsFor = new ConcurrentHashMap<>();

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

    // semaphore.acquire() with exception handling.
    private void acquire_semaphore(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            // Exception thrown per project specification.
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    /*
     * INPUT: ComponentTransfer object for which the latches need to be initialized.
     * FUNCTION: Updates the 'transferPhaseLatches' map for the given transfer.
     * It creates an EnumMap with two entries, one for each phase (PREPARE and PERFORM), and associates a new
     * CountDownLatch with each phase. The CountDownLatch for each phase is initialized with a count of 1.
     * OUTPUT: No explicit output. The function modifies the 'transferPhaseLatches' map as a side effect.
     */
    private void init_transferPhaseLatch(ComponentTransfer transfer) {
        EnumMap<LatchPhase, CountDownLatch> latches = new EnumMap<>(LatchPhase.class);
        latches.put(LatchPhase.PREPARE, new CountDownLatch(1));
        latches.put(LatchPhase.PERFORM, new CountDownLatch(1));

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

        // End of checking the arguments of componentTransfer and basic legality of the operation on a component.

        // REMOVE transfer if its legal, it is performed immediately. (It is always allowed.)
        if (transferType == TransferType.REMOVE) {
            transferOperation.release(); // Release the mutex.

            // TODO: Search for transfers waiting to happen inside deviceQueues
            transfer.prepare();

            transfer.perform();


            acquire_semaphore(transferOperation); // Acquire the mutex.

            // Component is removed from the system
            componentPlacement.remove(component);
            deviceTakenSlots.get(source).decrementAndGet(); // Decrement number of slots taken up by components.
            isComponentTransferred.remove(component);

            transferOperation.release(); // Release the mutex.

            return; // REMOVE transfer is finished.
        }

        // If there is free space on the destination device, ADD/MOVE transfer starts.
        if (deviceTakenSlots.get(destination).get() < deviceTotalSlots.get(destination)) {
            deviceTakenSlots.get(destination).incrementAndGet(); // prevents race condition
            transferOperation.release(); // Release the mutex.


            transfer.prepare();
            // TODO: DOES ANYTHING HAVE TO HAPPEN HERE?
            transfer.perform();


            acquire_semaphore(transferOperation); // Acquire the mutex.

            if (transferType == TransferType.MOVE) {
                // Update data structures for source device.
                componentPlacement.remove(component);
                deviceTakenSlots.get(source).decrementAndGet();
            }

            // Steps that are identical for MOVE and ADD

            componentPlacement.put(component, destination);
            isComponentTransferred.put(component, false);

            transferOperation.release(); // Release the mutex.

            return; // ADD or MOVE transfer is finished - case of enough space on destination device.
        }

        // Now we have to write code for executing transfer, when there is not enough space on the destination device
        // and transfer type is ADD/MOVE

        // TODO: Where transfers will wait on latches?
        init_transferPhaseLatch(transfer); // Initialize latches for transfer
        deviceQueues.get(destination).add(transfer); // Add transfer to the waiting queue of the destination device

        // Program logic for MOVE transfers that have to wait in the queue
        if (transferType == TransferType.MOVE) {
            graph.addEdge(transfer);

            // Look for a cycle withing graph of transfers.
            ArrayList<ComponentTransfer> cycle = new ArrayList<>(graph.cycleOfTransfers(transfer));

            if (!cycle.isEmpty()) {
                // Update waitsFor map for all transfers in a cycle
                cycleTransfersWaitForUpdate(cycle);

                // Call prepare() in all transfers in a cycle
                for (ComponentTransfer cycle_transfer : cycle) {
                    // TODO: Transfer that is closing the cycle cannot wait on the latch as there may not be any
                    // other thread to wake him up
                    assert transferPhaseLatches.containsKey(cycle_transfer);
                    transferPhaseLatches.get(cycle_transfer).get(LatchPhase.PREPARE).countDown(); // Latch for prepare()

                    // TODO: Finish program flow for cycles
                }
            }
        }

        // TODO: "You cannot call prepare() on the place being freed by transfer X and then call perform() on the place of transfer Y."

        transferOperation.release();

        awaitLatch(transferPhaseLatches.get(transfer).get(LatchPhase.PREPARE)); // waits on latch before calling prepare()

        // Transfer waiting for us can call prepare()
        ComponentTransfer whoWaitsForMe = waitsFor.get(transfer);
        if (whoWaitsForMe != null) {
            transferPhaseLatches.get(whoWaitsForMe).get(LatchPhase.PREPARE).countDown();
        }

        transfer.prepare();

        // TODO: Update all maps after prepare()

        // Transfer waiting for us can call perform()
        if (whoWaitsForMe != null) {
            transferPhaseLatches.get(whoWaitsForMe).get(LatchPhase.PERFORM).countDown();
        }

        awaitLatch(transferPhaseLatches.get(transfer).get(LatchPhase.PERFORM)); // waits on latch before calling perform()



    }

    // TODO: (Maybe) Create method for updating maps instead of copying code

    /*
     * INPUT: ArrayList of ComponentTransfer objects representing a cycle in the graph of transfers.
     * FUNCTION: Updates the 'waitsFor' map for all transfers in the cycle.
     * For each transfer in the cycle, it sets the next transfer in the cycle as the one it waits for in 'waitsFor' map.
     * The last transfer in the cycle waits for the first one, closing the cycle.
     * OUTPUT: No explicit output. The function modifies the 'waitsFor' map as a side effect.
     */
    private void cycleTransfersWaitForUpdate(ArrayList<ComponentTransfer> cycle) {
        int cycleSize = cycle.size();
        for (int i = 0; i < cycleSize; i++) {
            ComponentTransfer currentTransfer = cycle.get(i);

            // Get the next transfer in the cycle, wrap around to the first element if at the end
            ComponentTransfer nextTransfer = cycle.get((i + 1) % cycleSize);
            waitsFor.put(currentTransfer, nextTransfer);
        }
    }

    // latch.await() with exception handling.
    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            // Exception thrown per project specification.
            throw new RuntimeException("panic: unexpected thread interruption");
        }
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

    private class suggestedTEMP { // TODO: Review Copilot suggestion for updating maps instead of repeating code.

        private void updateMapsAfterTransfer(ComponentTransfer transfer, TransferType transferType) {
        ComponentId component = transfer.getComponentId();
        DeviceId source = transfer.getSourceDeviceId();
        DeviceId destination = transfer.getDestinationDeviceId();

        switch (transferType) {
            case ADD:
                // Update data structures for ADD transfer
                componentPlacement.put(component, destination);
                deviceTakenSlots.get(destination).incrementAndGet();
                break;
            case REMOVE:
                // Update data structures for REMOVE transfer
                componentPlacement.remove(component);
                deviceTakenSlots.get(source).decrementAndGet();
                break;
            case MOVE:
                // Update data structures for MOVE transfer
                componentPlacement.remove(component);
                deviceTakenSlots.get(source).decrementAndGet();
                componentPlacement.put(component, destination);
                deviceTakenSlots.get(destination).incrementAndGet();
                break;
        }

        // Common updates for all transfer types
        isComponentTransferred.put(component, false);
    }

    }

}
