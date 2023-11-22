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

    /*
     * Enum for transfer phase latches - PREPARE/PERFORM.
     * PREPARE - transfer can call prepare().
     * PERFORM - transfer can call perform().
     */
    public enum TransferPhase {
        PREPARE, PERFORM
    }

    // Mutex for operating on a transfer and checking if it is legal.
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
    // Value has to wait for Key to finish its phases (prepare()/perform()).
    // <whoHasToAct, whoHasToWait>
    private final ConcurrentHashMap<ComponentTransfer, ComponentTransfer> waitsFor;

    /*
     * transferPhaseLatches hashmap will be updated when component transfer is inserted into the deviceQueue,
     * so transfers that do not have to wait for free space on destination device will never be put into this hashmap.
     *
     * EnumMap of latches for each ComponentTransfer will be of size 2.
     * PREPARE latch is released, when component transfer can call prepare().
     * PERFORM latch is release, when component transfer can call perform().
     */
    private final ConcurrentHashMap<ComponentTransfer, EnumMap<TransferPhase, CountDownLatch>> transferPhaseLatches;

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
        EnumMap<TransferPhase, CountDownLatch> latches = new EnumMap<>(TransferPhase.class);
        latches.put(TransferPhase.PREPARE, new CountDownLatch(1));
        latches.put(TransferPhase.PERFORM, new CountDownLatch(1));

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
        if (transferType == TransferType.ADD && destination != null && componentPlacement.get(component) != null) {
            throw new ComponentAlreadyExists(component, destination);
        }

        // Check if component is moved from device to the same device.
        if (transferType == TransferType.MOVE && source != null && destination != null && source.compareTo(destination) == 0) {
            throw new ComponentDoesNotNeedTransfer(component, source);
        }

        // Check if component exists on the source device for MOVE or REMOVE operations.
        if ((transferType == TransferType.MOVE || transferType == TransferType.REMOVE)
                && source != null && componentPlacement.get(component).compareTo(source) != 0) {
            throw new ComponentDoesNotExist(component, source);
        }

        if (!isComponentTransferred.containsKey(component))
            isComponentTransferred.put(component, false);

        // Check if component is already being transferred.
        if (isComponentTransferred.get(component)) {
            throw new ComponentIsBeingOperatedOn(component);
        }

        // Component transfer starts, when it is legal.
        isComponentTransferred.put(component, true);

        // End of checking the arguments of componentTransfer and basic legality of the operation on a component.

        // REMOVE transfer if its legal, it is performed immediately. (It is always allowed.)
        if (transferType == TransferType.REMOVE) {
            lookForWaitingTransfers(transfer); // If waiting transfer is found, countDown() on its latch

            transferOperation.release(); // Release the mutex.

            transfer.prepare();
            modifyMapsAfterPrepare(transfer);

            transfer.perform();
            modifyMapsAfterPerform(transfer);

            return; // REMOVE transfer is finished.
        }

        // TODO: REMOVE AND ADD/MOVE program flows (when transfer does not have to wait in any queue) are very similar

        // If there is free space on the destination device, ADD/MOVE transfer starts.
        if (deviceTakenSlots.get(destination).get() < deviceTotalSlots.get(destination)) {
            deviceTakenSlots.get(destination).incrementAndGet(); // prevents race condition

            if (transferType == TransferType.MOVE) {
                lookForWaitingTransfers(transfer); // If waiting transfer is found, countDown() on its latch
            }

            transferOperation.release(); // Release the mutex.

            transfer.prepare();
            modifyMapsAfterPrepare(transfer);

            transfer.perform();
            modifyMapsAfterPerform(transfer);

            return; // ADD or MOVE transfer is finished - case of enough space on destination device.
        }

        // Below is logic for executing transfer, when there is not enough space on the destination device
        // and transfer type is ADD/MOVE

        // Where transfers will wait on latches? Answer: just before they call prepare() and perform() respectively,
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
                    assert transferPhaseLatches.containsKey(cycle_transfer);

                    // Transfer that starts the cycle calls countDown() on its own PREPARE latch.
                    transferPhaseLatches.get(cycle_transfer).get(TransferPhase.PREPARE).countDown();
                }
            }
        }

        transferOperation.release(); // release the mutex

        awaitLatch(transferPhaseLatches.get(transfer).get(TransferPhase.PREPARE)); // waits before calling prepare()

        // TODO: Is it possible inside waitsFor there will be two pairs:
        // <t1, t_waiting> and <t2, t_waiting>?
        if (transferType == TransferType.MOVE) {
            acquire_semaphore(transferOperation);
            lookForWaitingTransfers(transfer); // If waiting transfer is found, countDown() on latch
            transferOperation.release();
        }

        transfer.prepare();
        modifyMapsAfterPrepare(transfer);

        awaitLatch(transferPhaseLatches.get(transfer).get(TransferPhase.PERFORM)); // waits before calling perform()

        transfer.perform();
        modifyMapsAfterPerform(transfer);
    } // End of execute()

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
    // TODO: Check if inside cycleTransfersWaitForUpdate there is a correct order of transfers that are put in the map.

    // latch.await() with exception handling.
    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            // Exception thrown per project specification.
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    private void lookForWaitingTransfers(ComponentTransfer transfer) {
        DeviceId source = transfer.getSourceDeviceId();

        // Transfer waiting for us can call prepare()
        ComponentTransfer whoWaitsForMe = deviceQueues.get(source).poll();
        if (whoWaitsForMe != null) {
            waitsFor.put(transfer, whoWaitsForMe);
            transferPhaseLatches.get(whoWaitsForMe).get(TransferPhase.PREPARE).countDown();
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

    /*
     * INPUT: ComponentTransfer object which has just called prepare() method.
     *
     * FUNCTION: Updates the maps after the prepare() method of a ComponentTransfer is called.
     * Depending on the type of the transfer (ADD, REMOVE, or MOVE), it updates the componentPlacement and
     * deviceTakenSlots maps.
     * For REMOVE and MOVE transfers, it removes the component from its source device in the componentPlacement map
     * and decrements the count of taken slots on the source device in the deviceTakenSlots map.
     * If there is a transfer waiting for the current transfer to finish, it allows the waiting transfer to call
     * its perform() method by counting down its PERFORM latch.
     *
     * OUTPUT: No explicit output. Modifies the componentPlacement, deviceTakenSlots,
     * and transferPhaseLatches maps as a side effect.
     */
    private void modifyMapsAfterPrepare(ComponentTransfer transfer) {
        acquire_semaphore(transferOperation);

        TransferType transferType = assignTransferType(transfer);
        ComponentId componentId = transfer.getComponentId();
        DeviceId source = transfer.getSourceDeviceId();

        if (transferType == TransferType.MOVE || transferType == TransferType.REMOVE) {
            componentPlacement.remove(componentId);
            deviceTakenSlots.get(source).decrementAndGet();

            // Transfer waiting for us can call prepare()
            ComponentTransfer whoWaitsForMe = waitsFor.get(transfer);
            if (whoWaitsForMe != null) {
                transferPhaseLatches.get(whoWaitsForMe).get(TransferPhase.PERFORM).countDown();
            }
        }

        transferOperation.release();
    }

    /*
     * INPUT: ComponentTransfer object which has just called perform() method.
     *
     * FUNCTION: Updates the maps after the perform() method of a ComponentTransfer is called.
     * Depending on the type of the transfer (ADD, MOVE), it updates the componentPlacement and deviceTakenSlots maps.
     * For ADD and MOVE transfers, it adds the component to its destination device in the componentPlacement map and
     * increments the count of taken slots on the destination device in the deviceTakenSlots map.
     * If the transfer type is MOVE, it also removes the edge representing the transfer from the graph of transfers.
     *
     * OUTPUT: No explicit output. Modifies the componentPlacement, deviceTakenSlots,
     * isComponentTransferred, waitsFor, and transferPhaseLatches maps as a side effect.
     */
    private synchronized void modifyMapsAfterPerform(ComponentTransfer transfer) {
        acquire_semaphore(transferOperation);

        TransferType transferType = assignTransferType(transfer);
        ComponentId componentId = transfer.getComponentId();
        DeviceId destination = transfer.getDestinationDeviceId();

        if (transferType == TransferType.MOVE) graph.removeEdge(transfer);

        if (transferType == TransferType.ADD || transferType == TransferType.MOVE) {
            componentPlacement.put(componentId, destination);
            deviceTakenSlots.get(destination).incrementAndGet();
        }

        isComponentTransferred.put(componentId, false);
        waitsFor.remove(transfer);
        transferPhaseLatches.remove(transfer);

        if (transferType == TransferType.REMOVE)
            isComponentTransferred.remove(componentId);

        transferOperation.release();
    }

}
