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
    public enum LatchPhase {
        PREPARE, PERFORM
    }

    /*
     * Enum for the current step of a transfer within the system.
     * LEGAL: The transfer has been checked and is legal to proceed.
     * STARTED: The transfer has started.
     * ENDED_PREPARE: The transfer has ended its prepare phase.
     * ENDED_PERFORM: The transfer has ended its perform phase.
     */
    public enum TransferStep {
        LEGAL, STARTED, ENDED_PREPARE, ENDED_PERFORM
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
    private final ConcurrentHashMap<ComponentTransfer, EnumMap<LatchPhase, CountDownLatch>> transferPhaseLatches;

    // Current step of any transfer inside the system.
    private final ConcurrentHashMap<ComponentTransfer, TransferStep> transferStep;

    private final TransfersGraph graph; // Directed graph of MOVE transfers.


    public StorageSystemImplementation(HashMap<DeviceId, Integer> deviceTotalSlots,
                                       HashMap<ComponentId, DeviceId> componentPlacement) {
        this.deviceTotalSlots = deviceTotalSlots;
        this.componentPlacement = componentPlacement;
        this.transferPhaseLatches = new ConcurrentHashMap<>();
        this.waitsFor = new ConcurrentHashMap<>();
        this.transferStep = new ConcurrentHashMap<>();

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
        DeviceId source = transfer.getSourceDeviceId();
        DeviceId destination = transfer.getDestinationDeviceId();

        acquire_semaphore(transferOperation); // Acquire the mutex.

        checkIfTransferIsLegal(transfer);
        transferStep.put(transfer, TransferStep.LEGAL);

        // REMOVE transfer if its legal, it is performed immediately. (It is always allowed.)
        // OR
        // If there is free space on the destination device, ADD/MOVE transfer starts.
        if (transferType == TransferType.REMOVE ||
                (deviceTakenSlots.get(destination).get() < deviceTotalSlots.get(destination)) ) {
            if (destination != null)
                deviceTakenSlots.get(destination).incrementAndGet(); // prevents race condition

            if (source != null) {
                lookForWaitingTransfers(transfer); // If waiting transfer is found, countDown() on its latch
            }

            transferStep.put(transfer, TransferStep.STARTED);

            transferOperation.release(); // Release the mutex.

            transfer.prepare();
            transferStep.put(transfer, TransferStep.ENDED_PREPARE);
            modifyMapsAfterPrepare(transfer);

            transfer.perform();
            transferStep.put(transfer, TransferStep.ENDED_PERFORM);
            modifyMapsAfterPerform(transfer);

            return; // ADD or MOVE transfer is finished - case of enough space on destination device.
                    // REMOVE transfer is finished
        }

        // Below is logic for executing transfer, when there is not enough space on the destination device
        // and transfer type is ADD/MOVE

        // Where transfers will wait on latches? Answer: just before they call prepare() and perform() respectively,
        init_transferPhaseLatch(transfer); // Initialize latches for transfer
        deviceQueues.get(destination).add(transfer); // Add transfer to the waiting queue of the destination device

        // MOVE transfers that are waiting in the deviceQueue look for a cycle
        if (transferType == TransferType.MOVE) {
            // Modifies graph and if cycle is found, countDown() all PREPARE latches for transfers inside the cycle.
            lookForCycle(transfer);
        }

        // Check transferStep of all transfers with a given source
        ComponentTransfer started = lookForStartedTransfers(destination);
        if (started != null) {
            deviceQueues.get(destination).remove(transfer);
            waitsFor.put(started, transfer);
            transferPhaseLatches.get(transfer).get(LatchPhase.PREPARE).countDown();
        }

        transferOperation.release(); // release the mutex

        awaitLatch(transferPhaseLatches.get(transfer).get(LatchPhase.PREPARE)); // waits before calling prepare()

        if (transferType == TransferType.MOVE) {
            acquire_semaphore(transferOperation);
            lookForWaitingTransfers(transfer); // If waiting transfer is found, countDown() on latch
            transferOperation.release();
        }

        transferStep.put(transfer, TransferStep.STARTED);
        transfer.prepare();
        transferStep.put(transfer, TransferStep.ENDED_PREPARE);
        modifyMapsAfterPrepare(transfer);

        awaitLatch(transferPhaseLatches.get(transfer).get(LatchPhase.PERFORM)); // waits before calling perform()

        transfer.perform();
        transferStep.put(transfer, TransferStep.ENDED_PERFORM);
        modifyMapsAfterPerform(transfer);
    } // End of execute()

    /*
     * INPUT: ArrayList of ComponentTransfer objects representing a cycle in the graph of transfers.
     * FUNCTION: Updates the 'waitsFor' map for all transfers in the cycle.
     * For each transfer in the cycle, it sets the next transfer in the cycle as the one it waits for in 'waitsFor' map.
     * The last transfer in the cycle waits for the first one, closing the cycle.
     * OUTPUT: No explicit output. The function modifies the 'waitsFor' map as a side effect.
     */
    private void cycleTransfers_waitsFor_Update(ArrayList<ComponentTransfer> cycle) {
        int cycleSize = cycle.size();
        for (int i = 0; i < cycleSize; i++) {
            ComponentTransfer currentTransfer = cycle.get(i);

            // Remove transfer from the queue
            deviceQueues.get(currentTransfer.getSourceDeviceId()).remove();

            // Get the next transfer in the cycle, wrap around to the first element if at the end
            ComponentTransfer nextTransfer = cycle.get((i + 1) % cycleSize);
            waitsFor.put(nextTransfer, currentTransfer);
        }
    }

    private ComponentTransfer lookForStartedTransfers(DeviceId source) {
        for (Map.Entry<ComponentTransfer, TransferStep> entry : transferStep.entrySet()) {
            ComponentTransfer transfer = entry.getKey();
            if (transfer.getSourceDeviceId() != null) {
                TransferStep step = entry.getValue();

                if (transfer.getSourceDeviceId().equals(source) &&
                        (step == TransferStep.STARTED ||
                                step == TransferStep.ENDED_PREPARE || step == TransferStep.ENDED_PERFORM) &&
                        !waitsFor.containsKey(transfer)) {
                    return transfer;
                }
            }
        }
        return null;
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

    // Look for transfer inside source device queue and then if found, countDown() its PREPARE latch.
    private void lookForWaitingTransfers(ComponentTransfer transfer) {
        DeviceId source = transfer.getSourceDeviceId();

        // Transfer waiting for us can call prepare()
        ComponentTransfer whoWaitsForMe = deviceQueues.get(source).poll();
        if (whoWaitsForMe != null) {
            waitsFor.put(transfer, whoWaitsForMe);
            transferPhaseLatches.get(whoWaitsForMe).get(LatchPhase.PREPARE).countDown();
        }
    }

    // Looks for cycle and then if found, countDown() all PREPARE latches for transfers inside the cycle.
    private void lookForCycle(ComponentTransfer transfer) {
        graph.addEdge(transfer);

        // Look for a cycle withing graph of transfers.
        ArrayList<ComponentTransfer> cycle = new ArrayList<>(graph.cycleOfTransfers(transfer));

        if (!cycle.isEmpty()) {
            // Update waitsFor map for all transfers in a cycle
            cycleTransfers_waitsFor_Update(cycle);

            // Call prepare() in all transfers in a cycle
            for (ComponentTransfer cycle_transfer : cycle) {
                // Transfer that starts the cycle calls countDown() on its own PREPARE latch.
                transferPhaseLatches.get(cycle_transfer).get(LatchPhase.PREPARE).countDown();
            }
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

    // All the checks for possible exceptions regarding executed transfer.
    private void checkIfTransferIsLegal(ComponentTransfer transfer) throws TransferException {
        TransferType transferType = assignTransferType(transfer);
        DeviceId source = transfer.getSourceDeviceId();
        DeviceId destination = transfer.getDestinationDeviceId();

        // Check if source device exists.
        if (source != null && !deviceTotalSlots.containsKey(source)) {
            throw new DeviceDoesNotExist(source);
        }

        // Check if destination device exists.
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
                && source != null &&
                (!componentPlacement.containsKey(component) || componentPlacement.get(component).compareTo(source) != 0) ) {
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

        ComponentId componentId = transfer.getComponentId();
        DeviceId source = transfer.getSourceDeviceId();

        if (source != null) {
            componentPlacement.remove(componentId);
            deviceTakenSlots.get(source).decrementAndGet();

            // Transfer waiting for us can call prepare()
            ComponentTransfer whoWaitsForMe = waitsFor.get(transfer);
            if (whoWaitsForMe != null) {
                transferPhaseLatches.get(whoWaitsForMe).get(LatchPhase.PERFORM).countDown();
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
    private void modifyMapsAfterPerform(ComponentTransfer transfer) {
        acquire_semaphore(transferOperation);

        TransferType transferType = assignTransferType(transfer);
        ComponentId componentId = transfer.getComponentId();
        DeviceId destination = transfer.getDestinationDeviceId();

        if (transferType == TransferType.MOVE) graph.removeEdge(transfer);

        if (destination != null) {
            componentPlacement.put(componentId, destination);
        }

        isComponentTransferred.put(componentId, false);
        waitsFor.remove(transfer);
        transferPhaseLatches.remove(transfer);
        transferStep.remove(transfer);

        if (transferType == TransferType.REMOVE)
            isComponentTransferred.remove(componentId);

        transferOperation.release();
    }

}