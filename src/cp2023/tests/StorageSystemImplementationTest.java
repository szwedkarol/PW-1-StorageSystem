/*
 * University of Warsaw
 * Concurrent Programming Course 2023/2024
 * Java Assignment
 *
 * Authors: Karol Szwed (ks430171@students.mimuw.edu.pl)
 */
package cp2023.tests;

import cp2023.solution.StorageSystemImplementation;
import cp2023.solution.TransfersGraph;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;

import cp2023.base.*;
import cp2023.solution.*;

import static org.junit.jupiter.api.Assertions.*;

public class StorageSystemImplementationTest {

    @Test
    public void dfsReturnsFalseWhenNoCycleInGraph() {
        // Create a TransfersGraph instance
        LinkedList<DeviceId> devices = new LinkedList<>();
        DeviceId device1 = new DeviceId(1);
        DeviceId device2 = new DeviceId(2);
        DeviceId device3 = new DeviceId(3);
        devices.add(device1);
        devices.add(device2);
        devices.add(device3);
        TransfersGraph graph = new TransfersGraph(devices);

        // Add edges (transfers) to the graph to form a cycle
        ComponentId component1 = new ComponentId(101);
        ComponentId component2 = new ComponentId(102);

        ComponentTransfer transfer1 = new ComponentTransferImplementation(component1, device1, device2);
        ComponentTransfer transfer2 = new ComponentTransferImplementation(component2, device2, device3);


        graph.addEdge(transfer1);
        graph.addEdge(transfer2);

        LinkedList<TransfersGraph.DeviceNode> cycle = new LinkedList<>();
        assertFalse(graph.dfs(graph.getDeviceNode(device1), cycle));
        assertTrue(cycle.isEmpty());
    }

    @Test
    public void testDFSCycleInGraph() {
        // Create a TransfersGraph instance
        LinkedList<DeviceId> devices = new LinkedList<>();
        DeviceId device1 = new DeviceId(1);
        DeviceId device2 = new DeviceId(2);
        DeviceId device3 = new DeviceId(3);
        devices.add(device1);
        devices.add(device2);
        devices.add(device3);
        TransfersGraph graph = new TransfersGraph(devices);

        // Add edges (transfers) to the graph to form a cycle
        ComponentId component1 = new ComponentId(101);
        ComponentId component2 = new ComponentId(102);
        ComponentId component3 = new ComponentId(103);
        ComponentTransfer transfer1 = new ComponentTransferImplementation(component1, device1, device2);
        ComponentTransfer transfer2 = new ComponentTransferImplementation(component2, device2, device3);
        ComponentTransfer transfer3 = new ComponentTransferImplementation(component3, device3, device1);
        graph.addEdge(transfer1);
        graph.addEdge(transfer2);
        graph.addEdge(transfer3);

        // Call the dfs() method with a node that is part of the cycle
        LinkedList<TransfersGraph.DeviceNode> deviceNodesCycle = new LinkedList<>();
        TransfersGraph.DeviceNode startNode = graph.getDeviceNode(device1);
        boolean hasCycle = graph.dfs(startNode, deviceNodesCycle);

        // Assert that the returned value is true and the deviceNodesCycle list contains the correct sequence of nodes that form the cycle
        assertTrue(hasCycle);
        assertEquals(3, deviceNodesCycle.size());
        assertTrue(deviceNodesCycle.contains(graph.getDeviceNode(device1)));
        assertTrue(deviceNodesCycle.contains(graph.getDeviceNode(device2)));
        assertTrue(deviceNodesCycle.contains(graph.getDeviceNode(device3)));
    }



/*
    // This version of the test is not needed anymore, because the dfs() now works differently (returns list of
    // DeviceNode objects that make up a cycle).
    @Test
    public void testCycleOfTransfers() {
        // Create a TransfersGraph instance
        LinkedList<DeviceId> devices = new LinkedList<>();
        DeviceId device1 = new DeviceId(1);
        DeviceId device2 = new DeviceId(2);
        DeviceId device3 = new DeviceId(3);
        devices.add(device1);
        devices.add(device2);
        devices.add(device3);
        TransfersGraph graph = new TransfersGraph(devices);

        // Add edges (transfers) to the graph to form a cycle
        ComponentId component1 = new ComponentId(101);
        ComponentId component2 = new ComponentId(102);
        ComponentId component3 = new ComponentId(103);
        ComponentTransfer transfer1 = new ComponentTransferImplementation(component1, device1, device2);
        ComponentTransfer transfer2 = new ComponentTransferImplementation(component2, device2, device3);
        ComponentTransfer transfer3 = new ComponentTransferImplementation(component3, device3, device1);
        graph.addEdge(transfer1);
        graph.addEdge(transfer2);
        graph.addEdge(transfer3);

        // Call the cycleOfTransfers() method with a transfer that is part of the cycle
        LinkedList<ComponentTransfer> cycle = graph.cycleOfTransfers(transfer1);

        // Assert that the returned list contains the correct sequence of transfers that form the cycle
        assertEquals(3, cycle.size());
        assertTrue(cycle.contains(transfer1));
        assertTrue(cycle.contains(transfer2));
        assertTrue(cycle.contains(transfer3));
    }
*/


    @Test
    public void testDeviceTakenSlotsInitialization() throws NoSuchFieldException, IllegalAccessException {
        HashMap<DeviceId, Integer> deviceTotalSlots = new HashMap<>();
        HashMap<ComponentId, DeviceId> componentPlacement = new HashMap<>();

        // Fill the maps with some test data
        DeviceId device1 = new DeviceId(1);
        DeviceId device2 = new DeviceId(2);
        ComponentId component1 = new ComponentId(101);
        ComponentId component2 = new ComponentId(102);
        ComponentId component3 = new ComponentId(103);

        deviceTotalSlots.put(device1, 2);
        deviceTotalSlots.put(device2, 1);

        componentPlacement.put(component1, device1);
        componentPlacement.put(component2, device1);
        componentPlacement.put(component3, device2);

        // Create an instance of StorageSystemImplementation
        StorageSystemImplementation system = new StorageSystemImplementation(deviceTotalSlots, componentPlacement);

        // Use reflection to access the private deviceTakenSlots field
        Field deviceTakenSlotsField = StorageSystemImplementation.class.getDeclaredField("deviceTakenSlots");
        deviceTakenSlotsField.setAccessible(true);
        HashMap<DeviceId, Integer> deviceTakenSlots = (HashMap<DeviceId, Integer>) deviceTakenSlotsField.get(system);

        // Assert that the deviceTakenSlots map has the same size as the deviceTotalSlots map
        assertEquals(deviceTotalSlots.size(), deviceTakenSlots.size());

        // For each entry in the deviceTakenSlots map, assert that the value (number of slots taken) is correct
        assertEquals(2, deviceTakenSlots.get(device1));
        assertEquals(1, deviceTakenSlots.get(device2));
    }
}