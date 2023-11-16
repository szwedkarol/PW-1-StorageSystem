package cp2023.solution;

import cp2023.base.*;

import java.util.HashMap;
import java.util.LinkedList;

public class TransfersGraph {

    /*
     * Node in a directed graph of transfers (TransferType.MOVE).
     * Nodes are devices and edges are transfers.
     *
     * Each node (device) contains a list of outgoing edges (transfers) that is transfers from this device to some other
     * device inside a graph.
     */
    public class DeviceNode {
        private final DeviceId device;
        private HashMap<ComponentTransfer, DeviceNode> outgoingEdges;

        public DeviceNode(DeviceId device) {
            assert device != null;
            this.device = device;
            this.outgoingEdges = new HashMap<>();
        }

        public void addEdge(ComponentTransfer transfer, DeviceNode destination) {
            assert transfer.getSourceDeviceId() == this.device; // Transfer must be from this device.
            assert transfer.getDestinationDeviceId() != null; // Transfer must be MOVE.

            outgoingEdges.put(transfer, destination);
        }

        public HashMap<ComponentTransfer, DeviceNode> getOutgoingEdges() {
            return outgoingEdges;
        }

        public DeviceId getDevice() {
            return device;
        }
    }

    // TODO: DeviceNode class should be private, but it is not possible to test it then.

    private final HashMap<DeviceId, DeviceNode> graph; // Graph of transfers.

    public TransfersGraph(LinkedList<DeviceId> devices) {
        this.graph = new HashMap<>();

        // Initialize graph with all devices.
        for (DeviceId device : devices) {
            graph.put(device, new DeviceNode(device));
        }
    }

    public void addEdge(ComponentTransfer transfer) {
        DeviceNode source = graph.get(transfer.getSourceDeviceId());
        DeviceNode destination = graph.get(transfer.getDestinationDeviceId());
        source.addEdge(transfer, destination);
    }

    public void removeEdge(ComponentTransfer transfer) {
        DeviceNode source = graph.get(transfer.getSourceDeviceId());
        source.getOutgoingEdges().remove(transfer);
    }

    // TODO: Likely to not be needed outside of testing.
    public DeviceNode getDeviceNode(DeviceId device) {
        return graph.get(device);
    }

    /*
     * Returns a list of transfers that form a cycle.
     *
     * INPUT: Transfer (MOVE) that is being executed.
     * FUNCTION: Checks if there is a cycle in the graph of transfers. Uses DFS algorithm.
     * OUTPUT: List of transfers that form a cycle. If there is no cycle, returns empty list.
     */
    public LinkedList<ComponentTransfer> cycleOfTransfers(ComponentTransfer transfer) {
        LinkedList<ComponentTransfer> cycleOfTransfers = new LinkedList<>();
        LinkedList<DeviceNode> cycleOfNodes = new LinkedList<>();
        DeviceNode startingNode = graph.get(transfer.getSourceDeviceId());

        if (dfs(startingNode, cycleOfNodes)) {
            // TODO: Implement creation of cycleOfTransfers list out of cycleOfNodes list.

            return cycleOfTransfers;
        } else {
            return new LinkedList<>();
        }
    }

    /*
     * Depth-first search algorithm.
     * RETURNS TRUE IF THERE IS A CYCLE, FALSE OTHERWISE.
     * SIDE EFFECT: if there is a cycle, "deviceNodesCycle" list is filled with nodes that form a cycle.
     */
    public boolean dfs(DeviceNode start, LinkedList<DeviceNode> deviceNodesCycle) {
        HashMap<DeviceNode, Boolean> visited = new HashMap<>();
        HashMap<DeviceNode, DeviceNode> parent = new HashMap<>();
        LinkedList<DeviceNode> stack = new LinkedList<>();

        stack.push(start);

        while (!stack.isEmpty()) {
            DeviceNode node = stack.pop();

            if (visited.getOrDefault(node, false)) {
                DeviceNode cycleStart = node;
                DeviceNode current = node;
                do {
                    deviceNodesCycle.addFirst(current);
                    current = parent.get(current);
                } while (!current.equals(cycleStart));
                return true;
            }

            visited.put(node, true);

            for (DeviceNode neighbor : node.getOutgoingEdges().values()) {
                if (!visited.getOrDefault(neighbor, false)) {
                    parent.put(neighbor, node);
                    stack.push(neighbor);
                } else if (neighbor == start) {
                    DeviceNode current = node;
                    do {
                        deviceNodesCycle.addFirst(current);
                        current = parent.get(current);
                    } while (!current.equals(start));
                    deviceNodesCycle.addFirst(start);
                    return true;
                }
            }
        }

        return false;
    }


    // TODO: CURRENTLY NOT WORKING. TEST IT ON GRAPH CONSISTING OF A SIMPLE CYCLE.
    /*
     * Depth-first search algorithm.
     * RETURNS TRUE IF THERE IS A CYCLE, FALSE OTHERWISE.
     * SIDE EFFECT: if there is a cycle, "deviceNodesCycle" list is filled with nodes that form a cycle.
     */
    /*public boolean dfs(DeviceNode start, LinkedList<DeviceNode> deviceNodesCycle) {
        HashMap<DeviceNode, Boolean> visited = new HashMap<>();
        LinkedList<DeviceNode> stack = new LinkedList<>();

        stack.push(start);

        while (!stack.isEmpty()) {
            DeviceNode node = stack.pop();

            if (visited.getOrDefault(node, false)) {
                LinkedList<DeviceNode> tempStack = new LinkedList<>(stack);
                while (!tempStack.peek().equals(node)) {
                    tempStack.pop();
                }
                deviceNodesCycle.addAll(tempStack);
                return true;
            }

            visited.put(node, true);

            for (DeviceNode neighbor : node.getOutgoingEdges().values()) {
                if (!visited.getOrDefault(neighbor, false)) {
                    stack.push(neighbor);
                }
            }
        }

        return false;
    }*/


    // BELOW ARE ATTEMPTS TO IMPLEMENT DFS ALGORITHM. THEY ARE NOT WORKING.

    /*
     * Version of depth-first search algorithm.
     *
     * INPUT:
     * - "node" - DeviceNode that is being visited,
     * - "visited" - HashMap of visited nodes,
     * - "stack" - stack of visited nodes,
     * - "cycle" - list of transfers
     * FUNCTION: TODO
     * OUTPUT: True if there is a cycle, false otherwise. Side effect: if there is a cycle,
     * "cycle" list is filled with transfers that form a cycle.
     */
    /*private boolean dfs(DeviceNode node, HashMap<DeviceNode, Boolean> visited, LinkedList<ComponentTransfer> stack,
                        LinkedList<ComponentTransfer> cycle) {
        visited.put(node, true);
        for (Map.Entry<ComponentTransfer, DeviceNode> outgoingEdge : node.getOutgoingEdges().entrySet()) {
            DeviceNode neighborDevice = outgoingEdge.getValue();
            ComponentTransfer transfer = outgoingEdge.getKey();

            if (!visited.getOrDefault(neighborDevice, false)) {
                stack.push(transfer);
                if (dfs(neighborDevice, visited, stack, cycle)) {
                    return true;
                }
                stack.pop();
            } else if (stack.contains(transfer)) {
                LinkedList<ComponentTransfer> tempStack = new LinkedList<>(stack);
                while (!tempStack.peek().equals(transfer)) {
                    tempStack.pop();
                }
                cycle.addAll(tempStack);
                return true;
            }
        }
        return false;
    }*/


    // Still not working, but closer to the solution.
    /*private boolean dfs(DeviceNode node, HashMap<DeviceNode, Boolean> visited, LinkedList<ComponentTransfer> stack,
                        LinkedList<ComponentTransfer> cycle) {
        visited.put(node, true);
        for (Map.Entry<ComponentTransfer, DeviceNode> outgoingEdge : node.getOutgoingEdges().entrySet()) {
            DeviceNode neighborDevice = outgoingEdge.getValue();
            ComponentTransfer transfer = outgoingEdge.getKey();

            if (!visited.getOrDefault(neighborDevice, false)) {
                stack.push(transfer);
                if (dfs(neighborDevice, visited, stack, cycle)) {
                    return true;
                }
                stack.pop();
            } else if (stack.contains(transfer)) {
                while (!stack.peek().equals(transfer)) {
                    stack.pop();
                }
                cycle.addAll(stack);
                return true;
            }
        }
        return false;
    }*/

    // DEBUGGING - WRONG (likely)
    /*private boolean dfs(DeviceNode node, HashMap<DeviceNode, Boolean> visited, LinkedList<DeviceNode> stack,
                        LinkedList<ComponentTransfer> cycle) {
        // Initialize visited map.
        for (DeviceNode device : graph.values()) {
            visited.put(device, false);
        }
        visited.put(node, true);

        for (Map.Entry<ComponentTransfer, DeviceNode> outgoingEdge : node.getOutgoingEdges().entrySet()) {
            DeviceNode neighborDevice = outgoingEdge.getValue();
            ComponentTransfer transfer = outgoingEdge.getKey();

            if (!visited.get(neighborDevice)) {
                stack.push(neighborDevice);
                if (dfs(outgoingEdge.getValue(), visited, stack, cycle)) {
                    return true;
                }
                stack.pop();
            } else if (stack.contains(node)) {
                cycle.add(transfer);
                return true;
            }
        }

        return false;
    }*/

}
