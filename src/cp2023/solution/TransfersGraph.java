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
    private class DeviceNode {
        private final DeviceId device;
        private HashMap<ComponentTransfer, DeviceNode> outgoingEdges;

        public DeviceNode(DeviceId device) {
            this.device = device;
            this.outgoingEdges = new HashMap<>();
        }

        public void addEdge(ComponentTransfer transfer, DeviceNode device) {
            assert transfer.getSourceDeviceId() == this.device; // Transfer must be from this device.
            assert transfer.getDestinationDeviceId() != null; // Transfer must be MOVE.
            outgoingEdges.put(transfer, device);
        }

        public HashMap<ComponentTransfer, DeviceNode> getOutgoingEdges() {
            return outgoingEdges;
        }

        public DeviceId getDevice() {
            return device;
        }
    }

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

}
