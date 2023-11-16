/*
 * University of Warsaw
 * Concurrent Programming Course 2023/2024
 * Java Assignment
 *
 * Authors: Karol Szwed (ks430171@students.mimuw.edu.pl)
 */
package cp2023.tests;

import cp2023.base.*;

/*
 * Basic implementation of the ComponentTransfer interface, only to be used in tests.
 */
public class ComponentTransferImplementation implements ComponentTransfer {

    private final ComponentId componentId;
    private final DeviceId sourceDeviceId;
    private final DeviceId destinationDeviceId;

    public ComponentTransferImplementation(ComponentId componentId, DeviceId sourceDeviceId, DeviceId destinationDeviceId) {
        this.componentId = componentId;
        this.sourceDeviceId = sourceDeviceId;
        this.destinationDeviceId = destinationDeviceId;
    }

    @Override
    public ComponentId getComponentId() {
        return componentId;
    }

    @Override
    public DeviceId getSourceDeviceId() {
        return sourceDeviceId;
    }

    @Override
    public DeviceId getDestinationDeviceId() {
        return destinationDeviceId;
    }

    @Override
    public void prepare() {

    }

    @Override
    public void perform() {

    }
}
