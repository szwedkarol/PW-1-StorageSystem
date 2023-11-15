package cp2023.solution;

import org.junit.Test;
import java.lang.reflect.Field;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import cp2023.base.*;

public class StorageSystemImplementationTest {

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