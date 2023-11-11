/*
 * University of Warsaw
 * Concurrent Programming Course 2023/2024
 * Java Assignment
 *
 * Authors: Konrad Iwanicki (iwanicki@mimuw.edu.pl),
 *          Karol Szwed (ks430171@students.mimuw.edu.pl)
 */
package cp2023.solution;

import java.util.Map;

import cp2023.base.ComponentId;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;


public final class StorageSystemFactory {

    public static StorageSystem newSystem(
            Map<DeviceId, Integer> deviceTotalSlots,
            Map<ComponentId, DeviceId> componentPlacement) {

        // Tests to check if method arguments are correct.
        if (deviceTotalSlots == null || componentPlacement == null) {
            throw new IllegalArgumentException("Arguments cannot be null.");
        }

        // This also tests, if there are devices without number of slots provided.
        for (Map.Entry<DeviceId, Integer> entry : deviceTotalSlots.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("Map deviceTotalSlots cannot contain null values.");
            }
        }

        for (Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("Map componentPlacement cannot contain null values.");
            }
        }

        // Test to check, if the number of slots is less than or equal to zero.
        for (Map.Entry<DeviceId, Integer> entry : deviceTotalSlots.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException("Number of slots cannot be negative.");
            }
        }

        // Checks, if each component is placed on an existing device.
        for (Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()) {
            if (!deviceTotalSlots.containsKey(entry.getValue())) {
                throw new IllegalArgumentException("Component cannot be placed on a non-existing device.");
            }
        }

        // Checks, if the number of slots on device is not exceeded.
        for (Map.Entry<DeviceId, Integer> device : deviceTotalSlots.entrySet()) {
            int slots = 0;
            for (Map.Entry<ComponentId, DeviceId> component : componentPlacement.entrySet()) {
                if (component.getValue().equals(device.getKey())) {
                    slots++;
                }
            }
            if (slots > device.getValue()) {
                throw new IllegalArgumentException("Number of slots on device exceeded.");
            }
        }

        return new StorageSystemImplementation(
                new java.util.concurrent.ConcurrentHashMap<>(deviceTotalSlots),
                new java.util.concurrent.ConcurrentHashMap<>(componentPlacement)
        );
    }

}
