/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.fail;

import java.util.Map;
import java.util.stream.Collectors;

import oracle.kv.impl.util.registry.AsyncRegistryUtils;

public class AsyncTestUtils {

    /**
     * Check that there are no active dialog types. Use this check in tests to
     * make sure that all servers have been unexported.
     */
    public static void checkActiveDialogTypes() {
        final Map<Integer, String> active = getActiveDialogTypes();
        if (!active.isEmpty()) {
            /*
             * Print out the failure message since it might not be visible
             * otherwise if subsequent failures because of the active servers
             * causes the test JVM to exit
             */
            System.err.println("Expected no active dialog types, found: " +
                               active);
            fail("Expected no active dialog types, found: " + active);
        }
    }

    /**
     * Returns a map from active dialog type numbers to descriptions of
     * currently active dialog type. Returns an empty map if no dialog types
     * are currently active.
     */
    public static Map<Integer, String> getActiveDialogTypes() {
        return ((AbstractEndpointGroup)
                AsyncRegistryUtils.getEndpointGroup())
            .getListeners().values().stream()
            .map(listener -> listener.getDialogHandlerFactoryMap())
            .filter(map -> map.hasActiveDialogTypes())
            .flatMap(map -> map.getActiveDialogTypes().stream())
            .collect(
                Collectors.toMap(
                    t -> t, AsyncRegistryUtils::describeDialogTypeNumber));
    }
}
