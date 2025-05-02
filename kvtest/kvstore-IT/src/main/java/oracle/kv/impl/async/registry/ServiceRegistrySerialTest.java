/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.registry;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.util.SerialTestUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;

import org.junit.Test;

/** Test serial version compatibility */
public class ServiceRegistrySerialTest extends TestBase {

    @Test
    public void testSerialVersion() {
        final ServiceEndpoint serviceEndpoint =
            new ServiceEndpoint(
                new InetNetworkAddress("host", 1000),
                StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE,
                new ClearClientSocketFactory("name", 1, 2, null, 3));
        checkAll(
            Stream.of(
                serialVersionChecker(
                    new ServiceRegistry.GetSerialVersionCall(),
                    0xda39a3ee5e6b4b0dL),
                serialVersionChecker(
                    new ServiceRegistry.LookupCall("name", null),
                    0x8cda18f15360c4e9L),
                serialVersionChecker(
                    new ServiceRegistry.BindCall("name", serviceEndpoint),
                    SerialVersion.MINIMUM, 0xad4aa06b8b0e690dL),
                serialVersionChecker(
                    new ServiceRegistry.UnbindCall("name"),
                    0x8cda18f15360c4e9L),
                serialVersionChecker(
                    new ServiceRegistry.ListCall(),
                    0xda39a3ee5e6b4b0dL))
            .map(svc ->
                 svc.equalsChecker(SerialTestUtils::equalClasses)));
    }
}
