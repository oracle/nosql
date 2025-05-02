/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.async.StandardDialogTypeFamily.COMMAND_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;
import static oracle.kv.util.TestUtils.checkException;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.logging.Logger;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.junit.Test;

public class JavaSerialInitiatorProxyTest extends TestBase {

    @Test
    public void testEquals() throws Exception {
        final CreatorEndpoint endpoint =
            createMock(CreatorEndpoint.class);
        expect(endpoint.getRemoteAddress())
            .andReturn(new InetNetworkAddress("localhost", 5000));
        final DialogType csDialogType =
            new DialogType(COMMAND_SERVICE_TYPE_FAMILY, 1);
        final CommandService cs =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint, csDialogType, 10000, logger);
        assertEquals(cs, cs);
        assertEquals(cs.hashCode(), cs.hashCode());
        assertNotEquals(cs, null);
        assertNotEquals(cs, "foo");

        final CommandService cs2 =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint, csDialogType, 10000, logger);
        assertEquals(cs, cs2);
        assertEquals(cs.hashCode(), cs2.hashCode());

        /* Make sure equals detects each meaningful difference */

        final MonitorAgent ma =
            createProxy(MonitorAgent.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint, csDialogType, 10000, logger);
        assertNotEquals(cs, ma);

        checkException(() ->
                       createProxy(CommandService.class,
                                   MONITOR_AGENT_TYPE_FAMILY, endpoint,
                                   csDialogType, 10000, logger),
                       IllegalArgumentException.class,
                       "Dialog type");

        final CreatorEndpoint endpoint2 =
            createMock(CreatorEndpoint.class);
        expect(endpoint2.getRemoteAddress())
            .andReturn(new InetNetworkAddress("localhost", 5000));
        final CommandService csEndpointDiff =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint2, csDialogType, 10000, logger);
        assertNotEquals(cs, csEndpointDiff);

        final CreatorEndpoint endpointHost =
            createMock(CreatorEndpoint.class);
        expect(endpointHost.getRemoteAddress())
            .andReturn(new InetNetworkAddress("otherhost", 5000));
        final CommandService csEndpointHostDiff =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpointHost, csDialogType, 10000, logger);
        assertNotEquals(cs, csEndpointHostDiff);

        final CreatorEndpoint endpointPort =
            createMock(CreatorEndpoint.class);
        expect(endpointPort.getRemoteAddress())
            .andReturn(new InetNetworkAddress("localhost", 6000));
        final CommandService csEndpointPortDiff =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpointPort, csDialogType, 10000, logger);
        assertNotEquals(cs, csEndpointPortDiff);

        final DialogType csDialogType2 =
            new DialogType(COMMAND_SERVICE_TYPE_FAMILY, 2);
        final CommandService csDialogTypeDiff =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint, csDialogType2, 10000, logger);
        assertNotEquals(cs, csDialogTypeDiff);

        final DialogType maDialogType =
            new DialogType(MONITOR_AGENT_TYPE_FAMILY, 1);
        final CommandService csDialogTypeDiffMa =
            createProxy(CommandService.class, MONITOR_AGENT_TYPE_FAMILY,
                        endpoint, maDialogType, 10000, logger);
        assertNotEquals(cs, csDialogTypeDiffMa);

        final CommandService csTimeoutDiff =
            createProxy(CommandService.class, COMMAND_SERVICE_TYPE_FAMILY,
                        endpoint, csDialogType, 20000, logger);
        assertNotEquals(cs, csTimeoutDiff);
    }

    static <T extends VersionedRemote>
        T createProxy(Class<T> serviceInterface,
                      DialogTypeFamily dialogTypeFamily,
                      CreatorEndpoint endpoint,
                      DialogType dialogType,
                      long timeoutMs,
                      Logger logger) {
        return JavaSerialInitiatorProxy.createProxy(
            serviceInterface, dialogTypeFamily, endpoint,
            dialogType, timeoutMs, logger);
    }
}
