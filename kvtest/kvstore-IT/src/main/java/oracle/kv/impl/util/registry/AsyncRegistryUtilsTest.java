/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.util.registry.AsyncRegistryUtils.describeDialogTypeNumber;
import static oracle.kv.impl.util.registry.AsyncRegistryUtils.getDialogTypeNumber;
import static oracle.kv.impl.util.registry.AsyncRegistryUtils.getServiceTypeNumber;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.function.IntSupplier;

import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.ServiceResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for the AsyncRegistryUtils class. */
public class AsyncRegistryUtilsTest extends UncaughtExceptionTestBase {

    private static class MyDialogTypeFamily implements DialogTypeFamily {
        private final int familyId;
        MyDialogTypeFamily(int familyId) {
            this.familyId = familyId;
        }
        @Override
        public int getFamilyId() { return familyId; }
        @Override
        public String getFamilyName() { return "MyDialogTypeFamily"; }
    }

    private static DialogTypeFamily myTypeFamily = new MyDialogTypeFamily(77);
    static {
        DialogType.registerTypeFamily(myTypeFamily);
    }

    private KVRepTestConfig config;

    @BeforeClass
    public static void setUpStatic() {
        assumeTrue("AsyncRegistry tests require async",
                   AsyncControl.serverUseAsync);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (config != null) {
            config.stopRHs();
        }
        LoggerUtils.closeAllHandlers();
        AsyncRegistryUtils.resetDialogTypeRegistryPortValues();
        super.tearDown();
    }

    /* Tests */

    /**
     * Test that getRequestHandler returns null if the timeout is 0 or
     * negative. [KVSTORE-420]
     */
    @Test
    public void testGetRequestHandler() throws Exception {
        config = new KVRepTestConfig(this, 1, 1, 1, 10);
        config.startupRHs();
        final Topology topo = config.getTopology();
        final RepNodeId rnId = new RepNodeId(1, 1);

        assertEquals(null,
                     AsyncRegistryUtils.getRequestHandler(topo, rnId, null,
                                                          -3, logger)
                     .get(10_000, MILLISECONDS));

        assertEquals(null,
                     AsyncRegistryUtils.getRequestHandler(topo, rnId, null,
                                                          0, logger)
                     .get(10_000, MILLISECONDS));

        assertNotNull(AsyncRegistryUtils.getRequestHandler(topo, rnId, null,
                                                           10_000, logger)
                      .get(10_000, MILLISECONDS));
    }

    @Test
    public void testGetDialogTypeNumber() {
        AsyncRegistryUtils.resetDialogTypeRegistryPortValues();

        assertEquals(0, getDialogTypeNumber(sn(0), 5000));
        assertEquals(10, getDialogTypeNumber(sn(1), 5000));
        assertEquals(50, getDialogTypeNumber(sn(5), 5000));
        assertEquals(9990, getDialogTypeNumber(sn(999), 5000));
        assertEqualsOverflow(50, () -> getDialogTypeNumber(sn(1005), 5000));
        assertEquals(500_070, getDialogTypeNumber(sn(7), 6000));

        assertEquals(12, getDialogTypeNumber(admin(1), 5000));
        assertEquals(52, getDialogTypeNumber(admin(5), 5000));
        assertEquals(9992, getDialogTypeNumber(admin(999), 5000));
        assertEqualsOverflow(52,
                             () -> getDialogTypeNumber(admin(1005), 5000));
        assertEquals(500_072, getDialogTypeNumber(admin(7), 6000));

        assertEquals(521, getDialogTypeNumber(rn(1, 2), 5000));
        assertEquals(531, getDialogTypeNumber(rn(1, 3), 5000));
        assertEquals(591, getDialogTypeNumber(rn(1, 9), 5000));
        assertEqualsOverflow(571, () -> getDialogTypeNumber(rn(1, 307), 5000));
        assertEquals(4041, getDialogTypeNumber(rn(8, 4), 5000));
        assertEquals(499_541, getDialogTypeNumber(rn(999, 4), 5000));
        assertEqualsOverflow(1041,
                             () -> getDialogTypeNumber(rn(1_002, 4), 5000));
        assertEquals(1_006_541, getDialogTypeNumber(rn(13, 4), 7000));

        assertEquals(523, getDialogTypeNumber(arb(1, 2), 5000));
        assertEquals(533, getDialogTypeNumber(arb(1, 3), 5000));
        assertEquals(593, getDialogTypeNumber(arb(1, 9), 5000));
        assertEqualsOverflow(573,
                             () -> getDialogTypeNumber(arb(1, 307), 5000));
        assertEquals(4043, getDialogTypeNumber(arb(8, 4), 5000));
        assertEquals(499_543, getDialogTypeNumber(arb(999, 4), 5000));
        assertEqualsOverflow(1043,
                             () -> getDialogTypeNumber(arb(1_002, 4), 5000));
        assertEquals(506_543, getDialogTypeNumber(arb(13, 4), 6000));

        for (int i = 0; i < 40; i++) {
            assertEquals((i * 500_000) + 20,
                         getDialogTypeNumber(sn(2), 5000 + (i * 1000)));
        }
        for (int i = 40; i < 60; i++) {
            final int j = i;
            assertEqualsOverflow(
                ((j - 40) * 500_000) + 20,
                () -> getDialogTypeNumber(sn(2), 5000 + (j * 1000)));
        }

        for (int i = 0; i < 40; i++) {
            assertEquals((i * 500_000) + 521,
                         getDialogTypeNumber(rn(1, 2), 5000 + (i * 1000)));
        }
        for (int i = 40; i < 60; i++) {
            final int j = i;
            assertEqualsOverflow(
                ((j - 40) * 500_000) + 521,
                () -> getDialogTypeNumber(rn(1, 2), 5000 + (j * 1000)));
        }

        checkException(() -> getDialogTypeNumber(sn(-1), 5000),
                       IllegalArgumentException.class,
                       "Node number is negative");
        checkException(() -> getDialogTypeNumber(rn(1, -3), 5000),
                       IllegalArgumentException.class,
                       "Node number is negative");
        checkException(() -> getDialogTypeNumber(rn(-1, 3), 5000),
                       IllegalArgumentException.class,
                       "Group ID is negative");
    }

    @Test
    public void testDescribeDialogTypeNumber() {

        /* SN */
        int node = 0;
        for (final DialogTypeFamily family : new DialogTypeFamily[] {
                StandardDialogTypeFamily.SERVICE_REGISTRY_TYPE_FAMILY,
                StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY,
                StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY,
                myTypeFamily
            }) {
            testDescribeDialogTypeNumber(sn(node++), family);
        }

        /* Admin */
        node = 1;
        for (final DialogTypeFamily family : new DialogTypeFamily[] {
                StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY,
                StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY,
                StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.COMMAND_SERVICE_TYPE_FAMILY,
                StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                myTypeFamily
            }) {
            testDescribeDialogTypeNumber(admin(node++), family);
        }

        /* RN */
        int group = 1;
        node = 1;
        for (final DialogTypeFamily family : new DialogTypeFamily[] {
                StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY,
                StandardDialogTypeFamily.REP_NODE_ADMIN_TYPE_FAMILY,
                StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY,
                StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                myTypeFamily
            }) {
            testDescribeDialogTypeNumber(rn(group++, node++), family);
        }

        /* Arbiter */
        group = 1;
        node = 1;
        for (final DialogTypeFamily family : new DialogTypeFamily[] {
                StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.ARB_NODE_ADMIN_TYPE_FAMILY,
                StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY,
                StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY,
                StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                myTypeFamily
            }) {
            testDescribeDialogTypeNumber(arb(group++, node++), family);
        }
    }

    private void testDescribeDialogTypeNumber(ServiceResourceId serviceId,
                                              DialogTypeFamily typeFamily) {
        testDescribeDialogTypeNumber(0, serviceId, typeFamily);
        testDescribeDialogTypeNumber(1, serviceId, typeFamily);
    }

    private void testDescribeDialogTypeNumber(int port,
                                              ServiceResourceId serviceId,
                                              DialogTypeFamily typeFamily) {
        final ResourceType serviceType = serviceId.getType();
        final int serviceIdNum = getServiceTypeNumber(serviceId);
        final int dialogFamilyNum = typeFamily.getFamilyId();
        final DialogTypeFamily dialogFamily =
            StandardDialogTypeFamily.getOrNull(dialogFamilyNum);
        assertEquals(
            "dialogTypeNumber[" +
            "port=" + port +
            " group=" + serviceId.getGroupId() +
            " node=" + serviceId.getNodeNum() +
            " service=" + serviceType + "(" + serviceIdNum + ")" +
            " family=" +
            ((dialogFamily != null) ? dialogFamily : dialogFamilyNum) +
            "]",
            describeDialogTypeNumber(
                new DialogType(
                    typeFamily, getDialogTypeNumber(serviceId, port))
                .getDialogTypeId()));
    }

    /* Other methods */

    private static StorageNodeId sn(int id) {
        return new StorageNodeId(id);
    }

    private static AdminId admin(int id) {
        return new AdminId(id);
    }

    private static RepNodeId rn(int group, int node) {
        return new RepNodeId(group, node);
    }

    private static ArbNodeId arb(int group, int node) {
        return new ArbNodeId(group, node);
    }

    private static void assertEqualsOverflow(int expected,
                                             IntSupplier supplier) {
        assertTrue(TestStatus.isActive());
        checkException(() -> supplier.getAsInt(), AssertionError.class);
        try {
            TestStatus.setActive(false);
            assertEquals(expected, supplier.getAsInt());
        } finally {
            TestStatus.setActive(true);
        }
    }
}
