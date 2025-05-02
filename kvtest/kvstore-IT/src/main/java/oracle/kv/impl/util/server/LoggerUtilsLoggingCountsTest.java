/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.server;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import oracle.kv.TestBase;
import oracle.kv.impl.mgmt.jmx.JmxAgent;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils.LoggingCounts;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/** Test LoggerUtils.getLoggingStatsCounts */
public class LoggerUtilsLoggingCountsTest extends TestBase {
    private CreateStore createStore;
    private static final int startPort = 5000;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
        TestStatus.setActive(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Log to each service and make sure that getLoggingStatsCounts returns the
     * expected value.
     */
    @Test
    public void testGetLogStatsCounts() throws Exception {
        createStore = new CreateStore(kvstoreName, startPort,
                                      3, /* SNs */
                                      2, /* RF */
                                      100, /* partitions */
                                      2, /* capacity */
                                      MB_PER_SN * 2, /* memoryMB */
                                      true, /* useThreads */
                                      JmxAgent.class.getName());
        createStore.start();

        /* Collect information about all services */
        final List<LoggingInfo> loggingInfoList = new ArrayList<>();

        final Topology topo = createStore.getAdminMaster().getTopology();
        final StorageNodeId[] snIds = createStore.getStorageNodeIds();
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, createStore.getSNALoginManager(snIds[0]),
                              logger);

        for (int i = 0; i < 2; i++) {
            loggingInfoList.add(
                new LoggingInfo(createStore.getAdminId(i),
                                registryUtils.getAdminTest(snIds[i])));
        }

        for (final RepNodeId rnId : topo.getRepNodeIds()) {
            loggingInfoList.add(
                new LoggingInfo(rnId, registryUtils.getRepNodeTest(rnId)));
        }

        for (final ArbNodeId arbId : topo.getArbNodeIds()) {
            loggingInfoList.add(
                new LoggingInfo(arbId, registryUtils.getArbNodeTest(arbId)));
        }

        for (final StorageNodeId snId : snIds) {
            loggingInfoList.add(
                new LoggingInfo(snId,
                                registryUtils.getStorageNodeAgentTest(snId)));
        }

        /* Test logging to each service */
        for (final LoggingInfo info : loggingInfoList) {
            for (final Level level :
                     new Level[] { Level.SEVERE, Level.WARNING, Level.INFO }) {
                for (final boolean useJeLogging :
                         new boolean[] { true, false }) {

                    /* Only RNs and Admins currently support JE logging */
                    if (useJeLogging &&
                        !(info.resourceId instanceof RepNodeId ||
                          info.resourceId instanceof AdminId)) {
                        continue;
                    }
                    loggingInfoList.forEach(LoggingInfo::clearLoggingCounts);
                    final int count = (level == Level.INFO) ? 42 : 1;
                    for (int i = 0; i < count; i++) {
                        info.testAPI.logMessage(
                            level,
                            "Test logging " + level + " " + i,
                            useJeLogging);
                    }
                    loggingInfoList.forEach(
                        li -> li.checkLogging(info, level));
                }
            }
        }
    }

    /** Records information about logging for a particular resource */
    private class LoggingInfo {
        final ResourceId resourceId;
        final RemoteTestAPI testAPI;
        final LoggingCounts loggingCounts;
        LoggingInfo(ResourceId resourceId,
                    RemoteTestAPI testAPI) {
            this.resourceId = resourceId;
            this.testAPI = testAPI;
            loggingCounts = LoggerUtils.getLoggingStatsCounts(
                resourceId, kvstoreName);
        }

        /**
         * Checks for the expected logging counts given that logging was
         * performed on the target at the specified level.
         */
        void checkLogging(LoggingInfo target, Level level) {
            final boolean isTarget = equals(target);
            final boolean isSevereTarget = isTarget && (level == Level.SEVERE);
            assertEquals(this + " SEVERE" +
                         (isSevereTarget  ? " (target)" : ""),
                         isSevereTarget ? 1 : 0,
                         loggingCounts.getSevere().get());

            final boolean isWarningTarget =
                isTarget && (level == Level.WARNING);

            /*
             * There are occasional warnings for sn3 because that SN has no RNs
             * and there is a bug makes it generate warnings, so take that into
             * account.
             */
            final boolean extraWarnings =
                (resourceId instanceof StorageNodeId) &&
                (((StorageNodeId) resourceId).getStorageNodeId() == 3);
            if (!extraWarnings) {
                assertEquals(this + " WARNING" +
                             (isWarningTarget ? " (target)" : ""),
                             isWarningTarget ? 1 : 0,
                             loggingCounts.getWarning().get());
            } else if (isWarningTarget) {
                final long warningsCount = loggingCounts.getWarning().get();
                assertTrue(this + "WARNING (target): " + warningsCount,
                           warningsCount > 0);
            }

            final boolean isInfoTarget = isTarget && (level == Level.INFO);
            if (isInfoTarget) {
                final long infoCount = loggingCounts.getInfo().get();
                assertTrue(this + " INFO (target): " + infoCount,
                           infoCount >= 42);
            }
        }

        void clearLoggingCounts() {
            loggingCounts.getSevere().set(0);
            loggingCounts.getWarning().set(0);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof LoggingInfo)) {
                return false;
            }
            final LoggingInfo other = (LoggingInfo) obj;
            return resourceId.equals(other.resourceId);
        }

        @Override
        public int hashCode() {
            return resourceId.hashCode();
        }

        @Override
        public String toString() {
            return "[resourceId=" + resourceId + " " + loggingCounts + "]";
        }
    }
}
