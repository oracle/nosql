/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.mgmt;

import java.util.List;

import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.SnaLoggingStatsTracker;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * The Agent represents external values to the mgmt Agent implementations.
 */
public abstract class AgentInternal implements MgmtAgent {

    protected final StorageNodeAgent sna;
    private ServiceStatusTracker snaStatusTracker;
    private SnaLoggingStatsTracker snaLoggingStatsTracker;

    protected AgentInternal(StorageNodeAgent sna) {
        this.sna = sna;
    }

    @Override
    public void setSnaStatusTracker(ServiceStatusTracker tracker) {
        if (snaStatusTracker == tracker) {
            return;
        }
        snaStatusTracker = tracker;
        snaStatusTracker.addListener(new StatusListener());

        /* Set the starting status from the tracker's current status. */
        ServiceStatusChange s =
            new ServiceStatusChange(snaStatusTracker.getServiceStatus());
        updateSNStatus(s, s);
    }

    @Override
    public void setSnaLoggingStatsTracker(SnaLoggingStatsTracker tracker) {
        if (snaLoggingStatsTracker == tracker) {
            return;
        }
        snaLoggingStatsTracker = tracker;
        snaLoggingStatsTracker.addListener(this::updateStorageNodeStats);
    }

    @Override
    public void shutdown() {
        /*
         * If a notification was sent about the StorageNode being STOPPED, give
         * it a couple of seconds to escape before killing the mechanism that
         * sends it.
         */
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        }
    }

    protected abstract void updateSNStatus
        (ServiceStatusChange prev, ServiceStatusChange current);

    class StatusListener implements ServiceStatusTracker.Listener {
        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {

            AgentInternal.this.updateSNStatus(prevStatus, newStatus);
        }
    }

    protected abstract void updateStorageNodeStats(StatsPacket packet);

    @Override
    public void proxiedStatusChange(ProxiedServiceStatusChange sc) {
        /* First figure out what kind of a thing is being reported on. */
        ResourceId rid = sc.getTarget(sna.getStorageNodeId());
        if (rid instanceof RepNodeId) {
            RepNodeId rnid = (RepNodeId) rid;
            updateNodeStatus(rnid, sc);
        }
        if (rid instanceof AdminId) {
             /*
              * Proxied statuses are never "RUNNING" so it's safe to say
              * that "isMaster" is false.
              */
            updateAdminStatus(sc, false);
        }
    }

    /* Accessors for SNA parameters. */
    public int getSnId() {
        return sna.getStorageNodeId().getStorageNodeId();
    }

    public int getRegistryPort() {
        return sna.getRegistryPort();
    }

    public int getFreePort() {
        String rangeString = sna.getServicePortRange();
        final List<Integer> range = PortRange.getRange(rangeString);
        return RegistryUtils.findFreePort
            (range.get(0), range.get(1), getHostname());
    }

    public boolean restrictPortRange() {
        return (sna.getServicePortRange() != null);
    }

    public String getStoreName() {
        return nullToEmptyString(sna.getStoreName());
    }

    public String getHostname() {
        return nullToEmptyString(sna.getHostname());
    }

    public String getHAHostname() {
        return nullToEmptyString(sna.getHAHostname());
    }

    public String getBootstrapDir() {
        return nullToEmptyString(sna.getBootstrapDir());
    }

    public String getBootstrapFile() {
        return nullToEmptyString(sna.getBootstrapFile());
    }

    public String getKvConfigFile() {
        return nullToEmptyString(sna.getKvConfigFile().toString());
    }

    public boolean isHostingAdmin() {
        return sna.getBootstrapParams().isHostingAdmin();
    }

    public String getRootDir() {
        return nullToEmptyString(sna.getBootstrapParams().getRootdir());
    }

    public Integer getCapacity() {
        return sna.getCapacity();
    }

    public Integer getLogFileLimit() {
        return sna.getLogFileLimit();
    }

    public Integer getLogFileCount() {
        return sna.getLogFileCount();
    }

    public String getSnHaPortRange() {
        return nullToEmptyString(sna.getHAPortRange());
    }

    public int getNumCpus() {
        return sna.getNumCpus();
    }

    public int getMemoryMB() {
        return sna.getMemoryMB();
    }

    public long getCollectorInterval() {
        return sna.getCollectorInterval();
    }

    public String getMountPointsString() {
        return nullToEmptyString(sna.getMountPointsString());
    }

    public String getRNLogMountPointsString() {
        return nullToEmptyString(sna.getRNLogMountPointsString());
    }

    public String getAdminMountPointsString() {
        return nullToEmptyString(sna.getAdminMountPointsString());
    }

    /**
     * JDMK doesn't like null strings.
     */
    private static String nullToEmptyString(String s) {
        return s == null ? "" : s;
    }

}
