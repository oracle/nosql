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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.SnaLoggingStatsTracker;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ServiceStatusTracker;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * A No-op version of the MgmtAgent.
 */
public class NoOpAgent implements MgmtAgent {

    @SuppressWarnings("unused")
    public NoOpAgent(StorageNodeAgent sna,
                    int pollingPort,
                    String trapHostName,
                    int trapPort,
                    ServiceStatusTracker tracker) {
    }

    @Override
    public void setSnaStatusTracker(ServiceStatusTracker snaStatusTracker) {
    }

    @Override
    public void setSnaLoggingStatsTracker(SnaLoggingStatsTracker tracker) { }

    @Override
    public void shutdown() {
    }

    @Override
    public void proxiedStatusChange(ProxiedServiceStatusChange sc) {
    }

    @Override
    public void addRepNode(RepNodeParams rnp, ServiceManager mgr) {
    }

    @Override
    public void removeRepNode(RepNodeId rnid) {
    }

    @Override
    public void addArbNode(ArbNodeParams arp, ServiceManager mgr) {
    }

    @Override
    public void removeArbNode(ArbNodeId arbid) {
    }

    @Override
    public boolean checkParametersEqual(int pollp, String traph, int trapp) {
        return true;
    }

    @Override
    public void addAdmin(AdminParams ap, ServiceManager mgr) throws Exception {
    }

    @Override
    public void removeAdmin() {
    }

    @Override
    public void updateNodeStatus(ResourceId si, ServiceStatusChange ns) { }

    @Override
    public void updateAdminStatus(ServiceStatusChange ns, boolean im) { }

    @Override
    public void receiveStats(ResourceId si, StatsPacket p) { }

    @Override
    public void receiveAdminStats(StatsPacket p) { }

    @Override
    public void receiveNewParams(ResourceId si, ParameterMap nm) { }

    @Override
    public void updatePlanStatus(String ps) { }

    @Override
    public void updateReplicationState(RepNodeId ri, StateChangeEvent ce) { }
}
