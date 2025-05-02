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

package oracle.kv.impl.admin.topo;

import java.io.Serializable;

import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TopologyPrinter;

/**
 * A RealizedTopology is created as a byproduct when a topology changing plan
 * executes. It's a snapshot of the changes created by the plan.
 */
public class RealizedTopology implements Serializable {

    private static final long serialVersionUID = 1;

    private Long deployStartMillis;

    /**
     * Some, but not all RealizedTopologies are created by plans that take a
     * TopologyCandidate as a parameter. If one has been provided to the plan,
     * it's saved here as part of the audit trail.
     */
    private final String candidateName;
    private final String planName;
    private final int planId;
    private Topology topology;

    /**
     * The very first initial empty topology
     */
    public RealizedTopology(String storeName) {
        topology = new Topology(storeName);
        candidateName = TopologyCandidate.NO_NAME;
        planName = Plan.NO_NAME;
        planId = 0;
        setStartTime();
    }

    public RealizedTopology(Topology topo, DeploymentInfo info) {
        deployStartMillis = info.getDeployStartTime();
        topology = topo;
        this.planName = info.getPlanName();
        this.planId = info.getPlanId();
        this.candidateName = info.getCandidateName();
    }

    /**
     * TODO: since the start time is the primary key, we need to guarantee
     * that new realizedTopologies always ascend. If there is some clock skew
     * between nodes in the Admin replication group, there is the possibility
     * that the start millis is {@literal <=} to the last saved
     * RealizedTopology.
     */
    public void setStartTime() {
        deployStartMillis = System.currentTimeMillis();
    }

    public long getStartTime() {
        return deployStartMillis;
    }

    public Topology getTopology() {
       return topology;
    }

    public void setTopology(Topology topo) {
       topology = topo;
    }

    /**
     * @param concise if true, do not display the whole topology.
     */
    public String display(boolean concise) {
        StringBuilder sb = new StringBuilder();
        sb.append("deployTime=");
        sb.append(FormatUtils.formatDateTimeMillis(deployStartMillis));
        sb.append(" plan=").append(planId).append("/").append(planName);
        sb.append(" candidate=").append(candidateName);
        if (!concise) {
            sb.append('\n');
            sb.append(TopologyPrinter.printTopology(topology));
        }
        return sb.toString();
    }
}
