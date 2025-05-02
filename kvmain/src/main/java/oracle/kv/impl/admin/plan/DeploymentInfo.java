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

package oracle.kv.impl.admin.plan;

import oracle.kv.impl.admin.topo.TopologyCandidate;

/**
 * A struct to package together information that goes into the RealizedTopology.
 * A DeploymentInfo instance is meant to be a transient field in the plan that
 * deploys a topology. The instance is valid only during the time when the plan
 * is actively executing, and is initialized at the beginning of a plan 
 * execution run.
 */
public class DeploymentInfo {

    private long deployStartTime;

    private final String planName;
    private final int planId;
    private final String candidateName;

    /**
     * The preferred way to obtain a DeploymentInfo, rather than using the
     * constructor, to ensure that the DeployInfo's start time is properly 
     * validated. The start time is used as a key for topology storage, and 
     * must be unique and ascending.
     */
    public static DeploymentInfo makeDeploymentInfo(AbstractPlan plan,
                                                    String candidateName) {
        DeploymentInfo info = new DeploymentInfo(plan.getName(),
                                                 plan.getId(),
                                                 candidateName);
        long validatedTime =  plan.getAdmin().validateStartTime
            (info.getDeployStartTime());
        info.setDeployStartTime(validatedTime);
        return info;
    }

    /** 
     * Used when a plan is not available, either because we're starting up
     * the Admin service, or it's a unit test situation.
     */
    public static DeploymentInfo makeStartupDeploymentInfo() {
        return new DeploymentInfo(Plan.NO_NAME, 0, TopologyCandidate.NO_NAME);
    }

    /**
     * Used when setting up an initial empty topology. Care must be taken to
     * ensure that the deploy start time is an ascending, unique value, so this
     * constructor is private, and the caller should use the factory method
     * makeDeploymentInfo();
     */
    private DeploymentInfo(String planName, int planId, String candidateName) {
        this.planName = planName;
        this.planId = planId;
        this.candidateName = candidateName;
        deployStartTime = System.currentTimeMillis();
    }

    public long getDeployStartTime() {
        return deployStartTime;
    }

    public void setDeployStartTime(long validatedTime) {
        deployStartTime = validatedTime;
    }
    
    public String getPlanName() {
        return planName;
    }

    public int getPlanId() {
        return planId;
    }

    public String getCandidateName() {
        return candidateName;
    }
}