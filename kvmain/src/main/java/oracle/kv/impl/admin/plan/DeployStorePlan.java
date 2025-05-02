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

import java.util.Set;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;

/**
 * A plan for deploying a new store.
 * Deprecated as of R2, replaced by DeployTopoPlan
 */
public class DeployStorePlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /**
     * The initial arguments for the plan are preserved in order to display
     * them later.
     */
    private StorageNodePool targetPool;
    private int repFactor;
    private int numPartitions;

    /*
     * RepNodeParams created by the plan for deployment, saved before plan
     * execution.
     */
    @SuppressWarnings("unused")
    private Set<RepNodeParams> firstDeploymentRNPs;

    DeployStorePlan() {
        super(null, null, null);
    }

    @Override
    public String getDefaultName() {
        return null;
    }

    public StorageNodePool getTargetPool() {
        return targetPool;
    }

    public int getRepFactor() {
        return repFactor;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
