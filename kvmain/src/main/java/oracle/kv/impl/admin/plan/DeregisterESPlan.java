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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.UpdateESConnectionInfo;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.util.ErrorMessage;

/**
 * A plan for informing the store of the existence of an Elasticsearch node.
  */
public class DeregisterESPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    public DeregisterESPlan(String name, Planner planner) {
        super(name, planner);

        final Admin admin = getAdmin();
        final Parameters p = admin.getCurrentParameters();

        verifyNoTextIndexes(admin);

        /*
         *  update the ES connection info list on each SNA.
         */
        int taskCount = 0;
        for (StorageNodeParams snaParams : p.getStorageNodeParams()) {
            if (! ("" == snaParams.getSearchClusterName())) {
                taskCount++;
                addTask(new UpdateESConnectionInfo
                        (this, snaParams.getStorageNodeId(), "", "",false));
            }
        }
        if (taskCount == 0) {
            throw new IllegalCommandException
                ("No ES cluster is currently registered with the store.");
        }
    }

    private void verifyNoTextIndexes(Admin admin) {
        final TableMetadata tableMd = admin.getMetadata(TableMetadata.class,
                                                        MetadataType.TABLE);
        if (tableMd == null) {
            return;
        }

        final Set<String> indexnames = tableMd.getTextIndexNames();
        if (indexnames.isEmpty()) {
            return;
        }

        final StringBuilder sb = new StringBuilder
            ("Cannot deregister ES because these text indexes exist:");
        String eol = System.getProperty("line.separator");
        for (String s : indexnames) {
            sb.append(eol);
            sb.append(s);
        }

        throw new IllegalCommandException(sb.toString());
    }

    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        final Admin admin = getAdmin();
        verifyNoTextIndexes(admin);
        admin.getCurrentParameters().getStorageNodeParams();
        Set<StorageNodeId> snSet = new HashSet<StorageNodeId>();
        for (StorageNodeParams snp :
             admin.getCurrentParameters().getStorageNodeParams()) {
            snSet.add(snp.getStorageNodeId());
        }

        for (Task t : this.getTaskList().getTasks()) {
           if (t instanceof UpdateESConnectionInfo) {
               UpdateESConnectionInfo ueci = (UpdateESConnectionInfo)t;
               if (!snSet.contains(ueci.getSNId())) {
                   throw new IllegalCommandException
                   (this + " was created based on a different set of " +
                   "Storage Nodes that is now currently defined. " +
                    "Please cancel this plan and create a new plan. ",
                    ErrorMessage.NOSQL_5400, CommandResult.PLAN_CANCEL);
               }
           }
        }
    }

    @Override
    public String getDefaultName() {
        return "Deregister Elasticsearch cluster";
    }

    @Override
	public void stripForDisplay() {
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
