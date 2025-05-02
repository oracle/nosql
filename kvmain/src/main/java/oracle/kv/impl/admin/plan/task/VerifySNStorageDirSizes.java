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

package oracle.kv.impl.admin.plan.task;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.diagnostic.ParametersValidator;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.util.ErrorMessage;

/**
 * This task checks for the combined storage directory sizes of the SN with
 * the total disk capacity.
 * The task will fail if the combined storage directory sizes is greater
 * than the disk capacity. The task will also fail if the plan is created but
 * not executed and some changes happened in the storage directory sizes in
 * the meantime. So when the plan is executed, it consisted of the stale
 * storage directory map as hence the task will fail.
 * If this task fails, all the other tasks in the plan will not be executed.
 */

public class VerifySNStorageDirSizes extends SingleJobTask{

    private static final long serialVersionUID = 1L;

    private final ParameterMap newMap;
    private final AbstractPlan plan;

    public VerifySNStorageDirSizes(AbstractPlan plan, ParameterMap newMap) {
        this.newMap = newMap;
        this.plan = plan;
    }

    @Override
    public State doWork() throws Exception {
        /*
         * The parameters passed when creating an ChangeSNParamsPlan will just
         * be the mount point parameter when changing storage directory sizes.
         * So the value of newParams is same sort of thing as is returned
         * by calling StorageNodeParams.getStorageDirMap.
         */
        ParameterMap storageDirMap = newMap.copy();
        storageDirMap.remove("storageNodeId");
        String msg = ParametersValidator.
            checkAllStorageDirectoriesSizeOnDisk(storageDirMap);
        if (msg != null) {
            throw new CommandFaultException(msg, ErrorMessage.NOSQL_5200,
                CommandResult.NO_CLEANUP_JOBS);
        }
        return State.SUCCEEDED;
    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
