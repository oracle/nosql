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

import java.util.concurrent.Callable;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.VerifyDataPlanV2;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;

public abstract class VerifyData<T extends ResourceId> extends AbstractTask {
    private static final long serialVersionUID = 1L;

    protected VerifyDataPlanV2 plan;
    protected T targetId;
    protected VerificationOptions verifyOptions;

    protected int retryTimes;

    protected boolean started;

    private VerificationInfo verifyResult;

    protected static final int LOST_CONNECTION_RETRY = 10;

    public VerifyData(VerifyDataPlanV2 plan,
                      T targetId,
                      VerificationOptions verifyOptions) {
        super();
        this.plan = plan;

        this.targetId = targetId;

        this.verifyOptions = verifyOptions;
        retryTimes = 0;
        this.started = false;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

    @Override
    public Callable<State> getFirstJob(int taskId,
                                       TaskRunner runner)
        throws Exception {
        return makeStartPollVerifyJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke a verify job.
     */
    protected JobWrapper
        makeStartPollVerifyJob(final int taskId,
                               final TaskRunner runner) {

        return new JobWrapper(taskId, runner, "verify data") {

            @Override
            public NextJob doJob() {
                return startAndPollVerify(taskId, runner);
            }
        };
    }

    protected abstract NextJob startAndPollVerify(int taskId,
                                                  TaskRunner runner);

    public VerificationInfo getVerifyResult() {
        return verifyResult;
    }

    protected boolean checkForRetry() {
        retryTimes++;

        if (retryTimes > LOST_CONNECTION_RETRY) {
            return false;
        }
        return true;
    }

    protected boolean checkForCompletion(VerificationInfo info) {
        if (info != null && info.getCurrentState().isDone()) {
            verifyResult = info;
            return true;
        }
        return false;
    }

    public T getNodeId() {
        return targetId;
    }

}
