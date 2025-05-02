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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.streamservice.MRT.Response;

/**
 * Task to wait for a response message from a multi-region service. This task
 * will wait indefinitely for a SUCCESS or ERROR response.
 */
public class WaitForMRT extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private static final DurationParameter POLL_PERIOD =
            new DurationParameter("POLL_PERIOD", TimeUnit.SECONDS, 1L);

    private final MetadataPlan<TableMetadata> plan;

    public WaitForMRT(MetadataPlan<TableMetadata> plan) {
        this.plan = plan;
    }

    @Override
    protected MetadataPlan<TableMetadata> getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public Callable<State> getFirstJob(int taskId, TaskRunner runner) {
        return makeWaitForResponseJob(taskId, runner);
    }

    private JobWrapper makeWaitForResponseJob(int taskId, TaskRunner runner) {
        return new JobWrapper(taskId, runner, "wait for multi-region service") {
            @Override
            public NextJob doJob() {
                return waitForResponse(taskId, runner);
            }
        };
    }

    private NextJob waitForResponse(int taskId, TaskRunner runner) {
        final Response response = plan.getAdmin()
                                      .getMRTServiceManager()
                                      .getResponse(plan.getId());
        if (response == null || !response.isComplete()) {
            /* response not available or incomplete, wait */
            plan.getRateLimitingLogger()
                .log(getName(), Level.INFO,
                     () -> this + " no response or response incomplete, " +
                           "retry in seconds=" + POLL_PERIOD.asString());
            return new NextJob(Task.State.RUNNING,
                               makeWaitForResponseJob(taskId, runner),
                               POLL_PERIOD);
        }

        if (response.isCompleteSucc()) {
            /* response is complete and all responses are success */
            return NextJob.END_WITH_SUCCESS;
        }

        /* response is complete but with error response */
        return new NextJob(Task.State.ERROR, null, null, response.toString());
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" for response to ")
                    .append(plan.getId());
    }

    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }
}
