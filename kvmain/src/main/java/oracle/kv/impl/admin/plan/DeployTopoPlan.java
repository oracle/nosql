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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.HealthCheck;
import oracle.kv.impl.admin.plan.task.MigratePartition;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.ANWrongDC;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * Deploys a target topology.
 */
public class DeployTopoPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /*
     * Collection of shardIds generated during plan execution for brand new
     * shards.
     *
     * When tasks are generated to implement a candidate topology, the tasks
     * must refer to not-yet-created shards by a shard index. During task
     * execution, shards are created, and a RepGroupId is instantiated. Later
     * tasks that implement the RNs lookup the newly created shard ids by
     * index value.
     */
    private final List<RepGroupId> newlyGeneratedShardIds = new ArrayList<>();
    protected final String candidateName;

    /*
     * The topology sequence number identifying the current topology in effect
     * when the plan's tasks are generated.
     *
     * Plan tasks are generated when the user creates the plan. It's possible
     * that the plan may actually be executed later on. The generated tasks are
     * only valid if the current, deployed topology at execution time is
     * identical to the current topology at task generation time. This field is
     * used to check this condition.
     *
     * Once the plan as started, the seq # is updated during the plan execution
     * whenever the topology is updated. This allows the plan to be re-executed
     * even after parts of the plan have completed successfully.
     */
    private int sourceTopoSequence;

    private transient DeploymentInfo deploymentInfo;

    /**
     * Creates a plan to deploy a topology.
     *
     * @param planName a custom name for the plan, or null for the default
     * @param planner the planner to use to manage plans
     * @param current the current topology
     * @param candidate the new topology candidate to deploy
     * @param failedShard the failedShard parameter, if any, or null
     */
    public static DeployTopoPlan create(String planName,
                                        Planner planner,
                                        Topology current,
                                        TopologyCandidate candidate,
                                        RepGroupId failedShard) {
        final DeployTopoPlan plan =
            new DeployTopoPlan(planName, planner, current, candidate);
        plan.generateTasks(current, candidate, failedShard);
        return plan;
    }

    /** Internal constructor */
    protected DeployTopoPlan(String planName,
                             Planner planner,
                             Topology current,
                             TopologyCandidate candidate) {

        super(planName, planner);

        sourceTopoSequence = current.getSequenceNumber();
        candidateName = candidate.getName();
    }

    /**
     * Generate the tasks for this plan, throwing an exception
     * if the arguments used to create the plan are illegal.
     * Call this method after constructing the plan.
     *
     * @param current the current topology
     * @param candidate the topology to be deployed
     * @param failedShard the failedShard parameter, if any, or null
     * @throws IllegalCommandException if the plan is illegal
     */
    protected void generateTasks(Topology current,
                                 TopologyCandidate candidate,
                                 RepGroupId failedShard) {

        TopoTaskGenerator generator =
            new TopoTaskGenerator(this, current, candidate,
                                  planner.getAdmin().getParams(),
                                  getOfflineZones(), failedShard);
        generator.generate();
    }

    public boolean isNewShardCreated(int planShardIdx) {
        return newlyGeneratedShardIds.get(planShardIdx) != null;
    }

    public void setNewShardId(int planShardIdx, RepGroupId shardId) {
        newlyGeneratedShardIds.add(planShardIdx, shardId);
    }

    public RepGroupId getShardId(int planShardIdx) {
        if (newlyGeneratedShardIds.size() > planShardIdx) {
            return newlyGeneratedShardIds.get(planShardIdx);
        }

        return null;
    }

    /**
     * Returns a set containing the datacenter IDs of zones that are currently
     * offline.
     *
     * @return the IDs of zones that are offline
     */
    public Set<DatacenterId> getOfflineZones() {
        return Collections.emptySet();
    }


    @Override
    public String getDefaultName() {
        return "Deploy Topo";
    }

    @Override
    public DeploymentInfo getDeployedInfo() {
        return deploymentInfo;
    }

    @Override
    synchronized PlanRun startNewRun() {
        deploymentInfo = DeploymentInfo.makeDeploymentInfo(this,
                                                           candidateName);
        return super.startNewRun();
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    /**
     * Log information about the current and future topologies, and a preview
     * ofo the work to be done. Check if the target topology introduces new
     * violations. If it does, refuse to execute it unless the force flag is
     * set.
     * @param force if false, the plan will check if the proposed topology
     * introduces new topology violations. If it does, the plan will not
     * execute. If true, the plan will skip any violation checks.
     */
    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        Admin admin = planner.getAdmin();
        Topology current = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();
        TopologyCandidate candidate = admin.getCandidate(candidateName);
        Topology future = candidate.getTopology();

        /*
         * For ease of debugging, log information about the changes
         * this plan would execute. Always log this information, before doing
         * any validation, so that we have a consistent set of information in
         * the log
         */
        executeLogger.log(Level.INFO, "{0} deploying topology candidate {1}.",
                          new Object[]{this.toString(), candidateName});

        executeLogger.log(Level.INFO,
                          "Current topology: {0}",
                          TopologyPrinter.printTopology(current));

        /*
         * Don't do validation when diffing the topology. Validation will be
         * done later, within the context of the force flag.
         */
        TopologyDiff diff = new TopologyDiff(current, null /*sourceName*/,
                                             candidate, params,
                                             false /*validate*/);
        executeLogger.log(Level.INFO,
                          "Preview of changes to be executed by {0}:\n{1}",
                           new Object[]{this.toString(),
                                        diff.display(true /*verbose*/)});

        executeLogger.log(Level.INFO,
                          "Target topology candidate: {0}\n{1}",
                          new Object[]{candidate.getName(),
                                       TopologyPrinter.printTopology(future)});

        if (current.getSequenceNumber() != sourceTopoSequence) {
            throw new IllegalCommandException
                (this + " was based on the system topology at " +
                 "sequence " + sourceTopoSequence +
                 " but the current topology is at sequence " +
                 current.getSequenceNumber() +
                 ". Please cancel this plan and create a new plan with the " +
                 "command \"plan deploy-topology -name " + candidateName,
                 ErrorMessage.NOSQL_5400, CommandResult.PLAN_CANCEL);
        }

        if (force) {
            /* Don't bother checking */
            executeLogger.log(Level.INFO,
                              "-force specified for {0} so no topology " +
                              "validation will be done.", this.toString());
            return;
        }

        final Results existingState = Rules.validate(current, params, false);
        final int existingViolations = existingState.getViolations().size();

        final Results futureState = Rules.validate(future, params, false);
        final int futureViolations = futureState.getViolations().size();

        final Results newIssues = futureState.remove(existingState);
        final int newViolations = newIssues.getViolations().size();

        /* Require that violations be reduced if there are any */
        if ((futureViolations > 0) &&
            (futureViolations >= existingViolations)) {
            final String errorDesc = (newViolations == 0) ?
                ("leave " +
                 ((existingViolations == 1) ?
                  "an existing violation: " : "existing violations: ") +
                 existingState) :
                ("introduce " +
                 ((newViolations == 1) ?
                  "a new violation: " : " new violations: ") +
                 newIssues);

            /*
             * Check for violations related to arbiter placement that might
             * suggest that a topology rebalance is needed. Warnings are not
             * included because they don't prevent deploying the topology. Only
             * arbiter problems are included here because operations that
             * change the expected number of RNs already do a rebalance.
             */
            final boolean needsArbRebalance =
                newIssues.getViolations().stream().anyMatch(
                    violation ->
                    (violation instanceof InsufficientANs) ||
                    (violation instanceof ANNotAllowedOnSN) ||
                    (violation instanceof ANWrongDC));

            throw new IllegalCommandException
                ("Deploying topology candidate \"" + candidateName +
                 "\" will " + errorDesc + "\nTo deploy anyway, " +
                 "use plan deploy-topology -name <candidateName> -force. " +
                 "Use topology validate [-name <candidate name>] " +
                 "to view violations in the candidate \"" + candidateName +
                 "\" and the current, deployed topology." +
                 (needsArbRebalance ?
                  " You may need to use" +
                  " topology rebalance -name <candidate-name> to correct" +
                  " the placement of arbiters." :
                  ""),
                 ErrorMessage.NOSQL_5400, CommandResult.TOPO_PLAN_REPAIR);
        }

        /*
         * Do not check zone and shard health if there are any offline zone,
         * which is the case zones are changed to offline secondary zones by
         * a FailoverPlan.
         */
        if (!getOfflineZones().isEmpty()) {
            return;
        }

        boolean typeChanged = false;
        for (Datacenter dc : current.getSortedDatacenters()) {
            if (diff.typeChanged(dc.getResourceId())) {
                typeChanged = true;

                /*
                 * verify if a majority of nodes in each zone being changed
                 * are healthy.
                 */
                if (!Utils.verifyZoneHealth(dc.getResourceId(), current,
                                            executeLogger)) {
                    throw new IllegalCommandException(
                        "Deploying topology candidate \"" + candidateName +
                        "\" attempts to change type of zone " +
                        dc.getResourceId() + ", but a majority of nodes in " +
                        "this zone are offline. Retry this plan after " +
                        "correcting the problems that caused nodes " +
                        "to be offline", ErrorMessage.NOSQL_5400,
                        CommandResult.TOPO_PLAN_REPAIR);
                }
            }
        }

        if (typeChanged) {
            /* confirm all shards have quorum */
            try {
                HealthCheck.
                    createForGroups(
                            getAdmin(),
                            toString(),
                            current.getRepGroupIds()).
                    verify((new HealthCheck.Requirements()).
                            and(HealthCheck.SIMPMAJ_RUNNING));
            } catch (OperationFaultException ofe) {
                /*
                 * Catch health check exception and rethrow with a more
                 * detailed info.
                 */
                throw new IllegalCommandException(
                        String.format(
                            "Deploying topology candidate \"%s\" " +
                            "changes the type of one or more zones, " +
                            "but resulted in shard health problems " +
                            "which would cause the plan to fail. " +
                            "Retry the plan after correcting the problems. " +
                            "Problems:\n%s",
                            candidateName, ofe.getMessage()),
                        ErrorMessage.NOSQL_5400,
                        CommandResult.TOPO_PLAN_REPAIR);
            }
        }
    }

    /**
     * Override to inform the Admin in case of a successful deployment. If the
     * Admin throws an exception the state of the plan is set to ERROR and
     * hopefully the plan will be re-run.
     */
    @Override
    synchronized void setState(PlanRun planRun,
                               Planner plr,
                               State newState,
                               String msg) {
        if (newState == State.SUCCEEDED) {
            try {
                getAdmin().storeDeploymentSucceeded();
            } catch (Exception e) {
                super.setState(planRun, plr,
                               State.ERROR,
                               "Plan failed to update deployment state: " +
                               e.getMessage());
                return;
            }
        }
        super.setState(planRun, plr, newState, msg);
    }

    /**
     * Describe all running tasks, for a status report.
     */
    @Override
    public void describeRunning(Formatter fm,
                                final List<TaskRun> running,
                                boolean verbose) {

        /*
         * Treat the partition migration tasks specially by summarizing and
         * showing details for those tasks. Display all the non-migrations
         * first. In R2, migrations only run concurrently with other
         * migrations, so the migration list will end up containing all the
         * tasks.
         */
        List<TaskRun> migrations = new ArrayList<>();

        for (TaskRun tRun : running) {
            if (tRun.getTask() instanceof MigratePartition) {
                migrations.add(tRun);
            } else {
                fm.format("   Task %d/%s started at %s\n",
                          tRun.getTaskNum(), tRun.getTask(),
                          FormatUtils.formatDateTime(tRun.getStartTime()));
            }
        }

        /* Now show migration information. */
        int numQueued = 0;
        int numRunning = 0;
        int numSucceeded = 0;
        int numFailed = 0;
        int numUnknown = 0;
        long succeededTime = 0;

        /*
         * Process all the migrations, and show a summary in both verbose and
         * non-verbose mode. Save the details in a separate StringBuilder for
         * appending to the end of the description.
         */
        StringBuilder detailBd = new StringBuilder();
        Formatter taskDetailFM = new Formatter(detailBd);

        for (TaskRun m : migrations) {

            /* Get the target's details, to see what the migration state was.*/
            Map<String, String> details = m.getDetails();
            PartitionMigrationStatus targetStatus =
                PartitionMigrationStatus.parseTargetStatus(details);
            if (targetStatus == null) {
                continue;
            }

            if (verbose) {
                /* Only get the source's details if we are in verbose mode.*/
                PartitionMigrationStatus sourceStatus =
                    PartitionMigrationStatus.parseSourceStatus(details);
                if ((sourceStatus != null) &&
                    (sourceStatus.getStartTime() > 0)) {
                    taskDetailFM.format("   Task %d/%s:\n     %s\n     %s\n",
                                        m.getTaskNum(), m.getTask(),
                                        targetStatus,  sourceStatus);
                } else {
                    taskDetailFM.format("   Task %d/%s:\n     %s\n",
                              m.getTaskNum(), m.getTask(), targetStatus);
                }
            }

            switch (targetStatus.getState()) {
            case PENDING:
                numQueued++;
                break;
            case RUNNING:
                numRunning++;
                break;
            case SUCCEEDED:
                numSucceeded++;
                succeededTime += (targetStatus.getEndTime() -
                                  targetStatus.getStartTime());
                break;
            case ERROR:
                numFailed++;
                break;
            case UNKNOWN:
                numUnknown++;
                break;
            }
        }

        /* Show summary */
        if (numQueued != 0) {
            fm.format("   %d partition migrations queued\n", numQueued);
        }

        if (numRunning != 0) {
            fm.format("   %d partition migrations running\n", numRunning);
        }

        if (numSucceeded != 0) {
            fm.format("   %d partition migrations succeeded, " +
                      "avg migration time = %d ms.\n",
                      numSucceeded, (succeededTime / numSucceeded));
        }

        if (numUnknown != 0) {
            fm.format("   %d partition migrations could not be " +
                      "contacted for status\n", numUnknown);
        }

        if (numFailed != 0) {
            fm.format("   %d partition migrations failed\n", numFailed);
        } else if (numSucceeded > 0) {
            final long elapsedMS = System.currentTimeMillis() - createTime;
            final long elapsedMSPerTask = elapsedMS / numSucceeded;
            final long estMSRemaining =
                (numQueued + numRunning + numUnknown) * elapsedMSPerTask;
            fm.format(StatusReport.STRING_LABEL, "Estimated completion:",
                      FormatUtils.formatDateTime
                      (estMSRemaining + System.currentTimeMillis()));
        }

        if (verbose && (detailBd.length() > 0)) {
            fm.format("%s", detailBd.toString());
        }

        taskDetailFM.close();
    }

    /**
     * Describe all pending tasks, for a status report. Plans can override
     * this to provide a more informative, user friendly report for specific
     * plans.
     */
    @Override
    public void describeNotStarted(Formatter fm,
                                   final List<Task> notStarted,
                                   boolean verbose) {

        int migrationCount = 0;
        for (Task t : notStarted) {
            boolean showDetail = true;

            if (!verbose && (t instanceof MigratePartition)) {
                migrationCount++;
                showDetail = false;
            }
            if (showDetail) {
                fm.format("   Task %s\n", t);
            }
        }
        if ((!verbose) && (migrationCount > 0)) {
            fm.format("   %d partition migrations waiting", migrationCount);
        }
    }

    public String getCandidateName() {
        return candidateName;
    }

    /**
     * Update the sourceTopoSequence after every task, in case it has been
     * incremented by topo updates.
     */
    @Override
    synchronized void incrementEndCount(PlanRun planRun,
                                        Task.State state) {

        updatingTopo(planner.getAdmin().getCurrentTopology());
        super.incrementEndCount(planRun, state);
    }

    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        final MetadataType type = metadata.getType();
        switch (type) {
            case TOPOLOGY:
                return updatingTopo((Topology)metadata);
            case TABLE:
                final TableMetadata currentTableMd = this.getAdmin().
                        getMetadata(TableMetadata.class, MetadataType.TABLE);

                if (currentTableMd == null) {
                    return true;
                }
                return metadata.getSequenceNumber() >
                    currentTableMd.getSequenceNumber();
            case SECURITY:
                final SecurityMetadata currentSecMd = this.getAdmin().
                    getMetadata(SecurityMetadata.class, MetadataType.SECURITY);

                if (currentSecMd == null) {
                    return true;
                }
                return metadata.getSequenceNumber() >
                    currentSecMd.getSequenceNumber();
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    private boolean updatingTopo(Topology topology) {

        /**
         * If the topology is updated, then update the source seq # and
         * return true, which will persist the plan (and the new source seq #).
         */
        final int seqNum = topology.getSequenceNumber();

        if (seqNum <= sourceTopoSequence) {
            return false;
        }
        sourceTopoSequence = seqNum;
        return true;
    }

    /**
     * Add custom task status information to the TaskRun which records
     * information about each task execution. Must be synchronized on the
     * plan instance, to coordinate between different threads who are modifying
     * task state and persisting the plan instance.
     */
    public synchronized void addTaskDetails(Map<String, String> taskRunStatus,
                                            Map<String, String> info) {
        taskRunStatus.putAll(info);
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    @Override
    public String getOperation() {
        return "plan deploy-topology -name " + candidateName;
    }

    /*
     * TODO: replace field names with constants held in a json/command output
     * utility class.
     */
    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        jsonTop.put("plan_id", getId());
        jsonTop.put("topo_name", candidateName);
        return jsonTop;
    }
}
