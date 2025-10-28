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

package oracle.kv.impl.admin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.systables.SysTableDescriptor;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ShutdownThread;


/**
 * A task that monitors system tables. It will create the tables if they don't
 * exist and will upgrade existing tables if needed. This task should be run
 * when the Admin becomes the master. If any action is taken (creating or
 * evolving a table) the task will reschedule itself so that the actions can
 * be retried in case they fail.
 *
 * This task is designed so it is not necessary to monitor plan failures. If
 * the plan fails a new one will be created the next go around. An attempt is
 * made to cancel plans that end in non-terminal state. However, if they are
 * missed the plan pruning mechanism will eventually remove them.
 */
class SysTableMonitor extends ShutdownThread {

    /* Minimum/maximum time to wait between retries */
    private final static int MIN_WAIT_MS = 2 * 1000;
    private final static int MAX_WAIT_MS = 60 * 1000;

    private final Admin admin;

    /**
     * Map of descriptors to plan IDs. The map is initialized to the
     * set of table descriptors with the plan ID set to NO_PLAN. If the table
     * needs attention the resulting plan ID is added. If no action is taken,
     * the descriptor is removed. The task will retry (up to 10 times) as
     * long as the map is not empty.
     */
    private final Map<SysTableDescriptor, Integer> descriptors;

    /**
     * Value in descriptors map to indicate there is no plan associated with
     * the table.
     */
    private static final int NO_PLAN = -1;

    SysTableMonitor(Admin admin) {
        super("System table monitor");
        this.admin = admin;
        descriptors = new HashMap<>(SysTableRegistry.descriptors.length);
        for (SysTableDescriptor desc : SysTableRegistry.descriptors) {
            descriptors.put(desc, NO_PLAN);
        }
    }

    /**
     * Checks to see if the system tables in the descriptors map are up to
     * the correct version. If they are not, a plan is created to upgrade them.
     */
    @Override
    public void run() {
        getLogger().log(Level.INFO, "Starting {0}", this);

        int waitTimeMS = MIN_WAIT_MS;

        while (!isShutdown() && !descriptors.isEmpty()) {
            boolean pendingWork = checkTables();

            if (isShutdown() || descriptors.isEmpty()) {
                break;
            }
            /*
             * descriptors is not empty. If there is no pending work then
             * there has been an error or need to wait for an upgrade.
             */
            if (pendingWork) {
                waitTimeMS = MIN_WAIT_MS;
            } else {
                /*
                 * Increase the wait time (up to max) if retrying because of
                 * an error or upgrade.
                 */
                waitTimeMS += waitTimeMS;
                if (waitTimeMS > MAX_WAIT_MS) {
                    waitTimeMS = MAX_WAIT_MS;
                }
            }
            waitFor(waitTimeMS);
        }

        /*  System tables are good, attempt to init the table MD system table */
        waitTimeMS = MIN_WAIT_MS;
        while (!isShutdown()) {
            if (MDTableUtil.check(admin, this)) {
                break;
            }
            waitTimeMS += waitTimeMS;
            if (waitTimeMS > MAX_WAIT_MS) {
                waitTimeMS = MAX_WAIT_MS;
            }
            waitFor(waitTimeMS);
        }
        getLogger().log(Level.INFO, "Exit {0}", this);
    }

    /*
     * Waits for the specified time. May return early if the
     * thread is shut down.
     */
    private synchronized void waitFor(int millis) {
        try {
            waitForMS(millis);
        } catch (InterruptedException ie) {
            /* Ignore */
        }
    }

    /*
     * Checks system tables, performing any operations necessary (create,
     * evolve, etc.). If a table does not need any attention it is removed
     * from the descriptors map. If descriptors is empty on return the
     * check is done.
     *
     * If descriptors is not empty and true is returned there are ongoing
     * operations that need time to complete.
     *
     * If false is returned then a) there has been an error on one or more
     * of the tables or b) a table requires an upgrade.
     */
    private boolean checkTables() {
        getLogger().log(Level.INFO, "Checking status of system tables");

        final TableMetadata md = admin.getMetadata(TableMetadata.class,
                                                   Metadata.MetadataType.TABLE);

        final Iterator<Entry<SysTableDescriptor, Integer>> itr =
                descriptors.entrySet().iterator();

        boolean pendingWork = false;

        while (!isShutdown() && itr.hasNext()) {
            final Entry<SysTableDescriptor, Integer> entry = itr.next();
            final SysTableDescriptor desc = entry.getKey();
            int planId = entry.getValue();

            /*
             * If there is already a plan in place to handle this table, check
             * on its state. Returns true if there is a plan and it is running.
             */
            if (checkPlan(planId,  desc)) {
                pendingWork = true;
                continue;
            }
            getLogger().log(Level.FINE,
                            "Checking status of system table {0}",
                            desc.getTableName());

            /*
             * If the table does not exist, or the metadata does not exist
             * create the table (the plan will also create the metadata if
             * needed).
             */
            final TableImpl table =
                    (md == null) ? null : md.getTable(desc.getTableName());
            if (table == null) {
                final TableImpl newTable = desc.buildTable();
                if (checkVersionRequirement(newTable)) {
                    continue;
                }

                try {
                    planId = admin.getPlanner().
                                    createAddTablePlan("Create system table",
                                                       newTable,
                                                       null,
                                                       true /* systemTable */,
                                                       null);
                    admin.approvePlan(planId);
                    admin.executePlanOrFindMatch(planId);
                    descriptors.put(desc, planId);
                    pendingWork = true;
                } catch (Exception e) {
                    if (!isShutdown()) {
                        getLogger().log(Level.INFO,
                                        "Exception creating system" +
                                        " table {0} {1}",
                                        new Object[]{newTable.getName(),
                                                     e.getMessage()});
                    }
                }
                continue;
            }

            /*
             * See if the table needs to be upgraded.
             */
            final TableImpl newTable = desc.evolveTable(table, getLogger());
            if (newTable != null) {
                try {
                    assert !newTable.hasIdentityColumn();

                    if (checkVersionRequirement(newTable)) {
                        continue;
                    }

                    planId = admin.getPlanner().
                        createEvolveTablePlan("Upgrade system table",
                            newTable.getInternalNamespace(),
                            newTable.getFullName(),
                            table.getTableVersion(),
                            newTable.getFieldMap(),
                            newTable.getDefaultTTL(),
                            newTable.getBeforeImageTTL(),
                            newTable.getDescription(),
                            true /* systemTable */,
                            null /*identityColumnInfo*/,
                            null /*sequenceDefChange*/,
                            null /*regions*/);
                    admin.approvePlan(planId);
                    admin.executePlanOrFindMatch(planId);
                    descriptors.put(desc, planId);
                    pendingWork = true;
                } catch (Exception e) {
                    if (!isShutdown()) {
                        getLogger().log(Level.INFO,
                                        "Exception evolving system" +
                                        " table {0} {1}",
                                        new Object[]{table.getName(),
                                                     e.getMessage()});
                    }
                }
                continue;
            }

            /*
             * See if the table needs any indexes.
             */
            final SysTableDescriptor.IndexDescriptor idxDesc =
                    desc.getIndexDescriptor(table);
            if (idxDesc != null) {
                assert table.getIndex(idxDesc.getIndexName()) == null;
                try {
                    planId = admin.getPlanner().
                            createAddIndexPlan("Add index to system table",
                                    table.getInternalNamespace(),
                                    idxDesc.getIndexName(),
                                    table.getFullName(),
                                    idxDesc.getIndexedFields(),
                                    idxDesc.getIndexedTypes(),
                                    false /*indexesNulls*/,
                                    false /*isUnique*/,
                                    idxDesc.getDescription(),
                                    true);
                    admin.approvePlan(planId);
                    admin.executePlanOrFindMatch(planId);
                    descriptors.put(desc, planId);
                    pendingWork = true;
                } catch (Exception e) {
                    if (!isShutdown()) {
                        getLogger().log(Level.INFO,
                                "Exception creating index {0} on" +
                                        " system table {1} {2}",
                                new Object[]{idxDesc.getIndexName(),
                                        table.getName(),
                                        e.getMessage()});
                    }
                }
                continue;
            }
            /* Nothing needed to be done, remove the descriptor. */
            itr.remove();
        }
        return pendingWork;
    }

    /**
     * Checks the version requirement of the specified table. Returns true if
     * the store needs to be upgraded before the table can be updated (or
     * created) otherwise returns false;
     *
     * @return true if the store needs to be upgraded
     */
    private boolean checkVersionRequirement(TableImpl table) {
        return checkVersionRequirement(
                SerialVersion.getKVVersion(table.getRequiredSerialVersion()),
                table.getName());
    }

    private boolean checkVersionRequirement(KVVersion requiredVersion,
                                            String tableName) {
        try {
            TablePlanGenerator.checkStoreVersion(admin, requiredVersion);
        } catch (IllegalCommandException ice) {
            getLogger().log(Level.INFO,
                            "Unable to create/update table {0}, store needs" +
                                        " to be at {1}, waiting for upgrade",
                            new Object[] { tableName, requiredVersion });
            return true;
        } catch (Exception e) {
            /*
             * There was an exception getting the store version. One or more
             * nodes may be down, perhaps due to an upgrade in progress. Return
             * true assuming it is an upgrade.
             */
            getLogger().log(Level.INFO,
                            "Exception getting store version: {0}", e);
            return true;
        }
        return false;
    }

    /**
     * Checks if there is a plan handling the descriptor and if there is
     * checks whether it is still running. Returns true if there is a plan
     * and it is still running, otherwise false. This method may also
     * modify the entry in descriptors based on the state of the plan.
     */
    private boolean checkPlan(int planId, SysTableDescriptor desc) {
        if (planId == NO_PLAN) {
            return false;
        }
        final Plan plan = admin.getPlanById(planId);

        /*
         * In theory the plan could be pruned. In this case we don't know
         * what state it ended in, so retry just in case.
         */
        if (plan == null) {
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} is missing, retrying",
                            new Object[]{planId, desc});
            descriptors.put(desc, NO_PLAN);
            return false;
        }

        switch (plan.getState()) {
        case PENDING:
        case APPROVED:
        case RUNNING:
            /* Plan is still running. */
            return true;

        case SUCCEEDED:
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} completed",
                            new Object[]{planId, desc});
            descriptors.put(desc, NO_PLAN);
            return false;

        case INTERRUPTED:
        case ERROR:
        case INTERRUPT_REQUESTED:
            /* Cancel old plan */
            admin.cancelPlan(planId);

            //$FALL-THROUGH$
        case CANCELED:
             /* Plan failed or was canceled. Retry. */
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} did not complete, retrying",
                            new Object[]{planId, desc});
            descriptors.put(desc, NO_PLAN);
            return false;
        }
        throw new IllegalStateException("Unknown plan state: " +
                                        plan.getState());
    }

    @Override
    protected Logger getLogger() {
        return admin.getLogger();
    }
}
