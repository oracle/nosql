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

import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.metadata.Metadata;

import com.sleepycat.je.Transaction;

/**
 * Broadcasts metadata to all RNs. This task may be used standalone
 * or may be subclassed to provide additional functionality.
 */
public class UpdateMetadata<T extends Metadata<?>> extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    /**
     * plan can be null in the case of a MultiMetadataPlan. It is private
     * to avoid coding errors in subclasses. Any public or protected methods
     * of this class that use plan need to be overridden in a task used in
     * a MultiMetadataPlan.
     */
    private final MetadataPlan<T> plan;

    public UpdateMetadata(MetadataPlan<T> plan) {
        this.plan = plan;
    }

    /**
     * Gets the updated metadata to broadcast. The default implementation
     * calls plan.getMetadata(). If null is returned no broadcast is made and
     * the task ends with SUCCEEDED.
     *
     * @return the metadata to broadcast or null
     */

    public T doUpdateMetadata(Transaction txn) {
        T md = getMetadata(txn);

        /*
         * The above getMetadata call will not lock the MD object if it does not
         * exists. If null is returned we attempt to write an empty MD object
         * to the Admin store which will create a lock.
         */
        if (md == null) {
            md = createMetadata();
            assert md != null;

            /*
             * If the save with noOverwrite returns true, then the MD has been
             * written by someone else since the getMetadata call above. In
             * that case read the newly stored MD.
             */
            if (plan.getAdmin().saveMetadata(md, txn, true /* noOverwrite */)) {
                md = getMetadata(txn);

                /*
                 * If MD is still null there is a problem.
                 */
                if (md == null) {
                    throw new IllegalCommandException(
                              "Unexpected error getting metadata in task: " +
                              toString());
                }
            }
        }

        return updateMetadata(md, txn);
    }

    @SuppressWarnings("unused")
    protected T updateMetadata(T md, Transaction txn) {
        return md;
    }

    @Override
    protected AbstractPlan getPlan() {
        if (plan == null) {
            throw new IllegalStateException(
                "Tasks with null plans must override getPlan()");
        }
        return plan;
    }

    /**
     * Creates the metadata object. Overridden by tasks which could expect that
     * the metadata doesn't yet exist (like add* tasks).
     */
    protected T createMetadata() {
        throw new IllegalStateException(
                                  "Metadata not found in task: " + toString());
    }

    protected T getMetadata() {
        if (plan == null) {
            throw new IllegalStateException(
                "Task was not initialized with a MetadataPlan properly, " +
                "or this task did not overload this method correctly");
        }
        return plan.getMetadata();
    }

    protected T getMetadata(Transaction txn) {
        if (plan == null) {
            throw new IllegalStateException(
                "Tasks with null plans must override getMetadata(Transaction)");
        }
        return plan.getMetadata(txn);
    }

    @Override
    public State doWork() throws Exception {
        final Admin admin = getPlan().getAdmin();
        final T md = admin.updateMetadata(this);

        if (md == null) {
            return State.SUCCEEDED;
        }
        admin.getLogger().log(Level.FINE, "{0} about to broadcast {1}",
                              new Object[]{this, md});

        if (!Utils.broadcastMetadataChangesToRNs(admin.getLogger(),
                                                 md,
                                                 admin.getCurrentTopology(),
                                                 getName(),
                                                 admin.getParams()
                                                 .getAdminParams(),
                                                 getPlan())) {
            return State.INTERRUPTED;
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }
}
