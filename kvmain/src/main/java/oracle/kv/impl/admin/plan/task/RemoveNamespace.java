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

import static oracle.kv.impl.admin.plan.task.AddTable.tableMetadataNotFound;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NamespaceNotFoundException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MultiMetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Transaction;

/**
 * Remove/drop a namespace
 */
public class RemoveNamespace extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected final MultiMetadataPlan multiMetadataPlan;
    protected final String namespace;
    protected final boolean cascade;

    /**
     * Creates a task to only remove the namespace.
     *
     * Note: Tasks for removing tables inside the
     * namespace must be added before this task.
     * Note: cascade flag just inhibits initial check on empty
     * namespace when task is created, like in the case of cascade option.
     */
    public RemoveNamespace(MultiMetadataPlan plan,
        String namespace, boolean cascade) {

        super(null);
        /*
         * Caller verifies parameters
         */
        this.multiMetadataPlan = plan;
        this.namespace = namespace;
        this.cascade = cascade;

        checkNamespaceForRemove();
    }

    @Override
    protected TableMetadata getMetadata() {
        return multiMetadataPlan.getTableMetadata();
    }

    @Override
    protected AbstractPlan getPlan() {
        return this.multiMetadataPlan;
    }

    @Override
    protected TableMetadata getMetadata(Transaction txn) {
        return multiMetadataPlan.getTableMetadata(txn);
    }

    /**
     * Checks if namespace to be removed can be found. This method must be
     * called once the table metadata is available.
     */
    protected void checkNamespaceForRemove() {
        final TableMetadata md = getMetadata();

        if (md == null) {
            throw tableMetadataNotFound();
        }

        if (!md.hasNamespace(namespace)) {
            throw new NamespaceNotFoundException("Namespace '" + namespace +
                "' does not exist.");
        }

        if (!cascade && !md.isNamespaceEmpty(namespace)) {
            throw new IllegalCommandException("Namespace '" + namespace + "'" +
                " cannot be removed because it is not empty. Remove all " +
                "tables contained in this namespace or use CASCADE option.");
        }
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockNamespace(planner, getPlan(), namespace);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {
        final Admin admin = getPlan().getAdmin();

        /*
         * See if the namespace is still present.  This will not throw if the
         * ns is absent. Return the metadata so that it is broadcast, just
         * in case this is a re-execute.
         */
        if (md.hasNamespace(namespace)) {

            /*
             * If it is not the same ns, then the original ns was
             * removed and the metadata updated. In that case just exit.
             */
            md.dropNamespace(namespace);

            admin.saveMetadata(md, txn);
            TxnUtil.localCommit(txn, admin.getLogger());
        }
        MDTableUtil.removeNamespace(namespace, md, admin);
        return md;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
            namespace,
            null,
            null);
    }

    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }

        RemoveNamespace other = (RemoveNamespace) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return true;
    }
}
