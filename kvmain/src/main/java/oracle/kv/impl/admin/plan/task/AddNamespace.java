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

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NamespaceAlreadyExistsException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadata.NamespaceImpl;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Transaction;

/**
 * Adds a namespace
 */
public class AddNamespace extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String namespace;

    public AddNamespace(MetadataPlan<TableMetadata> plan, String namespace) {
        super(plan);

        this.namespace = namespace;
        /*
         * Caller verifies parameters
         */

        final TableMetadata md = plan.getMetadata();
        if (md == null) {
            throw tableMetadataNotFound();
        }
        if (md.hasNamespace(namespace)) {
            throw namespaceAlreadyExists();
        }
    }


    static IllegalCommandException tableMetadataNotFound() {
        return new IllegalCommandException("Table metadata not found");
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockNamespace(planner, getPlan(), namespace);
    }

    @Override
    protected TableMetadata createMetadata() {
        return new TableMetadata(true);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        final Admin admin = getPlan().getAdmin();

        /* If the namespace does not exist, add it */
        NamespaceImpl ns = md.getNamespace(namespace);
        if (ns == null) {
            ns = md.createNamespace(namespace, SecurityUtils.currentUserAsOwner());
            admin.saveMetadata(md, txn);
            TxnUtil.localCommit(txn, admin.getLogger());

            /*
             * The table metadata as been persisted in the Admin DB, now update
             * the table MD system table.
             */
            MDTableUtil.addNamespace(ns, md, admin);
            return md;
        }

        /* Already exist, make sure it is in the MD system table */
        MDTableUtil.addNamespace(ns, md, admin);

        /*
         * TODO - this seems incorrect. Tasks should be idempotent
         * so should just exit with success if the owner is the same.?
         */
        throw namespaceAlreadyExists();
    }

    /**
     * Throws a "Namespace  already exists" IllegalCommandException.
     */
    private NamespaceAlreadyExistsException namespaceAlreadyExists() {
        return new NamespaceAlreadyExistsException
            ("Namespace '" + namespace + "' already exists");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb), namespace, null,
            null);
    }

    /**
     * Returns true if this AddNamespace will end up creating the same
     * namespace.
     * Checks that namespace is the same. Intentionally excludes r2compat,
     * schemId, and description, since those don't directly affect the table
     * metadata.
     */
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

        AddNamespace other = (AddNamespace) t;
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
