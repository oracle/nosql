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

import com.sleepycat.je.Transaction;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.util.TxnUtil;

/**
 * Set table limits.
 */
public class SetTableLimits extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String tableName;
    private final TableLimits newLimits;

    /**
     */
    public SetTableLimits(MetadataPlan<TableMetadata> plan,
                          String namespace, String tableName,
                          TableLimits newLimits) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.namespace = namespace;
        this.tableName = tableName;
        this.newLimits = newLimits;

        final TableMetadata md = plan.getMetadata();
        if (md == null) {
            throw new IllegalCommandException
                ("Table metadata not found");
        }
        if (md.getTable(namespace, tableName) == null) {
            throw new IllegalCommandException
                ("Table does not exist: " + tableName);
        }
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {
        if (md == null) {
            throw new IllegalStateException("Table metadata not found");
        }
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            throw new IllegalStateException("Cannot find table: " + tableName);
        }
        md.setLimits(table, newLimits);
        final Admin admin = getPlan().getAdmin();
        admin.saveMetadata(md, txn);
        TxnUtil.localCommit(txn, admin.getLogger());

        /*
         * The table metadata as been persisted in the Admin DB, now update
         * the table MD system table.
         */
        MDTableUtil.updateTable(table, md, admin);
        return md;
    }

    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (!(t instanceof SetTableLimits)) {
            return false;
        }

        final SetTableLimits other = (SetTableLimits) t;
        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        if (!tableName.equalsIgnoreCase(other.tableName)) {
            return false;
        }

        if (newLimits == null) {
            return other.newLimits == null;
        }
        return newLimits.equals(other.newLimits);
    }
}
