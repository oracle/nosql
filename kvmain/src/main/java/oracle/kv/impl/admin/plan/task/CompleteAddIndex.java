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
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Transaction;

/**
 * Completes index addition, making the index publicly visible.
 */
public class CompleteAddIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String indexName;
    private final String tableName;
    private final String namespace;

    /**
     */
    public CompleteAddIndex(MetadataPlan<TableMetadata> plan,
                            String namespace,
                            String indexName,
                            String tableName) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            getPlan().getLogger().log(Level.INFO,
                                "{0}, unexpected error, table {1} not" +
                                     " found in metadata",
                                     new Object[]{this, tableName});
            return null;
        }
        final Admin admin = getPlan().getAdmin();
        final IndexImpl index = (IndexImpl)table.getIndex(indexName);
        /*
         * The index should exist because the plan to create it started, but
         * it will not if, for example, the index population failed. Log this
         * situation and succeed;
         */
        if (index == null) {
            getPlan().getLogger().log(Level.INFO,
                                      "{0} index {1} does not exist in table," +
                                      " index population may have failed",
                                      new Object[]{this, indexName});
        } else {
            /* Use the TableMetadata method to bump the seq. number */
            md.updateIndexStatus(namespace, indexName, tableName,
                    IndexImpl.IndexStatus.READY);
            admin.saveMetadata(md, txn);
            TxnUtil.localCommit(txn, admin.getLogger());
        }
        MDTableUtil.updateTable(table, md, admin);
        return md;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           indexName);
    }

    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }
}
