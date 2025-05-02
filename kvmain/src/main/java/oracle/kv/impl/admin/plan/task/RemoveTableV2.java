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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MultiMetadataPlan;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Transaction;

/**
 * Remove table task used by MultiMetadataPlan.
 */
public class RemoveTableV2 extends RemoveTable {
    private static final long serialVersionUID = 1L;

    private final MultiMetadataPlan multiMetadataPlan;

    public static RemoveTableV2 newInstance(MultiMetadataPlan plan,
                                            String namespace,
                                            String tableName,
                                            boolean removeChildTables,
                                            boolean markForDelete) {
        final RemoveTableV2 removeTable =
            new RemoveTableV2(plan, namespace, tableName, markForDelete);
        removeTable.checkTableForRemove(removeChildTables);
        return removeTable;
    }

    private RemoveTableV2(MultiMetadataPlan plan,
                          String namespace,
                          String tableName,
                          boolean markForDelete) {


        super(null, namespace, tableName, markForDelete);
        this.multiMetadataPlan = plan;
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
}
