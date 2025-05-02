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
 * Remove index task used by MultiMetadataPlan.
 */
public class RemoveIndexV2 extends RemoveIndex {

    private static final long serialVersionUID = 1L;

    private final MultiMetadataPlan multiMetadataPlan;

    public static RemoveIndexV2 newInstance(MultiMetadataPlan plan,
                                            String namespace,
                                            String indexName,
                                            String tableName) {
        final RemoveIndexV2 removeIndexV2 =
            new RemoveIndexV2(plan, namespace, indexName, tableName);
        removeIndexV2.checkIndexForRemove();
        return removeIndexV2;
    }

    private RemoveIndexV2(MultiMetadataPlan plan,
                          String namespace,
                          String indexName,
                          String tableName) {
        super(null, namespace, indexName, tableName);
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
