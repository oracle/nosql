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

package oracle.kv.impl.api.ops;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.AncestorList;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.table.Table;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link IndexKeysIterate}.
 */
public class IndexKeysIterateHandler
        extends IndexOperationHandler<IndexKeysIterate> {

    IndexKeysIterateHandler(OperationHandler handler) {
        super(handler, OpCode.INDEX_KEYS_ITERATE, IndexKeysIterate.class);
    }

    @Override
    Result execute(IndexKeysIterate op,
                   Transaction txn,
                   PartitionId partitionId  /* Not used */) {

        verifyTableAccess(op);

        final AncestorList ancestors =
            new AncestorList(operationHandler,
                             txn,
                             op.getResumePrimaryKey(),
                             op.getTargetTables().getAncestorTableIds());

        final Table table = getTable(op);
        final IndexImpl index = getIndex(op,
            ((TableImpl)table).getInternalNamespace(),
            table.getFullName());
        IndexScanner scanner =
            new IndexScanner(op, txn,
                             getSecondaryDatabase(
                                 ((TableImpl)table).getInternalNamespace(),
                                 table.getFullName(),
                                 op.getIndexName()),
                             index,
                             op.getIndexRange(),
                             op.getResumeSecondaryKey(),
                             op.getResumePrimaryKey(),
                             true, // key-only, don't fetch data
                             true /* moveAfterResumeKey */);

        final List<ResultIndexKeys> results =  new ArrayList<>();

        boolean moreElements;

        /*
         * Cannot get the DatabaseEntry key objects from the scanner until it
         * has been initialized.
         */
        try {
            while ((moreElements = scanner.next()) == true) {

                final DatabaseEntry indexKeyEntry = scanner.getIndexKey();
                final DatabaseEntry primaryKeyEntry = scanner.getPrimaryKey();

                /*
                 * Data has not been fetched but the record is locked.
                 * Create the result from the index key and information
                 * available in the index record.
                 */
                List<ResultKey> ancestorKeys =
                    ancestors.addAncestorKeys(primaryKeyEntry);

                if (ancestorKeys != null) {

                    for (ResultKey key : ancestorKeys) {

                        byte[] indexKey = indexKeyEntry.getData();

                        if (indexKey != null) {
                            results.add(new ResultIndexKeys(
                                        key.getKeyBytes(),
                                        indexKey,
                                        key.getExpirationTime()));
                        }
                    }
                }

                byte[] indexKey = indexKeyEntry.getData();
                if (indexKey != null) {
                    results.add(new ResultIndexKeys(
                                primaryKeyEntry.getData(),
                                indexKey,
                                scanner.getExpirationTime()));
                }

                /*
                 * Check the size limit after add the entry to result rather
                 * than throwing away the already-fetched entry.
                 */
                if (exceedsMaxReadKB(op)) {
                    break;
                }

                if (op.getBatchSize() > 0 &&
                    results.size() >= op.getBatchSize()) {
                    break;
                }
            }
            return new Result.IndexKeysIterateResult(getOpCode(),
                                                     op.getReadKB(),
                                                     op.getWriteKB(),
                                                     results,
                                                     moreElements);
        } finally {
            scanner.close();
        }
    }
}
