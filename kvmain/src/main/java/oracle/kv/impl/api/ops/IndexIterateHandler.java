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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.AncestorList;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link IndexIterate}.
 */
class IndexIterateHandler extends IndexOperationHandler<IndexIterate> {

    IndexIterateHandler(OperationHandler handler) {
        super(handler, OpCode.INDEX_ITERATE, IndexIterate.class);
    }

    @Override
    Result execute(IndexIterate op,
                   Transaction txn,
                   PartitionId partitionId  /* Not used */) {

        verifyTableAccess(op);

        final AncestorList ancestors =
            new AncestorList(operationHandler,
                             txn,
                             op.getResumePrimaryKey(),
                             op.getTargetTables().getAncestorTableIds());

        IndexScanner scanner =
            getIndexScanner(op,
                            txn,
                            CURSOR_DEFAULT,
                            LockMode.DEFAULT,
                            false, /* not key-only */
                            true /* moveAfterResumeKey */);

        final List<ResultIndexRows> results =
            new ArrayList<ResultIndexRows>();

        boolean moreElements;

        final DatabaseEntry dataEntry = scanner.getData();
        final SecondaryCursor cursor = scanner.getCursor();
        /*
         * Cannot get the DatabaseEntry key objects from the scanner until it
         * has been initialized.
         */
        try {
            while ((moreElements = scanner.next()) == true) {

                final DatabaseEntry indexKeyEntry = scanner.getIndexKey();
                final DatabaseEntry primaryKeyEntry = scanner.getPrimaryKey();

                assert !dataEntry.getPartial();
                final ResultValueVersion valVers =
                    operationHandler.makeValueVersion(cursor,
                                                      dataEntry,
                                                      scanner.getResult());
                ancestors.addIndexAncestorValues
                    (primaryKeyEntry, results, indexKeyEntry.getData(),
                     op.getOpSerialVersion());

                byte[] valBytes = operationHandler.reserializeToOldValue
                        (primaryKeyEntry.getData(), valVers.getValueBytes(),
                         op.getOpSerialVersion());

                results.add(new ResultIndexRows
                            (indexKeyEntry.getData(),
                             primaryKeyEntry.getData(),
                             valBytes,
                             valVers.getVersion(),
                             valVers.getExpirationTime(),
                             valVers.getModificationTime()));

                /*
                 * Check the size limit after add the entry to result rather
                 * than throwing away the already-fetched entry.
                 */
                if (exceedsMaxReadKB(op)) {
                    /**
                     * TODO:
                     * The primary row data is always read and lock even if it
                     * ends up rejected because of size, possible to check the
                     * size before actually read data?
                     */
                    break;
                }

                if (op.getBatchSize() > 0 &&
                    results.size() >= op.getBatchSize()) {
                    break;
                }
            }
            return new Result.IndexRowsIterateResult(getOpCode(),
                                                     op.getReadKB(),
                                                     op.getWriteKB(),
                                                     results,
                                                     moreElements);
        } finally {
            scanner.close();
        }
    }
}
