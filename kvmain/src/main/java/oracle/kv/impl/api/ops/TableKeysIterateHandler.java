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
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link TableKeysIterate}.
 */
class TableKeysIterateHandler
        extends TableIterateOperationHandler<TableKeysIterate> {

    TableKeysIterateHandler(OperationHandler handler) {
        super(handler, OpCode.TABLE_KEYS_ITERATE, TableKeysIterate.class);
    }

    @Override
    Result execute(TableKeysIterate op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyTableAccess(op);

        final List<ResultKey> results = new ArrayList<>();

        final OperationTableInfo tableInfo = new OperationTableInfo();
        Scanner scanner = getScanner(op,
                                     tableInfo,
                                     txn,
                                     partitionId,
                                     op.getMajorComplete(),
                                     op.getDirection(),
                                     op.getResumeKey(),
                                     true, /*moveAfterResumeKey*/
                                     CURSOR_DEFAULT,
                                     LockMode.READ_UNCOMMITTED_ALL,
                                     true); // set keyOnly. Handle fetch here

        try {
            DatabaseEntry keyEntry = scanner.getKey();
            Cursor cursor = scanner.getCursor();
            boolean moreElements;

            /*
             * Don't charge the cost of reading key in scanner, the cost of
             * reading key will be charged after key check by keyInTargetTable()
             *   - match > 0, valid key, charge min. read.
             *   - match < 0, no key found, charge empty read.
             *   - match = 0, invalid key for the target table and continue
             *                to next key, no charge.
             */
            scanner.setChargeKeyRead(false);
            while ((moreElements = scanner.next()) == true) {
                int match = keyInTargetTable(op,
                                             tableInfo,
                                             scanner,
                                             false /* chargeReadCost */);
                if (match > 0) {

                    /*
                     * The iteration was done using READ_UNCOMMITTED_ALL
                     * and with the cursor set to getPartial().  It is
                     * necessary to call getCurrent() here to both lock
                     * the record and fetch the data.
                     */
                    boolean ret = scanner.getCurrent();
                    if (ret) {

                        if (scanner.isTombstone() &&
                            !op.getIncludeTombstones()) {
                            continue;
                        }

                        /*
                         * Add ancestor table results.  These appear
                         * before targets, even for reverse iteration.
                         */
                        tableInfo.addAncestorKeys(cursor, results, keyEntry);
                        addKeyResult(results,
                                     keyEntry.getData(),
                                     scanner.getExpirationTime());
                    }

                    /* Charge min. read for reading the matched key */
                    op.addMinReadCharge();

                    if (exceedsMaxReadKB(op, 0)) {
                        break;
                    }
                } else {
                    if (match < 0) {
                        moreElements = false;
                        /* No matched key found, charge empty read */
                        op.addEmptyReadCharge();
                        break;
                    }
                }
                if (op.getBatchSize() != 0 &&
                    results.size() >= op.getBatchSize()) {
                    break;
                }
            }
            return new Result.KeysIterateResult(getOpCode(),
                                                op.getReadKB(), op.getWriteKB(),
                                                results, moreElements);
        } finally {
            scanner.close();
        }
    }
}
