/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.txn;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.function.Supplier;

import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.utilint.Timestamp;

/**
 * Transaction commit.
 */
public class TxnCommit extends VersionedWriteTxnEnd {

    public TxnCommit(long id,
                     long lastLsn,
                     Supplier<MasterIdTerm> masterIdTermSupplier,
                     long dtvlsn) {
        super(id, lastLsn, masterIdTermSupplier, dtvlsn);
        if ((masterId > 0) && (dtvlsn < NULL_VLSN)) {
            /*
             * Note that the dtvln will be NULL when a Txn is created on a
             * master, so allow for it.
             */
            throw new IllegalStateException("DTVLSN value:" + dtvlsn);
        }
    }

    public TxnCommit(long id, long lastLsn,
                     Supplier<MasterIdTerm> masterIdTermSupplier,
                     long dtvlsn,
                     Timestamp time) {
        this(id, lastLsn, masterIdTermSupplier, dtvlsn);
        this.time = time;
    }

    /**
     * For constructing from the log during tests.
     */
    public TxnCommit() {
    }

    @Override
    protected String getTagName() {
        return "TxnCommit";
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof TxnCommit)) {
            return false;
        }

        VersionedWriteTxnEnd otherCommit = (VersionedWriteTxnEnd) other;

        return super.logicalEquals(otherCommit);
    }
}
