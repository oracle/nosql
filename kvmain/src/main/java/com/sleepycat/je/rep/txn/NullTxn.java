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

package com.sleepycat.je.rep.txn;

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;

import java.util.stream.Collectors;

import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * A NullTxn is an internally generated txn. NullTxns are generated
 * infrequently in a system with active updates, since subsequent ongoing
 * commits effectively advance the DTVLSN and hence the quorum DTVLSN
 * independently of the txn. The NullTxn is basically a fallback, when no
 * application txn is available to propagate the dtvlsn to the replicas,
 * e.g. when the store has a single application issuing updates in
 * sequence.
 *
 * It is used for two purposes:
 *
 * 1) To update the DTVLSN information persistently in the logs, so that
 * it's readily recovered in the face of node failures.
 *
 * 2) As a way to ensure that any inflight non-durable txns at the time of
 * failure in the preceding term are made durable in a new term without
 * having to wait for the application itself to create a durable txn. That
 * is, it speeds up the final state of these non-durable txns, since the
 * ack of the null txn advances the dtvlsn.
 *
 * NullTxn commits are always committed with SIMPLE_MAJORITY durability but
 * they are handled slightly differently during commit processing:
 *
 * 1) The commit processing avoids all "waits" for authoritative masters.
 *
 * 2) It also avoids waiting for majority acknowledgments in a thread.
 * Instead, the feeder thread itself notices when the ack requirements are
 * satisfied and updates the DTVLSN accordingly.
 *
 * 3) NullTxns only have a single element cache in FeederTxns, to help recover
 * the txn object associated with its txn id.
 *
 */
public class NullTxn extends MasterTxn {

    /**
     * Convenience constant used by the DTVLSN flusher when committing the null
     * transaction. Note that the commit for a NullTxn does not wait for an
     * acknowledgement. Its acknowledgments get special treatment: The
     * FeederManager makes note of the last null txn that was generated and
     * uses it to process its acknowledgments, effectively making it ack
     * processing async.
     */
    private static TransactionConfig NULL_TXN_CONFIG = new TransactionConfig();

    static {
       NULL_TXN_CONFIG.
           setDurability(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                        SyncPolicy.WRITE_NO_SYNC,
                                        ReplicaAckPolicy.SIMPLE_MAJORITY));
    }

    public NullTxn(EnvironmentImpl envImpl) {
        super(envImpl, NULL_TXN_CONFIG);
    }

    @Override
    public boolean isNullTxn() {
        return true;
    }

    @Override
    protected boolean updateLoggedForTxn() {
        /*
         * Return true so that the commit will be logged even though there are
         * no changes associated with this txn
         */
        return true;
    }

    @Override
    protected void checkLockInvariant() {
        if (getCommitLsn() != DbLsn.NULL_LSN) {
            /*
             * The txn has been committed.
             */
            return;
        }

        if ((lastLoggedLsn == DbLsn.NULL_LSN) &&
            getWriteLockIds().isEmpty()) {
            /* Uncommitted, state as expected. */
          return;
        }

        final String lsns = "[" +
            getWriteLockIds().stream(). map((l) -> DbLsn.getNoFormatString(l)).
                collect(Collectors.joining(",")) +
            "]";
        final String msg = "Unexpected lock state  for null txn" +
            " lastLoggedLsn=" + DbLsn.getNoFormatString(lastLoggedLsn) +
             " locked lsns:" + lsns + " txn=" + getId();
        LoggerUtils.severe(envImpl.getLogger(), envImpl, msg);
        throw unexpectedState(envImpl, msg);
    }
}