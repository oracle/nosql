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

package oracle.kv.impl.tif;


/**
 * Interface to define client's callback to process each txn or operation
 * after txn agenda commit it to ES index.
 */
interface TransactionPostCommitCallback {

    /**
     * Callback after committing a transaction in agenda
     *
     * @param txn  txn committed
     * @param commitVLSN  vlsn of commit
     */
    void postCommit(TransactionAgenda.Transaction txn, long commitVLSN);

    /**
     * Callback after committing COPY operation from partition transfer. In
     * this case there is no commit, and we commit each COPY operation from
     * partition transfer.
     *
     * @param op  index operation built on the COPY operation
     */
    void postCommit(IndexOperation op);
}
