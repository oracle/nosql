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

package oracle.kv.impl.rep.subscription.partreader;

import oracle.kv.impl.topo.PartitionId;

/**
 * Interface of callback which is provided by clients to process
 * each entry received from source node partition migration service.
 */
public interface PartitionReaderCallBack {

    /**
     * callback function to process a COPY or PUT operation
     *
     * @param pid             id of partition
     * @param vlsn            vlsn of operation
     * @param expirationTime  expiration time of record
     * @param key             key to copy or put
     * @param value           value part of operation
     */
    void processCopy(PartitionId pid, long vlsn, long expirationTime,
                     byte[] key, byte[] value);

    /**
     * callback function to process a COPY or PUT operation
     *
     * @param pid             id of partition
     * @param vlsn            vlsn of operation
     * @param expirationTime  expiration time of record
     * @param key             key to copy or put
     * @param value           value part of operation
     * @param txnId           id of transaction
     */
    void processPut(PartitionId pid, long vlsn, long expirationTime,
                    byte[] key, byte[] value,
                    long txnId);

    /**
     * callback function to process a DEL operation
     *
     * @param pid          id of partition
     * @param vlsn         vlsn of operation
     * @param key          key to delete
     * @param txnId        id of transaction
     */
    void processDel(PartitionId pid, long vlsn, byte[] key, long txnId);

    /**
     * callback function to process a PREPARE operation
     *
     * @param pid          id of partition
     * @param txnId        id of transaction
     */
    void processPrepare(PartitionId pid, long txnId);

    /**
     * callback function to COMMIT a transaction
     *
     * @param pid          id of partition
     * @param txnId        id of transaction
     */
    void processCommit(PartitionId pid, long txnId);

    /**
     * callback function to ABORT a transaction
     *
     * @param pid          id of partition
     * @param txnId        id of transaction
     */
    void processAbort(PartitionId pid, long txnId);

    /**
     * callback function to process EOD
     *
     * @param pid  id of partition
     */
    void processEOD(PartitionId pid);

}
