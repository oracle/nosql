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

package com.sleepycat.je.rep.subscription;

import com.sleepycat.je.dbi.DatabaseId;

/**
 * Interface of subscription callback function, to be implemented by clients to
 * process each received subscription message.
 */
public interface SubscriptionCallback {

    /**
     * Process a put (insert or update) entry from stream
     *
     * @param vlsn       VLSN of the insert entry
     * @param key        key of the insert entry
     * @param value      value of the insert entry
     * @param txnId      id of txn the entry belongs to
     * @param dbid       id of database the entry belongs to
     * @param ts         timestamp of the last update
     * @param expiration the expiration time as the system time in ms
     * @param beforeImgEnabled true if before image enabled, false otherwise
     * @param valBeforeImg value of before image if enabled, or null
     * @param tsBeforeImg timestamp in ms of before image, or 0
     * @param expBeforeImg expiration time in ms of before image, or 0
     */
    void processPut(long vlsn, byte[] key, byte[] value, long txnId,
                    DatabaseId dbid, long ts, long expiration,
                    boolean beforeImgEnabled,
                    byte[] valBeforeImg,
                    long tsBeforeImg,
                    long expBeforeImg);

    /**
     * Process a delete entry from stream
     *
     * @param vlsn   VLSN of the delete entry
     * @param key    key of the delete entry
     * @param val    val of tombstone if exists, null otherwise
     * @param txnId  id of txn the entry belongs to
     * @param dbid   id of database the entry belongs to
     * @param ts     timestamp of the last update
     * @param beforeImgEnabled true if before image enabled, false otherwise
     * @param valBeforeImg value of before image if enabled, or null
     * @param tsBeforeImg timestamp in ms of before image, or 0
     * @param expBeforeImg expiration time in ms of before image, or 0
     */
    void processDel(long vlsn, byte[] key, byte[] val,
                    long txnId, DatabaseId dbid, long ts,
                    boolean beforeImgEnabled,
                    byte[] valBeforeImg,
                    long tsBeforeImg,
                    long expBeforeImg);

    /**
     * Process a commit entry from stream
     *
     * @param vlsn  VLSN of commit entry
     * @param txnId id of txn to commit
     * @param timestamp timestamp of the commit entry
     */
    void processCommit(long vlsn, long txnId, long timestamp);

    /**
     * Process an abort entry from stream
     *
     * @param vlsn  VLSN of abort entry
     * @param txnId id of txn to abort
     * @param timestamp timestamp of the abort entry
     */
    void processAbort(long vlsn, long txnId, long timestamp);

    /**
     * Process the exception from stream.
     *
     * @param exp  exception raised in service and to be processed by
     *             client
     */
    void processException(final Exception exp);
}
