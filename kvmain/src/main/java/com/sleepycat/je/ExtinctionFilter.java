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

package com.sleepycat.je;

/**
 * Used in conjunction with {@link Environment#discardExtinctRecords} to
 * discard and purge extinct records. The {@code ExtinctionFilter} is
 * specified by calling {@link EnvironmentConfig#setExtinctionFilter}.
 * A non-null extinction filter must be specified before calling
 * {@code Environment.discardExtinctRecords}.
 *
 * <p>The benefit of record extinction, compared to explicit record deletion,
 * is that an entry is not logged or replicated for each extinct record,
 * resulting in less resource usage. Only a single entry is logged and
 * replicated when calling {@code Environment.discardExtinctRecords}, and a
 * small amount of cleaner metadata is written when extinct records are
 * counted obsolete. Performance impacts to be aware of are as follows.</p>
 *
 * <ul>
 *     <li>A Btree scan is performed asynchronously, after calling
 *     {@code Environment.discardExtinctRecords}. This scan will have little
 *     or no impact on ongoing operations, as long as Btree nodes (BINs in
 *     particular) are in the JE cache as recommended. The scan counts the
 *     size of the specified extinct records, logs this change to JE cleaner
 *     metadata, and removes the extinct records from the Btree. The changes
 *     to the Btree are logged at the next checkpoint.</li>
 *
 *     <li>The performance benefits of record extinction are proportional to
 *     the size of the set of extinct records. For small sets of records (1000
 *     or less), deleting them transactionally may perform just as well.
 *     Because cleaner metadata is logged for each call to {@code
 *     Environment.discardExtinctRecords}, using record extinction is not
 *     recommended for discarding many, very small sets of records (for
 *     example, discarding millions of sets of 100 records or less).</li>
 * </ul>
 *
 * <p>Record extinction is intended for optimized record deletion in cases
 * where a large set of records is no longer needed and the keys of the
 * extinct records will not be accessed again. It is the responsibility of the
 * application to ensure that this condition holds. If extinct records are
 * accessed, JE does not provide many of the usual transactional
 * guarantees:</p>
 * <ul>
 *     <li><em>WARNING:</em>If an extinct record is inserted by the
 *     application, it may or may not be discarded by JE. This could appear
 *     as a leakage of memory and disk space.</li>
 *
 *     <li>If an attempt is made by the application to read an extinct record,
 *     the record may or may not be found. Discarding of extinct records is
 *     asynchronous, and is performed independently on each node in a
 *     replication group.</li>
 *
 *     <li>Transactional locking guarantees are not provided for extinct
 *     records. If an extinct record is read (or written) and locked by a
 *     transaction, it may (or may not) appear to be deleted when accessed
 *     again by the same transaction.</li>
 * </ul>
 *
 * <p>Below are examples of applications where keys are never accessed again,
 * and therefore could be considered extinct.</p>
 * <ul>
 *     <li>A single database is used to contain a set of logical "tables" where
 *     the records in a table are identified by a key prefix. Once a table
 *     is dropped, its key prefix is never reused. This is the case for
 *     Oracle NoSQL DB tables and record extinction is used for dropping
 *     tables.</li>
 *
 *     <li>A database is used as a FIFO queue and record keys are sequentially
 *     assigned integers. A range of keys at the "out" end of the queue are
 *     removed after being processed and the keys are never reused. Using
 *     record extinction for removing the processed records would be
 *     beneficial, if the batches of dropped records are large enough to get
 *     performance benefits of record extinction (see above).</li>
 * </ul>
 *
 * <p>Each call to {@code Environment.discardExtinctRecords} specifies a key
 * range, along with an optional filter, in a set of one ore more databases.
 * When the transaction passed to this method is committed (or auto-commit is
 * used), JE will asynchronously remove the specified records from the Btree
 * and count them obsolete to cause log cleaning, which will reclaim unused
 * disk space over time.</p>
 *
 * <p>The {@link #getExtinctionStatus} method in this interface, on the other
 * hand, must return either {@link ExtinctionStatus#EXTINCT} or
 * {@link ExtinctionStatus#MAYBE_EXTINCT} for all record keys specified in
 * <em>all</em> previous calls to {@code Environment.discardExtinctRecords}.
 * This is necessary to avoid integrity errors and is the responsibility of
 * the application. If {@link ExtinctionStatus#NOT_EXTINCT} is returned
 * for a key previously specified as extinct using
 * {@code Environment.discardExtinctRecords}, JE integrity checks may
 * incorrectly fail, and an {@link EnvironmentFailureException} may be
 * thrown.</p>
 *
 * <p>The {@code getExtinctionStatus} method should return
 * {@link ExtinctionStatus#MAYBE_EXTINCT} when
 * {@code Environment.discardExtinctRecords} may have been called for a
 * given key, but the status of the key is temporarily unknown to the
 * application. For example, the application may use metadata stored in a
 * JE Database to determine the extinction status of a record. During
 * startup when this metadata has not yet been read from the Database,
 * {@link ExtinctionStatus#MAYBE_EXTINCT} should be returned.</p>
 *
 * <p>Although not recommended, it is possible to call
 * {@code Environment.discardExtinctRecords} and implement a
 * {@code getExtinctionStatus} method that always returns
 * {@link ExtinctionStatus#MAYBE_EXTINCT}. This will not cause integrity
 * errors, but is likely to result in lower performance and will disable
 * certain internal integrity checks for all records.</p>
 *
 * <p>The reason for lowered performance when
 * {@link ExtinctionStatus#MAYBE_EXTINCT} is returned is that the
 * {@code getExtinctionStatus} method is used by the JE cleaner to quickly
 * purge extinct records without a Btree lookup. When
 * {@code getExtinctionStatus} returns {@link ExtinctionStatus#MAYBE_EXTINCT}
 * for records that are actually extinct, performance will be less than optimal
 * for two reasons.</p>
 * <ul>
 *     <li>The JE cleaner must perform a Btree lookup for each extinct record
 *     before it can be discarded.</li>
 *
 *     <li>Some extinct records may be migrated forward by the cleaner.</li>
 * </ul>
 *
 * <p>For a given set of extinct records, the application should perform
 * actions in the following sequence.</p>
 * <ol>
 *     <li>Stop accessing the extinct records.</li>
 *
 *     <li>Arrange for the {@code ExtinctionFilter.getExtinctionStatus} method
 *     to return {@link ExtinctionStatus#EXTINCT} for the extinct records.</li>
 *
 *     <li>Call {@code Environment.discardExtinctRecords} for the extinct
 *     records.</li>
 * </ol>
 *
 * <p>If step 2 is omitted, or step 2 is performed after step 3, performance
 * will be suboptimal as described above. If step 1 is not performed first,
 * normal transactional guarantees may be violated as described further
 * above.</p>
 *
 * <p>Although record extinction is asynchronous, for testing and debugging
 * the {@link Environment#isRecordExtinctionActive} may be useful for
 * determining when related tasks are complete on a given node.</p>
 *
 * <p>In a replicated environment, {@code Environment.discardExtinctRecords}
 * may not be called until all nodes have been upgraded to JE 18.1 or later,
 * and it will throw {@link IllegalStateException} if this is not the case.
 * The {@link Environment#isRecordExtinctionAvailable()} method can be used
 * for determining this in advance.</p>
 *
 * @see Environment#discardExtinctRecords
 * @see EnvironmentStats#getNLNsExtinct
 * @since 18.1
 */
public interface ExtinctionFilter {

    /*
     * Design Note: The ExtinctionFilter is not used for filtering queries
     * or compression, by design, because the callback implementation could be
     * quite expensive. In KVS, for example, it requires lookup of a table by
     * key and hashing the key to get the partition number. Using it for
     * cleaning is acceptable because cleaning is a less CPU-sensitive task.
     * Also the callback may return MAYBE_EXTINCT as described above, so a
     * trade-off can be made against the cost of Btree lookups.
     */

    /**
     * Extinction status values returned by {@link #getExtinctionStatus}.
     *
     * @see ExtinctionFilter
     */
    enum ExtinctionStatus {

        /**
         * The record with the given key is extinct, i.e., it has been
         * specified as an extinct key when calling
         * {@link Environment#discardExtinctRecords}.
         *
         * @see ExtinctionFilter
         */
        EXTINCT,

        /**
         * The record with the given key is not extinct, i.e., it has not been
         * specified as an extinct key when calling
         * {@link Environment#discardExtinctRecords}.
         *
         * @see ExtinctionFilter
         */
        NOT_EXTINCT,

        /**
         * The record with the given key may or may not be extinct, normally
         * because the status of the key is temporarily unknown to the
         * application.
         *
         * @see ExtinctionFilter
         */
        MAYBE_EXTINCT
    }

    /**
     * Implemented to determine whether a given key is extinct. This method
     * must return {@link ExtinctionStatus#EXTINCT} or
     * {@link ExtinctionStatus#MAYBE_EXTINCT} for all extinct records
     * identified by calls to {@link Environment#discardExtinctRecords}.
     *
     * @param dbName the name of the JE database containing the record.
     *
     * @param dups whether the JE database has duplicate keys, which is
     * assumed to mean it is a secondary database.
     *
     * @param key is the primary key of the record. If dups is true, this is
     * the record's data and is assumed to be a primary key.
     *
     * @return the non-null ExtinctionStatus.
     */
    ExtinctionStatus getExtinctionStatus(String dbName,
                                         boolean dups,
                                         byte[] key);
}
