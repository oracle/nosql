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

package com.sleepycat.je.beforeimage;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.tree.UncachedLN;

/**
 * A BeforeImageLN represents a Leaf Node in the {@literal name->database} id
 * mapping tree.
 */
public final class BeforeImageLN extends UncachedLN {

    // TODO change the name to something more clearer
    private static final String BEGIN_TAG = "<beforeImageLN>";
    private static final String END_TAG = "</beforeImageLN>";

    public BeforeImageLN(byte[] data) {
        super(data);
    }

    public BeforeImageLN(DatabaseEntry entry) {
        super((entry != null) ? entry.getData() : null);
    }

    public BeforeImageLN() {
        super();
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    @Override
    public long getMemorySizeIncludedByParent() {
        /**
         * Reuse the LN method here as the parent is uncachedLN which throws
         * EFE
         */
        int size = MemoryBudget.LN_OVERHEAD;
        if (getData() != null) {
            size += MemoryBudget.byteArraySize(getDataSize());
        }
        return size;
    }

    /**
     * Return the correct log entry type for a BeforeImageLN depends on whether
     * it's transactional.
     */
    @Override
    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional,
                                      DatabaseImpl db) {

        if (isDeleted()) {
            assert !isInsert;
            return isTransactional
                ? LogEntryType.LOG_DEL_LN_TRANSACTIONAL_WITH_BEFORE_IMAGE
                : LogEntryType.LOG_DEL_LN;
        }

        if (isInsert) {
            return isTransactional ? LogEntryType.LOG_INS_LN_TRANSACTIONAL
                : LogEntryType.LOG_INS_LN;
        }

        return isTransactional
            ? LogEntryType.LOG_UPD_LN_TRANSACTIONAL_WITH_BEFORE_IMAGE
            : LogEntryType.LOG_UPD_LN;
    }

    /**
     * Each LN knows what kind of log entry it uses to log itself
     */
    @Override
    protected LNLogEntry<?> createLogEntry(LogEntryType entryType,
                                           DatabaseImpl dbImpl,
                                           Txn txn,
                                           long abortLsn,
                                           boolean abortKD,
                                           byte[] abortKey,
                                           byte[] abortData,
                                           long abortVLSN,
                                           int abortExpiration,
                                           boolean abortExpirationInHours,
                                           long abortModificationTime,
                                           long abortCreationTime,
                                           boolean abortTombstone,
                                           byte[] newKey,
                                           boolean newEmbeddedLN,
                                           int newExpiration,
                                           boolean newExpirationInHours,
                                           long creationTime,
                                           long newModTime,
                                           boolean newTombstone,
                                           boolean newBlindDeletion,
                                           int priorSize,
                                           long priorLsn,
                                           ReplicationContext repContext,
                                           BeforeImageContext bImgCtx) {

        return new BeforeImageLNLogEntry(entryType, dbImpl.getId(), txn,
                                         abortLsn, abortKD, abortKey, abortData,
                                         abortVLSN,
                                         abortExpiration,
                                         abortExpirationInHours,
                                         abortModificationTime,
                                         abortCreationTime,
                                         abortTombstone, newKey, this,
                                         newEmbeddedLN, newExpiration,
                                         newExpirationInHours, creationTime, newModTime,
                                         newTombstone,
                                         newBlindDeletion, priorSize, priorLsn,
                                         bImgCtx);
    }
}
