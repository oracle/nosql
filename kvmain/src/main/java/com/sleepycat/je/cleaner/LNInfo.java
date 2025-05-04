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

package com.sleepycat.je.cleaner;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.tree.LN;

/**
 * The information necessary to lookup an LN.  Used for pending LNs that are
 * locked and must be migrated later, or cannot be migrated immediately during
 * a split.  Also used in a look ahead cache in FileProcessor.
 *
 * Is public for Sizeof only.
 */
public final class LNInfo {

    private final LN ln;
    private final DatabaseId dbId;
    private final byte[] key;
    private final long expirationTime;
    private final long modificationTime;
    private final int headerSize;
    private final int itemSize;

    public LNInfo(final LN ln,
                  final DatabaseId dbId,
                  final byte[] key,
                  final long expirationTime,
                  final long modificationTime,
                  int headerSize,
                  int itemSize) {
        this.ln = ln;
        this.dbId = dbId;
        this.key = key;
        this.expirationTime = expirationTime;
        this.modificationTime = modificationTime;
        this.headerSize = headerSize;
        this.itemSize = itemSize;
    }

    LN getLN() {
        return ln;
    }

    DatabaseId getDbId() {
        return dbId;
    }

    byte[] getKey() {
        return key;
    }

    long getExpirationTime() {
        return expirationTime;
    }

    long getModificationTime() {
        return modificationTime;
    }

    int getHeaderSize() { return headerSize; }

    int getItemSize() { return itemSize; }

    /**
     * Note that the dbId is not counted because it is shared with the
     * DatabaseImpl, where it is accounted for in the memory budget.
     */
    int getMemorySize() {
        int size = MemoryBudget.LN_INFO_OVERHEAD;
        if (ln != null) {
            size += ln.getMemorySizeIncludedByParent();
        }
        if (key != null) {
            size += MemoryBudget.byteArraySize(key.length);
        }
        return size;
    }
}
