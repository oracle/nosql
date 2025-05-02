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

package com.sleepycat.je.tree;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;

/**
 * Used for logging an LN without copying the user's data array, when the LN
 * will not be cached in the BIN.
 */
public class UncachedLN extends LN {

    private final DatabaseEntry entry;

    public UncachedLN(byte[] data) {
        super(data);
        this.entry = null;
    }

    public UncachedLN() {
        super();
        this.entry = null;
    }

    public UncachedLN(DatabaseEntry entry) {
        super((entry != null) ? entry.getData() : null);
        this.entry = entry;
    }

    @Override
    public int getDataOffset() {
        return (entry != null) ? entry.getOffset() : super.getDataOffset();
    }

    @Override
    public int getDataSize() {
        return (entry != null) ? entry.getSize() : super.getDataSize();
    }

    @Override
    public long getMemorySizeIncludedByParent() {
        throw EnvironmentFailureException.unexpectedState();
    }
}
