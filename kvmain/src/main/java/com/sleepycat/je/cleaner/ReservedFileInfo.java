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

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.utilint.VLSN;

/**
 */
public class ReservedFileInfo {
    public final long firstVLSN;
    public final long lastVLSN;

    ReservedFileInfo(final long firstVLSN,
                     final long lastVLSN) {
        this.firstVLSN = firstVLSN;
        this.lastVLSN = lastVLSN;
    }

    public static Long entryToKey(final DatabaseEntry entry) {
        return LongBinding.entryToLong(entry);
    }

    public static void keyToEntry(Long key, final DatabaseEntry entry) {
        LongBinding.longToEntry(key, entry);
    }

    public static ReservedFileInfo entryToObject(final DatabaseEntry entry) {
        final TupleInput input = TupleBase.entryToInput(entry);
        input.readByte(); /* Future flags. */
        final long firstVLSN = input.readPackedLong();
        final long lastVLSN = input.readPackedLong();

        /* DB IDs are no longer used, just skip over them. */
        final int nDbs = input.readPackedInt();
        for (int i = 0; i < nDbs; i += 1) {
            input.readPackedLong();
        }

        return new ReservedFileInfo(firstVLSN, lastVLSN);
    }

    public static void objectToEntry(final ReservedFileInfo info,
                                     final DatabaseEntry entry) {
        final TupleOutput output = new TupleOutput();
        output.writeByte(0); /* Future flags. */
        output.writePackedLong(info.firstVLSN);
        output.writePackedLong(info.lastVLSN);

        /* DB IDs are no longer used, just write a zero size. */
        output.writePackedInt(0);

        TupleBase.outputToEntry(output, entry);
    }
}
