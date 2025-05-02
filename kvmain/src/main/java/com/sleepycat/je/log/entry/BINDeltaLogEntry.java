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

package com.sleepycat.je.log.entry;

import com.sleepycat.je.tree.BIN;

/**
 * Holds a partial BIN that serves as a live BIN delta.
 *
 * A live delta may appear in the Btree to serve as an incomplete BIN.
 */
public class BINDeltaLogEntry extends INLogEntry<BIN> {

    public BINDeltaLogEntry(Class<BIN> logClass) {
        super(logClass);
    }

    /**
     * When constructing an entry for writing to the log, use LOG_BIN_DELTA.
     */
    public BINDeltaLogEntry(BIN bin) {
        super(bin, true /*isBINDelta*/);
    }

    /*
     * Whether this LogEntry reads/writes a BIN-Delta logrec.
     */
    @Override
    public boolean isBINDelta() {
        return true;
    }
}
