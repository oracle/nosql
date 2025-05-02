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
package com.sleepycat.je.log;

import com.sleepycat.je.utilint.DbLsn;

/**
 * Indicates that an erased entry was encountered when fetching a log entry,
 * presumably because its LSN appears in the active Btree. A checked exception
 * is used so it can be caught and handled internally in some cases.  When
 * not handled internally, it is wrapped with an EnvironmentFailureException
 * with EnvironmentFailureReason.LOG_ERASED before being propagated through
 * the public API.
 */
public class ErasedException extends Exception {

    private static final long serialVersionUID = 1;

    public ErasedException(final long lsn,
                           final LogEntryHeader header) {
        super("Erased entry at " + DbLsn.getNoFormatString(lsn) +
                " header: " + header);
    }
}
