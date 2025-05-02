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

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.DbBackup;

/**
 * Thrown by {@link DbBackup#startBackup()} if we can't abort erasure within
 * {@link EnvironmentParams#ERASE_ABORT_TIMEOUT}. The timeout is long, so
 * this should not happen unless the eraser thread is wedged or starved.
 *
 * <p>To handle the case where the erasure thread is starved (rather than
 * wedged) or perhaps ERASE_ABORT_TIMEOUT_MS is too small, the backup may be
 * retried.</p>
 *
 * <p>This exception is undocumented because erasure is not an exposed
 * feature.</p>
 */
public class EraserAbortException extends RuntimeException {

    public EraserAbortException(final String msg) {
        super(msg);
    }
}
