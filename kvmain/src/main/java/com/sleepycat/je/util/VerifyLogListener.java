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

package com.sleepycat.je.util;

import java.util.List;

import com.sleepycat.je.Environment;

/**
 * Can be used during log verification to monitor progress, and to cancel
 * verification prematurely.
 *
 * @see DbVerifyLog#DbVerifyLog(Environment, int, VerifyLogListener)
 */
public interface VerifyLogListener {

    /**
     * Called before verification starts.
     *
     * @param totalFiles is the number of files to be verified.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * not called.
     */
    boolean onBegin(int totalFiles);

    /**
     * Called when verification ends for any reason.
     *
     * @param summary is the same object returned by the verify method.
     */
    void onEnd(VerifyLogSummary summary);

    /**
     * Called after each file read.
     *
     * @param file the file number.
     *
     * @param bytesRead is the non-negative number of bytes read.
     */
    boolean onRead(long file, int bytesRead);

    /**
     * Called after verifying a file.
     *
     * @param file the file number.
     *
     * @param errors the errors detected for the given file. Note that errors
     * is empty if deleted is true.
     *
     * @param deleted is true if the file was deleted during verification
     * due to log cleaning and disk space reclamation. Note that errors is
     * empty if deleted is true.
     *
     * @return false to cancel verification, in which case {@link #onEnd} is
     * called.
     */
    boolean onFile(long file, List<VerifyLogError> errors, boolean deleted);
}
