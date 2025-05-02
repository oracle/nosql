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
import java.util.Map;

/**
 * Summarizes information about a log verification. Returned by some
 * {@link DbVerifyLog} methods and passed to {@link VerifyLogListener#onEnd}.
 *
 * <p>Currently at most one error per file will be detected and reported.
 * Eventually we will add the ability to skip over a corruption and check for
 * additional errors in the same file. At that point, more than one error
 * per file could be reported. We also intend to add a VerifyLogError.getSize
 * method to report the size of the corrupted area.</p>
 *
 * @see DbVerifyLog#verifyAll
 * @see DbVerifyLog#verify
 */
public interface VerifyLogSummary {

    /**
     * Whether any errors were detected.
     */
    boolean hasErrors();

    /**
     * Whether a {@link VerifyLogListener} method returned false to cancel the
     * verification.
     */
    boolean wasCanceled();

    /**
     * Number of files reads, which is the number of calls to
     * {@link VerifyLogListener#onRead}.
     */
    long getFileReads();

    /**
     * Number of bytes read, which is the sum of the bytesRead parameter of
     * {@link VerifyLogListener#onRead}.
     */
    long getBytesRead();

    /**
     * Number of files to be verified, which is the totalFiles parameter of
     * {@link VerifyLogListener#onBegin}.
     */
    int getTotalFiles();

    /**
     * Number of files verified, which is the number of calls to
     * {@link VerifyLogListener#onFile}.
     * This total includes files deleted during verification.
     */
    int getFilesVerified();

    /**
     * Number of files with errors, which is the number of calls to
     * {@link VerifyLogListener#onFile} with a non-empty errors list.
     * This total does not include files deleted during verification.
     */
    int getFilesWithErrors();

    /**
     * Number of files deleted during verification due to log cleaning and
     * disk space reclamation, which is the number of calls to
     * {@link VerifyLogListener#onFile} with a true 'deleted' parameter.
     */
    int getFilesDeleted();

    /**
     * Map of file number to the VerifyLogErrors reported for each file.
     */
    Map<Long, List<VerifyLogError>> getAllErrors();
}
