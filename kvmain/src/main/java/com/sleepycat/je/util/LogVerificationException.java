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

import java.io.IOException;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * Thrown during log verification if a checksum cannot be verified or a log
 * entry is determined to be invalid by examining its contents.
 *
 * <p>Note that when this exception is thrown, the Environment is not
 * invalidated and a marker file is not created (as described under
 * {@link EnvironmentFailureException#isCorrupted}).</p>
 *
 * <p>This class extends {@code IOException} so that it can be thrown by the
 * {@code InputStream} methods of {@link LogVerificationInputStream}.</p>
 */
public class LogVerificationException extends IOException {
    private static final long serialVersionUID = 1L;

    private long lsn;

    public LogVerificationException(final String message) {
        super(message);
    }

    public LogVerificationException(final String message,
                                    final long lsn,
                                    final Throwable cause) {
        super(message);
        this.lsn = lsn;
        initCause(cause);
    }

    public long getLsn() {
        return lsn;
    }
}
