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

import java.io.Serializable;

import com.sleepycat.je.utilint.DbLsn;

/**
 * A log verification error (checksum error) detected by {@link DbVerifyLog}.
 */
public class VerifyLogError implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String msg;
    private final long lsn;

    /**
     * For internal use.
     */
    public VerifyLogError(
        final String msg,
        final long lsn) {

        this.msg = msg;
        this.lsn = lsn;
    }

    /**
     * Returns a longer message describing the error.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * Returns the number of the file containing the error.
     */
    public long getFile() {
        return DbLsn.getFileNumber(lsn);
    }

    /**
     * Returns the byte offset of the error within the file.
     */
    public long getOffset() {
        return DbLsn.getFileOffset(lsn);
    }

    /**
     * Returns the LSN (log sequence number) representing the file number and
     * offset..
     */
    public long getLsn() {
        return lsn;
    }

    @Override
    public String toString() {
        return "[VerifyLogError" +
            " lsn=" + DbLsn.getNoFormatString(lsn) +
            " msg=" + msg +
            "]";
    }
}
