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

package com.sleepycat.je;

import java.io.Serializable;

import com.sleepycat.je.ExtinctionFilter.ExtinctionStatus;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A Btree verification error detected by {@link Environment#verify}.
 */
public class VerifyError implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Types of problems that are detected during Btree verification.
     *
     * <p>Problems associated with an LSN are described with an
     * {@link #getLsn() LSN} link below. {@link #getLsn()} or
     * {@link #getLsnFile()} may be called to obtain the LSN value.</p>
     *
     * <p>Problems only impacting a single record result in a call to
     * {@link VerifyListener#onRecord} and do not stop further verification of
     * the database. Other problems may prevent further verification of a
     * database and result in a call to {@link VerifyListener#onDatabase}.
     * Problems accessing JE metadata or other misc problems result in a
     * call to {@link VerifyListener#onOtherError}.</p>
     */
    public enum Problem {

        /**
         * Indicates media corruption that happened to be detected while
         * fetching data from disk.
         */
        CHECKSUM_ERROR,

        /**
         * Active data was (incorrectly) erased for privacy reasons.
         */
        ENTRY_ERASED,

        /**
         * {@link #getLsn() LSN} refers to a missing (incorrectly deleted)
         * data file.
         */
        LSN_FILE_MISSING,

        /**
         * {@link #getLsn() LSN} has a file number that is larger than the
         * last file in the Environment, or an offset that is larger than the
         * file size.
         */
        LSN_OUT_OF_BOUNDS,

        /**
         * {@link #getLsn() LSN} was (incorrectly) counted obsolete, which
         * eventually results in {@link #LSN_FILE_MISSING}.
         */
        LSN_COUNTED_OBSOLETE,

        /**
         * {@link #getLsn() LSN} refers to a reserved file, which eventually
         * results in {@link #LSN_FILE_MISSING} it is not repaired. If
         * {@link VerifyConfig#setRepairReservedFiles reserved file repair} is
         * configured, an attempt is made to repair the file.
         */
        LSN_IN_RESERVED_FILE,

        /**
         * {@link #getLsn() LSN} refers to a reserved file that was
         * reactivated due to a {@link #LSN_IN_RESERVED_FILE} problem.
         */
        RESERVED_FILE_REPAIRED,

        /**
         * While verifying a primary database, a reference to a missing
         * secondary database record was detected.
         */
        SECONDARY_KEY_MISSING,

        /**
         * While verifying a secondary database, a reference to a missing
         * primary database record was detected.
         */
        PRIMARY_KEY_MISSING,

        /**
         * While verifying a secondary database, a reference to a missing
         * foreign database record was detected.
         */
        FOREIGN_KEY_MISSING,

        /**
         * While verifying secondary references a lock conflict occurred,
         * preventing further verification of the record.
         */
        SECONDARY_LOCK_CONFLICT,

        /**
         * The expiration time in the BIN (Btree bottom internal node) does not
         * match the expiration time in the LN (leaf node).
         */
        EXPIRATION_TIME_MISMATCH,

        /**
         * The {@link ExtinctionFilter} returned
         * {@link ExtinctionStatus#MAYBE_EXTINCT} for a record's primary key,
         * preventing verification of the record's LSN.
         */
        MAYBE_EXTINCT,

        /**
         * An unexpected exception occurred during verification of a record,
         * preventing further verification of the record.
         */
        RECORD_ACCESS_EXCEPTION,

        /**
         * An unexpected exception occurred accessing the Btree, preventing
         * further verification of the database. This error is passed to
         * {@link VerifyListener#onDatabase}.
         */
        DATABASE_ACCESS_EXCEPTION,

        /**
         * An invalid Btree node was detected, preventing further verification
         * of the database. This error is passed to
         * {@link VerifyListener#onDatabase}.
         */
        INTERNAL_NODE_INVALID,

        /**
         * refers to a dangling ln that was repaired by deletion of it 
         * from the BIN.
         */
        DANGLING_USER_LSN_REPAIRED,
    }

    private final Problem problem;
    private final String msg;
    private final long lsn;

    /**
     * Used internally.
     */
    public VerifyError(
        final Problem problem,
        final String msg,
        final long lsn) {

        this.problem = problem;
        this.msg = msg;
        this.lsn = lsn;
    }

    /**
     * Returns the problem identifier.
     */
    public Problem getProblem() {
        return problem;
    }

    /**
     * Returns a longer message describing the specific problem.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * Returns the LSN (log sequence number) associated with the problem, or
     * negative one if no LSN is present.
     */
    public long getLsn() {
        return lsn;
    }

    /**
     * Returns the data file number of the LSN, or negative one if no LSN is
     * present.
     */
    public long getLsnFile() {
        return lsn == DbLsn.NULL_LSN ? -1 : DbLsn.getFileNumber(lsn);
    }

    /**
     * Returns whether the Problem is severe and could cause corruption or
     * invalidation of the env, as opposed to an internal warning such as when
     * verification cannot be completed or a repair is performed.
     */
    public boolean isSevere() {
        switch (problem) {
            case RESERVED_FILE_REPAIRED:
            case SECONDARY_LOCK_CONFLICT:
            case MAYBE_EXTINCT:
                return false;
            default:
                return true;
        }
    }

    @Override
    public String toString() {
        return "[VerifyError" +
            " problem=" + problem +
            " lsn=" + DbLsn.getNoFormatString(lsn) +
            " msg=" + msg +
            "]";
    }
}
