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

package com.sleepycat.je.dbi;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.LN;

/**
 * Classifies all databases as specific internal databases or user databases.
 * This can be thought of as a substitute for having DatabaseImpl subclasses
 * for different types of databases.  It also identifies each internal database
 * by name.
 */
public enum DbType {

    ID("IdMap") {
        @Override
        public boolean mayCreateDeletedLN() {
            return false;
        }
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            throw EnvironmentFailureException.unexpectedState();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
    },

    NAME("NameMap") {
        @Override
        public boolean mayCreateDeletedLN() {
            return false;
        }
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            throw EnvironmentFailureException.unexpectedState();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
        @Override
        public boolean isMixedReplication() {
            return true;
        }
    },

    UTILIZATION("Utilization") {
        @Override
        public LN createDeletedLN(EnvironmentImpl envImpl) {
            return FileSummaryLN.makeDeletedLN();
        }
        @Override
        public boolean mayCreateUpdatedLN() {
            return false;
        }
        @Override
        public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
            throw EnvironmentFailureException.unexpectedState();
        }
    },

    EXPIRATION("Expiration"),

    REP_GROUP("RepGroupDB"),

    VLSN_MAP("VlsnMapDb"),

    SYNC("SyncDb"),

    RESERVED_FILES("ReservedFilesDb") {
        @Override
        public LogEntryType getLogType(boolean isTransactional) {
            return LogEntryType.LOG_RESERVED_FILE_LN;
        }
    },

    EXTINCT_SCANS("ExtinctScansDb") {
        @Override
        public LogEntryType getLogType(boolean isTransactional) {
            return isTransactional ?
                LogEntryType.LOG_EXTINCT_SCAN_LN_TRANSACTIONAL :
                LogEntryType.LOG_EXTINCT_SCAN_LN;
        }
        @Override
        public boolean isMixedReplication() {
            return true;
        }
    },

    BEFORE_IMAGE("BeforeImagesDb") {
        @Override
        public boolean isMixedTransactional() {
            return true;
        }
    },

    METADATA("Metadata"),

    USER(null);

    private final String internalName;

    DbType(String internalSuffix) {
        internalName = (internalSuffix != null) ?
            (DbTree.INTERNAL_DB_NAME_PREFIX + internalSuffix) :
            null;
    }

    /**
     * Returns true if this is an internal DB, or false if it is a user DB.
     */
    public boolean isInternal() {
        return internalName != null;
    }

    /**
     * Returns the DB name for an internal DB type.
     *
     * @throws EnvironmentFailureException if this is not an internal DB type.
     */
    public String getInternalName() {
        if (internalName == null) {
            throw EnvironmentFailureException.unexpectedState();
        }
        return internalName;
    }

    /**
     * Returns true for certain internal DBs that support a mixture of
     * replicated and non-replicated records.
     */
    public boolean isMixedReplication() {
        return false;
    }

    /**
     * Returns true for before image database that support a same locker being
     * reused for the local write and replicated write 
     */
    public boolean isMixedTransactional() {
        return false;
    }

    /**
     * Returns true if createUpdatedLN may be called.
     */
    public boolean mayCreateUpdatedLN() {
        return true;
    }

    /**
     * Creates an updated LN for use in an optimization in
     * CursorImpl.putCurrentAlreadyLatchedAndLocked.  Without this method it
     * would be necessary to fetch the existing LN and call LN.modify.
     *
     * Does NOT copy the byte array, so after calling this method the array is
     * "owned" by the Btree and should not be modified.
     *
     * @throws EnvironmentFailureException if this is not allowed.
     */
    public LN createUpdatedLN(EnvironmentImpl envImpl, byte[] newData) {
        return LN.makeLN(envImpl, newData);
    }

    /**
     * Returns true if createDeletedLN may be called.
     */
    public boolean mayCreateDeletedLN() {
        return true;
    }

    /**
     * Creates a deleted LN for use in an optimization in CursorImpl.delete.
     * Without this method it would be necessary to fetch the existing LN and
     * call LN.delete.
     *
     * @throws EnvironmentFailureException if this is not allowed.
     */
    public LN createDeletedLN(EnvironmentImpl envImpl) {
        return LN.makeLN(envImpl, (byte[]) null);
    }

    /**
     * Returns the LogEntryType for LNs in this DB, or null if the usual user
     * LN types should be used.
     */
    public LogEntryType getLogType(boolean isTransactional) {
        return null;
    }
}
