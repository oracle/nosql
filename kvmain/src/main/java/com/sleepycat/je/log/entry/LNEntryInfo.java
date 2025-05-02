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

import com.sleepycat.je.DatabaseEntry;

/**
 * Information about an LNLogEntry that may reside in a buffer in serialized
 * form.
 *
 * <p>Used to obtain information about the LNLogEntry without having to incur
 * the costs (mainly memory allocations) of fully instantiating it.</p>
 *
 * @see LNLogEntry#parseEntry
 * @since 19.5
 */
public class LNEntryInfo {

    /**
     * The ID of the database containing the log entry. A valid ID (GTE 0) is
     * always available for an LN.
     */
    public long databaseId;

    /**
     * The ID of the transaction when one is present in the log entry, or -1
     * if no transaction is present. A transaction is always present in a
     * replicated entry. Transactions are removed from log entries during
     * cleaner migration, but such entries are not replicated.
     */
    public long transactionId;

    /**
     * The last modification time of the log entry, or zero if the LN belongs
     * to a secondary (duplicates) database or was originally written using
     * JE 19.3 or earlier.
     */
    public long modificationTime;

    /**
     * The tombstone property of the record.
     */
    public boolean tombstone;

    /**
     * The byte array containing the record key.
     */
    public byte[] key;

    /**
     * The offset of the key in the array.
     */
    public int keyOffset;

    /**
     * The length of the key in the array.
     */
    public int keyLength;

    /**
     * The byte array containing the record data.
     */
    public byte[] data;

    /**
     * The offset of the data in the array.
     */
    public int dataOffset;

    /**
     * The length of the data in the array.
     */
    public int dataLength;

    public void getKey(final DatabaseEntry entry) {
        entry.setData(key, keyOffset, keyLength);
    }

    public void getData(final DatabaseEntry entry) {
        entry.setData(data, dataOffset, dataLength);
    }
}
