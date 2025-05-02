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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.utilint.StringUtils;

public class MetadataStore {

    /** Metadata record keys are predefined here. */
    public static final String KEY_ERASER = "eraser";

    private final EnvironmentImpl envImpl;
    private DatabaseImpl db;

    MetadataStore(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    public synchronized void openDb() {

        if (db != null) {
            return;
        }

        db = envImpl.getDbTree().openNonRepInternalDB(DbType.METADATA);
    }

    public OperationResult get(final String key, final DatabaseEntry data) {

        openDb();

        final DatabaseEntry keyEntry =
            new DatabaseEntry(StringUtils.toUTF8(key));

        final Locker locker = BasicLocker.createBasicLocker(
            envImpl, false /*noWait*/);

        try (final Cursor cursor = DbInternal.makeCursor(db, locker, null)) {
            return cursor.get(keyEntry, data, Get.NEXT, null);
        } finally {
            locker.operationEnd();
        }
    }

    public OperationResult put(final String key, final DatabaseEntry data) {

        openDb();

        final DatabaseEntry keyEntry =
            new DatabaseEntry(StringUtils.toUTF8(key));

        final Locker locker = BasicLocker.createBasicLocker(
            envImpl, false /*noWait*/);

        try (final Cursor cursor = DbInternal.makeCursor(db, locker, null)) {
            return cursor.put(keyEntry, data, Put.OVERWRITE, null);
        } finally {
            locker.operationEnd();
        }
    }
}
