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

package com.sleepycat.je.test.util;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Get;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VerifyError;
import com.sleepycat.je.VerifyError.Problem;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.util.DbVerifyLog;
import com.sleepycat.je.util.VerifyLogError;
import com.sleepycat.je.util.VerifyLogSummary;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Utilities for creating log corruption and Btree corruption. NOTE: these
 * utilities are shared by JE and KVS, so breaking API changes need to be
 * avoided or coordinated with the KVS team.
 */
public class Corruption {

    private static final VerifyConfig VERIFY_CONFIG = new VerifyConfig();
    static {
        VERIFY_CONFIG.setVerifyDataRecords(true);
        VERIFY_CONFIG.setVerifySecondaries(true);
        VERIFY_CONFIG.setVerifyObsoleteRecords(true);
        VERIFY_CONFIG.setBatchDelay(0, null);
    }

    /**
     * Creates a {@link VerifyLogError} corruption in the middle of each of
     * the specified files by overwriting one byte.
     *
     * @return a list containing the LSN of each corrupted location. The
     * offset of the LSN is simply the middle byte of the file so it will not
     * correspond to the start of a log entry and will not correspond to the
     * LSN reported by log verification. So it is not all that useful.
     */
    public static List<Long> corruptLog(final File envHome,
                                        final long... fileNums)
        throws IOException {

        final List<Long> lsns = new ArrayList<>(fileNums.length);

        for (long fileNum : fileNums) {
            final File file =
                new File(envHome, FileManager.getFileName(fileNum));

            final RandomAccessFile rafile =
                new RandomAccessFile(file, "rw");

            final long offset = rafile.length() / 2;
            rafile.seek(offset);
            final byte b = (byte) (rafile.readByte() + 1);
            rafile.seek(offset);
            rafile.writeByte(b);
            rafile.close();

            lsns.add(DbLsn.makeLsn(fileNum, offset));
        }

        return lsns;
    }

    /**
     * Creates an {@link Problem#LSN_OUT_OF_BOUNDS} in the given database by
     * setting a record's LSN to an out-of-bounds value.
     *
     * @param recordKey a record key at which to create the problem, or null
     * to use the second record in the database.
     *
     * @return the corrupt LSN that will be returned by
     * {@link VerifyError#getLsn()}.
     */
    public static long corruptLSNOutOfBounds(final Environment env,
                                             final String dbName,
                                             final byte[] recordKey) {
        final long lsn = DbLsn.makeLsn(Integer.MAX_VALUE, 0x100);
        corruptBtree(env, dbName, recordKey, cursorImpl -> {
            final BIN bin = cursorImpl.getBIN();
            final int idx = cursorImpl.getIndex();
            bin.setLsn(idx, lsn);
        });
        return lsn;
    }

    /**
     * Creates an {@link Problem#INTERNAL_NODE_INVALID} in the given database
     * by reversing the key order of records within a BIN.
     */
    public static void corruptINKeyOrder(final Environment env,
                                         final String dbName) {
        corruptBtree(env, dbName, null, cursorImpl -> {
            final BIN bin = cursorImpl.getBIN();
            final int idx = cursorImpl.getIndex();
            final byte[] key1 = bin.getKey(idx);
            final byte[] key2 = bin.getKey(idx + 1);
            bin.setKey(idx, key2, null, false);
            bin.setKey(idx + 1, key1, null, false);
        });
    }

    /**
     * Creates an {@link Problem#INTERNAL_NODE_INVALID} in the given database
     * by setting the BIN's identifier key to a value that is not present in
     * any of the BIN's records.
     */
    public static void corruptIdentifierKey(final Environment env,
                                            final String dbName) {
        corruptBtree(env, dbName, null, cursorImpl -> {
            final BIN bin = cursorImpl.getBIN();
            final byte[] maxKey = bin.getKey(bin.getNEntries() - 1);
            final int len = maxKey.length;
            final byte[] newIdenKey = new byte[(len + 1)];
            System.arraycopy(maxKey, 0, newIdenKey, 0, len);
            newIdenKey[len] = 100;
            bin.setIdentifierKey(newIdenKey, true);
        });
    }

    /**
     * Opens the given database and positions the cursor at the given record,
     * and calls the corrupter with the cursor. Makes the corruption persistent
     * by performing a checkpoint.
     *
     * @param recordKey a record key at which to create the problem, or null
     * to use the second record in the database.
     *
     * @param corrupter a callback to corrupt the BIN using the current
     * cursor position.
     */
    private static void corruptBtree(final Environment env,
                                     final String dbName,
                                     final byte[] recordKey,
                                     final Consumer<CursorImpl> corrupter) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final DatabaseConfig dbConfig = new DatabaseConfig()
            .setUseExistingConfig(true)
            .setTransactional(
                DbInternal.getEnvironmentImpl(env).isTransactional());

        try (final Database db = env.openDatabase(null, dbName, dbConfig)) {
            try (final Cursor cursor = db.openCursor(null, null)) {

                if (recordKey != null) {
                    key.setData(recordKey);
                    assertNotNull(
                        "Key not found: " + Arrays.toString(recordKey) +
                            " in " + dbName,
                        cursor.get(key, data, Get.SEARCH, null));
                } else {
                    assertNotNull(
                        dbName + " is empty",
                        cursor.get(key, data, Get.FIRST, null));
                    assertNotNull(
                        dbName + " does not have at least two records" +
                            " in the first BIN",
                        cursor.get(key, data, Get.NEXT, null));
                }

                final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
                cursorImpl.latchBIN();
                try {
                    corrupter.accept(cursorImpl);
                } finally {
                    cursorImpl.releaseBIN();
                }
            }
        }

        final CheckpointConfig cc = new CheckpointConfig();
        cc.setForce(true);
        env.checkpoint(cc);
        env.flushLog(false);
    }

    public static void verifyAll(final Environment env) {
        final DbVerifyLog verifyLog = new DbVerifyLog(env);
        final VerifyLogSummary logSummary;
        try {
            logSummary = verifyLog.verifyAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (logSummary.hasErrors()) {
            throw new RuntimeException(
                "Node: " + DbInternal.getEnvironmentImpl(env).getName() +
                    " " + logSummary);
        }
        final VerifySummary btreeSummary = env.verify(VERIFY_CONFIG);
        if (btreeSummary.hasErrors()) {
            throw new RuntimeException(
                "Node: " + DbInternal.getEnvironmentImpl(env).getName() +
                    " " + btreeSummary);
        }
    }
}
