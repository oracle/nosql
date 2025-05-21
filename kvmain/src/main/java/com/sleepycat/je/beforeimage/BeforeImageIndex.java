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

package com.sleepycat.je.beforeimage;

import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * {@literal
 * This class manages the beforeimage database which stores previous records of
 * all the database records configured with enableBeforeImage 
 * }
 */
public class BeforeImageIndex {

    protected final EnvironmentImpl envImpl;

    private final Logger logger = LoggerUtils.getLogger(getClass());

    /*
     * For storing the persistent version of the BeforeImageIndex. the key is
     * the abortLSN of the inserted data , data is previous image of the data.
     */
    private volatile DatabaseImpl beforeImageDbImpl;

    /*
     * Statistics associated with the BeforeImage index
     */
    private final StatGroup statistics;

    private IntStat nBImgRecords;

    private LongStat sBImgDataSize;

    private IntStat nBImgByUpds;

    private IntStat nBImgByDels;

    private IntStat nBImgByTombs;

    private static TestHook<?> beforeImageHook;

    public static class DBEntry {
        private long abortLsn;
        private int expTime;
        private DatabaseEntry data;
        private Locker lck;
        private boolean expInHours;
        BeforeImageContext bImgCtx;
        private PutContext pCtx;

        public static enum PutContext {
            UPDATE, DELETE, TOMBSTONE
        }

        public DBEntry(final Locker lck, final long abortLsn,
                final DatabaseEntry data, BeforeImageContext bImgCtx,
                PutContext pCtx) {
            this.abortLsn = abortLsn;
            this.data = data;
            this.lck = lck;
            this.expTime = bImgCtx.getExpTime();
            this.expInHours = bImgCtx.isExpTimeInHrs();
            this.pCtx = pCtx;
        }

        public long getAbortLsn() {
            return abortLsn;
        }

        public int getExpTime() {
            return expTime;
        }

        public DatabaseEntry getData() {
            return data;
        }

        public Locker getLock() {
            return lck;
        }

        public boolean isExpInHours() {
            return expInHours;
        }

        public PutContext getPutCtx() {
            return pCtx;
        }
    }

    public static class BeforeImagePayLoad {

        /*this is the data stored as value 
         * in beforeimage database
         */
        private final byte[] bImgData;
        private final long modTime;
        private int version;

        public BeforeImagePayLoad(Builder builder) {
            this.bImgData = builder.bImgData;
            this.modTime = builder.modTime;
            this.version = builder.version;
        }

        public int getVersion() {
            return version;
        }

        public long getModTime() {
            return modTime;
        }

        public byte[] getbImgData() {
            return bImgData;
        }

        public byte[] marshalData() {
            int totLength = Integer.BYTES + bImgData.length + Long.BYTES;
            // totLength += version >= 26 ? Long.BYTES 
            ByteBuffer buf = ByteBuffer.allocate(totLength);
            LogUtils.writePackedInt(buf, version);
            LogUtils.writeByteArray(buf, bImgData);
            LogUtils.writePackedLong(buf, modTime);
            /*
               if (version  >= 26) {
               write new data
               }
             */
            return buf.array();
        }

        public static BeforeImagePayLoad unMarshalData(byte[] data) {
            if (data == null) {
                return null;
            }
            ByteBuffer buf = ByteBuffer.wrap(data);
            int version = LogUtils.readPackedInt(buf);
            byte[] bImgData = LogUtils.readByteArray(buf);
            long modTime = LogUtils.readPackedLong(buf);
            /*
             * if (version >= 26) {
             *     read new data and set below in the builder using setters
             * }
             */
            return new BeforeImagePayLoad.Builder(bImgData, modTime)
                .setVersion(version).build();
        }

        public static class Builder {
            private int version;
            private byte[] bImgData;
            private long modTime;
            public Builder(byte[] bImgData, long modTime) {
                this.bImgData = bImgData;
                this.modTime = modTime;
            }
            public Builder setVersion(int version) {
                this.version = version;
                return this;
            }
            /*
             * template to add new field 
             * public Builder setBImgTxnId(long txnId) { 
             * this.txnId = txnId;
             *      return this;
             * }
             */
            public BeforeImagePayLoad build() {
                if (version == 0) {
                    /*
                     * Feature introduced version 
                     * TODO map this to logentryversion?
                     */
                    version = 25;
                }
                return new BeforeImagePayLoad(this);
            }
        }
    }

    private Cursor makeCursor(Locker locker) {
        assert beforeImageDbImpl != null;
        Cursor cursor = DbInternal.makeCursor(beforeImageDbImpl, locker,
                CursorConfig.DEFAULT);
        DbInternal.getCursorImpl(cursor).setAllowEviction(false);
        return cursor;
    }

    public BeforeImageIndex(EnvironmentImpl envImpl) throws DatabaseException {

        this.envImpl = envImpl;
        statistics = new StatGroup(BeforeImageIndexStatDefinition.GROUP_NAME,
                BeforeImageIndexStatDefinition.GROUP_DESC);
        nBImgRecords = new IntStat(statistics,
                BeforeImageIndexStatDefinition.N_BIMG_RECORDS);
        sBImgDataSize = new LongStat(statistics,
                BeforeImageIndexStatDefinition.S_BIMG_DATA_SIZE);
        nBImgByUpds = new IntStat(statistics,
                BeforeImageIndexStatDefinition.N_BIMG_RECORDS_BY_UPDATES);
        nBImgByDels = new IntStat(statistics,
                BeforeImageIndexStatDefinition.N_BIMG_RECORDS_BY_DELETES);
        nBImgByTombs = new IntStat(statistics,
                BeforeImageIndexStatDefinition.N_BIMG_RECORDS_BY_TOMBSTONES);

    }

    public static void setBeforeImageHook(TestHook<?> hook) {
        beforeImageHook = hook;
    }

    /**
     * Returns the statistics associated with the BeforeImageIndex
     *
     * @return the BeforeImageIndex statistics.
     */

    public StatGroup getStats(StatsConfig config) {
        return statistics.cloneGroup(config.getClear());
    }

    /**
     * Opens the BeforeImage Internal Database. It was created by the first
     * operation which needs the beforeimage and subsequent operations makes use
     * of the same beforeImagedatabase.
     *
     * We need to keep it synchronized as it was created as part of user
     * operation which can be concurrent.
     *
     */
    private synchronized void openBeforeImageDatabase()
        throws DatabaseException {

            // We cannot use the locker from user operation to avoid issues
            // when other user, get the db, and the user operation which
            // created the db was aborted. isn't this simpler to just initialize
            // after env creation when we are single threaded.
            final Locker locker = Txn.createLocalAutoTxn(envImpl,
                    new TransactionConfig());

            try {
                DbTree dbTree = envImpl.getDbTree();
                DatabaseImpl db = dbTree.getDb(locker,
                        DbType.BEFORE_IMAGE.getInternalName(),
                        null /* databaseHandle */, false);
                if (db == null) {
                    if (envImpl.isReadOnly()) {
                        /* This should have been caught earlier. */
                        throw EnvironmentFailureException.unexpectedState(
                                "A replicated environment can't be opened read only.");
                    }
                    DatabaseConfig dbConfig = new DatabaseConfig();
                    dbConfig.setReplicated(false);
                    db = dbTree.createInternalDb(locker,
                            DbType.BEFORE_IMAGE.getInternalName(), dbConfig);
                }
                beforeImageDbImpl = db;
            } finally {
                locker.operationEnd(true);
            }
        }

    private void updateStats(final DBEntry entry) {
        nBImgRecords.increment();
        sBImgDataSize
            .add(entry.getData() != null ? entry.getData().getSize() : 0);
        switch (entry.getPutCtx()) {
            case UPDATE:
                nBImgByUpds.increment();
                break;
            case DELETE:
                nBImgByDels.increment();
                break;
            case TOMBSTONE:
                nBImgByTombs.increment();
                break;
            default:
                throw new IllegalArgumentException(
                        "No such context available while updating beforeimage");
        }
    }

    /**
     * 
     * @param entry to be inserted to beforeimage database
     * @return true if successfully added the entry
     */
    public boolean put(final DBEntry entry) {
        assert entry != null;
        LoggerUtils.fine(logger, envImpl,
                "beforeImageIndex put " + entry.getAbortLsn() + " "
                + entry.getExpTime() + " " + entry.isExpInHours());
        if (beforeImageDbImpl == null) {
            openBeforeImageDatabase();
        }
        TestHookExecute.doHookIfSet(beforeImageHook);
        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(entry.getAbortLsn(), key);
        Cursor c = makeCursor(entry.getLock());
        try {
            OperationResult res = c.put(key, entry.getData(), Put.NO_OVERWRITE,
                    new WriteOptions().setTTL(entry.getExpTime(),
                        entry.isExpInHours() ? TimeUnit.HOURS
                        : TimeUnit.DAYS));

            if (res != null) {
                updateStats(entry);
                return true;
            }

            throw EnvironmentFailureException.unexpectedState(envImpl,
                    "Unable to write before image with lsn as it already exists "
                    + entry.getAbortLsn());
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     *
     * @param abortLsn the key to search for in beforeimage database
     * @param lck      if the caller wants to do multiple of this op in single
     *                 txn or {@code null}
     * @return the value of the abortLsn or {@code null} if no abortLsn exists.
     */
    public DatabaseEntry get(long abortLsn, Locker lck) {

        LoggerUtils.fine(logger, envImpl, "beforeImageIndex get " + abortLsn);

        if (beforeImageDbImpl == null) {
            // todo throw an exception
            throw EnvironmentFailureException
                .unexpectedState("No BeforeImage Database Exists ");
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        LongBinding.longToEntry(abortLsn, key);
        boolean txnScopeLocal = false;
        if (lck == null) { // TODO check this correctness
            lck = Txn.createLocalAutoTxn(envImpl, new TransactionConfig());
            txnScopeLocal = true;
        }
        Cursor c = makeCursor(lck);
        try {
            OperationResult res = c.get(key, data, Get.SEARCH, null);
            if (res != null) {
                return data;
            }
            return null;
        } finally {
            if (c != null) {
                c.close();
            }
            if (txnScopeLocal) {
                lck.operationEnd(true);
            }
        }
    }

    /* close the database */
    public void close() throws DatabaseException {
        if (beforeImageDbImpl != null) {
            envImpl.getDbTree().releaseDb(beforeImageDbImpl);
            beforeImageDbImpl = null;
        }
    }

    /**
     * For debugging and unit tests
     */
    public DatabaseImpl getDatabaseImpl() {
        return beforeImageDbImpl;
    }

    /**
     * For debugging and unit tests
     *
     * @throws DatabaseException
     */
    public void dumpDb(boolean display, List<DatabaseEntry> idxDataList) {

        Cursor cursor = null;
        Locker locker = null;
        try {
            locker = BasicLocker.createBasicLocker(envImpl);
            cursor = makeCursor(locker);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (cursor.getNext(key, data,
                        LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                Long keyValue = LongBinding.entryToLong(key);

                if (display) {
                    LoggerUtils.info(logger, envImpl, "key => " + keyValue);
                    // System.out.println("key => " + keyValue);
                }
                if (idxDataList != null) {
                    idxDataList.add(data);
                    data = new DatabaseEntry();
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd(true);
            }
        }
    }
}
