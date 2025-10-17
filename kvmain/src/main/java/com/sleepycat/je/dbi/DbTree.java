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

import static com.sleepycat.je.EnvironmentFailureException.assertState;
import static com.sleepycat.je.log.entry.DbOperationType.CREATE;
import static com.sleepycat.je.log.entry.DbOperationType.REMOVE;
import static com.sleepycat.je.log.entry.DbOperationType.RENAME;
import static com.sleepycat.je.log.entry.DbOperationType.TRUNCATE;
import static com.sleepycat.je.log.entry.DbOperationType.UPDATE_CONFIG;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.log.DbOpReplicationContext;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.HandleLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.utilint.StringUtils;

/**
 * {@literal
 * DbTree represents the database directory for this environment. DbTree is
 * itself implemented through two databases. The nameDatabase maps
 * databaseName-> an internal databaseId. The idDatabase maps
 * databaseId->DatabaseImpl.
 *
 * For example, suppose we have two databases, foo and bar. We have the
 * following structure:
 *
 *           nameDatabase                          idDatabase
 *               IN                                    IN
 *                |                                     |
 *               BIN                                   BIN
 *    +-------------+--------+            +---------------+--------+
 *  .               |        |            .               |        |
 * NameLNs         NameLN    NameLN      MapLNs for   MapLN        MapLN
 * for internal    key=bar   key=foo     internal dbs key=53       key=79
 * dbs             data=     data=                    data=        data=
 *                 dbId79    dbId53                   DatabaseImpl DatabaseImpl
 *                                                        |            |
 *                                                   Tree for foo  Tree for bar
 *                                                        |            |
 *                                                     root IN       root IN
 *
 * Databases, Cursors, the cleaner, compressor, and other entities have
 * references to DatabaseImpls. It's important that object identity is properly
 * maintained, and that all constituents reference the same DatabaseImpl for
 * the same db, lest they develop disparate views of the in-memory database;
 * corruption would ensue. To ensure that, all entities must obtain their
 * DatabaseImpl by going through the idDatabase.
 *
 * DDL type operations such as create, rename, remove and truncate get their
 * transactional semantics by transactionally locking the NameLN appropriately.
 * A read-lock on the NameLN, called a handle lock, is maintained for all DBs
 * opened via the public API (openDatabase).  This prevents them from being
 * renamed or removed while open.  See HandleLocker for details.
 *
 * However, for internal database operations, no handle lock on the NameLN is
 * acquired and MapLNs are obtained using a fast map lookup in a cache
 * maintained here, as described below.
 *
 * - The ConcurrentHashMap DB cache contains all in-use DBs. Calling getDb
 *   adds the DB to the cache and releaseDb allows it to be evicted or
 *   deleted. A cache is used because the Btree lookup and record lock
 *   overhead is too large to do this for every operation, for example, for
 *   every log entry processed by the cleaner or replica replay. getDb is also
 *   called when a DB is opened by the app, and releaseDb is called when it is
 *   closed by the app, to keep the DB in cache during this period.
 *
 * - The MapLN's BIN latch is held while adding it to the cache. Every DB also
 *   has an access latch in the DatabaseImpl. The access SH-latch is held
 *   while the DB is in use (in which case it is also cached). The getDb
 *   method acquires the SH-latch. The access EX-latch and BIN latch are held
 *   while removing a DB from the cache during eviction and MapLN deletion.
 *   The access latch is not held while the DB is cached but not in use.
 *
 * - A SemaphoreLatch is used for the access latch rather than a thread-based
 *   SharedLatch because access latches are sometimes acquired in one thread
 *   and released in another thread. For example, a DB may be opened by the
 *   app in one thread and closed in another, and replica replay may wish to
 *   obtain a DB in a replay preprocessor thread but release it in the main
 *   replay thread. To avoid deadlocks the order for blocking locks is always:
 *     1. DB's access latch, 2. MapLN's BIN latch, 3. MapLN's record lock.
 *
 * - Per-thread caches may be used to avoid the SH-latch and ConcurrentHashMap
 *   lookup for each operation. The getDb method with a Map param is useful
 *   in such cases. However, such caches must be frequently cleared (via the
 *   releaseDb method with the Map param) to support DB deletion. Such
 *   caches are of questionable value and may be removed in the future.
 *   However, one thing that is useful is to cache the deleted state of a DB;
 *   this is because the DB is not in the DbTree cache, so each time getDb is
 *   called a Btree lookup must be performed. Better yet, we should add a
 *   deleted DB size limited set to DbTree and check it before doing the Btree
 *   search.
 *
 * - One may ask why the MapLN record's read lock is not held while MapLN
 *   is in cache, to avoid the need for the DB access latch. One reason is
 *   that record read locks must be released to support obtaining a record
 *   write lock when the MapLN needs to be flushed by modifyDbRoot to update
 *   the root LSN, during checkpoints for example. Perhaps this can be improved
 *   in the future by using a different lock (e.g., BIN latch, root latch)
 *   to protect the root LSN. But another reason not to use a record lock for
 *   accessing the DB is that record locking is much more expensive than
 *   latching; a record lock adds a locker object to a lock owners list, while
 *   a latch just increments an atomic counter.
 *
 * Regarding DB eviction, note that removing the DB from the cache is only
 * needed when the entire MapLN is evicted. It's possible to evict all INs
 * including the root IN of a database without removing it from the DB cache
 * (or closing it in the app), since that doesn't interfere with the
 * DatabaseImpl object identity.
 *
 * Why are the retry loops necessary in the DbTree methods?
 * --------------------------------------------------------
 * Three methods that access the MapLN perform retries (forever) when there is
 * a lock conflict: getDb, modifyDbRoot and deleteMapLN.  Initially the retry
 * loops were added to compensate for certain slow operations. To solve that
 * problem, perhaps there are alternative solutions (increasing the lock
 * timeout).  However, the deleteMapLN retry loop is necessary to avoid
 * deleting it when the DB is in use by reader operations.
 * }
 */
public class DbTree implements Loggable {

    /* The id->DatabaseImpl tree is always id 0 */
    public static final DatabaseId ID_DB_ID = new DatabaseId(0);
    /* The name->id tree is always id 1 */
    public static final DatabaseId NAME_DB_ID = new DatabaseId(1);

    public static final String INTERNAL_DB_NAME_PREFIX = "_je";

    /** Map from internal DB name to type. */
    private final static Map<String, DbType> INTERNAL_TYPES_BY_NAME;
    static {
        final EnumSet<DbType> set = EnumSet.allOf(DbType.class);
        INTERNAL_TYPES_BY_NAME = new HashMap<>(set.size());
        for (DbType t : set) {
            if (t.isInternal()) {
                INTERNAL_TYPES_BY_NAME.put(t.getInternalName(), t);
            }
        }
    }

    /**
     * Returns the DbType for a given DB name.
     */
    static DbType typeForDbName(String dbName) {
        final DbType t = INTERNAL_TYPES_BY_NAME.get(dbName);
        if (t != null) {
            return t;
        }
        return DbType.USER;
    }

    private final ConcurrentHashMap<DatabaseId, DatabaseImpl> dbCache =
        new ConcurrentHashMap<>();

    /*
     * Database Ids:
     * We need to ensure that local and replicated databases use different
     * number spaces for their ids, so there can't be any possible conflicts.
     * Local, non replicated databases use positive values, replicated
     * databases use negative values.  Values -1 thru NEG_DB_ID_START are
     * reserved for future special use.
     */
    public static final long NEG_DB_ID_START = -256L;
    private final AtomicLong lastAllocatedLocalDbId;
    private final AtomicLong lastAllocatedReplicatedDbId;

    private final DatabaseImpl idDatabase;          // map db ids -> databases
    private final DatabaseImpl nameDatabase;        // map names -> dbIds

    /*
     * The log version at the time the env was created. Is -1 if the initial
     * version is unknown, which means it is prior to version 15 because this
     * field was added in version 15. For environments created with log version
     * 15 and greater, no log entries can have a version LT this field's value.
     */
    private int initialLogVersion;

    /* The flags byte holds a variety of attributes. */
    private byte flags;

    /*
     * The replicated bit is set for environments that are opened with
     * replication. The behavior is as follows:
     *
     * Env is     Env is     Persistent          Follow-on action
     * replicated brand new  value of
     *                       DbTree.isReplicated
     *
     * 0             1         n/a               replicated bit = 0;
     * 0             0           0               none
     * 0             0           1               true for r/o, false for r/w
     * 1             1          n/a              replicated bit = 1
     * 1             0           0               require config of all dbs
     * 1             0           1               none
     */
    private static final byte REPLICATED_BIT = 0x1;

    /*
     * The rep converted bit is set when an environments was originally created
     * as a standalone (non-replicated) environment, and has been changed to a
     * replicated environment.
     *
     * The behaviors are as follows:
     *
     * Value of      Value of the    What happens      Can open     Can open
     * RepConfig.      DbTree        when we call       as r/o       as r/2
     * allowConvert  replicated bit  ReplicatedEnv()  Environment  Environment
     *                                                   later on?   later on?
     *
     *                           throw exception,   Yes, because  Yes, because
     *                            complain that the    env is not   env is not
     *  false          false         env is not        converted    converted
     *                               replicated
     *
     *                                              Yes, always ok  No, this is
     *                                                 open a         now a
     *  true           false          do conversion   replicated     replicated
     *                                               env with r/o       env
     *
     *
     *  Ignore         true or      open as a replicated
     * allowConvert   brand new      env the usual way       Yes         No
     *               Environment
     */
    private static final byte REP_CONVERTED_BIT = 0x2;

    /*
     * Since log version 8 and earlier are not supported, this bit is no
     * longer used. However, it will be set in some existing environments, so
     * we can't use it for a different purpose.
     *
     * private static final byte DUPS_CONVERTED_BIT = 0x4;
     */

    /*
     * Prior to log version 19 this flag was used to indicate that a
     * replicated environment preserved VLSNs in cleaned log entries. VLSNs
     * are now always preserved. This bit is cleared in version 19 and above
     * to make it possible to reuse the bit in the future.
     */
    private static final byte OLD_PRESERVE_VLSN_BIT = 0x8;

    /**
     * See {@link EnvironmentParams#AUTO_RESERVED_FILE_REPAIR}.
     * The repaired-reserved-files-done bit is set after repair is complete.
     * It is cleared when the AUTO_RESERVED_FILE_REPAIR param is disabled.
     */
    private static final byte AUTO_REPAIR_RESERVED_FILES_DONE_BIT = 0x10;

    private EnvironmentImpl envImpl;

    /**
     * Create a dbTree from the log.
     */
    public DbTree() {
        this.envImpl = null;
        idDatabase = new DatabaseImpl();

        /*
         * The default is false, but just in case we ever turn it on globally
         * for testing this forces it off.
         */
        idDatabase.clearKeyPrefixing();
        nameDatabase = new DatabaseImpl();
        nameDatabase.clearKeyPrefixing();

        /* These sequences are initialized by readFromLog. */
        lastAllocatedLocalDbId = new AtomicLong();
        lastAllocatedReplicatedDbId = new AtomicLong();

        initialLogVersion = -1;
    }

    /**
     * Create a new dbTree for a new environment.
     */
    public DbTree(EnvironmentImpl env, boolean replicationIntended)
        throws DatabaseException {

        this.envImpl = env;

        /*
         * Sequences must be initialized before any databases are created.  0
         * and 1 are reserved, so we start at 2. We've put -1 to
         * NEG_DB_ID_START asided for the future.
         */
        lastAllocatedLocalDbId = new AtomicLong(1);
        lastAllocatedReplicatedDbId = new AtomicLong(NEG_DB_ID_START);

        /* The id database is local */
        DatabaseConfig idConfig = new DatabaseConfig();
        idConfig.setReplicated(false /* replicated */);

        /*
         * The default is false, but just in case we ever turn it on globally
         * for testing this forces it off.
         */
        idConfig.setKeyPrefixing(false);
        idDatabase = new DatabaseImpl(null,
                                      DbType.ID.getInternalName(),
                                      new DatabaseId(0),
                                      env,
                                      idConfig);
        /* Force a reset if enabled globally. */
        idDatabase.clearKeyPrefixing();

        DatabaseConfig nameConfig = new DatabaseConfig();
        nameConfig.setKeyPrefixing(false);
        nameDatabase = new DatabaseImpl(null,
                                        DbType.NAME.getInternalName(),
                                        new DatabaseId(1),
                                        env,
                                        nameConfig);
        /* Force a reset if enabled globally. */
        nameDatabase.clearKeyPrefixing();

        if (replicationIntended) {
            setIsReplicated();
        }

        /* Clear defunct flag so it can be reused in the future. */
        flags &= ~OLD_PRESERVE_VLSN_BIT;

        initialLogVersion = LogEntryType.LOG_VERSION;
    }

    /**
     * The last allocated local and replicated db ids are used for ckpts.
     */
    public long getLastLocalDbId() {
        return lastAllocatedLocalDbId.get();
    }

    public long getLastReplicatedDbId() {
        return lastAllocatedReplicatedDbId.get();
    }

    /**
     * We get a new database id of the appropriate kind when creating a new
     * database.
     */
    private long getNextLocalDbId() {
        return lastAllocatedLocalDbId.incrementAndGet();
    }

    private long getNextReplicatedDbId() {
        return lastAllocatedReplicatedDbId.decrementAndGet();
    }

    /**
     * Initialize the db ids, from recovery.
     */
    public void setLastDbId(long lastReplicatedDbId, long lastLocalDbId) {
        lastAllocatedReplicatedDbId.set(lastReplicatedDbId);
        lastAllocatedLocalDbId.set(lastLocalDbId);
    }

    /**
     * @return true if this id is for a replicated db.
     */
    private boolean isReplicatedId(long id) {
        return id < NEG_DB_ID_START;
    }

    /*
     * Tracks the lowest replicated database id used during a replay of the
     * replication stream, so that it's available as the starting point if this
     * replica transitions to being the master.
     */
    public void updateFromReplay(DatabaseId replayDbId) {
        assert !envImpl.isMaster();

        final long replayVal = replayDbId.getId();

        if (replayVal > 0 && !envImpl.isRepConverted()) {
            throw EnvironmentFailureException.unexpectedState
                ("replay database id is unexpectedly positive " + replayDbId);
        }

        /*
         * Even though replay is single threaded, multiple threads may be
         * updating lastAllocatedReplicatedDbId. This can happen if a DB write
         * op is attempted on the replica, and the sequence is changed before
         * ReplicaWriteException is thrown.
         */
        while (true) {
            final long val = lastAllocatedReplicatedDbId.get();
            if (replayVal >= val) {
                /* Sequence is past replayVal (values are negative). */
                break;
            }
            if (lastAllocatedReplicatedDbId.compareAndSet(val, replayVal)) {
                /* Successfully updated. */
                break;
            }
        }
    }

    /**
     * Initialize the db tree during recovery, after instantiating the tree
     * from the log.
     * a. set up references to the environment impl
     * b. check for replication rules.
     */
    void initExistingEnvironment(EnvironmentImpl eImpl)
        throws DatabaseException {

        eImpl.checkRulesForExistingEnv(isReplicated());
        this.envImpl = eImpl;
        idDatabase.setEnvironmentImpl(eImpl);
        nameDatabase.setEnvironmentImpl(eImpl);
    }

    /**
     * Creates a new database object given a database name.
     *
     * Increments the use count of the new DB to prevent it from being evicted.
     * releaseDb should be called when the returned object is no longer used,
     * to allow it to be evicted.  See DatabaseImpl.isInUse.  [#13415]
     */
    public DatabaseImpl createDb(Locker locker,
                                 String databaseName,
                                 DatabaseConfig dbConfig,
                                 HandleLocker handleLocker)
        throws DatabaseException {

        return doCreateDb(locker,
                          databaseName,
                          dbConfig,
                          handleLocker,
                          null,  // replicatedLN
                          null); // repContext, to be decided by new db
    }

    /**
     * Create a database for internal use. It may or may not be replicated.
     * Since DatabaseConfig.replicated is true by default, be sure to
     * set it to false if this is a internal, not replicated database.
     */
    public DatabaseImpl createInternalDb(Locker locker,
                                         String databaseName,
                                         DatabaseConfig dbConfig)
        throws DatabaseException {

        /* Force all internal databases to not use key prefixing. */
        dbConfig.setKeyPrefixing(false);
        DatabaseImpl ret =
            doCreateDb(locker,
                       databaseName,
                       dbConfig,
                       null,  // handleLocker,
                       null,  // replicatedLN
                       null); // repContext, to be decided by new db
        /* Force a reset if enabled globally. */
        ret.clearKeyPrefixing();
        return ret;
    }

    /**
     * Create a replicated database on this replica node.
     */
    public DatabaseImpl createReplicaDb(Locker locker,
                                        String databaseName,
                                        DatabaseConfig dbConfig,
                                        NameLN replicatedLN,
                                        ReplicationContext repContext)
        throws DatabaseException {

        return doCreateDb(locker,
                          databaseName,
                          dbConfig,
                          null, // handleLocker
                          replicatedLN,
                          repContext);
    }

    /**
     * Create a database.
     *
     * Gets a SH-latch on the new DB to prevent it from being evicted.
     * releaseDb should be called when the returned object is no longer used,
     * to allow it to be evicted.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    private DatabaseImpl doCreateDb(Locker nameLocker,
                                    String databaseName,
                                    DatabaseConfig dbConfig,
                                    HandleLocker handleLocker,
                                    NameLN replicatedLN,
                                    ReplicationContext repContext)
        throws DatabaseException {

        /* Create a new database object. */
        DatabaseId newId;

        if (ReservedDBMap.getValue(databaseName) != null) {
            // currently only supporting non replicated DB's
            // TODO check partition issues if it's a replicated DB.
            newId = new DatabaseId(ReservedDBMap.getValue(databaseName));
        } else {

            if (replicatedLN != null) {

                /*
                 * This database was created on a master node and is being
                 * propagated to this client node.
                 */
                newId = replicatedLN.getId();
            } else {

                /*
                 * This database has been created locally, either because this
                 * is a non-replicated node or this is the replicated group
                 * master.
                 */
                if (envImpl.isReplicated() && dbConfig.getReplicated()) {

                    /*
                     * Check for ReplicaWrite before updating the ID sequence to
                     * to reduce wasting of IDs when an exception is thrown.
                     * IDs will still be wasted if the node is a master now but
                     * becomes a replica before the commit (which will then
                     * throw a ReplicaWrite), but that is rare and difficult to
                     * avoid.
                     */
                    checkReplicaWrite(nameLocker, ReplicationContext.MASTER);

                    newId = new DatabaseId(getNextReplicatedDbId());
                } else {
                    newId = new DatabaseId(getNextLocalDbId());
                }
            }
        }

        DatabaseImpl newDb;
        CursorImpl idCursor = null;
        CursorImpl nameCursor = null;
        boolean operationOk = false;
        Locker idDbLocker = null;
        try {
            newDb = new DatabaseImpl(nameLocker,
                                     databaseName, newId, envImpl, dbConfig);

            /* Get effective rep context. */
            ReplicationContext useRepContext = repContext;
            if (repContext == null) {
                useRepContext = newDb.getOperationRepContext(CREATE);
            }

            /* Insert it into name -> id db. */
            nameCursor = new CursorImpl(nameDatabase, nameLocker);
            LN nameLN;
            if (replicatedLN != null) {
                nameLN = replicatedLN;
            } else {
                nameLN = new NameLN(newId);
            }

            /*
             * TODO: If another thread creates the DB before this thread,
             * shouldn't we throw an internal exception and retry the open
             * in Environment.setupDatabase?
             */
            insertRecord(
                nameCursor, StringUtils.toUTF8(databaseName),
                nameLN, useRepContext,
                replicatedLN == null /*mustSucceed*/);

            final Txn txn = nameLocker.getTxnLocker();
            long nameLSN = DbLsn.NULL_LSN;
            if (txn != null) {
                nameLSN = txn.getLastLsn();
            }

            /* Record handle lock. */
            if (handleLocker != null) {
                acquireHandleLock(nameCursor, handleLocker);
            }

            /* Insert it into id -> name db, in auto commit mode. */
            idDbLocker = BasicLocker.createBasicLocker(envImpl);
            idCursor = new CursorImpl(idDatabase, idDbLocker);

            insertRecord(
                idCursor, newId.getBytes(), new MapLN(newDb),
                ReplicationContext.NO_REPLICATE,
                replicatedLN == null /*mustSucceed*/);

            /* Cache DB and acquire SH-latch (equivalent of getDb). */
            newDb.getAccessLatch().acquireShared();
            final DatabaseImpl prev =
                dbCache.putIfAbsent(newDb.getId(), newDb);
            assertState(prev == null);
            if (txn instanceof MasterTxn) {
                ((MasterTxn)txn).addDbCleanupAndCloseidDb(
                        new DbCleanup(newDb, DbCleanup.Action.CREATE,
                            true, nameLSN), idDbLocker);
                operationOk = !((MasterTxn)txn).getRaceOnDBCleanUp();
            } else {
                nameLocker.addDbCleanup(
                        new DbCleanup(newDb, DbCleanup.Action.CREATE,
                            true, nameLSN));
                operationOk = true;
            }

        } finally {
            if (idCursor != null) {
                idCursor.close();
            }

            if (nameCursor != null) {
                nameCursor.close();
            }

            if (idDbLocker != null && operationOk) {
                idDbLocker.operationEnd(operationOk);
            }
        }

        return newDb;
    }

    /**
     * Opens (or creates if it does not exist) an internal, non-replicated DB.
     * Returns null only if the DB does not exist and the env is read-only.
     */
    public DatabaseImpl openNonRepInternalDB(final DbType dbType) {

        final String name = dbType.getInternalName();

        final Locker autoTxn = Txn.createLocalAutoTxn(
            envImpl, new TransactionConfig());

        boolean operationOk = false;
        try {
            DatabaseImpl db = getDb(autoTxn, name, null, false);

            if (db == null) {

                if (envImpl.isReadOnly()) {
                    return null;
                }

                final DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setReplicated(false);

                db = createInternalDb(autoTxn, name, dbConfig);
            }
            operationOk = true;
            return db;
        } finally {
            autoTxn.operationEnd(operationOk);
        }
    }

    /**
     * Called after locking a NameLN with nameCursor when opening a database.
     * The NameLN may be locked for read or write, depending on whether the
     * database existed when openDatabase was called.  Here we additionally
     * lock the NameLN for read on behalf of the handleLocker, which is kept
     * by the Database handle.
     *
     * The lock must be acquired while the BIN is latched, so the locker will
     * be updated if the LSN changes.  There is no lock contention possible
     * because the HandleLocker shares locks with the nameCursor locker, and
     * jumpAheadOfWaiters=true is passed in case another locker is waiting on a
     * write lock.
     *
     * If the lock is denied, checkPreempted is called on the nameCursor
     * locker, in case the lock is denied because the nameCursor's lock was
     * preempted. If so, DatabasePreemptedException will be thrown.
     *
     * @see CursorImpl#lockLN
     * @see HandleLocker
     */
    private void acquireHandleLock(CursorImpl nameCursor,
                                   HandleLocker handleLocker) {
        nameCursor.latchBIN();
        try {
            final long lsn = nameCursor.getCurrentLsn();

            final LockResult lockResult = handleLocker.nonBlockingLock
                (lsn, LockType.READ, true /*jumpAheadOfWaiters*/,
                 nameDatabase, null);

            if (lockResult.getLockGrant() == LockGrantType.DENIED) {
                nameCursor.getLocker().checkPreempted();
                throw EnvironmentFailureException.unexpectedState
                    ("No contention is possible with HandleLocker: " +
                     DbLsn.getNoFormatString(lsn));
            }
        } finally {
            nameCursor.releaseBIN();
        }
    }

    /**
     * Write the MapLN to disk.
     * @param db the database represented by this MapLN
     */
    public void modifyDbRoot(DatabaseImpl db)
        throws DatabaseException {

        modifyDbRoot(db, DbLsn.NULL_LSN /*ifBeforeLsn*/, true /*mustExist*/);
    }

    /**
     * Write a MapLN to the log in order to:
     *  - propagate a root change
     *  - save per-db utilization information
     *  - save database config information.
     * Any MapLN writes must be done through this method, in order to ensure
     * that the root latch is taken, and updates to the rootIN are properly
     * safeguarded. See MapN.java for more detail.
     *
     * @param db the database whose root is held by this MapLN
     *
     * @param ifBeforeLsn if argument is not NULL_LSN, only do the write if
     * this MapLN's current LSN is before isBeforeLSN.
     *
     * @param mustExist if true, throw DatabaseException if the DB does not
     * exist; if false, silently do nothing.
     */
    public void modifyDbRoot(
        DatabaseImpl db,
        long ifBeforeLsn,
        boolean mustExist)
        throws DatabaseException {

        /*
         * Do not write LNs in read-only env.  This method is called when
         * recovery causes a root split. [#21493]
         */
        if (envImpl.isReadOnly() && envImpl.isInInit()) {
            return;
        }

        if (db.getId().equals(ID_DB_ID) ||
            db.getId().equals(NAME_DB_ID)) {
            envImpl.logMapTreeRoot();
        } else {
            DatabaseEntry keyDbt = new DatabaseEntry(db.getId().getBytes());

            /*
             * Retry indefinitely in the face of lock timeouts since the
             * lock on the MapLN is only supposed to be held for short
             * periods.
             */
            while (true) {
                Locker idDbLocker = null;
                CursorImpl cursor = null;
                boolean operationOk = false;
                try {
                    idDbLocker = BasicLocker.createBasicLocker(envImpl);
                    cursor = new CursorImpl(idDatabase, idDbLocker);

                    boolean found = cursor.searchExact(keyDbt, LockType.WRITE);

                    if (!found) {
                        if (mustExist) {
                            throw new EnvironmentFailureException(
                                envImpl,
                                EnvironmentFailureReason.LOG_INTEGRITY,
                                "Can't find database ID: " + db.getId());
                        }
                        /* Do nothing silently. */
                        break;
                    }

                    /* Check BIN LSN while latched. */
                    if (ifBeforeLsn == DbLsn.NULL_LSN ||
                        DbLsn.compareTo(
                            cursor.getCurrentLsn(), ifBeforeLsn) < 0) {

                        MapLN mapLN = (MapLN) cursor.getCurrentLN(
                            true, /*isLatched*/ true/*unlatch*/);

                        assert mapLN != null; /* Should be locked. */

                        /* Perform rewrite. */
                        RewriteMapLN writeMapLN = new RewriteMapLN(cursor);
                        mapLN.getDatabase().getTree().withRootLatchedExclusive(
                            writeMapLN);

                        operationOk = true;
                    }
                    break;
                } catch (LockConflictException e) {
                    /* Continue loop and retry. */
                } finally {
                    if (cursor != null) {
                        cursor.releaseBIN();
                        cursor.close();
                    }
                    if (idDbLocker != null) {
                        idDbLocker.operationEnd(operationOk);
                    }
                }
            }
        }
    }

    private static class RewriteMapLN implements WithRootLatched {
        private final CursorImpl cursor;

        RewriteMapLN(CursorImpl cursor) {
            this.cursor = cursor;
        }

        public IN doWork(@SuppressWarnings("unused") ChildReference root)
            throws DatabaseException {

            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
            cursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt,
                new WriteParams(ReplicationContext.NO_REPLICATE),
                null /*foundData*/, null /*returnNewData*/);
            return null;
        }
    }

    /**
     * In other places (e.g., when write locking a record in ReadOnlyTxn) we
     * allow writes to the naming DB on a replica, since we allow both
     * replicated and non-replicated DBs and therefore some NameLNs are
     * replicated and some are not.  Below is the sole check to prevent a
     * creation, removal, truncation, or configuration update of a replicated
     * DB on a replica.  It will throw ReplicaWriteException on a replica if
     * this operation would assign a new VLSN. [#20543]
     */
    private void checkReplicaWrite(Locker locker,
                                   ReplicationContext repContext) {
        if (repContext != null && repContext.mustGenerateVLSN()) {
            locker.disallowReplicaWrite();
        }
    }

    /**
     * Used by lockNameLN to get the rep context, which is needed for calling
     * checkReplicaWrite.
     */
    interface GetRepContext {
        ReplicationContext get(DatabaseImpl dbImpl);
    }

    /**
     * Thrown by lockNameLN when an incorrect locker was used via auto-commit.
     * See Environment.DbNameOperation.  A checked exception is used to ensure
     * that it is always handled internally and never propagated to the app.
     */
    public static class NeedRepLockerException extends Exception
        implements NotSerializable {
        static final long serialVersionUID = 0;
    }

    /**
     * Helper for database operations. This method positions a cursor
     * on the NameLN that represents this database and write locks it.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     *
     * @throws IllegalStateException via
     * Environment.remove/rename/truncateDatabase
     */
    private NameLockResult lockNameLN(Locker locker,
                                      String databaseName,
                                      String action,
                                      GetRepContext getRepContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        /*
         * We have to return both a cursor on the naming tree and a
         * reference to the found DatabaseImpl.
         */
        NameLockResult result = new NameLockResult();

        /* Find the existing DatabaseImpl and establish a cursor. */
        result.dbImpl = getDb(locker, databaseName, null, true);
        if (result.dbImpl == null) {
            throw new DatabaseNotFoundException
                ("Attempted to " + action + " non-existent database " +
                 databaseName);
        }

        boolean success = false;
        try {
            /* Get effective rep context and check for replica write. */
            result.repContext = getRepContext.get(result.dbImpl);
            checkReplicaWrite(locker, result.repContext);

            /*
             * Check for an incorrect locker created via auto-commit.  This
             * check is made after we have the DatabaseImpl and can check
             * whether it is replicated.  See Environment.DbNameOperation.
             */
            if (envImpl.isReplicated() &&
                result.dbImpl.isReplicated() &&
                locker.getTxnLocker() != null &&
                locker.getTxnLocker().isAutoTxn() &&
                !locker.isReplicated()) {
                throw new NeedRepLockerException();
            }

            result.nameCursor = new CursorImpl(nameDatabase, locker);

            /* Position the cursor at the specified NameLN. */
            DatabaseEntry key =
                new DatabaseEntry(StringUtils.toUTF8(databaseName));
            /* See [#16210]. */
            boolean found = result.nameCursor.searchExact(key, LockType.WRITE);

            if (!found) {
                throw new DatabaseNotFoundException(
                    "Attempted to " + action + " non-existent database " +
                    databaseName);
            }

            /* Call lockAndGetCurrentLN to write lock the nameLN. */
            result.nameLN = (NameLN) result.nameCursor.getCurrentLN(
                true, /*isLatched*/ true/*unlatch*/);
            assert result.nameLN != null; /* Should be locked. */

            /*
             * Check for open handles after we have the write lock and no other
             * transactions can open a handle.  After obtaining the write lock,
             * other handles may be open only if (1) we preempted their locks,
             * or (2) a handle was opened with the same transaction as used for
             * this operation.  For (1), we mark the handles as preempted to
             * cause a DatabasePreemptedException the next time they are
             * accessed.  For (2), we throw IllegalStateException.
             */
            if (locker.getImportunate()) {
                /* We preempted the lock of all open DB handles. [#17015] */
                final String msg =
                    "Database " + databaseName +
                    " has been forcibly closed in order to apply a" +
                    " replicated " + action + " operation.  This Database" +
                    " and all associated Cursors must be closed.  All" +
                    " associated Transactions must be aborted.";
                for (Database db : result.dbImpl.getReferringHandles()) {
                    DbInternal.setPreempted(db, databaseName, msg);
                }
            } else {
                /* Disallow open handles for the same transaction. */
                int handleCount = result.dbImpl.getReferringHandleCount();
                if (handleCount > 0) {
                    throw new IllegalStateException
                        ("Can't " + action + " database " + databaseName +
                         ", " + handleCount + " open Database handles exist");
                }
            }
            success = true;
        } finally {
            if (!success) {
                releaseDb(result.dbImpl);
                if (result.nameCursor != null) {
                    result.nameCursor.releaseBIN();
                    result.nameCursor.close();
                }
            }
        }

        return result;
    }

    private static class NameLockResult {
        CursorImpl nameCursor;
        DatabaseImpl dbImpl;
        NameLN nameLN;
        ReplicationContext repContext;
    }

    /**
     * Update the NameLN for the DatabaseImpl when the DatabaseConfig changes.
     *
     * JE MapLN actually includes the DatabaseImpl information, but it is not
     * transactional, so the DatabaseConfig information is stored in
     * NameLNLogEntry and replicated.
     *
     * So when there is a DatabaseConfig changes, we'll update the NameLN for
     * the database, which will log a new NameLNLogEntry so that the rep stream
     * will transfer it to the replicas and it will be replayed.
     *
     * @param locker the locker used to update the NameLN
     * @param dbName the name of the database whose corresponding NameLN needs
     * to be updated
     * @param repContext information used while replaying a NameLNLogEntry on
     * the replicas, it's null on master
     */
    public void updateNameLN(Locker locker,
                             String dbName,
                             final DbOpReplicationContext repContext)
        throws LockConflictException {

        assert dbName != null;

        /* Find and write lock on the NameLN. */
        final NameLockResult result;
        try {
            result = lockNameLN(
                locker, dbName, "updateConfig",
                dbImpl -> (repContext != null) ?
                    repContext :
                    dbImpl.getOperationRepContext(UPDATE_CONFIG, null));

        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }

        final CursorImpl nameCursor = result.nameCursor;
        final DatabaseImpl dbImpl = result.dbImpl;
        final ReplicationContext useRepContext = result.repContext;
        try {

            /* Log a NameLN. */
            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
            nameCursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt,
                new WriteParams(useRepContext),
                null /*foundData*/, null /*returnNewData*/);
        } finally {
            releaseDb(dbImpl);
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Rename the database by creating a new NameLN and deleting the old one.
     *
     * @return the database handle of the impacted database
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found.
     */
    private DatabaseImpl doRenameDb(Locker locker,
                                    String databaseName,
                                    String newName,
                                    NameLN replicatedLN,
                                    final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        final NameLockResult result = lockNameLN(
            locker, databaseName, "rename",
            dbImpl -> (repContext != null) ?
                repContext :
                dbImpl.getOperationRepContext(RENAME));

        final CursorImpl nameCursor = result.nameCursor;
        final DatabaseImpl dbImpl = result.dbImpl;
        final ReplicationContext useRepContext = result.repContext;
        try {

            /*
             * Rename simply deletes the one entry in the naming tree and
             * replaces it with a new one. Remove the oldName->dbId entry and
             * insert newName->dbId.
             */
            nameCursor.deleteCurrentRecord(ReplicationContext.NO_REPLICATE);
            final NameLN useLN =
                (replicatedLN != null) ?
                 replicatedLN :
                 new NameLN(dbImpl.getId());
            /*
             * Reset cursor to remove old BIN before calling insertRecord.
             * [#16280]
             */
            nameCursor.reset();

            /*
             * TODO: Shouldn't this throw a user-visible exception when the
             * newName already exists? It doesn't look like this was accounted
             * for in the Environment.renameDatabase API.
             */
            insertRecord(
                nameCursor, StringUtils.toUTF8(newName), useLN, useRepContext,
                replicatedLN == null /*mustSucceed*/);

            final Txn txn = locker.getTxnLocker();
            long nameLSN = DbLsn.NULL_LSN;
            if (txn != null) {
                nameLSN = txn.getLastLsn();
            }
            /*
             * Schedule MapLN for name change and update if txn commits. This
             * should be the last action taken, since this will take effect
             * immediately for non-txnal lockers.
             *
             * Do not call releaseDb here on dbImpl, since that is taken care
             * of by addDbCleanup.
             */
            locker.addDbCleanup(new DbCleanup(
                dbImpl, DbCleanup.Action.RENAME, true, nameLSN, newName));

            return dbImpl;
        } finally {
            nameCursor.close();
        }
    }

    /**
     * Stand alone and Master invocations.
     *
     * @see #doRenameDb
     */
    public DatabaseImpl dbRename(Locker locker,
                                 String databaseName,
                                 String newName)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doRenameDb(locker, databaseName, newName, null, null);
    }

    /**
     * Replica invocations.
     *
     * @see #doRenameDb
     */
    public DatabaseImpl renameReplicaDb(Locker locker,
                                        String databaseName,
                                        String newName,
                                        NameLN replicatedLN,
                                        DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            return doRenameDb(locker, databaseName, newName, replicatedLN,
                              repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /**
     * Remove the database by deleting the nameLN.
     *
     * @return a handle to the renamed database
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found, or the non-null checkId argument does not
     * match the database identified by databaseName.
     */
    private DatabaseImpl doRemoveDb(Locker locker,
                                    String databaseName,
                                    DatabaseId checkId,
                                    final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        final NameLockResult result = lockNameLN(
            locker, databaseName, "remove",
            dbImpl -> (repContext != null) ?
                repContext :
                dbImpl.getOperationRepContext(REMOVE));

        final CursorImpl nameCursor = result.nameCursor;
        final DatabaseImpl dbImpl = result.dbImpl;
        final ReplicationContext useRepContext = result.repContext;
        boolean releaseDb = true;
        try {
            if (checkId != null && !checkId.equals(result.nameLN.getId())) {
                throw new DatabaseNotFoundException
                    ("ID mismatch: " + databaseName);
            }

            /*
             * Must call prepareForDbExtinction before logging a NameLN that
             * will cause a DB deletion on a replica.
             */
            envImpl.getExtinctionScanner().prepareForDbExtinction(
                useRepContext);

            /*
             * Delete the NameLN. There's no need to mark any Database
             * handle invalid, because the handle must be closed when we
             * take action and any further use of the handle will re-look
             * up the database.
             */
            nameCursor.deleteCurrentRecord(useRepContext);
            final Txn txn = locker.getTxnLocker();
            long nameLSN = DbLsn.NULL_LSN;
            if (txn != null) {
                nameLSN = txn.getLastLsn();
            }

            /*
             * Schedule database for final deletion during commit. This
             * should be the last action taken, since this will take
             * effect immediately for non-txnal lockers.
             *
             * Do not call releaseDb here on dbImpl, since that is taken
             * care of by addDbCleanup.
             */
            locker.addDbCleanup(
                new DbCleanup(dbImpl, DbCleanup.Action.DELETE, true, nameLSN));
            releaseDb = false;

            return dbImpl;
        } finally {
            nameCursor.close();
            if (releaseDb) {
                releaseDb(dbImpl);
            }
        }
    }

    /**
     * Stand alone and Master invocations.
     *
     * @see #doRemoveDb
     */
    public DatabaseImpl dbRemove(Locker locker,
                         String databaseName,
                         DatabaseId checkId)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doRemoveDb(locker, databaseName, checkId, null);
    }

    /**
     * Replica invocations.
     *
     * @see #doRemoveDb
     */
    public void removeReplicaDb(Locker locker,
                                String databaseName,
                                DatabaseId checkId,
                                DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            doRemoveDb(locker, databaseName, checkId, repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /**
     * To truncate, remove the database named by databaseName and
     * create a new database in its place.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     *
     * @param returnCount if true, must return the count of records in the
     * database, which can be an expensive option.
     *
     * @return the record count, oldDb and newDb packaged in a TruncateDbResult
     *
     * @throws DatabaseNotFoundException if the operation fails because the
     * given DB name is not found.
     */
    private TruncateDbResult doTruncateDb(
        Locker locker,
        String databaseName,
        boolean returnCount,
        NameLN replicatedLN,
        final DbOpReplicationContext repContext)
        throws DatabaseNotFoundException, NeedRepLockerException {

        assert replicatedLN == null || (repContext != null);

        final NameLockResult result = lockNameLN(
            locker, databaseName, "truncate",
            dbImpl -> (repContext != null) ?
                repContext :
                dbImpl.getOperationRepContext(TRUNCATE, dbImpl.getId()));

        final CursorImpl nameCursor = result.nameCursor;
        final ReplicationContext useRepContext = result.repContext;
        try {
            /*
             * Must call prepareForDbExtinction before logging a NameLN that
             * will cause a DB deletion on a replica.
             */
            envImpl.getExtinctionScanner().prepareForDbExtinction(
                useRepContext);

            /*
             * Make a new database with an empty tree. Make the nameLN refer to
             * the id of the new database. If this database is replicated, the
             * new one should also be replicated, and vice versa.
             */
            DatabaseImpl oldDb = result.dbImpl;
            final DatabaseId newId =
                (replicatedLN != null) ?
                 replicatedLN.getId() :
                 new DatabaseId(isReplicatedId(oldDb.getId().getId()) ?
                                getNextReplicatedDbId() :
                                getNextLocalDbId());

            DatabaseImpl newDb = oldDb.cloneDatabase();
            newDb.setId(newId);
            newDb.setTree(new Tree(newDb));

            /*
             * Insert the new MapLN into the id tree. Do not use a transaction
             * on the id database, because we can not hold long term locks on
             * the mapLN.
             */
            Locker idDbLocker = null;
            CursorImpl idCursor = null;
            boolean operationOk = false;
            try {
                idDbLocker = BasicLocker.createBasicLocker(envImpl);
                idCursor = new CursorImpl(idDatabase, idDbLocker);

                insertRecord(
                    idCursor, newId.getBytes(), new MapLN(newDb),
                    ReplicationContext.NO_REPLICATE,
                    replicatedLN == null /*mustSucceed*/);

                /* Cache DB and acquire SH-latch (equivalent of getDb). */
                newDb.getAccessLatch().acquireShared();
                final DatabaseImpl prev =
                    dbCache.putIfAbsent(newDb.getId(), newDb);
                assertState(prev == null);

                operationOk = true;
            } finally {
                if (idCursor != null) {
                    idCursor.close();
                }

                if (idDbLocker != null) {
                    idDbLocker.operationEnd(operationOk);
                }
            }
            result.nameLN.setId(newDb.getId());

            /* If required, count the number of records in the database. */
            final long recordCount = (returnCount ? oldDb.count() : 0);

            /* log the nameLN. */
            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);

            nameCursor.updateCurrentRecord(
                null /*replaceKey*/, dataDbt,
                new WriteParams(useRepContext),
                null /*foundData*/, null /*returnNewData*/);

            final Txn txn = locker.getTxnLocker();
            long nameLSN = DbLsn.NULL_LSN;
            if (txn != null) {
                nameLSN = txn.getLastLsn();
            }
            /*
             * Marking the lockers should be the last action, since it
             * takes effect immediately for non-txnal lockers.
             *
             * Do not call releaseDb here on oldDb or newDb, since that is
             * taken care of by addDbCleanup.
             */

            /* Schedule old database for deletion if txn commits. */
            locker.addDbCleanup(
                new DbCleanup(oldDb, DbCleanup.Action.DELETE, true, nameLSN));

            /* Schedule new database for deletion if txn aborts. */
            locker.addDbCleanup(
                new DbCleanup(newDb, DbCleanup.Action.DELETE, false, nameLSN));

            return new TruncateDbResult(oldDb, newDb, recordCount);
        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /*
     * Effectively a struct used to return multiple values of interest.
     */
    public static class TruncateDbResult {
        final DatabaseImpl oldDB;
        public final DatabaseImpl newDb;
        public final long recordCount;

        TruncateDbResult(DatabaseImpl oldDB,
                         DatabaseImpl newDb,
                         long recordCount) {
            this.oldDB = oldDB;
            this.newDb = newDb;
            this.recordCount = recordCount;
        }
    }

    /**
     * @see #doTruncateDb
     */
    public TruncateDbResult truncate(Locker locker,
                                     String databaseName,
                                     boolean returnCount)
        throws DatabaseNotFoundException, NeedRepLockerException {

        return doTruncateDb(locker, databaseName, returnCount, null, null);
    }

    /**
     * @see #doTruncateDb
     */
    public TruncateDbResult truncateReplicaDb(Locker locker,
                                              String databaseName,
                                              boolean returnCount,
                                              NameLN replicatedLN,
                                              DbOpReplicationContext repContext)
        throws DatabaseNotFoundException {

        try {
            return doTruncateDb(locker, databaseName, returnCount,
                                replicatedLN, repContext);
        } catch (NeedRepLockerException e) {
            /* Should never happen; db is known when locker is created. */
            throw EnvironmentFailureException.unexpectedException(envImpl, e);
        }
    }

    /**
     * Attempts to insert a record.
     *
     * If mustSucceed is true, this method invalidates the env if the
     * insertion fails because the key exists. This is used to perform
     * NameLN and MapLN insertions when there is an assumption that the key
     * does not exist.
     *
     * If mustSucceed is false, this method fails silently if the insertion
     * fails. This is used when inserting a MapLN or NameLN during replica
     * replay. In this case, a prior txn may have been truncated and
     * resurrected by recovery.  Also an orphaned MapLN entry may be found
     * because recovery does not undo the MapLN insert when rolling back a
     * database creation.
     */
    private void insertRecord(
        final CursorImpl cursor,
        final byte[] key,
        final LN ln,
        final ReplicationContext repContext,
        final boolean mustSucceed) {

        assert (ln instanceof MapLN) || (ln instanceof NameLN) :
            ln.getClass().getName();

        if (cursor.insertRecord(
            key, ln, false /*blindInsertion*/, repContext)) {
            return;
        }

        if (!mustSucceed) {
            return;
        }

        final long dbId;
        final String dbName;

        if (ln instanceof MapLN) {
            final DatabaseImpl db = ((MapLN) ln).getDatabase();
            dbId = db.getId().getId();
            dbName = db.getName();
        } else {
            final NameLN nameLN = (NameLN) ln;
            dbId = nameLN.getId().getId();
            dbName = StringUtils.fromUTF8(key);
        }

        throw EnvironmentFailureException.unexpectedState(
            envImpl,
            "Internal naming record already exists. Class=" +
                ln.getClass().getName() + " id=" + dbId + " name=" + dbName);
    }

    /**
     * Remove the mapLN that refers to this database.
     *
     * <p>The caller must call getDb prior to this method, which implicitly
     * calls releaseDb before returning.</p>
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    public void deleteMapLN(DatabaseId id) {
        assert !id.equals(idDatabase.getId());
        assert !id.equals(nameDatabase.getId());

        /* Caller previously called getDb. Upgrade latch from SH to EX. */
        DatabaseImpl db = dbCache.get(id);
        assertState(db != null);
        while (true) {
            /* Must release SH-latch to upgrade and avoid deadlocks. */
            db.getAccessLatch().releaseShared();
            db.getAccessLatch().acquireExclusive();
            if (db == dbCache.get(id)) {
                break;
            }
            /* Another thread modified the cache entry for the id. */
            db.getAccessLatch().releaseExclusive();
            db = getDb(id);
            if (db == null) {
                /* MapLN was deleted by another thread. */
                return;
            }
        }
        /*
         * We have an EX-latch on the DB in cache. Delete the MapLN and remove
         * the DB from the cache.
         */
        try {
            final DatabaseEntry key = new DatabaseEntry(id.getBytes());
            while (true) {
                final Locker idDbLocker =
                    BasicLocker.createBasicLocker(envImpl);
                try {
                    final CursorImpl idCursor =
                        new CursorImpl(idDatabase, idDbLocker);
                    try {
                        if (!idCursor.searchExact(key, LockType.WRITE)) {
                            /* MapLN was deleted by another thread. */
                            return;
                        }
                        final MapLN mapLN = (MapLN) idCursor.getCurrentLN(
                            true, /*isLatched*/ true/*unlatch*/);
                        assertState(mapLN.getDatabase() == db);

                        idCursor.deleteCurrentRecord(
                            ReplicationContext.NO_REPLICATE);

                        /*
                         * It is not necessary to hold the BIN latch while
                         * removing the DB from cache, since we have an
                         * EX-latch and the DB is no longer in the Btree.
                         */
                        dbCache.remove(id);
                        return;
                    } catch (LockConflictException e) {
                        /* Retry indefinitely in the face of lock conflicts. */
                    } finally {
                        /* searchExact leaves BIN latched. */
                        idCursor.releaseBIN();
                        idCursor.close();
                    }
                } finally {
                    idDbLocker.operationEnd();
                }
            }
        } finally {
            db.getAccessLatch().releaseExclusive();
        }
    }

    /**
     * Get a database object given a database name.  Increments the use count
     * of the given DB to prevent it from being evicted.  releaseDb should be
     * called when the returned object is no longer used, to allow it to be
     * evicted.  See DatabaseImpl.isInUse.
     * [#13415]
     *
     * @param nameLocker is used to access the NameLN. As always, a NullTxn
     *  is used to access the MapLN.
     * @param databaseName target database
     * @return null if database doesn't exist
     */
    public DatabaseImpl getDb(final Locker nameLocker,
                              final String databaseName,
                              final HandleLocker handleLocker,
                              final boolean writeLock)
        throws DatabaseException {

        /* Use count is not incremented for idDatabase and nameDatabase. */
        if (databaseName.equals(DbType.ID.getInternalName())) {
            return idDatabase;
        }
        if (databaseName.equals(DbType.NAME.getInternalName())) {
            return nameDatabase;
        }

        final DatabaseId id = getDbIdFromName(
            nameLocker, databaseName, handleLocker, writeLock);

        if (id == null) {
            return null;
        }

        /* Now search the id tree. */
        return getDb(id);
    }

    /**
     * Get a database ID given a database name.
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    public DatabaseId getDbIdFromName(final Locker nameLocker,
                                      final String databaseName,
                                      final HandleLocker handleLocker,
                                      final boolean writeLock) {

        /* Search the nameDatabase tree for the NameLn for this name. */
        final CursorImpl nameCursor = new CursorImpl(nameDatabase, nameLocker);

        try {
            final DatabaseEntry keyDbt =
                new DatabaseEntry(StringUtils.toUTF8(databaseName));

            /*
             * A read lock is needed here and will be hold until the
             * nameLocker closes.
             */
            Txn tempLocker = null;
            if (nameLocker.isOptimisticReadIsolation() && !writeLock) {
                tempLocker = (Txn) nameLocker;
                tempLocker.setOptimisticReadIsolation(false);
            }
            try {
                if (!nameCursor.searchExact(
                        keyDbt, writeLock ? LockType.WRITE : LockType.READ)) {
                    return null;
                }
            } finally {
                if (tempLocker != null) {
                    tempLocker.setOptimisticReadIsolation(true);
                }
            }

            final NameLN nameLN = (NameLN) nameCursor.getCurrentLN(
                true, /*isLatched*/ true/*unlatch*/);

            assert nameLN != null; /* Should be locked. */

            final DatabaseId id = nameLN.getId();

            /* Record handle lock. */
            if (handleLocker != null) {
                acquireHandleLock(nameCursor, handleLocker);
            }

            return id;

        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Get a database object based on an id only, caching the id-db mapping in
     * the given map.
     *
     * <p>TODO: This method should eventually be removed and its callers
     * should directly call getDb(id) instead, now that an efficient global
     * cache is in place.</p>
     */
    public DatabaseImpl getDb(DatabaseId dbId,
                              Map<DatabaseId, DatabaseImpl> dbCache) {
        if (dbCache.containsKey(dbId)) {
            return dbCache.get(dbId);
        }
        DatabaseImpl db = getDb(dbId);
        dbCache.put(dbId, db);
        return db;
    }

    /**
     * Calls releaseDb for all DBs in the given map of DatabaseId to
     * DatabaseImpl.  See getDb(DatabaseId, long, Map). [#13415]
     */
    public void releaseDbs(Map<DatabaseId,DatabaseImpl> dbCache) {
        if (dbCache != null) {
            dbCache.values().forEach(this::releaseDb);
        }
    }

    /**
     * Get a database object based on an id only.
     *
     * <p>Returns null if the db is deleted. When non-null is returned, the DB
     * is in cache and a SH-latch is held; {@link #releaseDb} must be called
     * to release the latch, which enables eviction and deletion.</p>
     *
     * <p>Note that non-null is returned if deletion is in progress, in which
     * case {@link DatabaseImpl#isDeleting()} returns true and
     * {@link DatabaseImpl#isDeleteFinished()} returns false.</p>
     *
     * Do not evict (do not call CursorImpl.setAllowEviction(true)) during low
     * level DbTree operation. [#15176]
     */
    public DatabaseImpl getDb(DatabaseId dbId) {
        if (dbId.equals(idDatabase.getId())) {
            return idDatabase;
        }
        if (dbId.equals(nameDatabase.getId())) {
            return nameDatabase;
        }
        while (true) {
            final DatabaseImpl db = dbCache.get(dbId);
            if (db != null) {
                db.getAccessLatch().acquireShared();
                if (dbCache.get(dbId) == db) {
                    /* We have an SH-latch on the DB in cache. */
                    return db;
                }
                /* DB was removed/added to cache by another thread. */
                db.getAccessLatch().releaseShared();
                continue;
            }
            /* DB was not in cache. Get and lock MapLN using a cursor. */
            final DatabaseEntry key = new DatabaseEntry(dbId.getBytes());
            final Locker locker = BasicLocker.createBasicLocker(envImpl);
            try {
                final CursorImpl idCursor = new CursorImpl(idDatabase, locker);
                try {
                    if (!idCursor.searchExact(key, LockType.READ)) {
                        /* DB was deleted. */
                        return null;
                    }
                    final LN ln = idCursor.getCurrentLN(
                        true /*isLatched*/, false /*unlatch*/);
                    if (ln == null || !(ln instanceof MapLN)) {
                        throw new DatabaseNotFoundException(
                            "MapLN missing for dbId: "
                            + dbId + " at LSN: "
                            + DbLsn.toString(idCursor.getCurrentLsn())
                            + " ln: " + ((ln == null) ? "null" : ln));
                    }
                    final MapLN mapLN = (MapLN) ln;
                    /*
                     * Attempt to add to cache while holding BIN latch. Then
                     * continue at top of loop in case another thread added it
                     * to the cache first, and to follow lock ordering rule.
                     */
                    final DatabaseImpl prev =
                        dbCache.putIfAbsent(dbId, mapLN.getDatabase());
                    assertState(prev == null || prev == mapLN.getDatabase());
                } finally {
                    idCursor.releaseBIN();
                    idCursor.close();
                }
            } catch (LockConflictException e) {
                /* Retry indefinitely in the face of lock conflicts. */
            } finally {
                locker.operationEnd();
            }
        }
    }

    /**
     * Releases SH-latch of the given DB, allowing it to be evicted or deleted.
     * Must be called to release a DatabaseImpl that was returned by a method
     * in this class.
     */
    public void releaseDb(DatabaseImpl db) {
        if (db == null ||
            db == idDatabase ||
            db == nameDatabase) {
            /* Latch is not held for idDatabase and nameDatabase. */
            return;
        }
        assertState(db == dbCache.get(db.getId()));
        db.getAccessLatch().releaseShared();
    }

    /**
     * Returns true if the DB is not in the cache or is evicted by this
     * method, meaning in either case that the DB is not in use.
     *
     * <p>The latch for the BIN containing the MapLN must be held when this
     * method is called. While holding the DB EX-latch we are guaranteed
     * that the DB is not currently open or otherwise in use. And it cannot
     * be subsequently opened or used (although this method releases the DB
     * latch when it before returning) until the BIN latch is released, since
     * the BIN latch will block getDb. The caller will evict the MapLN before
     * releasing the BIN latch. After releasing the BIN latch, if a caller
     * of getDb is waiting on the BIN latch, then it will fetch the evicted
     * MapLN and proceed to open/use the DB.</p>
     */
    public boolean evictDb(DatabaseImpl db) {
        /*
         * This method uses a different locking order than usual, since it
         * acquires the DB latch after the BIN latch, but this won't deadlock
         * because it uses a non-blocking latch call.
         */
        if (db.getAccessLatch().acquireExclusiveNoWait()) {
            final DatabaseImpl removedDb = dbCache.remove(db.getId());
            assertState(removedDb == null || db == removedDb);
            db.getAccessLatch().releaseExclusive();
            return true;
        }
        return false;
    }

    /**
     * Rebuild the IN list after recovery.
     */
    public void rebuildINListMapDb()
        throws DatabaseException {

        idDatabase.getTree().rebuildINList();
    }
    /**
     * @return a map of database ids to database names (Strings).
     */
    public Map<DatabaseId,String> getDbNamesAndIds()
        throws DatabaseException {

        final Map<DatabaseId,String> nameMap = new HashMap<>();

        class Traversal implements CursorImpl.WithCursor {
            public boolean withCursor(CursorImpl cursor,
                                      DatabaseEntry key,
                                      @SuppressWarnings("unused")
                                      DatabaseEntry data)
                throws DatabaseException {

                NameLN nameLN = (NameLN) cursor.lockAndGetCurrentLN(
                    LockType.NONE);
                DatabaseId id = nameLN.getId();
                nameMap.put(id, StringUtils.fromUTF8(key.getData()));
                return true;
            }
        }
        Traversal traversal = new Traversal();
        CursorImpl.traverseDbWithCursor
            (nameDatabase, LockType.NONE, false /*allowEviction*/, traversal);
        return nameMap;
    }

    /**
     * @return a list of database names held in the environment, as strings.
     */
    public List<String> getDbNames()
        throws DatabaseException {

        final List<String> nameList = new ArrayList<>();

        CursorImpl.traverseDbWithCursor(nameDatabase,
                                        LockType.NONE,
                                        true /*allowEviction*/,
            (cursor, key, data) -> {

                String name = StringUtils.fromUTF8(key.getData());
                if (!isReservedDbName(name)) {
                    nameList.add(name);
                }
                return true;
            });

        return nameList;
    }

    /**
     * Returns true if the name is a reserved JE database name.
     */
    public static boolean isReservedDbName(String name) {
        return typeForDbName(name).isInternal();
    }

    /**
     * @return the higest level node for this database.
     */
    public int getHighestLevel(DatabaseImpl dbImpl)
        throws DatabaseException {

        /* The highest level in the map side */
        RootLevel getLevel = new RootLevel(dbImpl);
        dbImpl.getTree().withRootLatchedShared(getLevel);
        return getLevel.getRootLevel();
    }

    boolean isReplicated() {
        return (flags & REPLICATED_BIT) != 0;
    }

    void setIsReplicated() {
        flags |= REPLICATED_BIT;
    }

    /*
     * Return true if this environment is converted from standalone to
     * replicated.
     */
    boolean isRepConverted() {
        return (flags & REP_CONVERTED_BIT) != 0;
    }

    void setIsRepConverted() {
        flags |= REP_CONVERTED_BIT;
    }

    public DatabaseImpl getIdDatabaseImpl() {
        return idDatabase;
    }

    public DatabaseImpl getNameDatabaseImpl() {
        return nameDatabase;
    }

    public boolean isAutoRepairReservedFilesDone() {
        return (flags & AUTO_REPAIR_RESERVED_FILES_DONE_BIT) != 0;
    }

    public void setAutoRepairReservedFilesDone() {
        flags |= AUTO_REPAIR_RESERVED_FILES_DONE_BIT;
    }

    public void clearAutoRepairReservedFilesDone() {
        flags &= ~AUTO_REPAIR_RESERVED_FILES_DONE_BIT;
    }

    /**
     * Returns the initial log version at the time the env was created, or -1
     * if the env was created prior to log version 15.
     */
    public int getInitialLogVersion() {
        return initialLogVersion;
    }

    /**
     * Release resources and update memory budget. Should only be called
     * when this dbtree is closed and will never be accessed again.
     */
    public void close() {
        /* Do nothing for now. */
    }

    /*
     * RootLevel lets us fetch the root IN within the root latch.
     */
    private static class RootLevel implements WithRootLatched {
        private final DatabaseImpl db;
        private int rootLevel;

        RootLevel(DatabaseImpl db) {
            this.db = db;
            rootLevel = 0;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            if (root == null) {
                return null;
            }
            IN rootIN = (IN) root.fetchTarget(db, null);
            rootLevel = rootIN.getLevel();
            return null;
        }

        int getRootLevel() {
            return rootLevel;
        }
    }

    /*
     * Logging support
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        return
            LogUtils.getLongLogSize() + // lastAllocatedLocalDbId
            LogUtils.getLongLogSize() + // lastAllocatedReplicatedDbId
            idDatabase.getLogSize() +
            nameDatabase.getLogSize() +
            1 + // 1 byte of flags
            LogUtils.getPackedIntLogSize(initialLogVersion);
            //initialLogVersion
    }

    /**
     * This log entry type is configured to perform marshaling (getLogSize and
     * writeToLog) under the write log mutex.  Otherwise, the size could change
     * in between calls to these two methods as the result of utilizaton
     * tracking.
     *
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        /*
         * Long format, rather than packed long format, is used for the last
         * allocated DB IDs.  The IDs, and therefore their packed length, can
         * change between the getLogSize and writeToLog calls. Since the root
         * is infrequently logged, the simplest solution is to use fixed size
         * values. [#18540]
         */
        LogUtils.writeLong(logBuffer, lastAllocatedLocalDbId.get());
        LogUtils.writeLong(logBuffer, lastAllocatedReplicatedDbId.get());

        idDatabase.writeToLog(logBuffer);
        nameDatabase.writeToLog(logBuffer);
        logBuffer.put(flags);
        LogUtils.writePackedInt(logBuffer, initialLogVersion);
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer itemBuffer,
                            int entryVersion) {

        lastAllocatedLocalDbId.set(LogUtils.readLong(itemBuffer));
        lastAllocatedReplicatedDbId.set(LogUtils.readLong(itemBuffer));

        idDatabase.readFromLog(envImpl, itemBuffer, entryVersion);
        nameDatabase.readFromLog(envImpl, itemBuffer, entryVersion);

        flags = itemBuffer.get();

        if (entryVersion >= 15) {
            initialLogVersion = LogUtils.readPackedInt(itemBuffer);
        } else {
            initialLogVersion = -1;
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<dbtree lastLocalDbId = \"");
        sb.append(lastAllocatedLocalDbId);
        sb.append("\" lastReplicatedDbId = \"");
        sb.append(lastAllocatedReplicatedDbId);
        sb.append("\">");
        sb.append("<idDb>");
        idDatabase.dumpLog(sb, verbose);
        sb.append("</idDb><nameDb>");
        nameDatabase.dumpLog(sb, verbose);
        sb.append("</nameDb>");
        sb.append("</dbtree>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(@SuppressWarnings("unused") Loggable other) {
        return false;
    }

    /*
     * For unit test support
     */

    String dumpString(int nSpaces) {
        StringBuilder self = new StringBuilder();
        self.append(TreeUtils.indent(nSpaces));
        self.append("<dbTree lastDbId =\"");
        self.append(lastAllocatedLocalDbId);
        self.append("\">");
        self.append('\n');
        self.append(idDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append(nameDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append("</dbtree>");
        return self.toString();
    }

    @Override
    public String toString() {
        return dumpString(0);
    }

    /**
     * For debugging.
     */
    public void dump() {
        idDatabase.getTree().dump();
        nameDatabase.getTree().dump();
    }
}
