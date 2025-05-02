/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.rep;

import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.fault.ProcessFaultException;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientReplicasException;

/**
 * Maintains the local and replicated version information, consolidating the
 * information whenever necessary.
 */
public class VersionManager {

    /* The name of the local database used to store version information. */
    private static final String VERSION_DATABASE_NAME = "VersionDatabase";
    private static final String VERSION_KEY = "LocalVersion";
    private static final DatabaseEntry VKEY_ENTRY = new DatabaseEntry();

    static {
        StringBinding.stringToEntry(VERSION_KEY, VKEY_ENTRY);
    }

    /* The RN whose version information is being managed. */
    private final RepNode repNode;

    private final Logger logger;

    public VersionManager(Logger logger, RepNode repNode) {
        this.logger = logger;
        this.repNode = repNode;
    }

    /**
     * Invoked at RN startup to check that the code version matches the version
     * stored in the RN's environment. This method is invoked as soon as the
     * environment is opened before any access to its contents. If the
     * version database contains an older version than the current code
     * version, it updates the version database with KVVersion.CURRENT_VERSION.
     *
     * @param env the environment associated with the RN. Note the deliberate
     * typing of the parameter as Environment instead of ReplicateEnvironment
     * to emphasize that no changes are made to any replicated databases.
     */
    void checkCompatibility(Environment env) {

        Database vdb = null;

        try {
            vdb = openDb(env, null);

            final KVVersion localVersion = getLocalVersion(vdb);

            if (localVersion != null) {

                /* If the old version is the same, nothing to do */
                if (localVersion.equals(KVVersion.CURRENT_VERSION)) {
                    return;
                }

                /* Check for upgrade (or downgrade) compatibility. */
                try {
                    VersionUtil.checkUpgrade(localVersion, "previous");
                } catch (IllegalStateException ise) {
                    throw new ProcessFaultException(ise.getMessage(), ise);
                }
            }

            /* Note that the version change may be a patch downgrade */
            repNode.versionChange(env, localVersion);

            /*
             * Finally, install the current version after any earlier changes
             * have been completed.
             */
            final DatabaseEntry vdata = new DatabaseEntry(SerializationUtil.
                getBytes(KVVersion.CURRENT_VERSION));
            try {
                final OperationStatus status = vdb.put(null, VKEY_ENTRY, vdata);
                if (status != OperationStatus.SUCCESS) {
                    throw new IllegalStateException(
                            "Could not install new version");
                }
            } catch (DiskLimitException | InsufficientReplicasException dle) {
                /*
                 * We should not keep going when disk limit exception happens.
                 * One scenario that could cause trouble is that:
                 * (1) we keep going without persist version;
                 * (2) we do some in-memory operations for upgrade;
                 * (3) disk limit problem is solved and we do some more
                 * persistent operation. At this point, we would have
                 * inconsistency among persisted/in-memory data and our
                 * version.
                 *
                 * Plus the chance that we run into disk limit while upgrading
                 * is small, so better safe than sorry.
                 */
                throw new IllegalStateException(
                        "Could not install new version " +
                        "since disk limit reached");
            }

            /*
             * Ensure it's in stable storage, since the version database is
             * non-transactional.
             */
            env.flushLog(true /*fsync*/);
            logger.info("Local Environment version updated to: " +
                        KVVersion.CURRENT_VERSION +
                        " Previous version: " +
                        ((localVersion == null) ?
                         " none" :
                         localVersion.getVersionString()));
        } finally {
            /*
             * Note that since the version database is a non-transactional
             * local database, there is no transaction to abort and undo
             * changes, so the sequence of changes must be carefully organized
             * so they can be retried at a higher level without an undo. That
             * is, they must be idempotent.
             */
            if (vdb != null) {
                TxnUtil.close(logger, vdb, "version");
            }
        }
    }

    /**
     * Returns the current version stored in the version database.
     *
     * @param env the RN's environment
     * @param dbConfig the database configuration or null for the default
     *
     * @return the KVVersion or null
     */
    public static KVVersion getLocalVersion(Logger logger,
                                            Environment env,
                                            DatabaseConfig dbConfig) {
        Database db = null;

        try {
            db = openDb(env, dbConfig);
            return getLocalVersion(db);
        } finally {
            if (db != null) {
                TxnUtil.close(logger, db, "version");
            }
        }
    }

    /* Returns the version from the version database. */
    static KVVersion getLocalVersion(Database versionDB) {
        final DatabaseEntry vdata = new DatabaseEntry();
        final OperationStatus status =
            versionDB.get(null, VKEY_ENTRY, vdata, LockMode.DEFAULT);

        return (status == OperationStatus.SUCCESS) ?
            SerializationUtil.getObject(vdata.getData(), KVVersion.class) :
            null;
    }

    /**
     * Returns a handle to the version DB.
     */
    static Database openDb(Environment env, DatabaseConfig dbConfig) {
        if (dbConfig == null) {
            dbConfig = new DatabaseConfig().
                    setTransactional(false). /* local db cannot be transactional */
                    setAllowCreate(true).
                    setReplicated(false);
        }
        return env.openDatabase(null, VERSION_DATABASE_NAME, dbConfig);
    }
}
