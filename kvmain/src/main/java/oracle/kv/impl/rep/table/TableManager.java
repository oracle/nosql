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

package oracle.kv.impl.rep.table;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.metadata.Metadata.EMPTY_SEQUENCE_NUMBER;
import static oracle.kv.impl.param.ParameterState.RN_PARTITION_SIZE_PERCENT;
import static oracle.kv.impl.param.ParameterState.RN_SG_SIZE_UPDATE_INTERVAL;
import static oracle.kv.impl.param.ParameterUtils.getDurationMillis;
import static oracle.kv.impl.rep.table.SecondaryInfoMap.SECONDARY_INFO_CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableChange;
import oracle.kv.impl.api.table.TableChangeList;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.measurement.TableInfo;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.DatabaseTrigger;
import oracle.kv.impl.rep.MetadataManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.RateRecord;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.table.ResourceCollector.TopCollector;
import oracle.kv.impl.rep.table.ResourceCollector.ChildCollector;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.rep.table.TableMetadataPersistence.TableKeyGenerator;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.tif.TextIndexFeederManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.StateTracker;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.resolver.ConflictResolver;
import oracle.kv.impl.xregion.resolver.LastWriteWinResolver;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;

/**
 * Manages the secondary database handles for the rep node. The TableManager,
 * SecondaryInfoMap, and MaintenanceThread classes are tightly coupled and
 * there are things that are safe because of the way we know they are called
 * from one another.
 */
public class TableManager extends MetadataManager<TableMetadata>
                          implements SecondaryAssociation, TableKeyGenerator,
                                     ParameterListener {

    private static final String SECONDARY_INFO_DB_NAME = "SecondaryInfoDB";
    private static final String TABLE_DB_NAME = "TableMetadata";

    /*
     * The serialization upgrade batches writes to allow the operation to
     * shutdown and to lower contention on the table metadata. Public for unit
     * test
     */
    public static final int SERIALIZATION_UPDATE_BATCH_SIZE = 100;

    /*
     * Create the name of the secondary DB based on the specified index
     * and table names.  Use index name first. Format is:
     *   indexName.tableName[:namespace]
     * The index name is first to more easily handle parent/child names in
     * tableName (parent.child).
     *
     * The ":" is used to separate the namespace for the same reason.
     */
    public static String createDbName(String namespace,
                                      String indexName,
                                      String tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indexName).append(".").append(tableName);
        if (namespace != null) {
            sb.append(":").append(namespace);
        }
        return sb.toString();
    }

    /*
     * Gets the table name from a secondary database name.
     */
    static String getTableName(String dbName) {
        if (dbName.contains(":")) {
            return dbName.split(":")[0].split("\\.", 2)[1];
        }
        return dbName.split("\\.", 2)[1];
    }

    static String getNamespace(String dbName) {
        if (dbName.contains(":")) {
            return dbName.split(":", 2)[1];
        }
        return null;
    }

    private final Params params;

    private final StateTracker stateTracker;

    /*
     * The table metadata. TableMetada is not thread safe. The instance
     * referenced by tableMetadata should be treated as immutable. Updates
     * to the table metadata must be made to a copy first and then replace the
     * reference with the copy.
     */
    private volatile TableMetadata tableMetadata = null;

    private final int maxChanges;

    /**
     * Cache of store serial version. The value is usually the max serial
     * based on the store version parameter. However the parameter value may
     * lag the actual store version due to the way the parameter is updated
     * after an upgrade. Therefore the field may be greater than the parameter
     * value if a higher serial version is detected. The field should only
     * be accessed via the getStoreSerialVersion() methods.
     */
    private volatile short storeSerialVersion = SerialVersion.MINIMUM;

    /*
     * Flag indicating that the table metadata serial form needs to be updated.
     * Public for unit test.
     */
    public volatile boolean requiresSerializationUpdate = false;

    /*
     * The working copy of the metadata when being updated via the replication
     * stream. This field is not synchronized and should only be used within a
     * database callback.
     */
    private volatile TableMetadata updatingMetadata = null;

    /*
     * The id of the transaction currently updating the metadata via the
     * replication stream.
     */
    private long updateTxnId = 0L;

    /**
     * Secondary info map database. Must be synchronized, access the DB via
     * getInfoDb().
     */
    private volatile Database infoDatabase = null;

    /*
     * Map of secondary database handles. Modification or iteration can only
     * be made within the maintenance thread, or with the threadLock held and
     * the maintenance thread stopped.
     */
    private final Map<String, DbHolder> dbHolders = new ConcurrentHashMap<>();

    /*
     * Thread used to asynchronously open secondary database handles and
     * preform other maintenance operations such as populating secondary DBs.
     */
    private MaintenanceThread maintenanceThread = null;

    /*
     * Lock controlling the maintenanceThread, modification to secondaryDbMap,
     * and creation of invalidatedSecondaries. If object synchronization and
     * the threadLock is needed the synchronization should be first.
     */
    private final ReentrantLock threadLock = new ReentrantLock();

    /**
     * Set to true if there is ongoing maintenance operations. The
     * isBusyMaintenance field and associated accessor method are used to
     * interlock table maintenance with partition migration. The interlock
     * scheme is:
     * 1) a maintenance thread sets isBusyMaintenance
     * 2) the maintenance thread waits for migration to complete (by calling
     *    either awaitIdle or awaitTargetIdle on the migration manager)
     * 4) when maintenance is completed (or is interrupted)
     *    isBusyMaintenance is cleared
     *
     * Note: This assumes only one maintenance thread is running at a time. If
     * this changes, the busy indicator will need to be reworked.
     */
    private volatile boolean isBusyMaintenance;

    /*
     * The TableMetadata sequence number, it is updated in updateDbHandles()
     */
    private volatile int metadataSeqNum = EMPTY_SEQUENCE_NUMBER;

    /*
     * The sequence number of MD during an update. If no update is in
     * progress the value is EMPTY_SEQUENCE_NUMBER.
     */
    private final AtomicInteger pendingSeqNum =
                                    new AtomicInteger(EMPTY_SEQUENCE_NUMBER);

    /*
     * The map is an optimization for secondary DB lookup. This lookup happens
     * on every operation and so should be as efficient as possible. This map
     * parallels the Table map in TableMetadata except that it only contains
     * entries for tables which contain indexes (and therefore reference
     * secondary DBs). The map is reconstructed whenever the table MD is
     * updated.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that secondaryLookupMap may be out-of-date relitive to the
     * non-synchronized tableMetadata.
     */
    private volatile Map<String, TableEntry> secondaryLookupMap = null;

    /*
     * Map to lookup a table via its ID.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that idLookupMap may be out-of-date relative to the non-synchronized
     * tableMetadata.
     */
    private volatile Map<Long, TableImpl> idLookupMap = null;

    /*
     * Maximum of all table IDs seen so far, including child tables. A value
     * of 0 indicates that table metadata has not yet be updated.
     */
    private volatile long maxTableId = 0;

    /**
     * Map to lookup a top-level table via its Key as a byte array.
     *
     * The map uses {@link IDBytesComparator} to allow lookup of a full record
     * key (as a byte array). Since the map only contains top-level tables,
     * only the first component of the key (the table ID) is compared.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * A TreeMap is used, rather than a HashMap, because we cannot override
     * the equals/hashcode methods for the byte array.
     */
    private volatile Map<byte[], TableImpl> idBytesLookupMap = null;

    /**
     * Map to lookup a r2compat table id via its name.  It is used to
     * accelerate the r2compat table match test in permission checking.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that r2NameLookupMap may be out-of-date relative to the
     * non-synchronized tableMetadata.
     */
    private volatile Map<String, Long> r2NameIdLookupMap = null;

    /*
     * Maps a table ID to a resource collector object. A collector is
     * present only if limits are present on that table. There is
     * only one collector per table hierarchy. Child table IDs are included
     * in the map and will reference the collector instance for the top
     * level table.
     */
    private volatile Map<Long, ResourceCollector> collectorLookupMap = null;

    /**
     * The default partition size percent. This is the amount that a table
     * can go over it's per-partition limit. Set from the RN parameter
     * RN_PARTITION_SIZE_PERCENT.
     */
    private volatile int partitionSizePercent = 0;

    /**
     * A map of secondary databases which have been corrupted. It is used to
     * cache the corrupted DB on the client operation code path. Creation of
     * this map is synchronized by threadLock. The map is a ConcurrentHashMap
     * which is thread-safe for get/put operations. Iteration and clear are not
     * guaranteed to produce consistent results, but that is OK, see comment in
     * MaintenanceThread#resetSecondaries.
     */
    private volatile Map<String, SecondaryIntegrityException>
                                                        invalidatedSecondaries;

    /*
     * Read-only cache of the secondary info map used when opening secondary
     * databases (openSecondaryDBs called from the maintenance thread after an
     * update). The map is needed to set the incremental population state on
     * each DB. It exist so that the info map is not read from the DB for each
     * secondary on update.
     *
     * The cache must be invalidated (set to null) if the map is modified for
     * a new secondary (openSecondaryDb) or the population state is changed
     * (resetSecondary). Modification from the maintenance thread outside of
     * the openSecondaryDBs method is not a concern since maintenance is
     * single threaded.
     *
     * The only async updates to the map comes from partition migration but
     * does not affect the cache because it does not change the population
     * state.
     */
    private volatile SecondaryInfoMap mapCache = null;

    private final RateLimitingLogger<String> rateLimitingLogger;

    /** conflict resolver */
    private final ConflictResolver resolver;

    /* The hook affects before remove database */
    public static TestHook<Integer> BEFORE_REMOVE_HOOK;
    /* The hook affects after remove database */
    public static TestHook<Integer> AFTER_REMOVE_HOOK;

    /* Hook into update after secondry DB is opened */
    public TestHook<Database> updateHook;

    /* Hook into secondary population */
    public TestHook<Database> populateHook;

    /* Hook into secondary cleaning */
    public TestHook<Database> cleaningHook;

    public TableManager(RepNode repNode, Params params) {
        super(repNode, params);
        this.params = params;
        maxChanges = params.getRepNodeParams().getMaxTopoChanges();
        logger.log(Level.INFO, "Table manager created (max change history={0})",
                   maxChanges);
        rateLimitingLogger = new RateLimitingLogger<>(50 * 1000, 10, logger);
        stateTracker = new TableManagerStateTracker(logger);
        resolver = new LastWriteWinResolver(this);
        setPartitionSizePercent(params.getRepNodeParams().getMap());
    }

    private void setPartitionSizePercent(ParameterMap map) {
        final int newDefault =
                    map.getOrDefault(RN_PARTITION_SIZE_PERCENT).asInt();
        if (partitionSizePercent == newDefault) {
            return;
        }
        logger.log(Level.FINE,
                   () -> "Setting partition size percent to " +
                         newDefault + " from " + partitionSizePercent);

        /*
         * The default has changed, we need to update the partition size
         * limit on all active collectors.
         */
        partitionSizePercent = newDefault;
        final Map<Long, ResourceCollector> clm = collectorLookupMap;
        if (clm != null) {
            for (ResourceCollector rc : clm.values()) {
                if (rc instanceof TopCollector) {
                    ((TopCollector)rc).
                                updatePartitionSizeLimit(partitionSizePercent);
                }
            }
        }

        /*
         * The per-partition size limit depends on the intermediate size
         * update to be running. Check and issue warning if not.
         */
        if ((partitionSizePercent > 0) &&
            (getDurationMillis(map, RN_SG_SIZE_UPDATE_INTERVAL) == 0L)) {
            logger.log(Level.WARNING,
                       () -> "Per-partition size limits depend on the" +
                             " intermediate size update to be running. This" +
                             " requires the parameter " +
                             RN_SG_SIZE_UPDATE_INTERVAL +
                             " to be set to a value greater than zero");
        }
    }

    @Override
    public void shutdown() {
        stateTracker.shutdown();
        shutdownMaintenance();
        /*
         * Signal that we are shutdown before closing the InfoDb, otherwise
         * it is possible to get stuck for a long time trying to close the
         * InfoDb because another thread is stuck in a loop trying to
         * update the InfoDb, but cannot because the master is not
         * authoritative.  The other thread will eventually give up, but that
         * can be a very long wait.  See MetadataManager.tryDBOperation.
         */
        super.shutdown();
        closeInfoDb();
    }

    /* -- From ParameterListener -- */

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        setPartitionSizePercent(newMap);
    }

    RateLimitingLogger<String> getRateLimitingLogger() {
        return rateLimitingLogger;
    }

    /**
     * Returns conflict resolver
     * @return conflict resolver
     */
    public ConflictResolver getResolver() {
        return resolver;
    }

    /**
     * Returns the table metadata object compatible with the specified serial
     * version. Elements of the metadata may be missing if they are not
     * compatible. Returns null if there was an error.
     *
     * @return the table metadata object or null
     */
    public TableMetadata getTableMetadata(short serVersion) {
        final TableMetadata md = getTableMetadata();
        return (md == null) ? null : md.getCompatible(serVersion);
    }

    /**
     * Returns the table metadata object. Returns null if there was an error.
     *
     * @return the table metadata object or null
     */
    public TableMetadata getTableMetadata() {

        final TableMetadata currentTableMetadata = tableMetadata;
        if (currentTableMetadata != null) {
            return currentTableMetadata;
        }

        synchronized (this) {
            if (tableMetadata == null) {
                try {
                    tableMetadata = fetchMetadata();
                } catch (DatabaseNotReadyException dnre) {
                    /* DB not ready, ignore */
                    return  null;
                }
                /*
                 * If the DB is empty, we create a new instance so
                 * that we don't keep re-reading.
                 */
                if (tableMetadata == null) {
                    /*
                     * Keep change history because it's used to push to
                     * other RNs (in other shards) that need metadata
                     * updates. See MetadataUpdateThread.
                     */
                    tableMetadata = new TableMetadata(true);
                }
            }
        }
        return tableMetadata;
    }

    /**
     * Overridden to populate the metadata object in case it is split.
     */
    @Override
    protected TableMetadata readFullMetadata(Database db, Transaction txn) {
        final TableMetadata fetchedMD = super.readFullMetadata(db, txn);
        if (fetchedMD == null) {
            return null;
        }
        if (fetchedMD.isShallow()) {
            requiresSerializationUpdate =
                    TableMetadataPersistence.readTables(fetchedMD, db, txn,
                                                       this, logger);
            if (requiresSerializationUpdate) {
                logger.info("Table metadata requires serialization update");
            }
        }
        return fetchedMD;
    }

    /**
     * Returns the table metadata sequence number.
     *
     * @return the table metadata sequence number
     */
    public int getTableMetadataSeqNum() {
        return metadataSeqNum;
    }

    /* -- public index APIs -- */

    /**
     * Returns true if the specified index has been successfully added.
     *
     * @param indexName the index ID
     * @param tableName the fully qualified table name
     * @return true if the specified index has been successfully added
     */
    public boolean addIndexComplete(String namespace,
                                    String indexName,
                                    String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);

        /* If there is an issue reading the info object, punt */
        if (secondaryInfoMap == null) {
            return false;
        }

        final String dbName = createDbName(namespace, indexName, tableName);

        final SecondaryInfo info = secondaryInfoMap.getSecondaryInfo(dbName);

        if (info == null) {
            logger.log(Level.FINE, "addIndexComplete({0}), info is null, " +
                       "returning false", new Object[]{dbName});
            return false;
        }

        final String msg = info.getErrorString();
        if (msg != null) {
            logger.log(Level.INFO,
                       "addIndexComplete({0}) throwing exception {1}",
                       new Object[]{dbName, msg});
            throw new WrappedClientException(
                new IllegalArgumentException(msg));
        }

        logger.log(Level.FINE, "addIndexComplete({0}) info is {1}",
                   new Object[]{dbName, info});
        return !info.needsPopulating();
    }

    /**
     * Returns true if the data associated with the specified table has been
     * removed from the store.
     *
     * @param tableName the fully qualified table name
     * @return true if the table data has been removed
     */
    public boolean removeTableDataComplete(String namespace, String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);

        /* If there is an issue reading the info object, punt */
        if (secondaryInfoMap == null) {
            return false;
        }

        final DeletedTableInfo info =
            secondaryInfoMap.getDeletedTableInfo(
                NameUtils.makeQualifiedName(namespace, tableName));
        if (info != null) {
            return info.isDone();
        }
        final TableMetadata md = getTableMetadata();

        return (md == null) ? false :
            (md.getTable(namespace, tableName) == null);
    }

   /**
    * Gets the table instance for the specified ID. If no table is defined,
    * or the table is being deleted null is returned.
    *
    * @param tableId a table ID
    * @return the table instance or null
    * @throws RNUnavailableException is the table metadata is not yet
    * initialized
    */
    public TableImpl getTable(long tableId) {
        final TableImpl table = getTableInternal(tableId);
        return table == null ? null : table.isDeleting() ? null : table;
    }

    /**
     * Gets the table instance for the specified ID. If no table is defined
     * null is returned. Note that the table returned may be in a deleting
     * state.
     *
     * @param tableId a table ID
     * @return the table instance or null
     * @throws RNUnavailableException is the table metadata is not yet
     * initialized
     */
    TableImpl getTableInternal(long tableId) {
        final Map<Long, TableImpl> map = idLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }
        return map.get(tableId);
    }

    /**
     * Gets the table instance for a given record key as a byte array.
     *
     * @param key the record key as a byte array.
     * @return the table instance or null if the key is not a table key.
     * @throws RNUnavailableException is the table metadata is not yet
     * initialized
     * @throws DroppedTableException if the key is not for an existing table,
     * and the key does not appear to be a non-table (KV API) key.
     *
     * See IDBytesComparator.
     */
    public TableImpl getTable(byte[] key) {
        final Map<byte[], TableImpl> map = idBytesLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                "Table metadata is not yet initialized");
        }
        TableImpl table = map.get(key);
        final int nextOff = Key.findNextComponent(key, 0);
        if (table == null) {
            /* Check for a dropped top-level table. */
            TableImpl.checkForDroppedTable(key, 0, nextOff, maxTableId);
            return null;
        }
        table = table.findTargetTable(key, nextOff + 1, maxTableId);
        if (table == null) {
            return null;
        }

        /* A "deleting" table be considered as dropped */
        if (table.isDeleting()) {
            throw new DroppedTableException();
        }
        return table;
    }

    /**
     * Gets the table instance for the specified table name. Returns null if no
     * table with that name is in the metadata.
     *
     * @param tableName a table name
     * @return the table instance or null
     */
    TableImpl getTable(String namespace,
                       String tableName) {
        final TableMetadata md = getTableMetadata();

        return (md == null) ? null : md.getTable(namespace, tableName);
    }

    /**
     * Gets the specified table with an optional resource cost. If the table
     * is not found, null is returned. The specified cost will be charged
     * against the table's resource limits. If the cost is greater than 0
     * and the table has resource limits and those limits have been exceeded,
     * either by this call, or by other table activity a ResourceLimitException
     * will be thrown.
     */
    public TableImpl getTable(String namespace, String tableName, int cost) {
        final TableImpl table = getTable(namespace, tableName);
        if ((table == null) || (cost == 0)) {
            return table;
        }

        /*
         * If there are limits on this table, check for access and charge
         * the specified cost.
         */
        final ResourceCollector rc = getResourceCollector(table.getId());
        if (rc != null) {
            rc.addReadUnits(cost);
        }
        return table;
    }

    /**
     * Gets a r2-compat table instance for the specified table name. Returns
     * null if no  r2-compat table with that name is defined.
     *
     * @param tableName a table name
     * @return the table instance or null
     */
    public TableImpl getR2CompatTable(String tableName) {
        final Map<String, Long> map = r2NameIdLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }
        final Long tableId = map.get(tableName);
        return tableId == null ? null : getTable(tableId);
    }

    /**
     * Gets the params used to construct this instance.
     */
    public Params getParams() {
        return params;
    }

    /**
     * Gets the secondary info database.
     */
    synchronized Database getInfoDb(ReplicatedEnvironment repEnv) {
        if (DatabaseUtils.needsRefresh(infoDatabase, repEnv)) {
            closeInfoDb();
            infoDatabase = openInfoDb(repEnv);
        }
        assert infoDatabase != null;
        return infoDatabase;
    }

    /**
     * Opens (or creates) the replicated secondary info DB. The
     * caller is responsible for all exceptions.
     */
    private Database openInfoDb(ReplicatedEnvironment repEnv) {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true).
                 setTransactional(true).
                 getTriggers().add(new InfoDbTrigger());

        final TransactionConfig txnConfig = new TransactionConfig().
              setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        Transaction txn = null;
        Database db = null;
        try {
            txn = repEnv.beginTransaction(null, txnConfig);
            db = repEnv.openDatabase(txn, SECONDARY_INFO_DB_NAME, dbConfig);
            txn.commit();
            txn = null;
            final Database ret = db;
            db = null;
            return ret;
        } finally {
            TxnUtil.abort(txn);

            if (db != null) {
                try {
                    db.close();
                } catch (DatabaseException de) {
                    /* Ignore */
                }
            }
        }
    }

    /**
     * Opens the secondary info database.
     */
    static Database openInfoDB(Environment env,
                               DatabaseConfig dbConfig,
                               Transaction txn) {
        return env.openDatabase(txn, SECONDARY_INFO_DB_NAME, dbConfig);
    }

    /*
     * Closes the secondary info database.
     */
    private synchronized void closeInfoDb() {
        TxnUtil.close(logger, infoDatabase, SECONDARY_INFO_DB_NAME);
        infoDatabase = null;
    }

    /**
     * Gets the secondary info map. Returns null if there is an error getting
     * the map, or the map does not yet exist.
     */
    SecondaryInfoMap getSecondaryInfoMap(ReplicatedEnvironment repEnv) {
        try {
            return SecondaryInfoMap.fetch(getInfoDb(repEnv));
        } catch (RuntimeException re) {
            DatabaseUtils.handleException(re, logger, SECONDARY_INFO_DB_NAME);
        }
        return null;
    }

    /* -- Secondary DB methods -- */

    /**
     * Gets the secondary database for the specified index. Returns null if the
     * secondary database does not exist or the secondary is being populated.
     *
     * @param indexName the index name
     * @param tableName the table name
     * @return a secondary database or null
     */
    public SecondaryDatabase getIndexDB(String namespace,
                                        String indexName,
                                        String tableName) {
        final String dbName = createDbName(namespace, indexName, tableName);
        final DbHolder dbHolder = dbHolders.get(dbName);

        /* Return null if the secondary is being populated */
        return dbHolder == null ? null :
                                  dbHolder.isPopulating ? null : dbHolder.db;
    }

    /**
     * Gets the secondary database of the specified name. Returns null if the
     * secondary database does not exist. This method returns the DB if it
     * exist independent of whether it is being populated.
     *
     * @param dbName the name of a secondary database
     * @return a secondary database or null
     */
    SecondaryDatabase getSecondaryDb(String dbName) {
        final DbHolder dbHolder = dbHolders.get(dbName);
        return dbHolder == null ? null : dbHolder.db;
    }

    /**
     * For each index open its secondary DB.
     */
    int openSecondaryDBs(Map<String, IndexImpl> indexes,
                         Database infoDb,
                         ReplicatedEnvironment repEnv,
                         MaintenanceThread mt) {
        int errors = 0;

        /* The cache is only valid within the context of this method call */
        mapCache = null;

        for (Entry<String, IndexImpl> entry : indexes.entrySet()) {
            if (mt.isStopped()) {
                break;
            }

            final String dbName = entry.getKey();
            SecondaryDatabase db = null;
            try {
                final IndexImpl index = entry.getValue();
                db = getSecondaryDB(dbName, index, infoDb, repEnv);
            } catch (SecondaryIntegrityException sie) {
                resetSecondary(infoDb, sie, "update", repEnv);
                errors++;
            } catch (RuntimeException re) {

                /*
                 * If a DB was opened, and there was some other error (like
                 * not getting the info record), close and remove the DB
                 * from the map.
                 */
                if (db != null) {
                    removeSecondary(dbName);
                    closeSecondaryDb(db);
                }

                /*
                 * Index in metadata but the database was removed by the
                 * master. Likely the secondary was corrupted and will be
                 * rebuilt. So just retry.
                 */
                if (re instanceof DatabasePreemptedException) {
                    logger.log(Level.FINE, "Failed to open database " +
                               "for {0}. {1}",
                               new Object[] {dbName, re.getMessage()});
                    errors++;
                    continue;
                }

                if (re instanceof IllegalCommandException) {
                    logger.log(Level.WARNING, "Failed to open database " +
                               "for {0}. {1}",
                               new Object[] {dbName, re.getMessage()});
                    continue;
                }
                if (DatabaseUtils.handleException(re, logger, dbName)) {
                    errors++;
                }
            }
        }
        mapCache = null;
        return errors;
    }

    /**
     * Gets the specified secondary, opening or creating the DB if needed.
     */
    private SecondaryDatabase getSecondaryDB(String dbName,
                                             IndexImpl index,
                                             Database infoDb,
                                             ReplicatedEnvironment repEnv) {
        DbHolder dbHolder = dbHolders.get(dbName);
        if ((dbHolder == null) ||
            DatabaseUtils.needsRefresh(dbHolder.db, repEnv)) {

            final SecondaryDatabase db =
                                openSecondaryDb(dbName, index, infoDb, repEnv);
            assert db != null;
            assert TestHookExecute.doHookIfSet(updateHook, db);

            dbHolder = new DbHolder(db);
            setIncrementalPopulation(dbName, dbHolder, infoDb);
            dbHolders.put(dbName, dbHolder);
            return db;
        }
        assert TestHookExecute.doHookIfSet(updateHook, dbHolder.db);

        setIncrementalPopulation(dbName, dbHolder, infoDb);
        updateIndexKeyCreator(dbHolder.db, index);
        return dbHolder.db;
    }

    /*
     * Sets the incremental populate mode on the secondary DB.
     */
    private void setIncrementalPopulation(String dbName,
                                          DbHolder dbHolder,
                                          Database infoDb) {
        if (mapCache == null) {
            mapCache = SecondaryInfoMap.fetch(infoDb);
        }
        final SecondaryInfo info = mapCache.getSecondaryInfo(dbName);

        if (info == null) {
            throw new IllegalStateException("Secondary info record for " +
                                            dbName + " is missing");
        }
        if (info.needsPopulating()) {
            dbHolder.startPopulation();
        } else {
            dbHolder.endPopulation();
        }
    }

    /**
     * Records that population of a secondary database has ended.
     */
    void endPopulation(String dbName) {
        final DbHolder dbHolder = dbHolders.get(dbName);
        if (dbHolder == null) {
            throw new IllegalStateException("Secondary holder missing for " +
                                            dbName);
        }
        dbHolder.endPopulation();
    }

    /*
     * Update the index in the secondary's key creator.
     */
    private void updateIndexKeyCreator(SecondaryDatabase db,
                                       IndexImpl index) {
        logger.log(Level.FINE,
                   "Updating index metadata for index {0} in table {1}",
                   new Object[]{index.getName(),
                                index.getTable().getFullName()});
        final SecondaryConfig config = db.getConfig();
        final IndexKeyCreator keyCreator = (IndexKeyCreator)
            (index.isMultiKey() || index.isGeometryIndex() ?
             config.getMultiKeyCreator() :
             config.getKeyCreator());
        assert keyCreator != null;
        keyCreator.setIndex(index);
    }

    /**
     * Resets the specified secondary database due to corruption. The
     * secondary DB is removed and the secondary info is reset for
     * population. Returns true if the reset was successful
     */
    boolean resetSecondary(Database infoDb,
                           SecondaryIntegrityException sie,
                           String operation,
                           ReplicatedEnvironment repEnv) {
        final String dbName = sie.getSecondaryDatabaseName();

        if (operation != null) {
            rateLimitingLogger.log(dbName, Level.SEVERE,
                                   () -> "Integrity problem " +
                                         "with index during " +
                                         operation + ": " + sie.getMessage());
        }

        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);

            /* Close and remove the secondary DB */
            closeSecondary(dbName);
            try {
                repEnv.removeDatabase(txn, dbName);
            } catch (DatabaseNotFoundException dnfe) {
                /* already gone */
                removeSecondary(dbName);
                return true;
            }

            final SecondaryInfoMap infoMap =
                            SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

            /*
             * Reset secondary info. It should be present, but if not it will
             * be created during update.
             */
            final SecondaryInfo info = infoMap.getSecondaryInfo(dbName);
            if (info != null) {
                info.resetForPopulation(sie);
                infoMap.persist(infoDb, txn);
            }
            txn.commit();
            txn = null;

            /* Safe to remove from map */
            removeSecondary(dbName);

            if ((info != null) && (info.getErrorString() != null)) {
                /*
                 * This is a permanent condition. The index will need to be
                 * explicitly removed and then re-added.
                 */
                logger.log(Level.SEVERE,
                           () -> "Secondary reset for " + dbName +
                                 " failed: too many retries, the index must" +
                                 " be explicitly removed and re-added");
            } else {
                logger.log(Level.INFO, () -> "Secondary reset for " + dbName +
                                             " " + info);
            }
            return true;
        } catch (Exception e) {
            /*
             * If there is an exception, log it and return. The population will
             * eventually continue and fail with an SIE, which will call
             * back here, essentially a retry.
             */
            logger.log(Level.WARNING,
                       () -> "Secondary reset for " + dbName + " failed: " +
                             LoggerUtils.getStackTrace(e));
        } finally {
            mapCache = null;
            TxnUtil.abort(txn);
        }
        return false;
    }

    /*
     * Opens the specified secondary database.
     */
    private SecondaryDatabase openSecondaryDb(String dbName,
                                              IndexImpl index,
                                              Database infoDb,
                                              ReplicatedEnvironment repEnv) {
        logger.log(Level.FINE, "Open secondary DB {0}", dbName);

        final IndexKeyCreator keyCreator = new IndexKeyCreator(
            index,
            params.getRepNodeParams().getMaxIndexKeysPerRow());

        /*
         * Use NO_CONSISTENCY so that the handle establishment is not
         * blocked trying to reach consistency particularly when the env is
         * in the unknown state and we want to permit read access.
         */
        final TransactionConfig txnConfig = new TransactionConfig().
           setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        final SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setExtractFromPrimaryKeyOnly(keyCreator.primaryKeyOnly()).
                 setSecondaryAssociation(this).
                 setTransactional(true).
                 setAllowCreate(repEnv.getState().isMaster()).
                 setDuplicateByteComparator(Key.BytesComparator.class).
                 setSortedDuplicates(true);

        if (keyCreator.isMultiKey() || index.isGeometryIndex()) {
            dbConfig.setMultiKeyCreator(keyCreator);
        } else {
            dbConfig.setKeyCreator(keyCreator);
        }

        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, txnConfig);
            final SecondaryDatabase db =
                  repEnv.openSecondaryDatabase(txn, dbName, null, dbConfig);

            /*
             * If we are the master, add the info record for this secondary.
             */
            if (repEnv.getState().isMaster()) {
                SecondaryInfoMap.add(dbName, infoDb, txn, logger);
            }
            txn.commit();
            txn = null;
            return db;
        } catch (IllegalStateException e) {

            /*
             * The exception was most likely thrown because the environment
             * was closed.  If it was thrown for another reason, though,
             * then invalidate the environment so that the caller will
             * attempt to recover by reopening it.
             */
            if (repEnv.isValid()) {
                EnvironmentFailureException.unexpectedException(
                    DbInternal.getEnvironmentImpl(repEnv), e);
            }
            throw e;

        } finally {
            /*
             * Clear the cache so that it is refreshed with the new secondary's
             * info.
             */
            mapCache = null;
            TxnUtil.abort(txn);
        }
    }

    /**
     * Removes the specified secondary from the secondary map.
     */
    void removeSecondary(String dbName) {
        dbHolders.remove(dbName);
    }

    /**
     * Closes all secondary DB handles.
     */
    @Override
    public void closeDbHandles() {
        logger.log(Level.INFO, "Closing secondary database handles");

        threadLock.lock();
        try {
            shutdownMaintenance();

            final Iterator<DbHolder> itr = dbHolders.values().iterator();
            while (itr.hasNext()) {
                closeSecondaryDb(itr.next().db);
                itr.remove();
            }
        } finally {
            threadLock.unlock();
            super.closeDbHandles();
        }
    }

    /**
     * Closes the specified secondary database. Returns true if the close
     * succeeded, or the database was not open.
     *
     * @param dbName the name of the secondary DB to close
     * @return true on success
     */
    boolean closeSecondary(String dbName) {
        return closeSecondaryDb(getSecondaryDb(dbName));
    }

    /**
     * Closes the specified secondary DB. Returns true if the close was
     * successful or db is null.
     *
     * @param db secondary database to close
     * @return true if successful or db is null
     */
    boolean closeSecondaryDb(SecondaryDatabase db) {
        if (db != null) {
            try {
                db.close();
            } catch (RuntimeException re) {
                logger.log(Level.INFO, "close of secondary DB failed: {0}",
                           re.getMessage());
                return false;
            }
        }
        return true;
    }

    /* -- From SecondaryAssociation -- */

    @Override
    public boolean isEmpty() {

        /*
         * This method is called on every operation. It must be as fast
         * as possible.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                    "Table metadata is not yet initialized");
        }
        return map.isEmpty();
    }

    @Override
    public Database getPrimary(DatabaseEntry primaryKey) {
        return repNode.getPartitionDB(primaryKey.getData());
    }

    @Override
    public Collection<SecondaryDatabase> getSecondaries(
        DatabaseEntry primaryKey) {
        return getSecondaries(primaryKey, null, null, null, false, false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<SecondaryDatabase> getSecondaries(
        DatabaseEntry primaryKey,
        String[] allIndexDbNames,
        long[] allIndexIds,
        boolean[] indexesToUpdate,
        boolean expirationUpdated,
        boolean isTombstone) {

        /*
         * This is not synchronized so the map may have nulled since isEmpty()
         * was called.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }

        /* Start by looking up the top level table for this key */
        final BinaryKeyIterator keyIter =
                                    new BinaryKeyIterator(primaryKey.getData());

        /* The first element of the key will be the top level table */
        final String rootId = keyIter.next();
        final TableEntry entry = map.get(rootId);

        /* The entry could be null if the table doesn't have indexes */
        if (entry == null) {
            return Collections.EMPTY_SET;
        }

        /* We have a table with indexes, match the rest of the key. */
        final Map<Long, String> matchedIndexes = entry.matchIndexes(keyIter);

        /* This could be null if the key did not match any table with indexes */
        if (matchedIndexes == null) {
            return Collections.EMPTY_SET;
        }
        final List<SecondaryDatabase> secondaries =
                        new ArrayList<>(matchedIndexes.size());

        /*
         * Get the DB for each of the matched DB. Opening of the DBs is async
         * so we may not be able to complete the list. In this case thow an
         * exception and hopefully the operation will be retried.
         */
        for (Map.Entry<Long, String> index : matchedIndexes.entrySet()) {

            long idxId = index.getKey();
            String dbName = index.getValue();

            if (allIndexDbNames != null &&
                !expirationUpdated &&
                !isTombstone) {
                boolean skip = false;
                for (int i = 0; i < allIndexDbNames.length; ++i) {
                    if ((idxId < 0 || idxId == allIndexIds[i]) &&
                        dbName.equals(allIndexDbNames[i])) {
                        if (!indexesToUpdate[i]) {
                            skip = true;
                            break;
                        }
                    }
                }

                if (skip) {
                    continue;
                }
            }

            // logger.info("XXX Updating Index " + dbName + " with id = " + idxId);

            final SecondaryDatabase db = getSecondaryDb(dbName);
            if (db == null) {
                /* Throwing RNUnavailableException should cause a retry */
                throw new RNUnavailableException(
                                       "Secondary db not yet opened " + dbName);
            }
            secondaries.add(db);
        }
        return secondaries;
    }

    /* -- Metadata update methods -- */

    /**
     * Updates the table metadata with the specified metadata object. Returns
     * true if the update is successful.
     *
     * @param newMetadata a new metadata
     * @return true if the update is successful
     */
    public boolean updateMetadata(Metadata<?> newMetadata) {
        if (!(newMetadata instanceof TableMetadata)) {
            throw new IllegalStateException("Bad metadata?" + newMetadata);
        }

        /* If no env, or not a master then we can't do an update */
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return getLatestSequenceNumber() >= newMetadata.getSequenceNumber();
        }

        /*
         * If an update is in progress, exit to avoid stalls. Return true if
         * we have seen this update.
         */
        if (!pendingSeqNum.compareAndSet(EMPTY_SEQUENCE_NUMBER,
                                         newMetadata.getSequenceNumber())) {
            return getLatestSequenceNumber() >= newMetadata.getSequenceNumber();
        }

        try {
            return updateMetadata((TableMetadata)newMetadata, repEnv);
        } finally {
            /* Reset pendingSeqNum, checking that it didn't change */
            if (!pendingSeqNum.compareAndSet(newMetadata.getSequenceNumber(),
                                             EMPTY_SEQUENCE_NUMBER)) {
                throw new IllegalStateException("Expected table seq#:" +
                                                newMetadata.getSequenceNumber()+
                                                " but found:" +
                                                pendingSeqNum.get());
            }
        }
    }

    private synchronized boolean updateMetadata(TableMetadata newMetadata,
                                                ReplicatedEnvironment repEnv) {
        final TableMetadata md = getTableMetadata();

        /* Can't update if we can't read it */
        if (md == null) {
            return false;
        }
        /* If the current md is up-to-date or newer, exit */
        if (md.getSequenceNumber() >= newMetadata.getSequenceNumber()) {
            return true;
        }
        logger.log(Level.INFO, "Updating table metadata with {0}", newMetadata);
        return update(newMetadata, null, repEnv);
    }

    /**
     * Updates the table metadata with the specified metadata info object.
     * Returns the sequence number of the table metadata at the end of the
     * update.
     *
     * @param metadataInfo a table metadata info object
     * @return the post update sequence number of the table metadata
     */
    public int updateMetadata(MetadataInfo metadataInfo) {
        /*  Only TableChangeList should appear here. */
        if (!(metadataInfo instanceof TableChangeList)) {
            throw new IllegalArgumentException("Unknow metadata info: " +
                                               metadataInfo);
        }
        final TableChangeList changeList = (TableChangeList)metadataInfo;

        /* If no env, or not a master then we can't do an update */
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return getLatestSequenceNumber();
        }

        /*
         * If an update is in progress, exit to avoid stalls. Return the best
         * guess as to our sequence number.
         */
        if (!pendingSeqNum.compareAndSet(EMPTY_SEQUENCE_NUMBER,
                                         changeList.getSequenceNumber())) {
            return getLatestSequenceNumber();
        }

        try {
            return updateMetadata(changeList, repEnv);
        } finally {
            /* Reset pendingSeqNum, checking that it didn't change */
            if (!pendingSeqNum.compareAndSet(changeList.getSequenceNumber(),
                                             EMPTY_SEQUENCE_NUMBER)) {
                throw new IllegalStateException("Expected table seq#:" +
                                                changeList.getSequenceNumber() +
                                                " but found:" +
                                                pendingSeqNum.get());
            }
        }
    }

    private synchronized int updateMetadata(TableChangeList changeList,
                                            ReplicatedEnvironment repEnv) {
        final TableMetadata md = getTableMetadata();
        if (md == null) {
            return Metadata.EMPTY_SEQUENCE_NUMBER;
        }

        /*
         * If the current seq num is >= that of the highest number in the
         * update, skip it; we have a more current version.
         */
        if (md.getSequenceNumber() >= changeList.getSequenceNumber()) {
            logger.log(Level.FINE, "Metadata not updated, current seq " +
                       "number {0} is >= the update {1}",
                       new Object[]{md.getSequenceNumber(),
                                    changeList.getSequenceNumber()});
            return md.getSequenceNumber();
        }
        final TableMetadata newMetadata = md.getCopy();
        try {
            logger.log(Level.FINE,
                       "Attempting to update table metadata, seq num {0}," +
                       " with {1}", new Object[] {md.getSequenceNumber(),
                                                  changeList});
            final Map<Long, TableImpl> changed = apply(changeList, newMetadata);
            if (changed == null) {
                logger.log(Level.FINE,
                           "No changes made, current seq number {0}",
                           md.getSequenceNumber());
                return md.getSequenceNumber();
            }
            logger.log(Level.FINE,
                       "Update of table metadata with {0}," +
                       " resulting seq num {1}",
                       new Object[] { changeList,
                                      newMetadata.getSequenceNumber() });
            if (update(newMetadata, changed, repEnv)) {
                return newMetadata.getSequenceNumber();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error updating table metadata with " +
                       changeList, e);
        }

        logger.log(Level.INFO, "Metadata update failed, current seq number {0}",
                   md.getSequenceNumber());
        /* Update failed, return the current seq # */
        return md.getSequenceNumber();
    }

    /*
     * Updates the metadata from the changes in the specified change list.
     * Returns a map of the table IDs and the top level table instances that
     * have been modified. The map will be empty if the metadata was updated
     * but no tables were changed. Null is returned if no changes occurred.
     */
    private Map<Long, TableImpl> apply(TableChangeList changeList,
                                       TableMetadata md) {
        if (changeList.isEmpty()) {
            return null;
        }
        final int origSeqNum = md.getSequenceNumber();

        /* Map of top level tables which have been modified */
        final Map<Long, TableImpl> changed = new HashMap<>();

        for (TableChange change : changeList) {
            if (change.getSequenceNumber() <= md.getSequenceNumber()) {
                /* ignore older changes that will have been applied */
                continue;
            }
            if (change.getSequenceNumber() > (md.getSequenceNumber() + 1)) {
                /*
                 * if there is an out of order change, fail. Changes should
                 * appear in a list from lower to higher. Generally it should
                 * not be possible to get here because changes are always sent
                 * relative to the current sequence number.
                 */
                logger.warning("Change list element " + change +
                               " out of order, ignoring" +
                               " remainder of " + changeList);
                break;
            }
            /*
             * Apply the change to the metadata. If a table is modified
             * add the top level table to the changed map. Note that a null
             * return may indicate that a non-table change was made to the
             * metadata.
             */
            try {
                final TableImpl table = md.apply(change);
                if (table != null) {
                    final TableImpl top = table.getTopLevelTable();
                    changed.put(top.getId(), top);
                }
            } catch (IllegalCommandException ice) {
                /*
                 * An exception from apply() likely indicates that the metadata
                 * is corrupt. Log at severe and exit.
                 */
                logger.log(Level.WARNING,
                           "Unexpected exception updating " + md +
                           " with " + change + ": " + ice.getMessage());
                break;
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           "Unexpected exception updating " +
                           md + " with " + change, e);
                break;
            }
        }
        return origSeqNum != md.getSequenceNumber() ? changed : null;
    }

    /**
     * Persists the specified table metadata and updates the secondary DB
     * handles. The metadata is persisted only if this node is the master.
     *
     * @return true if the update was successful, or false if in shutdown or
     * unable to restart text index feeder.
     */
    private boolean update(final TableMetadata newMetadata,
                           Map<Long, TableImpl> changed,
                           ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);
        assert repEnv != null;

        /* Only the master can persist the metadata */
        if (repEnv.getState().isMaster() &&
            !persistTableMetadata(newMetadata, changed)) {
            return false;    /* In shutdown */
        }

        /*
         * The TIF needs to see the new metadata in the RepNode when it
         * restarts, so we set it now; however we must save the old metadata in
         * case the TIF metadata update fails in some way, so that it can be
         * restored.  The success or failure of a metadata update is ultimately
         * determined by whether the new metadata was installed, so if there
         * was a failure we want to make sure we don't return from here with
         * the new metadata installed.
         */
        final TableMetadata oldMetadata = tableMetadata;

        /* Tentatively update the cached metadata. */
        tableMetadata = newMetadata;

        /* Notify the text index feeder, if there is one. */
        final TextIndexFeederManager tifm = repNode.getTextIndexFeederManager();
        if (tifm != null) {
            try {
                tifm.newTableMetadata(oldMetadata, newMetadata);
            } catch (Exception e) {
                // TODO - Potential problem. The new MD is persisted at this
                // point, resetting tableMetadata will result in the cached copy
                // being out-of-date and inconsistent relative to the persisted
                // state and replicas! The exception better bring down the RN...
                // in which case why even reset tableMetadata??
                tableMetadata = oldMetadata; /* update failed, undo */
                throw e;
            }
        }

        /* Finally, update the DB handles */
        updateDbHandles(repEnv);

        return true;
    }

    /**
     * Persists the table metadata. If changed is null, all tables are
     * written, otherwise only the tables in the changed map are persisted.
     *
     * @param newMetadata the metadata instance
     * @param changed map of top level tables to write or null
     * @return true on success
     */
    private boolean persistTableMetadata(TableMetadata newMetadata,
                                         Map<Long, TableImpl> changed) {
        assert Thread.holdsLock(this);

        /*
         * persistMetadata will persist the metadata object returned by
         * getMdToWrite (below). Only a  shallow header will be written and
         * the individual table changes need to be written in the TxnOperation.
         */
        return persistMetadata(newMetadata, maxChanges,
                               (TableMetadata oldMetadata,
                                Database db, Transaction txn) -> {

            /*
             * If the old metadata was not split (serial version == 0),
             * first write all of its tables to their new records. Passing
             * null to the change list parameter will cause all tables to
             * be written.
             */
            if ((oldMetadata != null) &&
                (oldMetadata.getSerialVersion() == 0)) {
                TableMetadataPersistence.writeTables(oldMetadata,
                                                     null, /*changed*/
                                                     getStoreSerialVersion(),
                                                     db, txn,
                                                     this,
                                                     logger);
            }
            TableMetadataPersistence.writeTables(newMetadata,
                                                 changed,
                                                 getStoreSerialVersion(),
                                                 db, txn,
                                                 this,
                                                 logger);
            return newMetadata;
        });
    }

    /**
     * Overridden to return a shallow copy of the metadata.
     */
    @Override
    protected TableMetadata getMdToWrite(TableMetadata currentMetadata,
                                         TableMetadata newMetadata) {
        final TableMetadata mdToWrite = super.getMdToWrite(currentMetadata,
                                                           newMetadata);
        if (mdToWrite == null) {
            return null;
        }

        /*
         * The current metadata may have already been written at a higher
         * serial version. If so we know we can at least use that.
         */
        final short minimum =
                (currentMetadata == null) ? SerialVersion.MINIMUM :
                                            currentMetadata.getSerialVersion();
        return mdToWrite.getShallowCopy(getStoreSerialVersion(minimum));
    }

    /*
     * Gets the highest serial version that can be used.
     */
    private short getStoreSerialVersion() {
        return getStoreSerialVersion(SerialVersion.MINIMUM);
    }

    /*
     * Gets the serial version which is the maximum of the specified serial
     * version and the store serial version. The store serial version is
     * usually the max serial based on the store version parameter. See
     * field description for storeSerialVersion.
     */
    private short getStoreSerialVersion(short minimum) {
        assert minimum <= SerialVersion.CURRENT;

        /*
         * If the serial version is not at the current version, check whether
         * it can be updated.
         */
        if (storeSerialVersion < SerialVersion.CURRENT) {
            setStoreSerialVersion(minimum);

            /* If still not there, check the store version */
            if (storeSerialVersion < SerialVersion.CURRENT) {
                setStoreSerialVersion(
                  SerialVersion.getMaxSerialVersion(repNode.getStoreVersion()));
            }
        }
        return storeSerialVersion;
    }

    private synchronized void setStoreSerialVersion(short newSerialVersion) {
        if (newSerialVersion > storeSerialVersion) {
            storeSerialVersion = newSerialVersion;
        }
    }

    /*
     * Gets the latest metadata sequence number. This will be the seq # of the
     * current metadata or the metadata being updated, which ever is highest.
     * There is no synchronization. The returned value should only be used in
     * non-critical situations as it is transient and the value can decrease
     * if a pending update fails.
     *
     * @return the latest metadata sequence number or EMPTY_SEQUENCE_NUMBER
     */
    private int getLatestSequenceNumber() {
        return Math.max(pendingSeqNum.get(), metadataSeqNum);
    }

    /**
     * Updates the secondary database handles based on the current table
     * metadata. Update of the handles is done asynchronously. If an update
     * thread is already running it is stopped and a new thread is started.
     *
     * This is called by the RN when 1) the handles have to be renewed due to
     * an environment change 2) when the topology changes, and 3) when the
     * table metadata is updated.
     *
     * The table maps are also set.
     *
     * @param repEnv the replicated environment handle
     *
     * @return true if table metadata handle needed to be updated.
     */
    @Override
    public synchronized boolean updateDbHandles(ReplicatedEnvironment repEnv) {
        if (repEnv == null) {
            return false;
        }
        final boolean refresh = super.updateDbHandles(repEnv);

        if (updateTableMaps(repEnv)) {
            requestMaintenanceUpdate();
        }
        return refresh;
    }

    /**
     * Rebuilds the secondaryLookupMap and idLookupMap maps. Returns true if
     * the update was successful. If false is returned the table maps have been
     * set such that operations will be disabled.
     *
     * @return true if the update was successful
     */
    private boolean updateTableMaps(ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);
        assert repEnv != null;

        final TableMetadata tableMd = getTableMetadata();

        /*
         * If env is invalid, or tableMD null, disable ops
         */
        if (!repEnv.isValid() || (tableMd == null)) {
            secondaryLookupMap = null;
            idLookupMap = null;
            idBytesLookupMap = null;
            r2NameIdLookupMap = null;
            collectorLookupMap = null;
            return false;
        }
        metadataSeqNum = tableMd.getSequenceNumber();
        maxTableId = tableMd.getMaxTableId();

        /*
         * If empty, then a quick return. Note that we return true so that
         * the update thread runs because there may have been tables/indexes
         * that need cleaning.
         */
        if (tableMd.isEmpty()) {
            secondaryLookupMap = Collections.emptyMap();
            idLookupMap = Collections.emptyMap();
            idBytesLookupMap = Collections.emptyMap();
            r2NameIdLookupMap = Collections.emptyMap();
            collectorLookupMap = Collections.emptyMap();
            return true;
        }

        // TODO - optimize if the MD has not changed?

        final Topology topo = repNode.getTopology();
        final Map<String, TableEntry> slm = new HashMap<>();
        final Map<Long, TableImpl> ilm = new HashMap<>();
        final Map<String, Long> r2nilm = new HashMap<>();
        final Map<Long, ResourceCollector> clm = new HashMap<>();
        final Map<byte[], TableImpl> iblm =
            new TreeMap<>(new IDBytesComparator());

        /* Loop through the top level tables */
        for (Table table : tableMd.getTables().values()) {
            final TableImpl tableImpl = (TableImpl)table;

            /*
             * Add an entry for each table that has indexes somewhere in its
             * hierarchy.
             */
            final TableEntry entry = new TableEntry(tableImpl);

            if (entry.hasSecondaries()) {
                slm.put(tableImpl.getIdString(), entry);
            }

            /*
             * rc is non-null if the table hierarchy has limits. Limits are
             * set on the top level table only.
             */
            ResourceCollector rc;
            if (tableImpl.hasThroughputLimits() ||
                tableImpl.hasSizeLimit()) {
                rc = collectorLookupMap == null ? null :
                                      collectorLookupMap.get(tableImpl.getId());
                /*
                 * If no collector exists, create a new one. Otherwise
                 * reuse the previous one, updating the table instance.
                 */
                if (rc == null) {
                    rc = new TopCollector(tableImpl, repNode, topo,
                                          partitionSizePercent);
                } else {
                    assert rc instanceof TopCollector;
                    ((TopCollector)rc).updateTable(tableImpl, topo,
                                                   partitionSizePercent);
                }
            } else {
                rc = null;
            }

            /*
             * The id map has an entry for each table, so descend into its
             * hierarchy.
             */
            addToMap(tableImpl, ilm, r2nilm, clm, rc);

            /* The ID bytes map has entries only for top-level tables. */
            iblm.put(tableImpl.getIDBytes(), tableImpl);
        }
        secondaryLookupMap = slm;
        idLookupMap = ilm;
        idBytesLookupMap = iblm;
        r2NameIdLookupMap = r2nilm;
        collectorLookupMap = clm;
        return true;
    }

    private void addToMap(TableImpl tableImpl,
                          Map<Long, TableImpl> map,
                          Map<String, Long> r2Map,
                          Map<Long, ResourceCollector> clMap,
                          ResourceCollector rc) {
        map.put(tableImpl.getId(), tableImpl);
        if (rc != null) {
            clMap.put(tableImpl.getId(), rc);
        }
        if (tableImpl.isR2compatible()) {
            r2Map.put(tableImpl.getFullName(), tableImpl.getId());
        }
        for (Table child : tableImpl.getChildTables().values()) {
            addToMap((TableImpl)child, map, r2Map, clMap,
                     rc == null ? null : new ChildCollector(rc));
        }
    }

    /**
     * Starts the state tracker
     * TODO - Perhaps start the tracker on-demand in noteStateChange()?
     */
    public void startTracker() {
        stateTracker.start();
    }

    /**
     * Notes a state change in the replicated environment. The actual
     * work to change state is made asynchronously to allow a quick return.
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
        stateTracker.noteStateChange(stateChangeEvent);
    }

    @Override
    protected MetadataType getType() {
        return MetadataType.TABLE;
    }


    /* -- From TableKeyGenerator -- */

    /**
     * The table ID is used for the key to store the table records. The IDs
     * are short and unique. The RN does not need to access these records
     * by table name (or ID for that matter).
     *
     * The table ID does not conflict with the metadata key which is the
     * string returned from MetadataType.getType().
     */

    /* Table IDs start at 1 */
    private final static long START_TABLE_KEY = 1L;

    @Override
    public void setStartKey(DatabaseEntry key) {
        LongBinding.longToEntry(START_TABLE_KEY, key);
    }

    @Override
    public void setKey(DatabaseEntry key, TableImpl table) {
        assert table.isTop();
        LongBinding.longToEntry(table.getId(), key);
    }

    /* -- From DatabaseTrigger -- */

    /**
     * The replica's metadata is refreshed via database triggers. When the
     * metadata is updated the header is written first followed by one or more
     * table records, all under a single transaction.
     *
     * When a metadata header arrives in a put, it is initialized with the
     * current set of tables and is kept in updatingMetadata. As other put or
     * delete calls arrive for the same transaction the metadata in
     * updatingMetadata is modified accordingly.
     *
     * So the sequence would be one of:
     *
     * 1) non-shallow metadata commit
     *
     * 2) shallow metadata [table metadata]+ commit
     *
     * At commit (for the same txn) the current metadata is replaced by
     * updatingMetadata and a metadata update is triggered.
     *
     * If at any time something goes wrong, possibly the txn is different
     * before the commit, the updating metadata is tossed. If this happens at
     * commit the full metadata will be re-read by the update. This is very
     * unlikely since writing the metadata on the RN is single threaded.
     *
     * Note that the database trigger callbacks are synchronous with the
     * replication stream. Activity done in these methods should be kept to a
     * minimum.
     */
    @Override
    public void put(Transaction txn,
                    DatabaseEntry key,
                    DatabaseEntry oldData,
                    DatabaseEntry newData) {

        if (!canContinue()) {
            /*
             * If we cannot continue, clear out the updating metadata and
             * set the txn ID. This will prevent further action until a new
             * txn shows up.
             */
            updatingMetadata = null;
            updateTxnId = txn.getId();
            return;
        }
        assert txn != null;

        try {
            /* Check for the metadata header */
            if (Arrays.equals(key.getData(), metadataKey.getData())) {
                final TableMetadata newMD = getMetadata(newData);

                /*
                 * If this is not a shallow copy, then the master is still
                 * operating in pre-split mode.
                 */
                if (!newMD.isShallow()) {
                    /*
                     * It would be nice to use the newMD, but need to make sure
                     * we stay in non-split mode until the next txn? Seems
                     * unlikely this could be a problem.
                     */
                    updatingMetadata = newMD;
                    updateTxnId = txn.getId();
                    return;
                }

                /*
                 * We can only update directly from the replication stream if
                 * the metadata is already present.
                 */
                TableMetadata oldMD = tableMetadata;
                if (oldMD == null) {
                    /* Prevent further action until next txn */
                    updatingMetadata = null;
                    updateTxnId = txn.getId();
                    return;
                }

                if ((updateTxnId == 0) || (updateTxnId != txn.getId())) {
                    /* New txn */
                    updateTxnId = txn.getId();
                } else {
                    /* Same txn, if updatingMetadata is null, then skip */
                    if (updatingMetadata == null) {
                        return;
                    }
                    /*
                     * If updatingMetadata is not null we are already updating
                     * and the header must have been written again in the txn.
                     */
                    oldMD = updatingMetadata;
                }
                updatingMetadata = newMD;
                updatingMetadata.initializeTables(oldMD);
                return;
            }

            /* Checks that it is a valid table record */
            checkTableKey(key);

            /*
             * Put of a table instance. Skip if updatingMetadata is
             * null or this is a different txn.
             */
            if ((updatingMetadata == null) || (updateTxnId != txn.getId())) {
                return;
            }

            final TableImpl newTable =
                                    TableMetadataPersistence.getTable(newData);
            updatingMetadata.addTableHierarchy(newTable);
        } catch (Exception e) {
            logStreamFailure("put", e);
            /*
             * Skip this transaction on any failure. This is to
             * avoid dealing with them here in the replication stream. What
             * will happen is that a full update will be done at commit and
             * it is better suited to deal with problems in the metadata.
             */
            updatingMetadata = null;
            updateTxnId = txn.getId();
        }
    }

    @Override
    public void delete(Transaction txn,
                       DatabaseEntry key,
                       DatabaseEntry oldData) {
        if (!canContinue()) {
            updatingMetadata = null;
            updateTxnId = txn.getId();
            return;
        }
        if ((updatingMetadata == null) || (updateTxnId != txn.getId())) {
            return;
        }

        try {
            /* Checks that it is a valid table record */
            checkTableKey(key);

            final TableImpl table = TableMetadataPersistence.getTable(oldData);
            updatingMetadata.removeTableHierarchy(table);
        } catch (Exception e) {
            logStreamFailure("delete", e);
            /* Skip this transaction on any failure. */
            updatingMetadata = null;
            updateTxnId = txn.getId();
        }
    }

    /**
     * Refreshes the table metadata due to it being updated by the master.
     * Called from the database trigger.
     */
    @Override
    public void commit(Transaction txn) {
        if (canContinue()) {
            synchronized (this) {
                /*
                 * If this is the same txn set tableMetadata to the updated
                 * metadata. If updatingMetadata is null or the txn is
                 * different tableMetadata will be null and will force the
                 * metadata to be re-read from the db (in updateDBHandles).
                 */
                tableMetadata =
                        (updateTxnId == txn.getId()) ? updatingMetadata : null;
                updateDbHandles(getEnv());
            }
        }
        updatingMetadata = null;
        updateTxnId = 0L;
    }

    /**
     * Cleans up if the current transaction is aborted.
     */
    @Override
    public void abort(Transaction txn) {
        if (updateTxnId == txn.getId()) {
            updatingMetadata = null;
            updateTxnId = 0L;
        }
    }

    /**
     * Gets the table key from the input data. Throws IllegalStateException if
     * the input data is not a key for a table records.
     */
    private void checkTableKey(DatabaseEntry key) {
        final long id = LongBinding.entryToLong(key);
        if (id <= 0L) {
            throw new IllegalStateException("Unknown table record key: " + id);
        }
    }

    /**
     * Logs a failure encountered while processing the replication stream.
     */
    private void logStreamFailure(String op, Exception e) {
        rateLimitingLogger.log(op, Level.INFO,
                               "Exception during " + op +
                               (updatingMetadata == null ? ": " :
                                                   " of " + updatingMetadata) +
                               e.getMessage());
    }

    /**
     * Note that this method should only be called from the maintenance thread.
     */
    Set<String> getSecondaryDbs() {
        return dbHolders.keySet();
    }

    /**
     * Gets the resource collector for the specified table, if the table
     * has throughput limits enforced. If the TableManager has not been
     * initialized or there is no collector for the table, null is returned.
     *
     * @param tableId a table ID
     * @return the resource collector for the specified table or null
     */
    public ResourceCollector getResourceCollector(long tableId) {
        final Map<Long, ResourceCollector> clm = collectorLookupMap;
        return (clm == null) ? null : clm.get(tableId);
    }

    /**
     * Gets the resource collector for the specified table, if the table
     * has throughput limits enforced. If the TableManager has not been
     * initialized or there is no collector for the table, null is returned.
     *
     * @param tableName a table name
     * @return the resource collector for the specified table or null
     */
    public ResourceCollector getResourceCollector(String tableName) {
        final TableMetadata md = getTableMetadata();
        if (md == null) {
            return null;
        }
        final TableImpl table = md.getTable(tableName);
        return (table == null) ? null : getResourceCollector(table.getId());
    }

    /**
     * Gets the resource collector map. Null may be returned during a
     * metadata update. Returns the actual instance. The map should not
     * be modified.
     *
     * @return the resource collector map or null
     */
    public Map<Long, ResourceCollector> getCollectorMap() {
        return collectorLookupMap;
    }

    public ResourceInfo getResourceInfo(long sinceMillis,
                                        Collection<UsageRecord> usageRecords,
                                        RepNodeId repNodeId, int topoSeqNum) {
        final Map<Long, ResourceCollector> clm = collectorLookupMap;
        if (clm == null) {
            return null;
        }
        final long nanoNow = System.nanoTime();

        final ResourceInfo info = collectInfo(clm, sinceMillis, nanoNow,
                                              repNodeId, topoSeqNum);

        /* If limit records were sent, update the TCs */
        if (usageRecords != null) {

            for (UsageRecord ur : usageRecords) {
                final ResourceCollector rc = clm.get(ur.getTableId());
                if (rc != null) {
                    rc.report(ur, nanoNow);
                }
            }
        }
        return info;
    }

    private ResourceInfo collectInfo(Map<Long, ResourceCollector> clm,
                                     long sinceMillis, long nanoNow,
                                     RepNodeId repNodeId, int topoSeqNum) {
        /*
         * If sinceMills is <= 0 return an empty ResourceInfo. We send this
         * instead of null so that the topo sequence number is sent.
         */
        if (sinceMillis <= 0) {
            return new ResourceInfo(repNodeId, topoSeqNum, null);
        }

        /* Get rate information for current tables */

        final Map<Long, TableImpl> map = idLookupMap;
        if (map == null) {
            return null;
        }
        final Set<RateRecord> records = new HashSet<>();

        /* Convert since times to seconds */
        final long sinceSec = MILLISECONDS.toSeconds(sinceMillis);
        for (TableImpl table : map.values()) {
            /* Only need to get records for the top level tables */
            if (!table.isTop()) {
                continue;
            }
            final ResourceCollector rc = clm.get(table.getId());
            if (rc != null) {
                assert rc instanceof TopCollector;
                ((TopCollector)rc).collectRateRecords(records,
                                                      sinceSec,
                                                      nanoNow);
            }
        }
        return new ResourceInfo(repNodeId, topoSeqNum, records);
    }

    /**
     * Returns a set of table info objects. An info object will be included
     * in the set if there has been activity on that table since the last call
     * to getTableInfo. That is, if the table total read or write KBytes is
     * non-zero. If the TableManager has not been initialized, or there
     * are no tables with activity null is returned.
     *
     * Info object are only generated for top level tables.
     *
     * @return a set of table info objects
     */
    public Set<TableInfo> getTableInfo() {
        final Map<Long, TableImpl> ilm = idLookupMap;
        final Map<Long, ResourceCollector> clm = collectorLookupMap;

        if ((ilm == null) || (clm == null)) {
            return null;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        final Set<TableInfo> infoSet = new HashSet<>();
        for (TableImpl table : ilm.values()) {
            /* Only need to get records for the top level tables */
            if (!table.isTop()) {
                continue;
            }
            final ResourceCollector rc = clm.get(table.getId());
            if (rc != null) {
                assert rc instanceof TopCollector;
                final TableInfo info =
                            ((TopCollector)rc).getTableInfo(currentTimeMillis);
                if (info != null) {
                    infoSet.add(info);
                }
            }
        }
        return infoSet.isEmpty() ? null : infoSet;
    }

    /**
     * Checks if the specified table has any indexes (if indexes != null) or if
     * the table is deleted and needs it data removed. If it has indexes, add
     * them to indexes map. If the table is marked for deletion add that to the
     * deletedTables map.
     *
     * If the table has children, recursively check those.
     */
    static void scanTable(TableImpl table,
                          Map<String, IndexImpl> indexes,
                          Set<TableImpl> deletedTables) {

        if (table.getStatus().isDeleting()) {

            // TODO - should we check for consistency? If so, exception?
            if (!table.getChildTables().isEmpty()) {
                throw new IllegalStateException("Table " + table +
                                                " is deleted but has children");
            }
            if (!table.getIndexes(Index.IndexType.SECONDARY).isEmpty()) {
                throw new IllegalStateException("Table " + table +
                                                " is deleted but has indexes");
            }
            deletedTables.add(table);
            return;
        }
        for (Table child : table.getChildTables().values()) {
            scanTable((TableImpl)child, indexes, deletedTables);
        }

        if (indexes == null) {
            return;
        }
        for (Index index :
            table.getIndexes(Index.IndexType.SECONDARY).values()) {

            indexes.put(createDbName(table.getInternalNamespace(),
                                     index.getName(),
                                     table.getFullName()),
                        (IndexImpl)index);
        }
    }

    /**
     * Invalidates a secondary database due to a SecondaryIntegrityException.
     * The corrupted secondary will be removed and re-populated.
     */
    public void invalidateSecondary(SecondaryIntegrityException sre) {
        assert sre != null;
        threadLock.lock();
        try {
            if (invalidatedSecondaries == null) {
                invalidatedSecondaries = new ConcurrentHashMap<>();
            }
            /* If this is a new entry, request maintenance. */
            if (invalidatedSecondaries.put(sre.getSecondaryDatabaseName(),
                                           sre) == null) {
                requestMaintenanceUpdate();
            }
        } finally {
            threadLock.unlock();
        }
    }

    Map<String, SecondaryIntegrityException> getInvalidatedSecondaries() {
        return invalidatedSecondaries;
    }

    /* -- FastExternalizable conversion */

    /**
     * Updates table records if needed. This is called from the maintenance
     * thread. Clears requiresSerializationUpdate when the update is complete.
     */
    void doSerializationUpdate() {
        /*  Return if we don't need it, or can't do it now */
        if (!requiresSerializationUpdate ||
            (getStoreSerialVersion() <
                        TableMetadataPersistence.SWITCH_TO_JAVA_SERIAL)) {
            return;
        }

        final Database db = getMetadataDatabase();
        if (db == null) {
            return;
        }

        logger.info("Starting table MD serialization update");
        final DatabaseEntry key = new DatabaseEntry();
        setStartKey(key);

        while (!isShutdown()) {
            Transaction txn = null;
            try {
                txn = db.getEnvironment().beginTransaction(null,
                                                         WRITE_NO_SYNC_CONFIG);
                final int nWrites = TableMetadataPersistence.
                                updateTables(db, txn, key,
                                             SERIALIZATION_UPDATE_BATCH_SIZE,
                                             logger);
                logger.log(Level.FINE, () -> "Serialization update modified " +
                                             nWrites + " records");
                if (nWrites > 0) {
                    txn.commit();
                    txn = null;
                }

                /* If updated less than batch, done */
                if (nWrites < SERIALIZATION_UPDATE_BATCH_SIZE) {
                    logger.info("Table MD serialization update complete");
                    requiresSerializationUpdate = false;
                    return;
                }
            } catch (IllegalStateException ise) {
                /*
                 * Log the issue and exit. Update will be retried and hopefully
                 * the problem is transient.
                 */
                logger.log(Level.WARNING,
                           "Exception updating table metadata records", ise);
                return;
            } finally {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Opens the TableMetadata database.
     */
    public static Database openDb(Environment env,
                                  DatabaseConfig dbConfig,
                                  Transaction txn) {
        return env.openDatabase(txn, TABLE_DB_NAME, dbConfig);
    }

    /**
     * Retrieve list of tables from TableMetadata database.
     */
    public static List<TableImpl> getTables(Database dbTable) {
        boolean firstRec = true;
        List<TableImpl> tables = new ArrayList<>();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        try (Cursor curTable = dbTable.openCursor(null, null)) {
            while (curTable.get(key, data, Get.NEXT, null) != null) {
                if (firstRec) {
                    firstRec = false;
                } else {
                    try {
                        TableImpl tableImpl = TableMetadataPersistence
                                        .getTable(data);
                        tables.add(tableImpl);
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return tables;
    }

    /**
     * Container class for a secondary DB handle. Indexes (secondary DBs)
     * cannot be accessed by client operations while the secondary be being
     * populated, so a flag is kept to gate access to the handle.
     */
    private static class DbHolder {
        final SecondaryDatabase db;
        private volatile boolean isPopulating = false;

        DbHolder(SecondaryDatabase db) {
            this.db = db;
        }

        void startPopulation() {
            db.startIncrementalPopulation();
            isPopulating = true;
        }

        void endPopulation() {
            db.endIncrementalPopulation();
            isPopulating = false;
        }
    }

    /**
     * Thread to manage replicated environment state changes.
     */
    private class TableManagerStateTracker extends StateTracker {
        TableManagerStateTracker(Logger logger) {
            super(TableManagerStateTracker.class.getSimpleName(),
                  repNode.getRepNodeId(), logger,
                  repNode.getExceptionHandler());
        }

        @Override
        protected void doNotify(StateChangeEvent sce) {
            logger.log(Level.INFO, "Table manager change state to {0}",
                       sce.getState());

            /**
             * Run the maintenance thread if needed. If the master, then start
             * the maintenance thread in case maintenance operations need to
             * resume. Otherwise, if the thread is already running make a
             * request for update so that the thread notices the state change.
             */
            if (sce.getState().isMaster() || sce.getState().isReplica()) {
                requestMaintenanceUpdate();
            } else {
                shutdownMaintenance();
            }
        }
    }

    /**
     * A container class for quick lookup of secondary DBs. There is a
     * TableEntry instance for each table that has secondary indexes. The entry
     * stores the names of the JE DBs that store these seondary indexes. If the
     * table has child tables, its TableEntry also stores the TableEntrries of
     * those child tables that save secondaries themselves. 
     */
    private static class TableEntry {
        private final int keySize;
        private long fakeIndexIds = -1;
        private final Map<Long, String> secondaries = new HashMap<>();
        private final Map<String, TableEntry> children = new HashMap<>();

        TableEntry(TableImpl table) {
            /* For child tables subtract the key count from parent */
            keySize = (table.getParent() == null ?
                       table.getPrimaryKeySize() :
                       table.getPrimaryKeySize() -
                       ((TableImpl)table.getParent()).getPrimaryKeySize());

            /* For each index, save the secondary DB name */
            for (Index index :
                table.getIndexes(Index.IndexType.SECONDARY).values()) {

                String secDBName = createDbName(
                    ((TableImpl)index.getTable()).getInternalNamespace(),
                    index.getName(),
                    index.getTable().getFullName());
                long indexId = ((IndexImpl)index).getId();
                if (indexId == 0) {
                    /* The index was created before index ids were introduced
                     * (before v24.4). Assign it a fake/transient index id. This
                     * id is < 0, so that it will not be equal to any real
                     * index id (for an index created at or after v24.4). */
                    secondaries.put(fakeIndexIds--, secDBName);
                } else {
                    secondaries.put(indexId, secDBName);
                }
            }

            /* Add only children which have indexes */
            for (Table child : table.getChildTables().values()) {
                final TableEntry entry = new TableEntry((TableImpl)child);

                if (entry.hasSecondaries()) {
                    children.put(((TableImpl)child).getIdString(), entry);
                }
            }
        }

        private boolean hasSecondaries() {
            return !secondaries.isEmpty() || !children.isEmpty();
        }

        private Map<Long, String> matchIndexes(BinaryKeyIterator keyIter) {
            /* Match up the primary keys with the input keys, in number only */
            for (int i = 0; i < keySize; i++) {
                /* If the key is short, then punt */
                if (keyIter.atEndOfKey()) {
                    return null;
                }
                keyIter.skip();
            }

            /* If both are done we have a match */
            if (keyIter.atEndOfKey()) {
                return secondaries;
            }

            /* There is another component, check for a child table */
            final String childId = keyIter.next();
            final TableEntry entry = children.get(childId);
            return (entry == null) ? null : entry.matchIndexes(keyIter);
        }
    }

    /**
     * Requests a maintenance update. If a maintenance thread is present
     * MaintenanceThread.requestUpdate() is called. If unsuccessful (the
     * thread is already in shutdown) or there is no maintenance thread,
     * a new maintenance thread is started.
     */
    private void requestMaintenanceUpdate() {
        threadLock.lock();
        try {
            if ((maintenanceThread != null) &&
                maintenanceThread.requestUpdate()) {
                return;
            }
            maintenanceThread = new MaintenanceThread(maintenanceThread,
                                                      this, repNode,
                                                      logger);
            maintenanceThread.start();
        } finally {
            threadLock.unlock();
        }
    }

    /**
     * Shuts down the maintenance thread.
     */
    private void shutdownMaintenance() {
        final MaintenanceThread t;
        threadLock.lock();
        try {
            t = maintenanceThread;
            maintenanceThread = null;
        } finally {
            threadLock.unlock();
        }
        /*
         * Shuts down the thread outside the thread lock since the maintenance
         * thread may wait to synchronize on this object which causes deadlock.
         */
        if (t != null) {
            t.shutdown();
        }
    }

    /**
     * Returns true if there is active table maintenance operations that
     * require migration to be idle.
     *
     * @return true if there is active table maintenance operations
     */
    public boolean isBusyMaintenance() {
        return isBusyMaintenance;
    }

    /**
     * Returns true if there is active table maintenance operations that
     * require migration to be idle. Also returns true if there is pending
     * secondary DB cleaning.
     *
     * @return true if there is active maintenance or pending secondary
     * DB cleaning
     */
    public boolean busySecondaryCleaning() {
        /* If active maintennce exit early */
        if (isBusyMaintenance) {
            return true;
        }
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        /* If not the right env. we don't know so report busy to be safe */
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return true;
        }

        try {
            final SecondaryInfoMap infoMap = getSecondaryInfoMap(repEnv);

            /* infoMap == null means nothing is going on */
            return (infoMap == null) ? false : infoMap.secondaryNeedsCleaning();
        } catch (RuntimeException re) {
            logger.log(Level.WARNING, "Unexpected exception", re);
            return true;    /* report busy on error */
        }
    }

    /**
     * Sets is busy maintenance flag to true. This method should be called at
     * the start of the maintenance operation that must be guarded. After
     * returning, the thread should wait until migration operations have
     * ceased. After completing the maintenance operation the thread must
     * call clearBusyMaintenance().
     */
    void setBusyMaintenance() {
        isBusyMaintenance = true;
    }

    /**
     * Clears the busy maintenance flag.
     */
    void clearBusyMaintenance() {
        isBusyMaintenance = false;
    }

    public void notifyRemoval(PartitionId partitionId) {
        if (dbHolders.isEmpty()) {
            return;
        }
        logger.log(Level.INFO, "{0} has been removed, removing obsolete " +
                   "records from secondaries", partitionId);

        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return; // TODO - humm - lost info?
        }
        SecondaryInfoMap.markForSecondaryCleaning(getInfoDb(repEnv), logger);

        requestMaintenanceUpdate();
    }

    @Override
    public String toString() {
        return "TableManager[" +
               ((tableMetadata == null) ? "-" :
                                          tableMetadata.getSequenceNumber()) +
               ", " + dbHolders.size() + "]";
    }

    /**
     * Comparator for keys that identify a top level table.
     *
     * <p>Assumes that either param may be the input key param, passed to
     * Map.get. This param will be a byte[] for the entire key, and we
     * should only compare the first component, which is the top-level table
     * ID. Therefore we must check the bytes of each param for delimiters,
     * even though keys in the map will not contain delimiters.
     *
     * <p>The ordering is simply a byte-by-byte comparison of the bytes in the
     * table ID. This is not a meaningful ordering, but we only intend the
     * map to be used for key lookups.
     *
     * <p>For use by the TableManager, an alternate implementation would be to
     * use a ThreadLocal to hold the length of the input param, since this is
     * already calculated by {@link #getTable(byte[])}. This would avoid
     * checking for the delimiter in the comparator, but would require
     * accessing a ThreadLocal every time the comparator is called. I suspect
     * the ThreadLocal access is more expensive, since we check for delimiters
     * only once per comparison.
     */
    public static class IDBytesComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            int o1Len = len(o1);
            int o2Len = len(o2);
            int minLen = Math.min(o1Len, o2Len);

            /* Compare bytes prior to the first delimiter. */
            for (int i = 0; i < minLen; i += 1) {
                if (o1[i] != o2[i]) {
                    return o1[i] - o2[i];
                }
            }

            /* The longer key is considered greater. */
            return (o1Len - o2Len);
        }

        private int len(byte[] o) {
            for (int i = 0; i < o.length; ++i) {
                if (Key.isDelimiter(o[i])) {
                    return i;
                }
            }
            return o.length;
        }
    }

    /**
     * Checks secondary info against the local holder map, specifically the
     * population state between the two. They can differ on the replica since
     * the holder map is local. Returns true if the populating state is
     * different. If fix is true the local state is fixed if possible.
     */
    boolean checkLocalState(ReplicatedEnvironment repEnv, boolean fix) {
        if ((repEnv == null) || !repEnv.getState().isReplica()) {
            return false;
        }
        final SecondaryInfoMap infoMap = getSecondaryInfoMap(repEnv);
        if (infoMap == null) {
            return false;
        }
        boolean different = false;
        final Iterator<Entry<String, DbHolder>> itr =
                                            dbHolders.entrySet().iterator();
        while (itr.hasNext()) {
            final Entry<String, DbHolder> e = itr.next();
            final SecondaryInfo info = infoMap.getSecondaryInfo(e.getKey());
            if (info == null) {
                /*
                 * If the info is missing the secondary was dropped, in
                 * that case the metadata update will handle the local state.
                 */
                continue;
            }
            final DbHolder dbHolder = e.getValue();
            if (dbHolder.isPopulating == info.needsPopulating()) {
                /* Populating state has not changed */
                continue;

            }
            /*
             * If transitioned to populating and fix is true we close the
             * secondary DB (it was likely deleted by the master) and remove
             * the holder object. In this case no further action is needed.
             */
            if (info.needsPopulating() && fix) {
                closeSecondaryDb(dbHolder.db);
                itr.remove();
                continue;
            }
            different = true;
        }
        return different;
    }

    /**
     * Trigger for the secondary info map database. Tracks commits to keep
     * local state on the replica updated.
     */
    private class InfoDbTrigger extends DatabaseTrigger {

        @Override
        public void commit(Transaction t) {
            /*
             * If the local state needs updating, request maintenance. Call
             * with fix == false because changes to local state need to
             * made by the maintenance thread.
             */
            if (canContinue() && checkLocalState(getEnv(), false)) {
                requestMaintenanceUpdate();
            }
        }

        /* -- From DatabaseTrigger -- */

        @Override
        protected boolean isShutdown() {
            return shutdown;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected ReplicatedEnvironment getEnv() {
            return repNode.getEnv(0);
        }
    }
}
