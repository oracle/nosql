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

package oracle.kv.impl.xregion.service;

import static oracle.kv.RequestLimitConfig.DEFAULT_NODE_LIMIT_PERCENT;
import static oracle.kv.RequestLimitConfig.DEFAULT_REQUEST_THRESHOLD_PERCENT;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_AGENT_ID;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_CHECKPOINT;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_SOURCE_REGION;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_TABLE_NAME;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;
import static oracle.kv.impl.xregion.init.TableInitCheckpoint.CKPT_TABLE_WRITE_OPT;
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_MAX_CONCURRENT_OTHER_REQUESTS;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.COMPLETE;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.RequestLimitConfig;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.systables.MRTableInitCkptDesc;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.impl.xregion.init.TableInitCheckpoint;
import oracle.kv.impl.xregion.stat.TableInitStat.TableInitState;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.FieldDef;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.json.JsonUtils;

/**
 * Object that manages the metadata in {@link XRegionService}.
 */
public class ServiceMDMan {

    /**
     * rate limiting log period in ms
     */
    public static final int RL_LOG_PERIOD_MS = 60 * 1000;
    /**
     * rate limiting log period in ms with longer sampling interval
     */
    private static final int RL_LOG_PERIOD_LONG_MS = 60 * 10 * 1000;
    /**
     * max # objects in rl logger, enough to hold all MR tables
     */
    public static final int RL_MAX_NUM_OBJECTS = 1024 * 1024;
    /**
     * default sleep in ms in retry get table
     */
    private static final long DEFAULT_GET_TABLE_RETRY_SLEEP_MS = 100;

    /**
     * unit test only, test hook to generate failure. The hook will be called
     * when reading checkpoint from the local store. The arguments pass the
     * name of exception to throw if the hook is fired.
     */
    public static volatile ExceptionTestHook<String, RuntimeException>
        expHook = null;

    /**
     * interval in ms to fetch the system table
     */
    private final static int SYS_TABLE_INTV_MS = 5 * 1000;

    /**
     * polling internal in ms
     */
    private static final int POLL_INTERVAL_MS = 1000;

    /**
     * max times to pull table instance from server
     */
    private static final int MAX_PULL_TABLE_RETRY = 10;

    /**
     * sleep time in ms before retry on table not found exception
     */
    private static final int MNFE_SLEEP_MS = 5 * 1000;

    /**
     * sleep time in ms before retry on fault exception
     */
    private static final int FE_SLEEP_MS = 100;

    /**
     * sleep time in seconds before retry to create a kvstore
     */
    private static final int CREATE_KVS_SLEEP_SECS = 5;

    /**
     * private logger
     */
    private final Logger logger;

    /**
     * rate-limit logger
     */
    private final RateLimitingLogger<String> rlLogger;

    /**
     * rate-limit logger with longer sampling interval
     */
    private final RateLimitingLogger<String> rlLoggerLong;

    /**
     * id of the subscriber
     */
    private final NoSQLSubscriberId sid;

    /**
     * the region this agent serves
     */
    private final RegionInfo servRegion;

    /**
     * map of regions from config
     */
    private final Map<String, RegionInfo> regionMap;

    /**
     * all PITR tables serviced in agent
     */
    private final Map<String, Table> pitrTables = new ConcurrentHashMap<>();

    /**
     * map of regions and kvstore handles
     */
    private final Map<RegionInfo, KVStore> allKVS = new ConcurrentHashMap<>();

    /**
     * region id translator
     */
    private final RegionIdTrans ridTrans;

    /**
     * region id mapper
     */
    private volatile RegionIdMapping ridMapping;

    /**
     * true if closed
     */
    private volatile boolean closed;

    /**
     * true if in some unittest
     */
    private final boolean unitTest;

    /**
     * handle to statistics from stats manager
     */
    private volatile XRegionStatistics stats;

    /**
     * json config
     */
    private final JsonConfig jsonConf;

    /**
     * encryption and decryption, null if encryption disabled
     */
    private final AESCipherHelper encryptor;

    /**
     * test hook for unit test only
     */
    private final ConcurrentMap<TableInitState, TestHook<TableInitState>>
        ckptHook = new ConcurrentHashMap<>();

    /**
     * Cached table instances from remote regions, mapped from the remote
     * region name to a map from table name to the table instance. The cached
     * table is added when the table is found from the remote region, and
     * removed when 1) the table is removed from the stream from the remote
     * region or 2) the agent of the remote region is removed when all cached
     * tables from that region are removed.
     */
    private final Map<String, Map<String, Table>> remoteTables =
        new ConcurrentHashMap<>();

    /**
     * system table for table initialization checkpoint, initialized when used
     */
    private volatile Table ticSysTable = null;

    /**
     * System table containing MR table information. The field may be null
     * during upgrade.
     */
    private volatile XRegionTableInfo xRegionTableInfo = null;

    /**
     * remember dropped tables up to a limit, for internal use only
     */
    private static final int MAX_RECORDED_DROPPED_TABLE = 1024;

    /**
     * all locally dropped tables, mapping from table id to table name
     */
    private final Map<Long, String> allLocalDroppedTables =
        new ConcurrentHashMap<>();

    /**
     * parent xregion service
     */
    private final XRegionService parent;

    /**
     * Constructs metadata manager
     *
     * @param sid      subscriber id
     * @param jsonConf json config
     * @param logger   private logger
     */
    public ServiceMDMan(NoSQLSubscriberId sid,
                        JsonConfig jsonConf,
                        Logger logger) {
        this(null, sid, jsonConf, false, logger);
    }

    /**
     * Unit test only
     */
    public ServiceMDMan(NoSQLSubscriberId sid,
                        JsonConfig jsonConf,
                        boolean unitTest,
                        Logger logger) {
        this(null, sid, jsonConf, unitTest, logger);
    }

    /**
     * Constructs metadata manager
     *
     * @param sid      subscriber id
     * @param jsonConf json config
     * @param unitTest true if in unit test
     * @param logger   private logger
     */
    public ServiceMDMan(XRegionService parent,
                        NoSQLSubscriberId sid,
                        JsonConfig jsonConf,
                        boolean unitTest,
                        Logger logger) {

        this.parent = parent;
        this.sid = sid;
        this.jsonConf = jsonConf;
        this.unitTest = unitTest;
        this.logger = logger;
        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS,
                                            RL_MAX_NUM_OBJECTS,
                                            logger);
        rlLoggerLong = new RateLimitingLogger<>(RL_LOG_PERIOD_LONG_MS,
                                                RL_MAX_NUM_OBJECTS,
                                                logger);

        /* create region map */
        regionMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Arrays.stream(jsonConf.getRegions())
              .forEach(r -> regionMap.put(r.getName(), r));

        /* create kvs for served store */
        if (jsonConf.isSecureStore()) {
            final File file = new File(jsonConf.getSecurity());
            if (!file.exists()) {
                throw new IllegalStateException("Security file not found: " +
                                                jsonConf.getSecurity());
            }
        }

        /* init region to kvs map */
        servRegion = new RegionInfo(jsonConf.getRegion(),
                                    jsonConf.getStore(),
                                    jsonConf.getHelpers());
        if (jsonConf.isSecureStore()) {
            servRegion.setSecurity(jsonConf.getSecurity());
        }
        final KVStore kvs;
        try {
            kvs = createKVS(servRegion);
        } catch (Exception exp) {
            /* cannot reach local region, upper service will retry */
            throw new UnreachableHostRegionException(servRegion, exp);
        }
        allKVS.put(servRegion, kvs);

        /* will be initialized when used */
        ridMapping = null;

        /* init region id translation */
        ridTrans = new RegionIdTrans(this, logger);
        if (jsonConf.getEncryptTableCheckpoint() && jsonConf.isSecureStore()) {
            final String path = jsonConf.getPasswdFile();
            encryptor = new AESCipherHelper(path, logger);
            logger.info(lm("Encryptor created from file=" + path));
        } else {
            encryptor = null;
        }

        /* still closed, will be opened after initialization */
        closed = true;
    }

    /**
     * Gets the agent subscriber id
     *
     * @return the agent subscriber id
     */
    public NoSQLSubscriberId getSid() {
        return sid;
    }

    /**
     * Translates the region id from streamed operation to localized region id.
     *
     * @param source source where the op streamed from
     * @param rid    region id in streamed op
     * @return localized region id
     */
    public int translate(String source, int rid) {
        if (unitTest) {
            /* unit test without translation, create a random id > 1 */
            return 2;
        }
        if (!ridTrans.isReady()) {
            throw new IllegalStateException("Region id translation is not " +
                                            "available.");
        }
        return ridTrans.translate(source, rid);
    }

    /**
     * Returns the KVStore handle of the service region
     *
     * @return the KVStore handle of the service region
     */
    public KVStore getServRegionKVS() {
        return allKVS.get(servRegion);
    }

    /**
     * Gets an KVStore handle for a region, or create a new kvstore handle if
     * it does not exist.
     *
     * @param region a region
     * @return kvstore handle of the region
     */
    public KVStore getRegionKVS(RegionInfo region) {
        return allKVS.computeIfAbsent(region, u -> createKVS(region));
    }

    /**
     * Dumps property
     */
    public static String dumpProperty(Properties prop) {
        final StringBuilder sp = new StringBuilder("\n");
        for (Object key : prop.keySet()) {
            sp.append(key).append("=")
              .append(prop.getProperty((String) key))
              .append("\n");
        }
        return sp.toString();
    }

    /**
     * Shuts down the metadata manger. Clear all cached tables and close all
     * kvstore handles.
     */
    public void shutdown() {
        closed = true;
        synchronized (this) {
            notifyAll();
        }
        allKVS.values().forEach(KVStore::close);
        pitrTables.clear();
        ridTrans.close();
        logger.fine(() -> lm("Service md manager id=" + sid + " has closed"));
    }

    /**
     * Returns true if the service md manager has closed
     *
     * @return true if the service md manager has closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Return existing tables at source region within timeout
     *
     * @param source    source regions
     * @param tables    tables
     * @param timeoutMs timeout in ms
     * @return set of found tables, or empty set if no table is found
     */
    public Set<String> tableExists(RegionInfo source,
                                   Set<String> tables,
                                   long timeoutMs) {
        final Set<String> found = new HashSet<>();
        final boolean succ =
            new PollCondition(POLL_INTERVAL_MS, timeoutMs) {
                @Override
                protected boolean condition() {
                    found.addAll(checkTable(source, tables));
                    return tables.equals(found);
                }
            }.await();
        if (!succ) {
            logger.fine(() -> lm("Timeout(ms)=" + timeoutMs +
                                 " in waiting for tables=" + tables +
                                 " to appear in region=" + source +
                                 ", found=" + found));
        }

        return found;
    }

    /**
     * Checks if tables have compatible schema at source and target
     *
     * @param source source region
     * @param tables tables to check, table not found at either region will
     *               be removed
     * @return set of tables which either has mismatching schema, or are
     * missing at the source region
     */
    public Set<String> matchingSchema(RegionInfo source, Set<Table> tables) {

        /* remoted table that are dropped */
        final Set<String> remoteDropped = new HashSet<>();
        /* table exists on both sides but with mismatch schema */
        final Set<String> incompatible = new HashSet<>();
        /* table with compatible schema */
        final Set<String> compatible = new HashSet<>();

        for (Table tgtTable : tables) {
            final String tb = tgtTable.getFullNamespaceName();
            final Table srcTable = getTableFromRegionRetry(source, tb);
            if (srcTable == null) {
                remoteDropped.add(tb);
                logger.warning(lm("Fail to match schema, remote table=" + tb +
                                  " dropped at region=" + source.getName()));
                remoteDropped.add(tb);
                continue;
            }

            final String reason = compatibleSchema(srcTable, tgtTable);
            if (reason != null) {
                final String msg = "Table=" + tb + " with mismatched schema" +
                                   " at local region=" +
                                   getServRegion().getName() +
                                   " and at remote region=" +
                                   source.getName() +
                                   ", details=[" + reason + "]";
                logger.warning(lm(msg));
                incompatible.add(tb);
                continue;
            }

            /* table with compatible schema */
            compatible.add(tb);
        }

        logger.info(lm("Tables with compatible schema=" + compatible +
                       " at region=" + source.getName() +
                       (incompatible.isEmpty() ? "" :
                           ", incompatible=" + incompatible) +
                       (remoteDropped.isEmpty() ? "" :
                           ", dropped(remote)=" + remoteDropped)));

        /* all tables that need to add to polling thread */
        remoteDropped.addAll(incompatible);
        return remoteDropped;
    }

    /**
     * Returns true if the primary key of actual table does not match that of
     * expected table, or false otherwise.
     *
     * @param expected expected table
     * @param actual   actual table
     * @return Returns true if the primary key does not match, or false
     * otherwise.
     */
    public boolean isPKeyMismatch(Table expected, Table actual) {

        final List<String> expPk = expected.getPrimaryKey();
        final List<String> actPk = actual.getPrimaryKey();

        if (expPk.size() != actPk.size()) {
            logger.fine(() -> lm("Mismatch # cols in primary key " +
                                 ", expect=" + expPk.size() +
                                 ", while actual=" + actPk.size()));
            return true;
        }

        /* name of col must match */
        if (!actPk.containsAll(expPk)) {
            logger.fine(() -> lm("Mismatch col names in primary key " +
                                 ", expect=" + expPk +
                                 ", while actual=" + actPk));
            return true;
        }

        /* col type must match */
        final Set<String> mismach = expPk.stream().filter(
            col -> !expected.getField(col).getType()
                            .equals(actual.getField(col).getType()))
                                         .collect(Collectors.toSet());
        if (!mismach.isEmpty()) {
            logger.fine(() -> lm("Mismatch type for columns=" + mismach));
            return true;
        }

        return false;
    }

    /**
     * Returns the configuration from JSON
     *
     * @return the configuration from JSON
     */
    public JsonConfig getJsonConf() {
        return jsonConf;
    }

    /**
     * Initializes by fetching the multi-region tables from served region,
     * shall be called outside the constructor and before the metadata
     * manager is used
     */
    public void initialize() {

        /* open for business, allow fetch tables from local store */
        closed = false;

        /* first initialize the MR table info system table */
        xRegionTableInfo = new XRegionTableInfo(this, logger);
        xRegionTableInfo.initialize();

        /* initialize table init checkpoint system table */
        final String tableName = MRTableInitCkptDesc.TABLE_NAME;
        ticSysTable = getSysTable(tableName);
        if (ticSysTable == null) {
            throw new IllegalStateException(
                "Cannot find the table init checkpoint system table=" +
                tableName + " from region=" + servRegion.getName() +
                ", store=" + servRegion.getStore());
        }
        logger.fine(() -> lm("Found table init checkpoint system table=" +
                             ticSysTable.getFullNamespaceName() + " at " +
                             "region=" + servRegion.getName() +
                             ", store=" + servRegion.getStore()));

        logger.info(lm("Service metadata manager initialized"));
    }

    /**
     * Checks if source and target table have compatible schemas
     *
     * @param src source table
     * @param tgt target table
     * @return null if compatible, or a description of incompatibility
     */
    private String compatibleSchema(Table src, Table tgt) {
        /* primary key must match */
        if (isPKeyMismatch(src, tgt)) {
            return "Primary key does not match, source pkey hash=" +
                   src.createPrimaryKey().toJsonString(false).hashCode() +
                   ", target pkey hash=" +
                   tgt.createPrimaryKey().toJsonString(false).hashCode();
        }

        /* check other fields */
        final List<String> srcCols = src.getFields();
        final List<String> tgtCols = tgt.getFields();
        /* for common col name, type must match */
        final List<String> intersect = srcCols.stream()
                                              .filter(tgtCols::contains)
                                              .collect(Collectors.toList());
        for (String col : intersect) {
            final FieldDef srcFd = src.getField(col);
            final FieldDef tgtFd = tgt.getField(col);
            if (!srcFd.equals(tgtFd)) {
                return "col=" + col +
                       ", type at source=" + getTypeTrace(srcFd) +
                       ", type at target=" + getTypeTrace(tgtFd);
            }
        }
        return null;
    }

    private String getTypeTrace(FieldDef fd) {
        return fd.getType() +
               (fd.getType().equals(FieldDef.Type.JSON) ?
                   "(has CRDT=" + ((FieldDefImpl) fd).hasJsonMRCounter() + ")"
                   : "");
    }

    /**
     * Returns region with given region name, or null if the region does not
     * exist
     *
     * @param regionName region name
     * @return region
     */
    RegionInfo getRegion(String regionName) {
        return regionMap.get(regionName);
    }

    /**
     * Gets the set of source regions for multi-region tables
     *
     * @return the set of source regions
     */
    public Collection<RegionInfo> getSrcRegionsForMRT() {
        return regionMap.values();
    }

    /**
     * Gets the region that the agent services
     *
     * @return the region that the agent services
     */
    public RegionInfo getServRegion() {
        return servRegion;
    }

    /**
     * Initialize region translation table
     *
     * @param regions set of regions
     */
    void initRegionTransTable(Set<RegionInfo> regions) {
        ridTrans.initRegionTransTable(regions);
    }

    /**
     * Returns a map from region to a set of mrt on that region
     */
    Map<RegionInfo, Set<Table>> getRegionMRTMap(Set<Table> mrts) {
        final Map<RegionInfo, Set<Table>> r2t = new TreeMap<>();
        for (Table t : mrts) {
            final Set<Integer> ids = ((TableImpl) t).getRemoteRegions();
            final Set<String> src = new HashSet<>();
            for (int id : ids) {
                final String region = getRegionName(id);
                if (region == null) {
                    final String err = "Unknown region in table md, " +
                                       " region id=" + id +
                                       " table=" + t.getFullNamespaceName();
                    logger.warning(lm(err));
                    throw new ServiceException(sid, err);
                }
                src.add(region);
            }

            src.stream()
               .map(regionMap::get)
               .forEach(r -> r2t.computeIfAbsent(r, u -> new HashSet<>())
                                .add(t));

        }
        return r2t;
    }

    /**
     * Returns region name from region id, or null if region id is unknown
     *
     * @param id region id
     * @return region name or null
     */
    String getRegionName(int id) {
        if (ridMapping == null) {
            final KVStore kvs = getServRegionKVS();
            ridMapping =
                new RegionIdMapping(servRegion.getName(), kvs, sid, logger);
            if (ridMapping.getKnownRegions() == null) {
                return null;
            }
            final Set<String> sb = new HashSet<>();
            ridMapping.getKnownRegions().values()
                      .forEach(r -> {
                          final String map =
                              r + " -> " + ridMapping.getRegionIdByName(r);
                          sb.add(map);
                      });
            logger.info(lm("Create region id mapping for served region=" +
                           servRegion.getName() + ", mapping=" + sb));
        }
        return ridMapping.getRegionName(id);
    }

    /**
     * Removes multi-region tables
     *
     * @param tables multi-region tables to remove
     */
    public void dropMRTable(Set<Table> tables) {
        if (closed) {
            throw new IllegalStateException("Metadata manager closed");
        }
        if (tables.isEmpty()) {
            return;
        }
        final Set<String> tbls =
            tables.stream().map(Table::getFullNamespaceName)
                  .collect(Collectors.toSet());
        tables.forEach(t -> stats.removeTableMetrics(t.getFullNamespaceName()));
        logger.info(lm("Table=" + tbls + " removed from stats"));

        /* get set of non-served regions from which no table is streamed */
        final Set<RegionInfo> regions =
            parent.getAllAgents().stream()
                  .map(RegionAgentThread::getSourceRegion)
                  .collect(Collectors.toSet());
        final Set<RegionInfo> noStreamRegion =
            allKVS.keySet().stream()
                  .filter(r -> !regions.contains(r) && !r.equals(servRegion))
                  .collect(Collectors.toSet());

        noStreamRegion.forEach(r -> {
            /* this region no longer has any mrt */
            allKVS.get(r).close();
            allKVS.remove(r);
            logger.fine(() -> lm("KVStore to region=" + r.getName() +
                                 " closed."));
        });
    }

    /**
     * Adds PITR tables
     *
     * @param tables tables to add
     */
    public void addPITRTable(Set<Table> tables) {
        if (closed) {
            throw new IllegalStateException("Metadata manager closed");
        }
        if (tables.isEmpty()) {
            return;
        }
        final Set<String> tableNames =
            tables.stream().map(Table::getFullNamespaceName)
                  .collect(Collectors.toSet());
        final Set<String> toAdd =
            tableNames.stream().filter(t -> !pitrTables.containsKey(t))
                      .collect(Collectors.toSet());
        for (String t : toAdd) {
            final Table table = getLocalTableRetry(t);
            if (table == null) {
                final String err = "PITR Table=" + t + " cannot be found";
                logger.warning(lm(err));
                continue;
            }
            pitrTables.put(t, table);
        }
        logger.fine(() -> lm("PITR table=" + tableNames + " added"));
    }

    /**
     * Removes a PITR table
     *
     * @param tables PITR tables to remove
     */
    public void removePITRTable(Set<Table> tables) {
        if (closed) {
            throw new IllegalStateException("Metadata manager closed");
        }
        if (tables.isEmpty()) {
            return;
        }
        final Set<String> tableNames =
            tables.stream().map(Table::getFullNamespaceName)
                  .collect(Collectors.toSet());
        final Set<String> toRemove =
            tableNames.stream().filter(pitrTables::containsKey)
                      .collect(Collectors.toSet());
        toRemove.forEach(pitrTables::remove);
        logger.fine(() -> lm("PITR table=" + tableNames + " removed"));
    }

    /**
     * Gets a set of PITR table names
     *
     * @return PITR tables
     */
    public Set<Table> getPITRTables() {
        return new HashSet<>(pitrTables.values());
    }

    /**
     * Unit test only
     * Gets a set of MR table names
     *
     * @return MRT table names
     */
    public Set<String> getMRTNames() {
        return getMRTables().stream()
                            .map(Table::getFullNamespaceName)
                            .collect(Collectors.toSet());
    }

    /**
     * Gets a collection of MR tables
     *
     * @return MRT tables
     */
    Collection<Table> getMRTables() {
        if (parent == null) {
            /* unit test only, pull from store */
            final TableAPIImpl tapi =
                (TableAPIImpl) getServRegionKVS().getTableAPI();
            return tapi.getMultiRegionTables(true);
        }

        final Set<Table> ret = new HashSet<>();
        regionMap.keySet().forEach(r -> {
            final RegionAgentThread rt = parent.getRegionAgent(r);
            if (rt != null) {
                ret.addAll(rt.getTgtTables());
            }
        });
        return ret;
    }

    /**
     * Fetches the table instances from local store
     *
     * @param tables tables to fetch
     * @return arrays of fetched tables
     */
    public Table[] fetchTables(String[] tables) {
        return Arrays.stream(tables)
                     .distinct()
                     .map(this::getLocalTableRetry)
                     .filter(Objects::nonNull)
                     .toArray(Table[]::new);
    }

    /**
     * Unit test only
     * <p>
     * Returns the MR table instance, or null if the table cannot be found
     *
     * @param tbName table name
     * @return table instance
     */
    public Table getMRT(String tbName, String region) {
        final RegionAgentThread rt = parent.getRegionAgent(region);
        if (rt == null) {
            return null;
        }
        return rt.getTgtTable(tbName);
    }

    /**
     * Sets the statistics
     */
    public void setStats(XRegionStatistics stats) {
        if (stats == null) {
            throw new IllegalArgumentException("Null statistics");
        }
        this.stats = stats;
    }

    /**
     * Unit test only
     */
    RegionIdTrans getRidTrans() {
        return ridTrans;
    }

    /*----------- PRIVATE FUNCTIONS --------------*/
    private String lm(String msg) {
        return "[MdMan-" + sid + "] " + msg;
    }

    /* trace header for static mehtod */
    private static String lms(String msg) {
        return "[MdMan] " + msg;
    }

    /**
     * Creates kvstore handle to a region
     *
     * @param region region to create kvstore
     * @return kvstore handle
     */
    private KVStore createKVS(RegionInfo region) {
        final boolean local = servRegion.equals(region);
        final KVStore ret = createKVS(region.getName(),
                                      region.getStore(),
                                      region.getHelpers(),
                                      (region.isSecureStore() ?
                                          new File(region.getSecurity()) :
                                          null),
                                      local);
        if (!ensureEE(ret, region)) {
            final String err =
                "XRegion Service cannot connect to region=" + region.getName() +
                ", store=" + region.getStore() +
                " because at least one SN is not on Enterprise Edition.";
            logger.warning(lm(err));
            throw new IllegalStateException(err);
        }
        // TODO - investigate using the cache within the table API
        /* Service maintains it's own cache */
        ((TableAPIImpl) ret.getTableAPI()).setCacheEnabled(false);
        return ret;
    }

    /**
     * Creates kvstore handle to a region
     *
     * @param regionName   region name
     * @param storeName    store name
     * @param helpers      helper hosts
     * @param securityFile security file, or null if non-secure
     * @param localRegion  true if store is from local region, false if remote
     * @return kvstore handle
     */
    private KVStore createKVS(String regionName,
                              String storeName,
                              String[] helpers,
                              File securityFile,
                              boolean localRegion) {

        final KVStoreConfig conf = new KVStoreConfig(storeName, helpers);
        conf.setRequestLimit(computeRequestLimitConfig());
        final boolean security = (securityFile != null);
        if (security) {
            final Properties sp =
                XRegionService.setSecureProperty(conf, securityFile);
            logger.fine(() -> lm("Set security property=" + dumpProperty(sp)));
        }

        try {
            final KVStore ret = createKVSRetry(conf, regionName, localRegion);
            if (localRegion) {
                logger.info(lm("KVStore created for local " +
                               "region=" + servRegion.getName() +
                               ", store=" + conf.getStoreName() +
                               ", requestLimitConfig=" +
                               conf.getRequestLimit()));
            } else {
                logger.fine(() -> lm("KVStore created for store=" +
                                     conf.getStoreName()));
            }
            return ret;
        } catch (Exception ex) {
            final String msg = "Cannot connect Oracle NoSQL store=" +
                               conf.getStoreName() + " at " +
                               (localRegion ? "local" : "remote") +
                               " region=" + regionName;
            logger.warning(lm(msg + ", error=" + ex));
            throw new IllegalStateException(
                msg  + ", error=" + exceptionString(ex), ex);
        }
    }

    /**
     * Creates kvstore handle, retry after sleep if store is not reachable.
     *
     * @param conf configuration of the kvstore
     * @param regionName region name
     * @param localRegion true if it is a local region, false otherwise
     * @return a kvstore handle
     * @throws InterruptedException if interrupted in waiting.
     */
    private KVStore createKVSRetry(KVStoreConfig conf,
                                   String regionName,
                                   boolean localRegion)
        throws InterruptedException {
        while (!closed) {
            try {
                return KVStoreFactory.getStore(conf);
            } catch (FaultException fe) {
                final String msg = "Cannot connect Oracle NoSQL store=" +
                                   conf.getStoreName() + " at " +
                                   (localRegion ? "local" : "remote") +
                                   " region=" + regionName +
                                   ", will retry in seconds=" +
                                   CREATE_KVS_SLEEP_SECS;
                rlLoggerLong.log(regionName, Level.WARNING, msg);
                synchronized (this) {
                    wait(CREATE_KVS_SLEEP_SECS * 1000);
                }
            }
        }

        /* closed */
        throw new IllegalStateException("in shutdown");
    }

    /***
     * Computes the request limit configuration for kvstore.
     *
     * @return the request limit configuration
     */
    private RequestLimitConfig computeRequestLimitConfig() {
        //TODO: the computation is for local region but used by both local
        // and remote regions. May want to compute a reasonable configuration
        // for remote region separately.

        /* max # of concurrent requests of stream ops */
        final int streamOpsRequests = jsonConf.getNumConcurrentStreamOps();
        /* total # of concurrent requests */
        final int totalRequests =
            DEFAULT_MAX_CONCURRENT_OTHER_REQUESTS + streamOpsRequests;
        /*
         * max active requests should be big enough such that request
         * threshold percentage of that number is big enough to hold
         * all concurrent requests
         */
        final int maxActiveRequests = totalRequests * 100 /
                                      DEFAULT_REQUEST_THRESHOLD_PERCENT;
        return new RequestLimitConfig(maxActiveRequests,
                                      DEFAULT_REQUEST_THRESHOLD_PERCENT,
                                      DEFAULT_NODE_LIMIT_PERCENT);
    }

    /**
     * Fetches all multi-region tables that are subscribing to a remote region
     */
    Set<Table> fetchMRT() {
        final Set<Table> mrTables = new HashSet<>();
        final TableAPI tableAPI = getTableAPI();
        final List<Table> tables =
            ((TableAPIImpl) tableAPI).getMultiRegionTables(
                false /*includeLocalOnly*/);
        tables.forEach(table -> {
            final String name = table.getFullNamespaceName();
            if (verifyTableInfo(table.getFullNamespaceName(), servRegion)) {
                logger.fine(() -> lm("Verified table id for table=" + name +
                                     " at region=" + servRegion.getName()));
                mrTables.add(table);
            } else {
                logger.warning(lm("Fail to verify id of table=" + name +
                                  " at local region=" + servRegion.getName() +
                                  ", table can be dropped or recreated, " +
                                  "this table will not be included in " +
                                  "initialization for now. " +
                                  "If table is recreated, agent would " +
                                  "process the drop and create " +
                                  "requests to initialize" +
                                  " the recreated table later"));
            }
        });
        return mrTables;
    }

    /**
     * Returns a list of table names which are the tables in the input set
     * that were found at the specified region.
     *
     * @param region source region
     * @param tbls   tables
     * @return set of found tables, or empty set if no table is found
     */
    public Set<String> checkTable(RegionInfo region, Set<String> tbls) {

        /* return found tables */
        final Set<String> found = new HashSet<>();
        for (String tableName : tbls) {
            /* fetch the remote table with retry */
            final Table tb = getTableFromRegionRetry(region, tableName);
            final String reg = region.getName();
            final long recordId = getRecordedTableId(tableName, reg);
            if (tb == null) {
                /*
                 * if table not found in remote region, or remote region is not
                 * available, consider the table missing and the agent will
                 * check later
                 */
                if (recordId > 0) {
                    /*
                     * delete the recordId since the table is not found, or
                     * remote region is not available, ok if fail to delete
                     * it, since we will try to delete it later
                     */
                    removeRemoteTableId(tableName, reg);
                }
                continue;
            }

            final long tid = ((TableImpl) tb).getId();
            if (recordId == 0) {
                /* no record, first time see this table at remote region */
                recordRemoteTableId(tableName, reg, tid);
                logger.info(lm("No previous record, record table=" + tableName +
                               ", remote id=" + tid + ", region=" + reg));
            } else {
                /* saw this table at remote before, verify */
                if (recordId == tid) {
                    /* verified */
                    logger.fine(() -> "Table=" + tableName + " verified at " +
                                      "region=" + reg +
                                      "(tableId=" + tid + ")");
                } else {
                    /*
                     * Table recreated! drop the recorded id and treat it as
                     * missing table. In polling thread, the table will
                     * be initialized if it is found later
                     */
                    final String msg = "Table=" + tableName + " found at " +
                                       "region=" + reg + " but with " +
                                       "mismatched id(actual=" + tid +
                                       ", recorded=" + recordId + "), the " +
                                       "table has been recreated, delete " +
                                       "existing record";
                    removeRemoteTableId(tableName, reg);
                    logger.info(lm(msg));
                    /* do not include the table as found */
                    continue;
                }
            }

            /* remember the table instance */
            remoteTables.computeIfAbsent(region.getName(), u -> new HashMap<>())
                        .put(tableName, tb);
            logger.fine(() -> lm("Add remote table=" + tableName +
                                 "(remote id=" + ((TableImpl) tb).getId() +
                                 ") from region=" + region.getName()));
            found.add(tableName);
        }
        return found;
    }

    /**
     * Verifies the table info of given table at given region
     *
     * @param tableName name of table
     * @param region    region of the table
     * @return true if the table info verified, false otherwise
     */
    public boolean verifyTableInfo(String tableName, RegionInfo region) {
        if (xRegionTableInfo == null) {
            logger.warning(lm("MR table info not initialized"));
            return false;
        }
        final Table tbl = getTableFromRegionRetry(region, tableName);
        if (tbl == null) {
            logger.warning(lm("MR table=" + tableName + " has been dropped " +
                              "at region=" + region.getName()));
            return false;
        }

        return xRegionTableInfo.verifyTableInfo(tableName, region.getName(),
                                                ((TableImpl) tbl).getId());
    }

    /**
     * Gets the table id of a given MR table at a give region recorded in
     * system table
     *
     * @return remote table id or 0, if the table info is not available
     */
    public long getRecordedTableId(String tableName, String regionName) {
        if (xRegionTableInfo == null) {
            logger.warning(lm("MR table info not initialized, " +
                              "table=" + tableName + ", region=" + regionName));
            return 0;
        }
        return xRegionTableInfo.getRecordedTableId(tableName, regionName);
    }

    /**
     * Returns recorded local table id, or 0 if local table id is not recorded.
     *
     * @param tableName name of table
     * @return recorded local table id, or 0
     */
    public long getRecordedLocalTableId(String tableName) {
        final String region = getServRegion().getName();
        return getRecordedTableId(tableName, region);
    }

    /**
     * Returns the id of remote tables from given region
     *
     * @param region name of remote region
     * @param table  name of table
     * @return id of the remote table, or 0 if the table is not found
     */
    public long getRemoteTableId(String region, String table) {
        final Table tbl = getRemoteTable(region, table);
        if (tbl == null) {
            return 0;
        }
        return ((TableImpl) tbl).getId();
    }

    /**
     * Drops a table from a given region
     *
     * @param region  region of the table
     * @param table   table name
     * @return true if table dropped, false otherwise
     */
    public boolean dropTable(RegionInfo region, String table) {
        final KVStore kvs = getRegionKVS(region);
        if (kvs == null) {
            logger.info(lm("KVStore unavailable for region=" + region));
            return false;
        }
        if (kvs.getTableAPI().getTable(table) == null) {
            /* already dropped */
            return true;
        }
        final StatementResult sr = kvs.executeSync("DROP TABLE " + table);
        return sr.isSuccessful();
    }

    /**
     * Adds the table id for local table
     */
    public boolean recordLocalTableId(String table, long tableId) {
        if (xRegionTableInfo == null) {
            return false;
        }
        return xRegionTableInfo.insert(table, servRegion.getName(), tableId);
    }

    /**
     * Removes the table id for local table
     */
    boolean removeLocalTableId(String table) {
        if (xRegionTableInfo == null) {
            return false;
        }
        return xRegionTableInfo.delete(table, servRegion.getName());
    }

    /**
     * Adds the table id for the given table and region.
     */
    private void recordRemoteTableId(String table, String region, long tid) {
        if (xRegionTableInfo == null) {
            return;
        }
        xRegionTableInfo.insert(table, region, tid);
    }

    /**
     * Removes the table id information for the given table and region.
     * Usually the remote table id will be removed when 1) the table is
     * removed from the stream and we no longer need the info; or 2) when the
     * agent finds that the remote table is dropped during periodical check,
     * and the remote id is no longer valid.
     */
    public boolean removeRemoteTableId(String table, String regionName) {
        if (xRegionTableInfo == null) {
            return false;
        }
        return xRegionTableInfo.delete(table, regionName);
    }

    /**
     * Gets the table API for the local region.
     */
    public TableAPI getTableAPI() {
        return getServRegionKVS().getTableAPI();
    }

    /**
     * Returns the cached table instance from remote region, or null if the
     * table is not found in cache.
     *
     * @param region remote region name
     * @param table  table name
     * @return table instance from given region
     */
    public Table getRemoteTable(String region, String table) {
        final Map<String, Table> tbls = remoteTables.get(region);
        if (tbls == null) {
            return null;
        }
        return tbls.get(table);
    }

    /**
     * Removes cached remote table instance
     *
     * @param region remote region name
     * @param table  table name
     */
    public void removeRemoteTables(String region, String table) {
        final Map<String, Table> tbls = remoteTables.get(region);
        if (tbls == null) {
            return;
        }
        tbls.remove(table);
    }

    /**
     * Removes all cached tables from the remote region
     *
     * @param region remote region name
     */
    void removeRemoteTables(String region) {
        remoteTables.remove(region);
    }

    /* Exception when the host region is not reachable */
    public static class UnreachableHostRegionException
        extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final RegionInfo region;

        UnreachableHostRegionException(RegionInfo region, Throwable cause) {
            super(cause);
            this.region = region;
        }

        public RegionInfo getRegion() {
            return region;
        }
    }

    /**
     * Returns true if all SNs at a store are on EE, false otherwise
     *
     * @param kvs    kvstore
     * @param region region
     * @return true if all SNs at a store are on EE, false otherwise
     */
    private boolean ensureEE(KVStore kvs, RegionInfo region) {
        final RegistryUtils regUtil =
            ((KVStoreImpl) kvs).getDispatcher().getRegUtils();
        if (regUtil == null) {
            final String msg = "The request dispatcher has not initialized " +
                               "itself yet, region=" + region.getName() +
                               ", store=" + region.getStore();
            logger.warning(lm(msg));
            return false;
        }
        /* Since SNs can be mixed, ping each SN */
        for (StorageNodeId snId : regUtil.getTopology().getStorageNodeIds()) {
            try {
                final KVVersion ver = regUtil.getStorageNodeAgent(snId)
                                             .ping().getKVVersion();
                if (!VersionUtil.isEnterpriseEdition(ver)) {
                    logger.warning(lm("In region=" + region.getName() +
                                      ", store=" + region.getStore() +
                                      ", sn=" + snId +
                                      " is not running Enterprise Edition, " +
                                      "ver=" + ver));
                    return false;
                }
            } catch (RemoteException | NotBoundException e) {
                logger.fine(() -> lm("Cannot reach region=" + region.getName() +
                                     ", store= " + region.getStore() +
                                     ", sn=" + snId));
            }
        }
        return true;
    }

    /**
     * Gets table with retry from store. In some cases, table exists in store
     * but TableAPI.getTable() may return null. Retry till the table is
     * found, or it has reached max attempts. If it is unable to retrieve the
     * table because of {@link FaultException}, it would retry as well.
     *
     * @param region name of region to get table
     * @param table table name
     * @param max max number of attempts
     * @return table instance, or null if max attempts is reached
     */
    private Table getTableRetry(RegionInfo region, String table, long max) {
        final long sleepMs = DEFAULT_GET_TABLE_RETRY_SLEEP_MS;
        final TableAPI tableAPI = getRegionKVS(region).getTableAPI();
        Table ret = null;
        int count = 0;
        final long ts = System.currentTimeMillis();
        while (count < max && !closed) {
            count++;
            try {
                ret = tableAPI.getTable(table);
                if (ret != null) {
                    break;

                }
                rlLogger.log(table, Level.INFO,
                             lm("Cannot find table=" + table +
                                " from region=" + region.getName() +
                                ", retry sleep(ms)=" + sleepMs +
                                ", max=" + max));
                try {
                    synchronized (this) {
                        wait(sleepMs);
                        if (closed) {
                            break;
                        }
                    }
                } catch (InterruptedException ie) {
                    logger.fine(() -> lms("Interrupted in reading table=" +
                                          table));
                    break;
                }
            } catch (FaultException fe) {
                logger.fine(() -> lms("Retry get table on fault exception=" +
                                      fe));
            }
        }

        if ((ret == null) && !closed) {
            logger.fine(() -> lm("Cannot find table=" + table + " in time(ms)" +
                                 "=" + (System.currentTimeMillis() - ts)));
        }
        return ret;
    }

    /**
     * Gets table from local region without sleep
     */
    Table getLocalTableRetry(String table) {
        return getLocalTableRetry(table, MAX_PULL_TABLE_RETRY);
    }

    /**
     * Gets table from local region with sleep time
     */
    public Table getLocalTableRetry(String table, long max) {
        return getTableRetry(servRegion, table, max);
    }

    /**
     * Gets table from a particular region with retry
     */
    public Table getTableFromRegionRetry(RegionInfo region, String table) {
        return getTableRetry(region, table, MAX_PULL_TABLE_RETRY);
    }

    /**
     * Writes table initialization checkpoint with given state, if fail to
     * write the checkpoint, log the failure
     */
    public boolean writeTableInitCkpt(String sourceRegion,
                                      String tableName,
                                      long tableId,
                                      PrimaryKey pkey,
                                      TableInitState state,
                                      String errMsg) {
        final TableInitCheckpoint ckpt =
            buildCkpt(sourceRegion, tableName, tableId, pkey, state);
        ckpt.setMessage(errMsg);
        if (pkey != null) {
            ckpt.setPrimaryKey(encrypt(pkey.toJsonString(false)));
        }
        final Optional<Version> result = TableInitCheckpoint.write(this, ckpt);
        if (result.isEmpty()) {
            logger.info(lm("Fail to write checkpoint at " +
                           "region=" + getServRegion().getName() +
                           ", ckpt=" + ckpt));
            return false;
        }
        final Version ver = result.get();
        final String msg = "Done checkpoint at region=" +
                           getServRegion().getName() +
                            ", table=" + tableName + "(tid=" + tableId +
                            ", remote tid=" + ckpt.getRemoteTableId() + ")" +
                            ", state=" + ckpt.getState() +
                           /* more trace if fine logging */
                           (logger.isLoggable(Level.FINE) ?
                               ", ver=(vlsn=" + ver.getVLSN() +
                               ", rn=" + ver.getRepNodeId() +
                               "), ckpt=" + ckpt : "");
        logger.info(lm(msg));
        assert TestHookExecute.doHookIfSet(ckptHook.get(state), state);
        return true;
    }

    /**
     * Reads table initialization checkpoint with given source region and
     * table name
     *
     * @param src   source region
     * @param table table name
     * @return checkpoint or null if not exist
     */
    public TableInitCheckpoint readTableInitCkpt(String src, String table) {
        final Optional<TableInitCheckpoint> opt =
            TableInitCheckpoint.read(this, sid.toString(), src, table);
        if (opt.isEmpty()) {
            return null;
        }
        final TableInitCheckpoint ckpt = opt.get();
        if (ckpt.getPrimaryKey() != null) {
            ckpt.setPrimaryKey(decrypt(ckpt.getPrimaryKey()));
        }
        return ckpt;
    }

    /**
     * Deletes checkpoint from system table
     *
     * @param region source region
     * @param table  table name
     */
    public void delTableInitCkpt(String region, String table) {
        final String agentId = sid.toString();
        TableInitCheckpoint.del(this, agentId, region, table);
        logger.info(lm("Table initialization checkpoint deleted" +
                       ", table=" + table + ", region="  + region));
    }

    /**
     * Deletes all table checkpoints for a given region
     *
     * @param region region name
     */
    void delAllInitCkpt(String region) {
        delInitCkpt(region,
                    null /* all tables checkpoints */,
                    false /* delete my own checkpoints */);
    }

    /**
     * Deletes table checkpoints for all agents for a given region and for the
     * given table, or for all tables if table is null.
     *
     * @param region region name
     * @param table  table name, or null if delete all table checkpoints
     * @param keepMyCkpt true if keep checkpoints from myself, false otherwise
     */
    public void delInitCkpt(String region, String table, boolean keepMyCkpt) {
        final TableAPI tableAPI = getTableAPI();
        final TableIterator<Row> itr =
            tableAPI.tableIterator(ticSysTable.createPrimaryKey(), null, null);
        while (itr.hasNext()) {
            final Row row = itr.next();
            final String id = row.get(COL_NAME_AGENT_ID).asString().get();
            if (keepMyCkpt && id.equals(sid.toString())) {
                /* keep checkpoint made by myself */
                continue;
            }

            final String tb = row.get(COL_NAME_TABLE_NAME).asString().get();
            final String src = row.get(COL_NAME_SOURCE_REGION).asString().get();
            if (src.equals(region)) {
                if (table == null /* all tables */ || table.equals(tb)) {
                    /* must ensure the checkpoint is deleted */
                    deleteRetry(row.createPrimaryKey());
                }
            }
        }
        logger.info(lm("Deleted all table init checkpoints from" +
                       " region=" + region +
                       "(include my ckpt?=" + keepMyCkpt + ")" +
                       (table == null ? " for all tables" :
                           ", table=" + table)));
    }

    /**
     * Gets the set of tables that need to resume initialization from the
     * remote region
     *
     * @param region remote region
     * @return set of tables
     */
    public Set<Table> getTablesResumeInit(String region) {
        final Set<Table> ret = new HashSet<>();
        final Set<TableInitCheckpoint> ckpts = readInitCkptWithRetry(region);
        final Set<String> dropped = new HashSet<>();
        final Set<String> complete = new HashSet<>();
        final Set<String> recreated = new HashSet<>();
        final Set<String> noregion = new HashSet<>();
        for (TableInitCheckpoint ckpt : ckpts) {
            final String tableName = ckpt.getTable();
            final long tidCkpt = ckpt.getTableId();
            final TableImpl table = (TableImpl) getLocalTableRetry(tableName);
            if (table == null) {
                /* the table has been dropped, delete ckpt */
                delTableInitCkpt(region, tableName);
                dropped.add(tableName);
                continue;
            }

            /* table id in ckpt and table md must match */
            if (tidCkpt != table.getId()) {
                final String msg =
                    "To delete checkpoint because table=" + tableName +
                    " is recreated with table id=" + table.getId() +
                    ", while in checkpoint table id=" + tidCkpt;
                logger.fine(() -> lm(msg));
                delTableInitCkpt(region, tableName);
                recreated.add(tableName);
                continue;
            }

            final boolean foundRegion =
                table.getRemoteRegions().stream().map(this::getRegionName)
                     .anyMatch(region::equals);
            if (!foundRegion) {
                /* the table no longer stream from the region, delete ckpt */
                logger.fine(() -> lm("Table=" + tableName + " no longer " +
                                     "stream from  the region=" + region + "," +
                                     " delete ckpt"));
                delTableInitCkpt(region, tableName);
                noregion.add(tableName);
                continue;
            }

            if (ckpt.getState().equals(COMPLETE)) {
                /* table initialization is complete */
                logger.fine(() -> lm("Table=" + tableName + " initialization" +
                                     " is complete, ckpt=" + ckpt));
                complete.add(tableName);
                continue;
            }

            /* table need to resume from checkpoint */
            ret.add(table);
        }

        logger.info(lm("Tables to resume initialization=" +
                       ret.stream().map(Table::getFullNamespaceName)
                          .collect(Collectors.toSet()) +
                       ", tables initialization completed=" + complete +
                       ", checkpoints deleted for tables:" +
                       " dropped=" + dropped +
                       ", recreated=" + recreated +
                       ", region removed=" + noregion));
        return ret;
    }

    /**
     * Reads all table init checkpoints for a given region, retry if running
     * into error
     *
     * @param region remote region
     * @return set of table checkpoints
     */
    private Set<TableInitCheckpoint> readInitCkptWithRetry(String region) {
        int attempt = 0;
        while (!closed) {
            try {
                attempt++;
                final Set<TableInitCheckpoint> ret = readAllInitCkpt(region);
                final int finalAttempt = attempt;
                logger.fine(() -> lm("Fetched all table init checkpoint for " +
                                     "region=" + region +
                                     ", attempts=" + finalAttempt));
                return ret;
            } catch (StoreIteratorException | FaultException exp) {
                logger.info(lm("Rescan the table checkpoints" +
                               ", after sleep ms=" + POLL_INTERVAL_MS +
                               ", region=" + region +
                               ", attempt=" + attempt +
                               ", error=" + exp +
                               ", cause=" + exp.getCause()));
                try {
                    synchronized (this) {
                        wait(POLL_INTERVAL_MS);
                    }
                } catch (InterruptedException e) {
                    logger.info(lm("Interrupted in sleeping in reading table" +
                                   " init checkpoint for region=" + region +
                                   ", attempts=" + attempt));
                    break;
                }
            }
        }
        logger.info(lm("Return empty table init checkpoint for " +
                       "region=" + region + ", service md manager" +
                       " closed=" + closed + ", attempts=" + attempt));
        return Collections.emptySet();
    }

    /**
     * Writes NOT_START checkpoint. For correctness, the NOT_START checkpoint
     * has to be persisted before the table starts initialization, otherwise
     * the agent may not be able to resume initialization from failure.
     *
     * @param region source region
     * @param table  table name
     * @param tid    table id
     * @param pkey   primary key
     * @param st     state
     */
    public void writeCkptRetry(String region, String table, long tid,
                               PrimaryKey pkey, TableInitState st) {
        writeDelCkptRetry(region, table, tid, pkey, st, false);
    }

    /**
     * Deletes the checkpoint row with retry
     */
    private void deleteRetry(PrimaryKey pkey) {
        writeDelCkptRetry(null, null,
                          0 /* table id does not matter for delete */,
                          pkey, null, true);
    }

    /**
     * Writes or deletes a checkpoint row with retry
     */
    private void writeDelCkptRetry(String region, String table, long id,
                                   PrimaryKey pkey, TableInitState st,
                                   boolean delete) {
        int count = 0;
        do {
            count++;
            try {
                if (delete) {
                    final TableAPI api = getTableAPI();
                    if (api.delete(pkey, null, CKPT_TABLE_WRITE_OPT)) {
                        return;
                    }
                } else {
                    if (writeTableInitCkpt(region, table, id, pkey, st, null)) {
                        return;
                    }
                }
            } catch (Exception exp) {
                final String err = "Fail to " + (delete ? "delete" : "write") +
                                   " init checkpoint in attempts=" + count +
                                   ", will retry, error=" + exp;
                logger.warning(lm(err));
            }
        } while (!closed);
    }

    /**
     * Reads all table checkpoint for given region. The checkpoint can be
     * made by any agent
     */
    private Set<TableInitCheckpoint> readAllInitCkpt(String region) {
        final TableAPI tableAPI = getTableAPI();
        final String agentId = sid.toString();
        final Set<TableInitCheckpoint> ret = new HashSet<>();
        /* test hook to throw FaultException in creating table iterator */
        fireExpTestTook(FaultException.class.getSimpleName());
        final TableIterator<Row> itr =
            tableAPI.tableIterator(ticSysTable.createPrimaryKey(), null, null);
        while (itr.hasNext()) {
            /* test hook to throw store iterator exception in iteration */
            fireExpTestTook(StoreIteratorException.class.getSimpleName());
            final Row row = itr.next();
            final String src = row.get(COL_NAME_SOURCE_REGION).asString().get();
            if (region.equals(src)) {
                final String json =
                    row.get(COL_NAME_CHECKPOINT).asString().get();
                try {
                    final TableInitCheckpoint ckpt =
                        JsonUtils.readValue(json, TableInitCheckpoint.class);
                    ret.add(ckpt);
                } catch (RuntimeException jse) {
                    logger.warning(lm("Problem reading checkpoint for" +
                                      ", agent id=" + agentId +
                                      ", region=" + region +
                                      ", json string=" + json +
                                      ", error=" + jse));
                }
            }
        }
        return ret;
    }

    private void fireExpTestTook(String exp) {
        assert ExceptionTestHookExecute.doHookIfSet(expHook, exp);
    }

    private Table getSysTable(String tableName) throws IllegalStateException {
        while (!closed) {
            final Table ret =
                getLocalTableRetry(MRTableInitCkptDesc.TABLE_NAME);
            if (ret != null) {
                return ret;
            }
            logger.info(lm("System table=" + tableName + " not found at " +
                           "region=" + servRegion.getName() + ", waiting..."));
            try {
                synchronized (this) {
                    wait(SYS_TABLE_INTV_MS);
                    if (closed) {
                        return null;
                    }
                }
            } catch (InterruptedException exp) {
                final String err = "Interrupted in waiting for system table=" +
                                   tableName;
                logger.warning(lm(err));
                throw new IllegalStateException(err, exp);
            }
        }
        return null;
    }

    private TableInitCheckpoint buildCkpt(String srcRegion,
                                          String tableName,
                                          long tableId,
                                          PrimaryKey pkey,
                                          TableInitState st) {
        final String agentId = sid.toString();
        final String tgtRegion = servRegion.getName();
        final String json = (pkey == null) ? null : pkey.toJsonString(false);
        return new TableInitCheckpoint(agentId, srcRegion, tgtRegion,
                                       tableName, tableId,
                                       getRemoteTableId(srcRegion, tableName),
                                       json,
                                       System.currentTimeMillis(), st);
    }

    private String encrypt(String msg) {
        if (encryptor == null) {
            return msg;
        }
        return encryptor.encrypt(msg);
    }

    private String decrypt(String msg) {
        if (encryptor == null) {
            return msg;
        }
        return encryptor.decrypt(msg);
    }

    /**
     * unit test only
     */
    public void setCkptHook(TableInitState st, TestHook<TableInitState> hook) {
        ckptHook.put(st, hook);
    }

    /**
     * unit test only
     */
    public TestHook<TableInitState> getCkptHook(TableInitState state) {
        return ckptHook.get(state);
    }

    /**
     * Unit test only
     */
    public AESCipherHelper getEncryptor() {
        return encryptor;
    }

    /**
     * Unit test only
     */
    public Table getTicSysTable() {
        return ticSysTable;
    }

    /**
     * Adds a table to the dropped table list
     *
     * @param table table name
     * @param tid   table id
     */
    public void addDroppedTable(String table, long tid) {
        if (allLocalDroppedTables.containsKey(tid)) {
            /* already added to the list */
            return;
        }
        if (allLocalDroppedTables.size() == MAX_RECORDED_DROPPED_TABLE) {
            /* if full, remove the first key */
            final Long key = allLocalDroppedTables.keySet().iterator().next();
            final String tb = allLocalDroppedTables.remove(key);
            logger.info(lm("Remove table=" + tb + "(id=" + key +
                           ") from dropped table list"));
        }
        allLocalDroppedTables.put(tid, table);
        logger.info(lm("Add table=" + table + "(id=" + tid + ") to dropped " +
                       "table list"));
    }

    /**
     * Unit test only
     * Returns the set of dropped tables
     */
    Set<String> getAllDroppedTables() {
        return new HashSet<>(allLocalDroppedTables.values());
    }

    /**
     * Returns true if the table has been dropped
     *
     * @param tid table tid
     * @return true if the table has been dropped, false otherwise
     */
    public boolean isFromDroppedTable(long tid) {
        return allLocalDroppedTables.containsKey(tid);
        /*
         * Removed the check for recreated table. If the table is recreated,
         * the create request should remove the table from the drop list, and
         * then writes from the table will be pushed to the recreated table.
         * This is cleaner than checking the recreated table here.
         */
    }

    /**
     * Reads from a table in local store, retry if table is not found or it is
     * unable to read because of {@link FaultException}
     */
    public Row readRetry(PrimaryKey pkey,
                         ReadOptions options,
                         long maxAttempts) {
        final TableAPI tableAPI = getTableAPI();
        final String table = pkey.getTable().getFullNamespaceName();
        long attempts = 0;
        while (!isClosed() && attempts < maxAttempts) {
            attempts++;
            try {
                return tableAPI.get(pkey, options);
            } catch (FaultException fe) {
                onFaultExp("Retry read table=" + table + ", error=" + fe);
            } catch (MetadataNotFoundException mnfe) {
                onMNFE("Cannot read table=" + table + ", error=" + mnfe);
            }
        }
        logger.info(lm("Cannot read table=" + table +
                       " after #attempts=" + attempts +
                       ", #max=" + maxAttempts +
                       ", closed=" + isClosed()));
        return null;
    }

    /**
     * Writes to a table in local store, retry if table is not found, or it is
     * unable to write because of {@link FaultException}
     */
    public Version putRetry(Row row,
                            WriteOptions options,
                            long maxAttempts) {
        final TableAPI tableAPI = getTableAPI();
        final String table = row.getTable().getFullNamespaceName();
        long attempts = 0;
        while (!isClosed() && attempts < maxAttempts) {
            attempts++;
            try {
                return tableAPI.put(row, null, options);
            } catch (FaultException fe) {
                onFaultExp("Retry put table=" + table + ", error=" + fe);
            } catch (MetadataNotFoundException mnfe) {
                onMNFE("Cannot write table=" + table + ", error=" + mnfe);
            }
        }
        logger.info(lm("Cannot put table=" + table +
                       " after #attempts=" + attempts +
                       ", #max=" + maxAttempts +
                       ", closed=" + isClosed()));
        return null;
    }

    /**
     * Deletes from a table in local store , retry if table is not found or it
     * is unable to delete because of {@link FaultException}
     */
    public boolean deleteRetry(PrimaryKey pkey,
                               WriteOptions options,
                               long maxAttempts) {
        final TableAPI tableAPI = getTableAPI();
        final String table = pkey.getTable().getFullNamespaceName();
        long attempts = 0;
        while (!isClosed() && attempts < maxAttempts) {
            attempts++;
            try {
                /* not return del result, it may not be accurate in retry */
                return tableAPI.delete(pkey, null, options);
            } catch (FaultException fe) {
                onFaultExp("Retry delete table=" + table + ", error=" + fe);
            } catch (MetadataNotFoundException mnfe) {
                onMNFE("Cannot delete table=" + table + ", error=" + mnfe);
            }
        }
        logger.info(lm("Cannot delete from table=" + table +
                       " after #attempts=" + attempts +
                       ", #max=" + maxAttempts +
                       ", closed=" + isClosed()));
        return false;
    }

    /**
     * Wait if table not found. During partial upgrade, the table may not be
     * found when operating on the table, and it will retry until the table
     * is found and operation is successful.
     */
    private void onMNFE(String msg) {
        try {
            logger.info(lm(msg));
            Thread.sleep(MNFE_SLEEP_MS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted in retry on MNFE");
        }
    }

    /**
     * Wait on fault exception
     */
    private void onFaultExp(String msg) {
        try {
            logger.fine(() -> msg);
            Thread.sleep(FE_SLEEP_MS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted in retry on FE");
        }
    }

    public static Set<String> getTrace(Collection<Table> tables) {
        if (tables == null) {
            return null;
        }
        return tables.stream().map(ServiceMDMan::getTrace)
                     .collect(Collectors.toSet());
    }

    public static String getTrace(Table table) {
        if (table == null) {
            return null;
        }
        return table.getFullNamespaceName() +
               "(id=" + ((TableImpl) table).getId() + ", ver=" +
               table.getTableVersion() + ")";
    }

    public static Set<String> getTbNames(Collection<Table> tables) {
        if (tables == null) {
            return null;
        }
        return tables.stream().map(Table::getFullNamespaceName)
                     .collect(Collectors.toSet());
    }

    /* Get all tables in the hierarchy, including the top level table. */
    public void getAllChildTables(Table topLevelTable, Set<Table> tables) {
        PublishingUnit.getAllChildTables(topLevelTable, tables);
    }

    /**
     * Read checkpoints made by any agent
     *
     * @param table table name
     * @param region source region
     * @return table init checkpoint
     */
    public TableInitCheckpoint readCkptAnyAgent(String table, String region) {

        final Set<TableInitCheckpoint> allCkpt = readInitCkptWithRetry(region);
        final Set<TableInitCheckpoint> ckpt =
            allCkpt.stream().filter(t -> t.getTable().equals(table))
                   .collect(Collectors.toSet());
        if (ckpt.isEmpty()) {
            logger.info(lm("Cannot find any table initialization checkpoint " +
                           "for table=" + table + " from region=" + region));
            return null;
        }

        /* pick a ckpt with high timestamp */
        TableInitCheckpoint high = ckpt.stream().findAny().get();
        for (TableInitCheckpoint c : ckpt) {
            if (high.getTimestamp() < c.getTimestamp()) {
                high = c;
            }
        }

        return high;
    }

    public XRegionService getParent() {
        return parent;
    }
}