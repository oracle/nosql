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

import static oracle.kv.KVVersion.CURRENT_VERSION;
import static oracle.kv.impl.streamservice.MRT.Request.V1;
import static oracle.kv.impl.streamservice.MRT.Response.STREAM_TABLES_REQUEST_ID;
import static oracle.kv.impl.xregion.service.JsonConfig.MIN_XREGION_GROUP_STORE_VER;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.MRT_ADD;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.MRT_DROP;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.MRT_REMOVE;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.MRT_UPDATE;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.REGION_ADD;
import static oracle.kv.impl.xregion.service.XRegionRequest.RequestType.REGION_DROP;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.FaultException;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.streamservice.MRT.Manager;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.streamservice.ServiceMessage;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.stat.ReqRespStat;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * Object responsible for interaction with request and response table
 */
public class ReqRespManager extends Manager {

    /** rate limiting logger sampling interval in ms */
    private static final int RL_INTV_MS = 30 * 60 * 1000;

    /** rate limiting logger max number of log objects */
    private static final int RL_MAX_OBJ = 1024;

    /** timeout in ms to put request queue */
    private static final int REQ_QUEUE_PUT_TIMEOUT_MS = 1000;

    /** min back off in ms before retry */
    private static final int MIN_BACKOFF_MS = 100;

    /** max back off in ms before retry */
    private static final long MAX_BACKOFF_MS = 30 * 1000;

    /** table api to deal with request and response table */
    private final TableAPI tableAPI;

    /** subscriber id */
    private final NoSQLSubscriberId sid;

    /** metadata manager */
    private final ServiceMDMan mdMan;

    /** request queue */
    private final BlockingQueue<XRegionRequest> reqQueue;

    /** metrics */
    private final ReqRespStat metrics;

    /** requests that are in process */
    private final ConcurrentMap<Long, XRegionRequest> reqInProcess;

    /** true if requested to shut down */
    private volatile boolean shutDownRequested;

    /**
     * Software version negotiated between the store and the agent. It is
     * the minimum version that the agent must support.
     */
    private volatile KVVersion minAgentVersion = null;

    /**
     * The maximum serial version known to be supported by both the store and
     * the agent. This number only increases.
     */
    private volatile short maxSerialVersion = V1;

    /** rate limiting logger */
    private final RateLimitingLogger<String> rlLogger;

    /** thread to poll the request table periodically */
    private final PollingTask reqPollingThread;

    /** parent agent service */
    private final XRegionService parent;

    /**
     * Table instances in {@link ServiceMDMan} is only updated after the
     * request is complete successfully. When processing back-to-back
     * requests on the same table, the table instance in {@link ServiceMDMan}
     * might be not be complete. For example, request 1 to remove a region
     * and request 2 to add the region back. When processing request 2, the
     * request 1 is not complete and table instance in {@link ServiceMDMan}
     * is not updated. As a result, processing request 2 (which is UPDATE and
     * agent needs to figure out changed regions) may incorrectly using the
     * table instance before request 1, and thus incorrectly decides there is
     * no region change.
     *
     * The table map is initialized when the agent starts up from the
     * multi-region tables fetched from local store.
     *
     * TODO: if the request can tell adding or removing regions, instead of
     * say it is an UPDATE and let agent figure the changed regions, we no
     * longer need this map because we can figure out changed regions solely
     * from the request without looking up the table metadata in
     * {@link ServiceMDMan}.
     */
    private final Map<Long, Table> tableMap;

    public ReqRespManager(XRegionService parent,
                          NoSQLSubscriberId sid,
                          BlockingQueue<XRegionRequest> reqQueue,
                          ServiceMDMan mdMan,
                          Logger logger) {
        super(logger);
        this.parent = parent;
        this.sid = sid;
        this.reqQueue = reqQueue;
        this.mdMan = mdMan;
        metrics = new ReqRespStat();
        shutDownRequested = false;
        tableAPI = mdMan.getServRegionKVS().getTableAPI();
        reqInProcess = new ConcurrentHashMap<>();
        reqPollingThread = new PollingTask("ReqTablePollingThread",
                                           getReqTablePollingIntv());
        rlLogger =
            new RateLimitingLogger<>(RL_INTV_MS, RL_MAX_OBJ, logger);
        tableMap = new ConcurrentHashMap<>();
    }

    /**
     * Checks that components are ready and are at correct software versions.
     */
    @Override
    public void checkForReady() {
        super.checkForReady();

        checkMinVersion();

        /* Check the version of the previous agent, if known */
        final KVVersion oldVersion = getAgentVersion();
        if ((oldVersion != null) &&
            (VersionUtil.compareMinorVersion(oldVersion,
                                             CURRENT_VERSION) > 0)) {
            throw new IllegalStateException(
                "Cannot downgrade agent from " +
                oldVersion.getNumericVersionString() + " to " +
                CURRENT_VERSION.getNumericVersionString());
        }

        /* check if store supports multi-agents */
        final MultiServiceConfigValidator configValidator =
            parent.getConfigValidator();
        if (configValidator.isStoreVersionTooLow()) {
            final String err =
                "Store version not high enough to support XRegion group " +
                "(min version=" +
                MIN_XREGION_GROUP_STORE_VER.getVersionString() + ")" +
                ", agent will shut down";
            throw new ServiceException(sid, err);
        }

        /* checks any misconfiguration inconsistent with lead agent */
        if (!configValidator.startupValidate(sid, CURRENT_VERSION)) {
            final String err = "Misconfiguration detected, the agent will" +
                               " shut down, please check is configuration to " +
                               "ensure it is consistent with the lead agent";
            logger.warning(lm(err));
            throw new ServiceException(sid, err);
        }
    }

    /**
     * Checks the minimum required agent version. If the cached minimum version
     * is lower than this agent's current software version, or there is no
     * cached value, attempt to get it from the request table.
     *
     * In the unlikely cases where either the agent has been downgraded and is
     * starting with an older version than the last time it started, or if this
     * is the first time starting the agent and the agent version is older than
     * the store version, throws an exception for the problem. Otherwise,
     * returns true if the cached minimum agent version was already available
     * or has been updated.
     */
    private boolean checkMinVersion() {
        if ((minAgentVersion != null) &&
            (VersionUtil.compareMinorVersion(minAgentVersion,
                                             CURRENT_VERSION) == 0)) {
            /*
             * Cached agent version is the same as the current software version
             */
            return true;
        }

        /*
         * The cached minimum required version is older than this agent's
         * source code version, or we don't have anything cached. Attempt to
         * get the minimum agent version from the request table. If the
         * required minimum has moved past us (which it should not), the agent
         * must be upgraded to stay compatible.
         */
        minAgentVersion = getMinAgentVersion();
        if (minAgentVersion == null) {
            /*
             * Store is not started, or will need to be upgraded before it can
             * specify the minimum agent version
             */
            return false;
        }

        /*
         * Check whether the admin expects a newer agent version.
         */
        if (VersionUtil.compareMinorVersion(minAgentVersion,
                                            CURRENT_VERSION) > 0) {
            final String minVer = minAgentVersion.getNumericVersionString();
            final String err = "Minimum ver=" + minVer + ", agent ver=" +
                               CURRENT_VERSION.getNumericVersionString() +
                               ", must be upgraded to version " + minVer +
                               " or above";
            logger.warning(lm(err));
            throw new IllegalStateException(err);
        }

        /*
         * Check if the agent is too new: The agent's prereq is newer than the
         * version negotiated with the admin.
         *
         * This check is overly conservative if an old agent was not kept up to
         * date, resulting in the min. version ending up being less than the
         * store version. It would be better to compare the prereq with the
         * store version directly, but we don't actually know the current store
         * version.
         */
        if (VersionUtil.compareMinorVersion(KVVersion.PREREQUISITE_VERSION,
                                            minAgentVersion) > 0) {
            final String minVer = minAgentVersion.getNumericVersionString();
            String err = "Minimum ver=" + minVer + ", agent ver=" +
                         CURRENT_VERSION.getNumericVersionString() +
                         ", agent is newer and does not support" +
                         " the minimum version " + minVer;
            final KVVersion storeVersion = getStoreVersion();
            if (storeVersion != null) {
                err = err + ", store version= " +
                      storeVersion.getNumericVersionString();
            }
            logger.warning(lm(err));
            throw new IllegalStateException(err);
        }
        return true;
    }

    @Override
    protected short getMaxSerialVersion() {
        if ((maxSerialVersion < SerialVersion.CURRENT) && checkMinVersion()) {
            assert minAgentVersion != null;
            maxSerialVersion =
                    SerialVersion.getMaxSerialVersion(minAgentVersion);
        }
        return maxSerialVersion;
    }

    /**
     * Returns true if request and response manager is running
     *
     * @return true if request and response manager is running
     */
    public boolean isRunning() {
        return !shutDownRequested;
    }

    /**
     * Starts periodic polling task
     */
    void startPollingTask() {
        reqPollingThread.start();
        logger.fine(() -> lm("Request polling thread starts up."));
    }

    @Override
    protected TableAPI getTableAPI() {
        return tableAPI;
    }

    @Override
    protected void handleIOE(String error, IOException ioe) {
        /*
         * When agent encounters IOE in posting response (agent does not post
         * request), the IOE should be logged here instead of being thrown
         * to caller. The agent fail to post the response, and will continue.
         */
        logger.warning(lm("Fail to post response message, error=" + error +
                          ", IOE error=" + ioe));
        logger.fine(() -> lm(LoggerUtils.getStackTrace(ioe)));
    }

    public ReqRespStat getMetrics() {
        return metrics;
    }

    /**
     * Shuts down the request and response manager
     */
    public void shutdown() {
        if (shutDownRequested) {
            return;
        }

        shutDownRequested = true;
        synchronized (this) {
            notifyAll();
        }

        reqPollingThread.shutdownThread(logger);
        logger.info(lm("Request polling thread shuts down"));

        /* shut down all ongoing response threads */
        final long shutdownCount =
            reqInProcess.values().stream()
                        .filter(req -> req.getResp() != null)
                        .peek(req -> req.getResp().shutdown())
                        .count();
        logger.fine(() -> lm(shutdownCount + " response thread shuts down"));
        logger.info(lm("Request response manager shuts down"));
    }

    /**
     * Unit test only
     */
    public NoSQLSubscriberId getSid() {
        return sid;
    }

    /**
     * Submits a cross-region service request to service
     *
     * @param req request body
     * @throws InterruptedException if interrupted in enqueue
     */
    public void submit(XRegionRequest req) throws InterruptedException {
        if (req == null) {
            return;
        }
        while (!shutDownRequested) {
            try {
                if (reqQueue.offer(req, REQ_QUEUE_PUT_TIMEOUT_MS,
                                   TimeUnit.MILLISECONDS)) {
                    reqInProcess.put(req.getReqId(), req);
                    logger.finest(() -> lm(logPrefix(req.getReqId()) +
                                           "Request enqueued, type=" +
                                           req.getReqType()));
                    break;
                }

                logger.finest(() -> lm(logPrefix(req.getReqId()) +
                                       "Unable enqueue message=" + req +
                                       " for time(ms)=" +
                                       REQ_QUEUE_PUT_TIMEOUT_MS +
                                       ", keep trying..."));
            } catch (InterruptedException ie) {

                if (shutDownRequested) {
                    /* in shutdown, ignore */
                    logger.warning(lm(logPrefix(req.getReqId()) +
                                      "Interrupted, shut down requested, " +
                                      "current request: " + req));
                    return;
                }

                /* This might have to get smarter. */
                logger.warning(lm(logPrefix(req.getReqId()) +
                                  "Interrupted offering request queue, " +
                                  "req=" + req + ", error=" + ie));
                throw ie;
            }
        }
    }

    /*--------------------*
     * Private functions  *
     *--------------------*/

    /**
     * Adds logger header
     *
     * @param msg logging msg
     * @return logging msg with header
     */
    private String lm(String msg) {
        return "[ReqRespMan-" + sid + "] " + msg;
    }

    private String logPrefix(long requestId) {
        return "[reqId=" + requestId + "] ";
    }

    /**
     * Handles the request from request table
     *
     * @param req request from request table
     * @return true if the request is new request and processed, false
     * otherwise
     * @throws InterruptedException if interrupted
     */
    private boolean handleRequest(Request req) throws InterruptedException {
        /* filter processed request */
        if (isReqProcessed(req)) {
            return false;
        }

        /* filter in-process request */
        if (isReqInProcessing(req)) {
            return false;
        }

        switch (req.getType()) {
            case CREATE_REGION:
                final Request.CreateRegion cr = (Request.CreateRegion) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To create region=" +
                                     cr.getRegion().getName() +
                                     ", request=" + req));
                submitRegionRequest(cr.getRequestId(), REGION_ADD,
                                    cr.getRegion().getId());
                break;

            case DROP_REGION:
                final Request.DropRegion dr = (Request.DropRegion) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To drop region, request=" + req));
                submitRegionRequest(dr.getRequestId(), REGION_DROP,
                                    dr.getRegionId());
                break;

            case CREATE_TABLE:
                final Request.CreateTable ct = (Request.CreateTable) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To create table=" +
                                     ct.getTable().getFullNamespaceName() +
                                     ", request=" + req));
                processTableCreate(ct);
                break;

            case UPDATE_TABLE:
                /* update table, add or remove regions */
                final Request.UpdateTable ut = (Request.UpdateTable) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To update table=" +
                                     ut.getTable().getFullNamespaceName() +
                                     ", request=" + req));
                processTableUpdate(ut);
                break;

            case DROP_TABLE:
                final Request.DropTable dt = (Request.DropTable) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To drop table=" + dt.getTableName() +
                                     ", request=" + req));
                processTableDrop(dt);
                break;

            case CREATE_CHILD:
                final Request.CreateChild cct = (Request.CreateChild) req;
                final Table childTable = getChildTable(cct.getTopTable(),
                                                       cct.getTableId());
                if (childTable == null) {
                    /* child is not found, might be already dropped? */
                    final String err = "Child table id=" + cct.getTableId() +
                                       " not found in top table=" +
                                       cct.getTopTable();
                    logger.warning(lm(logPrefix(req.getRequestId()) + err));
                    final XRegionResp resp =
                        new XRegionResp(cct.getRequestId(), MRT_ADD, null);
                    resp.postFailResp(err);
                    break;
                }
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To create child table=" +
                                     childTable.getFullName() +
                                     ", request=" + req));
                processChildTableCreate(cct, childTable);
                break;

            case DROP_CHILD:
                final Request.DropChild dc = (Request.DropChild) req;
                logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                     "To drop child table=" +
                                     dc.getTableName() + ", request=" + req));
                processChildDrop(dc);
                break;
            default:
                throw new IllegalStateException(logPrefix(req.getRequestId()) +
                                                "Unsupported request type=" +
                                                req.getType());
        }

        return true;
    }

    /*
     * Get the child table with the specified tableId from the top
     * level table.
     */
    private Table getChildTable(Table topLevelTable, long tableId) {
        if (((TableImpl) topLevelTable).getId() == tableId) {
            return topLevelTable;
        }
        for (Table c : topLevelTable.getChildTables().values()) {
            Table t = getChildTable(c, tableId);
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    /**
     * Processes table creation request
     *
     * @param ct table creation request
     * @throws InterruptedException if interrupted during request submission
     */
    private void processTableCreate(Request.CreateTable ct)
        throws InterruptedException {
        final TableImpl t = ct.getTable();
        /* expensive so only in FINE logging level */
        logger.fine(() -> lm(getProcessTableCreateTrace(ct)));

        final long reqId = ct.getRequestId();
        final Set<RegionInfo> regions = getRegions(t.getRemoteRegions());
        final Set<Table> tbs = Collections.singleton(t);
        final XRegionResp resp = new XRegionResp(reqId, MRT_ADD, regions, tbs);
        tableMap.put(t.getId(), t);
        submitReq(resp);
    }

    private void processChildTableCreate(Request.CreateChild cct,
                                         Table childTable)
        throws InterruptedException {
        final TableImpl topTable = cct.getTopTable();

        final int reqId = cct.getRequestId();
        final Set<RegionInfo> regs = getNotEmptyRegion(topTable, reqId);
        final String tableName = childTable.getFullName();
        logger.fine(() -> lm(logPrefix(reqId) +
                             "Child Table=" + tableName + " not empty at " +
                             (regs.isEmpty() ? "all regions." :
                                 ("regions=" + regs))));

        final Set<RegionInfo> regions = getRegions(topTable.getRemoteRegions());
        final Set<Table> tbs = Collections.singleton(childTable);
        final XRegionResp resp = new XRegionResp(reqId, MRT_ADD, regions, tbs);
        tableMap.put(((TableImpl) childTable).getId(), childTable);
        submitReq(resp);
    }

    private String getProcessTableCreateTrace(Request.CreateTable ct) {
        final TableImpl t = ct.getTable();
        final String tbl = ct.getTable().getFullNamespaceName();
        final int reqId = ct.getRequestId();
        final Set<RegionInfo> regs = getNotEmptyRegion(t, reqId);
        if (!regs.isEmpty()) {
            return "Table=" + tbl + " not empty at regions=" + regs;
        }
        return logPrefix(reqId) + "Table=" + tbl + " empty at all regions";
    }

    /**
     * Processes table drop request
     *
     * @param dt table drop request
     * @throws InterruptedException if interrupted during request submission
     */
    private void processTableDrop(Request.DropTable dt)
        throws InterruptedException {

        final TableImpl table = dt.getTable();
        final long reqId = dt.getRequestId();
        final Set<Table> tb = Collections.singleton(table);
        final Set<RegionInfo> reg = getRegions(table.getRemoteRegions());
        final XRegionResp resp = new XRegionResp(reqId, MRT_DROP, reg, tb);
        tableMap.remove(table.getId());
        submitReq(resp);
    }

    private void processChildDrop(Request.DropChild dc)
        throws InterruptedException {
        final long tableId = dc.getTableId();
        final long reqId = dc.getRequestId();
        final Table topTable = dc.getTopLevelTable();

        final TableImpl t = (TableImpl) getChildTable(topTable, tableId);
        /* child table must exist in the top table metadata */
        assert t != null;
        final Set<Table> tb = Collections.singleton(t);
        final Set<RegionInfo> reg = getRegions(t.getRemoteRegions());
        final XRegionResp resp = new XRegionResp(reqId, MRT_DROP, reg, tb);
        tableMap.remove(tableId);
        submitReq(resp);
    }

    /**
     * Processes table update request
     *
     * @param ut table update request
     * @throws InterruptedException if interrupted during request submission
     */
    private void processTableUpdate(Request.UpdateTable ut)
        throws InterruptedException {

        final int requestId = ut.getRequestId();
        final TableImpl updated = ut.getTable();
        final String tName = updated.getFullNamespaceName();
        final long tableId = updated.getId();

        /* remember current instance */
        final TableImpl curr = (TableImpl) tableMap.get(tableId);
        tableMap.put(tableId, updated);

        /* add regions */
        final Set<Integer> addRids = getAddedRegions(updated, curr);
        final Set<Integer> removeRids = getRemovedRegions(updated, curr);
        /* if region not changed, just update the table instance */
        if (addRids.isEmpty() && removeRids.isEmpty()) {
            final Set<RegionInfo> regions =
                getRegionInfos(updated.getRemoteRegions());
            /*
             * table can be evolved with version change, or other changes
             * without version change like setting TTL, in either case,
             * submit the request MRT_EVOLVE to each agent to update its table
             * instance, in order to ensure the update order is same as the
             * order the table is updated at store (request table)
             */
            final Set<String> regs = regions.stream().map(RegionInfo::getName)
                                            .collect(Collectors.toSet());
            final long ver = updated.getTableVersion();
            final long currVer = (curr == null) ? 0 : curr.getTableVersion();
            final boolean evolve = (curr == null) || (ver > currVer);
            logger.info(lm(logPrefix(requestId) +
                           "In request type=" + ut.getType() +
                           ", regions in table=" + tName +
                           " and all child tables not changed" +
                           ", regions=" + regs +
                           (evolve ? ", table evolve from ver=" + currVer +
                                     " to ver=" + ver : " no schema evolve")));

            final Set<Table> tbls = Collections.singleton(updated);
            final XRegionResp resp =
                new XRegionResp(requestId, MRT_UPDATE, regions, tbls);
            submitReq(resp);
            return;
        }

        if (!addRids.isEmpty()) {
            Set<Table> tables = new HashSet<>();
            mdMan.getAllChildTables(updated, tables);
            processAddRemoveRegion(requestId, tables,
                                   MRT_ADD, addRids);
        }

        if (!removeRids.isEmpty()) {
            Set<Table> tables = new HashSet<>();
            mdMan.getAllChildTables(updated, tables);
            processAddRemoveRegion(requestId, tables,
                                   MRT_REMOVE, removeRids);
        }
    }

    /**
     * Initializes table maps
     * @param tables all multi-region tables
     */
    void initTableMap(Collection<Table> tables) {
        tables.forEach(t -> {
            final long tid = ((TableImpl) t).getId();
            tableMap.put(tid, t);
        });

        logger.info(lm("Table map in initialized=" +
                       tableMap.entrySet().stream()
                               .map(entry -> {
                                   final long tid = entry.getKey();
                                   final String tb =
                                       entry.getValue().getFullNamespaceName();
                                   return tb + "(id=" + tid + ")";
                               }).collect(Collectors.toSet())));
    }

    /**
     * Processes add or remove region in table update request
     *
     * @param requestId request id
     * @param tbls      table set
     * @param type      type of operation
     * @param rids      set of region ids
     * @throws InterruptedException if interrupted during request submission
     */
    private void processAddRemoveRegion(long requestId,
                                        Set<Table> tbls,
                                        XRegionRequest.RequestType type,
                                        Set<Integer> rids)
        throws InterruptedException {
        if (rids == null || rids.isEmpty()) {
            throw new IllegalArgumentException("Null region ids");
        }
        final Set<RegionInfo> rinfos = getRegionInfos(rids);
        final Set<String> rnames = rinfos.stream().map(RegionInfo::getName)
                                         .collect(Collectors.toSet());
        final Set<String> tables =
            tbls.stream()
                .map(t -> t.getFullNamespaceName() +
                          "(id=" + ((TableImpl) t).getId() + ")")
                .collect(Collectors.toSet());
        logger.info(lm(logPrefix(requestId) +
                       "To=" + type + " tables=" + tables +
                       ", regions=" + rnames));
        final XRegionResp resp = new XRegionResp(requestId, type, rinfos, tbls);
        submitReq(resp);
    }

    /**
     * Translates region ids to region info
     *
     * @param regionIds set of region ids
     * @return set of region info
     */
    private Set<RegionInfo> getRegionInfos(Set<Integer> regionIds) {
        final Set<RegionInfo> ret = new HashSet<>();
        for (int id : regionIds) {
            final String name = mdMan.getRegionName(id);
            if (name == null) {
                throw new IllegalArgumentException("Cannot translate region " +
                                                   "id=" + id + " to region " +
                                                   "name");
            }
            final RegionInfo regionInfo = mdMan.getRegion(name);
            if (regionInfo == null) {
                throw new IllegalArgumentException(
                    "Region=" + name + " is unknown to the xregion service, " +
                    "please check the json config file");
            }
            ret.add(regionInfo);
        }
        return ret;
    }

    /**
     * Compares the updated and existent table to get a set of added regions,
     * or return empty set if no new region is found
     *
     * @param updated updated table
     * @param curr    existing table
     * @return set of ids of added regions
     */
    private Set<Integer> getAddedRegions(TableImpl updated, TableImpl curr) {
        /*
         * creating a MR table with local region only does not generate a
         * request for agent, thus when the user adds regions, it is like a
         * new table, in this case, just return list of remote regions in the
         * updated table.
         */
        if (curr == null) {
            return updated.getRemoteRegions();
        }

        if (updated.getId() != curr.getId()) {
            throw new IllegalArgumentException(
                "Table id of current table=" + curr.getId() +
                " does not match that in updated table=" + updated.getId());
        }
        /* id in the updated table but not in current table */
        return updated.getRemoteRegions().stream()
                      .filter(id -> !curr.getRemoteRegions().contains(id))
                      .collect(Collectors.toSet());
    }

    /**
     * Compares the updated and existent table to get a set of removed regions,
     * or return empty set if no region is removed
     *
     * @param updated updated table
     * @param curr    existing table
     * @return set of ids of remove regions
     */
    private Set<Integer> getRemovedRegions(TableImpl updated, TableImpl curr) {
        if (curr == null) {
            /* nothing to remove */
            return Collections.emptySet();
        }
        if (updated.getId() != curr.getId()) {
            throw new IllegalArgumentException(
                "Table id of current table=" + curr.getId() +
                " does not match that in updated table=" + updated.getId());
        }

        /* id in the current but not in updated */
        return curr.getRemoteRegions().stream()
                   .filter(id -> !updated.getRemoteRegions().contains(id))
                   .collect(Collectors.toSet());
    }

    /**
     * Submits a region request
     *
     * @param reqId request id
     * @param type  type of request
     * @param rid   region id
     * @throws InterruptedException if interrupted
     */
    private void submitRegionRequest(long reqId,
                                     XRegionRequest.RequestType type, int rid)
        throws InterruptedException {
        final Set<RegionInfo> regions = getRegions(Collections.singleton(rid));
        final XRegionResp resp = new XRegionResp(reqId, type, regions);
        if (rid == Region.LOCAL_REGION_ID) {
            processLocalRegionRequest(reqId, resp, type);
            return;
        }

        if (regions.isEmpty()) {
            /* an unknown remote region */
            final String region = mdMan.getRegionName(rid);
            if (mdMan.getJsonConf().getCascadingRep()) {
                final String msg = "Region=" + region + " in req id=" + reqId +
                                   " is unknown, Ok since cascading " +
                                   "replication is on";
                logger.info(lm(logPrefix(reqId) + msg));
                resp.postSuccResp();
                return;
            }
            final String err = "Region=" + region + " in req id=" + reqId +
                               " is unknown, please add to the config file " +
                               "before creating it";
            logger.warning(lm(logPrefix(reqId) + err));
            /*
             * the region is unknown to the agent, post failure to avoid
             * reprocessing the request
             */
            resp.postFailResp(err);
            return;
        }
        submitReq(resp);
    }

    /**
     * Processes a region request with local region
     *
     * @param reqId request id
     * @param resp  response handler
     * @param type  type of request
     */
    private void processLocalRegionRequest(long reqId,
                                           XRegionResp resp,
                                           XRegionRequest.RequestType type) {
        /* a local region operation */
        if (REGION_ADD.equals(type)) {
            logger.info(lm(logPrefix(reqId) + "Set local region"));
            resp.postSuccResp();
            return;
        }

        if (REGION_DROP.equals(type)) {
            final String err = logPrefix(reqId) +
                               "Trying to drop a local region";
            logger.warning(lm(err));
            resp.postFailResp(err);
            return;
        }

        throw new IllegalArgumentException(logPrefix(reqId) +
                                           "Unsupported request type=" + type);
    }

    /**
     * Submits request
     *
     * @param resp response handler
     * @throws InterruptedException if interrupted
     */
    private void submitReq(XRegionResp resp)
        throws InterruptedException {
        final XRegionRequest.RequestType type = resp.getReqType();
        final XRegionRequest req;
        switch (type) {
            case MRT_ADD:
                req = XRegionRequest.getAddMRTReq(resp);
                break;
            case MRT_REMOVE:
                req = XRegionRequest.getRemoveMRTReq(resp);
                break;
            case MRT_UPDATE:
                req = XRegionRequest.getUpdateMRTReq(resp);
                break;
            case MRT_DROP:
                req = XRegionRequest.getDropMRTReq(resp);
                break;
            case CHANGE_PARAM:
                req = XRegionRequest.getChangeParamReq(resp);
                break;
            case SHUTDOWN:
                req = XRegionRequest.getShutdownReq();
                break;
            case REGION_ADD:
                req = XRegionRequest.getAddRegionReq(resp);
                break;
            case REGION_DROP:
                req = XRegionRequest.getDropRegionReq(resp);
                break;
            default:
                /* all other request will be ignored */
                return;
        }
        submit(req);
        final String regions = (resp.getRegions() == null) ? "n/a" :
            resp.getRegions().stream().map(RegionInfo::getName)
                .collect(Collectors.toSet()).toString();
        final boolean drop = req.getReqType().equals(MRT_DROP);
        logger.info(lm(logPrefix(resp.getReqId()) +
                       "Submitted request, type=" + type +
                       (drop ? "(drop table)" : "") +
                       ", regions=" + regions +
                       ", tables=" + resp.getTables()));

    }

    /**
     * Gets the region info from region ids, only remote region known to the
     * agent will be returned.
     *
     * @param ids set of region ids
     * @return set of remote region info
     */
    private Set<RegionInfo> getRegions(Set<Integer> ids) {
        /* translate region id to name */
        final Set<String> regs = new HashSet<>();
        for (int id : ids) {
            if (id == Region.LOCAL_REGION_ID) {
                continue;
            }

            final String region = mdMan.getRegionName(id);
            if (region == null) {
                final String err = "Unknown region id=" + id;
                logger.fine(() -> lm(err));
                throw new ServiceException(sid, err);
            }
            regs.add(region);
        }

        final Set<RegionInfo> regions = new HashSet<>();
        for (String r : regs) {
            final RegionInfo region = mdMan.getRegion(r);
            if (region == null) {
                /* the region is unknown */
                logger.fine(() -> lm("Unknown region=" + r));
                continue;
            }
            regions.add(region);
        }
        return regions;
    }

    /**
     * Returns true if the request has already been responded
     *
     * @param req request
     * @return true if the req has already been responded, false otherwise
     */
    private boolean isReqProcessed(Request req) {
        final Response sm = getResponse(req.getRequestId());
        if (sm == null) {
            logger.fine(() -> lm("No response for request id=" + req));
            return false;
        }
        if (!sm.hasResponse(sid)) {
            logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                 "Missing my response" +
                                 ", type=" + sm.getType() +
                                 ", #responses=" + sm.getNumResponses() +
                                 ", #total=" + sm.getGroupSize()));
            return false;
        }

        /* my response is posted */
        logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                             "Request already processed, " +
                             "#response=" + sm.getNumResponses() +
                             " all responses=" + sm));
        return true;
    }

    /**
     * Returns true if the request has in being responded
     *
     * @param req request
     * @return true if the req has in being responded, false otherwise
     */
    private boolean isReqInProcessing(Request req) {
        if (!reqInProcess.containsKey((long) req.getRequestId())) {
            return false;
        }
        logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                             "Request in processing"));
        return true;
    }

    /**
     * Returns source region where the table is not empty, or null if the
     * table is empty in all source regions
     *
     * @param table table instance
     * @param reqId request id
     * @return source region that the table is not empty, or null
     */
    private Set<RegionInfo> getNotEmptyRegion(TableImpl table, int reqId) {
        final Set<RegionInfo> ret = new HashSet<>();
        final String tableName = table.getFullNamespaceName();
        final Set<RegionInfo> regions = getRegions(table.getRemoteRegions());
        for (RegionInfo r : regions) {
            final Table tbl = mdMan.getTableFromRegionRetry(r, tableName);
            if (tbl == null) {
                /* ok, table not exist */
                continue;
            }
            final TableAPI remoteAPI = mdMan.getRegionKVS(r).getTableAPI();
            TableIterator<Row> iter = null;
            try {
                iter = remoteAPI.tableIterator(
                    table.createPrimaryKey(), null, null);
                if (iter.hasNext()) {
                    ret.add(r);
                }
            } catch (RuntimeException exp) {
                /* just to create trace, OK to skip the region */
                logger.warning(lm(logPrefix(reqId) +
                                  "Cannot read table=" + tableName + " at " +
                                  "region=" + r.getName() + ", " +
                                  ", error=" + exp));
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
        }
        return ret;
    }

    /**
     * Fetches the next request from request table, retry if needed
     *
     * @param itr request table iterator
     * @return the next request or null if iterating all requests
     * @throws InterruptedException if interrupted
     */
    private Request nextRequest(RequestIterator itr)
        throws InterruptedException {
        long backOffMs = MIN_BACKOFF_MS;
        while (!shutDownRequested) {
            try {
                if (!itr.hasNext()) {
                    return null;
                }
                return itr.next();
            } catch (FaultException fe) {
                /* exponential back-off before retry */
                final Throwable cause = fe.getCause();
                final String err = "Back-off reading request, error=" + fe +
                                   ", cause=" + cause;
                /* message may contain timestamp, use the class name as key */
                final String key = (fe.getClass().getName() +
                                    (cause == null ? "" :
                                        "-" + cause.getClass().getName()));
                rlLogger.log(key, Level.INFO,
                             lm(err + ", backoff(ms)=" + backOffMs));
                synchronized (this) {
                    wait(backOffMs);
                    if (shutDownRequested) {
                        return null;
                    }
                }
                backOffMs = Math.min(backOffMs * 2, MAX_BACKOFF_MS);
            }
        }
        return null;
    }

    /**
     * Polling thread to download requests from the request table in store
     */
    private class PollingTask extends StoppableThread {

        /**
         * request table iterator timeout in seconds
         */
        private static final int REQ_TABLE_ITER_TIMEOUT_SECS = 10;
        /**
         * default request polling initial delay in secs
         */
        private static final int DEFAULT_REQ_POLLING_INIT_DELAY_SECS = 1;
        /**
         * polling interval in ms
         */
        private final long intvMs;
        /**
         * # of requests downloaded from request table in one polling
         */
        private int nDownloaded;
        /**
         * # of submitted requests in one polling
         */
        private int nSubmitted;
        /**
         * start timestamp of one polling
         */
        private long ts;
        /**
         * request table iterator
         */
        private RequestIterator itr;
        /**
         * recorded error
         */
        private Throwable error;

        PollingTask(String threadName, int pollingIntvSecs) {
            super(threadName);
            intvMs = pollingIntvSecs * 1000L;
            error = null;
        }

        @Override
        public void run() {

            logger.info(lm("Requests polling starts up, intv(ms)=" + intvMs));

            /*
             * This will update the minimum version if the store is behind and
             * is being upgraded.
             */
            checkMinVersion();

            try {
                /* init delay */
                synchronized (this) {
                    wait(DEFAULT_REQ_POLLING_INIT_DELAY_SECS * 1000L);

                    /* just exit if shut down requested */
                    if (shutDownRequested) {
                        return;
                    }
                }
                preparePolling();
                while (!shutDownRequested) {

                    /* ensure configuration is correct */
                    if (parent != null &&
                        !parent.getConfigValidator().runtimeValidate()) {
                        logger.severe(lm("Shut down service due to " +
                                         "misconfiguration, please check " +
                                         "config file"));
                        parent.shutdown();
                        break;
                    }

                    final Request req = nextRequest(itr);
                    if (req == null) {
                        /* iterated all requests, done with this polling */
                        if (nSubmitted > 0) {
                            /* summary of this polling if submitted request */
                            logger.info(lm("Polling completed" +
                                           ", #download=" + nDownloaded +
                                           ", #submit=" + nSubmitted +
                                           ", #total=" + metrics.getRequests() +
                                           ", elapsed(ms)=" +
                                           (System.currentTimeMillis() - ts) +
                                           ", next polling in ms=" + intvMs));
                        }
                        /* sleep and wait for the next polling */
                        synchronized (this) {
                            wait(intvMs);
                        }
                        /* wake up */
                        if (shutDownRequested) {
                            break;
                        }
                        preparePolling();
                        continue;
                    }

                    logger.fine(() -> lm(logPrefix(req.getRequestId()) +
                                         "Downloaded request" +
                                         ", type=" + req.getType() +
                                         ", service=" + req.getServiceType() +
                                         ", serial ver=" +
                                         req.getSerialVersion()));
                    nDownloaded++;
                    /* ignore unsupported request */
                    final ServiceMessage.ServiceType st = req.getServiceType();
                    if (!st.equals(ServiceMessage.ServiceType.MRT) &&
                        !st.equals(ServiceMessage.ServiceType.PITR)) {
                        logger.warning(lm(logPrefix(req.getRequestId()) +
                                          "Ignore unsupported service " +
                                          "type=" + st));
                        continue;
                    }

                    if (handleRequest(req)) {
                        /* increment request counter */
                        metrics.incrRequest();
                        nSubmitted++;
                    }
                }
            } catch (InterruptedException ie) {
                logger.fine(() -> lm("Interrupted, request polling exits"));
            } catch (RuntimeException exp) {
                /* ignore error if in shutdown */
                if (!shutDownRequested) {
                    error = exp;
                }
            } finally {
                logger.info(lm("Request polling thread exits" +
                               ", shutdown=" + shutDownRequested +
                               ", #download=" + nDownloaded +
                               ", #submit=" + nSubmitted +
                               ", #requests=" + metrics.getRequests()));
                if (error != null) {
                    logger.warning(lm("Error=" + error + "\n" +
                                      LoggerUtils.getStackTrace(error)));
                }
            }
        }

        @Override
        protected int initiateSoftShutdown() {
            /* wake up the thread if in waiting */
            synchronized (this) {
                notifyAll();
            }
            /*
             * wait for iterator timeout plus polling interval to give
             * the thread enough time to exit by itself
             */
            return REQ_TABLE_ITER_TIMEOUT_SECS * 1000 +
                   getReqTablePollingIntv() * 1000;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        /**
         * Initializes before each polling
         */
        private void preparePolling() {
            nDownloaded = 0;
            nSubmitted = 0;
            ts = System.currentTimeMillis();
            itr = getRequestIterator(REQ_TABLE_ITER_TIMEOUT_SECS,
                                     TimeUnit.SECONDS);
            final String err = "Ready to poll the request table";
            rlLogger.log(err, Level.INFO, lm(err));
        }

        private String lm(String msg) {
            return "[ReqRespMan-ReqPolling-" + sid + "] " + msg;
        }
    }

    private int getReqTablePollingIntv() {
        return (mdMan == null ?
            JsonConfig.DEFAULT_REQ_POLLING_INTV_SECS /* unit test */ :
            mdMan.getJsonConf().getRequestTablePollIntvSecs());
    }

    /**
     * Response handler
     */
    private class XRegionResp extends XRegionRespHandlerThread {

        XRegionResp(long reqId,
                    XRegionRequest.RequestType reqType,
                    Set<RegionInfo> regions,
                    Set<Table> tables) {
            super(reqId, reqType, regions, tables, mdMan, logger);
        }

        XRegionResp(long reqId,
                    XRegionRequest.RequestType reqType,
                    Set<RegionInfo> regions) {
            super(reqId, reqType, regions, null, mdMan, logger);
        }

        /**
         * Posts failure response
         *
         * @param msg summary of error
         */
        @Override
        public void postFailResp(String msg) {
            final Response sm = Response.createReqResp((int) reqId,
                                                       sid.getTotal());
            sm.addFailResponse(sid, msg);
            postResponse(sm, false /*overwrite*/);
            logger.info(lm(logPrefix(sm.getRequestId()) +
                           "Post failure response for sid=" + sid +
                           ", error=" + msg));
            reqInProcess.remove(reqId);
            metrics.incrResponse();
        }

        /**
         * Posts success response
         */
        @Override
        public void postSuccResp() {
            final Response sm = Response.createReqResp((int) reqId,
                                                       sid.getTotal());
            sm.addSuccResponse(sid);
            postResponse(sm, false /*overwrite*/);
            logger.info(lm(logPrefix(sm.getRequestId()) +
                           "Post success response for sid=" + sid));
            reqInProcess.remove(reqId);
            metrics.incrResponse();
        }
    }

    /**
     * Updates the response table with the set of tables in streaming
     *
     * @param region source region name
     * @param tableIds ids of tables in streaming
     */
    public void updateStreamTables(String region, Set<Long> tableIds) {
        updateStreamTables(sid, region, tableIds);
    }

    /**
     * Updates the response table with the set of tables in streaming with
     * given agent id
     *
     * @param agentId agent id
     * @param sourceRegion source region name
     * @param tableIds ids of tables in streaming
     */
     public void updateStreamTables(NoSQLSubscriberId agentId,
                                    String sourceRegion,
                                    Set<Long> tableIds) {
        if (tableIds == null) {
            throw new IllegalArgumentException(
                "Set of table ids cannot be null");
        }
        final Response resp = Response.createStreamTbResp(agentId.getTotal());
        resp.setStreamingTables(agentId, sourceRegion, tableIds);
        postResponse(resp, true);
        logger.fine(() -> lm(logPrefix(resp.getRequestId()) +
                             "Update stream tables in response table" +
                             ", agent id=" + agentId +
                             ", table ids=" + tableIds));
    }

    /**
     * Returns the set of agent index that is streaming the given remote
     * table id from given source region, or empty set if no agent is streaming
     * the table
     *
     * @param tableId table id
     * @param region source region
     * @return the set of agent index that is streaming the table, or null
     * @throws IOException if unable to read the response table
     */
    public Set<Integer> getStreamAgentIdx(long tableId, String region)
        throws IOException {
        final Map<Integer, Set<Long>> tbls = getStreamTables(region);
        final Set<Integer> ret = new HashSet<>();
        tbls.keySet().forEach(index -> {
            if (tbls.get(index).contains(tableId)) {
                ret.add(index);
            }
        });
        return ret;
    }

    /**
     * Returns the map of per-agent stream tables from response table
     *
     * @param region source region
     * @return the map of stream tables from response table, or empty map if
     * row does not exist
     * @throws IOException if unable to read the response table
     */
    private Map<Integer, Set<Long>> getStreamTables(String region)
        throws IOException {
        final int reqId = STREAM_TABLES_REQUEST_ID;
        final Row row = readReqRespWithRetry(reqId, false);
        if (row == null) {
            /* not found */
            return Collections.emptyMap() ;
        }

        final Response res = Response.getFromRow(row);
        if (!res.getType().equals(Response.Type.STREAM_TABLE)) {
            throw new IllegalArgumentException(
                "Row with request id=" + reqId +
                " is not a stream table response");
        }
        return res.getStreamingTables(region);
    }
}
