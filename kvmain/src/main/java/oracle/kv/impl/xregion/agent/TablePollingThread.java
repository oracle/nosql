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

package oracle.kv.impl.xregion.agent;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.RegionAgentThread.RegionAgentReq;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.table.Table;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * A thread that lives in {@link RegionAgentThread} and periodically polling
 * a remote region for given tables, and start initialization for any found
 * table.
 */
public class TablePollingThread extends StoppableThread {

    //TODO: make them configurable in JSON config
    /**
     * default polling interval in ms
     */
    static final int DEFAULT_POLL_INTV_MS = 30 * 1000;
    /**
     * wait time in ms during soft shutdown
     */
    static final int SOFT_SHUTDOWN_WAIT_MS = 5 * 1000;
    /**
     * Rate limiting logger sampling interval in ms
     */
    private static final int RL_LOG_PERIOD_MS = 30 * 60 * 1000;
    /**
     * Rate limiting logger max objects
     */
    private static final int RL_LOG_MAX_OBJ = 1024;
    /**
     * parent region agent thread
     */
    private final RegionAgentThread parent;
    /**
     * private logger
     */
    private final Logger logger;
    /**
     * tables to check, indexed by local table id
     */
    private final Map<Long, Table> tablesToCheck;
    /**
     * requests that need response after table initialized
     */
    private final Set<RegionAgentReq> requests;
    /**
     * handle to metadata manager
     */
    private final ServiceMDMan mdMan;
    /**
     * Source region
     */
    private final RegionInfo source;
    /**
     * Target region
     */
    private final RegionInfo target;
    /**
     * true if shut down is requested
     */
    private final AtomicBoolean shutdownRequested;
    /**
     * Polling interval in ms
     */
    private volatile int pollIntvMs;
    /**
     * Rate limiting logger
     */
    private final RateLimitingLogger<String> rlLogger;
    /**
     * A child thread to manage table initialization
     */
    private final TablePollingTableInitThread tableInitThread;

    TablePollingThread(RegionAgentThread parent, Logger logger) {
        super("TablePollingThread-" + parent.getAgentId());
        this.parent = parent;
        this.logger = logger;
        rlLogger =
            new RateLimitingLogger<>(RL_LOG_PERIOD_MS, RL_LOG_MAX_OBJ, logger);
        pollIntvMs = DEFAULT_POLL_INTV_MS;
        mdMan = parent.getMdMan();
        source = parent.getSourceRegion();
        target = parent.getTargetRegion();
        shutdownRequested = new AtomicBoolean(false);
        tablesToCheck = new ConcurrentHashMap<>();
        requests = new HashSet<>();
        tableInitThread = new TablePollingTableInitThread(this, logger);
    }

    /**
     * Returns region agent id
     *
     * @return region agent id
     */
    String getAgentId() {
        return parent.getAgentId();
    }

    /**
     * Returns the source region from which to stream
     *
     * @return source region
     */
    RegionInfo getSourceRegion() {
        return parent.getSourceRegion();
    }

    /**
     * Returns the parent region agent
     */
    RegionAgentThread getParent() {
        return parent;
    }

    /**
     * Returns the target region
     */
    RegionInfo getTarget() {
        return target;
    }

    /**
     * Returns true if the table is in checklist
     *
     * @param tid id of the table to check
     * @return true if the table is in checklist
     */
    public boolean inPolling(long tid) {
        synchronized (tablesToCheck) {
            return tablesToCheck.containsKey(tid);
        }
    }

    /**
     * Returns true if the table is found and queued to transfer or in
     * transfer
     *
     * @param tid id of table
     * @return true if the table is found and queued to transfer or in
     * transfer
     */
    public boolean inTrans(long tid) {
        final TableImpl tbInTrans =
            (TableImpl) tableInitThread.getTablesInTrans();
        if (tbInTrans != null && tid == tbInTrans.getId()) {
            return true;
        }
        return tableInitThread.tableInQueue(tid);
    }

    /**
     * Adds tables to check list
     *
     * @param req region agent request or null if not needed
     * @param tbls tables
     */
    void addTables(RegionAgentReq req, Set<Table> tbls) {
        if (tbls == null || tbls.isEmpty()) {
            return;
        }
        synchronized (tablesToCheck) {
            /* do not add if already in the list */
            final Set<String> added = new HashSet<>();
            for (Table tb : tbls) {
                final long tid = ((TableImpl) tb).getId();
                if (!tablesToCheck.containsKey(tid)) {
                    tablesToCheck.put(tid, tb);
                    added.add(ServiceMDMan.getTrace(tb));
                }
            }
            if (req != null) {
                requests.add(req);
            }
            logger.info(lm("Tables=" + added +
                           " added to check list=" +
                           ServiceMDMan.getTrace(tablesToCheck.values()) +
                           (req != null ?
                               " from req id=" + req.getReqId() : "")));
            tablesToCheck.notifyAll();
        }
    }

    /**
     * Removes tables from check list
     *
     * @param tbls tables to remove
     */
    void removeTableFromCheckList(Set<Table> tbls) {
        if (tbls == null || tbls.isEmpty()) {
            return;
        }
        synchronized (tablesToCheck) {
            final Set<Table> tbs = new HashSet<>();
            for (Table tb : tbls) {
                final long tid = ((TableImpl) tb).getId();
                if (tablesToCheck.remove(tid) != null) {
                    tbs.add(tb);
                }
            }
            logger.fine(() -> lm("Tables=" + ServiceMDMan.getTrace(tbs) +
                                 " removed from check list"));
        }
    }

    /**
     * Stops the thread from another thread without error
     */
    void shutdown() {
        if (!shutdownRequested.compareAndSet(false, true)) {
            logger.fine(() -> lm("Thread already shut down"));
            return;
        }
        final long ts = System.currentTimeMillis();
        synchronized (tablesToCheck) {
            tablesToCheck.notifyAll();
        }
        tableInitThread.shutdown();
        logger.fine(() -> lm("Table initialization thread shutdown"));

        shutdownThread(logger);
        logger.info(lm("Polling thread shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected int initiateSoftShutdown() {
        logger.fine(() -> lm("Wait for thread " + getName() + " to shutdown" +
                             " in time(ms)=" + SOFT_SHUTDOWN_WAIT_MS));
        return SOFT_SHUTDOWN_WAIT_MS;
    }

    @Override
    public void run() {

        logger.info(lm("Table polling thread starts, source region=" +
                       source.getName() + ", polling interval(ms)=" +
                       pollIntvMs));

        /* start table initialization thread */
        tableInitThread.start();

        try {
            while (!shutdownRequested.get()) {

                /* sleep at beginning of each loop */
                if (pollIntvMs > 0) {
                    synchronized (tablesToCheck) {
                        tablesToCheck.wait(pollIntvMs);
                    }
                    if (shutdownRequested.get()) {
                        break;
                    }
                }

                /* first check all streaming tables are valid at remote */
                verifyStreamingRemoteTables();

                /* nothing to check, go back to sleep  */
                if (tablesToCheck.isEmpty()) {
                    continue;
                }

                /* check table at source region */
                final Set<Table> tablesToCheckCopy;
                synchronized (tablesToCheck) {
                    /* checking table may take a while, make a copy */
                    tablesToCheckCopy = new HashSet<>(tablesToCheck.values());
                }
                final Set<Table> found = checkTable(tablesToCheckCopy);
                if (found.isEmpty()) {
                    logger.fine(() -> lm(
                        "Cannot find any of tables=" +
                        ServiceMDMan.getTrace(tablesToCheckCopy)));
                    continue;
                }
                /* filter out all tables not ready for snapshotting */
                final Set<Table> ready = found.stream()
                                              .filter(this::readyToSnapshot)
                                              .collect(Collectors.toSet());
                /* start initialization */
                final Set<Table> initialized = initializeTables(ready);
                logger.info(lm("In source region=" + source.getName() +
                               ", tables found=" +
                               ServiceMDMan.getTrace(found) +
                               ", tables ready for snapshotting=" +
                               ServiceMDMan.getTrace(ready) +
                               ", tables submitted to initialize=" +
                               ServiceMDMan.getTrace(initialized) +
                               ", tables in check list=" +
                               ServiceMDMan.getTrace(tablesToCheckCopy)));

                if (!initialized.isEmpty()) {
                    postSucc();
                }
            }
        } catch (InterruptedException ie) {
            logger.fine(() -> "Interrupted in waiting and exit, " +
                              ", table to check=" +
                              ServiceMDMan.getTrace(tablesToCheck.values()));
        } catch (Exception exp) {
            logger.warning(lm("Polling thread shuts down on error=" + exp +
                              ", stack:\n" + LoggerUtils.getStackTrace(exp)));
        } finally {
            logger.info(lm("Polling thread exits" +
                           ", shutdown request=" + shutdownRequested +
                           (tablesToCheck.isEmpty() ? "" :
                               ", tables=" +
                               ServiceMDMan.getTrace(tablesToCheck.values()))));
        }
    }

    private boolean readyToSnapshot(Table table) {
        if (parent.getAgentSubscriberId().isSingleAgentGroup()) {
            /* single agent group, no need to check */
            return true;
        }
        final Set<Integer> agents = parent.streamingAgents(table);
        final Set<Integer> remain = parent.getComplementAgents(agents);
        if (!remain.isEmpty()) {
            logger.info(lm("Table=" + ServiceMDMan.getTrace(table) +
                           " not ready for snapshotting" +
                           ", will wait for the next turn" +
                           ", streaming not started on agents=" + remain));
            return false;
        }
        logger.info(lm("Table=" + ServiceMDMan.getTrace(table) +
                       " ready for snapshotting, streaming started on " +
                       "all agents=" + agents));
        return true;
    }

    /**
     * Unit test only
     * Returns tables in check list
     */
    public Set<String> getTablesToCheck() {
        return tablesToCheck.values().stream()
                            .map(Table::getFullNamespaceName)
                            .collect(Collectors.toSet());
    }

    /**
     * Unit test only
     */
    public Set<RegionAgentReq> getRequests() {
        return requests;
    }

    /**
     * Unit test only
     */
    void setPollIntvMs(int val) {
        pollIntvMs = val;
    }

    /**
     * Unit test only
     */
    int getPollIntvMs() {
        return pollIntvMs;
    }

    String getLogHeader() {
        return "TablePollingThread-" +
                parent.getSourceRegion().getName() + "-" +
                parent.getSid();
    }

    /*--------------------*
     * Private functions  *
     *--------------------*/
    private String lm(String msg) {
        return "[" + getLogHeader() + "] " + msg;
    }

    /**
     * Initializes the given tables
     *
     * @param tbls tables to initialize
     * @return set of tables that are initialized successfully
     */
    private Set<Table> initializeTables(Set<Table> tbls) {

        /* adding found tables to running stream */
        final Map<Table, StreamPosition> inStream = new HashMap<>();
        for (Table t : tbls) {
            final StreamPosition sp = addTableHelper(t);
            if (sp != null) {
                inStream.put(t, sp);
                continue;
            }
            logger.info(lm("Fail to add table=" + t + " to stream, will " +
                           "retry in next polling"));
        }
        if (inStream.keySet().isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Table> transfer = new HashSet<>();
        final String region = source.getName();
        logger.info(lm("Added tables(id, position) to stream=" +
                       inStream.entrySet().stream()
                               .map(e -> {
                                   final Table tb = e.getKey();
                                   final String tbName =
                                       tb.getFullNamespaceName();
                                   final StreamPosition sp = e.getValue();
                                   final long remoteTid =
                                       mdMan.getRemoteTableId(region, tbName);
                                   return ServiceMDMan.getTrace(tb) +
                                          ", remote id=" + remoteTid +
                                          ", pos=" + sp + ")";
                               })
                               .collect(Collectors.toSet())));
        for (Table table : inStream.keySet()) {
            /* create table initialization thread */
            final long timeoutMs = parent.getTimeoutMs(null);
            logger.info(lm("Start initializing table=" +
                           table.getFullNamespaceName() +
                           " from region=" + source.getName() +
                           ", timeoutMs=" + timeoutMs));
            /* put the table in the queue */
            if (!tableInitThread.enqueue(table)) {
                final String msg = "Queue is full, keep table=" +
                                   ServiceMDMan.getTrace(table) +
                                   " in the checklist for the next turn";
                logger.info(lm(msg));
                continue;
            }
            /* table pending transfer, no longer in check list */
            removeTableFromCheckList(Collections.singleton(table));
            transfer.add(table);
        }

        return transfer;
    }

    /**
     * Checks if the tables in the list are existent at source region
     *
     * @return tables that are existent and have compatible schema
     */
    private Set<Table> checkTable(Set<Table> tables) {

        /* check table at source region */
        final Set<String> found = mdMan.checkTable(
            source, ServiceMDMan.getTbNames(tables));
        if (!found.isEmpty()) {
            logger.info(lm("Found tables=" + found +
                           " at region=" + source.getName()));
        }
        final Set<Table> foundTbs =
            found.stream().map(this::getTableMd).collect(Collectors.toSet());
        /* check schema for each found table, dump warning if incompatible */
        foundTbs.forEach(this::checkSchema);

        /* dump warnings for not-found tables, hope user will catch */
        final Set<String> notFound =
            tablesToCheck.values().stream()
                         .filter(t -> !foundTbs.contains(t))
                         .map(Table::getFullNamespaceName)
                         .collect(Collectors.toSet());
        if (!notFound.isEmpty()) {
            logger.info(lm("Table either missing or incompatible schema, " +
                           "tables=" + notFound +
                           ", source region=" + source.getName()));
        }
        return foundTbs;
    }

    /**
     * Checks the schema of the given table and log if incompatible.
     * Note that replication with incompatible schema is allowed. In that
     * case, the subscriber will make a best-effort approach to convert
     * rows from source to target table. If successful, the persistence
     * would be made; if unsuccessful, the row will be rejected.
     */
    private void checkSchema(Table table) {
        final Set<String> result =
            parent.getMdMan().matchingSchema(parent.getSourceRegion(),
                                             Collections.singleton(table));
        if (!result.isEmpty()) {
            final String sb = "Table=" + result + " are either missing or " +
                              "have mismatched schema at remote region=" +
                              parent.getSourceRegion().getName();
            logger.info(lm(sb));
        }
    }

    /**
     * Adds the table to the stream
     *
     * @param table table to add
     * @return stream position where the table is added, or null if fail to
     * add the table to stream
     */
    private StreamPosition addTableHelper(Table table) {
        /* need to sync with regent agent thread */
        return parent.addTableHelper(table);
    }

    private synchronized boolean tableInitDone(RegionAgentReq req) {
        final Set<String> tbs = ServiceMDMan.getTbNames(tablesToCheck.values());
        /* no table in request is in check list */
        return req.getTables().stream().noneMatch(tbs::contains);
    }

    /**
     * Checks each request to see if all tables are found, if yes, post
     * success for the request
     */
    private void postSucc() {
        final Iterator<RegionAgentReq> iter = requests.iterator();
        while (iter.hasNext()) {
            final RegionAgentReq req = iter.next();
            if (tableInitDone(req)) {
                parent.postSucc(req.getReqId(), req.getResp());
                iter.remove();
            }
        }
    }

    /**
     * Verifies all streaming tables by matching their table ids with the
     * recorded in local system table. If a table is missing or has
     * mismatched table id, that means the table has been dropped at the
     * remote region. Such table would be put to the list of tables to poll in
     * the polling thread like other missing tables, and will be
     * re-initialized when it is recreated at the remote region.
     */
    private void verifyStreamingRemoteTables() throws InterruptedException {
        if (parent == null) {
            /* unit test only, skip check remote tables without full agent */
            return;
        }
        /* all MR tables in this region */
        final BaseRegionAgentSubscriber subscriber = parent.getSubscriber();
        if (subscriber == null) {
            logger.fine(() -> lm("Stream not started yet"));
            return;
        }
        final NoSQLSubscription sub = subscriber.getSubscription();
        if (sub == null || sub.isCanceled()) {
            logger.fine(() -> lm("Subscription unavailable or has been " +
                                 "canceled"));
            return;
        }

        final Set<String> tables = sub.getSubscribedTables();
        if (tables.isEmpty()) {
            logger.fine(() -> lm("No table in streaming"));
            return;
        }
        final int count = tables.size();
        rlLogger.log(tables.toString(), Level.INFO,
                     lm("Verify #=" + count + " streaming tables=" + tables +
                        " existence at region=" + source.getName()));

        /* check each streaming table */
        final String region = source.getName();
        final Set<String> dropped = new HashSet<>();
        final Set<String> recreated = new HashSet<>();
        final String from = "table polling thread";
        for (String t : tables) {
            final Table tbl =
                parent.getMdMan().getTableFromRegionRetry(source, t);
            if (tbl == null) {
                /* table dropped */
                dropped.add(t);
                logger.info(lm("Table=" + t + " dropped at region=" +
                               source.getName() + ", remove and add it back"));
                parent.recreateRemoteTable(t, from);
                continue;
            }

            /* verify table id */
            final long exp = parent.getMdMan().getRecordedTableId(t, region);
            if (exp <= 0) {
                logger.fine(() -> lm("Id of table=" + t + " not recorded"));
                continue;
            }

            final long act = ((TableImpl) tbl).getId();
            if (exp != act) {
                logger.info(lm("Table=" + t + " recreated at region=" +
                               region + ", id=" + exp + " -> " + act));
                recreated.add(t);
                parent.recreateRemoteTable(t, from);
            }
        }

        if (!dropped.isEmpty()) {
            logger.warning(lm("Found tables dropped in region=" + region +
                              ", dropped=" + dropped));
        }

        if (!recreated.isEmpty()) {
            logger.warning(lm("Found tables recreated in region=" + region +
                              ", recreated=" + recreated));
        }
    }

    private Table getTableMd(String name) {
        for (Table t : tablesToCheck.values()) {
            if (t.getFullNamespaceName().equals(name)) {
                return t;
            }
        }
        return null;
    }
}
