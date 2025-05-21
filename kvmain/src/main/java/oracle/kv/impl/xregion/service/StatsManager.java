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

import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_AGENT_ID;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_STATISTICS;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_TABLE_ID;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_TIMESTAMP;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.TABLE_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.BaseRegionAgentMetrics;
import oracle.kv.impl.xregion.agent.BaseRegionAgentSubscriber;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.impl.xregion.stat.JsonRegionStat;
import oracle.kv.impl.xregion.stat.ReqRespStat;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.json.JsonUtils;

/**
 * Object that is responsible for stat collection and reporting
 */
public class StatsManager {

    /** table id used for agent statistics */
    public static final long TABLE_ID_AGENT_STAT = 0L;

    /** monitor and stats collection internal in ms */
    private static final int STATCOLL_INTERVAL_MS = 1000;

    /** statistics table write timeout in ms */
    private static final int STAT_TABLE_TIMEOUT_MS =
        KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;

    /** retry interval in ms */
    private static final int RETRY_INTERVAL_MS = 1000;

    /** maximum number of attempts */
    private static final int MAX_NUM_ATTEMPTS = 20;

    /**
     * write option to put stat, stat table has default per-table TTL
     * {@link oracle.kv.impl.systables.MRTableAgentStatDesc#DEFAULT_TTL}
     */
    private static final WriteOptions STAT_TABLE_WRITE_OPT =
        new WriteOptions(Durability.COMMIT_SYNC,
                         STAT_TABLE_TIMEOUT_MS,
                         TimeUnit.MILLISECONDS);

    /** Whether the stat manager has been shutdown. */
    private volatile boolean shutDownRequested;

    /** parent xregion service */
    private final XRegionService parent;

    /** table api for the kvstore */
    private final TableAPI tableAPI;

    /** XRegion Service id */
    private final String agentId;

    /** TTL of each stat row */
    private final TimeToLive statRowTTL;

    /** private logger */
    private final Logger logger;

    /** service agent statistics */
    private volatile XRegionStatistics stats;

    /** stats report interval in seconds */
    private volatile DurationParameter statIntv;

    /**
     * A scheduled executor service for retrying operations after fault
     * exceptions
     */
    private final ScheduledExecutorService executor;

    /** Timestamp of last reporting */
    private volatile long lastReportTimeMs;

    /** Stat reporter future */
    private Future<?> statReportFuture;

    /**
     * True if report the interval stats, that is, only report the stats
     * since last report; false if report the accumulated stats
     */
    private volatile boolean intvStats = true;

    /** the system table handle */
    private Table sysTable;

    /**
     * Dependency: stats manager can only be initialized after metadata
     * manager is ready in parent xregion service
     */
    StatsManager(XRegionService parent, Logger logger) {
        this.parent = parent;
        this.logger = logger;
        tableAPI = parent.getMdMan().getServRegionKVS().getTableAPI();
        agentId = getStatAgentId(parent.getSid());
        final String localRegion = parent.getMdMan().getServRegion().getName();
        stats = new XRegionStatistics(agentId, localRegion);
        shutDownRequested = false;
        statReportFuture = null;
        lastReportTimeMs = System.currentTimeMillis();
        statIntv = new DurationParameter("STAT_REPORT_INTERVAL",
                                         TimeUnit.SECONDS,
                                         parent.getMdMan().getJsonConf()
                                               .getStatIntervalSecs());
        statRowTTL = TimeToLive.ofDays(parent.getMdMan().getJsonConf()
                                             .getStatTTLDays());
        executor =
            Executors.newScheduledThreadPool(1, new StatReportThreadFactory());
    }

    /**
     * Unit test only
     */
    public StatsManager(KVStore kvstore,
                        String agentId,
                        Logger logger) {
        this.tableAPI = kvstore == null ? null : kvstore.getTableAPI();
        this.agentId = agentId;
        this.logger = logger;
        parent = null;
        final String storeName = (kvstore == null) ? "NA" :
            ((KVStoreImpl) kvstore).getTopology().getKVStoreName();
        stats = new XRegionStatistics(agentId, storeName);
        shutDownRequested = false;
        statReportFuture = null;
        statIntv = new DurationParameter(
            "STAT_REPORT_INTERVAL", TimeUnit.SECONDS,
            JsonConfig.DEFAULT_STAT_REPORT_INTERVAL_SECS);
        statRowTTL =
            TimeToLive.ofDays(JsonConfig.DEFAULT_STAT_REPORT_TTL_DAYS);

        executor =
            Executors.newScheduledThreadPool(1, new StatReportThreadFactory());
    }

    private static String getStatAgentId(NoSQLSubscriberId sid) {
        return "XRegionService-" + sid;
    }

    /**
     * Returns the complete service statistics
     *
     * @return the complete service statistics
     */
    public XRegionStatistics getStats() {
        return stats;
    }

    /**
     * Returns the timestamp of last reported statistics
     *
     * @return the timestamp of last reported statistics
     */
    public long getLastReportTimestamp() {
        return lastReportTimeMs;
    }

    /**
     * Shuts down the stat report manager, cancel ongoing stat reporting and
     * close the scheduler.
     */
    public void shutdown() {
        if (shutDownRequested) {
            return;
        }

        shutDownRequested = true;
        if (statReportFuture != null) {
            statReportFuture.cancel(false);
        }

        /* wake up waiting */
        synchronized (executor) {
            executor.notifyAll();
        }
        try {
            final boolean succ = executor.awaitTermination(1, TimeUnit.SECONDS);
            if (succ) {
                logger.fine(() -> lm("Executor terminated successfully"));
            } else {
                logger.fine(() -> lm("Timeout elapsed before termination"));
            }
        } catch (InterruptedException ie) {
            logger.fine(() -> lm("Waiting for stat report to complete " +
                                 "failed, error=" + ie));
        }
        executor.shutdown();

        logger.fine(() -> lm("Stats report manager shuts down"));
    }

    /**
     * Schedules a periodical stat reporting.
     * This method schedules a task to be executed periodically. If
     * a task takes longer to execute than the period between its scheduled
     * executions, the next execution will start after the current execution
     * finishes. The scheduled task will not be executed by more than one
     * thread at a time.
     */
    public void scheduleStatReport() {
        statReportFuture = ScheduleStart.scheduleAtFixedRate(
            executor, statIntv, this::collectReportStat, logger);
        logger.info(lm("Schedule stat report task" +
                       ", stats type=" +
                       (intvStats ? "interval" : "cumulative") +
                       ", collection interval=" + statIntv));
    }

    /**
     * Collects the statistics. In order to get interval stat, the region agent
     * stat should be cleared after collection. To get accumulate stats, the
     * region agent stat shall not be cleared.
     *
     * @return stat collection time
     */
    public long collect() {
        final long now = System.currentTimeMillis();
        if (parent == null) {
            /* unit test */
            return now;
        }

        /*
         * Collect all agent metrics if non-interval stat. For interval stat,
         * each region returns the current stat, and refresh with new stat
         * object.
         */
        final Set<BaseRegionAgentMetrics> ams =
            parent.getAllAgents().stream()
                  .map(r -> intvStats ? r.getMetricsRefresh() : r.getMetrics())
                  .collect(Collectors.toSet());

        /* collects stat for each MR table */
        parent.getMdMan().getMRTables()
              .forEach(t -> collectTableMetrics(ams, t.getFullNamespaceName(),
                                                now));

        /* build service aggregate stat */
        collectServiceAgentStat(ams, now);
        return now;
    }

    /**
     * Enables or disables interval stats -- for testing only.
     */
    public void setUseIntervalStats(boolean val) {
        intvStats = val;
    }

    /**
     * Unit test only
     */
    public void setStats(final XRegionStatistics expected) {
        stats = expected;
    }

    /**
     * Unit test only
     */
    public void setReportIntv(final DurationParameter durationParameter) {
        statIntv = durationParameter;
    }

    /**
     * Unit test only
     */
    public long getStatCollectIntervalMs() {
        return STATCOLL_INTERVAL_MS;
    }

    /**
     * Initializes the table stats, must be called after the MR tables are
     * available in {@link ServiceMDMan#initialize()}
     */
    public void initialize() {
        /* pass stats handle to md manager */
        parent.getMdMan().setStats(stats);
        logger.info(lm("Stat manager initialized"));
    }

    /* private functions */
    private String lm(String msg) {
        return "[StatMan-" + agentId + "] " + msg;
    }

    /**
     * Collects the service agent stat from table metrics
     *
     * @param ams all region agent stat
     */
    private void collectServiceAgentStat(Set<BaseRegionAgentMetrics> ams,
                                         long now) {
        final XRegionServiceMetrics sm = stats.getServiceMetrics();
        final Set<MRTableMetrics> all = stats.getAllTableMetrics();
        /* copy req/resp stat and refresh the original if interval stat */
        final ReqRespStat reqRespSt = parent.getReqRespMan().getMetrics();
        if (intvStats) {
            sm.setNRequest(reqRespSt.getReqAndRefresh());
            sm.setNResp(reqRespSt.getRespAndRefresh());
        } else {
            sm.setNRequest(reqRespSt.getRequests());
            sm.setNResp(reqRespSt.getResponses());
        }
        /* aggregate over all regions */
        sm.setPuts(all.stream().mapToLong(MRTableMetrics::getPuts).sum());
        sm.setDels(all.stream().mapToLong(MRTableMetrics::getDels).sum());
        sm.setWinPuts(all.stream().mapToLong(MRTableMetrics::getWinPuts).sum());
        sm.setWinDels(all.stream().mapToLong(MRTableMetrics::getWinDels).sum());
        sm.setStreamBytes(
            all.stream().mapToLong(MRTableMetrics::getStreamBytes)
               .sum());
        sm.setPersistStreamBytes(
            all.stream().mapToLong(MRTableMetrics::getPersistStreamBytes)
               .sum());
        sm.setIncompatibleRows(
            all.stream().mapToLong(MRTableMetrics::getIncompatibleRows)
               .sum());
        /* set per-region stat */
        ams.forEach(ra -> sm.setRegionStat(ra.getSourceRegion(),
                                           getRegionMetrics(ra)));
        /* set collection time */
        sm.setBeginMs(lastReportTimeMs);
        sm.setEndMs(now);
    }

    /**
     * Returns region metrics built from shard metrics and lagging metrics
     * from stream.
     *
     * @return region metrics
     */
    private JsonRegionStat getRegionMetrics(BaseRegionAgentMetrics ra) {
        final String regionName = ra.getSourceRegion();
        final RegionAgentThread rht = parent.getRegionAgent(regionName);
        final BaseRegionAgentSubscriber subscriber = rht.getSubscriber();
        if (subscriber == null) {
            /* too early */
            return null;
        }
        final NoSQLSubscriptionImpl stream =
            (NoSQLSubscriptionImpl) subscriber.getSubscription();
        if (stream == null) {
            /* too early */
            return null;
        }
        return new JsonRegionStat(stream.getFilterMetrics(),
                                  ra.getShardMetrics());
    }

    /**
     * Aggregates table metrics from all regions
     *
     * @param ams   all region metrics
     * @param table table name
     * @param ts    collection timestamp
     */
    private void collectTableMetrics(Set<BaseRegionAgentMetrics> ams,
                                     String table, long ts) {
        /* build a map region -> table metrics */
        final Map<String, MRTableMetrics> regionStats = new HashMap<>();
        for (BaseRegionAgentMetrics m : ams) {
            final String region = m.getSourceRegion();
            final MRTableMetrics tbm = m.getTableMetrics(table);
            if (tbm != null) {
                regionStats.put(region, tbm);
            }
        }

        if (regionStats.isEmpty()) {
            /*
             * Table has been removed in every region. Stats collection and
             * agent are running in parallel, table can be removed any time
             * in the underlying stream
             */
            return;
        }

        final String localRegion = parent.getMdMan().getServRegion().getName();
        final long tid = regionStats.values().iterator().next().getTableId();
        final MRTableMetrics tbm = new MRTableMetrics(localRegion, table, tid);
        /*
         * For each region, set the copy of per-region initialization stat,
         * and aggregate the stream stats of the table
         */
        regionStats.forEach((region, tableMetrics) -> {
            /* copy table initialization stat if exists */
            final TableInitStat tis = tableMetrics.getRegionInitStat(region);
            tbm.setRegionInitialization(new TableInitStat(tis));
            /* aggregate table streaming stat */
            tbm.aggregateStreamStat(tableMetrics);
        });

        tbm.setBeginMs(lastReportTimeMs);
        tbm.setEndMs(ts);
        tbm.setAgentId(stats.getAgentId());
        logger.fine(() -> lm("Collected table stat=" + tbm));
        stats.addTableMetrics(tbm);
    }

    /**
     * Collects the stat and report to server
     */
    public void collectReportStat() {

        logger.fine(() -> lm("Stat collection starts"));
        try {
            final long ts = collect();
            if (sysTable == null) {
                sysTable = parent.getMdMan().getLocalTableRetry(TABLE_NAME, 1);
                if (sysTable == null) {
                    logger.info(lm("Table=" + TABLE_NAME + " not found, " +
                                   "dump stats=\n" + stats));
                    return;
                }
            }

            logger.finest(() -> lm("Start report stat to table=" + TABLE_NAME));

            /* build a row for service stat  */
            final Row row = sysTable.createRow();
            final String id = (parent == null) ? agentId /* unit test only */ :
                Integer.toString(parent.getSid().getIndex());
            row.put(COL_NAME_AGENT_ID, id);
            row.put(COL_NAME_TIMESTAMP, ts);
            /* agent stat */
            row.put(COL_NAME_TABLE_ID, TABLE_ID_AGENT_STAT);
            row.putJson(COL_NAME_STATISTICS,
                        JsonUtils.print(stats.getServiceMetrics(), true));
            /* write a row */
            reportStat(row);
            logger.finest(() -> lm("Agent stats=" + row.toJsonString(true)));

            /* add per-table stat */
            final Set<String> tbls = stats.getTables();
            for (String t : tbls) {
                final MRTableMetrics tm = stats.getTableMetrics(t);
                final Row tblStat = sysTable.createRow();
                final long tableId = tm.getTableId();
                tblStat.put(COL_NAME_AGENT_ID, agentId);
                tblStat.put(COL_NAME_TIMESTAMP, ts);
                tblStat.put(COL_NAME_TABLE_ID, tableId);
                tblStat.putJson(COL_NAME_STATISTICS, JsonUtils.print(tm, true));
                /* write a row */
                reportStat(tblStat);
                logger.finest(() -> lm("Table=" + t +
                                       "(id=" + tableId + ") stats=" +
                                       tblStat.toJsonString(true)));
            }
            lastReportTimeMs = ts;
        } catch (InterruptedException ie) {
            if (!shutDownRequested) {
                logger.info(lm("Interrupted and exit, error=" + ie));
            }
        } catch (Exception exp) {
            logger.warning(lm("Unable to report, stat might be missing or " +
                              "incomplete, error=" + exp + ", stat=" + stats));
            logger.fine(() -> lm(LoggerUtils.getStackTrace(exp)));
            /*
             * In case the table handle is obsolete, update it when putting the
             * next stat row.
             * */
            sysTable = null;
        }
    }

    /**
     * Writes to stats table with retry
     *
     * @param row  row to write
     */
    private void reportStat(Row row) throws InterruptedException {

        int attempts = 0;
        row.setTTL(statRowTTL);
        while (!shutDownRequested) {
            try {
                attempts++;
                tableAPI.put(row, null, STAT_TABLE_WRITE_OPT);
                break;
            } catch (FaultException fe) {

                if (attempts == MAX_NUM_ATTEMPTS) {
                    final String err = "Cannot write the stat table= " +
                                       TABLE_NAME + " after attempts=" +
                                       MAX_NUM_ATTEMPTS;
                    logger.warning(lm(err));
                    break;
                }

                /* retry on fe */
                final String err = "Unable to write the stat table= " +
                                    TABLE_NAME + ", will retry after " +
                                   "time(ms)=" + RETRY_INTERVAL_MS +
                                   ", error=" + fe;
                logger.fine(() -> lm(err));
                synchronized (executor) {
                    if (shutDownRequested) {
                        break;
                    }
                    executor.wait(RETRY_INTERVAL_MS);
                }
            } catch (UnauthorizedException ue) {
                final String err = "Unauthorized to write table=" + TABLE_NAME +
                                   ", stat=" + row.toJsonString(true) +
                                   ", error=" + ue;
                logger.warning(lm(err));
                break;
            }
        }

        if (shutDownRequested) {
            /* let out caller to handle and exit */
            throw new InterruptedException(
                "Stats manager is in shutdown, abort writing to " +
                "table=" + TABLE_NAME + ", row=" + row.toJsonString(true));
        }
    }

    /**
     * A KV thread factory that logs and cancels the subscription if a thread
     * gets an unexpected exception.
     */
    private class StatReportThreadFactory extends KVThreadFactory {
        StatReportThreadFactory() {
            super("XRegionServiceStatReportThread", logger);
        }
        @Override
        public Thread.UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return (thread, ex) -> operationFailed(ex);
        }
    }

    /**
     * Log an unexpected exception that occurs during an operation
     */
    private void operationFailed(Throwable error) {
        logger.warning(lm("Cannot write statistics, error=" + error +
                          ", call stack=\n" +
                          LoggerUtils.getStackTrace(error)));
    }

    /**
     * Unit test only
     * Sets the system table instance in some unit test where parent xregion
     * service is not available
     * @param sys sys table instance
     */
    public void setSysTable(Table sys) {
        if (parent != null) {
            return;
        }
        sysTable = sys;
    }
}
