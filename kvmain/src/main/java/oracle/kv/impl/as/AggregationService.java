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

package oracle.kv.impl.as;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.KVVersion.CURRENT_VERSION;

import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/*import java.util.concurrent.atomic.AtomicLong;*/
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.as.AggregationService.Status.Beacon;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.RateRecord;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.HostPort;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.Ping.ExitCode;
import oracle.kv.util.shell.ShellCommandResult;

import oracle.nosql.common.json.ObjectNode;

/**
 * An aggregation service that collects throughput data and report limit
 * requests.
 *
 * Each KV store has one aggregation service. The service is a centralized
 * process to collect throughput data from all RNs and report back limit
 * requests. The throughput data collection adopts a polling mechanism for a
 * short interval, say, 10 seconds. The service then evaluates, for each table,
 * whether the RNs should be limiting the throughput. The evaluation adjusts
 * for credits from past history of long time, say, 3 minutes. The service then
 * send back limit requests to each RN according to the evaluation.
 *
 * The throughput evaluation will choose to underestimate user throughput when
 * the information is not available, for two reasons:
 * (1) In principle, we want to favor the user when we are not certain.
 * (2) The RNs caps the throughput by users' set limits when the aggregation
 * service is less responsive which makes the credit mechanism less effective.
 */
public class AggregationService {

    /*
     * Peak throughput history table definition.
     */
    public static final String PEAK_TABLE_NAME = "PeakThroughput";
    public static final int PEAK_TABLE_VERSION = 1;
    public static final String PEAK_TABLE_ID_FIELD_NAME = "id";
    public static final String PEAK_START_SECOND_FIELD_NAME = "startSecond";
    public static final String PEAK_READ_KB_FIELD_NAME = "peakReadKB";
    public static final String PEAK_WRITE_KB_FIELD_NAME = "peakWriteKB";

    /* Keep throughput history for less than 30 min. */
    private static final int MAX_HISTORY_SECONDS = 60 * 30;

    private static final Logger logger =
        ClientLoggerUtils.getLogger(AggregationService.class, "as");

    private final RateLimitingLogger<String> rateLimitLogger;

    private KVStore kvStore;
    private final TableAPI tableAPI;

    private final TableSizeAggregator tableAggregator;
    private final ScheduledExecutorService executor;
    private final int throughputPollPeriodSec;
    private final int tableSizePollPeriodSec;
    private final int peakThroughputCollectionPeriodSec;
    private final TimeToLive peakThroughputTTL;

    /* if this is true, don't send overage reports back to RNs */
    final boolean disableOverages;

    /*
     * Size of throughput history maintained for each table. The history
     * size = history time / throughput poll period
     */
    private final int historySize;

    /* Time to keep inactive acculator objects around. */
    private final int accumlatorTimeoutSec;

    private volatile LoginManager loginManager;

    private volatile boolean stop = false;
    private volatile boolean started = false;
    private volatile Topology topology;

    private volatile Collection<UsageRecord> sizeUsageRecords = null;

    /*  Throughput history. table ID -> accumulator */
    private final Map<Long, ThroughputAccumulator> accumulators =
                                                                new HashMap<>();

    /*
     * Map of table ID to peak throughput records. The map is replaced during
     * at the start of each collection period.
     */
    private volatile Map<Long, PeakRecord> peakRecords;

    /*
     * Starting second for the peak data set. This is initialized to
     * MAX_VALUE and set to the earliest time in found.
     */
    /* TODO - Because of the restriction to Java6 RateRecord cannot implement
     * LongUnaryOperator which would allow the use of AtomicLong for
     * peakStartSecond. So we need to synchronize setting and access.
     */
  /*private final AtomicLong peakStartSecond = new AtomicLong(Long.MAX_VALUE);*/
    private long peakStartSecond = Long.MAX_VALUE;

    /* Cached handle to the peak throughout table */
    private Table peakTable = null;

    /*
     * set by a caller if an instance of AS is created in-process.
     * it is used to allow clean stop/shutdown.
     */
    private Thread aggThread;

    /*
     * To save error messages and the worst health code AS ran into, until
     * it resets to a new Status.
     * It will be get and reset periodically by ASManager to report AS health
     * data.
     */
    private Status status;

    /* For testing */
    private long pollCount = 0L;

    /* Move down once compatibility constructor is removed */
    private static final int THROUGHPUT_HISTORY_DEFAULT_SEC = 60 * 3;

    /* For backward compatibility */
    @Deprecated
    public AggregationService(String storeName,
                              List<String> hostPorts,
                              int throughputPollPeriodSec,
                              int tableSizePollPeriodSec,
                              int peakThroughputCollectionPeriodSec,
                              int peakThroughputTTLDay,
                              int maxThreads)
            throws KVStoreException {
        this(storeName,
             hostPorts,
             THROUGHPUT_HISTORY_DEFAULT_SEC,
             throughputPollPeriodSec,
             tableSizePollPeriodSec,
             peakThroughputCollectionPeriodSec,
             peakThroughputTTLDay,
             maxThreads);
    }

    public AggregationService(String storeName,
                              List<String> hostPorts,
                              int throughputHistorySec,
                              int throughputPollPeriodSec,
                              int tableSizePollPeriodSec,
                              int peakThroughputCollectionPeriodSec,
                              int peakThroughputTTLDay,
                              int maxThreads)
            throws KVStoreException {
        /* backwards compatibility */
        /* determine disableOverages from system props */
        this(storeName,
             hostPorts,
             throughputHistorySec,
             throughputPollPeriodSec,
             tableSizePollPeriodSec,
             peakThroughputCollectionPeriodSec,
             peakThroughputTTLDay,
             maxThreads,
             Boolean.getBoolean("disableoverages"));
    }

    public AggregationService(String storeName,
                              List<String> hostPorts,
                              int throughputHistorySec,
                              int throughputPollPeriodSec,
                              int tableSizePollPeriodSec,
                              int peakThroughputCollectionPeriodSec,
                              int peakThroughputTTLDay,
                              int maxThreads,
                              boolean disableOverages)
            throws KVStoreException {
        if (throughputPollPeriodSec < 1) {
            throw new IllegalArgumentException("Throughput poll period" +
                                               " must be > 0");
        }
        if (throughputPollPeriodSec >= throughputHistorySec / 2) {
            throw new IllegalArgumentException("Throughput poll period" +
                                               " must be < " +
                                               throughputHistorySec / 2);
        }
        if (tableSizePollPeriodSec < 1) {
            throw new IllegalArgumentException("Table size poll period" +
                                               " must be > 0");
        }
        if (peakThroughputCollectionPeriodSec < 1) {
            throw new IllegalArgumentException("Peak throughput collection" +
                                               " period must be > 0");
        }
        if (peakThroughputTTLDay < 1) {
            throw new IllegalArgumentException("Peak throughput TTL" +
                                               " must be > 0");
        }
        if (throughputHistorySec > MAX_HISTORY_SECONDS)  {
            throw new IllegalArgumentException("Throughput history must be" +
                                               " less than " +
                                               MAX_HISTORY_SECONDS +
                                               " seconds");
        }
        if (throughputHistorySec < throughputPollPeriodSec)  {
            throw new IllegalArgumentException("Throughput history must be" +
                                               " greater than the throughput" +
                                               " poll period");
        }
        this.throughputPollPeriodSec = throughputPollPeriodSec;
        this.tableSizePollPeriodSec = tableSizePollPeriodSec;
        this.peakThroughputCollectionPeriodSec =
                                        peakThroughputCollectionPeriodSec;
        this.disableOverages = disableOverages;
        peakThroughputTTL = TimeToLive.ofDays(peakThroughputTTLDay);
        peakRecords = new ConcurrentHashMap<>();
        historySize = throughputHistorySec / throughputPollPeriodSec;
        assert historySize > 0;
        accumlatorTimeoutSec = throughputHistorySec * 2;

        rateLimitLogger = new RateLimitingLogger<>(60 * 1000, 10, logger);

        logger.log(Level.INFO,
                   "Starting AggregationService {0} for {1}," +
                   " throughput history: {2} seconds," +
                   " throughput poll period: {3} seconds," +
                   " table size poll period: {4} seconds," +
                   " peak throughput collection period: {5} seconds," +
                   " peak throughput TTL: {6} days," +
                   " overages disabled: {7}",
                   new Object[]{CURRENT_VERSION.getNumericVersionString(),
                                storeName,
                                throughputHistorySec,
                                throughputPollPeriodSec,
                                tableSizePollPeriodSec,
                                peakThroughputCollectionPeriodSec,
                                peakThroughputTTLDay,
                                disableOverages});

        final KVStoreConfig kvConfig = new KVStoreConfig(storeName,
                                                         hostPorts.get(0));
        kvStore = KVStoreFactory.getStore(kvConfig);
        tableAPI = kvStore.getTableAPI();
        loginManager = KVStoreImpl.getLoginManager(kvStore);

        topology = findTopo(hostPorts, maxThreads);
        assert topology != null;
        logger.log(Level.INFO, "Initial topology seq# {0}",
                   topology.getSequenceNumber());

        tableAggregator = new TableSizeAggregator(tableAPI, logger);
        executor = Executors.newScheduledThreadPool(maxThreads);
        status = new Status();
    }

    /* Synchronously execute the polling loop. */
    public void startPolling() throws InterruptedException {
        if (started) {
            return;
        }
        started = true;
        start();
    }

    private void start() throws InterruptedException {
        try {
            /*
             * Schedule a task to collect size information. At each call to
             * getTablesSizes sizeUsageRecords with be set to the latest size
             * information. The usage records, if any, are sent to the
             * RNs when polling for throughput information.
             *
             * Polling is delayed slightly so that it starts after the
             * key stats collection on the server is done (or at least
             * started). This assumes the server uses calculateDelay()
             * to control its scanning.
             */
            final long pollPeriodMillis =
                                      SECONDS.toMillis(tableSizePollPeriodSec);
            final long initialDelayMillis =
                    ScheduleStart.calculateDelay(pollPeriodMillis,
                                                 System.currentTimeMillis()) +
                    pollPeriodMillis/4;
            final long initialDelaySeconds =
                MILLISECONDS.toSeconds(initialDelayMillis);

            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        sizeUsageRecords = tableAggregator.getTableSizes(
                            tableSizePollPeriodSec * 1000,
                            AggregationService.this);
                    } catch (FaultException | StoreIteratorException e) {
                        logger.log(Level.WARNING,
                                   "Exception collecting table sizes {0}",
                                   e.getMessage());
                    } catch (Exception e) {
                        logger.log(Level.SEVERE,
                                  "Unexpected exception collecting table sizes",
                                   e);
                        stop = true;
                    }
                }
            }, initialDelaySeconds, tableSizePollPeriodSec, SECONDS);

            /*
             * Schedule a task to export peak throughput information.
             */
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        exportPeakThroughput();
                    } catch (Exception e) {
                        /*
                         * Note that we do not treat failures exporting peak
                         * throughout as fatal. We log the failure and export
                         * will be retried at the end of the next collection
                         * period. If the failures continue, hopefully the
                         * health monitor will take action.
                         */
                        logger.log(Level.WARNING,
                                   "Unexpected exception exporting peak" +
                                   " throughput", e);
                    }
                }
            },
            /*
             * Delay the initial run so that there is time to collect
             * peak data.
             */
            peakThroughputCollectionPeriodSec,  /* initialDelay */
            peakThroughputCollectionPeriodSec,  /* period */
            SECONDS);

            /*
             * Initialize the last call to be a poll period ago. This should
             * get the usual history to start things off.
             */
            final long periodMillis = SECONDS.toMillis(throughputPollPeriodSec);
            long lastCallMillis = System.currentTimeMillis() - periodMillis;

            /*
             * Throughput polling loop. Note that table size limits are sent
             * to the RNs here, not in the thread collecting size info.
             */
            while (!stop) {
                final long startMillis = System.currentTimeMillis();
                pollThroughput(lastCallMillis);
                lastCallMillis = startMillis;

                /*
                 * If the poll took less than the period, sleep for remaining
                 * time.
                 */
                final long finishMillis = System.currentTimeMillis();
                final long durationMillis = finishMillis - startMillis;
                final long healthyMillis = periodMillis / 2;
                final long remaining = periodMillis - durationMillis;
                if (remaining < 0) {
                    recordHealth(Beacon.RED,
                                 "Throughput collection did not complete " +
                                 "within polling period");
                } else if (durationMillis > healthyMillis) {
                    recordHealth(Beacon.YELLOW,
                                 "Throughput collection did not complete " +
                                 "within 50% polling period");
                }
                if (!stop && (remaining > 0)) {
                    Thread.sleep(remaining);
                }
                pollCount++;
            }
        } finally {
            executor.shutdownNow();
        }
        logger.info("Shutdown");
    }

    /* For testing */
    public long getPollCount() {
        return pollCount;
    }

    /**
     * Polls the RNs for throughput information. During the poll the size
     * records, if any, are sent to each node. Once the throughput information
     * is collected, the rates are aggregated and if any limits are exceeded
     * the limit records are sent to all of the RNs.
     */
    private void pollThroughput(long lastCallMillis)
            throws InterruptedException {

        /*
         * Create a local reference to tableSizeRecords since it may change.
         * The size limit records only need to be sent once. So if there
         * are no errors calling the RNs, the records can be cleared. It will
         * be OK to modify the collection using this reference (see below)
         * since the contents are not changed elsewhere.
         */
        final Collection<UsageRecord> sizeRecords = sizeUsageRecords;

        /*
         * Call all nodes to get resource info since lastCallMills. Send
         * any table size limit records.
         */
        final List<Future<ResourceInfo>> results =
                                    callAllNodes(lastCallMillis, sizeRecords);
        if (results == null) {
            return;
        }

        final TreeSet<RateRecord> records = new TreeSet<>();

        /*  Keep track of errors so we can clear the size limit records. */
        int errors = 0;

        /*
         * Collect all of the returned rate records. They are sorted by second.
         */
        for (Future<ResourceInfo> f : results) {
            if (!f.isDone() || f.isCancelled()) {
                final String result = f.isCancelled() ? "canceled" :
                                                        "incomplete";
                rateLimitLogger.log(result, Level.WARNING,
                                    "Task did not complete: " + result);
                errors++;
                continue;
            }
            try {
                final ResourceInfo info = f.get();
                if (info == null) {
                    errors++;
                    continue;
                }
                records.addAll(info.getRateRecords());
            } catch (Exception ex) {
                errors++;
                rateLimitLogger.log(ex.getClass().getName(),
                                    Level.WARNING,
                                    "Task failed " + ex.getLocalizedMessage());
            }
        }

        /* If re-authentication happened, renew cached login manager */
        LoginManager newLogin = KVStoreImpl.getLoginManager(kvStore);
        if (loginManager != newLogin) {
            loginManager = newLogin;
        }

        logger.log(Level.FINE, "Collected {0} records, {1} errors",
                   new Object[]{records.size(), errors});
        if (errors > 0) {
            final String errWord = (errors > 1 ? " errors" : " error");
            String errMsg = "Polling throughput had " +
                            errors + errWord;
            if (errors == results.size()) {
                recordHealth(Beacon.RED, errMsg);
            } else {
                recordHealth(Beacon.YELLOW, errMsg);
            }
        }

        /*
         * Iterate through the rate records in second order. For each
         * second accumulate the rates in the accumulators map. Then
         * see if any were over for that second. Any overages will generate
         * a record in the overageRecords map.
         */
        for (RateRecord rr : records) {
            /* Set second to earliest seen. Uses LongUnaryOperator to update */
            /*peakStartSecond.getAndUpdate(rr);*/
            updatePeakStartSecond(rr.getSecond());

            final long tableId = rr.getTableId();
            final ThroughputAccumulator accumulator = accumulators.get(tableId);
            if (accumulator == null) {
                accumulators.put(tableId, new ThroughputAccumulator(rr));
            } else {
                accumulator.add(rr);
            }
        }
        updatePeakRecords();

        /*
         * getUsageRecords() needs to be called, even if we're not sending
         * overages back to the RNs, because it has other side effects:
         * clearing old entries, rotating current values, etc...
         */
        final Map<Long, UsageRecord> throughputUsageMap = getUsageRecords();

        /*
         * If it's not disabled, and there were throughput overages, send
         * them. Passing a 0 for lastCall indicates that we do not want
         * throughput data to be sent back.
         */
        if (!disableOverages && !throughputUsageMap.isEmpty()) {
            callAllNodes(0, new ArrayList<>(throughputUsageMap.values()));
        }

        /*
         * If there were no errors, we can safely remove size limit records.
         */
        if ((errors == 0) && (sizeRecords != null)) {
            sizeRecords.clear();
        }
    }

    private synchronized void updatePeakStartSecond(long second) {
        if (second < peakStartSecond) {
            peakStartSecond = second;
        }
    }

    private synchronized long getAndResetPeakStartSecond() {
        final long ret = peakStartSecond;
        peakStartSecond = Long.MAX_VALUE;
        return ret;
    }

    /**
     * Creates usage records from the accumulators.
     */
    private Map<Long, UsageRecord> getUsageRecords() {
        final long currentSecond = System.currentTimeMillis() / 1000;

        final Map<Long, UsageRecord> throughputUsageMap = new HashMap<>();

        final Iterator<ThroughputAccumulator> itr =
                                            accumulators.values().iterator();
        while (itr.hasNext()) {
            final ThroughputAccumulator ta = itr.next();

            /* Age out inactive tables */
            if ((currentSecond - ta.second) > accumlatorTimeoutSec) {
                logger.log(Level.FINE,
                           "Removing {0} after {1} seconds",
                           new Object[]{ta, currentSecond - ta.second});
                itr.remove();
                continue;
            }
            final UsageRecord ra = ta.getUsageRecord();
            if (ra != null && disableOverages == false) {
                assert !throughputUsageMap.containsKey(ta.tableId);
                throughputUsageMap.put(ta.tableId, ra);
            }
        }
        return throughputUsageMap;
    }

    /**
     * Updates the peak record from the throughput accumulators.
     * Peak records are created and added to the peakRecords map as needed.
     */
    private void updatePeakRecords() {
        for (ThroughputAccumulator ta : accumulators.values()) {
             /*
             * Track peak throughput which is independent of the second. Note
             * that peakRecords may be refreshed between the get and set. That
             * is OK because pr will also be missing from the new (empty) map.
             */
            final PeakRecord pr = peakRecords.get(ta.tableId);
            if (pr == null) {
                peakRecords.put(ta.tableId,
                                new PeakRecord(ta.peakReadKB, ta.peakWriteKB));
            } else {
                pr.update(ta.peakReadKB, ta.peakWriteKB);
            }
        }
    }

    /* unit test only */
    void setStoreHandle(KVStore store) {
        this.kvStore = store;
        loginManager = KVStoreImpl.getLoginManager(kvStore);
    }

    /**
     * Calls getResourceInfo on all RNs in the store, returning the list of
     * futures with the results. The lastCall and usageRecords are passed to
     * the getResourceInfo method. If usageRecords is empty, null is sent.
     */
    List<Future<ResourceInfo>> callAllNodes(long lastCall,
                                            Collection<UsageRecord> usageRecords)
        throws InterruptedException {

        /* Send null if there are no records */
        final Collection<UsageRecord> usageRecord =
              ((usageRecords != null) && usageRecords.isEmpty()) ? null :
                                                                   usageRecords;

        final List<Callable<ResourceInfo>> tasks = new ArrayList<>();
        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            /* Generate tasks for each group */
            final RepGroup group = topology.get(rg.getResourceId());
            if (group == null) {
                logger.log(Level.INFO, "{0} missing from topo seq# {1}",
                           new Object[]{rg, topology.getSequenceNumber()});
                continue;
            }

            /* LoginManager not needed ??? */
            final RegistryUtils regUtils =
                new RegistryUtils(topology, loginManager, logger);
            for (final RepNode rn : group.getRepNodes()) {
                tasks.add(new Callable<ResourceInfo>() {
                    @Override
                    public ResourceInfo call() throws Exception {
                        final RepNodeId rnId = rn.getResourceId();

                        boolean reauth = false;
                        LoginManager requestLoginMgr = loginManager;
                        try {
                            return exchange(rnId, regUtils);
                        } catch (RepNodeAdminFaultException rafe) {
                            rateLimitLogger.log(
                                rnId.getFullName(),
                                Level.WARNING,
                                "Unexpected fault exception calling " +
                                rnId + " " + rafe.getMessage());
                        } catch (AuthenticationRequiredException are) {
                            reauth = true;
                        } catch (RemoteException re) {
                            if (re.getCause() instanceof
                                SessionAccessException) {

                                reauth = true;
                            }
                            rateLimitLogger.log(rnId.getFullName(),
                                                Level.WARNING,
                                                "Remote exception calling " +
                                                rnId + " " + re.getMessage());
                        } catch (Exception e) {
                            logger.log(Level.WARNING,
                                       "Unexpected exception calling " +
                                       rnId, e);
                        }

                        if (reauth) {
                            final KVStoreImpl store = (KVStoreImpl)kvStore;

                            /*
                             * Pass in original login manager. tryReauthenticate
                             * has the synchronization on a lock and skip the
                             * re-authentication if login manager has been
                             * renewed, so concurrent invocations won't result
                             * in multiple re-authentication.
                             */
                            if (!store.tryReauthenticate(requestLoginMgr)) {
                                return null;
                            }

                            /* exchange retry */
                            try {
                                LoginManager newLogin = KVStoreImpl
                                    .getLoginManager(kvStore);
                                return exchange(rnId,
                                                new RegistryUtils(topology,
                                                                  newLogin,
                                                                  logger));
                            } catch (Exception e) {
                                rateLimitLogger.log(
                                      rnId.getFullName(),
                                      Level.WARNING,
                                      "After re-authentication, still " +
                                      "failing to exchange info with " +
                                      rnId + e.getMessage());
                            }
                        }

                        /* Returning null will be recorded as an error */
                        return null;
                    }

                    private ResourceInfo exchange(RepNodeId rnId,
                                                  RegistryUtils ru)
                        throws Exception {

                        final RepNodeAdminAPI rna = ru.getRepNodeAdmin(rnId);

                        // TODO - do something with this?
                        //rna.getInfo().getSoftwareVersion();
                        final ResourceInfo info =
                            rna.exchangeResourceInfo(lastCall, usageRecord);
                        // TODO - info can be null????
                        checkTopology(info, rna);
                        return info;
                    }
                });
            }
        }

        return tasks.isEmpty() ? Collections.emptyList() :
                                 executor.invokeAll(tasks,
                                                    throughputPollPeriodSec,
                                                    TimeUnit.SECONDS);
    }

    /**
     * Writes a row to the peak throughput table for each non-empty
     * PeakRecord. The peakRecords map is recreated, and the peakSecond is
     * reset.
     */
    private void exportPeakThroughput() throws Exception {
        //final int startSecond = (int)peakStartSecond.getAndSet(Long.MAX_VALUE);
        final int startSecond = (int)getAndResetPeakStartSecond();
        final Map<Long, PeakRecord> prMap = peakRecords;
        peakRecords = new ConcurrentHashMap<>();

        final Table table = getPeakTable();
        assert table != null;

        for (Entry<Long, PeakRecord> e : prMap.entrySet()) {
            final PeakRecord pr = e.getValue();

            if (pr.hasPeak()) {
                final long tableId = e.getKey();
                logger.log(Level.FINE, "Peak for {0} starting at {1} {2}",
                           new Object[]{tableId, startSecond, pr});
                final Row row = table.createRow();
                row.put(PEAK_TABLE_ID_FIELD_NAME, tableId);
                row.put(PEAK_START_SECOND_FIELD_NAME, startSecond);
                row.put(PEAK_READ_KB_FIELD_NAME, pr.peakReadKB);
                row.put(PEAK_WRITE_KB_FIELD_NAME, pr.peakWriteKB);
                /* Set the TTL in case it is different from the table defult */
                row.setTTL(peakThroughputTTL);
                tableAPI.put(row, null, null);
            }
        }
    }

    /**
     * Gets the peak throughput table handle. The table is created if it does
     * not exist. The table handle is cached.
     */
    private Table getPeakTable() throws Exception {
        if (peakTable != null) {
            return peakTable;
        }

        peakTable = tableAPI.getTable(PEAK_TABLE_NAME);
        if (peakTable != null) {
            final int tableVersion =
                        Integer.parseInt(peakTable.getDescription());
            logger.log(Level.FINE, "Found " + PEAK_TABLE_NAME +
                       " version {0}", tableVersion);
            if (tableVersion > PEAK_TABLE_VERSION) {
                throw new Exception(PEAK_TABLE_NAME + " is at version " +
                                    tableVersion + " please upgrade the " +
                                    "aggregration service");
            }
            /*
             * TODO - Currently changing the default TTL on a table does not
             * affect existing records. If this changes, it would be worth
             * checking if the input TTL is different than the table's default
             * and if so change the table default.
             */
            return peakTable;
        }

        logger.info("Creating peak table");
        final String createDML =
                "CREATE TABLE " + PEAK_TABLE_NAME + " " +
                "COMMENT \"" + PEAK_TABLE_VERSION + "\" (" +
                        PEAK_TABLE_ID_FIELD_NAME + " LONG, " +
                        PEAK_START_SECOND_FIELD_NAME + " INTEGER, " +
                        PEAK_READ_KB_FIELD_NAME + " INTEGER, " +
                        PEAK_WRITE_KB_FIELD_NAME + " INTEGER, " +
                        "PRIMARY KEY(SHARD(" + PEAK_TABLE_ID_FIELD_NAME + "), "+
                                     PEAK_START_SECOND_FIELD_NAME + ")) " +
                "USING TTL " + peakThroughputTTL.getValue() + " DAYS";

        final StatementResult result = kvStore.executeSync(createDML);
        if (!result.isSuccessful()) {
            throw new Exception("Failed to create " +
                                PEAK_TABLE_NAME + ": " + result);
        }

        peakTable = tableAPI.getTable(PEAK_TABLE_NAME);
        if (peakTable == null) {
            throw new Exception("Unable to get " + PEAK_TABLE_NAME);
        }
        return peakTable;
    }

    /*
     * Object to record per-table peak throughput information.
     */
    private static class PeakRecord {
        private int peakReadKB;
        private int peakWriteKB;

        private PeakRecord(int readKB, int writeKB) {
            peakReadKB = readKB;
            peakWriteKB = writeKB;
        }

        /*
         * Updates the peak read and write peak data if the input values are
         * greater.
         */
        private void update(int readKB, int writeKB) {
            if (readKB > peakReadKB) {
                peakReadKB = readKB;
            }
            if (writeKB > peakWriteKB) {
                peakWriteKB = writeKB;
            }
        }

        /*
         * Returns true if the record has non-zero peak read or write
         * throughput data.
         */
        private boolean hasPeak() {
            return peakReadKB > 0 || peakWriteKB > 0;
        }

        @Override
        public String toString() {
            return "PeakRecord[" + peakReadKB + ", " + peakWriteKB + "]";
        }
    }

    private class ThroughputAccumulator {
        private final long tableId;
        private int readLimitKB;
        private int writeLimitKB;

        /* Per-second read/write KB */
        private int readKB = 0;
        private int writeKB = 0;

        /* Per-second peak read/write KB */
        private int peakReadKB = 0;
        private int peakWriteKB = 0;

        /* Accumulated read/write values */
        private int totalReadKB = 0;
        private int totalWriteKB = 0;

        private long second = 0L;

        /* Number of seconds of accumulation. */
        private int nSeconds = 0;

        /*
         * The histories are circular buffers which record the total
         * throughput at the time reset is called.
         */
        private final int[] readHistory;
        private final int[] writeHistory;

        /* Index into the last written history element. */
        private int historyIndex = 0;

        ThroughputAccumulator(RateRecord rr) {
            tableId = rr.getTableId();
            readHistory = new int[historySize];
            writeHistory = new int[historySize];
            add(rr);
        }

        /*
         * Adds the read and write KB from the rate records tracking both the
         * total read/write KB and the per-second peak read/write KB. The
         * rate record must be added in second order in order for the peak
         * values to be correct.
         */
        private void add(RateRecord rr) {
            assert rr.getTableId() == tableId;
            assert rr.getSecond() >= second;

            readLimitKB = rr.getReadLimitKB();
            writeLimitKB = rr.getWriteLimitKB();

            /*
             * If this is a new second (or first call) reset the per-second
             * read and write values.
             */
            if (second != rr.getSecond()) {

                /*
                 * The RN may return more rate records then needed. Only record
                 * the period number of seconds. Otherwise totals and averages
                 * will be off.
                 */
                if (nSeconds >= throughputPollPeriodSec) {
                    return;
                }
                second = rr.getSecond();
                nSeconds++;
                readKB = 0;
                writeKB = 0;
            }

            /* Accumulate the per-second and overal totals */
            readKB += rr.getReadKB();
            writeKB += rr.getWriteKB();
            totalReadKB += rr.getReadKB();
            totalWriteKB += rr.getWriteKB();

            /* Update the peak values if needed */
            if (readKB > peakReadKB) {
                peakReadKB = readKB;
            }
            if (writeKB > peakWriteKB) {
                peakWriteKB = writeKB;
            }
        }

        /*
         * Returns a usage record if there has been a throughput overage that
         * requires reporting, otherwise null is returned.
         */
        private UsageRecord getUsageRecord() {
            boolean report = false;

            /*
             * If the read or write throughput for this period is over the
             * limit check the history to see if there is a credit that
             * can be applied.
             */

            int adjustedReadTotal = totalReadKB;
            if (peakReadKB > readLimitKB) {

                /*
                 * History contains total throughput. Subtract it from the
                 * total allowed over that time to get a credit (positive)
                 * or deficit (negative).
                 */
                int credit = (readLimitKB * throughputPollPeriodSec *
                                                                   historySize);
                for (int i = 0; i < historySize; i++) {
                    credit -= readHistory[i];
                }

                /* If there is a credit apply it to the total */
                if (credit > 0) {
                    adjustedReadTotal -= credit;

                    /*
                     * If there was enough credit to cover the overage, set
                     * adjusted total to 0 to avoid reporting
                     */
                    if (adjustedReadTotal < 0) {
                        adjustedReadTotal = 0;
                    }
                }

                /* If after adjustment there is a total, report it */
                if (adjustedReadTotal > 0) {
                    report = true;
                }
            }

            int adjustedWriteTotal = totalWriteKB;
            if (peakWriteKB > writeLimitKB) {
                int credit = (writeLimitKB * throughputPollPeriodSec *
                                                                  historySize);
                for (int i = 0; i < historySize; i++) {
                    credit -= writeHistory[i];
                }
                if (credit > 0) {
                    adjustedWriteTotal -= credit;

                    if (adjustedWriteTotal < 0) {
                        adjustedWriteTotal = 0;
                    }
                }
                if (adjustedWriteTotal > 0) {
                    report = true;
                }
            }
            reset();

            if (report) {
                /* Adjusted totals are per period, convert to per second */
                return new UsageRecord(tableId,
                                  adjustedReadTotal / throughputPollPeriodSec,
                                  adjustedWriteTotal / throughputPollPeriodSec);
            }
            return null;
        }

        /*
         * Records the total throughput in the history and then resets
         * all read and write counters. historyIndex is incremented.
         */
        private void reset() {
            historyIndex++;
            if (historyIndex >= historySize) {
                historyIndex = 0;
            }
            readHistory[historyIndex] = totalReadKB;
            writeHistory[historyIndex] = totalWriteKB;

            readKB = 0;
            writeKB = 0;
            peakReadKB = 0;
            peakWriteKB = 0;
            totalReadKB = 0;
            totalWriteKB = 0;
            nSeconds = 0;
        }

        @Override
        public String toString() {
            return "ThroughputAccumulator[" + tableId +
                   ", sec: " + second +
                   ", cur: "+ readKB + " " + writeKB +
                   ", pk: " + peakReadKB + " " + peakWriteKB +
                   ", lmt: " + readLimitKB + " " + ", " + writeLimitKB +
                   ", tl: " + totalReadKB + " " + totalWriteKB +
                   ", secs: " + nSeconds + "]";
        }
    }

    private Topology findTopo(List<String> hostPorts, int maxThreads)
        throws KVStoreException {

        if (hostPorts == null) {
            throw new IllegalArgumentException("null hosts ports");
        }

        String[] hostPortsArray = new String[hostPorts.size()];
        hostPortsArray = hostPorts.toArray(hostPortsArray);

        /* Search available SNs for a topology */
        Topology newtopo = null;

        /*
         * The search for a new topo is confined to SNs that host RNs. If
         * Admins live on SNs which don't host RNs, we'll be delayed in
         * seeing a new topo; we'd have to wait for that to be propagated to
         * the RNs. That's ok; by design, the system will propagate topos to
         * RNs in a timely fashion, and it's not worth adding complications
         * for the unusual case of an Admin-only SN.
         */
        try {
            newtopo = TopologyLocator.get(hostPortsArray, 0,
                                          loginManager, null, null);
        } catch (KVStoreException topoLocEx) {
            /* had a problem getting a topology - try using the Admins */
            newtopo = searchAdminsForTopo(hostPortsArray, maxThreads);

            /* Still can't find a topology */
            if (newtopo == null) {
                throw topoLocEx;
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception locating topology: {0}", e);
        }
        return newtopo;
    }

    /**
     * Given a set of SNs, find an AdminService to find a topology
     */
    private Topology searchAdminsForTopo(String[] hostPortStrings,
                                         int maxThreads) {
        final HostPort[] targetHPs = HostPort.parse(hostPortStrings);

        /* Look for admins to get topology */
        final Collection<Callable<Topology>> tasks = new ArrayList<>();
        for (final HostPort hp : targetHPs) {
            tasks.add(new Callable<Topology>() {
                @Override
                public Topology call() throws Exception {
                    try {
                        final CommandServiceAPI admin =
                            getAdmin(hp.hostname(), hp.port());
                        return admin.getTopology();
                    } catch (RemoteException re) {
                        logger.log(Level.SEVERE,
                                   "Exception attempting to contact Admin {0}",
                                   re);
                        /*
                         * Throw out all Exceptions to tell this task failed to
                         * get topology.
                         */
                        throw re;
                    }
                }
            });
        }

        final ExecutorService es =
                Executors.newFixedThreadPool(maxThreads);
        try {
            /*
             * Returns the topology result got by the first completed task.
             */
            return es.invokeAny(tasks);
        } catch (Exception e) {
            /*
             * If it throws Exception, that means all task failed.
             * Can't find any Admins, there should be some in the list.
             */
            logger.severe("Searching for topology, can't contact any " +
                          "Admin services in the store");
            return null;
        } finally {
            es.shutdownNow();
        }
    }

    /**
     * Get the CommandService on this particular SN.
     */
    private CommandServiceAPI getAdmin(String snHostname, int snRegistryPort)
        throws NotBoundException, RemoteException {
        /*
         * Use login manager first, if it is available.
         */
        if (loginManager != null) {
            return RegistryUtils.getAdmin(snHostname, snRegistryPort,
                                          loginManager, logger);
        }

        /*
         * Non-secure case.
         */
        return RegistryUtils.getAdmin(snHostname, snRegistryPort, null,
                                      logger);
    }

    /**
     * Checks to see if the topology needs to be updated. The info object
     * contains the topo sequence number at that node. Check it against
     * the topo we have. If it is newer get the topo from the RN.
     */
    private void checkTopology(ResourceInfo info, RepNodeAdminAPI rna)
        throws RemoteException {
        if (info == null) {
            return;
        }
        if (topology.getSequenceNumber() >= info.getTopoSeqNum()) {
            return;
        }
        logger.log(Level.FINE, "Need to update topo, {0} >= {1}",
                   new Object[]{topology.getSequenceNumber(),
                                info.getTopoSeqNum()});
        final Topology newTopo = rna.getTopology();
        synchronized (this) {
            if (topology.getSequenceNumber() < newTopo.getSequenceNumber()) {
                logger.log(Level.FINE, "Updating to topopogy seq# {0}",
                           newTopo.getSequenceNumber());
                topology = newTopo;
            }
        }
    }

    /**
     * Get AS status that have a list of error messages, and the worst health
     * code AS ran into. And then reset it to GREEN status.
     */
    public synchronized Status getAndResetStatus() {
        Status oldStatus = status;
        status = new Status();
        return oldStatus;
    }

    synchronized void recordHealth(Beacon newBeacon, String msg) {
        status.recordHealth(newBeacon, msg);
    }

    /**
     * It is the aggregation of AS status. It records error messages and
     * the worst health code AS has ever encountered, until AS reset to a new
     * Status. That means once it is set to Red, it will always be Red. It
     * can't change back to YELLOW or GREEN. Once it is set to YELLOW, it can't
     * change back to GREEN.
     */
    public static final class Status {
        private Beacon beacon;
        private static final int ERROR_SIZE_LIMIT = 100;
        private final LinkedList<String> errors;

        Status() {
            beacon = Beacon.GREEN;
            errors = new LinkedList<>();
        }

        void recordHealth(Beacon newBeacon, String msg) {
            if (newBeacon.ordinal() > beacon.ordinal()) {
                beacon = newBeacon;
            }
            final String error = System.currentTimeMillis() + " " +
                newBeacon.name() + " " + msg;
            while (errors.size() >= ERROR_SIZE_LIMIT) {
                errors.poll();
            }
            errors.offer(error);
        }

        /**
         * Get the Beacon that represent the worst status AS ran into.
         */
        public Beacon getBeacon() {
            return beacon;
        }

        /**
         * Return an ordered list of error messages that AS has encountered.
         */
        public List<String> getErrors() {
            return errors;
        }

        /**
         * To represent AS health level.
         */
        public static enum Beacon {
            /* Do not change order */
            GREEN, YELLOW, RED
        }
    }

    public static final String COMMAND_NAME = "aggregationservice";
    public static final String COMMAND_DESC =
                                    "monitors resource usage of a store";
    private static final String HELPER_HOSTS_FLAG = "-helper-hosts";
    private static final String THROUGHPUT_POLL_PERIOD_FLAG =
                                                    "-throughput-poll-period";
    private static final int THROUGHPUT_POLL_PERIOD_DEFAULT_SEC = 5;
    private static final String TABLE_SIZE_POLL_PERIOD_FLAG =
                                                    "-table-size-poll-period";
    private static final int TABLE_SIZE_POLL_PERIOD_DEFAULT_SEC = 3600;
    private static final String PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG =
                                           "-peak-throughput-collection-period";
    private static final int PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC = 60;
    private static final String PEAK_THROUGHPUT_TTL_FLAG =
                                                        "-peak-throughput-ttl";
    private static final int PEAK_THROUGHPUT_DEFAULT_TTL_DAY = 14;

    private static final String THROUGHPUT_HISTORY_FLAG = "-throughput-history";
//    private static final int THROUGHPUT_HISTORY_DEFAULT_SEC = 60 * 3;

    private static final String MAX_THREADS_FLAG = "-max-threads";
    private static final int MAX_THREADS_DEFAULT = 10;
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + " or\n\t" +
        HELPER_HOSTS_FLAG + " <host:port[,host:port]*>\n\t" +
        THROUGHPUT_HISTORY_FLAG + "<seconds>\n\t" +
        THROUGHPUT_POLL_PERIOD_FLAG + " <seconds>\n\t" +
        TABLE_SIZE_POLL_PERIOD_FLAG + " <seconds>\n\t" +
        PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG + " <seconds>\n\t" +
        PEAK_THROUGHPUT_TTL_FLAG + " <days>\n\t" +
        MAX_THREADS_FLAG + " <n>\n\t" +
        CommandParser.optional(CommandParser.JSON_FLAG);

    private static class AggregationServiceParser extends CommandParser {
        private String helperHosts = null;
        private int throughputPollPeriodSec =
                                             THROUGHPUT_POLL_PERIOD_DEFAULT_SEC;
        private int throuputHistorySec = THROUGHPUT_HISTORY_DEFAULT_SEC;
        private int tableSizePollPeriodSec = TABLE_SIZE_POLL_PERIOD_DEFAULT_SEC;
        private int peakThroughputCollectionPeriodSec =
                                  PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC;
        private int peakThroughputTTLDay = PEAK_THROUGHPUT_DEFAULT_TTL_DAY;
        private int maxThreads = MAX_THREADS_DEFAULT;

        AggregationServiceParser(String[] args1) {
            super(args1);
        }

        @Override
        public void usage(String errorMsg) {
            /*
             * Note that you can't really test illegal arguments in a
             * threaded unit test -- the call to exit(..) when
             * dontExit is false doesn't kill the process, and the error
             * message gets lost. Still worth using dontExit so the
             * unit test process doesn't die, but unit testing of bad
             * arg handling has to happen with a process.
             */
            if (!getJson()) {
                if (errorMsg != null) {
                    System.err.println(errorMsg);
                }
                System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                                   "\n\t" + COMMAND_ARGS);
            }
            exit(errorMsg, ExitCode.EXIT_USAGE, System.err,
                 getJsonVersion());
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(HELPER_HOSTS_FLAG)) {
                helperHosts = nextArg(arg);
                return true;
            }

            if (arg.equals(THROUGHPUT_HISTORY_FLAG)) {
                throuputHistorySec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(THROUGHPUT_POLL_PERIOD_FLAG)) {
                throughputPollPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(TABLE_SIZE_POLL_PERIOD_FLAG)) {
                tableSizePollPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG)) {
                peakThroughputCollectionPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(PEAK_THROUGHPUT_TTL_FLAG)) {
                peakThroughputTTLDay = nextIntArg(arg);
                return true;
            }

            if (arg.equals(MAX_THREADS_FLAG)) {
                maxThreads = nextIntArg(arg);
                return true;
            }

            return false;
        }

        @Override
        protected void verifyArgs() {
            /* Check that one or more helper hosts are supplied */
            if (helperHosts != null &&
                (getHostname() != null || (getRegistryPort() != 0))) {
                usage("Only one of either " +  HELPER_HOSTS_FLAG + " or " +
                      HOST_FLAG + " plus " + PORT_FLAG +
                      " may be specified");
            }

            if (helperHosts == null) {
                if (getHostname() == null) {
                    missingArg(HOST_FLAG);
                }
                if (getRegistryPort() == 0) {
                    missingArg(PORT_FLAG);
                }
            } else {
                /*
                 * Helper hosts have been supplied - validate the
                 * argument.
                 */
                try {
                    validateHelperHosts(helperHosts);
                } catch (IllegalArgumentException e) {
                    usage("Illegal value for " + HELPER_HOSTS_FLAG );
                }
            }
        }

        /**
         * Validate that each helper host entry in the form
         * <string>:<number>
         */
        private void validateHelperHosts(String helperHostVal)
            throws IllegalArgumentException {

            if (helperHostVal == null) {
                throw new IllegalArgumentException
                    ("helper hosts cannot be null");
            }
            HostPort.parse(helperHostVal.split(","));
        }

        /**
         * Return a list of hostport strings. Assumes that an argument
         * to helperHosts has already been validated.
         */
        List<String> createHostPortList() {
            final String[] hosts;
            if (helperHosts != null) {
                hosts = helperHosts.split(",");
            } else {
                hosts = new String[1];
                hosts[0] = getHostname() + ":" + getRegistryPort();
            }
            final HostPort[] hps = HostPort.parse(hosts);
            final List<String> hpList = new ArrayList<>();
            for (HostPort hp : hps) {
                hpList.add(hp.toString());
            }
            return hpList;
        }
    }

    public static void main(String[] args) {
        final AggregationServiceParser asp = new AggregationServiceParser(args);
        try {
            asp.parseArgs();
        } catch (Exception e) {
            exit("Argument error: " + e.getMessage(),
                 ExitCode.EXIT_USAGE,
                 System.err, CommandParser.getJsonVersion(args));
            return;
        }

        try {
            new AggregationService(asp.getStoreName(),
                                   asp.createHostPortList(),
                                   asp.throuputHistorySec,
                                   asp.throughputPollPeriodSec,
                                   asp.tableSizePollPeriodSec,
                                   asp.peakThroughputCollectionPeriodSec,
                                   asp.peakThroughputTTLDay,
                                   asp.maxThreads).start();
        } catch (Exception e) {
            exit("Error: " + e.getMessage(),
                 ExitCode.EXIT_UNEXPECTED,
                 System.err, asp.getJsonVersion());
        }
        exit("Service exit", ExitCode.EXIT_OK, System.out,
             asp.getJsonVersion());
    }

    /*
     * The next few methods enable starting and stopping an in-process instance
     * of AggregationService. This can be used by test code to test generation
     * of throttling exceptions, for example. The in-process instance creates a
     * thread to provide context for the polling loop in start().
     *
     * The mechanism is:
     *   AggregationService as = createAggregationService(...);
     *    // do tests
     *   as.stop(); // shutdown
     */

    /**
     * Stops an in-process instance of this service. It sets the state to "stop"
     * which tells the polling loop to end and then waits for the thread to
     * exit.
     */
    public void stop() {
        stop = true;
        if (aggThread != null) {
            try {
                aggThread.join(10*1000);
            } catch (InterruptedException ie) {
                /* ignore */
            }
            aggThread = null;
        }
    }

    /**
     * Used to set the thread being used for the polling loop for an in-process
     * AS.
     */
    private void setThread(Thread aggThread) {
        this.aggThread = aggThread;
    }

    /* For backward compatibility */
    @Deprecated
    public static AggregationService createAggregationService(
        String storeName,
        String[] hostPorts,
        int throughputPollPeriodSec,
        int tableSizePollPeriodSec,
        int peakThroughputCollectionPeriodSec,
        int peakThroughputTTLDay,
        int maxThreads) throws Exception {
        return createAggregationService(storeName,
                                        hostPorts,
                                        THROUGHPUT_HISTORY_DEFAULT_SEC,
                                        throughputPollPeriodSec,
                                        tableSizePollPeriodSec,
                                        peakThroughputCollectionPeriodSec,
                                        peakThroughputTTLDay,
                                        maxThreads);
    }

    public static AggregationService createAggregationService(
        String storeName,
        String[] hostPorts,
        int throughputHistorySec,
        int throughputPollPeriodSec,
        int tableSizePollPeriodSec,
        int peakThroughputCollectionPeriodSec,
        int peakThroughputTTLDay,
        int maxThreads) throws Exception {

        final AggregationService as =
            new AggregationService(storeName,
                                   Arrays.asList(hostPorts),
                                   throughputHistorySec,
                                   throughputPollPeriodSec,
                                   tableSizePollPeriodSec,
                                   peakThroughputCollectionPeriodSec,
                                   peakThroughputTTLDay,
                                   maxThreads);

        /*
         * This thread provides context for the polling loop used by start()
         */
        final Thread aggThread = new Thread() {
                @Override
                public void run() {
                    try {
                        as.start();
                    } catch (InterruptedException ie) {
                        logger.log(Level.SEVERE,
                                   "AggregationService failed to start: {0}",
                                   ie);
                    }
                }
            };

        aggThread.start();

        /* set the thread in the instance to allow clean stop */
        as.setThread(aggThread);
        return as;
    }

    /**
     * Exit the process with the appropriate exit code, generating the
     * appropriate message.
     */
    private static void exit(String msg,
                             ExitCode exitCode,
                             PrintStream ps,
                             int jsonVersion) {
        if ((msg != null) && (ps != null)) {
            switch (jsonVersion) {
                case CommandParser.JSON_V2:
                    displayExitJson(msg, exitCode, ps);
                    break;
                case CommandParser.JSON_V1:
                    displayExitJsonV1(msg, exitCode, ps);
                    break;
                default:
                    ps.println(msg);
                    break;
            }
        }
        System.exit(exitCode.value());
    }

    private static final String EXIT_CODE_FIELD_V1 = "exit_code";

    private static final String EXIT_CODE_FIELD = "exitCode";

    private static void displayExitJsonV1(String msg,
                                          ExitCode exitCode,
                                          PrintStream ps) {
        final ObjectNode on = JsonUtils.createObjectNode();
        on.put(CommandJsonUtils.FIELD_OPERATION, "aggregationservice");
        on.put(CommandJsonUtils.FIELD_RETURN_CODE,
               exitCode.getErrorCode().getValue());
        final String description =
                        (msg == null) ? exitCode.getDescription() :
                                        exitCode.getDescription() + " - " + msg;
        on.put(CommandJsonUtils.FIELD_DESCRIPTION, description);
        on.put(EXIT_CODE_FIELD_V1, exitCode.value());

        /* print the json node. */
        ps.println(on.toPrettyString());
    }

    private static void displayExitJson(String msg,
                                        ExitCode exitCode,
                                        PrintStream ps) {
        final ShellCommandResult scr =
            ShellCommandResult.getDefault("aggregationservice");
        scr.setReturnCode(exitCode.getErrorCode().getValue());
        final String description =
            (msg == null) ? exitCode.getDescription() :
                            exitCode.getDescription() + " - " + msg;
        scr.setDescription(description);
        final ObjectNode on = JsonUtils.createObjectNode();
        on.put(EXIT_CODE_FIELD, exitCode.value());
        scr.setReturnValue(on);

        ps.println(scr.convertToJson());
    }
}
