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

package oracle.kv.impl.sna.collector;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStoreException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.Ping;
import oracle.kv.util.PingCollector;

import oracle.nosql.common.json.ObjectNode;

/**
 * This agent collect each component status by Ping utility. To gain resilience
 * for ping, we have each SN be responsible for two time slots: a primary time
 * slot(the one it's responsible for) and a secondary time slot. If after a
 * ping, the pinging SN notices that the SN associated with its secondary time
 * slot is down, it takes over that time slot as well, as long as that SN is
 * down.
 * Assume we have K number SNs and We order them by SN id. And the Nth SN will
 * be responsible for Nth slot as primary and (N + K / 2) % K slot as secondary.
 * For example, if there are 5 SNs and we order by id: SN1, SN2, SN4, SN5, SN7.
 * SNId Primary Secondary
 * SN1    0        2
 * SN2    1        3
 * SN4    2        4
 * SN5    3        0
 * SN7    4        1
 *
 * If there are 6 SNs and we order by id: SN1, SN2, SN4, SN5, SN7, SN9
 * SNId Primary Secondary
 * SN1    0        3
 * SN2    1        4
 * SN4    2        5
 * SN5    3        0
 * SN7    4        1
 * SN9    5        2
 *
 * If we have K number SNs, then we will have 0 to K-1 time slots.
 * If the interval is t, each time slots are as following:
 * 0 time slot is: 0, 0 + K*t, 0 + 2*K*t, 0 + 3*K*t,.... 0 + x*K*t...
 * 1 time slot is: t, t + K*t, t + 2*K*t, t + 3*K*t,.... t + x*K*t...
 * 2 time slot is: 2*t, 2*t + K*t, 2*t + 2*K*t, 2*t + 3*K*t,.... 2*t + x*K*t...
 * 3 time slot is: 3*t, 3*t + K*t, 3*t + 2*K*t, 3*t + 3*K*t,.... 3*t + x*K*t...
 * K-1 time slot is: (K-1)*t, (K-1)*t + K*t, (K-1)*t + 2*K*t, (K-1)*t + 3*K*t,
 *                   .... (K-1)*t + x*K*t...
 *
 * According to above strategy, all PingCollectorAgents will adjust their ping
 * interval dynamically base on last ping result(new topology, paired status).
 * We expect only one ping at each giving interval, but there may be some
 * redundant or missed ping result at the begin of collector service, topology
 * change or paired status change.
 */
public class PingCollectorAgent implements CollectorAgent {

    private LoginManager loginManager;
    private StorageNodeId snId;
    private String snHostName;
    private int snPort;
    private long interval;
    /* The default recorder to record ping result */
    private CollectorRecorder fileRecorder;
    /* Note that the optional recorder may be null */
    private CollectorRecorder optionalRecorder;

    /*
     * The recorderReference gives us a way to obtain a new optional
     * recorder, which might be updated if params change.
     */
    private final AtomicReference<CollectorRecorder> recorderReference;
    /* to log PingCollectorAgent execution process information */
    private Logger snLogger;

    /* Topology information */
    private int snTotal;
    private int snPosition;
    private int pairedPosition;
    private StorageNodeId pairedSNId;
    private Topology preTop;
    private List<String> helperHostPorts;

    /* to schedule ping task */
    private ScheduledExecutorService executor;
    private Future<?> future;

    /*
     * Increase sequence if stop PingCollectorAgent. Then the executing ping
     * executor will know its sequence is invalid and stop.
     */
    private volatile int agentSequence;

    public PingCollectorAgent(GlobalParams gp,
                              StorageNodeParams snp,
                              LoginManager internalLoginManager,
                              CollectorRecorder fileRecorder,
                              AtomicReference<CollectorRecorder>
                              recorderReference,
                              Logger snaLogger) {
        this.snLogger = snaLogger;
        this.loginManager = internalLoginManager;
        this.fileRecorder = fileRecorder;
        this.recorderReference = recorderReference;
        this.optionalRecorder = recorderReference.get();
        this.agentSequence = 0;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        init(gp, snp);
    }

    private void init(GlobalParams gp, StorageNodeParams snp) {
        snHostName = snp.getHostname();
        snPort = snp.getRegistryPort();
        snId = snp.getStorageNodeId();
        interval = gp.getCollectorInterval();

        /* reset topology information */
        preTop = null;
        helperHostPorts = new ArrayList<String>();
        helperHostPorts.add(getHostPort(snHostName, snPort));
    }

    @Override
    public void start() {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }
        if (future == null) {
            snLogger.info("CollectorService: PingCollectorAgent start.");
            future = executor.submit(new PingTask(agentSequence));
        }
    }

    @Override
    public synchronized void stop() {
        ++agentSequence;
        if (future != null) {
            future.cancel(false);
            future = null;
        }
        if (executor != null) {
            executor.shutdown();
            executor = null;
            snLogger.info("CollectorService: PingCollectorAgent stop.");
        }
    }

    @Override
    public void updateParams(GlobalParams newGlobalParams,
                             StorageNodeParams newSNParams) {

        fileRecorder.updateParams(newGlobalParams, newSNParams);
        optionalRecorder = recorderReference.get();
        if (newSNParams != null) {
            snId = newSNParams.getStorageNodeId();
        }
        if (newGlobalParams != null) {
            /* Ping interval will update after next ping */
            interval = newGlobalParams.getCollectorInterval();
            if (newGlobalParams.getCollectorEnabled()) {
                start();
            } else {
                stop();
            }
        }
    }


    private String getHostPort(String hostName, int port) {
        return hostName + ":" + port;
    }

    private class PingTask implements Runnable {

        private int executorSequence;

        private PingTask(int taskSequence) {
            this.executorSequence = taskSequence;
        }

        @Override
        public void run() {
            /* interval may be changed during this task, snap it */
            long snapInterval = interval;
            /* retry after one interval if ping failed */
            long pingDelayTime = snapInterval;
            try {
                Ping ping = doPing();

                Topology topo = ping.getTopology();
                updateTopology(topo);

                pingDelayTime = calculateDelayTime(snapInterval,
                                                   snPosition,
                                                   snTotal);
                /* check paired SN status */
                PingCollector collector = ping.getPingCollector();
                ServiceStatus pairedStatus =
                    collector.getTopologyStatus().get(pairedSNId);
                if (!ServiceStatus.RUNNING.equals(pairedStatus)) {
                    final long pairedDelayTime =
                        calculateDelayTime(snapInterval,
                                           pairedPosition,
                                           snTotal);
                    pingDelayTime = Math.min(pingDelayTime,
                                             pairedDelayTime);
                }
            } catch (Exception e) {
                snLogger.log(Level.WARNING,
                             "CollectorService: PingCollectorAgent error",
                             e);
            } finally {
                reliableSubmitPingTask(pingDelayTime);
            }
        }

        /*
         * Schedule a new ping task if executor sequence is valid. If fail to
         * schedule a new ping task for any reason, loop to retry at next
         * interval so PingCollectorAgent won't quit implicitly.
         */
        private void reliableSubmitPingTask(long delay) {
            while(true) {
                if (executorSequence != agentSequence) {
                    /*
                     * We have new sequence now, don't create new task in this
                     * executor.
                     */
                    return;
                }
                /* lock to make sure task is not stopped concurrently. */
                synchronized(PingCollectorAgent.this) {
                    if (executorSequence != agentSequence) { // double check
                        return;
                    }
                    try {
                        future =
                            executor.schedule(new PingTask(executorSequence),
                                              delay, TimeUnit.MILLISECONDS);
                        snLogger.fine("CollectorService: PingCollectorAgent "
                            + "schedule next ping after " + delay + "(ms)");
                        return;
                    } catch (Exception e) {
                        delay = 0;
                        snLogger.severe("CollectorService: PingCollectorAgent "
                            + "fail to schedule new PingTask: " + e.toString());
                    }
                }
                /* executor failed to schedule new task. Retry next interval. */
                try {
                    Thread.sleep(interval);
                } catch(Exception e) {}
            }
        }

        /**
         * position SN time slot is: position*interval + x*total*interval,
         * calculate when to arrive the next first at position SN time slot.
         * @param snapInterval ping interval configured in parameters.
         * @param position calculate the delay time for which SN
         * @param total the total number of SNs
         * @return how many milliseconds to arrive the next first at position
         * SN time slot.
         */
        private long calculateDelayTime(long snapInterval,
                                        int position,
                                        int total) {
            long currentTimeMillis = System.currentTimeMillis();
            long period = total * snapInterval;
            long nextTimeMillis = currentTimeMillis / period * period +
                position * snapInterval;
            if (nextTimeMillis <= currentTimeMillis) {
                nextTimeMillis += period;
            }
            return nextTimeMillis - currentTimeMillis;
        }

        private Ping doPing() throws KVStoreException {
            /* do ping and output json ping result */
            OutputStream os = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(os);
            Ping ping = new Ping(helperHostPorts, true,
                                 CommandParser.JSON_V2, ps,
                                 loginManager, snLogger);
            ping.pingTopology(null);
            final ObjectNode on =
                CommandJsonUtils.readObjectValue(os.toString());
            final String pingData = JsonUtils.toJsonString(on, false);
            fileRecorder.record(CollectorRecorder.MetricType.PING,
                                pingData);
            if (optionalRecorder != null) {
                optionalRecorder.record(CollectorRecorder.MetricType.PING,
                                        pingData);
            }
            return ping;
        }

        /**
         * Update topology related information if topology is changed.
         * @param topo is the current topology
         * @return true if topology is changed.
         */
        private boolean updateTopology(Topology topo) {
            if (topo == null) {
                throw new OperationFaultException(
                    "ping failed to get topology");
            }

            if (isTopoChanged(topo)) { // Topology is changed
                preTop = topo;
                helperHostPorts = getHostPorts(topo);
                List<StorageNodeId> sortedSNs = topo.getSortedStorageNodeIds();
                snTotal = sortedSNs.size();
                snPosition = -1;
                for (int i = 0; i < snTotal; i++) {
                    StorageNodeId id = sortedSNs.get(i);
                    if (id.equals(snId)) {
                        snPosition = i;
                        break;
                    }
                }
                if (snPosition == -1) {
                    /*
                     * snid doesn't exist in this topo
                     * set preTop to null since it is not valid.
                    */
                    preTop = null;
                    throw new IllegalStateException();
                }
                pairedPosition = (snPosition + snTotal / 2) % snTotal;
                pairedSNId = sortedSNs.get(pairedPosition);
                snLogger.info("CollectorService: PingCollectorAgent found "
                    + "topology changed: snTotal is " + snTotal +
                    ", snPosition is " + snPosition +
                    ", pairwisePosition is " + pairedPosition);
                return true;
            }
            return false;
        }

        private boolean isTopoChanged(Topology topo) {
            if (preTop == null) {
                return true;
            }
            if (topo.getSequenceNumber() != preTop.getSequenceNumber()) {
                return true;
            }
            return false;
        }

        private List<String> getHostPorts(Topology topo) {
            List<String> hostPorts = new ArrayList<String>();
            /* ping self first. */
            hostPorts.add(getHostPort(snHostName, snPort));
            StorageNodeMap sns = topo.getStorageNodeMap();
            for(StorageNode sn : sns.getAll()) {
                if (sn.getResourceId().equals(snId)) {
                    continue;
                }
                String host = sn.getHostname();
                int port = sn.getRegistryPort();
                hostPorts.add(getHostPort(host, port));
            }
            return hostPorts;
        }
    }
}
