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

package oracle.kv.impl.admin;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;

class ParamConsistencyChecker implements ParameterListener {

    private static final int MIN_THREADS = 1;
    private static final String THREAD_NAME = "ParamConsistencyChecker";

    private final Map<StorageNodeId, Boolean> needsProcessing;
    private final Admin admin;
    private final Logger logger;
    private final AtomicBoolean isShutdown;
    private final AtomicBoolean isProcessing;
    private final ScheduledThreadPoolExecutor executors;
    private final Map<String, AgentInfo> agents;
    private long pollIntervalMS;
    private int maxPlanWaitMS;

    /**
     * A thread that periodically checks the parameter consistency
     * between the Admin database and the Storage Node's configuration.
     * If a correctable inconsistency is found the Storage Node's configuration
     * is changed to be consistent with the Admin datgabase.
     */
    ParamConsistencyChecker(Admin admin,
                            long pollIntervalMS,
                            int maxPlanWaitMS,
                            Logger logger) {
        String threadName =
            admin.getParams().getAdminParams().getAdminId() + "_" + THREAD_NAME;
        this.admin = admin;
        this.logger = logger;
        this.pollIntervalMS = pollIntervalMS;
        this.maxPlanWaitMS = maxPlanWaitMS;
        needsProcessing = new ConcurrentHashMap<StorageNodeId, Boolean>();
        isShutdown = new AtomicBoolean(false);
        isProcessing = new AtomicBoolean(false);
        agents = new ConcurrentHashMap<String, AgentInfo>();
        executors =
            new ScheduledThreadPoolExecutor
            (MIN_THREADS, new KVThreadFactory(threadName, logger));

        AgentInfo agentInfo = new AgentInfo(admin.toString());
        setupFuture(agentInfo,  pollIntervalMS);
        agents.put(admin.toString(), agentInfo);
    }

    void shutdown() {
        logger.fine("Shutting down " + THREAD_NAME);
        isShutdown.set(true);
        unregisterAgent(admin.toString());
        executors.shutdown();

        /*
         * Best effort to shutdown. If the await returns false, we proceed
         * anyway.
         */
        try {
            executors.awaitTermination(1000,
                                       TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
    }

    void changeParameters(StorageNodeId snId) {
        if (snId != null) {
            logger.fine("ParamConsistencyChecker Set need processing " +
                        snId + " "+ format(System.currentTimeMillis()));
            needsProcessing.put(snId, Boolean.TRUE);
            return;
        }

        for (StorageNodeId id : needsProcessing.keySet()) {
            logger.fine("ParamConsistencyChecker Set need processing " +
                        id + " "+ format(System.currentTimeMillis()));
            needsProcessing.put(id, Boolean.TRUE);
        }
    }

    private void process() {
        if (isProcessing.getAndSet(true)) {
            return;
        }
        try {
            processInternal();
        } finally {
            isProcessing.set(false);
        }
    }

    private void processInternal() {
        RegistryUtils regUtils = null;
        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();
        Collection<StorageNodeParams> allSNs = params.getStorageNodeParams();
        logger.fine("ParamConsistencyChecker running");
        for (StorageNodeParams sn : allSNs) {
            SnConsistencyUtils.ParamCheckResults pcr = null;
            StorageNodeId snId = sn.getStorageNodeId();
            Boolean np = needsProcessing.get(snId);
            if (np != null && !np) {
                continue;
            }
            boolean gotError = false;
            Integer planId = null;
            try {
                if (regUtils == null) {
                    regUtils =
                        new RegistryUtils(topo, admin.getLoginManager(),
                                          logger);
                }
                pcr =
                    SnConsistencyUtils.checkParameters(regUtils, snId, params);
                if (pcr.getGlobalDiff() ||
                    !pcr.getDiffs().isEmpty() ||
                    !pcr.getMissing().isEmpty()) {
                    logger.fine("ParamConsistencyChecker adding plan for " +
                                "SN snid " + snId);
                    planId =
                        admin.getPlanner().createSNConsistencyPlan(
                            THREAD_NAME + "_plan", snId, pcr);
                    admin.approvePlan(planId);
                    logger.fine("ParamConsistencyChecker executing plan for " +
                                "SN snid "+snId);
                    admin.executePlan(planId, false);

                    Plan.State retStat =
                        admin.awaitPlan(planId,
                                        maxPlanWaitMS,
                                        TimeUnit.MILLISECONDS);
                    if (retStat != Plan.State.SUCCEEDED) {
                        gotError = true;
                    }
                    logger.fine("ParamConsistencyChecker executed plan for " +
                                snId + " return status "+ retStat);

                } else {
                    logger.fine("ParamConsistencyChecker no parameter" +
                                " differences" + snId);
                }
            } catch (RemoteException | NotBoundException e) {
                logger.fine("ParamConsistencyChecker could not access " +
                            snId + " Exception " + e);
                gotError = true;
            } catch (Exception e) {
                logger.warning("ParamConsistencyChecker exception creating " +
                               "or running plan for " +snId + ". Exception " +
                               e);
                if (planId != null) {
                    try {
                        admin.cancelPlan(planId);
                    } catch (Exception x) {

                    }
                }
                throw e;
            }

            if (gotError) {
                if (planId != null) {
                    try {
                        admin.cancelPlan(planId);
                    } catch (Exception x) {

                    }
                }
                continue;
            }
            if (pcr != null && !pcr.getHadError()) {
                logger.fine("ParamConsistencyChecker resetting " +
                            "work flag "+ snId);
                needsProcessing.put(snId, Boolean.FALSE);
            } 
        }
        logger.fine("ParamConsistencyChecker completed processing.");
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        DurationParameter pi =
            (DurationParameter)newMap.getOrDefault(
                ParameterState.AP_PARAM_CHECK_INTERVAL);
        if (pi.toMillis() != pollIntervalMS) {
            pollIntervalMS = pi.toMillis() ;
            resetAgents(pollIntervalMS);
        }
    }

    void setMaxPlanWait(int planWaitMS) {
        this.maxPlanWaitMS = planWaitMS;
    }

    private void setupFuture(AgentInfo info, long pollMillis) {
        Runnable pollTask = new PollTask(info);
        Future<?> future =
            executors.scheduleAtFixedRate(pollTask,
                                          pollMillis, // initial delay
                                          pollMillis,
                                          TimeUnit.MILLISECONDS);
        info.setFuture(future);
    }

    synchronized void unregisterAgent(String name) {
        AgentInfo info = agents.remove(name);
        if (info == null) {
            /* Nothing to do. */
            return;
        }

        if (info.future == null) {
            return;
        }

        logger.fine("Removing " + name + " from executing");
        info.future.cancel(false);
    }

    synchronized void resetAgents(long pollMillis) {
        logger.fine
            (THREAD_NAME + ": resetting interval to: " + pollMillis +
             " milliseconds (" + agents.size() + " agents)");
        for (final String key : new ArrayList<>(agents.keySet())) {
            final AgentInfo info = agents.remove(key);
            if (info.future != null) {
                info.future.cancel(false);
            }
            setupFuture(info, pollMillis);
            agents.put(key, info);
        }
    }

    private class PollTask implements Runnable {
        private final AgentInfo agentInfo;

        PollTask(AgentInfo agentInfo) {
            this.agentInfo = agentInfo;
        }

        @Override
        public void run() {

            try {
                if (isShutdown.get()) {
                    logger.fine("Collector is shutdown");
                    return;
                }
                logger.fine(THREAD_NAME + " polling " + agentInfo);
                process();
            } catch (Exception e) {

            }
        }
    }

    private class AgentInfo {
        private final String name;
        private Future<?> future;

        AgentInfo(String name) {
            this.name = name;
        }

        void setFuture(Future<?> f) {
             this.future = f;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static String format(long millis) {
        // S is the millisecond
        SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss:S");

        return simpleDateFormat.format(millis);
    }
}
