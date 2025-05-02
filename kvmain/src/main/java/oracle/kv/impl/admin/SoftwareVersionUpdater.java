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

import static oracle.kv.impl.util.VersionUtil.getMinorVersion;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A thread that periodically checks the SNs software version.
 * If needed, the value is updated in the admin database as a
 * parameter for the SN. May also update the store version which
 * is a global parameter.
 */
class SoftwareVersionUpdater implements ParameterListener  {

    private static final int MAX_PLAN_WAIT_MS = 5 * 60 * 1000;
    private static final int MIN_THREADS = 1;
    private static final String THREAD_NAME = "SoftwareVersionUpdater";
    private static final String UPDATE_VERSION_PLAN_NAME =
        "UpdateVersionMetadata";
    private static final String UPDATE_GLOBAL_VERSION_PLAN_NAME =
        "UpdateGlobalVersionMetadata";

    private final Admin admin;
    private final Logger logger;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ScheduledThreadPoolExecutor executor;
    private final Map<String, AgentInfo> agents = new ConcurrentHashMap<>();
    private long pollIntervalMS;
    private final long bootTime;
    /* Set in findUpdates */
    private KVVersion storeVersion = null;

    /*
     * The set of software minor versions found. If the set has one element
     * then everyone is at the same minor version. If empty, the software
     * version check was incomplete.
     */
    private final HashSet<KVVersion> snVersions = new HashSet<>();

    private final static long INITIAL_DELAY_MS = 1000L;
    private final static long SHORT_POLL_MS = 1000 * 60;
    private final static long SHORT_POLL_MAX_DUR = 1000 * 60 * 60;
    private boolean useShortPoll;

    SoftwareVersionUpdater(Admin admin,
                           long pollIntervalMS,
                           Logger logger) {
        String threadName =
            admin.getParams().getAdminParams().getAdminId() + "_" + THREAD_NAME;
        this.admin = admin;
        this.logger = logger;
        this.pollIntervalMS = pollIntervalMS;
        bootTime = System.currentTimeMillis();
        executor = new ScheduledThreadPoolExecutor
                       (MIN_THREADS, new KVThreadFactory(threadName, logger));

        /*
         * Do initial check. If we don't have the software version
         * for all SNs, or there are SNs at different major or minor
         * software versions set up a short poll time in case we are
         * in the midst of an upgrade.
         */
        useShortPoll = true;
        final AgentInfo agentInfo = new AgentInfo(admin.toString());
        agents.put(admin.toString(), agentInfo);
        setupFuture(agentInfo, INITIAL_DELAY_MS, SHORT_POLL_MS);
    }

    void shutdown(boolean force) {
        if (isShutdown.getAndSet(true)) {
            return;
        }
        logger.fine("Shutting down " + THREAD_NAME);

        unregisterAgent(admin.toString());

        if (force) {
            executor.shutdownNow();
            return;
        }
        executor.shutdown();
        /*
         * Best effort to shutdown. If the await returns false, we proceed
         * anyway.
         */
        try {
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.info(() -> THREAD_NAME + " interrupted during shutdown: " +
                              LoggerUtils.getStackTrace(e));
        }
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        DurationParameter pi =
            (DurationParameter)newMap.getOrDefault(
                ParameterState.AP_VERSION_CHECK_INTERVAL);
        if (pi.toMillis() != pollIntervalMS) {
            pollIntervalMS = pi.toMillis();
            /* If using pollIntervalMS, reset */
            if (!useShortPoll) {
                resetAgents(pollIntervalMS);
            }
        }
    }

    private void process() {
        boolean successful = processInternal();
        if (useShortPoll) {
            /*
             * If everyone is at the same version, or we have maxed out
             * the short polling, switch to the default polling interval.
             */
            if ((snVersions.size() == 1 && successful) ||
                System.currentTimeMillis() > (bootTime + SHORT_POLL_MAX_DUR)) {
                resetAgents(pollIntervalMS);
                useShortPoll = false;
            }
        }
    }

    private boolean processInternal() {
        boolean success = true;
        final HashMap<StorageNodeId, String> updates = findUpdates();
        if (!updates.isEmpty()) {
            logger.fine(() -> THREAD_NAME + " Updating software version.");
            final int planId =
                admin.getPlanner().createUpdateSoftwareVersionPlan(
                    UPDATE_VERSION_PLAN_NAME, updates);
            try {
                admin.approvePlan(planId);
                admin.executePlan(planId, false);
                admin.awaitPlan(planId, MAX_PLAN_WAIT_MS,
                                TimeUnit.MILLISECONDS);
            } catch (IllegalCommandException ice) {
                /* Lock conflict. Will retry on next pass. */
                admin.cancelPlan(planId);
                success =  false;
            } catch (Throwable e) {
                logger.log(Level.WARNING,
                           THREAD_NAME + " Encountered exception " +
                           "running update SN version plan.",
                           e);
                admin.cancelPlan(planId);
                success = false;
            }
        }

        /*
         * Check if we need to update the kvstore version.
         */
        if ((storeVersion == null) || isShutdown.get()) {
            return success;
        }

        logger.fine(() -> THREAD_NAME + " Updating store version to " +
                          storeVersion.getNumericVersionString());
        final ParameterMap pm =
            new ParameterMap(ParameterState.GLOBAL_TYPE,
                             ParameterState.GLOBAL_TYPE);
        pm.setParameter(ParameterState.GP_STORE_VERSION,
                        storeVersion.getNumericVersionString());
        final int planId =
            admin.getPlanner().createChangeGlobalComponentsParamsPlan(
                UPDATE_GLOBAL_VERSION_PLAN_NAME, pm, storeVersion);
        try {
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId, MAX_PLAN_WAIT_MS,  TimeUnit.MILLISECONDS);

            /*
             * Since the store version has been updated start or wake up the
             * system table monitor in case it was waiting for an upgrade.
             */
            admin.startSysTableMonitor();
        } catch (IllegalCommandException ice) {
            /* Lock conflict. Will retry on next pass. */
            admin.cancelPlan(planId);
            success = false;
        } catch (Throwable e) {
            logger.log(Level.WARNING,
                       THREAD_NAME + " Encountered exception " +
                       "running update store version plan.",
                       e);
            admin.cancelPlan(planId);
            success = false;
        }
        return success;
    }

    /**
     * Returns set of SN's and corresponding versions and may
     * set the storeVersion member if the information is available, otherwise
     * the storeVersion member is set to null. The storeVersion member variable
     * is set if a new store version is determined.
     *
     * This method sets the value of the snVersions member variable.
     * SnVersions is populated with the software versions of all
     * SN's in the KVStore. It is cleared if the version
     * of one or more SN's version cannot be determined
     * from the admin metadata or directly from the SN.
     *
     * This method set the value of the storeVersion if all SNs versions
     * can be determined from the Admin metadata or directly from the SNs,
     * otherwise storeVersion is set to null.
     *
     * @returns a map of Storage Node identifier to the KVVersion that needs
     *          updating in the Admin metadata database.
     */
    private HashMap<StorageNodeId, String> findUpdates() {
        final HashMap<StorageNodeId, String> retVal = new HashMap<>();
        final Topology topo = admin.getCurrentTopology();
        
        /* If the store is not yet created, try later */
        if ((topo == null) || topo.getNumPartitions() == 0) {
            return retVal;
        }
        final Parameters params = admin.getCurrentParameters();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginMgr, logger);

        storeVersion = null;

        KVVersion minVersion = null;
        boolean gotAll = true;
        snVersions.clear();

        for (StorageNodeParams tSNp : params.getStorageNodeParams()) {
            if (isShutdown.get()) {
                retVal.clear();
                return retVal;
            }
            KVVersion snVersion = null;
            StorageNodeId snId = tSNp.getStorageNodeId();
            ParameterMap snParams = tSNp.getMap();
            KVVersion dbVersion =
                getVersion(
                snParams.get(ParameterState.SN_SOFTWARE_VERSION).asString());

            try {
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                StorageNodeStatus sns = sna.ping();
                snVersion = sns.getKVVersion();

                if (!snVersion.equals(dbVersion)) {
                    retVal.put(snId, snVersion.getNumericVersionString());
                }
            } catch (NotBoundException | RemoteException e) {
            }
            if (snVersion == null && dbVersion == null) {
                gotAll = false;
            } else {
                KVVersion tMin = getMin(snVersion, dbVersion);
                if (minVersion == null) {
                    minVersion = tMin;
                } else if (minVersion.compareTo(tMin) > 0) {
                    minVersion = tMin;
                }
            }
            /*
             * Keep track of the minor versions found. If they are different
             * snVersions will contain more than one entry.
             */
            if (snVersion != null) {
                snVersions.add(getMinorVersion(snVersion));
            }
            if (dbVersion != null) {
                snVersions.add(getMinorVersion(dbVersion));
            }
        }
        /*
         * Note: if gotAll == true minVersion will not be null, but check is
         * needed to make compiler happy.
         */
        if (gotAll && (minVersion != null)) {

            /*
             * Heard back from everyone, so check whether the global store
             * version requires updating. If so set storeVersion.
             */
            final ParameterMap gp = params.getGlobalParams().getMap();
            /* Can be null, if NullParameter object */
            final String storeVersionString =
                               gp.get(ParameterState.GP_STORE_VERSION).asString();
            final KVVersion dbStoreVersion = getVersion(storeVersionString);
            if (dbStoreVersion == null ||
                dbStoreVersion.compareTo(minVersion) < 0) {
                storeVersion = minVersion;
            } else if (dbStoreVersion.compareTo(minVersion) > 0) {
                logger.severe(THREAD_NAME + " found minimum store version " +
                              minVersion.getNumericVersionString() +
                              " which is less than the current store version "
                              + storeVersion.getNumericVersionString());
                snVersions.clear();
            }
        } else {
            snVersions.clear();
        }
        return retVal;
    }

    private void setupFuture(AgentInfo info,
                             long initialDelay,
                             long pollMillis) {
        Runnable pollTask = new PollTask(info);
        Future<?> future =
            executor.scheduleAtFixedRate(pollTask,
                                         initialDelay,
                                         pollMillis,
                                         TimeUnit.MILLISECONDS);
        info.setFuture(future);
    }

    private synchronized void unregisterAgent(String name) {
        AgentInfo info = agents.remove(name);
        if (info == null) {
            /* Nothing to do. */
            return;
        }

        if (info.future == null) {
            return;
        }

        logger.fine(() -> "Removing " + name + " from executing");
        info.future.cancel(false);
    }

    private synchronized void resetAgents(long pollMillis) {
        logger.info(() -> THREAD_NAME + ": resetting interval to: " +
                          pollMillis + " milliseconds (" + agents.size() +
                          " agents)");
        for (final String key : new ArrayList<>(agents.keySet())) {
            final AgentInfo info = agents.remove(key);
            if (info.future != null) {
                info.future.cancel(false);
            }
            setupFuture(info, pollMillis, pollMillis);
            agents.put(key, info);
        }
    }

    private KVVersion getVersion(String versionString) {
        if (versionString == null) {
            return null;
        }
        KVVersion retVal = null;
        try {
            retVal = KVVersion.parseVersion(versionString);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception parsing version", e);
        }
        return retVal;
    }

    private KVVersion getMin(KVVersion v1, KVVersion v2) {
        if (v1 == null ) {
            return v2;
        }
        if (v2 == null) {
            return v1;
        }
        if (v1.compareTo(v2) < 0) {
            return v1;
        }
        return v2;
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
                    logger.fine("SoftwareVersionUpdater is shutdown");
                    return;
                }
                logger.fine(() -> THREAD_NAME + " polling " + agentInfo);
                process();
            } catch (Exception e) {
                logger.log(Level.WARNING, THREAD_NAME + " unexpected exception: ",
                           e);
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
}
