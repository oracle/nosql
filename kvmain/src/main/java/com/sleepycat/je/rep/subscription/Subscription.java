/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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
package com.sleepycat.je.rep.subscription;

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.ChangeResultHandler;
import com.sleepycat.je.rep.stream.FeederFilterChange;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;

/**
 * Object to represent a subscription to receive and process replication
 * streams from Feeder. It defines the public subscription APIs which can
 * be called by clients.
 */
public class Subscription {

    /* configuration parameters */
    private final SubscriptionConfig configuration;
    /* logger */
    private final Logger logger;
    /* subscription dummy environment */
    private final ReplicatedEnvironment dummyRepEnv;
    /* rep impl in dummy env */
    private final RepImpl repImpl;
    /* subscription statistics */
    private final SubscriptionStat statistics;
    private final StatGroup statGroup;

    /* main subscription thread */
    private SubscriptionThread subscriptionThread;

    /**
     * Create an instance of subscription from configuration
     *
     * @param configuration configuration parameters
     * @param logger        logging handler
     *
     * @throws IllegalArgumentException  if env directory does not exist
     */
    public Subscription(SubscriptionConfig configuration, Logger logger)
        throws IllegalArgumentException {

        this.configuration = configuration;
        this.logger = logger;

        /* init environment and parameters */
        dummyRepEnv = createDummyRepEnv(configuration, logger);
        repImpl = RepInternal.getNonNullRepImpl(dummyRepEnv);
        repImpl.setSubscription(this);
        subscriptionThread = null;
        statGroup = new StatGroup(SubscriptionStatDefinition.GROUP_NAME,
        		SubscriptionStatDefinition.GROUP_DESC);
        statistics = new SubscriptionStat(statGroup);
    }

    /**
     * Start subscription main thread, subscribe from the very first VLSN
     * from the feeder. The subscriber will stay alive and consume all entries
     * until it shuts down.
     *
     * @throws InsufficientLogException if feeder is unable to stream from
     *                                  start VLSN
     * @throws GroupShutdownException   if subscription receives group shutdown
     * @throws InternalException        if internal exception
     * @throws TimeoutException         if subscription initialization timeout
    */
    public void start()
        throws IllegalArgumentException, InsufficientLogException,
        GroupShutdownException, InternalException, TimeoutException {

        start(FIRST_VLSN);
    }

    /**
     * Start subscription main thread, subscribe from a specific VLSN
     * from the feeder. The subscriber will stay alive and consume all entries
     * until it shuts down.
     *
     * @param vlsn the start VLSN of subscription. It cannot be NULL_VLSN
     *             otherwise an IllegalArgumentException will be raised.
     *
     * @throws InsufficientLogException if feeder is unable to stream from
     *                                  start VLSN
     * @throws GroupShutdownException   if subscription receives group shutdown
     * @throws InternalException        if internal exception
     * @throws TimeoutException         if subscription initialization timeout
     * @throws ReplicationSecurityException if security check fails
     */
    public void start(long vlsn)
        throws IllegalArgumentException, InsufficientLogException,
        GroupShutdownException, InternalException, TimeoutException,
        ReplicationSecurityException {

        if (vlsn == NULL_VLSN) {
            throw new IllegalArgumentException("Start VLSN cannot be null");
        }

        subscriptionThread =
            new SubscriptionThread(dummyRepEnv, vlsn,
                                   configuration, statistics);
        /* fire the subscription thread */
        subscriptionThread.start();

        if (!waitForSubscriptionInitDone(subscriptionThread)) {
            final String err = "Subscription initialization timeout(ms)=" +
                               configuration.getPollTimeoutMs() + ", shut down";
            LoggerUtils.warning(logger, repImpl, err);
            shutdown();
            throw new TimeoutException(err);
        }

        /* if not success, throw exception to caller */
        final Exception exp = subscriptionThread.getStoredException();
        switch (subscriptionThread.getStatus()) {
            case SUCCESS:
                break;

            case VLSN_NOT_AVAILABLE:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw (InsufficientLogException) exp;

            case GRP_SHUTDOWN:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw (GroupShutdownException) exp;

            case SECURITY_CHECK_ERROR:
                shutdown();
                throw (ReplicationSecurityException) exp;

            case UNKNOWN_ERROR:
            case CONNECTION_ERROR:
            default:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw new InternalException("Error in subscription thread, " +
                                            "status=" +
                                            subscriptionThread.getStatus(),
                                            exp);
        }
    }

    /**
     * Shutdown a subscription completely, keep the thread object to keep the
     * state.
     */
    public void shutdown() {
        if (subscriptionThread != null && subscriptionThread.isAlive()) {
            subscriptionThread.shutdown();
        }

        if (dummyRepEnv != null) {
            final NodeType nodeType = configuration.getNodeType();
            if (nodeType.hasTransientId() && !dummyRepEnv.isClosed()) {
                RepInternal.getNonNullRepImpl(dummyRepEnv)
                           .getNameIdPair()
                           .revertToNull();
            }
            dummyRepEnv.close();
            LoggerUtils.fine(logger, repImpl,
                             () -> "Closed env=" + dummyRepEnv.getNodeName() +
                             "(forget transient id=" +
                             nodeType.hasTransientId() + ")");
        }
    }

    /**
     * Retrieves the stored exception from subscription thread
     *
     * @return the stored exception from subscription thread
     */
    public Exception getStoredException() {
        if (subscriptionThread == null) {
            return null;
        }
        return subscriptionThread.getStoredException();
    }

    /**
     * Get subscription thread status, if thread does not exit,
     * return subscription not yet started.
     *
     * @return status of subscription
     */
    public SubscriptionStatus getSubscriptionStatus() {
        if (subscriptionThread == null) {
            return SubscriptionStatus.INIT;
        } else {
            return subscriptionThread.getStatus();
        }
    }

    /**
     * Get subscription statistics
     *
     * @return  statistics
     */
    public SubscriptionStat getStatistics() {
        return statistics;
    }

    /**
     * Request a filter change. Each call should specify a filter change with a
     * unique request ID.
     *
     * @param change  filter change
     * @param handler change result handler
     *
     * @throws IllegalArgumentException if there is a pending request with the
     * same request ID
     * @throws IllegalStateException if subscription is shut down
     * @throws IOException if fail to queue the msg
     */
    public void changeFilter(FeederFilterChange change,
                             ChangeResultHandler handler)
        throws IllegalStateException, IOException {

        if (subscriptionThread == null ||
            subscriptionThread.isShutdown() ||
            !subscriptionThread.isAlive()) {
            final String err = "Cannot apply filter change when the " +
                               "subscription is shut down";
            LoggerUtils.info(logger, repImpl, lm(err));
            throw new IllegalStateException(err);
        }
        subscriptionThread.changeFilter(change, handler);
        LoggerUtils.fine(logger, repImpl,
                         () -> lm("Filter change request (id=" +
                                  change.getReqId() + ") " + "enqueued"));
    }

    private String lm(String msg) {
        return "[Subscription-" + configuration.getSubNodeName() + "] " + msg;
    }

    /**
     * Unit test only
     */
    SubscriptionThread getSubscriptionThread() {
        return subscriptionThread;
    }

    /**
     * For unit test only
     *
     * @return dummy env
     */
    ReplicatedEnvironment getDummyRepEnv() {
        return dummyRepEnv;
    }

    /**
     * For unit test only
     */
    ConcurrentMap<String, ChangeResultHandler> getHandleMap() {
        if (subscriptionThread == null      ||
            subscriptionThread.isShutdown() ||
            !subscriptionThread.isAlive()) {
            return null;
        }
        return subscriptionThread.getHandleMap();
    }

    /**
     * For unit test only
     *
     * @param testHook test hook
     */
    void setExceptionHandlingTestHook(TestHook<SubscriptionThread> testHook) {
        if (subscriptionThread != null) {
            subscriptionThread.setExceptionHandlingTestHook(testHook);
        }
    }

    /**
     * Create a dummy replicated env used by subscription. The dummy env will
     * be used in the SubscriptionThread, SubscriptionProcessMessageThread and
     * SubscriptionOutputThread to connect to feeder.
     *
     * @param conf   subscription configuration
     * @param logger logger
     * @return a replicated environment
     * @throws IllegalArgumentException if env directory does not exist
     */
    private static ReplicatedEnvironment
    createDummyRepEnv(SubscriptionConfig conf, Logger logger)
        throws IllegalArgumentException {

        final ReplicatedEnvironment ret;
        final File envHome = new File(conf.getSubscriberHome());
        if (!envHome.exists()) {
            throw new IllegalArgumentException("Env directory=" +
                                               envHome.getAbsolutePath() +
                                               " does not exist.");
        }
        ret =
            RepInternal.createInternalEnvHandle(envHome,
                                                conf.createReplicationConfig(),
                                                conf.createEnvConfig());

        /*
         * A safety check and clear id if necessary, to prevent env with
         * existing id from failing the subscription
         */
        final NameIdPair pair = RepInternal.getNonNullRepImpl(ret)
                                           .getNameIdPair();
        if (conf.getNodeType().hasTransientId() && !pair.hasNullId()) {
            pair.revertToNull();
        }
        return ret;
    }

    /**
     * Wait for subscription thread to finish initialization
     *
     * @param t thread of subscription
     * @return true if init done successfully, false if timeout
     */
    private boolean waitForSubscriptionInitDone(final SubscriptionThread t) {
        return new PollCondition(configuration.getPollIntervalMs(),
                                 configuration.getPollTimeoutMs()) {
            @Override
            protected boolean condition() {
                return t.getStatus() != SubscriptionStatus.INIT;
            }

        }.await();
    }
    
    public StatGroup getSubStats(StatsConfig sConfig) {
    	return statGroup.cloneGroup(sConfig.getClear());
    }
    
    public StatGroup getProtocolStats(StatsConfig sConfig) {
    	StatGroup protocolStats =
                new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                              BinaryProtocolStatDefinition.GROUP_DESC);
    	if (subscriptionThread != null) {
    		protocolStats.addAll(subscriptionThread.getProtocolStats(sConfig));
    	}
    	return protocolStats;
    }
}
