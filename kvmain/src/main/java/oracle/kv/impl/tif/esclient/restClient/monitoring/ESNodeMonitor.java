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

package oracle.kv.impl.tif.esclient.restClient.monitoring;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esRequest.GetHttpNodesRequest;
import oracle.kv.impl.tif.esclient.esResponse.GetHttpNodesResponse;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.HttpHost;

/**
 * This is a watcher class for ES Cluster State, basically the ES nodes joining
 * or leaving.
 * 
 * It runs a ScheduledThreadPool executor getting current ES cluster state after
 * a fixed delay period.
 * 
 * It uses API from MonitoringClient to get current Http Nodes.
 * 
 * It also doubles up as the FailureListener for the ESHttpClient.
 * 
 * A Failed HttpRequest results in again checking current Http Nodes in ES
 * Cluster, and setting the same on all ESHttpClients registered with this
 * Watcher class.
 * 
 *
 */
public class ESNodeMonitor extends ESHttpClient.FailureListener {

    private static final AtomicBoolean isTaskRunning =
            new AtomicBoolean(false);

    private static final ReentrantLock setNodesLock = new ReentrantLock();
    private static final Condition newNodes = setNodesLock.newCondition();

    private final MonitorClient monitorClient;

    private final ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(1);

    private final List<ESHttpClient> registeredESHttpClients;

    private long monitorDelayMillis = 5000L;

    private final boolean secure;
    
    private volatile boolean closed = false;

    private final Logger logger;

    public ESNodeMonitor(MonitorClient monitorClient,
            List<ESHttpClient> registeredESHttpClients,
            boolean secure,
            Logger logger) {
        this.monitorClient = monitorClient;
        this.registeredESHttpClients = registeredESHttpClients;
        this.secure = secure;
        this.logger = logger;
    }

    public ESNodeMonitor(MonitorClient monitorClient,
            long monitorDelayMillis,
            List<ESHttpClient> registeredESHttpClients,
            boolean secure,
            Logger logger) {
        this.monitorClient = monitorClient;
        this.monitorDelayMillis = monitorDelayMillis;
        this.registeredESHttpClients = registeredESHttpClients;
        this.secure = secure;
        this.logger = logger;
    }

    /*
     * If Scheduled Executor is not already running, start it.
     */
    @Override
    public void start() {
        if (!executor.isShutdown() && !executor.isTerminated()
                && !executor.isTerminating()) {
            executor.scheduleWithFixedDelay
                      (new GetNodesTask
                            (monitorClient,
                             registeredESHttpClients,
                             secure, logger),
                             0L, monitorDelayMillis,
                             TimeUnit.MILLISECONDS);
        }
    }

    /*
     * Shutdown executor service.
     */
    @Override
    public void close() {
        closed = true;
        if (!executor.isShutdown()) {
            executor.shutdownNow();
            logger.log(Level.INFO,
                       "Executor shutdown for client:" +
                       monitorClient.getAvailableNodes());
        }
    }
    
    @Override
    public boolean isClosed() {
        return closed;
    }

    public void resetAvailableNodes() {
        List<HttpHost> availableNodes = null;
        GetHttpNodesResponse resp = null;
        try {
            resp = monitorClient.getHttpNodesResponse
                                (new GetHttpNodesRequest());
        } catch (IOException e) {
            logger.log(Level.WARNING,
                       "Available Nodes were not fetched from ES", e);
        }
        if (resp != null) {
            availableNodes = resp.getHttpHosts(secure ?
                                                  ESHttpClient.Scheme.HTTPS
                                                      : 
                                                  ESHttpClient.Scheme.HTTP,
                                               logger);
        }

        monitorClient.compareAndSet(availableNodes, registeredESHttpClients);
    }

    /*
     * The ESHttpClient.FailureListener implementation.
     * 
     * There was a failure on this host. May be the node was not available.
     * Remove this host. If this is the only host, get all the nodes which
     * were part of this ES Cluster and try them again.
     * 
     * Check and set ES available nodes once more on all registered
     * ESHttpClients.
     * 
     */
    @Override
    public void onFailure(HttpHost host) {
        List<HttpHost> currentNodes = monitorClient.getAvailableNodes();
        // Remove this node where request failed.
        if (currentNodes.size() <= 1) {
            /*
             * TODO: Put a one-node cluster check.
             * Use Initial Cluster state at time
             * of registration.
             */
            /* Either it is a one node cluster or all hosts
             * have failed in the list.
             * wait for the monitor to get the
             * es cluster nodes state again.
             */
            setNodesLock.lock();
            try {
                newNodes.await(monitorDelayMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING,
                           "FailureListener await got interrupted.", e);
            } finally {
                setNodesLock.unlock();
            }
            currentNodes = monitorClient.getAllESHttpNodes();
        }
        if (currentNodes.size() > 1) {
            currentNodes.remove(host);
        }

        if (!monitorClient.getESHttpClient().isClosed()) {
            monitorClient.compareAndSet(currentNodes, registeredESHttpClients);
            resetAvailableNodes();
        }
    }

    /*
     * The task which gets executed by the ESNodeMonitor.
     * Uses a different httpClient(monitorClient) to ES than the restClient,
     * for which ESNodeMonitor also serves as failureListener.
     */
    private static class GetNodesTask implements Runnable {

        private final MonitorClient monitorClient;
        private final List<ESHttpClient> registeredESClients;
        private final boolean secure;
        private final Logger logger;

        public GetNodesTask(MonitorClient monitorClient,
                List<ESHttpClient> registeredESClients,
                boolean secure,
                Logger logger) {
            this.monitorClient = monitorClient;
            this.registeredESClients = registeredESClients;
            this.secure = secure;
            this.logger = logger;
        }

        @Override
        public void run() {

            try {
                if (!isTaskRunning.compareAndSet(false, true) || 
                        monitorClient.getESHttpClient().isClosed()) {
                    return;
                }

                List<HttpHost> availableNodes = null;
                GetHttpNodesResponse resp = monitorClient
                                            .getHttpNodesResponse
                                            (new GetHttpNodesRequest());
                availableNodes = resp.getHttpHosts(secure ?
                                                     ESHttpClient.Scheme.HTTPS
                                                           : 
                                                     ESHttpClient.Scheme.HTTP,
                                                   logger);
                setNodesLock.lock();
                try {
                    newNodes.signalAll();
                } finally {
                    setNodesLock.unlock();
                }

                monitorClient.compareAndSet(availableNodes,
                                            registeredESClients);
            } catch (IOException e) {
                logger.log(Level.WARNING,
                           "Monitoring GetHttpNodes task failed.", e);
            } finally {
                isTaskRunning.compareAndSet(true, false);
            }

        }

    }

}
