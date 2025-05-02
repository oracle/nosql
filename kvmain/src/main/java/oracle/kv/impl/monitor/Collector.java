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

package oracle.kv.impl.monitor;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The Collector collects monitoring information from all monitor agents.
 * There is a single Collector in the Monitor.
 */
 public class Collector {

     private ScheduledThreadPoolExecutor collectors;
     private final Map<ResourceId, AgentInfo> agents;
     private final Monitor monitor;
     private final Logger logger;
     private final AtomicBoolean isShutdown;
     private static final int MIN_THREADS = 2;

     Collector(Monitor monitor) {
         this.monitor = monitor;
         agents = new ConcurrentHashMap<ResourceId, AgentInfo>();
         this.logger = LoggerUtils.getLogger(this.getClass(),
                                             monitor.getParams());
         isShutdown = new AtomicBoolean(false);
         setupCollectors();
     }

     private void setupCollectors() {

         /*
          * Set up the collectors as a scheduled thread pool, starting with a
          * minimum number of threads.  This number can change as agents are
          * registered and unregistered.
          */
         collectors =
             new ScheduledThreadPoolExecutor
             (MIN_THREADS, new CollectorThreadFactory(logger));
     }

     /*
      * Called by the Admin for KVStore components which support monitoring.
      * Agents may or may not be ready to respond after being registered, so
      * the collector will keep on polling all registered agents, until they're
      * removed with the unregister call.
      */
     synchronized void registerAgent(String snHostname,
                                     int snRegistryPort,
                                     ResourceId agentId) {

         long pollMillis =
             monitor.getParams().getAdminParams().getPollPeriodMillis();

         logger.finest("Monitor interval for " + agentId + " =" + pollMillis);

         /* Unregister any previous registrations. */
         unregisterAgent(agentId);

         logger.info("Monitor collector: adding " + agentId + " to monitoring");
         AgentInfo agentInfo = new AgentInfo(snHostname, snRegistryPort);
         setupFuture(agentInfo, agentId, pollMillis);
         agents.put(agentId, agentInfo);

         /*
          * Adjust thread pool size.
          */
         collectors.setCorePoolSize(Math.max(agents.size(), MIN_THREADS));
     }

     private void setupFuture(AgentInfo info, ResourceId id, long pollMillis) {
         Runnable pollTask = new PollTask(info, id);
         Future<?> future = collectors.scheduleAtFixedRate
             (pollTask,
              0, // initial delay
              pollMillis,
              TimeUnit.MILLISECONDS);
         info.setFuture(future);
     }

     /*
      * Remove this agent from the Collector's set of known MonitorAgents.
      */
     synchronized void unregisterAgent(ResourceId agentId) {

         AgentInfo info = agents.remove(agentId);
         if (info == null) {
             /* Nothing to do. */
             return;
         }

         if (info.future == null) {
             return;
         }

         logger.info("Removing " + agentId + " from monitoring");
         info.future.cancel(false);

         /*
          * Adjust thread pool size.
          */
         collectors.setCorePoolSize(Math.max(agents.size(), MIN_THREADS));
     }

     synchronized void resetAgents(long pollMillis) {
         logger.info
             ("Monitor collector: resetting interval to: " + pollMillis +
              " milliseconds (" + agents.size() + " agents)");
         for (final ResourceId key : new ArrayList<>(agents.keySet())) {
             final AgentInfo info = agents.remove(key);
             if (info.future != null) {
                 info.future.cancel(false);
             }
             setupFuture(info, key, pollMillis);
             agents.put(key, info);
         }
     }

     /*
      * Gracefully shutdown the collector's thread pool.
      */
     void shutdown() {
         logger.info("Shutting down monitor collector");
         isShutdown.set(true);
         collectors.shutdown();

         /*
          * Best effort to shutdown. If the await returns false, we proceed
          * anyway.
          */
         try {
             collectors.awaitTermination(1000 /* TODO timeout */,
                                         TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
             logger.info ("Collector interrupted during shutdown: " +
                          LoggerUtils.getStackTrace(e));
         }
     }

     /**
      * Poll all agents for data without any delay.
      * @throws NotBoundException
      * @throws RemoteException
      */
     void collectNow() {
         for (ResourceId resourceId: agents.keySet()) {
             collectNow(resourceId);
         }
     }

     /**
      * Poll the specified agent for data without any delay.
      * @throws NotBoundException
      * @throws RemoteException
      */
     void collectNow(ResourceId resourceId) {
         AgentInfo agentInfo = agents.get(resourceId);
         if (agentInfo == null) {
             return;
         }
         collectors.execute(new PollTask(agentInfo, resourceId));
     }

     /**
      * For unit test support.
      */
     int getNumAgents() {
         return agents.size();
     }

     /**
      * A Runnable that makes a RMI request for new monitoring data from a
      * monitor agent. PollTask is resilient when the monitored agent can't be
      * reached, and will retry at the next interval.
      */
     private class PollTask implements Runnable {

         private MonitorAgentAPI agent;
         private final AgentInfo agentInfo;
         private final ResourceId agentId;
         private boolean doingRetry;
         private int numFaults;

         /**
          * Maximum exceptions before declaring something wrong with this task.
          * We could end a task when the max faults is reached, and have the
          * task recreated at some other time. Alternatively it's possible and
          * simpler to keep ignoring unexpected exceptions indefinitely and
          * just retry. Random exceptions are very unlikely.
          */
         private final static int MAX_FAULTS = 5;

         PollTask(AgentInfo agentInfo,
                  ResourceId agentId) {
             this.agentInfo = agentInfo;
             this.agentId = agentId;
             doingRetry = false;
             numFaults = 0;
         }

         @Override
         public void run() {

             try {
                 if (isShutdown.get()) {
                     logger.fine("Collector is shutdown");
                     return;
                 }
                 logger.fine("Monitor collector polling " + agentId);

                 if (agent == null) {
                     String storeName = monitor.getParams().getGlobalParams().
                         getKVStoreName();
                     agent = RegistryUtils.getMonitor(
                         storeName,
                         agentInfo.snHostname,
                         agentInfo.snRegistryPort,
                         agentId,
                         monitor.getLoginManager(),
                         logger);

                     logger.finer("Monitor collector looking in registry for " +
                                  agentId.getFullName());
                 }

                 List<Measurement> measurements = agent.getMeasurements();
                 if (measurements.size() > 0) {
                     monitor.publish(agentId, measurements);
                 }
                 logger.finest("Collected " + measurements.size() +
                               " measurements from " + agentId);
                 /**
                  * Reset numRetries in case the service went away temporarily
                  */
                 doingRetry = false;
                 numFaults = 0;
             } catch (RemoteException e) {
                 setupForRetry(e);
             } catch (NotBoundException e) {
                 setupForRetry(e);
             } catch (Exception e) {
                 String msg = "Collector: exception when polling agentId: " +
                     agentId + " Exception: ";
                 if (++numFaults > MAX_FAULTS) {
                     logger.severe(msg + LoggerUtils.getStackTrace(e));
                 } else {
                     logger.info(msg + e);
                 }
             }
         }

         private void setupForRetry(Exception e) {

             logger.fine("Collector encountered problem when polling " +
                         agentId + ", clear stub for retry. " + e);

             if (isShutdown.get()) {
                 logger.fine("Collector is shutdown");
                 return;
             }

             /* Clear the RMI stub so we'll get a new one on the next try. */
             agent = null;

             /*
              * If we've retried once and still can't reach this remote
              * service, declare it UNREACHABLE.  Give it one retry in case it
              * is a quick restart.
              */
             if (doingRetry) {
                 monitor.publish(new ProxiedServiceStatusChange
                                 (agentId, ServiceStatus.UNREACHABLE));
             }
             doingRetry = true;
         }
     }

     /**
      * Collector threads are named KVMonitorCollector and log uncaught
      * exceptions to the monitor logger.
      */
     private class CollectorThreadFactory extends KVThreadFactory {
         CollectorThreadFactory(Logger logger) {
             super(null, logger);
         }

         @Override
         public String getName() {
             return monitor.getParams().getAdminParams().getAdminId() +
                 "_MonitorCollector";
         }
     }

     /**
      * A struct for hanging on to both a poll task future (used for cancelling)
      * and the storage node id (used for creating new poll tasks when doing
      * immediate collection.
      */
     private class AgentInfo {
         private final String snHostname;
         private final int snRegistryPort;
         private Future<?> future;

         AgentInfo(String snHostname, int snRegistryPort) {
             this.snHostname = snHostname;
             this.snRegistryPort = snRegistryPort;
         }

         void setFuture(Future<?> f) {
              this.future = f;
         }
     }
 }
