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

package oracle.kv.impl.util;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.exception.GetUserException;
import oracle.kv.impl.client.admin.ClientAdminServiceAPI;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RemoteAPI;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Locates a current Topology given a list of SN registry locations
 */
public class TopologyLocator {

    public static final String HOST_PORT_SEPARATOR = ":";

    /**
     * A test hook that is called by the getInitialTopology method after
     * obtaining the currentAdmin -- for testing.
     */
    volatile static TestHook<Void> getTopologyHook;

    /**
     * Obtains a current topology from the SNs using the async registry if it
     * is enabled by default and the default logger.
     */
    public static Topology get(String[] registryHostPorts,
                               int maxRNs,
                               LoginManager loginMgr,
                               String expectedStoreName,
                               ClientId clientId)
        throws KVStoreException {

        return get(registryHostPorts, maxRNs, loginMgr, expectedStoreName,
                   clientId, Protocols.getDefault(),
                   getLogger(clientId));
    }

    /**
     * Returns a logger based on the client ID if it is not null, for use with
     * clients, otherwise a logger just based on the class name for use with
     * utilities.
    */
    private static Logger getLogger(ClientId clientId) {
        return (clientId != null) ?
            ClientLoggerUtils.getLogger(TopologyLocator.class,
                                        clientId.toString()) :
            Logger.getLogger(TopologyLocator.class.getName());
    }

    /**
     * Obtains a current topology from the SNs identified via
     * <code>registryHostPorts</code> and using async or RMI registries.
     * <p>
     * The location of the Topology is done as a two step process.
     * <ol>
     * <li>It first identifies an initial Topology that is the most up to date
     * one know to the RNs hosted by the supplied SNs.</li>
     * <li>
     * If <code>maxRNs</code> &gt; 0 it then uses the initial Topology to
     * further search some bounded number of RNs, for an even more up to date
     * Topology and returns it.</li>
     * </ol>
     *
     * @param registryHostPorts one or more strings containing the registry
     * host and port associated with the SN. Each string has the format:
     * hostname:port.
     *
     * @param maxRNs the maximum number of RNs to examine for an up to date
     * Topology. If maxRNs == 0 then the initial topology is returned.
     *
     * @param loginMgr a login manager that will be used to supply login tokens
     * to api calls.
     *
     * @param expectedStoreName the expected name of the store or null if not
     * known
     *
     * @param clientId the client ID of the caller or null if called in a
     * server context
     *
     * @param protocols what network protocols to use to locate topology
     * information
     *
     * @param logger the debug logger
     */
    public static Topology get(String[] registryHostPorts,
                               int maxRNs,
                               LoginManager loginMgr,
                               String expectedStoreName,
                               ClientId clientId,
                               Protocols protocols,
                               Logger logger)
        throws KVStoreException {

        /* The highest topo seq # found */
        final AtomicInteger initialTopoSeqNum = new AtomicInteger(0);

        final Topology initialTopology = getInitialTopology(registryHostPorts,
                                                            initialTopoSeqNum,
                                                            loginMgr,
                                                            expectedStoreName,
                                                            clientId,
                                                            protocols,
                                                            logger);

        return getTopology(initialTopology, initialTopoSeqNum, maxRNs,
                           loginMgr, clientId, protocols, logger);
    }

    /**
     * Uses the initial Topology to further search some bounded number of RNs,
     * to return an even more up to date Topology.
     */
    public static Topology getTopology(Topology initialTopology,
                                       AtomicInteger maxTopoSeqNum,
                                       int maxRNs,
                                       LoginManager loginMgr,
                                       ClientId clientId,
                                       Protocols protocols,
                                       Logger logger) {

        assert maxTopoSeqNum.get() > 0;

        if (maxRNs <= 0) {
            return initialTopology;
        }

        /* Keep the pool large enough to minimize the odds of a stall */
        final int poolSize = Math.max(maxRNs, 10);
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 poolSize, // corePoolSize
                 poolSize, // maximumPoolSize - ignored
                 0L, TimeUnit.MILLISECONDS,
                 new  LinkedBlockingQueue<Runnable>(),
                 new KVThreadFactory(" topology locator", null))) {

            /* The number of RNs to check */
            final AtomicInteger nRNs = new AtomicInteger(maxRNs);

            /*
             * Now use the initial Topology to find an even more current
             * version if it exists.
             */
            final AtomicReference<ClientRepNodeAdminAPI> currentAdmin =
                new AtomicReference<>();

            final RegistryUtils registryUtils =
                new RegistryUtils(initialTopology, loginMgr, clientId, logger);

            for (RepGroup rg : initialTopology.getRepGroupMap().getAll()) {
                for (final RepNode rn : rg.getRepNodes()) {

                    executor.submit(new Runnable() {

                        @Override
                        public void run() {

                            if (nRNs.get() <= 0) {
                                return;
                            }
                            try {
                                ClientRepNodeAdminAPI admin =
                                    registryUtils.getClientRepNodeAdmin(
                                        rn.getResourceId(), protocols);
                                int seqNum = admin.getTopoSeqNum();
                                synchronized (maxTopoSeqNum) {
                                    if (seqNum > maxTopoSeqNum.get()) {
                                        maxTopoSeqNum.set(seqNum);
                                        currentAdmin.set(admin);
                                    }
                                }
                                nRNs.decrementAndGet();
                            } catch (RemoteException re) {
                            } catch (NotBoundException nbe) { }
                        }});
                }
            }
            executor.shutdown();

            /*
             * Wait until the required number of RNs have been checked or all
             * tasks have run.
             */
            while (nRNs.get() > 0) {
                try {
                    if (executor.awaitTermination(100L,
                                                  TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException ie) { }
            }
            executor.shutdownNow();

            /* If a newer topo was found attempt to get it and return */
            if (currentAdmin.get() != null) {
                try {
                    return currentAdmin.get().getTopology();
                } catch (RemoteException re) { }
            }

            /*
             * If there was an issue getting the latest, or none newer were
             * found return the initial topo.
             */
            return initialTopology;
        }
    }

    /**
     * Locates an initial Topology based upon the supplied SNs. This method
     * is intended for use in the KVS client and meets the client side
     * exception interfaces.
     */
    public static @NonNull
        Topology getInitialTopology(final String[] registryHostPorts,
                                    final AtomicInteger maxTopoSeqNum,
                                    final LoginManager loginMgr,
                                    final String expectedStoreName,
                                    final ClientId clientId,
                                    final Protocols protocols,
                                    final Logger asyncLogger)
        throws KVStoreException {

        /* The exception cause collector */
        final AtomicReference<Throwable> cause =
            new AtomicReference<Throwable>();

        /* The RN admin or Admin service which has the highest topo */
        final AtomicReference<RemoteAPI> currentAdmin =
            new AtomicReference<>();

        /*
         * Try getting the topology from RNs first if we have already
         * determined that RNs are available because we found an RN
         * login manager, or if we don't know in the non-secure case.
         */
        if (!(loginMgr instanceof AdminLoginManager)) {
            applyToRNs(registryHostPorts,
                       expectedStoreName,
                       clientId,
                       "topology locator",
                       loginMgr,
                       cause,
                       rnAdmin -> {
                           final int seqNum = rnAdmin.getTopoSeqNum();
                           synchronized (maxTopoSeqNum) {
                               if (seqNum > maxTopoSeqNum.get()) {
                                   maxTopoSeqNum.set(seqNum);
                                   currentAdmin.set(rnAdmin);
                               }
                           }
                       },
                       protocols,
                       asyncLogger);
        }

        /*
         * Try admins instead if the login manager type tells us that only
         * admins were available, or in the non-secure case if we tried and
         * failed with RNs.
         */
        if ((loginMgr instanceof AdminLoginManager) ||
            ((loginMgr == null) && (currentAdmin.get() == null))) {
            applyToAdmins(registryHostPorts,
                          expectedStoreName,
                          clientId,
                          "topology locator",
                          loginMgr,
                          cause,
                          adminApi -> {
                              int seqNum = 0;
                              seqNum = adminApi.getTopoSeqNum();
                              synchronized (maxTopoSeqNum) {
                                  if (seqNum > maxTopoSeqNum.get()) {
                                      maxTopoSeqNum.set(seqNum);
                                      currentAdmin.set(adminApi);
                                  }
                              }
                          },
                          protocols,
                          asyncLogger);
        }

        if (currentAdmin.get() == null) {
            /* If there was already a KVSecurityException, throw that */
            if (cause.get() instanceof KVSecurityException) {
                throw (KVSecurityException)cause.get();
            }

            /* If there was already a FaultException, throw that */
            if (cause.get() instanceof FaultException) {
                throw (FaultException)cause.get();
            }
            /* If an Error occured, throw that */
            if (cause.get() instanceof Error) {
                throw (Error)cause.get();
            }

            /*
             * If no initial topology was found, can't continue so throw an
             * exception
             */
            throw new KVStoreException
                ("Could not contact any RepNode or Admin at: " +
                Arrays.toString(registryHostPorts), cause.get());
        }

        assert TestHookExecute.doHookIfSet(getTopologyHook, null);

        try {
            Topology result = null;
            if (currentAdmin.get() instanceof ClientAdminServiceAPI) {
                result = ((ClientAdminServiceAPI) currentAdmin.get())
                         .getTopology();
            }
            if (currentAdmin.get() instanceof ClientRepNodeAdminAPI) {
                result = ((ClientRepNodeAdminAPI) currentAdmin.get())
                         .getTopology();
            }
            if (result == null) {
                throw new KVStoreException
                    ("Could not establish an initial Topology from: " +
                     Arrays.toString(registryHostPorts), cause.get());
            }
            return result;
        } catch (RemoteException e) {
            throw new KVStoreException
                ("Could not establish an initial Topology from: " +
                 Arrays.toString(registryHostPorts), cause.get());
        } catch (InternalFaultException e) {
            /* Clients expect FaultException */
            throw new FaultException(e, false);
        }
    }

    /**
     * Apply a callback function to all discovered ClientAdminServiceAPI
     * services using the async or RMI registries.
     *
     * @since 24.2
     */
    public static
        void applyToAdmins(final String[] registryHostPorts,
                           final String expectedStoreName,
                           final ClientId clientId,
                           final String threadFactoryName,
                           final LoginManager loginMgr,
                           final AtomicReference<Throwable> cause,
                           final ClientAdminApiCallback adminApiCallback,
                           final Protocols protocols,
                           final Logger asyncLogger) {
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, registryHostPorts.length, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory(" " + threadFactoryName, null))) {
            for (final String registryHostPort : registryHostPorts) {
                executor.submit(
                    () -> applyToAdminOnSN(registryHostPort, expectedStoreName,
                                           clientId, loginMgr, cause,
                                           adminApiCallback, protocols,
                                           asyncLogger));
            }
            /* Wait until all of the tasks have finished */
            executor.shutdown();
            try {
                executor.awaitTermination(
                    10L, TimeUnit.MINUTES); // Nearly forever
            } catch (InterruptedException ie) {
                /* Leave a more meaningful exception if one is present */
                cause.compareAndSet(null, ie);
            }
        }
    }

    /**
     * Apply a callback function to all discovered RNs using the async or RMI
     * registries.
     */
    public static
        void applyToRNs(final String[] registryHostPorts,
                        final String expectedStoreName,
                        final ClientId clientId,
                        final String threadFactoryName,
                        final LoginManager loginMgr,
                        final AtomicReference<Throwable> cause,
                        final ClientRNAdminCallback rnAdminCallback,
                        final Protocols protocols,
                        final Logger asyncLogger) {
        /* TODO: Maybe use the ForkJoinPool.commonPool()? */
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, registryHostPorts.length, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory(" " + threadFactoryName, null))) {
            for (final String registryHostPort : registryHostPorts) {
                executor.submit(
                    () -> applyToRNsOnSN(registryHostPort, expectedStoreName,
                                         clientId, loginMgr, cause,
                                         rnAdminCallback, protocols,
                                         asyncLogger));
            }

            /* Wait until all of the tasks have finished */
            executor.shutdown();
            try {
                executor.awaitTermination(
                    10L, TimeUnit.MINUTES); // Nearly forever
            } catch (InterruptedException ie) {
                /* Leave a more meaningful exception if one is present */
                cause.compareAndSet(null, ie);
            }
        }
    }

    /**
     * Apply callback method on SN host-ports containing admin.
     *
     * @since 24.2
     */
    private static
        void applyToAdminOnSN(final String registryHostPort,
                              final String expectedStoreName,
                              final ClientId clientId,
                              final LoginManager loginMgr,
                              final AtomicReference<Throwable> cause,
                              final ClientAdminApiCallback adminApiCallback,
                              final Protocols protocols,
                              final Logger asyncLogger) {
        final HostPort hostPort = HostPort.parse(registryHostPort);
        final String registryHostname = hostPort.hostname();
        final int registryPort = hostPort.port();
        try {
            for (final String serviceName :
                     RegistryUtils.getServiceNames(
                         expectedStoreName, registryHostname, registryPort,
                         protocols, clientId, asyncLogger)) {

                /*
                 * Skip things that don't look like admin (this is for the
                 * client).
                 */
                if (!serviceName.equals(
                         GlobalParams.CLIENT_ADMIN_SERVICE_NAME)) {
                    continue;
                }

                try {
                    final ClientAdminServiceAPI adminApi =
                        RegistryUtils.getClientAdminService(expectedStoreName,
                            registryHostname, registryPort, loginMgr, clientId,
                            protocols, asyncLogger);
                    adminApiCallback.callback(adminApi);
                    break;
                } catch (Throwable e) {
                    handleFindAdminException(e, cause);
                }
            }
        } catch (RemoteException e) {
            cause.set(e);
        } catch (Throwable e) {
            cause.compareAndSet(null, e);
        }
    }

    /** Find all RNs for a single SN. */
    private static
        void applyToRNsOnSN(final String registryHostPort,
                            final String expectedStoreName,
                            final ClientId clientId,
                            final LoginManager loginMgr,
                            final AtomicReference<Throwable> cause,
                            final ClientRNAdminCallback rnAdminCallback,
                            final Protocols protocols,
                            final Logger asyncLogger) {
        final HostPort hostPort = HostPort.parse(registryHostPort);
        final String registryHostname = hostPort.hostname();
        final int registryPort = hostPort.port();
        try {
            for (final String serviceName :
                     RegistryUtils.getServiceNames(
                         expectedStoreName, registryHostname, registryPort,
                         protocols, clientId, asyncLogger)) {

                /*
                 * Skip things that don't look like RepNodes (this is for the
                 * client).
                 */
                if (!RegistryUtils.isRepNodeAdmin(serviceName)) {
                    continue;
                }

                try {
                    final ClientRepNodeAdminAPI admin =
                        RegistryUtils.getClientRepNodeAdmin(
                            expectedStoreName, registryHostname, registryPort,
                            serviceName, loginMgr, clientId, protocols,
                            true /* ignoreWrongType */, asyncLogger);
                    rnAdminCallback.callback(admin);
                } catch (Throwable e) {
                    handleFindAdminException(e, cause);
                }
            }
        } catch (RemoteException e) {
            cause.set(e);
        } catch (Throwable e) {
            cause.compareAndSet(null, e);
        }
    }

    private static
        void handleFindAdminException(Throwable exception,
                                      AtomicReference<Throwable> cause) {
        exception = GetUserException.getUserException(exception);
        try {
            throw exception;
        } catch (RemoteException e) {
            cause.set(e);
        } catch (NotBoundException e) {
            /* Should not happen since we are iterating over a bound list. */
            cause.set(e);
        } catch (InternalFaultException e) {
            /*
             * Be robust even in the presence of internal faults. Keep trying
             * with other functioning nodes. Preserve non-fault exception as
             * the reason if one's already present.
             */
            cause.compareAndSet(null, e);
        } catch (Throwable e) {
            cause.compareAndSet(null, e);
        }
    }

    public interface ClientAdminApiCallback {
        void callback(ClientAdminServiceAPI adminApi) throws RemoteException;
    }

    public interface ClientRNAdminCallback {
        void callback(ClientRepNodeAdminAPI rnAdmin) throws RemoteException;
    }
}
