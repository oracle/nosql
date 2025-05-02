/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.monitor;

import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.ServiceResourceId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A pseudo MonitorAgent for unit test support.
 */
public class MockMonitorAgent
    extends VersionedRemoteImpl implements MonitorAgent {

    private final int registryPort;
    private final String storeName;
    private final ServiceResourceId resourceId;

    private final ServiceStatusTracker statusTracker;

    private final AgentRepository agentRepository;
    private final Logger logger;
    private final Registry registry;
    private final ListenHandle registryHandle;
    private final ListenHandle listenHandle;

    /**
     * @param gp globalParams for this storage node
     * @param snp storageNodeParams for this storage node
     * @param resourceId id of the monitor agent.
     * @throws IOException
     */
    public MockMonitorAgent(GlobalParams gp,
                            StorageNodeParams snp,
                            ServiceResourceId resourceId)
        throws IOException {

        this.registryPort = snp.getRegistryPort();
        this.storeName = gp.getKVStoreName();
        this.resourceId = resourceId;
        /*
         * Make the respository before the agent is made available
         * as an RMI resource, so that it's in place before the
         * monitor polls it.
         */
        agentRepository = new AgentRepository(storeName, resourceId, 50);

        Registry r = null;
        try {
            LocateRegistry.getRegistry("localhost", registryPort).list();
        } catch (RemoteException e) {
            r = TestUtils.createRegistry(registryPort);
        }
        registry = r;

        /*
         * This is a typical logger that would live on the process implementing
         * the MonitorAgent.
         */
        logger = LoggerUtils.getLogger(this.getClass(), gp, snp);

        if (!AsyncRegistryUtils.serverUseAsync) {
            registryHandle = null;
        } else {
            final ClientId clientId = null;
            ListenHandle rh = null;
            try {
                AsyncRegistryUtils.getRegistry(storeName, "localhost",
                                               registryPort, clientId,
                                               logger).get();
            } catch (Exception e) {
                rh = TestUtils.createServiceRegistry(registryPort);
            }
            registryHandle = rh;
        }
        listenHandle =
            RegistryUtils.rebind("localhost",
                                 registryPort,
                                 storeName,
                                 resourceId,
                                 RegistryUtils.InterfaceType.MONITOR,
                                 this,
                                 DEFAULT_CSF,
                                 DEFAULT_SSF,
                                 MONITOR_AGENT_TYPE_FAMILY,
                                 () -> new MonitorAgentResponder(
                                     this, DEFAULT_THREAD_POOL, logger),
                                 logger);
        statusTracker = new ServiceStatusTracker(logger, agentRepository);
        logger.fine("Binding mock monitor agent to " +
                    resourceId.getFullName());
    }

    public Logger getLogger() {
        return logger;
    }

    public void add(Measurement m) {
        agentRepository.add(m);
    }

    @Override
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion) {

        return agentRepository.getAndReset().measurements;
    }

    public ServiceStatusTracker getStatusTracker() {
        return statusTracker;
    }

    ServiceResourceId getResourceId() {
        return resourceId;
    }

    void shutdown()
        throws RemoteException {

        RegistryUtils.unbind("localhost",
                             registryPort,
                             storeName,
                             resourceId.getFullName(),
                             RegistryUtils.InterfaceType.MONITOR,
                             this,
                             listenHandle,
                             logger);
        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }
    }

    int getRegistryPort() {
        return registryPort;
    }
}
