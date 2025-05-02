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

package oracle.kv.impl.sna;

import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.AgentRepository.Snapshot;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.monitor.MonitorAgentResponder;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The implementation of the SNA monitor agent. The MonitorAgent is typically
 * the first component started by the RepNodeService.
 */
@SecureAPI
public class MonitorAgentImpl
    extends VersionedRemoteImpl implements MonitorAgent {

    /* The service hosting this component. */
    private final StorageNodeAgent sna;
    private final Logger logger;
    private final AgentRepository measurementBuffer;
    private final GlobalParams globalParams;
    private final StorageNodeParams snp;
    private final SecurityParams securityParams;
    private final ServiceStatusTracker statusTracker;
    private MonitorAgent exportableMonitorAgent;

    public MonitorAgentImpl(StorageNodeAgent sna,
                            GlobalParams globalParams,
                            StorageNodeParams snp,
                            SecurityParams securityParams,
                            AgentRepository agentRepository,
                            ServiceStatusTracker statusTracker) {

        this.sna = sna;
        this.globalParams = globalParams;
        this.snp = snp;
        this.securityParams = securityParams;
        logger = LoggerUtils.getLogger(this.getClass(), globalParams, snp);
        measurementBuffer = agentRepository;
        this.statusTracker = statusTracker;
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion) {

        /* Empty out the measurement repository. */
        Snapshot snapshot = measurementBuffer.getAndReset();
        List<Measurement> monitorData = snapshot.measurements;

        /*
         * Add a current service status to each measurement pull.
         */
        if (snapshot.serviceStatusChanges == 0) {
            monitorData.add(new ServiceStatusChange
                            (statusTracker.getServiceStatus()));
        }

        /*
         * Check if any of the measurements have upgrade/versioning constraints
         * and need to be filtered or modified to support an older recipient.
         */
        versionBasedContentCheck(monitorData, serialVersion);
        logger.fine("MonitorAgent: Getting " + monitorData.size() +
                    " measurements");
        return monitorData;
    }

    /**
     * If the monitorData has information that will not be known by an older
     * recipient, replace or modify as appropriate.
     * @param monitorData
     * @param serialVersion
     */
    private void versionBasedContentCheck(List<Measurement> monitorData,
                                          short serialVersion) {
    }

    /**
     * Starts up monitoring. The Monitor agent is bound in the registry.
     */
    public void startup()
        throws RemoteException {

        final String kvStoreName = globalParams.getKVStoreName();
        final StorageNodeId snId = snp.getStorageNodeId();
        final String csfName =
                ClientSocketFactory.factoryName(kvStoreName,
                                                StorageNodeId.getPrefix(),
                                                RegistryUtils.InterfaceType.
                                                MONITOR.interfaceName());
        final RMISocketPolicy policy = securityParams.getRMISocketPolicy();
        final SocketFactoryPair sfp = snp.getMonitorSFP(policy, csfName);

        initExportableMonitorAgent();

        logger.info("Starting MonitorAgent. " +
                    " Server socket factory:" + sfp.getServerFactory() +
                    " Client socket factory:" + sfp.getClientFactory());

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        sna.rebind(exportableMonitorAgent,
                   RegistryUtils.bindingName(kvStoreName, snId.getFullName(),
                                             InterfaceType.MONITOR),
                   InterfaceType.MONITOR,
                   sfp.getClientFactory(), sfp.getServerFactory(),
                   MONITOR_AGENT_TYPE_FAMILY, MonitorAgentResponder::new);
    }

    /**
     * Unbind the monitor agent in the registry.
     *
     * <p>
     * In future, it may be worth waiting for the monitor poll period, so that
     * the last state can be communicated to the MonitorController.
     */
    public void stop()
        throws RemoteException {

        final String serviceName =
            RegistryUtils.bindingName(globalParams.getKVStoreName(),
                                      snp.getStorageNodeId().getFullName(),
                                      InterfaceType.MONITOR);
        sna.unbind(exportableMonitorAgent, serviceName, InterfaceType.MONITOR);
        logger.info("Stopping MonitorAgent");
    }

    public AgentRepository getAgentRepository() {
        return measurementBuffer;
    }

    private void initExportableMonitorAgent() {
        try {
            exportableMonitorAgent =
                SecureProxy.create(this,
                                   sna.getSNASecurity().getAccessChecker(),
                                   sna.getFaultHandler());
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }
}
