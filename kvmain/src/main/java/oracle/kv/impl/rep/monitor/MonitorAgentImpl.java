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

package oracle.kv.impl.rep.monitor;

import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.measurement.LoggerMessage;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.AgentRepository.Snapshot;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.monitor.MonitorAgentFaultHandler;
import oracle.kv.impl.monitor.MonitorAgentResponder;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The implementation of the RN monitor agent. The MonitorAgent is typically
 * the first component started by the RepNodeService.
 */
@SecureAPI
public class MonitorAgentImpl
    extends VersionedRemoteImpl implements MonitorAgent {

    /* The service hosting this component. */
    private final RepNodeService repNodeService;
    private final Logger logger;
    private final AgentRepository measurementBuffer;
    private final MonitorAgentFaultHandler faultHandler;
    private MonitorAgent exportableMonitorAgent;

    public MonitorAgentImpl(RepNodeService repNodeService,
                            AgentRepository agentRepository) {

        this.repNodeService = repNodeService;
        logger = LoggerUtils.getLogger(this.getClass(),
                                       repNodeService.getParams());
        measurementBuffer = agentRepository;
        faultHandler = new MonitorAgentFaultHandler(
            logger, repNodeService.getServiceStatusTracker());
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion) {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<List<Measurement>>() {
                @Override
                public List<Measurement> execute() {
                    return fetchMeasurements();
                }
            });
    }

    private List<Measurement> fetchMeasurements() {

        /* Empty out the measurement repository. */
        Snapshot snapshot = measurementBuffer.getAndReset();
        List<Measurement> info = snapshot.measurements;

        /*
         * Add a current service status to each measurement pull if there's not
         * already one, so that any waiting plans can be notified.
         */
        if (snapshot.serviceStatusChanges == 0) {
            ServiceStatus status =
                repNodeService.getStatusTracker().getServiceStatus();
            info.add(new ServiceStatusChange
                     (status));
        }

        /*
         * Log it to the local service console if configured for
         * that. Explicitly check levels since this could be non-trivial to
         * call m.toString() on the whole set of data.
         */
        if (logger.isLoggable(Level.FINE)) {
            for (Measurement m : info) {
                /*
                 * Do not log for LoggerMessage. Since logging itself is
                 * monitored, log messages are sent to the agent repository,
                 * which ends up being a circular reference that swamps the
                 * repository.
                 */
                if (!(m instanceof LoggerMessage)) {
                    logger.fine(m.toString());
                }
            }
        }

        return info;
    }

    /**
     * Starts up monitoring. The Monitor agent is bound in the registry.
     */
    public void startup()
        throws RemoteException {

        final RepNodeParams rnp = repNodeService.getRepNodeParams();
        final String kvsName =
                repNodeService.getParams().getGlobalParams().getKVStoreName();

        final String csfName = ClientSocketFactory.
                factoryName(kvsName,
                            RepNodeId.getPrefix(),
                            InterfaceType.MONITOR.interfaceName());

        final StorageNodeParams snp =
            repNodeService.getParams().getStorageNodeParams();

        RMISocketPolicy rmiPolicy = repNodeService.getParams().
            getSecurityParams().getRMISocketPolicy();
        SocketFactoryPair sfp =
            rnp.getMonitorSFP(rmiPolicy, snp.getServicePortRange(), csfName);

        logger.info("Starting RN MonitorAgent. " +
                    " Server socket factory:" + sfp.getServerFactory() +
                    " Client socket connect factory: " +
                    sfp.getClientFactory());

        initExportableMonitorAgent();

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        repNodeService.rebind(exportableMonitorAgent, InterfaceType.MONITOR,
                              sfp.getClientFactory(),
                              sfp.getServerFactory(),
                              MONITOR_AGENT_TYPE_FAMILY,
                              MonitorAgentResponder::new);
        logger.info("Starting MonitorAgent");
    }

    private void initExportableMonitorAgent() {
        try {
            exportableMonitorAgent =
                SecureProxy.create(this,
                                   repNodeService.getRepNodeSecurity().
                                   getAccessChecker(),
                                   faultHandler);
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    /**
     * Unbind the monitor agent in the registry.
     *
     * <p>
     * In future, it may be worth waiting for the monitor poll period, so that
     * the last state can be communicated to the MonitorController.
     */
    public void stop() {

        int numItemsRemaining = measurementBuffer.size();
        logger.info("MonitorAgent stopping, " + numItemsRemaining +
                    " items remain.");

        try {
            repNodeService.unbind(exportableMonitorAgent,
                                  RegistryUtils.InterfaceType.MONITOR);
        } catch (RemoteException e) {
            /* Ignore */
        }

        /* No data left in the monitor buffer, we can just return. */
        if (numItemsRemaining == 0) {
            return;
        }

        /*
         * Dump the last stats from this rep node to logging. There's always
         * a service status change, so don't count that as an indicator of
         * existing data.
         */
        List<Measurement> remaining = getMeasurements((AuthContext) null,
                                                      SerialVersion.CURRENT);
        if (remaining.size() <= 1) {
            return;
        }

        Params params = repNodeService.getParams();
        RepNodeParams repNodeParams = params.getRepNodeParams();
        Logger dumpLogger = LoggerUtils.getFileOnlyLogger
            (this.getClass(),
             repNodeParams.getRepNodeId(),
             params.getGlobalParams(),
             params.getStorageNodeParams(),
             params.getRepNodeParams());

        dumpLogger.info("Saving untransmitted monitor info at shutdown.");

        if (!TestStatus.isActive()) {
            for (Measurement m: remaining) {

                /*
                 * Skip logger messages, which have already been logged
                 * locally
                 */
                if (m instanceof LoggerMessage) {
                    continue;
                }

                if (!EmbeddedMode.isEmbedded()) {
                    System.err.println("[" + repNodeParams.getRepNodeId() +
                                       "] untransmitted monitor info " +
                                       "at shutdown: " +  m);
                }
                dumpLogger.info(m.toString());
            }
        }
    }
}
