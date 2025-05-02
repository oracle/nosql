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

package oracle.kv.impl.arb.admin;

import static oracle.kv.impl.async.StandardDialogTypeFamily.ARB_NODE_ADMIN_TYPE_FAMILY;

import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.arbiter.ArbiterStats;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.mgmt.ArbNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.arb.ArbNode;
import oracle.kv.impl.arb.ArbNodeService;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.server.LoggerUtils;

@SecureAPI
public class ArbNodeAdminImpl extends VersionedRemoteImpl
    implements ArbNodeAdmin {

    /**
     *  The arbNode being administered
     */
    private final ArbNode arbNode;
    private final ArbNodeService arbNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final ArbNodeAdminFaultHandler faultHandler;
    private final Logger logger;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;

    /**
     * The exportable/bindable version of this object
     */
    private ArbNodeAdmin exportableArbNodeAdmin;

    private static final String TEST_INTERFACE_NAME =
            "oracle.kv.impl.arb.ArbNodeTestInterface";

    public ArbNodeAdminImpl(ArbNodeService arbNodeService, ArbNode arbNode) {

        this.arbNodeService = arbNodeService;
        this.arbNode = arbNode;
        logger =
            LoggerUtils.getLogger(this.getClass(), arbNodeService.getParams());

        faultHandler = new ArbNodeAdminFaultHandler(arbNodeService,
                                                    logger,
                                                    ProcessExitCode.RESTART);
    }

    private void assertRunning() {
        ServiceStatus status =
            arbNodeService.getStatusTracker().getServiceStatus();
        if (status != ServiceStatus.RUNNING) {
            throw new IllegalArbNodeServiceStateException
                ("ArbNode is not RUNNING, current status is " + status);
        }
    }

    /**
     * Create the test interface if it can be found.
     */
    private void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(arbNodeService.getClass());
            rti = (RemoteTestInterface) c.newInstance(arbNodeService);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newParameters(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                assertRunning();
                arbNodeService.newParameters();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                arbNodeService.newGlobalParameters();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                return arbNode.getAllParams();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public void shutdown(boolean force,
                         AuthContext authCtx,
                         short serialVersion)
            throws RemoteException {
        shutdown(force, null /* reason */, authCtx, serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void shutdown(final boolean force,
                         final String reason,
                         AuthContext authCtx,
                         short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                arbNodeService.stop(force, reason);
            }
        });
    }

    /**
     * Starts up the admin component, binding its stub in the registry, so that
     * it can start accepting remote admin requests.
     *
     * @throws RemoteException
     */
    public void startup()
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                final String kvsName = arbNodeService.getParams().
                        getGlobalParams().getKVStoreName();
                final ArbNodeParams anp = arbNodeService.getArbNodeParams();
                final StorageNodeParams snp =
                    arbNodeService.getParams().getStorageNodeParams();

                final String csfName = ClientSocketFactory.
                        factoryName(kvsName,
                                    ArbNodeId.getPrefix(),
                                    InterfaceType.ADMIN.interfaceName());

                RMISocketPolicy rmiPolicy = arbNodeService.getParams().
                    getSecurityParams().getRMISocketPolicy();
                SocketFactoryPair sfp =
                    anp.getAdminSFP(rmiPolicy,
                                    snp.getServicePortRange(),
                                    csfName, kvsName);

                if (sfp.getServerFactory() != null) {
                    sfp.getServerFactory().setConnectionLogger(logger);
                }
                initExportableArbNodeAdmin();
                arbNodeService.rebind(exportableArbNodeAdmin,
                                      InterfaceType.ADMIN,
                                      sfp.getClientFactory(),
                                      sfp.getServerFactory(),
                                      ARB_NODE_ADMIN_TYPE_FAMILY,
                                      ArbNodeAdminResponder::new);

                logger.info("ArbNodeAdmin registered");
                startTestInterface();
            }
        });
    }

    private void initExportableArbNodeAdmin() {
        try {
            exportableArbNodeAdmin =
                SecureProxy.create(
                    ArbNodeAdminImpl.this,
                    arbNodeService.getArbNodeSecurity().getAccessChecker(),
                    faultHandler);
            logger.info(
                "Successfully created secure proxy for the arbnode admin");
        } catch (ConfigurationException ce) {
            logger.info("Unable to create proxy: " + ce + " : " +
                        ce.getMessage());
            throw new IllegalStateException("Unable to create proxy", ce);
        }
    }

    /**
     * Unbind the admin entry from the registry.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {
        try {
            arbNodeService.unbind(exportableArbNodeAdmin,
                                  RegistryUtils.InterfaceType.ADMIN);
            logger.info("ArbNodeAdmin stopping");
        } catch (RemoteException e) {
            /* Ignore */
        }
        if (rti != null) {
            try {
                rti.stop(SerialVersion.CURRENT);
            } catch (RemoteException e) {
                /* Ignore */
            }
        }
    }

    @Override
    @PublicMethod
    public ArbNodeStatus ping(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<ArbNodeStatus>() {

            @Override
            public ArbNodeStatus execute() {
                ServiceStatus status =
                    arbNodeService.getStatusTracker().getServiceStatus();
                State state = State.DETACHED;
                long currentVLSN = 0;

                try {
                    ArbiterStats sg = arbNode.getStats(false);
                    if (sg != null) {
                        currentVLSN = sg.getVLSN();
                        state = State.valueOf(sg.getState());
                    }
                } catch (EnvironmentFailureException ignored) {

                    /*
                     * The environment could be invalid.
                     */
                }
                String haHostPort =
                            arbNode.getArbNodeParams().getJENodeHostPort();

                return new ArbNodeStatus(status,
                                         currentVLSN, state, haHostPort,
                                         arbNode.getServiceStartTime());
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public ArbNodeInfo getInfo(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<ArbNodeInfo>() {

            @Override
            public ArbNodeInfo execute() {
                return new ArbNodeInfo(arbNode);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean updateMemberHAAddress(final String groupName,
                                         final String targetNodeName,
                                         final String targetHelperHosts,
                                         final String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
                                         throws RemoteException {
        return faultHandler.execute
                (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {

                @Override
                public Boolean execute() {
                    assertRunning();
                    try {
                        arbNodeService.updateMemberHAAddress(groupName,
                                                             targetNodeName,
                                                             targetHelperHosts,
                                                             newNodeHostPort);
                        return true;
                    } catch (UnknownMasterException e) {
                        return false;
                    }
                }
            });
    }

    /**
     * This method is deprecated as of 21.2. It shouldn't be called because
     * calls would have come from the SN on the same host which should have
     * been upgraded at the same time and should no longer call this method.
     * But leave the method as a stub for now just in case.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public void installStatusReceiver(final ArbNodeStatusReceiver receiver,
            AuthContext authCtx, short serialVersion) throws RemoteException {
    }
}
