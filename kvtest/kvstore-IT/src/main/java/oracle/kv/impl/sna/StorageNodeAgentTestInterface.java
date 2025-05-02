/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.test.RemoteTestBase;
import oracle.kv.impl.test.RemoteTestInterfaceResponder;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TwoArgTestHook;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;

/**
 * An implementation of RemoteTestInterface for StorageNodeAgents, providing a
 * way to kill running storage nodes.
 */
public class StorageNodeAgentTestInterface extends RemoteTestBase {

    private final StorageNodeAgent sna;

    public StorageNodeAgentTestInterface(
        StorageNodeAgentImpl storageNodeAgent)
    {
        this.sna = storageNodeAgent.getStorageNodeAgent();
    }

    @Override
    public void start(short serialVersion)
        throws RemoteException {

        /**
         * The SNA only starts the test interface when registered which means
         * that it has a valid store and StorageNodeId.
         */
        final String serviceName = RegistryUtils.bindingName(
            sna.getStoreName(), sna.getStorageNodeId().getFullName(),
            InterfaceType.TEST);
        sna.rebind(this,
                   serviceName,
                   InterfaceType.TEST,
                   CLIENT_SOCKET_FACTORY,
                   SERVER_SOCKET_FACTORY,
                   REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                   RemoteTestInterfaceResponder::new);
    }

    @Override
    public void stop(short serialVersion)
        throws RemoteException {

        final String serviceName = RegistryUtils.bindingName(
            sna.getStoreName(), sna.getStorageNodeId().getFullName(),
            InterfaceType.TEST);
        sna.unbind(this, serviceName, InterfaceType.TEST);
    }

    @Override
    protected Logger getLogger() {
        return sna.getLogger();
    }

    @Override
    protected ServiceStatusTracker getServiceStatusTracker() {
        return sna.getServiceStatusTracker();
    }

    @Override
    public boolean processIllegal(short serialVersion) {
        doProcessIllegal(sna.getFaultHandler());
        return true;
    }

    @Override
    public void addServiceExecArgs(final String serviceName,
                                   final String[] args,
                                   short serialVersion)
        throws RemoteException {

        ProcessServiceManager.setCreateExecArgsHook(
            new TwoArgTestHook<List<String>, ProcessServiceManager>() {
                @Override
                public void doHook(List<String> command,
                                   ProcessServiceManager manager) {
                    if (manager.getService().getServiceName().equals(
                            serviceName)) {
                        Collections.addAll(command, args);
                        ProcessServiceManager.setCreateExecArgsHook(null);
                    }
                }
            });
    }

    @Override
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
            throws RemoteException {
        /* TODO: Implement */
    }

    @Override
    public void logMessage(Level level,
                           String message,
                           boolean useJeLogger,
                           short serialVersion) {
        if (useJeLogger) {
            throw new UnsupportedOperationException(
                "Storage nodes do not have a JE environment");
        }
        sna.getLogger().log(level, message);
    }
}
