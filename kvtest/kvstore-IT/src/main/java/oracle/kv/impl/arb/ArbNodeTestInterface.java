/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.arb;

import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.test.RemoteTestBase;
import oracle.kv.impl.test.RemoteTestInterfaceResponder;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * An implementation of RemoteTestInterface for ArbNodes.  Initially it serves
 * as a way to kill running ArbNodes.  Over time it's use should expand.
 */
public class ArbNodeTestInterface extends RemoteTestBase {

    private final ArbNodeService arbNodeService;

    public ArbNodeTestInterface(ArbNodeService arbNodeService) {

        super();
        this.arbNodeService = arbNodeService;
    }

    @Override
    public void start(short serialVersion)
        throws RemoteException {

        arbNodeService.rebind(this, RegistryUtils.InterfaceType.TEST,
                              CLIENT_SOCKET_FACTORY,
                              SERVER_SOCKET_FACTORY,
                              REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                              RemoteTestInterfaceResponder::new);
    }

    @Override
    public void stop(short serialVersion)
        throws RemoteException {

        arbNodeService.unbind(this, RegistryUtils.InterfaceType.TEST);
    }

    @Override
    protected Logger getLogger() {
        return arbNodeService.getLogger();
    }

    @Override
    protected ServiceStatusTracker getServiceStatusTracker() {
        return arbNodeService.getServiceStatusTracker();
    }

    @Override
    public boolean processIllegal(short serialVersion) {
        doProcessIllegal(arbNodeService.getFaultHandler());
        return true;
    }

    @Override
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
        throws RemoteException {

    }

    @Override
    public void logMessage(Level level,
                           String message,
                           boolean useJeLogger,
                           short serialVersion) {
        /* TODO: Need to add Arbiter.getEnv method to JE in order to do this */
        if (useJeLogger) {
            throw new UnsupportedOperationException(
                "JE logging not supported for arbiters");
        }
        arbNodeService.getLogger().log(level, message);
    }
}
