/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A test implementation of RemoteTestBase used for JUnit tests.
 */
public class RemoteTestImpl extends RemoteTestBase
    implements RemoteTestExtension {

    static final String BINDING_NAME = "RTH";
    private static final Logger logger =
        Logger.getLogger(RemoteTestImpl.class.getName());

    private RemoteTestInterfaceTest testObj;
    private ListenHandle listenHandle;
    private final ServiceStatusTracker statusTracker =
        new ServiceStatusTracker(logger);

    public RemoteTestImpl(RemoteTestInterfaceTest testObj) {
        this.testObj = testObj;
    }

    @Override
    public void start(short serialVersion)
        throws RemoteException {

        listenHandle =
            RegistryUtils.rebind("localhost", testObj.getRegistryPort(),
                                 BINDING_NAME,
                                 /*
                                  * We only expect to create a single instance
                                  * for use in testing, so just pick an
                                  * arbitrary service resource ID
                                  */
                                 new StorageNodeId(1),
                                 this, DEFAULT_CSF, DEFAULT_SSF,
                                 REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                                 () -> new RemoteTestInterfaceResponder(
                                     this, DEFAULT_THREAD_POOL, logger),
                                 logger);
    }

    @Override
    public void stop(short serialVersion)
        throws RemoteException {

        try {
            RegistryUtils.unbind("localhost", testObj.getRegistryPort(),
                                 BINDING_NAME, this, listenHandle, logger);
        } catch (Exception ignored) {
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected ServiceStatusTracker getServiceStatusTracker() {
        return statusTracker;
    }

    @Override
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
        throws RemoteException {

        try {
            setHookInClass(testObj, hook, memberName);
        } catch (IllegalAccessException iae) {
            throw new RemoteException("setHook failed: ", iae);
        }
    }

    @Override
    public boolean testExtension() {
        return true;
    }

    @Override
    public void logMessage(Level level,
                           String message,
                           boolean useJeLogger,
                           short serialVersion) {
        throw new UnsupportedOperationException(
            "RemoteTestImpl does not support logging messages");
    }
}
