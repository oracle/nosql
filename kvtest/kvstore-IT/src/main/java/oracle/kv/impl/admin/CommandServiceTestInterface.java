/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.test.RemoteTestBase;
import oracle.kv.impl.test.RemoteTestInterfaceResponder;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.ServiceStatusTracker;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * An implementation of RemoteTestInterface for CommandService nodes, providing
 * a way to kill running Admin nodes.
 */
public class CommandServiceTestInterface extends RemoteTestBase {

    private final AdminService aservice;
    private volatile ListenHandle asyncListenHandle;

    public CommandServiceTestInterface(AdminService aservice) {
        this.aservice = aservice;
    }

    @Override
    public void start(short serialVersion)
        throws RemoteException {

        asyncListenHandle =
            aservice.rebind(this, GlobalParams.COMMAND_SERVICE_TEST_NAME,
                            CLIENT_SOCKET_FACTORY, SERVER_SOCKET_FACTORY,
                            REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                            RemoteTestInterfaceResponder::new);
    }

    @Override
    public void stop(short serialVersion)
        throws RemoteException {

        aservice.unbind(GlobalParams.COMMAND_SERVICE_TEST_NAME, this,
                        asyncListenHandle);
    }

    @Override
    protected Logger getLogger() {
        return aservice.getLogger();
    }

    @Override
    protected ServiceStatusTracker getServiceStatusTracker() {
        return aservice.getServiceStatusTracker();
    }

    @Override
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
        throws RemoteException {
        /* TODO: Implement */
    }

    @Override
    public boolean processInvalidateEnvironment(boolean corrupted,
                                                short serialVersion) {
        final Admin admin = aservice.getAdmin();
        if (admin == null) {
            return false;
        }
        final ReplicatedEnvironment repEnv = admin.getEnv();
        if (repEnv == null) {
            return false;
        }
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(repEnv);
        doProcessInvalidateEnvironment(corrupted, envImpl);
        return true;
    }

    @Override
    public boolean processIllegal(short serialVersion) {
        doProcessIllegal(aservice.getFaultHandler());
        return true;
    }

    @Override
    public void logMessage(Level level,
                           String message,
                           boolean useJeLogger,
                           short serialVersion) {
        if (!useJeLogger) {
            aservice.getLogger().log(level, message);
            return;
        }

        final Admin admin = aservice.getAdmin();
        if (admin == null) {
            throw new IllegalStateException("Unable to get Admin");
        }
        final ReplicatedEnvironment env = admin.getEnv();
        if (env == null) {
            throw new IllegalStateException(
                "Unable to get ReplicatedEnvironment");
        }
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
        if (envImpl == null) {
            throw new IllegalStateException("Unable to get EnvironmentImpl");
        }
        LoggerUtils.logMsg(envImpl.getLogger(), envImpl, level, message);
    }
}
