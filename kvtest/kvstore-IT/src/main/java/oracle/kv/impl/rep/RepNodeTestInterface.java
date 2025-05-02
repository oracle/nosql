/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.test.RemoteTestBase;
import oracle.kv.impl.test.RemoteTestInterfaceResponder;
import oracle.kv.impl.test.RollbackTestHook;
import oracle.kv.impl.test.TTLTestHook;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * An implementation of RemoteTestInterface for RepNodes.  Initially it serves
 * as a way to kill running RepNodes.  Over time it's use should expand.
 */
public class RepNodeTestInterface extends RemoteTestBase {

    /**
     * The number of milliseconds to wait when attempting to obtain the RN's JE
     * environment in order to invalidate it.  This value can be short, since
     * we expect the invalidation to be performed on an already running
     * environment.
     */
    private static final long GET_RN_ENV_TIMEOUT = 2000;

    private final RepNodeService repNodeService;

    public RepNodeTestInterface(RepNodeService repNodeService) {

        super();
        this.repNodeService = repNodeService;
    }

    @Override
    public void start(short serialVersion)
        throws RemoteException {

        repNodeService.rebind(this, RegistryUtils.InterfaceType.TEST,
                              CLIENT_SOCKET_FACTORY,
                              SERVER_SOCKET_FACTORY,
                              REMOTE_TEST_INTERFACE_TYPE_FAMILY,
                              RemoteTestInterfaceResponder::new);
        TTLTestHook.doHookIfSet(repNodeService.logger);
        RollbackTestHook.installIfSet();
    }

    @Override
    public void stop(short serialVersion)
        throws RemoteException {

        repNodeService.unbind(this, RegistryUtils.InterfaceType.TEST);
    }

    @Override
    protected Logger getLogger() {
        return repNodeService.logger;
    }

    @Override
    protected ServiceStatusTracker getServiceStatusTracker() {
        return repNodeService.getServiceStatusTracker();
    }

    @Override
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
        throws RemoteException {

    }

    @Override
    public boolean processInvalidateEnvironment(boolean corrupted,
                                                short serialVersion) {
        final RepNode repNode = repNodeService.getRepNode();
        if (repNode == null) {
            return false;
        }
        final ReplicatedEnvironment repEnv =
            repNode.getEnv(GET_RN_ENV_TIMEOUT);
        if (repEnv == null) {
            return false;
        }
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(repEnv);
        doProcessInvalidateEnvironment(corrupted, envImpl);
        return true;
    }

    @Override
    public boolean processIllegal(short serialVersion) {
        doProcessIllegal(repNodeService.getFaultHandler());
        return true;
    }

    /**
     * A value of 0 clears the test hook
     */
    @Override
    public void setTTLTime(long time, short serialVersion) {
        TTLTestHook.setTTLTime(time, "remote call", repNodeService.logger);
    }

    @Override
    public void logMessage(Level level,
                           String message,
                           boolean useJeLogger,
                           short serialVersion) {
        if (!useJeLogger) {
            repNodeService.logger.log(level, message);
            return;
        }
        final RepNode repNode = repNodeService.getRepNode();
        if (repNode == null) {
            throw new IllegalStateException("Unable to get RepNode");
        }
        final EnvironmentImpl envImpl = repNode.getEnvImpl(0);
        if (envImpl == null) {
            throw new IllegalStateException("Unable to get EnvironmentImpl");
        }
        LoggerUtils.logMsg(envImpl.getLogger(), envImpl, level, message);
    }
}
