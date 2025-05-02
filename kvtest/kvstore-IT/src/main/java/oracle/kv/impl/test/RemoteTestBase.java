/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.security.Security;
import java.util.logging.Logger;

import oracle.kv.impl.fault.InjectedFault;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ClearServerSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * A base class intended to be used by all implementations of
 * RemoteTestInterface.
 */
public abstract class RemoteTestBase
    extends VersionedRemoteImpl implements RemoteTestInterface {

    /** RMI client socket timeout. */
    private static final int TIMEOUT = 5000;

    /** RMI client socket factory that provides connect and read timeouts. */
    protected static final ClientSocketFactory CLIENT_SOCKET_FACTORY =
        new ClientSocketFactory("RemoteTestBase", TIMEOUT, TIMEOUT,
                                null /* clientId */);

    /** A default RMI server socket factory. */
    protected static final ServerSocketFactory SERVER_SOCKET_FACTORY =
        new ClearServerSocketFactory(0 /* backlog */, 0 /* startPort */,
                                     0 /* endPort */,
                                     0 /* acceptMaxActiveConns */);

    public RemoteTestBase() {
        super();
    }

    @Override
    public void processExit(final boolean restart, short serialVersion)
        throws RemoteException {

        getServiceStatusTracker().update(
            restart ?
            ServiceStatus.INJECTED_FAULT_RESTARTING :
            ServiceStatus.INJECTED_FAULT_NO_RESTART,
            "RemoteTestInterface.processExit");
        System.exit(restart ?
                    ProcessExitCode.INJECTED_FAULT_RESTART.getValue() :
                    ProcessExitCode.INJECTED_FAULT_NO_RESTART.getValue());
    }

    /** Return a logger for this instance. */
    protected abstract Logger getLogger();

    /** Return the service status tracker for the associated service. */
    protected abstract ServiceStatusTracker getServiceStatusTracker();

    /**
     * The property injection mechanism is done by SN only
     */
    @Override
    public void addServiceExecArgs(String serviceName,
                                   String[] args,
                                   short serialVersion)
        throws RemoteException {
    }

    @Override
    public void processHalt(short serialVersion)
        throws RemoteException {

        getServiceStatusTracker().update(
            ServiceStatus.INJECTED_FAULT_RESTARTING,
            "RemoteTestInterface.processHalt");
        Runtime.getRuntime().halt(
            ProcessExitCode.INJECTED_FAULT_RESTART.getValue());
    }

    /**
     * Invalidating the JE environment is not supported by default.
     */
    @Override
    public boolean processInvalidateEnvironment(boolean corrupted,
                                                short serialVersion) {
        return false;
    }

    /**
     * Invalidate the specified environment as requested.
     */
    protected void doProcessInvalidateEnvironment(boolean corrupted,
                                                  EnvironmentImpl envImpl) {
        if (corrupted) {
            @SuppressWarnings("unused")
            final EnvironmentFailureException efe =
                new InjectedEnvironmentFailureException(
                    envImpl,
                    EnvironmentFailureReason.LOG_CHECKSUM,
                    "Generated data corruption exception for testing");
        } else {
            /*
             * Like EnvironmentFailureException.unexpectedState, but using
             * InjectedEnvironmentFailureException
             */
            @SuppressWarnings("unused")
            final EnvironmentFailureException efe =
                new InjectedEnvironmentFailureException(
                    envImpl,
                    EnvironmentFailureReason.UNEXPECTED_STATE,
                    "Generated exception for testing");
        }
    }

    /**
     * An EnvironmentFailureException that represents an fault injected by a
     * test.
     */
    static class InjectedEnvironmentFailureException
            extends EnvironmentFailureException
            implements InjectedFault
    {
        private static final long serialVersionUID = 1;
        InjectedEnvironmentFailureException(EnvironmentImpl envImpl,
                                            EnvironmentFailureReason reason,
                                            String message) {
            super(envImpl, reason, message);
        }
        private InjectedEnvironmentFailureException(
            String msg, EnvironmentFailureException cause)
        {
            super(msg, cause);
        }
        @Override
        public InjectedEnvironmentFailureException
            wrapSelf(String msg, EnvironmentFailureException clonedCause)
        {
            return new InjectedEnvironmentFailureException(msg, clonedCause);
        }
    }

    @Override
    public boolean processIllegal(short serialVersion) throws RemoteException {
        return false;
    }

    /**
     * Cause the fault handler to throw an IllegalStateException, and log the
     * fault injection.
     */
    protected void doProcessIllegal(ProcessFaultHandler faultHandler) {
        faultHandler.execute(
            (Runnable)
            () -> { throw new InjectedIllegalStateException(); });
    }

    /**
     * An IllegalStateException that represents a fault injected by a test.
     */
    static class InjectedIllegalStateException extends IllegalStateException
            implements InjectedFault {
        private static final long serialVersionUID = 1;
        InjectedIllegalStateException() {
            super("Injected exception");
        }
    }

    /**
     * Setting TTL time is not done by default. It is only supported by
     * RepNodes at this time.
     */
    @Override
    public void setTTLTime(long time, short serialVersion) {
    }

    /**
     * Use reflection to set the hook on the target object, searching by
     * member name for the member to modify.
     */
    protected void setHookInClass(final Object target,
                                  final TestHook<?> hook,
                                  final String memberName)
        throws IllegalAccessException {
        final Field[] fields = target.getClass().getDeclaredFields();
        for(final Field field : fields) {
            field.setAccessible(true);
            if(field.getName().equals(memberName)) {
                field.set(target, hook);
                return;
            }
        }
    }

    /**
     * Returns the value of the specified security property in the current JVM.
     */
    @Override
    public String getSecurityProperty(String name, short serialVersion) {
        return Security.getProperty(name);
    }
}
