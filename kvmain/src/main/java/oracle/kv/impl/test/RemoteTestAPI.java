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

package oracle.kv.impl.test;

import static oracle.kv.impl.util.SerialVersion.REMOTE_TEST_INTERFACE_IMPROVEMENTS;

import java.rmi.RemoteException;
import java.util.logging.Level;

import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * RemoteTestAPI is used conditionally in order to extend a service's
 * interface to include test-only functions.  Implementations of this interface
 * should live in test packages and be conditionally instantiated using
 * reflection, e.g.:
 * try {
 *   Class cl = Class.forName("ImplementsRemoteTestInterface");
 *   {@literal Constructor<?>} c = cl.getConstructor(getClass());
 *   RemoteTestAPI rti = (RemoteTestAPI) c.newInstance(this);
 *   rti.start();
 * } catch (Exception ignored) {}
 *
 * Most implementations should extend RemoteTestBase which is implemented in
 * the test tree.
 */
public class RemoteTestAPI extends RemoteAPI {

    private final RemoteTestInterface remote;

    private RemoteTestAPI(RemoteTestInterface remote)
        throws RemoteException {

        super(remote);
        this.remote = remote;
    }

    public static RemoteTestAPI wrap(RemoteTestInterface remote)
        throws RemoteException {

        return new RemoteTestAPI(remote);
    }

    /**
     * Start the service. This call should create a binding.
     */
    public void start()
        throws RemoteException {

        remote.start(getSerialVersion());
    }

    /**
     * Stop the service.  This call should unbind the object.
     */
    public void stop()
        throws RemoteException {

        remote.stop(getSerialVersion());
    }

    /**
     * Tell the service to exit, calling Runtime.exit() with status
     * ProcessExitCode#INJECTED_FAULT_RESTART if restart is true, and
     * ProcessExitCode#INJECTED_FAULT_NO_RESTART if restart is false. Callers
     * should expect this to always throw an exception because the service's
     * process will be unable to reply.
     *
     * @param restart whether the process should be restarted
     */
    @SuppressWarnings("deprecation")
    public void processExit(boolean restart)
        throws RemoteException {

        final short serialVersion = getSerialVersion();
        if (serialVersion < REMOTE_TEST_INTERFACE_IMPROVEMENTS) {
            final ProcessExitCode exitCode = restart ?
                ProcessExitCode.INJECTED_FAULT_RESTART :
                ProcessExitCode.INJECTED_FAULT_NO_RESTART;
            remote.processExit(exitCode.getValue(), serialVersion);
            return;
        }
        remote.processExit(restart, serialVersion);
    }

    /**
     * Tell the service to halt, calling Runtime.getRuntime().halt() with
     * status ProcessExitCode#INJECTED_FAULT_RESTART. This differs from exit in
     * that shutdown hooks are not run. Callers should expect this to always
     * throw an exception because the service's process will be unable to
     * reply.
     */
    @SuppressWarnings("deprecation")
    public void processHalt()
        throws RemoteException {

        final short serialVersion = getSerialVersion();
        if (serialVersion < REMOTE_TEST_INTERFACE_IMPROVEMENTS) {
            remote.processHalt(
                ProcessExitCode.INJECTED_FAULT_RESTART.getValue(),
                serialVersion);
            return;
        }
        remote.processHalt(serialVersion);
    }

    /**
     * Tell the service to invalidate it's JE environment, returning whether
     * invalidation was performed successfully.
     */
    public boolean processInvalidateEnvironment(boolean corrupted)
        throws RemoteException {

        return
            remote.processInvalidateEnvironment(corrupted, getSerialVersion());
    }

    /**
     * Tell the service to throw an IllegalStateException, which will cause the
     * fault handler to restart the service. If supported by the service, this
     * method will throw IllegalStateException and will cause the service to
     * restart. Otherwise, the method will return false.
     *
     * @return false if the request is not supported
     * @throws IllegalStateException if the exception was delivered to the
     * service
     */
    public boolean processIllegal() throws RemoteException {
        return remote.processIllegal(getSerialVersion());
    }

    /**
     * Ask the implementation to set a TestHook.
     *
     * @param hook The TestHook instance to use.
     *
     * @param memberName The name of the member in the target class that holds
     * the TestHook instance.
     */
    public void setHook(TestHook<?> hook, String memberName)
        throws RemoteException {

        remote.setHook(hook, memberName, getSerialVersion());
    }

    /**
     * Asks the implementation to set the TTL time to the specified value.
     * This is used by TTL tests to set a RepNode's TTL time to allow timeouts
     * to occur in short periods of real time.
     *
     * @param time the absolute time to use for the TTL time. A value of 0
     * removes the test hook, restoring normal TTL time on the RN.
     */
    public void setTTLTime(long time)
        throws RemoteException {
        remote.setTTLTime(time, getSerialVersion());
    }

    /**
     * Add arguments to the command line for the service with the specified
     * name.  The storage node will add the arguments just for when the service
     * is next started or restarted.  This command only has an effect when
     * called on an SN.
     *
     * @param serviceName the name of the service
     * @param args the arguments to add for the named service
     */
    public void addServiceExecArgs(String serviceName, String... args)
        throws RemoteException {

        remote.addServiceExecArgs(serviceName, args, getSerialVersion());
    }

    /**
     * Returns the value of the specified security property of the service.
     */
    public String getSecurityProperty(String name)
        throws RemoteException {
        return remote.getSecurityProperty(name, getSerialVersion());
    }

    /**
     * Ask the service to log a message at the specified logging level. If
     * useJeLogger is true, log to a JE logger if the service has a JE
     * environment, throwing an exception if that is not possible.
     */
    public void logMessage(Level level, String message, boolean useJeLogger)
        throws RemoteException
    {
        remote.logMessage(level, message, useJeLogger, getSerialVersion());
    }
}
