/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

import org.junit.Test;

/**
 * Test the essential functions of RemoteTestInterface.
 */
public class RemoteTestInterfaceTest extends TestBase {

    TestHook<?> hook;
    private Registry registry;
    private ListenHandle registryHandle;
    private RemoteTestInterface rti;
    private int port;
    static FreePortLocator portLocator =
        new FreePortLocator("localhost", 5050, 5070);

    public RemoteTestInterfaceTest()
        throws Exception {

        port = portLocator.next();
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        startRegistry();
        startRemoteTestInterface();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        try {
            stopRemoteTestInterface();
        } catch (RemoteException e) {
            /* Ignore */
        }
        stopRegistry();
    }

    private void startRegistry()
        throws Exception {

        if (registry == null) {
            registry = TestUtils.createRegistry(port);
        }
        if (AsyncRegistryUtils.serverUseAsync &&
            (registryHandle == null)) {
            registryHandle = TestUtils.createServiceRegistry(port);
        }
    }

    private void stopRegistry()
        throws Exception {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }
    }

    private void startRemoteTestInterface()
        throws Exception {

        if (rti == null) {
            try {
                Class<?> cl =
                    Class.forName("oracle.kv.impl.test.RemoteTestImpl");
                Constructor<?> c = cl.getConstructor(this.getClass());
                rti = (RemoteTestInterface) c.newInstance(this);
                rti.start(SerialVersion.CURRENT);
            } catch (Exception ignored) {
                rti = null;
            }
        }
    }

    private void stopRemoteTestInterface()
        throws Exception {

        if (rti != null) {
            rti.stop(SerialVersion.CURRENT);
        }
    }

    public int getRegistryPort() {
        return port;
    }

    static class testhook
        implements TestHook<RemoteTestInterfaceTest>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void doHook(RemoteTestInterfaceTest rtit) {
        }
    }

    /*
     * The test cases...
     */

    /**
     * Test the extension mechanism.
     */
    @Test
    public void testExtension()
        throws Exception {

        RemoteTestExtension rte =
            (RemoteTestExtension) registry.lookup(RemoteTestImpl.BINDING_NAME);
        assert(rte.testExtension());
    }

    /**
     * Add a hook and verify that it was set.
     */
    @Test
    public void testHook()
        throws Exception {

        RemoteTestInterface rt =
            (RemoteTestInterface) registry.lookup(RemoteTestImpl.BINDING_NAME);
        assert(hook == null);
        rt.setHook(new testhook(), "hook", SerialVersion.CURRENT);
        assert(hook != null);
    }

    /**
     * Test the use of the processExit() interface by exec'ing a process and
     * killing it using that interface.
     */
    @Test
    public void testExit()
        throws Exception {

        /**
         * Get a free registry port.
         */
        int regPort = new FreePortLocator("localHost", 5080, 5090).next();
        Process process = startProcess(regPort);

        /**
         * Then look up the service in a loop to give the process a chance to
         * run.  NOTE: if there is a problem, this code will loop until
         * interrupted.  It could also result in a dangling process.
         */
        RemoteTestAPI rt = null;
        while (rt == null) {
            try {
                try {
                    int exitValue = process.exitValue();
                    fail("Process exited:" + exitValue);
                } catch (IllegalThreadStateException itse) {
                    /* Expected, process is not dead. */
                }

                rt = RegistryUtils.getRemoteTest(null /* storeName */,
                                                 "localhost", regPort,
                                                 RemoteTestImpl.BINDING_NAME,
                                                 logger);
            } catch (Exception ignored) {
                /* Process not ready as yet. */
            }
            Thread.sleep(1000);
        }

        /**
         * Put the exit in a try/catch because the RMI call will fail due
         * to the process exit.  Ignore the exception.
         */
        try {
            rt.processExit(true /* restart */);
        } catch (Exception ignored) {}

        int exitCode = process.waitFor();
        assertEquals(ProcessExitCode.INJECTED_FAULT_RESTART.getValue(),
                     exitCode);
    }

    private Process startProcess(int regPort)
        throws Exception {
        List<String> command = new ArrayList<String>();
        String cp = System.getProperty("java.class.path");
        command.add("java");
        JVMSystemUtils.addZingJVMArgs(command);
        command.add("-cp");
        command.add(cp);
        command.add("oracle.kv.impl.test.RemoteTestInterfaceTest");
        command.add(Integer.toString(regPort));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        return builder.start();
    }

    public static void main(String[] args)
        throws Exception {

        /**
         * Start a service instance.  It will be killed.
         */
        RemoteTestInterfaceTest rtit = new RemoteTestInterfaceTest();
        rtit.port = Integer.parseInt(args[0]);
        rtit.startRegistry();
        rtit.startRemoteTestInterface();

        /* Don't exit, wait to be killed. */
        Thread.sleep(Integer.MAX_VALUE);
    }
}
