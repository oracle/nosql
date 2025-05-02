/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the process management utilities of the SNA.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeProcess extends StorageNodeTestBase {

    public StorageNodeProcess(boolean useThread) {
        super(useThread);
    }

    /**
     * Override superclass genParams method to provide use_thread as false
     * only for the tests in this class.
     */
    @Parameters(name="Use_Thread={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{false}}); 
    }

    /**
     * Notes: It is required to call the super methods if override
     * setUp and tearDown methods. 
     */
    @Override
    public void setUp() 
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() 
        throws Exception {

        super.tearDown();
    }

    /**
     * Tests a race condition where the process may exit before the IO and
     * monitor threads are fully running.
     */
    @Test
    public void testStartupRace()
        throws Exception {

        String name = getClass().getCanonicalName();
        List<String> command = createJavaExecArgs(name);
        ProcessMonitor pm = new ProcessMonitor(command, 0, name, logger);
        for (int i = 0; i < 10; i++) {
            pm.startProcess();
            pm.stopProcess(false);
            pm.waitProcess(0);
        }
    }

    private List<String> createJavaExecArgs(String className) {
        List<String> command = new ArrayList<String>();

        String cp = System.getProperty("java.class.path");
        String jvmExtraArgs = System.getProperty("oracle.kv.jvm.extraargs");

        command.add("java");
        if (jvmExtraArgs != null) {
            for (String arg : TestUtils.splitExtraArgs(jvmExtraArgs)) {
                command.add(arg);
            }
            command.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }
        command.add("-cp");
        command.add(cp);
        command.add(className);
        return command;
    }

    /**
     * A main to use for process management testing.
     */
    public static void main(String[] args)
        throws Exception {
        /* do nothing */
    }
}
