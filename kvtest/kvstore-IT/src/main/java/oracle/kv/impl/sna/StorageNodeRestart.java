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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the mechanism used by the SNA to prevent excessive restarts of managed
 * processes.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeRestart extends StorageNodeTestBase {

    public StorageNodeRestart(boolean useThread) {
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
     * monitor threads are fully running.  Success means that this test
     * terminates.
     */
    @Test
    public void testTooManyRestarts()
        throws Exception {

        String name = getClass().getCanonicalName();
        List<String> command = createJavaExecArgs(name);
        ProcessMonitor pm =
            new ProcessMonitor(command, -1, "excessRestarts", logger);
        pm.startProcess();

        while (pm.canRestart()) {
            delay(1);
        }
        pm.stopProcess(false);
        pm.waitProcess(0);
    }

    private List<String> createJavaExecArgs(String className) {
        List<String> command = new ArrayList<String>();
        String cp = System.getProperty("java.class.path");
        command.add("java");
        command.add("-cp");
        command.add(cp);
        command.add(className);
        return command;
    }

    /**
     * A main to use for process management testing.  This process wil sleep
     * for a second then exit, resulting in restart.
     */
    public static void main(String[] args)
        throws Exception {
        delay(1);
    }
}
