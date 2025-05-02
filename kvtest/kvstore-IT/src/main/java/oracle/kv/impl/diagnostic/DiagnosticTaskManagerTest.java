/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import oracle.kv.TestBase;
import oracle.kv.impl.util.DiagnosticShell;

/**
 * Tests DiagnosticTaskManager.
 */
public class DiagnosticTaskManagerTest extends TestBase {

    @Override
    public void setUp() throws Exception {
        suppressSystemOut();
    }

    @Override
    public void tearDown() throws Exception {
        resetSystemOut();
    }

    @Test
    public void testBasic() throws Exception {
        DiagnosticShell shell = new DiagnosticShell(System.in, System.out);
        DiagnosticTaskManager manager = new DiagnosticTaskManager(shell);

        /* Initialize two tasks */
        DiagnosticTask task1 = new DiagnosticTask() {
            @Override
            public void doWork() throws Exception {
                Thread.sleep(3000);
                this.notifyCompleteSubTask("\nTestMessage\n");
            }
        };

        DiagnosticTask task2 = new DiagnosticTask() {
            @Override
            public void doWork() throws Exception {
                Thread.sleep(3000);
            }
        };
        task2.setTotalSubTaskCount(2);

        /* Add the two tasks into manager, and execute manager */
        manager.addTask(task1);
        manager.addTask(task2);
        manager.execute();

        /* Check whether the output string is expected */
        String expectedStatus = "Total: 3    Completed: 3    Status: done";
        String actualStatus = manager.getProgressStatus();
        assertEquals(actualStatus, expectedStatus);
    }
}
