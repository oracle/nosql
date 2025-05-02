/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TimeoutWithThreadDumpRule implements TestRule {
    private final int timeoutSecs;
    private final Object state;

    /*
     * This rule can be applied to a JUnit test class, to limit the time its
     * testcases can run.  If a testcase exceeds the limit, then a dump of the
     * stack traces of all threads in the VM is written to System.err.  The
     * result of toString() on the given state object will also be written,
     * after the thread dump.
     *
     * The intention is to allow posthumous analysis of tests that get stuck,
     * in particular those that time out in automated test runs but run fine in
     * development environments!
     *
     * Here is a simple example showing use of this Rule:
     *
     * public class MyTest {
     *     String myState = 
     *         "I was sleeping for all of ten seconds couldn't you just wait?";
     * 
     *     @Rule
     *     public TimeoutWithThreadDumpRule myRule =
     *         new TimeoutWithThreadDumpRule(5, this); // timeout after 5 sec
     * 	   
     *     @Test
     *     public void testRun() {
     *         try {
     *             Thread.sleep(10000);               // sleep for 10 sec
     *         } catch (InterruptedException e) {
     *             System.out.println("Interrupted from sleep");
     *         }
     *     }
     * 
     *     @Override
     *     public String toString() {
     *         return "myState = " + myState;
     *     }
     * }
     * 
     *
     * @param timeoutSecs: How long to allow the test to run.
     * @param state: An object whose toString() will be printed on timeout.
     */
    public TimeoutWithThreadDumpRule(int timeoutSecs, Object state) {
        super();
        this.timeoutSecs = timeoutSecs;
        this.state = state;
    }

    public TimeoutWithThreadDumpRule(int timeoutSecs) {
        this(timeoutSecs, null);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new TimeoutWithThreadDumpStatement(base, timeoutSecs, state);
    }
}

