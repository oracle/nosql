/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.rules;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.runners.model.Statement;

public class TimeoutWithThreadDumpStatement extends Statement {
	 
    private final Statement base;
    private final int timeoutSecs;
    private final Object state;
	 
    public TimeoutWithThreadDumpStatement(Statement base, int timeoutSecs, Object state) {
        this.base = base;
        this.timeoutSecs = timeoutSecs;
        this.state = state;
    }
	 
    /*
     * Run the test as a FutureTask, then use FutureTask.get to implement
     * the timeout.  Inspired by the
     * org.junit.internal.runners.statements.FailOnTimeout, in JUnit 4.12;
     * which implements the timeout more or less like this but does not provide
     * a means to get control and examinine the VM's state before interrupting
     * the test's main thread.
     */
    @Override
    public void evaluate() throws Throwable {
        CallableStatement callable = new CallableStatement();
        FutureTask<Throwable> task = new FutureTask<Throwable>(callable);
        Thread thread = new Thread(task, "Timed Statement");
        thread.setDaemon(true);
        thread.start();
        callable.awaitStarted();
        Throwable throwable = getResult(task, thread);
        if (throwable != null) {
            throw throwable;
        }
    }

    private Throwable getResult(FutureTask<Throwable> task, Thread thread) {
        try {
            if (timeoutSecs > 0) {
                return task.get(timeoutSecs, TimeUnit.SECONDS);
            }
            return task.get();
        } catch (InterruptedException e) {
            return e;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            return timeOutAction(thread);
        }
    }

    /* This method is called *before* the timeout interrupts the test's main
     * thread.  It dumps state on stderr, then interrupts the test, and finally
     * returns an exception to be thrown in the context of the invoking thread
     * (from evaluate() above).
     */
    private Throwable timeOutAction(Thread thread) {
        
        final String msg =
            "The test timed out after " + timeoutSecs + " seconds!";
        System.err.println(msg + " Thread dump:");

        /* Print all the thread's stacks. */
        ThreadMXBean tmxbean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] info =
            tmxbean.getThreadInfo(tmxbean.getAllThreadIds(), 1024);

        for (ThreadInfo t : info) {

            if (t == null) {
                continue;
            }

            System.err.println(t.getThreadName() +": " + t.getThreadState());

            for (StackTraceElement e : t.getStackTrace()) {
                System.err.println("\t" + e.toString());
            }
        }

        if (state != null) {
            System.err.println("\nThe state object says: " + state.toString());
        }
        System.err.println();
        
        thread.interrupt();
        return new RuntimeException(msg);
    }

    private class CallableStatement implements Callable<Throwable> {
        private final CountDownLatch startLatch = new CountDownLatch(1);

        @Override
        public Throwable call() throws Exception {
            try {
                startLatch.countDown();
                base.evaluate();
            } catch (Throwable e) {
                return e;
            }
            return null;
        }

        public void awaitStarted() throws InterruptedException {
            startLatch.await();
        }
    }
}

