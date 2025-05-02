/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import oracle.kv.TestBase;

import org.junit.Test;

public class KVThreadFactoryTest extends TestBase {

    private CountDownLatch countdown;

    @Test
    public void testUncaughtException() 
        throws InterruptedException {
        
        countdown = new CountDownLatch(1);
        ExecutorService executor = 
            Executors.newSingleThreadExecutor(new TestFactory(logger));
        executor.execute(new TestTask());
        countdown.await();
    }

    private class TestTask implements Runnable {
        @Override
        public void run() {
            throw new TestException();
        }
    }

    private class TestException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = -3399423946456779560L;
    }
    
    private class TestFactory extends KVThreadFactory {
        public TestFactory(Logger exceptionLogger) {
            super("Test", exceptionLogger);
        }

        @Override
        public Thread.UncaughtExceptionHandler
            makeUncaughtExceptionHandler() {
            return new TestExceptionHandler();
        }
    }

    private class TestExceptionHandler 
        implements Thread.UncaughtExceptionHandler {
        TestExceptionHandler() {
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e instanceof TestException) {
                countdown.countDown();
            } else {
                fail("Caught unexpected exception " + e);
            }
        }
    }
}
