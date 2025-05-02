/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.rgstate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestDispatcherTestBase;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.util.TestUtils;

import org.junit.Assume;
import org.junit.Test;

public class RepNodeStateUpdateThreadTest extends RequestDispatcherTestBase {

    private RepGroupStateTable rgStateTable;
    private int rnCount;
    private RepNodeStateUpdateThread thread;

    @Override
    public void setUp() throws Exception {

        RepNodeState.RATE_INTERVAL_MS = 3000;
        super.setUp();

        rgStateTable = dispatcher.getRepGroupStateTable();
        rnCount = rgStateTable.getRepNodeStates().size();
        assertTrue(rnCount > 0);

        thread = dispatcher.getStateUpdateThread();
        thread.start();

        boolean done = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                return thread.getResolveCount() == rnCount;
            }
        }.await();

        assertTrue(done);

        for (RepNodeState rns : rgStateTable.getRepNodeStates()) {
            assertTrue (!rns.reqHandlerNeedsResolution());
        }
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (thread != null) {
            thread.shutdown();
        }
    }

    @Test
    public void testBasic() {
        final RepNodeState r1n1s = rgStateTable.getNodeState(rg1n1Id);
        /* Simulate a failure. */
        r1n1s.noteReqHandlerException(new IllegalStateException("test"));

        boolean done = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                return !r1n1s.reqHandlerNeedsResolution();
            }
        }.await();

        /*
         * Note that the handle resolution could have been accomplished by
         * either the ResolveHandler task or by the periodic state refresh.
         */
        assertTrue(done);

        /* Check that state is updated on a periodic basis. */
        done = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                return thread.getRefreshCount() > rnCount;
            }
        }.await();

        assertTrue(done);

        assertEquals(0, thread.getResolveExceptionCount());
        assertEquals(0, thread.getResolveFailCount());
        /*
         * Refreshing the handler after noteReqHandlerException should not
         * fail. However, it is possible that an refreshing attempt happens
         * concurrently with the noteReqHandlerException causing it to fail
         * once.
         */
        assertTrue(thread.getRefreshExceptionCount() <= 1);
    }

    @Test
    public void testSlowConnection() {

        /*
         * This test is only needed for RMI, where the NoOp calls can block,
         * which is what the test depends on identify the problem case. The
         * async version times out promptly at 1 second, which makes this test
         * undependable, but also means that the issue with increasing numbers
         * of blocked threads that this test checks for is not a problem.
         */
        Assume.assumeFalse("Only test for sync/RMI case",
                           TestUtils.useAsync());

        final UpdateThreadPoolExecutor pool = thread.getThreadPool();

        /*
         * Starts with a core pool size of 1, which is sufficient when all
         * RNs are functioning normally.
         */
        assertEquals(1, pool.getMaxCorePoolSize());

        final CountDownLatch nopWaitLatch = new CountDownLatch(1);

        final List<RequestHandlerImpl> rhs = config.getRHs();
        final RequestHandlerImpl rh1 = rhs.get(1);
        rh1.setTestNOPHook(new TestHook<Request>() {
            @Override
            public void doHook(Request r) {
                try {
                    nopWaitLatch.await();
                } catch (InterruptedException e) {

                }
            }
        });

        /**
         * The above stall in the NOP consumes a core thread and requires that
         * that the core max threads be increased. They should jump from 1 to
         * 4 in this low range.
         */
        boolean done = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                return (pool.getMaxCorePoolSize() == 4);
            }
        }.await();

        nopWaitLatch.countDown();

        assertTrue(done);

        /*
         * Fix the problem and the core pool size should revert back to  the
         * default core pool size.
         */
        rh1.setTestNOPHook(null);
        done = new PollCondition(1000, 60000) {
            @Override
            protected boolean condition() {
                return (pool.getCorePoolSize() == 1);
            }
        }.await();

        assertTrue(done);
    }

    /**
     * Test that expected async dialog exceptions thrown when getting the
     * request handler are not logged [KVSTORE-404]
     */
    @Test
    public void testAsyncExceptionLogging() {

        /*
         * This test is only needed for async, since dialog exceptions are
         * specific to async.
         */
        Assume.assumeTrue("Only test for async case", TestUtils.useAsync());

        /*
         * Arrange for AsyncRegistryUtils.getRequestHandler to throw expected
         * and unexpected exceptions
         */
        final DialogException expectedException =
            new PersistentDialogException(false, false, "Expected",
                                       new IOException());
        assertTrue(expectedException.isExpectedException());
        final RuntimeException unexpectedException =
            new IllegalStateException("Unexpected");
        List<RuntimeException> exceptions = new ArrayList<>();
        exceptions.add(expectedException);
        exceptions.add(unexpectedException);
        tearDowns.add(() -> AsyncRegistryUtils.getRequestHandlerHook = null);
        AsyncRegistryUtils.getRequestHandlerHook = rnid -> {
            if (!exceptions.isEmpty()) {
                throw exceptions.remove(0);
            }
            AsyncRegistryUtils.getRequestHandlerHook = null;
        };

        final RepNodeState r1n1s = rgStateTable.getNodeState(rg1n1Id);

        /* Clear the cache so that we attempt another resolution */
        r1n1s.resetReqHandlerRef();

        final AtomicReference<LogRecord> warningForExpected =
            new AtomicReference<>();
        final AtomicReference<LogRecord> warningForUnexpected =
            new AtomicReference<>();
        final StreamHandler streamHandler = new StreamHandler() {
            @Override
            public synchronized void publish(final LogRecord record) {
                if (record.getLevel() != Level.WARNING) {
                    return;
                }
                final Throwable thrown = record.getThrown();
                if (thrown == expectedException) {
                    warningForExpected.set(record);
                } else if (thrown == unexpectedException) {
                    warningForUnexpected.set(record);
                }
            }
        };
        tearDowns.add(() -> r1n1s.logger.removeHandler(streamHandler));
        r1n1s.logger.addHandler(streamHandler);

        assertTrue(
            PollCondition.await(
                100, 60000, () -> !r1n1s.reqHandlerNeedsResolution()));

        final Formatter logFormatter = new SimpleFormatter();
        if (warningForExpected.get() != null) {
            fail("Got warning for expected exception: " +
                 logFormatter.format(warningForExpected.get()));
        }
        if (warningForUnexpected.get() == null) {
            fail("Should have gotten warning for unexpected exception");
        }
    }
}
