/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/** Miscellaneous RepNodeService tests using CreateStore. */
public class RepNodeServiceMiscTest extends TestBase {

    private static final int startPort = 5000;
    CreateStore createStore;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
    }

    /* Tests */

    /**
     * Test that a failure of the RN to push stats to the admin master because
     * of a SessionAccessException does not produce SEVERE or WARNING logging
     * [KVSTORE-1948]
     */
    @Test
    public void testSevereLoggingPushFailure()
        throws Exception {

        /*
         * Set a hook that causes sending RN stats to throw a
         * SessionAccessException. Check for when the hook has been called
         * twice since the incorrect SEVERE logging was happening as part of
         * the processing of the first throw, and would be complete by the time
         * the hook was called again.
         */
        final CountDownLatch hookCount = new CountDownLatch(2);
        ContextProxy.beforeInvokeNoAuthRetry = rnIdIgnore -> {
            final Thread t = Thread.currentThread();
            if (t.getName().contains(
                    "rg1-rn1" +
                    OperationsStatsTracker.COLLECTOR_THREAD_NAME_SUFFIX)) {
                hookCount.countDown();
                throw new SessionAccessException("Test");
            }
        };

        /*
         * Get the logger that the OperationsStatsTracker used to log failures
         * when delivering stats got a failure and check for SEVERE log
         * entries -- which is how SessionAccessException used to be logged --
         * or WARNING log entries -- which is how SessionAccessException is
         * logged now
         */
        final Logger rnStatsTrackerLogger =
            Logger.getLogger(OperationsStatsTracker.class.getName() +
                             ".rg1-rn1");
        final List<LogRecord> unexpectedLogging =
            Collections.synchronizedList(new ArrayList<>());
        rnStatsTrackerLogger.addHandler(new StreamHandler() {
            @Override
            public synchronized void publish(final LogRecord record) {
                if ((record.getLevel() == Level.SEVERE) ||
                    (record.getLevel() == Level.WARNING)) {
                    unexpectedLogging.add(record);
                }
            }
        });

        /*
         * Create a store using threads so we can add hooks and check logging
         */
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      1, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null /* mgmtImpl */);

        /* Reduce the interval for reporting stats to make the test faster */
        final ParameterMap parameterMap = new ParameterMap();
        parameterMap.setParameter(ParameterState.GP_COLLECTOR_INTERVAL, "5 s");
        createStore.setPolicyMap(parameterMap);

        createStore.start();

        /* Wait for the second call to push RN ops stats */
        hookCount.await(30, SECONDS);

        if (!unexpectedLogging.isEmpty()) {
            fail("Found unexpected logging: " +
                 unexpectedLogging.stream()
                 .map(lr -> new SimpleFormatter().format(lr))
                 .collect(Collectors.joining(", ")));
        }
    }
}
