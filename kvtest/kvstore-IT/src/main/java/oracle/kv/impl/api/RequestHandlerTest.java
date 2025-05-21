/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.util.TestUtils.checkException;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Delete;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.rep.EnvironmentFailureRetryException;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepEnvHandleManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DurabilityTranslator;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.util.TestUtils;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.MatchpointSearchResults;
import com.sleepycat.je.txn.Locker;

import org.junit.Test;

/**
 * Tests request via the RequestHandler.
 */
public class RequestHandlerTest extends RepNodeTestBase {

    final RepNodeId rg1rn1Id = new RepNodeId(1,1);
    final RepNodeId rg1rn2Id = new RepNodeId(1,2);

    private KVRepTestConfig config;
    private KVStore kvs;

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    /* (non-Javadoc)
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (config != null) {
            config.stopRHs();
            config.stopRepNodeServices();
        }
        if (kvs != null) {
            kvs.close();
        }
    }

    /**
     * Test DurabilityException.getNoSideEffects()
     */
    @Test
    public void testGetNoSideEffects() {
        oracle.kv.Durability.ReplicaAckPolicy commitPolicy =
            DurabilityTranslator.translate(ReplicaAckPolicy.SIMPLE_MAJORITY);
        Set<String> availableReplicas = new HashSet<>();
        availableReplicas.add("rg1-rn1");
        int requiredNodeCount = 3;

        Exception ioe = new IOException();
        DurabilityException negtiveDE = new DurabilityException (ioe,
            commitPolicy, requiredNodeCount, availableReplicas);
        assertTrue(!negtiveDE.getNoSideEffects());

        Locker locker = createMock(Locker.class);
        locker.setOnlyAbortable(isA(OperationFailureException.class));
        expectLastCall();
        replay(locker);

        int requiredAckCount = 3;
        InsufficientReplicasException ire = new InsufficientReplicasException(
            locker, ReplicaAckPolicy.SIMPLE_MAJORITY, requiredAckCount,
            availableReplicas);
        DurabilityException positiveDE = new DurabilityException (ire,
            commitPolicy, requiredNodeCount, availableReplicas);
        assertTrue(positiveDE.getNoSideEffects());
    }

    /**
     * Test basic operations.
     */
    @Test
    public void testGetPutDelete()
        throws IOException, EnvironmentFailureException {

        config = new KVRepTestConfig(this, 1, 1, 1, 2);
        RepNode rg1n1 = config.getRN(rg1rn1Id);
        RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());
        config.startupRHs();

        KeyGenerator kgen = new KeyGenerator(rg1n1);

        Durability durability = Durability.COMMIT_NO_SYNC;
        Consistency consistency = Consistency.NONE_REQUIRED;

        ClientId clientId = new ClientId(1);
        final Key[] testKeys = kgen.getKeys(10);

        for (Key key : testKeys) {
            final byte[] keyBytes = key.toByteArray();
            final PartitionId partitionId = rg1n1.getPartitionId(keyBytes);

            /* Key shouldn't be there. */
            InternalOperation op = new Get(keyBytes);
            Request request = new Request(op, partitionId, false, null,
                                          consistency, 0, 0, clientId,
                                          timeoutMs, null);

            Value v1 = handler.execute(request).getResult().getPreviousValue();
            assertNull(v1);

            /* Insert the key */
            v1 = Value.createValue(getKeyBytes(key));
            op = new Put(keyBytes, v1, ReturnValueVersion.Choice.NONE);
            request = new Request(op, partitionId, true, durability, null,
                                  0, 0, clientId, 100, null);
            assertNotNull(handler.execute(request).getResult().getNewVersion());

            /* Retrieve the key */
            op = new Get(keyBytes);
            request = new Request(op, partitionId, false, null, consistency,
                                  0, 0, clientId, 100, null);

            final Value v2 =
                handler.execute(request).getResult().getPreviousValue();
            assertTrue(Arrays.equals(v1.getValue(), v2.getValue()));
        }

        /* Re-test get after all inserts have completed. */
        for (Key key : testKeys) {

            final byte[] keyBytes = key.toByteArray();
            final PartitionId partitionId = rg1n1.getPartitionId(keyBytes);

            InternalOperation op = new Get(keyBytes);
            Request request = new Request(op, partitionId, false, null,
                                          consistency, 0, 0, clientId, 100,
                                          null);

            final Value v =
                handler.execute(request).getResult().getPreviousValue();
            assertTrue(Arrays.equals(getKeyBytes(key), v.getValue()));
        }

        /* Delete */
        for (Key key : testKeys) {

            final byte[] keyBytes = key.toByteArray();
            final PartitionId partitionId = rg1n1.getPartitionId(keyBytes);

            InternalOperation op = new Get(keyBytes);
            Request request = new Request(op, partitionId, false, null,
                                          consistency, 0, 0, clientId, 100,
                                          null);

            final Value v =
                handler.execute(request).getResult().getPreviousValue();
            assertTrue(Arrays.equals(getKeyBytes(key), v.getValue()));

            op = new Delete(keyBytes, ReturnValueVersion.Choice.NONE);
            request = new Request(op, partitionId, true, durability, null,
                                  0, 0, clientId, 100, null);
            boolean deleted =
                handler.execute(request).getResult().getSuccess();
            assertTrue(deleted);

            /*
             * Repeat the delete operation creating a new request since
             * ops cannot be reused.
             */
            op = new Delete(keyBytes, ReturnValueVersion.Choice.NONE);
            request = new Request(op, partitionId, true, durability, null,
                                  0, 0, clientId, 100, null);
            deleted = handler.execute(request).getResult().getSuccess();
            assertTrue(!deleted);

            op = new Get(keyBytes);
            request = new Request(op, partitionId, false, null, consistency,
                                  0, 0, clientId, 100, null);

            final Value v2 =
                handler.execute(request).getResult().getPreviousValue();
            assertNull(v2);
        }
        rg1n1.stop(false);
    }

    /*
     * Verifies that an interrupted request, which may happen during a node
     * shutdown, results in an RNUnavailable exception so that the request can
     * be retried by the dispatcher at another node.
     */
    @Test
    public void testInterruptedException()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 1, 1);
        final RepNode rg1n1 = config.getRN(rg1rn1Id);
        RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());
        config.startupRHs();

        handler.setTestHook(new TestHook<Request>() {

            @Override
            public void doHook(Request arg) {
                throw new ThreadInterruptedException
                (DbInternal.getEnvironmentImpl(rg1n1.getEnv(1000)),
                 "test", new InterruptedException("test"));
            }
        });

        InternalOperation op = new Get(new byte[0]);
        Request request = new Request(op, new PartitionId(1), false, null,
                                      Consistency.NONE_REQUIRED, 0, 0,
                                      new ClientId(1), timeoutMs, null);
        try {
            handler.execute(request);
            fail("Expected RNUnavailableException");
        } catch (RNUnavailableException rue) {
            /* Expected exception. */
        }
    }

    @Test
    public void testRollbackInReplica()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 3, 2);

        config.startupRHs();

        /* Simulate a RollbackException in a Replica */
        final RepNode rg1rn2 = config.getRN(rg1rn2Id);
        final ReplicatedEnvironment env2 = rg1rn2.getEnv(1000);
        assertEquals(ReplicatedEnvironment.State.REPLICA, env2.getState());

        /* Creating the exception invalidates env2 and creates a new one */
        @SuppressWarnings("unused")
        RollbackException rbe =
            new TestRollbackException(RepInternal.getRepImpl(env2),
                                      NULL_VLSN,
                                      new MatchpointSearchResults
                                           (RepInternal.getRepImpl(env2)));

        new PollCondition(1000, 60000) {

            @Override
            protected boolean condition() {
               return env2 != rg1rn2.getEnv(1000);
            }
        }.await();
        assertTrue(!env2.isValid());

        final ReplicatedEnvironment env2new = rg1rn2.getEnv(1000);
        assertNotNull(env2new);
        assertTrue(env2new != env2);
        assertEquals(ReplicatedEnvironment.State.REPLICA, env2new.getState());
    }

    @Test
    public void testRequestTimeoutOnInsufficientAcks()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 1, 1);

        final RepNode rg1n1 = config.getRN(rg1rn1Id);
        RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());

        config.startupRHs();
        handler.setPreCommitTestHook(new TestHook<RepImpl>() {
            @Override
            public void doHook(RepImpl arg) {
                throw new InsufficientAcksException("request timeout");
            }
        });

        InternalOperation op = new Get(new byte[0]);
        Request request = new Request(op, new PartitionId(1), false, null,
                                      Consistency.NONE_REQUIRED, 0, 0,
                                      new ClientId(1), timeoutMs, null);
        try {
            handler.execute(request);
            fail("Expected RequestTimeoutException");
        } catch (oracle.kv.RequestTimeoutException rue) {
            /* Expected exception. */
        }
    }

    /**
     * Verify that log messages associated with runtime exceptions associated
     * with closed or invalidate environment are rate limited.
     */
    @Test
    public void testRateLimitingLog()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 1, 1);

        final RepNode rg1n1 = config.getRN(rg1rn1Id);
        final RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());

        final Thread putThreads[] = new Thread[5];
        final CountDownLatch readyLatch = new CountDownLatch(putThreads.length);
        final CountDownLatch efeLatch = new CountDownLatch(1);
        final AtomicReference<String> failTest = new AtomicReference<>();

        try {
            config.startupRHs();
            handler.setPreCommitTestHook(new TestHook<RepImpl>() {
                @Override
                public void doHook(RepImpl arg) {
                    try {
                        readyLatch.countDown();

                        /* Wait for the EFE */
                        efeLatch.await();

                        /* Simulate a NPE following the EFE */
                        throw new NullPointerException("test");
                    } catch (InterruptedException e) {
                        failTest.compareAndSet(null,
                                               "Unexpected interrupt");
                    }
                }
            });
            final Value v = Value.createValue(new byte[0]);
            for (int i=0; i < putThreads.length; i++) {
                final int ki = i;

                /* Simulate client threads doing updates. */
                putThreads[i] = new Thread() {

                    @Override
                    public void run() {
                        InternalOperation op =
                            new Put(new byte[ki], v,
                                    ReturnValueVersion.Choice.NONE);
                        Request request = new Request(op, new PartitionId(1),
                                                      true,
                                                      Durability.COMMIT_NO_SYNC,
                                                      null, 0, 0,
                                                      new ClientId(1),
                                                      timeoutMs, null);
                        try {
                            handler.execute(request);
                        } catch (EnvironmentFailureException e) {
                            /* Ignore. */
                        } catch (EnvironmentFailureRetryException e) {
                        } catch (Exception e) {
                            failTest.compareAndSet(null,
                                                   ("Unexpected exception:" +
                                                    e));
                        }
                    }
                };

                putThreads[i].start();
            }

            /* Wait for requests to start transactions and do writes. */
            readyLatch.await();
            EnvironmentFailureException.
                unexpectedException(rg1n1.getEnvImpl(0),
                                    new IllegalStateException("test"));
            /* release client threads. */
            efeLatch.countDown();

            /* Wait for them to finish. */
            for (Thread putThread : putThreads) {
                putThread.join();
            }

            assertNull(failTest.get());

            /* Check for expected number of abbreviations. */
            assertEquals(putThreads.length - 1,
                         handler.getRateLimitingLoggerLong()
                         .getNumSuppressedMsgs());
            assertEquals(1, handler.getRateLimitingLoggerLong()
                         .getLimitedMessageCount());
        } catch (InterruptedException e) {
            fail("unexpected interrupt");
        }
    }

    /**
     * Test that the request handler throws RNUnavailableException if the
     * environment is not available for longer than the environment restart
     * retry time, and the request timeout is not exceeded.  [#22661]
     */
    @Test
    public void testEnvUnavailable()
        throws IOException {

        try {
            testEnvUnavailable(timeoutMs);
            fail("Missing exception");
        } catch (RNUnavailableException e) {
            assertTrue(e.getMessage(),
                       e.getMessage().
                       startsWith("Environment for RN: " + rg1rn2Id +
                         " was unavailable"));
            logger.info("Got expected exception: " + e);
        }
    }

    /**
     * Test that the request handle throws RequestTimeoutException if the
     * environment is not available for longer than the environment restart
     * retry time, and the request timeout is exceeded.  [#22661]
     */
    @Test
    public void testEnvUnavailableShortRequestTimeout()
        throws IOException {

        try {
            testEnvUnavailable(1);
            fail("Missing exception");
        } catch (RequestTimeoutException e) {
            logger.info("Got expected exception: " + e);
        }
    }

    /**
     * Test executing a request on an RN with an unavailable environment where
     * recovery takes a long time, using the specified request timeout.
     */
    private void testEnvUnavailable(final int requestTimeoutMs)
        throws IOException {

        /* Create an environment with RF=3 */
        config = new KVRepTestConfig(this, 1, 1, 3, 1);
        config.startupRHs();

        try {
            /*
             * Set a recovery progress listener test hook that delays recovery
             * progress -- 1 second should be long enough that the restart
             * takes longer than the timeout for getting the environment
             */
            RepEnvHandleManager.recoveryProgressTestHook =
                new TestHook<RecoveryProgress>() {
                    @Override
                    public void doHook(RecoveryProgress phase) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                    }
                };

            /* Invalidate the environment for replica rg1rn2 */
            final RepNode rg1rn2 = config.getRN(rg1rn2Id);
            final ReplicatedEnvironment env2 = rg1rn2.getEnv(1000);
            assertEquals(ReplicatedEnvironment.State.REPLICA, env2.getState());
            @SuppressWarnings("unused")
            final RollbackException rbe =
                new TestRollbackException(RepInternal.getRepImpl(env2),
                                          NULL_VLSN,
                                          new MatchpointSearchResults
                                           (RepInternal.getRepImpl(env2)));

            /* Arrange for requests to rg1rn2 */
            final RequestHandlerImpl handler = config.getRH(rg1rn2Id);
            final Request request = new Request(
                new Get(new byte[0]), new PartitionId(1), false, null,
                Consistency.NONE_REQUIRED, 0, 0, new ClientId(1),
                requestTimeoutMs, null);

            /* Perform the request */
            handler.execute(request);

        } finally {
            RepEnvHandleManager.recoveryProgressTestHook = null;
        }
    }

    protected byte[] getKeyBytes(Key key) {
        return key.getMajorPath().get(0).getBytes();
    }

    @SuppressWarnings("serial")
    static class TestRollbackException extends RollbackException {

        public TestRollbackException(RepImpl repImpl,
                                     long matchpointVLSN,
                                     MatchpointSearchResults searchResults) {
            super(repImpl, matchpointVLSN, searchResults);
        }

        /** Only for use by wrapSelf. */
        private TestRollbackException(String message,
                                      RollbackException cause) {
            super(message, cause);
        }

        @Override
        public String getMessage() {
            return "simulated rollback";
        }

        @Override
        public RollbackException wrapSelf(
            String msg,
            EnvironmentFailureException clonedCause) {

            return new TestRollbackException(
                msg, (RollbackException) clonedCause);
        }
    }

    /**
     * Tests the activity count for the latency tracker of the operation
     * tracker.
     */
    @Test
    public void testActivityCount() throws Exception {
        final OperationsStatsTracker opTracker = new OperationsStatsTracker();
        final long ts = opTracker.getLatencyTracker().markStart();
        opTracker.getLatencyTracker().markFinish(
            InternalOperation.OpCode.GET, ts);
        assertEquals(
            0,
            opTracker.getLatencyTracker()
            .getActivityCounter().getActiveCount());
    }

    @Test
    public void testGetVersionConsistency()
        throws Exception {

        config = new KVRepTestConfig(this,
            1, /* nDC */
            2, /* nSN */
            3, /* repFactor */
            10 /* nPartitions */);
        config.startRepNodeServices();
        RepNode rg1n1 = config.getRN(rg1rn1Id);

        /* Generate a key which will be on the rg1 */
        Key k1 = new KeyGenerator(rg1n1).getKeys(1)[0];
        Key k2 = new KeyGenerator(config.getRN(new RepNodeId(2, 1)))
            .getKeys(1)[0];
        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        Version version = kvs.put(k1, Value.EMPTY_VALUE);
        assertNotNull(kvs.put(k2, Value.EMPTY_VALUE));

        Consistency.Version versionConsistency =
            new Consistency.Version(version, 10, TimeUnit.SECONDS);
        KVStoreImpl kvsImpl = (KVStoreImpl) kvs;

        /* check if RequestHandler throws the correct exception */
        testGetRequest(kvsImpl, k2, versionConsistency);

        /* exercise storeIterator with version consistency */
        testStoreIterator(versionConsistency);
    }

    private void testGetRequest(KVStoreImpl kvsImpl,
                                Key key,
                                Consistency consistency)
        throws Exception {

        for (int i = 1; i <= 3; i++) {
            RepNodeId rnId = new RepNodeId(2, i);
            RequestHandlerImpl handler = config.getRH(new RepNodeId(2, i));
            RepNode rn = config.getRN(rnId);
            Request getReqeust = kvsImpl.makeGetRequest(key, 0, consistency,
                3, TimeUnit.SECONDS, true /* excludeTombstone */);

            if (rn.getEnv(1).getState() == State.MASTER) {
                Response res = handler.execute(getReqeust);
                assertNotNull(KVStoreImpl.processGetResult(res.getResult()));
            } else {
                try {
                    handler.execute(getReqeust);
                    fail("expect to fail");
                } catch (WrappedClientException wce) {
                    TestUtils.checkCause(
                        wce, IllegalArgumentException.class,
                        "Unable to ensure consistency");
                }
            }
        }
    }

    private void testStoreIterator(Consistency consistency) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                try {
                    Iterator<KeyValueVersion> iter = kvs.storeIterator(
                        Direction.UNORDERED, 100, null, null, null,
                        consistency, 10, TimeUnit.SECONDS);
                    while (iter.hasNext()) {
                        iter.next();
                    }
                    return false;
                } catch (IllegalArgumentException iae) {
                    TestUtils.checkException(
                        iae, IllegalArgumentException.class,
                        "Unable to ensure consistency");
                    return true;
                }
            }
        }.await();
        assert(success);
    }

    /**
     * Test that requests fail with RequestTimeoutException if the specified
     * timeout is already 0 or negative. Tests for a regression introduced by
     * work on KVSTORE-330.
     */
    @Test
    public void testTimedOut() throws Exception {
        config = new KVRepTestConfig(this, 1, 1, 1, 1);
        final RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        final RepNode rg1n1 = config.getRN(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());
        config.startupRHs();
        final InternalOperation op = new Get(new byte[0]);

        /* Test timeout=0 */
        final Request request =
            new Request(op, new PartitionId(1), false /* write */,
                        null /* durability */, Consistency.NONE_REQUIRED,
                        0 /* ttl */, 0 /* topoSeqNumber */, new ClientId(1),
                        0 /* timeout */, null /* readZoneIds */);
        checkException(() -> handler.execute(request),
                       RequestTimeoutException.class,
                       "timed out");

        /* Test timeout=-42 */
        final Request request2 =
            new Request(op, new PartitionId(1), false /* write */,
                        null /* durability */, Consistency.NONE_REQUIRED,
                        0 /* ttl */, 0 /* topoSeqNumber */, new ClientId(1),
                        -42 /* timeout */, null /* readZoneIds */);
        checkException(() -> handler.execute(request2),
                       RequestTimeoutException.class,
                       "timed out");
    }

    /** Tests the cleanup task on requesterMap */
    @Test
    public void testRequesterMap() throws Exception {
        config = new KVRepTestConfig(this, 1, 1, 1, 1);
        RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        handler.setReqMapEntryLifetimeNs(TimeUnit.MILLISECONDS.toNanos(1000));
        handler.setReqMapCleanupPeriodMs(100);

        RepNode rg1n1 = config.getRN(rg1rn1Id);
        handler.initialize(config.getParams(rg1n1.getRepNodeId()), rg1n1,
                           new OperationsStatsTracker());
        config.startupRHs();
        for (int i = 0; i < 3; i++) {
            assertNotNull(getResponseStatusChanges(handler, i));
        }
        /* Cleanup has not occured yet */
        for (int i = 0; i < 3; i++) {
            assertNull(getResponseStatusChanges(handler, i));
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException ie) {
        }

        assertNull(getResponseStatusChanges(handler, 1));
        try {
            Thread.sleep(600);
        } catch (InterruptedException ie) {
        }
        /* All client's entries should have been removed except c1 */
        assertNotNull(getResponseStatusChanges(handler, 0));
        assertNull(getResponseStatusChanges(handler, 1));
        assertNotNull(getResponseStatusChanges(handler, 2));
        try {
            Thread.sleep(1100);
        } catch (InterruptedException ie) {
        }
        /* All client's entries should have been removed */
        for (int i = 0; i < 3; i++) {
            assertNotNull(getResponseStatusChanges(handler, i));
        }
    }

    private StatusChanges getResponseStatusChanges(RequestHandlerImpl handler,
                                                   int clientId)
        throws Exception {
        Request request = new Request(new Get(new byte[0]),
                                      new PartitionId(1), false, null,
                                      Consistency.NONE_REQUIRED, 0, 0,
                                      new ClientId(clientId), timeoutMs, null);
        return handler.execute(request).getStatusChanges();
    }
}
