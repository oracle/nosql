/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static oracle.kv.util.TestUtils.checkException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.ServerResourceLimitException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.util.TestUtils;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests basic API operations for functional completeness and correctness, but
 * does not test exceptions, timeouts, durability or consistency.
 */
@RunWith(FilterableParameterized.class)
public class BasicOperationTest extends ClientTestBase {

    /* The explicit timeout associated with requests. */
    static final int REQ_TIMEOUT_SEC = 10;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /* These are K/V only tests */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
    }

    public BasicOperationTest(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp()
        throws Exception {

        checkRMIRegistryLookup();

        /*
         * Do not start services because we want to do this explicitly with a
         * list of commands and/or override the config settings before calling
         * KVStoreFactory.getStore.  Tests that want default behavior in this
         * respect should call open().
         */
        startServices = false;
        super.setUp();
    }

    /**
     * A single test method is used to avoid the overhead of service setup and
     * teardown for every test.
     */
    @Test
    public void testAll()
        throws Exception {

        open();
        basicTests();
        basicTestsConsistency();
        basicTestsDurability();
        basicTestsWithReturnValueVersion();
        conditionalPutDeleteTests();
        conditionalPutDeleteTestsWithReturnValueVersion();
        prohibitInternalKeyspaceTests();
        otherExceptionTests();
        close();
    }

    /**
     * Test mainstream usage of put/get/delete.
     */
    private void basicTests()
        throws Exception {

        final Key key = Key.createKey("one", "two");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);

        /* Get non-existent. */
        ValueVersion valVer = store.get(key);
        assertNull(valVer);

        /* Insert first version. */
        final Version ver1 = store.put(key, val1);
        assertNotNull(ver1);

        /* Get first version and check. */
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val1);
        assertEquals(valVer.getVersion(), ver1);

        /* Update to second version. */
        final Version ver2 = store.put(key, val2);
        assertNotNull(ver2);

        /* Get second version and check. */
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val2);
        assertEquals(valVer.getVersion(), ver2);

        /* Delete. */
        final boolean deleted = store.delete(key);
        assertTrue(deleted);

        /* Get non-existent. */
        valVer = store.get(key);
        assertNull(valVer);
    }

    /**
     * A very rudimentary test to ensure that consistency parameters
     * are accepted.
     */
    @SuppressWarnings("deprecation")
    private void basicTestsConsistency()
        throws Exception {

        final Key key = Key.createKey("onec", "twoc");
        final Value val1 = Value.createValue(new byte[1]);

        final Version ver1 = store.put(key, val1);
        assertNotNull(ver1);

        ValueVersion valVer = store.get(key);
        assertNotNull(valVer);

        final Consistency.Version versionConsistency =
            new Consistency.Version(ver1, 10L, TimeUnit.SECONDS);
        valVer = store.get(key, versionConsistency, REQ_TIMEOUT_SEC,
                           TimeUnit.SECONDS);

        assertTrue(valVer.getVersion().getVLSN() >=
                   versionConsistency.getVersion().getVLSN());

        final Consistency.Time timeConsistency =
            new Consistency.Time(5, TimeUnit.SECONDS, REQ_TIMEOUT_SEC,
                                 TimeUnit.SECONDS);

        valVer = store.get(key, timeConsistency, REQ_TIMEOUT_SEC,
                           TimeUnit.SECONDS);

        assertNotNull(valVer);

        valVer = store.get(key, Consistency.ABSOLUTE, REQ_TIMEOUT_SEC,
                           TimeUnit.SECONDS);

        assertNotNull(valVer);

        valVer = store.get(key, Consistency.NONE_REQUIRED, REQ_TIMEOUT_SEC,
                           TimeUnit.SECONDS);

        assertNotNull(valVer);

        valVer = store.get(key, Consistency.NONE_REQUIRED_NO_MASTER,
                           REQ_TIMEOUT_SEC, TimeUnit.SECONDS);

        assertNotNull(valVer);
    }

    /**
     * A very rudimentary test to ensure that durability parameters
     * are accepted.
     */
    private void basicTestsDurability()
        throws Exception {

        final Key key = Key.createKey("oned");
        final Value val1 = Value.createValue(new byte[1]);

        final Durability durability = Durability.COMMIT_SYNC;
        final Version ver1 = store.put(key, val1, null,
                                       durability,
                                       REQ_TIMEOUT_SEC,
                                       TimeUnit.SECONDS);
        assertNotNull(ver1);
    }

    /**
     * Same as basicTests but using ReturnValueVersion.
     */
    private void basicTestsWithReturnValueVersion()
        throws Exception {

        final Key key = Key.createKey("one", "two");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);

        final ReturnValueVersion prevVal =
            new ReturnValueVersion(ReturnValueVersion.Choice.ALL);

        /* Get non-existent. */
        ValueVersion valVer = store.get(key);
        assertNull(valVer);

        /* Insert first version. */
        final Version ver1 = store.put(key, val1, prevVal, null, 0, null);
        assertNotNull(ver1);
        assertNull(prevVal.getValue());
        assertNull(prevVal.getVersion());

        /* Get first version and check. */
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val1);
        assertEquals(valVer.getVersion(), ver1);

        /* Update to second version. */
        final Version ver2 = store.put(key, val2, prevVal, null, 0, null);
        assertNotNull(ver2);
        assertEquals(prevVal.getValue(), val1);
        assertEquals(prevVal.getVersion(), ver1);

        /* Get second version and check. */
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val2);
        assertEquals(valVer.getVersion(), ver2);

        /* Delete. */
        final boolean deleted = store.delete(key, prevVal, null, 0, null);
        assertTrue(deleted);
        assertEquals(prevVal.getValue(), val2);
        assertEquals(prevVal.getVersion(), ver2);

        /* Get non-existent. */
        valVer = store.get(key);
        assertNull(valVer);
    }

    /**
     * Test mainstream usage of putIfPresent, putIfAbsent, putIfVersion and
     * deleteIfVersion.
     */
    private void conditionalPutDeleteTests()
        throws Exception {

        final Key key = Key.createKey("one", "two");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);
        final Value val3 = Value.createValue(new byte[3]);

        /* Get non-existent. */
        ValueVersion valVer = store.get(key);
        assertNull(valVer);

        /* putIfPresent should fail. */
        Version ver1 = store.putIfPresent(key, val1);
        assertNull(ver1);
        valVer = store.get(key);
        assertNull(valVer);

        /* putIfAbsent should succeed. */
        ver1 = store.putIfAbsent(key, val1);
        assertNotNull(ver1);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val1);
        assertEquals(valVer.getVersion(), ver1);

        /* putIfAbsent should fail. */
        Version ver2 = store.putIfAbsent(key, val2);
        assertNull(ver2);

        /* putIfPresent should succeed. */
        ver2 = store.putIfPresent(key, val2);
        assertNotNull(ver2);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val2);
        assertEquals(valVer.getVersion(), ver2);

        /* putIfVersion should fail for ver1. */
        Version ver3 = store.putIfVersion(key, val3, ver1);
        assertNull(ver3);

        /* putIfVersion should succeed for ver2. */
        ver3 = store.putIfVersion(key, val3, ver2);
        assertNotNull(ver3);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);

        /* deleteIfVersion should fail for ver1 and ver2. */
        boolean deleted = store.deleteIfVersion(key, ver1);
        assertFalse(deleted);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);
        deleted = store.deleteIfVersion(key, ver2);
        assertFalse(deleted);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);

        /* deleteIfVersion should succeed for ver3. */
        deleted = store.deleteIfVersion(key, ver3);
        assertTrue(deleted);
        valVer = store.get(key);
        assertNull(valVer);

        /* Get non-existent. */
        valVer = store.get(key);
        assertNull(valVer);
    }

    /**
     * Same as conditionalPutDeleteTests but using ReturnValueVersion.
     */
    private void conditionalPutDeleteTestsWithReturnValueVersion()
        throws Exception {

        final Key key = Key.createKey("one", "two");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);
        final Value val3 = Value.createValue(new byte[3]);

        final ReturnValueVersion prevVal =
            new ReturnValueVersion(ReturnValueVersion.Choice.ALL);

        /* Get non-existent. */
        ValueVersion valVer = store.get(key);
        assertNull(valVer);

        /* putIfPresent should fail. */
        Version ver1 = store.putIfPresent(key, val1, prevVal, null, 0, null);
        assertNull(ver1);
        assertNull(prevVal.getValue());
        assertNull(prevVal.getVersion());
        valVer = store.get(key);
        assertNull(valVer);

        /* putIfAbsent should succeed. */
        ver1 = store.putIfAbsent(key, val1, prevVal, null, 0, null);
        assertNotNull(ver1);
        assertNull(prevVal.getValue());
        assertNull(prevVal.getVersion());
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val1);
        assertEquals(valVer.getVersion(), ver1);

        /* putIfAbsent should fail. */
        Version ver2 = store.putIfAbsent(key, val2, prevVal, null, 0, null);
        assertNull(ver2);
        assertEquals(prevVal.getValue(), val1);
        assertEquals(prevVal.getVersion(), ver1);

        /* putIfPresent should succeed. */
        ver2 = store.putIfPresent(key, val2, prevVal, null, 0, null);
        assertNotNull(ver2);
        assertEquals(prevVal.getValue(), val1);
        assertEquals(prevVal.getVersion(), ver1);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val2);
        assertEquals(valVer.getVersion(), ver2);

        /* putIfVersion should fail for ver1. */
        Version ver3 = store.putIfVersion(key, val3, ver1, prevVal, null, 0,
                                          null);
        assertNull(ver3);
        assertEquals(prevVal.getValue(), val2);
        assertEquals(prevVal.getVersion(), ver2);

        /* putIfVersion should succeed for ver2. */
        ver3 = store.putIfVersion(key, val3, ver2, prevVal, null, 0, null);
        assertNotNull(ver3);
        assertNull(prevVal.getValue());
        assertNull(prevVal.getVersion());
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);

        /* deleteIfVersion should fail for ver1 and ver2. */
        boolean deleted = store.deleteIfVersion(key, ver1, prevVal, null, 0,
                                                null);
        assertFalse(deleted);
        assertEquals(prevVal.getValue(), val3);
        assertEquals(prevVal.getVersion(), ver3);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);
        deleted = store.deleteIfVersion(key, ver2, prevVal, null, 0, null);
        assertEquals(prevVal.getValue(), val3);
        assertEquals(prevVal.getVersion(), ver3);
        assertFalse(deleted);
        valVer = store.get(key);
        assertNotNull(valVer);
        assertEquals(valVer.getValue(), val3);
        assertEquals(valVer.getVersion(), ver3);

        /* deleteIfVersion should succeed for ver3. */
        deleted = store.deleteIfVersion(key, ver3, prevVal, null, 0, null);
        assertTrue(deleted);
        assertNull(prevVal.getValue());
        assertNull(prevVal.getVersion());
        valVer = store.get(key);
        assertNull(valVer);

        /* Get non-existent. */
        valVer = store.get(key);
        assertNull(valVer);
    }

    /**
     * Ensures that an IllegalArgumentException is thrown when specifying a Key
     * in the internal keyspace (//).
     */
    private void prohibitInternalKeyspaceTests() {

        final String expectMsg = KeySerializer.EXCEPTION_MSG;
        final Key illegalKey = Key.createKey(Arrays.asList("", "a"));
        final Key legalKey = Key.createKey("a");
        final Value value = Value.createValue(new byte[1]);
        final Version version = store.put(legalKey, value);
        assertNotNull(version);

        /* get */
        try {
            store.get(illegalKey);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* put */
        try {
            store.put(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* delete */
        try {
            store.delete(illegalKey);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* putIfPresent */
        try {
            store.putIfPresent(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* putIfAbsent */
        try {
            store.putIfAbsent(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* putIfVersion */
        try {
            store.putIfVersion(illegalKey, value, version);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* deleteIfVersion */
        try {
            store.deleteIfVersion(illegalKey, version);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* multiGet */
        try {
            store.multiGet(illegalKey, null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* multiGetKeys */
        try {
            store.multiGetKeys(illegalKey, null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* multiGetIterator */
        try {
            store.multiGetIterator(Direction.FORWARD, 1, illegalKey, null,
                                   null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* multiGetKeysIterator */
        try {
            store.multiGetKeysIterator(Direction.FORWARD, 1, illegalKey, null,
                                   null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* storeIterator */
        try {
            store.storeIterator(Direction.UNORDERED, 1, illegalKey, null,
                                null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* storeKeysIterator */
        try {
            store.storeKeysIterator(Direction.UNORDERED, 1, illegalKey, null,
                                    null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* multiDelete */
        try {
            store.multiDelete(illegalKey, null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        final OperationFactory factory = store.getOperationFactory();

        /* createPut */
        try {
            factory.createPut(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* createPutIfAbsent */
        try {
            factory.createPutIfAbsent(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* createPutIfPresent */
        try {
            factory.createPutIfPresent(illegalKey, value);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* createPutIfVersion */
        try {
            factory.createPutIfVersion(illegalKey, value, version);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* createDelete */
        try {
            factory.createDelete(illegalKey);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        /* createDeleteIfVersion */
        try {
            factory.createDeleteIfVersion(illegalKey, version);
            fail();
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(expectMsg));
        }

        final boolean deleted = store.delete(legalKey);
        assertTrue(deleted);
    }

    private void otherExceptionTests() {

        /* Illegal Direction. */
        try {
            store.multiGetIterator(Direction.UNORDERED, 0,
                                   Key.createKey("x"), null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("Direction");
        }
        try {
            store.multiGetKeysIterator(Direction.UNORDERED, 0,
                                       Key.createKey("x"), null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("Direction");
        }
        try {
            store.storeIterator(Direction.FORWARD, 0);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("Direction");
        }
        try {
            store.storeKeysIterator(Direction.FORWARD, 0);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("Direction");
        }

        /* Illegal non-empty minor path in parent key. */
        try {
            store.storeIterator(Direction.UNORDERED, 0,
                                Key.createKey("x", "y"), null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("parentKey");
        }
        try {
            store.storeKeysIterator(Direction.UNORDERED, 0,
                                    Key.createKey("x", "y"), null, null);
            fail();
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("parentKey");
        }

        /*
         * Execute op list must be non-empty. Note that it's not possible to
         * call execute with a null parameter. The multiple overloadings of
         * KVStore.execute end up requiring that a non-null argument is used
         */
        try {
            store.execute(new ArrayList<Operation>());
            fail();
        } catch (OperationExecutionException e) {
            fail(e.toString());
        } catch (IllegalArgumentException expected) {
            expected.getMessage().contains("Operations");
        }
    }

    /**
     * Ensures that a ConsistencyException is thrown back through the API code
     * path in a couple simple cases.
     *
     * Also tests that the default Consistency can be overridden.
     *
     * Consistency.Version cannot be used for this test, because we don't
     * currently have a way to write a record, then stop the masters, then read
     * with a future version calculated from the write version.
     */
    @Test
    public void testConsistencyException()
        throws Exception {

        /*
         * Call Assume so that this test only run with non-secure configuration.
         */
        Assume.assumeFalse(secure);
        startServices(Arrays.asList(ClientTestServices.STOP_ALL_MASTERS));
        final long requestTimeout =
            config.getRequestTimeout(TimeUnit.MILLISECONDS);

        /*
         * Specify an extremely short permissible lag that cannot be satisfied
         * with the masters all stopped.  First try a consistency timeout
         * longer than the request timeout.
         */
        Consistency futureTimeConsistency =
            new Consistency.Time(1, TimeUnit.MILLISECONDS,
                                 requestTimeout * 2, TimeUnit.MILLISECONDS);
        config.setConsistency(futureTimeConsistency);
        store = KVStoreFactory.getStore(config);

        /*
         * Should fail with default future time consistency and long
         * consistency timeout
         */
        final Key key = Key.createKey("onec", "twoc");
        try {
            store.get(key);
            fail("Expected ConsistencyException");
        } catch (ConsistencyException expected) {
            assertEquals(futureTimeConsistency, expected.getConsistency());
        }

        /* Should also fail a short consistency timeout */
        futureTimeConsistency =
            new Consistency.Time(1, TimeUnit.MILLISECONDS,
                                 requestTimeout / 2, TimeUnit.MILLISECONDS);
        try {
            store.get(key, futureTimeConsistency, 0, null);
            fail("Expected ConsistencyException");
        } catch (ConsistencyException expected) {
            assertEquals(futureTimeConsistency, expected.getConsistency());
        }

        /* Should succeed with overridden Consistency.NONE_REQUIRED. */
        final ValueVersion valVer =
            store.get(key, Consistency.NONE_REQUIRED, 0, null);
        assertNull(valVer);

        /* Should succeed with overridden Consistency.NONE_REQUIRED_NO_MASTER.
         */
        @SuppressWarnings("deprecation")
        final ValueVersion valVer1 =
            store.get(key, Consistency.NONE_REQUIRED_NO_MASTER, 0, null);
        assertNull(valVer1);

        close();
    }

    /**
     * Ensures that a DurabilityException can be thrown back through the API
     * code path in at least one case.
     *
     * Also tests that the default Durability can be overridden.
     *
     * A zero timeout must be used or RequestTimeoutException will be thrown
     * instead of DurabilityException.
     */
    @Test
    public void testDurabilityException()
        throws Exception {

        /*
         * Call Assume so that this test only run with non-secure configuration.
         */
        Assume.assumeFalse(secure);
        startServices(Arrays.asList(ClientTestServices.STOP_ALL_REPLICAS));
        config.setDurability
            (new Durability(Durability.SyncPolicy.NO_SYNC,
                            Durability.SyncPolicy.NO_SYNC,
                            Durability.ReplicaAckPolicy.NONE));
        store = KVStoreFactory.getStore(config);

        /* Should fail with overridden ReplicaAckPolicy.SIMPLE_MAJORITY. */
        final Key key = Key.createKey("onec", "twoc");
        final Value val = Value.createValue(new byte[1]);
        final Durability durability = Durability.COMMIT_NO_SYNC;
        try {
            store.put(key, val, null /*prevValue*/, durability, 0, null);
            fail();
        } catch (RequestTimeoutException rte) {
            /*
             * A RTE can originate from different components of the request
             * processing pipeline. If the RTE is generated by the dialog
             * layer, then there may not be a known cause. In this specific
             * case, since MasterTxn.txnBeginHook wait for roughly the same
             * amount of time as the dialog, it being determined in both cases
             * by the request timeout, either can win the timeout race. If the
             * timeout was detected on the server side, then the cause will be
             * a DurabilityException.
             */
            if (rte.getCause() instanceof DurabilityException) {
                /* The server won the race. */
                DurabilityException expected =
                    (DurabilityException) rte.getCause();
                assertEquals(durability.getReplicaAck(),
                             expected.getCommitPolicy());
                assertTrue(expected.getRequiredNodeCount() > 0);
                assertTrue(expected.getAvailableReplicas().isEmpty());
            }
        }

        /* Should succeed with default ReplicaAckPolicy.NONE. */
        store.put(key, val);

        close();
    }

    @Test
    public void testDiskLimitException()
        throws Exception {

        Assume.assumeFalse(secure);
        startServices(Arrays.asList(
            ClientTestServices.STORAGE_SIZE, "50 MB"));
        config.setDurability
            (new Durability(Durability.SyncPolicy.NO_SYNC,
                            Durability.SyncPolicy.NO_SYNC,
                            Durability.ReplicaAckPolicy.NONE));
        store = KVStoreFactory.getStore(config);

        final Key key = Key.createKey("onec", "twoc");
        final Value val = Value.createValue(new byte[100 * 1024]);

        boolean gotFailure = false;

        /*
         * We should get a DiskLimitException after writing 100 MB, because
         * this is when the cleaner is woken up. This will also exceed the
         * disk limit of 50 MB. We should write 100 MB after 1000 iterations.
         */
        for (int i = 0; i < 2000; i += 1) {
            try {
                store.put(key, val, null /*prevValue*/, null, 0, null);
            } catch (FaultException expected) {
                final String remoteMsg = expected.getRemoteStackTrace();
                assertTrue(
                    remoteMsg, remoteMsg.contains("DiskLimitException"));
                gotFailure = true;
                break;
            }
        }

        assertTrue(gotFailure);

        close();
    }

    /**
     * Checks that an exception is thrown when a store is opened with the wrong
     * store name, even though the host/port are correct.
     */
    @Test
    public void testBadStoreName()
        throws Exception {

        /*
         * Call Assume so that this test only run with non-secure configuration.
         */
        Assume.assumeFalse(secure);
        startServices(EMPTY_LIST);
        config.setStoreName("storeNameThatDoesNotMatch");
        try {
            store = KVStoreFactory.getStore(config);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
        close();
    }

    /**
     * Provokes IllegalStateException expected when using a closed store.
     */
    @Test
    public void testIllegalStateAfterClosed()
        throws Exception {

        /* Get method works before close. */
        open();
        final Key key = Key.createKey("onec", "twoc");
        ValueVersion valVer = store.get(key);
        assertNull(valVer);

        /* Get method fails after close. */
        store.close();
        try {
            valVer = store.get(key, Consistency.NONE_REQUIRED, 0, null);
            fail();
        } catch (IllegalStateException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        /* Get method works after re-open. */
        store = KVStoreFactory.getStore(config);
        valVer = store.get(key);
        assertNull(valVer);
        close();
    }

    /**
     * Provokes IllegalArgumentExceptions expected when configuring a store.
     */
    @SuppressWarnings("unused")
    @Test
    public void testBasicIllegalArguments() {

        /*
         * Call Assume so that this test only run with non-secure configuration.
         */
        Assume.assumeFalse(secure);
        try {
            new KVStoreConfig(null, "a:1");
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            new KVStoreConfig("name");
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            new KVStoreConfig("name", (String) null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        final KVStoreConfig testConfig = new KVStoreConfig("name", "a:1");

        try {
            testConfig.setStoreName(null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setHelperHosts();
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setHelperHosts((String) null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setRequestTimeout(0, null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setConsistency(null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setDurability(null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        try {
            testConfig.setRequestLimit(null);
            fail();
        } catch (IllegalArgumentException expected) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */

        assertNull("Default read zones", testConfig.getReadZones());
        try {
            testConfig.setReadZones(new String[0]);
            fail();
        } catch (IllegalArgumentException expected) {
            logger.info("Expected: " + expected);
            assertNull("Read zones not changed", testConfig.getReadZones());
        }
        try {
            testConfig.setReadZones(new String[] { "zone1", null, "zone3" });
            fail();
        } catch (IllegalArgumentException expected) {
            logger.info("Expected: " + expected);
            assertNull("Read zones not changed", testConfig.getReadZones());
        }
        try {
            testConfig.setReadZones(new String[] { "a", "b", "c", "a" });
        } catch (IllegalArgumentException expected) {
            logger.info("Expected: " + expected);
            assertNull("Read zones not changed", testConfig.getReadZones());
        }
        try {
            testConfig.setReadZones(new String[] { "a", "b", "b", "a" });
        } catch (IllegalArgumentException expected) {
            logger.info("Expected: " + expected);
            assertNull("Read zones not changed", testConfig.getReadZones());
        }
        try {
            testConfig.setReadZones(new String[] { "a", "b", "c", "a", "d" });
        } catch (IllegalArgumentException expected) {
            logger.info("Expected: " + expected);
            assertNull("Read zones not changed", testConfig.getReadZones());
        }
    }

    /**
     * Test getting and setting read zones configuration.
     */
    @Test
    public void testConfigReadZones() {

        /*
         * Call Assume so that this test only run with non-secure configuration.
         */
        Assume.assumeFalse(secure);
        final KVStoreConfig testConfig = new KVStoreConfig("name", "a:1");

        assertNull("Default should be null", testConfig.getReadZones());

        assertThat("toString with default read zones",
                   testConfig.toString(), not(containsString("readZones")));
        logger.info("toString with no readZones: " + testConfig);

        testConfig.setReadZones("zone1");
        assertArrayEquals(new String[] { "zone1" }, testConfig.getReadZones());

        assertThat("toString with read zone 'zone1'",
                   testConfig.toString(), containsString("readZones=[zone1]"));
        logger.info("toString with readZones=[zone1]: " + testConfig);

        testConfig.setReadZones((String[]) null);
        assertNull(testConfig.getReadZones());

        String[] zones = new String[] { "zone1", "zone2" };
        testConfig.setReadZones(zones);
        zones[0] = null;
        assertArrayEquals("Input array should be copied",
                          new String[] { "zone1", "zone2" },
                          testConfig.getReadZones());

        assertThat("toString with read zone 'zone1' and 'zone2'",
                   testConfig.toString(),
                   containsString("readZones=[zone1, zone2]"));
        logger.info("toString with readZones=[zone1, zone2]: " + testConfig);

        zones = testConfig.getReadZones();
        zones[0] = null;
        assertArrayEquals("Output array should be copied",
                          new String[] { "zone1", "zone2" },
                          testConfig.getReadZones());
    }

    /**
     * Test the big key whose length exceeds the limitation.
     */
    @Test
    public void testBigKey() throws Exception {
        open();
        ArrayList<String> majorComponents = new ArrayList<String>();
        ArrayList<String> minorComponents = new ArrayList<String>();
        for (int i=0; i<Short.MAX_VALUE; i++) {
            majorComponents.add(String.valueOf(i));
            minorComponents.add(String.valueOf(i));
        }
        final Key key = Key.createKey(majorComponents, minorComponents);
        Value value = Value.createValue(("0").getBytes());
        Exception e = null;
        try {
            store.put(key, value);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));

        e = null;
        try {
            store.get(key);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));

        e = null;
        try {
            store.delete(key);
        } catch (FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));

        close();
    }

    /**
     * Test that exception classes are present for exceptions that are
     * inconvenient to exercise in a client-side test, but that we want to be
     * sure are present in the client JAR.
     */
    @Test
    public void testExceptionClassesPresent() {
        assertNotNull(new ServerResourceLimitException("msg"));
    }

    /**
     * Test that the socket read timeout can exceed the request timeout when
     * using async [KVSTORE-776]
     */
    @Test
    public void testShortSocketReadTimeout() throws Exception {

        startServices();

        /*
         * Try configuring store with socket read timeout shorter than the
         * request timeout
         */
        config.setSocketReadTimeout(100, TimeUnit.MILLISECONDS);
        config.setRequestTimeout(5000, TimeUnit.MILLISECONDS);

        if (config.getUseAsync()) {
            createKVStore();
            store.close();
        } else {
            checkException(this::createKVStore,
                           IllegalArgumentException.class, "read timeout");
        }

        /* Test specifying a shorter request timeout explicitly */
        config.setSocketReadTimeout(10000, TimeUnit.MILLISECONDS);
        config.setRequestTimeout(5000, TimeUnit.MILLISECONDS);
        createKVStore();

        final Key key = Key.createKey("a", "b");

        testShortSocketReadTimeout(() ->
                                   store.get(key, null, 15000,
                                             TimeUnit.MILLISECONDS));

        close();
    }

    private void testShortSocketReadTimeout(TestUtils.Operation op)
        throws Exception
    {
        if (config.getUseAsync()) {
            op.call();
        } else {
            checkException(op, IllegalArgumentException.class, "read timeout");
        }
    }

    /**
     * Test that performing a logout while FINE logging is enabled works
     * successfully -- without getting a NoClassDefFoundError due to a class
     * missing from kvclient.jar. [KVSTORE-1937]
     */
    @Test
    public void testLogoutWithLogging() throws Exception{

        /* This test requires security */
        Assume.assumeTrue(secure);

        open();
        final Logger kvLogger = Logger.getLogger("oracle.kv");
        final Level level = logger.getLevel();
        try {
            kvLogger.setLevel(Level.FINE);
            store.logout();
        } finally {
            kvLogger.setLevel(level);
            close();
        }
    }
}
