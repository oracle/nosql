/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api;

import static oracle.kv.Consistency.ABSOLUTE;
import static oracle.kv.Consistency.NONE_REQUIRED_NO_MASTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/**
 * Test read zones in RequestDispatcher.  This is a separate test so that each
 * test method can control the KVStore configuration.
 */
public class RequestDispatcherReadZonesTest extends RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;

    private RepNodeId rg1rn1Id;
    private Key key;
    private byte[] keyBytes;
    private PartitionId partitionId;

    /**
     * Individual tests should call methodSetUp after setting KVConfig
     * parameters.
     */
    @Override
    public void setUp()
        throws Exception {

        nDC = 3;
        nSN = 3;
        repFactor = 1;
        nShards = 3;
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        Request.setTestSerialVersion((short) 0);
    }

    /**
     * Set up an individual test
     */
    private void methodSetUp()
        throws Exception {

        super.setUp();
        final RepNode rn = config.getRNs().get(0);
        rg1rn1Id = rn.getRepNodeId();
        final KeyGenerator keygen = new KeyGenerator(rn);
        key = keygen.getKeys(1)[0];
        keyBytes = key.toByteArray();
        partitionId = dispatcher.getPartitionId(keyBytes);
    }

    /* Tests */

    /**
     * Test miscellaneous operations with reads restricted to replicas in a
     * primary zone.
     */
    @Test
    public void testPrimaryReplicas()
        throws Exception {

        readZones = new String[] { "DC1" };
        methodSetUp();

        tryReplicas("DC1");
    }

    /**
     * Test miscellaneous operations with reads restricted to replicas in a
     * secondary zone.
     */
    @Test
    public void testSecondaryReplicas()
        throws Exception {

        nSecondaryZones = 1;
        readZones = new String[] { "DC2" };
        methodSetUp();

        tryReplicas("DC2");
    }

    /** Test miscellaneous read operations on replicas in the specified zone. */
    @SuppressWarnings("deprecation")
    private void tryReplicas(final String zone)
        throws Exception {

        try {
            executeAndReturnZone(
                new Request(
                    new Get(keyBytes),
                    partitionId, false, null, ABSOLUTE, 2, seqNum,
                    clientId, timeoutMs, dispatcher.getReadZoneIds()));
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException e) {
            logger.info("Got expected exception for read ABSOLUTE" +
                        " with no master: " + e +
                        "\nCause: " + e.getCause());
        }

        assertEquals("Read with no consistency from requested read zone",
                     zone,
                     executeAndReturnZone(
                         new Request(
                             new Get(keyBytes),
                             partitionId, false, null, consistencyNone, 2,
                             seqNum, clientId, timeoutMs,
                             dispatcher.getReadZoneIds())));

        assertEquals("Read with no consistency no master from requested" +
                     " read zone",
                     zone,
                     executeAndReturnZone(
                         new Request(
                             new Get(keyBytes),
                             partitionId, false, null, NONE_REQUIRED_NO_MASTER,
                             2, seqNum, clientId, timeoutMs,
                             dispatcher.getReadZoneIds())));

        final RepNodeId rnIdDifferentShard =
            new RepNodeId((rg1rn1Id.getGroupId() + 1) % nSN, 1);
        final RepNodeId respondingRn = executeAndReturnRespondingRN(
            new Request(new Get(keyBytes),
                        partitionId, false, null, consistencyNone, 2,
                        seqNum, clientId, timeoutMs,
                        dispatcher.getReadZoneIds()),
            rnIdDifferentShard);
        assertEquals("Forwarding from another shard",
                     zone, getZoneName(respondingRn));
        assertEquals("Forwarded to correct shard",
                     rg1rn1Id.getGroupId(), respondingRn.getGroupId());
    }

    /**
     * Test read operations with absolute consistency when the read zones
     * contain the master.
     */
    @Test
    public void testAbsoluteWithMaster()
        throws Exception {

        nDC = 2;
        nSN = 2;
        nShards = 1;
        readZones = new String[] { "DC0" };
        methodSetUp();

        assertEquals("Read with absolute consistency from master",
                     "DC0",
                     executeAndReturnZone(
                         new Request(
                             new Get(keyBytes),
                             partitionId, false, null, ABSOLUTE, 2, seqNum,
                             clientId, timeoutMs,
                             dispatcher.getReadZoneIds())));
    }

    /**
     * Test specifying an unknown zone.
     */
    @Test
    public void testUnknownZone()
        throws Exception {

        readZones = new String[] { "DC1", "DC2", "Unknown Zone" };
        try {
            super.setUp();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Read zones not found: [Unknown Zone]",
                         e.getMessage());
        }
    }

    /**
     * Test operations with reads restricted to multiple zones.
     */
    @Test
    public void testMultiple()
        throws Exception {

        nSecondaryZones = 1;
        readZones = new String[] { "DC1", "DC2" };
        methodSetUp();

        boolean zone1 = false;
        boolean zone2 = false;
        final Topology topo = config.getTopology();
        for (int i = 0; i < 2; i++) {
            final RepNodeId responseRnId = executeAndReturnRespondingRN(
                new Request(new Get(keyBytes),
                            partitionId, false, null, consistencyNone, 2,
                            seqNum, clientId, timeoutMs,
                            dispatcher.getReadZoneIds()),
                null);
            final String zone = topo.getDatacenter(responseRnId).getName();
            if ("DC1".equals(zone)) {
                zone1 = true;
            } else if ("DC2".equals(zone)) {
                zone2 = true;
            } else {
                fail("Unexpected zone: " + zone);
            }
            /*
             * Have this RN say it is unavailable next time it is called, so
             * that another RN is chosen
             */
            config.getRH(responseRnId).setTestHook(new TestHook<Request>() {
                    @Override
                    public void doHook(Request r) {
                        throw new RNUnavailableException("Disabled by test");
                    }
                });
        }
        if (!zone1 || !zone2) {
            fail("Didn't get both zone1 and zone2");
        }
    }

    /**
     * Test that a read call fails if no RNs in the specified zones are
     * available.
     */
    @Test
    public void testNoneAvailable()
        throws Exception {

        nDC = 2;
        nSN = 2;
        nShards = 2;
        readZones = new String[] { "DC1" };
        methodSetUp();

        final List<RequestHandlerImpl> rhs = config.getRHs();
        final RequestHandlerImpl rh1 = rhs.get(1);
        rh1.setTestHook(new TestHook<Request>() {
                @Override
                public void doHook(Request r) {
                    throw new RNUnavailableException("Disabled by test");
                }
            });
        try {
            dispatcher.execute(
                new Request(new Get(keyBytes),
                            partitionId, false, null, consistencyNone, 2,
                            seqNum, clientId, timeoutMs,
                            dispatcher.getReadZoneIds()),
                LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException e) {
            logger.info("Got expected exception for no RNs: " + e +
                        "\nCause: " + e.getCause());
        }
    }

    /**
     * Test that a request from a client that does not understand restricted
     * read zones is no longer understood by RNs since that version is no
     * longer supported.
     */
    @Test
    public void testOlderClient()
        throws Exception {

        methodSetUp();
        
        final short unsupported = (short)(SerialVersion.MINIMUM - 1);
        
        /*
         * Set the client to the older serial version and make sure the RN
         * respects it.
         */
        Request.setTestSerialVersion(unsupported);
        try {
            dispatcher.execute(
                new Request(
                    new Get(keyBytes),
                    partitionId, false, null, consistencyNone, 2,
                    seqNum, clientId, timeoutMs,
                    null),
                LOGIN_MGR);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            logger.info("Got expected exception for older client: " + e);
        }
    }

    /* Utilities */

    private String executeAndReturnZone(final Request request)
        throws Exception {

        return executeAndReturnZone(request, null);
    }

    private String executeAndReturnZone(final Request request,
                                        final RepNodeId targetRN)
        throws Exception {

        return getZoneName(executeAndReturnRespondingRN(request, targetRN));
    }

    private String getZoneName(final RepNodeId rnId) {
        final Topology topo = config.getTopology();
        final Datacenter zone = topo.getDatacenter(rnId);
        return zone.getName();
    }

    private RepNodeId executeAndReturnRespondingRN(final Request request,
                                                   final RepNodeId targetRN)
        throws Exception {

        return dispatcher.execute(request, targetRN, LOGIN_MGR).
            getRespondingRN();
    }
}
