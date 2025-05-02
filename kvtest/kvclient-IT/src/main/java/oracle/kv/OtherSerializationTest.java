/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.util.TestUtils.checkFastSerialize;
import static oracle.kv.impl.util.TestUtils.checkSerialize;
import static oracle.kv.impl.util.TestUtils.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.impl.topo.RepNodeId;

import org.junit.Test;

/**
 * Tests serialization of classes other than Key and Version.
 */
public class OtherSerializationTest extends TestBase {

    @Override
    public void setUp() 
        throws Exception {
    }

    @Override
    public void tearDown() 
        throws Exception {
    }

    /**
     * Tests KeyRange.toByteArray, fromByteArray, and writeFastExternal.
     */
    @Test
    public void testKeyRange()
        throws Exception {

        final String[] rangeStrings = { "a", "ab", null, "c", "d" };
        for (int i = 1; i < rangeStrings.length; i += 1) {
            final String key1 = rangeStrings[i - 1];
            final String key2 = rangeStrings[i];

            for (final boolean incl1 : new boolean[] { true, false }) {
                for (final boolean incl2 : new boolean[] { true, false }) {
                    final KeyRange range = new KeyRange
                        (key1, incl1, key2, incl2);
                    final String m = range.toString();

                    final byte[] b = range.toByteArray();
                    final KeyRange range2 = KeyRange.fromByteArray(b);
                    assertEquals(m, range, range2);
                    final byte[] b2 = range2.toByteArray();
                    assertTrue(m, Arrays.equals(b, b2));
                    checkFastSerialize(range);
                }
            }
        }
    }

    /**
     * Tests Consistency.toByteArray, fromByteArray, writeFastExternal, and
     * standard serialization.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testConsistency()
        throws Exception {

        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;
        final long lsn = 0x1234567812345678L;
        final RepNodeId repNodeId = new RepNodeId(1234, 1234);
        final Version version = new Version(uuid, vlsn, repNodeId, lsn);

        for (final Consistency c1 : new Consistency[] {
             Consistency.NONE_REQUIRED, Consistency.NONE_REQUIRED_NO_MASTER,
             Consistency.ABSOLUTE,
             new Consistency.Version(version, 123L, TimeUnit.MILLISECONDS),
             new Consistency.Time(456L, TimeUnit.MILLISECONDS,
                                  789L, TimeUnit.MILLISECONDS)}) {

            final String m = c1.toString();
            final byte[] b = c1.toByteArray();
            final Consistency c2 = Consistency.fromByteArray(b);
            assertEquals(c1, c2);
            final byte[] b2 = c2.toByteArray();
            assertTrue(m, Arrays.equals(b, b2));
            checkSerialize(c1);
            checkFastSerialize(c1, Consistency::readFastExternal);
        }
    }

    /**
     * Tests Durability.toByteArray, fromByteArray, writeFastExternal, and
     * standard serialization.
     */
    @Test
    public void testDurability()
        throws Exception {

        for (final Durability d1 : new Durability[] {
             Durability.COMMIT_SYNC, Durability.COMMIT_NO_SYNC,
             Durability.COMMIT_WRITE_NO_SYNC,
             new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC,
                            ReplicaAckPolicy.ALL),
             new Durability(SyncPolicy.NO_SYNC, SyncPolicy.NO_SYNC,
                            ReplicaAckPolicy.NONE)
            }) {

            final String m = d1.toString();
            final byte[] b = d1.toByteArray();
            final Durability d2 = Durability.fromByteArray(b);
            assertEquals(d1, d2);
            final byte[] b2 = d2.toByteArray();
            assertTrue(m, Arrays.equals(b, b2));
            checkSerialize(d1);
            checkFastSerialize(d1);
        }
    }

    /** Tests serialization of RequestLimitConfig. */
    @Test
    public void testRequestLimitConfig()
        throws Exception {

        final RequestLimitConfig config = new RequestLimitConfig(3, 2, 1);
        final RequestLimitConfig config2 = serialize(config);

        /* RequestLimitConfig.equals is an identity check, so check fields */
        assertEqualRequestLimitConfigs(config, config2);
    }

    private static void assertEqualRequestLimitConfigs(
        final RequestLimitConfig expected,
        final RequestLimitConfig found) {
        assertEquals(expected.getMaxActiveRequests(),
                     found.getMaxActiveRequests());
        assertEquals(expected.getRequestThreshold(),
                     found.getRequestThreshold());
        assertEquals(expected.getNodeLimitPercent(),
                     found.getNodeLimitPercent());
        assertEquals(expected.getNodeLimit(), found.getNodeLimit());
    }

    /** Tests serialization of KVStoreConfig. */
    @Test
    public void testKVStoreConfig()
        throws Exception {

        final Properties securityProps = new Properties();
        securityProps.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY,
                                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        final KVStoreConfig config  =
            new KVStoreConfig("mystore", "host1:5000", "host2:6000")
            .setSocketOpenTimeout(100, MILLISECONDS)
            .setSocketReadTimeout(101, MILLISECONDS)
            .setRequestTimeout(102, MILLISECONDS)
            .setConsistency(Consistency.ABSOLUTE)
            .setDurability(Durability.COMMIT_WRITE_NO_SYNC)
            .setRequestLimit(new RequestLimitConfig(3, 2, 1))
            .setRegistryOpenTimeout(103, MILLISECONDS)
            .setRegistryReadTimeout(104, MILLISECONDS)
            .setLOBTimeout(105, MILLISECONDS)
            .setLOBSuffix("sfx")
            .setLOBVerificationBytes(106)
            .setLOBChunksPerPartition(107)
            .setLOBChunkSize(108)
            .setReadZones("zone1", "zone2")
            .setSecurityProperties(securityProps);
        final KVStoreConfig config2 = serialize(config);

        /* KVStoreConfig.equals is an identity check, so check fields */
        assertEquals(config.getStoreName(), config2.getStoreName());
        assertArrayEquals(config.getHelperHosts(), config2.getHelperHosts());
        assertEquals(config.getSocketOpenTimeout(MILLISECONDS),
                     config2.getSocketOpenTimeout(MILLISECONDS));
        assertEquals(config.getSocketReadTimeout(MILLISECONDS),
                     config2.getSocketReadTimeout(MILLISECONDS));
        assertEquals(config.getRequestTimeout(MILLISECONDS),
                     config2.getRequestTimeout(MILLISECONDS));
        assertEquals(config.getConsistency(), config2.getConsistency());
        assertEquals(config.getDurability(), config2.getDurability());

        /* RequestLimitConfig.equals is an identity check, so check fields */
        assertEqualRequestLimitConfigs(config.getRequestLimit(),
                                       config2.getRequestLimit());

        assertEquals(config.getRegistryOpenTimeout(MILLISECONDS),
                     config2.getRegistryOpenTimeout(MILLISECONDS));
        assertEquals(config.getRegistryReadTimeout(MILLISECONDS),
                     config2.getRegistryReadTimeout(MILLISECONDS));
        assertEquals(config.getLOBTimeout(MILLISECONDS),
                     config2.getLOBTimeout(MILLISECONDS));
        assertEquals(config.getLOBSuffix(), config2.getLOBSuffix());
        assertEquals(config.getLOBVerificationBytes(),
                     config2.getLOBVerificationBytes());
        assertEquals(config.getLOBChunksPerPartition(),
                     config2.getLOBChunksPerPartition());
        assertEquals(config.getLOBChunkSize(),
                     config2.getLOBChunkSize());
        assertArrayEquals(config.getReadZones(),
                          config2.getReadZones());
        assertEquals(config.getSecurityProperties(),
                     config2.getSecurityProperties());
    }
}
