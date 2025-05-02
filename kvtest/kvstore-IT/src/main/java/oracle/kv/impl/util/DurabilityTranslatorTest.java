/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import oracle.kv.Durability;
import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests oracle.kv.impl.util.DurabilityTranslator.
 */
public class DurabilityTranslatorTest extends TestBase {

    /* The 1:1 mappings between JE and KV Sync policies */
    private static final SyncPair[] syncPolicies = new SyncPair[] {
        new SyncPair(Durability.SyncPolicy.SYNC,
                     com.sleepycat.je.Durability.SyncPolicy.SYNC),
        new SyncPair(Durability.SyncPolicy.NO_SYNC,
                     com.sleepycat.je.Durability.SyncPolicy.NO_SYNC),
        new SyncPair(Durability.SyncPolicy.WRITE_NO_SYNC,
                     com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC),
        new SyncPair(null, null)
    };

    /* The 1:1 mappings between JE and KV Replica Ack policies */
    private static final AckPair[] ackPolicies = new AckPair[] {
        new AckPair(
            Durability.ReplicaAckPolicy.ALL,
            com.sleepycat.je.Durability.ReplicaAckPolicy.ALL),
        new AckPair(
            Durability.ReplicaAckPolicy.NONE,
            com.sleepycat.je.Durability.ReplicaAckPolicy.NONE),
        new AckPair(
            Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
            com.sleepycat.je.Durability.ReplicaAckPolicy.SIMPLE_MAJORITY),
        new AckPair(null, null)
    };

    /* Test the translation of the standard KV Durabilities into JE */
    @Test
    public void testTranslateDurabilityKvStdToJe() {

        /* Test translation for Durability.COMMIT_SYNC */
        final com.sleepycat.je.Durability jeCommitSync =
            DurabilityTranslator.translate(Durability.COMMIT_SYNC);
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.SYNC,
                     jeCommitSync.getLocalSync());
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.NO_SYNC,
                     jeCommitSync.getReplicaSync());
        assertEquals(
            com.sleepycat.je.Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
            jeCommitSync.getReplicaAck());

        /* Test translation for Durability.COMMIT_NO_SYNC */
        final com.sleepycat.je.Durability jeCommitNoSync =
            DurabilityTranslator.translate(Durability.COMMIT_NO_SYNC);
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.NO_SYNC,
                     jeCommitNoSync.getLocalSync());
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.NO_SYNC,
                     jeCommitNoSync.getReplicaSync());
        assertEquals(
            com.sleepycat.je.Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
            jeCommitNoSync.getReplicaAck());

        /* Test translation for Durability.COMMIT_WRITE_NO_SYNC */
        final com.sleepycat.je.Durability jeCommitWriteNoSync =
            DurabilityTranslator.translate(Durability.COMMIT_WRITE_NO_SYNC);
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC,
                     jeCommitWriteNoSync.getLocalSync());
        assertEquals(com.sleepycat.je.Durability.SyncPolicy.NO_SYNC,
                     jeCommitWriteNoSync.getReplicaSync());
        assertEquals(
            com.sleepycat.je.Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
            jeCommitWriteNoSync.getReplicaAck());
    }

    /* Test the translation of the standard JE Durabilities into KV */
    @Test
    public void testTranslateDurabilityJeStdToKv() {

        /* Test translation for Durability.COMMIT_SYNC */
        final Durability kvCommitSync = DurabilityTranslator.translate(
            com.sleepycat.je.Durability.COMMIT_SYNC);
        assertEquals(Durability.SyncPolicy.SYNC,
                     kvCommitSync.getMasterSync());
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvCommitSync.getReplicaSync());
        assertEquals(Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
                     kvCommitSync.getReplicaAck());

        /* Test translation for Durability.COMMIT_NO_SYNC */
        final Durability kvCommitNoSync = DurabilityTranslator.translate(
            com.sleepycat.je.Durability.COMMIT_NO_SYNC);
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvCommitNoSync.getMasterSync());
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvCommitNoSync.getReplicaSync());
        assertEquals(Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
                     kvCommitNoSync.getReplicaAck());

        /* Test translation for Durability.COMMIT_WRITE_NO_SYNC */
        final Durability kvCommitWriteNoSync = DurabilityTranslator.translate(
            com.sleepycat.je.Durability.COMMIT_WRITE_NO_SYNC);
        assertEquals(Durability.SyncPolicy.WRITE_NO_SYNC,
                     kvCommitWriteNoSync.getMasterSync());
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvCommitWriteNoSync.getReplicaSync());
        assertEquals(Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
                     kvCommitWriteNoSync.getReplicaAck());

        /* Test translation for Durability.READ_ONLY_TXN. READ_ONLY_TXN is
         * deprecated, but keep the test case here as long as we support
         * READ_ONLY_TXN 
         */
        @SuppressWarnings("deprecation")
		final Durability kvReadOnlyTxn = DurabilityTranslator.translate(
            com.sleepycat.je.Durability.READ_ONLY_TXN);
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvReadOnlyTxn.getMasterSync());
        assertEquals(Durability.SyncPolicy.NO_SYNC,
                     kvReadOnlyTxn.getReplicaSync());
        assertEquals(Durability.ReplicaAckPolicy.NONE,
                     kvReadOnlyTxn.getReplicaAck());
    }

    /* Test all possible permutations of kv Durability <-> je Durability */
    @Test
    public void testTranslateCustomDurability() {

        for (SyncPair mSync : syncPolicies) {
            final Durability.SyncPolicy kvMasterSync = mSync.getKvSync();
            final com.sleepycat.je.Durability.SyncPolicy jeLocalSync =
                mSync.getJeSync();

            for (SyncPair rSync : syncPolicies) {
                final Durability.SyncPolicy kvReplicaSync = rSync.getKvSync();
                final com.sleepycat.je.Durability.SyncPolicy jeReplicaSync =
                    rSync.getJeSync();

                for (AckPair ack : ackPolicies) {
                    final Durability.ReplicaAckPolicy kvReplicaAck =
                        ack.getKvAck();
                    final com.sleepycat.je.Durability.ReplicaAckPolicy
                        jeReplicaAck = ack.getJeAck();

                    final boolean nullParam =
                        (kvMasterSync == null) ||
                        (kvReplicaSync == null) ||
                        (kvReplicaAck == null);
                    final String nullParamDesc =
                        "Durability constructor should fail only if a" +
                        " parameter is null:" +
                        " kvMasterSync:" + kvMasterSync +
                        " kvReplicaSync:" + kvReplicaSync +
                        " kvReplicaAck:" + kvReplicaAck;
                    final com.sleepycat.je.Durability jeDurability;
                    try {
                        jeDurability = DurabilityTranslator.translate(
                            new Durability(
                                kvMasterSync, kvReplicaSync, kvReplicaAck));
                        assertFalse(nullParamDesc, nullParam);
                    } catch (IllegalArgumentException e) {
                        assertTrue(nullParamDesc, nullParam);
                        continue;
                    }

                    assertEquals(jeLocalSync, jeDurability.getLocalSync());
                    assertEquals(jeReplicaSync, jeDurability.getReplicaSync());
                    assertEquals(jeReplicaAck, jeDurability.getReplicaAck());

                    final Durability kvDurability =
                        DurabilityTranslator.translate(
                            new com.sleepycat.je.Durability(
                                jeLocalSync, jeReplicaSync, jeReplicaAck));

                    assertEquals(kvMasterSync, kvDurability.getMasterSync());
                    assertEquals(kvReplicaSync, kvDurability.getReplicaSync());
                    assertEquals(kvReplicaAck, kvDurability.getReplicaAck());
                }
            }
        }
    }

    /* Test mappings of kv sync policy <-> je sync policy */
    @Test
    public void testTranslateSyncPolicy() {

        for (SyncPair sync : syncPolicies) {
            final Durability.SyncPolicy kvSync = sync.getKvSync();
            final com.sleepycat.je.Durability.SyncPolicy jeSync =
                sync.getJeSync();

            final com.sleepycat.je.Durability.SyncPolicy jeKvSync =
                DurabilityTranslator.translate(kvSync);
            assertEquals(jeSync, jeKvSync);

            final Durability.SyncPolicy kvJeSync =
                DurabilityTranslator.translate(jeSync);
            assertEquals(kvSync, kvJeSync);
        }
    }

    /* Test mappings of kv replica ack policy <-> je replica ackpolicy */
    @Test
    public void testTranslateReplicaAckPolicy() {

        for (AckPair ack : ackPolicies) {
            final Durability.ReplicaAckPolicy kvAck = ack.getKvAck();
            final com.sleepycat.je.Durability.ReplicaAckPolicy jeAck =
                ack.getJeAck();

            final com.sleepycat.je.Durability.ReplicaAckPolicy jeKvAck =
                DurabilityTranslator.translate(kvAck);
            assertEquals(jeAck, jeKvAck);

            final Durability.ReplicaAckPolicy kvJeAck =
                DurabilityTranslator.translate(jeAck);
            assertEquals(kvAck, kvJeAck);
        }
    }

    /*
     * Support classes
     */

    /* Mapping between JE SyncPolicy and KV SyncPolicy */
    private static class SyncPair {
        private final Durability.SyncPolicy kvSync;
        private final com.sleepycat.je.Durability.SyncPolicy jeSync;

        public SyncPair(Durability.SyncPolicy kvSync,
                        com.sleepycat.je.Durability.SyncPolicy jeSync) {
            this.kvSync = kvSync;
            this.jeSync = jeSync;
        }

        Durability.SyncPolicy getKvSync() {
            return kvSync;
        }

        com.sleepycat.je.Durability.SyncPolicy getJeSync() {
            return jeSync;
        }
    }

    /* Mapping between JE ReplicaAckPolicy and KV ReplicaAckPolicy */
    private static class AckPair {
        private final Durability.ReplicaAckPolicy kvAck;
        private final com.sleepycat.je.Durability.ReplicaAckPolicy jeAck;

        public AckPair(Durability.ReplicaAckPolicy kvAck,
                       com.sleepycat.je.Durability.ReplicaAckPolicy jeAck) {

            this.kvAck = kvAck;
            this.jeAck = jeAck;
        }

        Durability.ReplicaAckPolicy getKvAck() {
            return kvAck;
        }

        com.sleepycat.je.Durability.ReplicaAckPolicy getJeAck() {
            return jeAck;
        }
    }
}
