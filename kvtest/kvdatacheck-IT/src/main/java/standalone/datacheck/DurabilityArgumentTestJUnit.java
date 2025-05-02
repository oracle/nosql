/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

import org.junit.Test;

/** Test {@link DurabilityArgument}. */
public class DurabilityArgumentTestJUnit extends JUnitTestBase {

    /* Tests */

    @Test(expected=IllegalArgumentException.class)
    public void testParseNull() {
        DurabilityArgument.parseDurability(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testToStringNull() {
        DurabilityArgument.toString(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParseJunk() {
        DurabilityArgument.parseDurability("FooBar");
    }

    @Test
    public void testCommitNoSync() {
        assertSame(Durability.COMMIT_NO_SYNC,
                   DurabilityArgument.parseDurability("COMMIT_NO_SYNC"));
        assertSame(Durability.COMMIT_NO_SYNC,
                   DurabilityArgument.parseDurability("commit_no_sync"));
        assertEquals("COMMIT_NO_SYNC",
                     DurabilityArgument.toString(Durability.COMMIT_NO_SYNC));
    }

    @Test
    public void testCommitSync() {
        assertSame(Durability.COMMIT_SYNC,
                   DurabilityArgument.parseDurability("COMMIT_SYNC"));
        assertSame(Durability.COMMIT_SYNC,
                   DurabilityArgument.parseDurability("commit_sync"));
        assertEquals("COMMIT_SYNC",
                     DurabilityArgument.toString(Durability.COMMIT_SYNC));
    }

    @Test
    public void testCommitWriteNoSync() {
        assertSame(Durability.COMMIT_WRITE_NO_SYNC,
                   DurabilityArgument.parseDurability("COMMIT_WRITE_NO_SYNC"));
        assertSame(Durability.COMMIT_WRITE_NO_SYNC,
                   DurabilityArgument.parseDurability("commit_write_no_sync"));
        assertEquals
            ("COMMIT_WRITE_NO_SYNC",
             DurabilityArgument.toString(Durability.COMMIT_WRITE_NO_SYNC));
    }

    @Test
    public void testMiscInstances() {
        testMiscInstance(new Durability(SyncPolicy.NO_SYNC,
                                        SyncPolicy.SYNC,
                                        ReplicaAckPolicy.ALL),
                         "masterSync=NO_SYNC" +
                         ",replicaSync=SYNC" +
                         ",replicaAck=ALL");
        testMiscInstance(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                        SyncPolicy.SYNC,
                                        ReplicaAckPolicy.NONE),
                         "masterSync=WRITE_NO_SYNC" +
                         ",replicaSync=SYNC" +
                         ",replicaAck=NONE");
        testMiscInstance(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                        SyncPolicy.SYNC,
                                        ReplicaAckPolicy.SIMPLE_MAJORITY),
                         "masterSync=WRITE_NO_SYNC" +
                         ",replicaSync=SYNC" +
                         ",replicaAck=SIMPLE_MAJORITY");
    }

    @Test
    public void testParseInvalid() {
        try {
            DurabilityArgument.parseDurability("masterSync=NO_SYNC");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
        }
        try {
            DurabilityArgument.parseDurability("masterSync=NO_SYNC," +
                                               ",replicaSync=" +
                                               ",replicaAck=NONE");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
        }
        try {
            DurabilityArgument.parseDurability("masterSync=FooBar," +
                                               ",replicaSync=NO_SYNC" +
                                               ",replicaAck=NONE");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
        }
        try {
            DurabilityArgument.parseDurability("masterSync=NO_SYNC," +
                                               ",replicaSync=NO_SYNC" +
                                               ",replicaAck=FooBar");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
        }
    }

    /* Misc */

    private static void testMiscInstance(Durability durability, String string) {
        assertEquals(durability, DurabilityArgument.parseDurability(string));
        assertEquals(durability,
                     DurabilityArgument.parseDurability(string.toLowerCase()));
        assertEquals(durability,
                     DurabilityArgument.parseDurability(string.toUpperCase()));
        assertEquals(string, DurabilityArgument.toString(durability));
    }
}
