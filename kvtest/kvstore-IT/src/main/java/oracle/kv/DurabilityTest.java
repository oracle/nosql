/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

import org.junit.Test;

/** Tests for {@link Durability}. */
public class DurabilityTest extends TestBase {

    /**
     * Test that the Durability constructor fails with null arguments. [#25827]
     */
    @SuppressWarnings("unused")
    @Test
    public void testConstructorNull() {
        try {
            new Durability(null, SyncPolicy.SYNC, ReplicaAckPolicy.ALL);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new Durability(SyncPolicy.SYNC, null, ReplicaAckPolicy.ALL);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }

    /**
     * Test that deserializing an old Durability instance created with null
     * arguments fails. [#25827]
     */
    @Test
    public void testDeserializeNull()
        throws Exception {

        final Durability d1;
        final Durability d2;
        final Durability d3;
        try {
            Durability.disableConstructorNullChecks = true;
            d1 = new Durability(null, SyncPolicy.SYNC, ReplicaAckPolicy.ALL);
            d2 = new Durability(SyncPolicy.SYNC, null, ReplicaAckPolicy.ALL);
            d3 = new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC, null);
        } finally {
            Durability.disableConstructorNullChecks = false;
        }

        checkDeserializeFails(d1);
        checkDeserializeFails(d2);
        checkDeserializeFails(d3);
    }

    private void checkDeserializeFails(Durability d)
        throws Exception {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(d);
        } finally {
            oos.close();
        }
        final ByteArrayInputStream bais =
            new ByteArrayInputStream(baos.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            ois.readObject();
            fail("Expected IOException");
        } catch (IOException e) {
        } finally {
            ois.close();
        }
    }
}
