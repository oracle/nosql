/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.SerialVersion.CURRENT;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import oracle.kv.table.TimeToLive;

import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link InternalOperation}.
 */
public class InternalOperationTest extends ClientTestBase {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final DataOutput out = new DataOutputStream(baos);

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        baos.reset();
    }

    @Test
    public void testTimeToLive()
        throws IOException {

        TimeToLive ttl = TimeToLive.ofDays(0);
        checkTTL(ttl);

        baos.reset();
        InternalOperation.writeTimeToLive(out, CURRENT, 0, null, "test");
        DataInput in = input(baos);
        assertEquals(ttl, TimeToLive.readFastExternal(in, CURRENT));
        assertEOF(in);

        checkTTL(TimeToLive.ofDays(42));
        checkTTL(TimeToLive.ofHours(0));
        checkTTL(TimeToLive.ofHours(33));

        baos.reset();
        try {
            InternalOperation.writeTimeToLive(out, CURRENT, 3, null, "test");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        try {
            InternalOperation.writeTimeToLive(
                out, CURRENT, 3, TimeUnit.MILLISECONDS, "test");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        out.writeInt(42);
        out.writeByte(4);
        in = input(baos);
        try {
            TimeToLive.readFastExternal(in, CURRENT);
            fail("Expected IOException");
        } catch (IOException e) {
        }
    }

    private void checkTTL(TimeToLive ttl)
        throws IOException {

        baos.reset();
        InternalOperation.writeTimeToLive(out, CURRENT, ttl, "test");
        DataInput in = input(baos);
        assertEquals(ttl, TimeToLive.readFastExternal(in, CURRENT));
        assertEOF(in);
    }

    private static DataInput input(ByteArrayOutputStream baos) {
        return new DataInputStream(
            new ByteArrayInputStream(baos.toByteArray()));
    }

    private static void assertEOF(DataInput in) throws IOException {
        try {
            in.readByte();
            fail("Expected EOFException");
        } catch (EOFException e) {
        }
    }
}
