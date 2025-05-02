/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import oracle.kv.impl.util.SerialVersion;

/**
 * Tests Value class in isolation.
 */
public class ValueTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    /**
     * Tests Value.toByteArray and fromByteArray.
     */
    @Test
    public void testSerialization()
        throws IOException {

        /* With Format.NONE, any byte array is allowed. */
        doSerializationTest(Value.Format.NONE, new byte[0]);
        doSerializationTest(Value.Format.NONE, new byte[] { 1 });
        doSerializationTest(Value.Format.NONE, new byte[] { 1, 2 });
        doSerializationTest(Value.Format.NONE, new byte[] { 1, 2, 3 });
    }

    private void doSerializationTest(Value.Format format, final byte[] val)
        throws IOException {

        /* Serialize with toByteArray. */
        final Value v1;
        final int extraLen;
        if (format == Value.Format.NONE) {
            v1 = Value.createValue(val);
            extraLen = 1;
        } else {
            v1 = Value.internalCreateValue(val, format);
            extraLen = 0;
        }
        assertSame(format, v1.getFormat());
        assertArrayEquals(val, v1.getValue());
        final byte[] bytes = v1.toByteArray();
        assertEquals(val.length + extraLen, bytes.length);

        /* Deserialize with fromByteArray. */
        final Value v2 = Value.fromByteArray(bytes);
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertArrayEquals(val, v2.getValue());
        assertSame(format, v2.getFormat());

        /* Serialize with writeFastExternal. */
        ByteArrayOutputStream baos = new ByteArrayOutputStream(50);
        DataOutputStream dos = new DataOutputStream(baos);
        v1.writeFastExternal(dos, SerialVersion.CURRENT);
        dos.close();
        final byte[] v3Serial = baos.toByteArray();

        /* Serialize with static writeFastExternal(..., byte[]). */
        baos = new ByteArrayOutputStream(50);
        dos = new DataOutputStream(baos);
        Value.writeFastExternal(dos, SerialVersion.CURRENT, bytes);
        dos.close();
        final byte[] v3SerialCheck = baos.toByteArray();
        assertArrayEquals(v3Serial, v3SerialCheck);

        /* Deserialize with FastExternalizable constructor. */
        ByteArrayInputStream bais = new ByteArrayInputStream(v3Serial);
        DataInputStream dis = new DataInputStream(bais);
        final Value v3 = new Value(dis, SerialVersion.CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, dis.read());
        assertEquals(v1, v3);
        assertEquals(v1.hashCode(), v3.hashCode());
        assertArrayEquals(val, v3.getValue());
        assertSame(format, v3.getFormat());

        /* Deserialize with readFastExternal. */
        bais = new ByteArrayInputStream(v3Serial);
        dis = new DataInputStream(bais);
        byte[] v3Bytes = Value.readFastExternal(dis, SerialVersion.CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, dis.read());
        assertArrayEquals(bytes, v3Bytes);
    }

    @Test
    public void testSerialVersion() {
        checkAll(
            serialVersionChecker(
                /* Format: NONE */
                Value.fromByteArray(new byte[] { 0, 10, 20 }),
                0x4bf8fd744cac242eL),
            serialVersionChecker(
                /* Format: AVRO */
                Value.fromByteArray(new byte[] { -1, 30, 40 }),
                0x726d837e1267ac48L),
            serialVersionChecker(
                /* Format: TABLE */
                Value.fromByteArray(new byte[] { 1, 50, 60 }),
                0x660c9caac5156950L),
            serialVersionChecker(
                /* Format: TABLE_V1 */
                Value.fromByteArray(new byte[] { 2, 70, 80 }),
                0xc1ea012f1c06d50L),
            serialVersionChecker(
                /* Format: MULTI_REGION_TABLE */
                Value.fromByteArray(new byte[] { 3, 90, 100 }),
                SerialVersion.MINIMUM, 0xcd1b552a8341734aL));
    }
}
