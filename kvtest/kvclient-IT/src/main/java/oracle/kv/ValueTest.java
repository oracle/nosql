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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import oracle.kv.impl.api.table.Region;
import org.junit.Test;

import oracle.kv.impl.util.SerialVersion;

/**
 * Tests Value class in isolation.
 */
public class ValueTest extends TestBase {

    private static String RMD = "{\"rm\":1}";

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

        /* Format.AVR, deprecated not symmetrically serializable */

        /* With Format.TABLE, any byte array is allowed. */
        doSerializationTest(Value.Format.TABLE, new byte[0]);
        doSerializationTest(Value.Format.TABLE, new byte[] { 1 });
        doSerializationTest(Value.Format.TABLE, new byte[] { 1, 2 });
        doSerializationTest(Value.Format.TABLE, new byte[] { 1, 2, 3 });

        /* With Format.TABLE_V1, any byte array is allowed. */
        doSerializationTest(Value.Format.TABLE_V1, new byte[0]);
        doSerializationTest(Value.Format.TABLE_V1, new byte[] { 1 });
        doSerializationTest(Value.Format.TABLE_V1, new byte[] { 1, 2 });
        doSerializationTest(Value.Format.TABLE_V1, new byte[] { 1, 2, 3 });

        /* With Format.MULTI_REGION_TABLE, any byte array is allowed. */
        doSerializationTest(Value.Format.MULTI_REGION_TABLE, new byte[0], Region.REGION_ID_START);
        doSerializationTest(Value.Format.MULTI_REGION_TABLE, new byte[] { 1 }, Region.LOCAL_REGION_ID);
        doSerializationTest(Value.Format.MULTI_REGION_TABLE, new byte[] { 1, 2 }, Region.LOCAL_REGION_ID);
        doSerializationTest(Value.Format.MULTI_REGION_TABLE, new byte[] { 1, 2, 3 }, Region.REGION_ID_START);

        /* With Format.TABLE_V5, any byte array is allowed. */
        // with present regionId and metadata string
        doSerializationTest(Value.Format.TABLE_V5, new byte[0], Region.LOCAL_REGION_ID, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1 }, Region.LOCAL_REGION_ID, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2 }, Region.REGION_ID_START, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2, 3 }, Region.LOCAL_REGION_ID, RMD);
        // without regionId but with metadata string
        doSerializationTest(Value.Format.TABLE_V5, new byte[0], Region.NULL_REGION_ID, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1 }, Region.NULL_REGION_ID, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2 }, Region.NULL_REGION_ID, RMD);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2, 3 }, Region.NULL_REGION_ID, RMD);
        // with regionId but without metadata
        doSerializationTest(Value.Format.TABLE_V5, new byte[0], Region.LOCAL_REGION_ID, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1 }, Region.REGION_ID_START, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2 }, Region.LOCAL_REGION_ID, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2, 3 }, 3, null);
        // without either regionId and without metadata
        doSerializationTest(Value.Format.TABLE_V5, new byte[0], Region.NULL_REGION_ID, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1 }, Region.NULL_REGION_ID, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2 }, Region.NULL_REGION_ID, null);
        doSerializationTest(Value.Format.TABLE_V5, new byte[] { 1, 2, 3 }, Region.NULL_REGION_ID, null);
    }

    private void doSerializationTest(Value.Format format, final byte[] val)
        throws IOException {
        doSerializationTest(format, val, Region.NULL_REGION_ID, null);
    }

    private void doSerializationTest(Value.Format format, final byte[] val, int regionId)
        throws IOException {
        doSerializationTest(format, val, regionId, null);
    }

    @SuppressWarnings("deprecation")
    private void doSerializationTest(Value.Format format, final byte[] val,
            int regionId, String metadata)
        throws IOException {

        /* Serialize with toByteArray. */
        final Value v1;
        final int extraLen;
        if (format == Value.Format.NONE) {
            v1 = Value.createValue(val);
            extraLen = 1 /* format */;
        } else if (format == Value.Format.AVRO) {
            v1 = Value.internalCreateValue(val, format, Region.NULL_REGION_ID, null);
            extraLen = 0 /* AVRO includes format */;
        } else if (format == Value.Format.TABLE ||
                format == Value.Format.TABLE_V1) {
            v1 = Value.internalCreateValue(val, format, Region.NULL_REGION_ID, null);
            extraLen = 1 /* format */ ;
        } else if (format == Value.Format.MULTI_REGION_TABLE) {
            v1 = Value.internalCreateValue(val, format, regionId);
            extraLen = 1 /* format*/ + 1 /* regionId small int */;
        } else if (format == Value.Format.TABLE_V5) {
            v1 = Value.internalCreateValue(val, format, regionId, metadata);
            extraLen = 1 /* format */ + 1 /* bitset */ +
                (regionId == Region.NULL_REGION_ID ? 0 : 1 /* regionId small int */) +
                (metadata != null ? 1 /* metadata strLength */ + metadata.length() /* metadata str */ : 0) ;
        } else {
            throw new IllegalArgumentException("Unknown format: " + format);
        }


        assertSame(format, v1.getFormat());
        assertArrayEquals(val, v1.getValue());
        final byte[] bytes = v1.toByteArray();
        assertEquals(val.length + extraLen, bytes.length);
        assertEquals(regionId != Region.NULL_REGION_ID, Value.hasRegionId(bytes));
        assertEquals(metadata != null, Value.hasRowMetadata(bytes));
        assertEquals(regionId, Value.getRegionIdFromByteArray(bytes));
        int offset =
            format == Value.Format.MULTI_REGION_TABLE ? 2 :
            format == Value.Format.TABLE_V5 ? 2 +
                (regionId == Region.NULL_REGION_ID ? 0 : 1 ) +
                (metadata == null ? 0 : 1 + metadata.length()) :
                1
            ;
        assertEquals(offset, Value.getValueOffset(bytes));

        /* Deserialize with fromByteArray. */
        final Value v2 = Value.fromByteArray(bytes);
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertArrayEquals(val, v2.getValue());
        assertSame(format, v2.getFormat());
        assertTrue(v2.getFormat() != Value.Format.MULTI_REGION_TABLE ||
            v2.getRegionId() == regionId);
        assertTrue(v2.getFormat() != Value.Format.TABLE_V5 ||
            v2.getRegionId() == regionId &&
            (metadata == null && v2.getRowMetadata() == null ||
             metadata != null && metadata.equals(v2.getRowMetadata())));
        assertTrue(v2.getRegionId() == Region.NULL_REGION_ID ||
            v2.getRegionId() == regionId);
        assertTrue( (metadata == null && v2.getRowMetadata() == null) ||
            (metadata != null && metadata.equals(v2.getRowMetadata())));

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
        assertTrue(v3.getFormat() != Value.Format.MULTI_REGION_TABLE ||
            v3.getRegionId() == regionId);
        assertTrue(v3.getFormat() != Value.Format.TABLE_V5 ||
            v3.getRegionId() == regionId &&
            (metadata == null && v3.getRowMetadata() == null ||
             metadata != null && metadata.equals(v3.getRowMetadata())));

        assertTrue(v3.getRegionId() == Region.NULL_REGION_ID ||
            v3.getRegionId() == regionId);
        assertEquals(metadata, v3.getRowMetadata());
        assertEquals(regionId != Region.NULL_REGION_ID, Value.hasRegionId(v3.toByteArray()));
        assertEquals(metadata != null, Value.hasRowMetadata(v3.toByteArray()));
        assertEquals(regionId, Value.getRegionIdFromByteArray(v3.toByteArray()));

        /* Deserialize with readFastExternal. */
        bais = new ByteArrayInputStream(v3Serial);
        dis = new DataInputStream(bais);
        byte[] v3Bytes = Value.readFastExternal(dis, SerialVersion.CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, dis.read());
        assertArrayEquals(bytes, v3Bytes);
        assertEquals(regionId != Region.NULL_REGION_ID, Value.hasRegionId(v3Bytes));
        assertEquals(metadata != null, Value.hasRowMetadata(v3Bytes));
        assertEquals(regionId, Value.getRegionIdFromByteArray(v3Bytes));
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
                SerialVersion.MINIMUM, 0xcd1b552a8341734aL),
            serialVersionChecker(
                /* Format: TABLE_V5 */
                Value.fromByteArray(new byte[] {4, 3, 1, 8, 123, 34, 114, 109, 34, 58, 49, 125, 1, 2, 3}),
                SerialVersion.ROW_METADATA_VERSION,0xe7a099d979a1edcaL),
            serialVersionChecker(
                /* Format: TABLE_V5 */
                Value.fromByteArray(new byte[] {4, 2, 8, 123, 34, 114, 109, 34, 58, 49, 125, 1, 2, 3}),
                SerialVersion.ROW_METADATA_VERSION,0xd4155254ce137292L)
        );
    }

    @Test
    public void testValueOffset() {
        Value value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.TABLE_V5, Region.LOCAL_REGION_ID, RMD);

        byte[] serialized = value.toByteArray();
        int offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);

        value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.TABLE_V5, Region.LOCAL_REGION_ID, null);
        serialized = value.toByteArray();
        offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);

        value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.TABLE_V5, Region.NULL_REGION_ID, RMD);
        serialized = value.toByteArray();
        offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);

        value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.TABLE_V5, Region.NULL_REGION_ID, null);
        serialized = value.toByteArray();
        offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);

        value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.MULTI_REGION_TABLE, 1, null);
        serialized = value.toByteArray();
        offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);

        value = Value.internalCreateValue(new byte[] {1,2,3}, Value.Format.TABLE_V1, Region.NULL_REGION_ID);
        serialized = value.toByteArray();
        offset = Value.getValueOffset(serialized);
        assertTrue(offset >= 0);
        assertEquals(serialized.length - 3, offset);
    }
}
