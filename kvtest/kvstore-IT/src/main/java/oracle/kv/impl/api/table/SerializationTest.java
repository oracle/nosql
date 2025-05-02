/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.util.SerialVersion.CURRENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;
import oracle.kv.table.FieldDef.Type;
import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.NioByteInputStream;
import oracle.nosql.nson.util.NioByteOutputStream;
import org.junit.Test;

public class SerializationTest extends TestBase {

    private TableImpl createJsonTable(String name) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addJson("test", null)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createIntegerTable(String name, boolean nullable,
            Integer defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addInteger("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createLongTable(String name, boolean nullable,
            Long defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addLong("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createDoubleTable(String name, boolean nullable,
            Double defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addDouble("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createFloatTable(String name, boolean nullable,
            Float defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addFloat("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createStringTable(String name, boolean nullable,
            String defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addString("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createNumberTable(String name, boolean nullable,
            BigDecimal defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addNumber("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createBooleanTable(String name, boolean nullable,
            Boolean defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addBoolean("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createBinaryTable(String name, boolean nullable,
            byte[] defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addBinary("test", "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createFixedBinaryTable(String name, int size,
            boolean nullable, byte[] defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addFixedBinary("test", size, "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createTimestampTable(String name, int precision,
            boolean nullable, Timestamp defaultValue) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addTimestamp("test", precision, "test", nullable, defaultValue)
                .primaryKey("id").buildTable();
        return table;
    }

    private TableImpl createRecordTable(String name, RecordDefImpl recDef) {
        TableImpl table = TableBuilder.createTableBuilder(name).addInteger("id")
                .addField("test", recDef).primaryKey("id").buildTable();
        return table;
    }

    private void roundTripTest(TableImpl table, int idvalue, FieldValue value,
            FieldValue expectedValue, FieldDef.Type type) {
        RowImpl row = table.createRow();
        row.put("id", idvalue);
        Key key = table.createKey(row, false);
        KeySerializer keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);

        if (value != null && !value.isNull()) {
            row.put("test", value);
        } else if (value != null && value.isNull()){
            row.putNull("test");
        }

        Value val = table.createValue(row);
        assertTrue(val != null);
        byte[] valBytes = val.toByteArray();

        RowImpl row1 = table.createRowFromBytes(keyBytes, valBytes, false);
        assertTrue(row1 != null);
        int id = row1.asRecord().get("id").asInteger().get();
        assertTrue(id == idvalue);
        if (expectedValue != null) {
            FieldValue testValue = null;
            switch (type) {
            case INTEGER:
                testValue = row1.asRecord().get("test").asInteger();
                break;
            case LONG:
                testValue = row1.asRecord().get("test").asLong();
                break;
            case DOUBLE:
                testValue = row1.asRecord().get("test").asDouble();
                break;
            case FLOAT:
                testValue = row1.asRecord().get("test").asFloat();
                break;
            case STRING:
                testValue = row1.asRecord().get("test").asString();
                break;
            case BINARY:
                testValue = row1.asRecord().get("test").asBinary();
                break;
            case FIXED_BINARY:
                testValue = row1.asRecord().get("test").asFixedBinary();
                break;
            case ENUM:
                testValue = row1.asRecord().get("test").asEnum();
                break;
            case ARRAY:
                testValue = row1.asRecord().get("test").asArray();
                break;
            case MAP:
                testValue = row1.asRecord().get("test").asMap();
                break;
            case BOOLEAN:
                testValue = row1.asRecord().get("test").asBoolean();
                break;
            case TIMESTAMP:
                testValue = row1.asRecord().get("test").asTimestamp();
                break;
            case NUMBER:
                testValue = row1.asRecord().get("test").asNumber();
                break;
            case RECORD:
                testValue = row1.asRecord().get("test").asRecord();
                break;
            default:
                throw new IllegalStateException("Unknown type: " + type);
            }
            assertEquals("Expected equal values after deserialization",
                    testValue, expectedValue);
        } else {
            assertTrue(row1.asRecord().get("test").isNull());
        }

        roundTripNson(table, idvalue, row1);
    }

    private void roundTripJson(TableImpl table, int idvalue, String json) {
        RowImpl row = table.createRow();
        row.put("id", idvalue);
        Key key = table.createKey(row, false);
        KeySerializer keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);

        if (json != null) {
            row.putJson("test", json);
        }

        Value val = table.createValue(row);
        assertTrue(val != null);
        byte[] valBytes = val.toByteArray();

        RowImpl row1 = table.createRowFromBytes(keyBytes, valBytes, false);
        assertTrue(row1 != null);
        int id = row1.asRecord().get("id").asInteger().get();
        assertTrue(id == idvalue);
        assertTrue(row1.equals(row));

        roundTripNson(table, idvalue, row);
    }

    /*
     * Round trip NSON:
     * 1. Serializes key and row to AVRO
     * 2. Serializes AVRO to NSON
     * 3. Serializes NSON to AVRO
     * 4. Validates created AVRO with the original AVRO
     * 5. Creates JSON from NSON value
     * 6. Creates row from the JSON
     * 7. Validates created row with the first row
     */
    private void roundTripNson(TableImpl table,
                               int idvalue,
                               RowImpl expectedRow) {
        Key key = table.createKey(expectedRow, false);
        KeySerializer keySerializer =
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);

        Value val = table.createValue(expectedRow);
        assertTrue(val != null);
        byte[] valBytes = val.toByteArray();

        BinaryKeyIterator keyIter = table.createBinaryKeyIterator(keyBytes);
        TableImpl targetTable = table.findTargetTable(keyIter);
        keyIter.reset();

        NioByteOutputStream out = NioByteOutputStream
            .createNioByteOutputStream();
        assertTrue(NsonUtil.createNsonFromKeyBytes(targetTable, keyIter, out));
        byte[] nsonKey = getBytes(out);
        out = NioByteOutputStream.createNioByteOutputStream();
        assertTrue(NsonUtil.createNsonFromValueBytes(
                       targetTable, valBytes, true, out));
        byte[] nsonValue = getBytes(out);

        final PrimaryKeyImpl pk =
            NsonUtil.createPrimaryKeyFromNsonBytes(table, nsonKey);
        int id = pk.get("id").asInteger().get();
        assertEquals(id, idvalue);
        Key key1 = table.createKeyInternal(pk, false);
        assertArrayEquals("Old and new key should be the same",
            key.toByteArray(), key1.toByteArray());
        Value val1 =
            NsonUtil.createValueFromNsonBytes(table, nsonValue, 0, false);
        /* only compare the byte arrays, exclude format and region */
        assertArrayEquals("Old and new Value should be the same",
                          val.getValue(), val1.getValue());

        String json = Nson.toJsonString(
            new NioByteInputStream(ByteBuffer.wrap(nsonValue)));

        RowImpl row1 = (RowImpl)table.createRowFromJson(json, false);
        assertNotNull(row1);
        for (String field : expectedRow.getFieldNames()) {
            if (!table.isKeyComponent(field)) {
                assertEquals(expectedRow.get(field), row1.get(field));
            }
        }
    }

    private static byte[] getBytes(NioByteOutputStream out) {
        ByteBuffer buffer = out.getBuffer();
        buffer.rewind();
        byte[] dst = new byte[out.getOffset()];
        buffer.get(dst);
        return dst;
    }

    private void testNotNullableWithNull(TableImpl table, int id) {
        RowImpl row = table.createRow();
        row.put("id", id);
        try {
            table.createValue(row);
            fail("Error: It should throw IllegalCommandException");
        } catch (IllegalCommandException ice) {
        }
    }

    @Test
    public void testTableTimestamp() {
        String name = "table";
        Timestamp[] values = new Timestamp[] { new Timestamp(0),
                TimestampUtils.createTimestamp(0, 1),
                TimestampUtils.createTimestamp(-1, 999999999),
                new Timestamp(System.currentTimeMillis()),
                new Timestamp(-1 * System.currentTimeMillis()),
                TimestampDefImpl.MIN_VALUE, TimestampDefImpl.MAX_VALUE };
        Timestamp defaultValue = TimestampDefImpl.MIN_VALUE;
        TableImpl table = null;
        // defaultValue != null and nullable
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            name += p;
            table = createTimestampTable(name, p, true, defaultValue);
            TimestampDef def = new TimestampDefImpl(p);
            int id = 0;
            for (Timestamp ts : values) {
                TimestampValue tsv = def.createTimestamp(ts);
                roundTripTest(table, id++, tsv, tsv, FieldDef.Type.TIMESTAMP);
            }
            roundTripTest(table, id++, null, def.createTimestamp(defaultValue),
                    FieldDef.Type.TIMESTAMP);
            roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                    FieldDef.Type.TIMESTAMP);
        }

        // defaultValue = null and nullable
        name = "tableNull";
        int precision = 1;
        int id = 0;
        table = createTimestampTable(name, precision, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.TIMESTAMP);

        // defaultValue != null and not nullable
        name = "table10";
        TimestampDef def = new TimestampDefImpl(precision);
        table = createTimestampTable(name, precision, false, defaultValue);
        roundTripTest(table, id++, null, def.createTimestamp(defaultValue),
                FieldDef.Type.TIMESTAMP);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createTimestampTable(name, precision, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableInteger() {
        String name = "table";
        int[] values = { Integer.MAX_VALUE, Integer.MIN_VALUE, 0, 100, -1000 };

        Integer defaultValue = 1000;
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createIntegerTable(name, true, defaultValue);
        IntegerDefImpl def = new IntegerDefImpl("test");
        int id = 0;
        for (int val : values) {
            IntegerValueImpl dv = def.createInteger(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.INTEGER);
        }
        roundTripTest(table, id++, null, def.createInteger(defaultValue),
                FieldDef.Type.INTEGER);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.INTEGER);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createIntegerTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.INTEGER);

        // defaultValue != null and not nullable
        name = "table10";
        table = createIntegerTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createInteger(defaultValue),
                FieldDef.Type.INTEGER);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createIntegerTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableLong() {
        String name = "table";
        long[] values = { Long.MAX_VALUE, Long.MIN_VALUE, 0L, 100L, -1000L };

        Long defaultValue = Long.valueOf(1000);
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createLongTable(name, true, defaultValue);
        LongDefImpl def = new LongDefImpl("test");
        int id = 0;
        for (long val : values) {
            LongValueImpl dv = def.createLong(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.LONG);
        }
        roundTripTest(table, id++, null, def.createLong(defaultValue),
                FieldDef.Type.LONG);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.LONG);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createLongTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.LONG);

        // defaultValue != null and not nullable
        name = "table10";
        table = createLongTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createLong(defaultValue),
                FieldDef.Type.LONG);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createLongTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableDouble() {
        String name = "table";
        double[] values = { Double.MAX_VALUE, Double.MIN_VALUE, 0.0, 100.00,
                -1000.00 };

        Double defaultValue = Double.valueOf(99.99);
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createDoubleTable(name, true, defaultValue);
        DoubleDefImpl def = new DoubleDefImpl("test");
        int id = 0;
        for (double val : values) {
            DoubleValueImpl dv = def.createDouble(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.DOUBLE);
        }
        roundTripTest(table, id++, null, def.createDouble(defaultValue),
                FieldDef.Type.DOUBLE);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.DOUBLE);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createDoubleTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.DOUBLE);

        // defaultValue != null and not nullable
        name = "table10";
        table = createDoubleTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createDouble(defaultValue),
                FieldDef.Type.DOUBLE);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createDoubleTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableFloat() {
        String name = "table";
        float[] values = { Float.MAX_VALUE, Float.MIN_VALUE, 0.0f, 100.0f,
                -1000.00f };

        Float defaultValue = Float.valueOf(99.99f);
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createFloatTable(name, true, defaultValue);
        FloatDefImpl def = new FloatDefImpl("test");
        int id = 0;
        for (float val : values) {
            FloatValueImpl dv = def.createFloat(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.FLOAT);
        }
        roundTripTest(table, id++, null, def.createFloat(defaultValue),
                FieldDef.Type.FLOAT);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.FLOAT);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createFloatTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.FLOAT);

        // defaultValue != null and not nullable
        name = "table10";
        table = createFloatTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createFloat(defaultValue),
                FieldDef.Type.FLOAT);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createFloatTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableNumber() {
        String name = "table";
        BigDecimal[] values = new BigDecimal[] { BigDecimal.ZERO,
                BigDecimal.ONE, BigDecimal.TEN, BigDecimal.valueOf(-1),
                BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE),
                BigDecimal.valueOf(-1 * Double.MAX_VALUE),
                BigDecimal.valueOf(Double.MAX_VALUE),
                new BigDecimal(
                        "123456789012345678901234567890.1234567890123456789"),
                new BigDecimal(
                        "-123456789012345678901234567890.1234567890123456789"),
                /* this will serialize with array length > a byte value */
                new BigDecimal(
                        "7.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049") };

        BigDecimal defaultValue = BigDecimal.valueOf(Long.MIN_VALUE);
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createNumberTable(name, true, defaultValue);
        NumberDefImpl def = new NumberDefImpl("test");
        int id = 0;
        for (BigDecimal val : values) {
            NumberValueImpl dv = def.createNumber(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.NUMBER);
        }
        roundTripTest(table, id++, null, def.createNumber(defaultValue),
                FieldDef.Type.NUMBER);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.NUMBER);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createNumberTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.NUMBER);

        // defaultValue != null and not nullable
        name = "table10";
        table = createNumberTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createNumber(defaultValue),
                FieldDef.Type.NUMBER);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createNumberTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableString() {
        String name = "table";
        String[] values = { "Abcdef", "OPQRST" };

        String defaultValue = "default";
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createStringTable(name, true, defaultValue);
        StringDefImpl def = new StringDefImpl("test");
        int id = 0;
        for (String val : values) {
            StringValueImpl dv = def.createString(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.STRING);
        }
        roundTripTest(table, id++, null, def.createString(defaultValue),
                FieldDef.Type.STRING);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.STRING);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createStringTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.STRING);

        // defaultValue != null and not nullable
        name = "table10";
        table = createStringTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createString(defaultValue),
                FieldDef.Type.STRING);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createStringTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableBoolean() {
        String name = "table";
        boolean[] values = { true, false };

        boolean defaultValue = true;
        TableImpl table = null;
        // defaultValue != null and nullable
        table = createBooleanTable(name, true, defaultValue);
        BooleanDefImpl def = new BooleanDefImpl("test");
        int id = 0;
        for (boolean val : values) {
            BooleanValueImpl dv = def.createBoolean(val);
            roundTripTest(table, id++, dv, dv, FieldDef.Type.BOOLEAN);
        }
        roundTripTest(table, id++, null, def.createBoolean(defaultValue),
                FieldDef.Type.BOOLEAN);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.BOOLEAN);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createBooleanTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.BOOLEAN);

        // defaultValue != null and not nullable
        name = "table10";
        table = createBooleanTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createBoolean(defaultValue),
                FieldDef.Type.BOOLEAN);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createBooleanTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableBinary() {
        String name = "table";
        String[] values = { "abcdef", "hijklmn" };

        TableImpl table = null;
        String defaultStr = "5ffffeeee";
        byte[] defaultValue = defaultStr.getBytes();
        // defaultValue != null and nullable
        table = createBinaryTable(name, true, defaultValue);
        BinaryDefImpl def = new BinaryDefImpl("test");
        int id = 0;
        for (String val : values) {
            BinaryValueImpl dv = def.createBinary(val.getBytes());
            roundTripTest(table, id++, dv, dv, FieldDef.Type.BINARY);
        }
        roundTripTest(table, id++, null, def.createBinary(defaultValue),
                FieldDef.Type.BINARY);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.BINARY);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createBinaryTable(name, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.BINARY);

        // defaultValue != null and not nullable
        name = "table10";
        table = createBinaryTable(name, false, defaultValue);
        roundTripTest(table, id++, null, def.createBinary(defaultValue),
                FieldDef.Type.BINARY);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createBinaryTable(name, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableFixedBinary() {
        String name = "table";
        String[] values = { "6ffffeeee", "7ffffeeee" };
        int size = 9;

        TableImpl table = null;
        String defaultStr = "5ffffeeee";
        byte[] defaultValue = defaultStr.getBytes();
        // defaultValue != null and nullable
        table = createFixedBinaryTable(name, size, true, defaultValue);
        FixedBinaryDefImpl def = new FixedBinaryDefImpl("test", size);
        int id = 0;
        for (String val : values) {
            FixedBinaryValueImpl dv = def.createFixedBinary(val.getBytes());
            roundTripTest(table, id++, dv, dv, FieldDef.Type.FIXED_BINARY);
        }
        roundTripTest(table, id++, null, def.createFixedBinary(defaultValue),
                FieldDef.Type.FIXED_BINARY);
        roundTripTest(table, id++, NullValueImpl.getInstance(), null,
                FieldDef.Type.FIXED_BINARY);

        // defaultValue = null and nullable
        name = "tableNull";
        id = 0;
        table = createFixedBinaryTable(name, size, true, null);
        roundTripTest(table, id++, null, null, FieldDef.Type.FIXED_BINARY);

        // defaultValue != null and not nullable
        name = "table10";
        table = createFixedBinaryTable(name, size, false, defaultValue);
        roundTripTest(table, id++, null, def.createFixedBinary(defaultValue),
                FieldDef.Type.FIXED_BINARY);

        // defaultValue = null and not nullable
        name = "tableNotNull";
        table = createFixedBinaryTable(name, size, false, null);
        testNotNullableWithNull(table, id++);
    }

    @Test
    public void testTableComplexField() {
        RecordBuilder rb = TableBuilder.createRecordBuilder("rec");
        rb.addInteger("home");
        rb.addInteger("work");

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addField(rb.build());

        RecordDefImpl complex = (RecordDefImpl) TableBuilder
                .createRecordBuilder("wholeRec").addInteger("idx")
                .addField("array", ab.build()).build();

        RecordValue record = complex.createRecord();
        record.put("idx", 678);
        ArrayValue array = record.putArray("array");
        RecordValue rec = array.addRecord();
        rec.put("home", 1);
        rec.put("work", 2);
        rec = array.addRecord();
        rec.put("home", 3456);
        rec.put("work", 5678);

        String name = "table";
        TableImpl table = null;
        table = createRecordTable(name, complex);
        int id = 0;
        roundTripTest(table, id++, record, record, FieldDef.Type.RECORD);
    }

    @Test
    public void testTableComplexField2() {
        RecordDefImpl complex = (RecordDefImpl) TableBuilder
                .createRecordBuilder("rec").addInteger("id")
                /* add string -- not nullable, default value */
                .addString("name", null, false, "joe").addFloat("float")
                .addEnum("days",
                        new String[] { "monday", "wednesday", "friday" }, null,
                        null, null)
                .build();

        RecordValue record = complex.createRecord();
        record.put("id", 678);
        record.put("name", "this is a name");
        record.put("float", 5678.1234F);
        record.putEnum("days", "monday");

        String name = "table";
        TableImpl table = null;
        table = createRecordTable(name, complex);
        int id = 0;
        roundTripTest(table, id++, record, record, FieldDef.Type.RECORD);
    }

    @Test
    public void testTableComplexField3() {
        RecordBuilder rb = TableBuilder.createRecordBuilder("rec");
        rb.addInteger("home");
        rb.addInteger("work");

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addField(rb.build());

        MapBuilder mb = TableBuilder.createMapBuilder("map");
        mb.addFloat("salary");

        RecordDefImpl complex = (RecordDefImpl) TableBuilder
                .createRecordBuilder("wholeRec").addInteger("idx")
                .addField("array", ab.build())
                .addField("map", mb.build()).build();

        RecordValue record = complex.createRecord();
        record.put("idx", 678);
        ArrayValue array = record.putArray("array");
        RecordValue rec = array.addRecord();
        rec.put("home", 1);
        rec.put("work", 2);
        rec = array.addRecord();
        rec.put("home", 3456);
        rec.put("work", 5678);
        MapValue map = record.putMap("map");
        map.put("salary", 100000f);

        String name = "table";
        TableImpl table = null;
        table = createRecordTable(name, complex);
        int id = 0;
        roundTripTest(table, id++, record, record, FieldDef.Type.RECORD);
    }

    @Test
    public void testTableJson() {
        final String json1 = "1";
        final String json2 = "\"a string\"";
        final String json3 = "17.56";
        final String json4 = "{}";
        final String json5 = "[]";
        final String json6 = "{\"a\": 1, \"b\": 2, \"map\":{\"m1\":6}}";
        final String json7 = "[1,2.7,3]";
        final String json8 = "null";
        final String json9 = "{\"a\": 1, \"b\": null, \"bool\": true," +
            "\"map\": {\"m1\": 5}, \"ar\" : [1,2.7,3]}";
        final String json10 = "{\"a\": 9234567890123456}";

        final String[] jsonDocs = {json1, json2, json3, json4, json5, json6,
                                   json7, json8, json9, json10};
        String name = "table";
        TableImpl table = null;

        table = createJsonTable(name);
        int id = 0;

        for (String json: jsonDocs) {
            roundTripJson(table, id++, json);
        }
    }

    @Test
    public void simpleFieldDef() throws IOException {
        IntegerDefImpl integerDef = new IntegerDefImpl();
        roundTrip(integerDef);
    }

    @Test
    public void complexFieldDef() throws IOException {
        RecordDefImpl nestedRecord =
            (RecordDefImpl) TableBuilder.createRecordBuilder(null)
            .addString("name")
            .addInteger("age", ""/*description*/,
                        false/*nullable*/, 1/*default val*/)
            .build();

        roundTrip(nestedRecord);

        RecordDefImpl complex =
            (RecordDefImpl) TableBuilder.createRecordBuilder(null)
            .addInteger("id")
            .addEnum("workday",
                     new String[]{"monday", "tuesday", "wednesday",
                                  "thursday", "friday"}, null, null, null)
            .addField("likes", TableBuilder.createArrayBuilder()
                      .addString().build())
            .addField("this_is_a_map", TableBuilder.createMapBuilder()
                      .addInteger().build())
            .addField("nested_record", nestedRecord)
            .build();

        roundTrip(complex);
    }

    @Test
    public void simpleFieldValue() throws IOException {
        DoubleValue value =
            FieldDefImpl.Constants.doubleDef.createDouble(5.678);
        roundTrip(value);
    }

    @Test
    public void complexFieldValue1() throws IOException {
        RecordDefImpl complex =
            (RecordDefImpl) TableBuilder.createRecordBuilder(null)
            .addInteger("id")
            /* add string -- not nullable, default value */
            .addString("name", null, false, "joe")
            .addFloat("float")
            .addEnum("days", new String[] {"monday", "wednesday", "friday"},
                     null, null, null)
            .build();

        roundTrip(complex);

        RecordValue record = complex.createRecord();
        record.put("id", 678);
        record.put("name", "this is a name");
        // record.put("float", 5678.1234F);
        record.putEnum("days", "monday");

        roundTrip(record, true /* partial */);
    }

    @Test
    public void complexFieldValue2() throws IOException {
        RecordBuilder rb = TableBuilder.createRecordBuilder(null);
        rb.addInteger("home");
        rb.addInteger("work");

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addField(rb.build());

        RecordDefImpl complex =
            (RecordDefImpl) TableBuilder.createRecordBuilder(null)
            .addInteger("id")
            .addField("array", ab.build())
            .build();

        roundTrip(complex);

        RecordValue record = complex.createRecord();
        record.put("id", 678);
        ArrayValue array = record.putArray("array");
        RecordValue rec = array.addRecord();
        rec.put("home", 1);
        rec.put("work", 2);
        rec = array.addRecord();
        rec.put("home", 3456);
        rec.put("work", 5678);

        roundTrip(record);
    }

    @Test
    public void testKeys() throws IOException {
        TableImpl table =
            TableBuilder.createTableBuilder("table")
            .addInteger("id")
            .addString("name")
            .primaryKey("id")
            .buildTable();

        PrimaryKey key = table.createPrimaryKey();
        key.put("id", 1);

        /*
         * Can't use PrimaryKey directly because the types won't match
         * in the deserialization path. Just serialize the values.
         */
        RecordValue val = key.getDefinition().createRecord();
        val.copyFrom(key);
        roundTrip(val);
    }

    /*
     * Tests round-tripping of strings
     */
    @Test
    public void testString() throws IOException {
        /* a few edge case strings for round-tripping */
        roundTripString(null);
        roundTripString(new String());
        roundTripString("");
        roundTripString("abc");
        roundTripString(" ");

        for (int i = 1; i < 10000; i++) {
            roundTripString(randomString(i));
        }
    }

    @Test
    public void testTimestamp() throws IOException {
        Timestamp[] values = new Timestamp[]{
            new Timestamp(0),
            TimestampUtils.createTimestamp(0, 1),
            TimestampUtils.createTimestamp(-1, 999999999),
            new Timestamp(System.currentTimeMillis()),
            new Timestamp(-1 * System.currentTimeMillis()),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE
        };

        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            TimestampDef def = new TimestampDefImpl(p);
            roundTrip(def);
            for (Timestamp ts : values) {
                TimestampValue tsv = def.createTimestamp(ts);
                roundTrip(tsv);
            }
        }

        Timestamp ts = TimestampUtils
                .parseString("2016-07-21T20:47:15.987654321");
        TimestampDef tsDef = new TimestampDefImpl(9);
        TimestampValueImpl tsval = new TimestampValueImpl(tsDef, ts);
        roundTrip(tsval, tsDef);
    }

    @Test
    public void testNumber() throws IOException {
        BigDecimal[] values1 = new BigDecimal[]{
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.TEN,
            BigDecimal.valueOf(-1),
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.valueOf(Long.MAX_VALUE),
            BigDecimal.valueOf(-1 * Double.MAX_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE),
            BigDecimal.valueOf(-1 * Double.MAX_VALUE),
            new BigDecimal("123456789012345678901234567890.1234567890123456789"),
            new BigDecimal("-123456789012345678901234567890.1234567890123456789"),
            /* this will serialize with array length > a byte value */
            new BigDecimal("7.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049")
        };

        NumberDefImpl def = new NumberDefImpl("test");
        roundTrip(def);

        for (BigDecimal val : values1) {
            NumberValueImpl dv = def.createNumber(val);
            roundTrip(dv);
        }
    }

    @Test
    public void testArray()  throws IOException {
        int vals[] = { 1, 2, 3, 4, 5 };
        ArrayDefImpl arrayDefImpl = new ArrayDefImpl(
                FieldDefImpl.Constants.integerDef);
        ArrayValueImpl arrayVal = new ArrayValueImpl(arrayDefImpl);
        arrayVal.add(vals);
        roundTrip(arrayVal);
    }

    @Test
    public void testMap() throws IOException {
        int vals[] = { 1, 2, 3, 4, 5 };
        MapValueImpl mapVal = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < vals.length; i++) {
            mapVal.put("value" + i, vals[i]);
        }
        roundTrip(mapVal);
    }

    @Test
    public void testEnum() throws IOException {
        String enumsval[] = { "a", "b", "c" };
        String strval = "a";
        EnumDefImpl enumDef = new EnumDefImpl("enumstr", enumsval);
        EnumValueImpl enumVal = new EnumValueImpl(enumDef, strval);
        roundTrip(enumVal);
    }

    /**
     * Tests serializing table to the oldest supported version.
     */
    @Test
    public void testReserializeToOldValue() {
        TableImpl table =
                TableBuilder.createTableBuilder("table")
                .addInteger("id")
                .addString("s")
                .addJson("json", null)
                .primaryKey("id")
                .buildTable();

        RowImpl row = table.createRow();
        row.put("id", 1);
        row.put("s", "row #1");

        Key key = table.createKey(row, false);
        KeySerializer keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);

        int[] lens = new int[] {10, 1024, 65535};
        final String jsonFmt = "{\"s\":\"%s\"}";

        final short oldVersion = SerialVersion.MINIMUM;
        final short currentVersion = SerialVersion.CURRENT;
        for (int len : lens) {
            row.putJson("json", String.format(jsonFmt, genLongString(len)));
            Value val = table.createValue(row);
            byte[] valBytes = val.toByteArray();

            byte[] valBytes1 = table.reserializeToOldValue(keyBytes,
                                                           valBytes,
                                                           oldVersion);
            RowImpl row1 = table.createRowFromBytes(keyBytes,
                                                    valBytes1,
                                                    false);
            assertTrue(row1 != null && row1.equals(row));

            byte[] valBytes2 = table.reserializeToOldValue(keyBytes,
                                                           valBytes1,
                                                           currentVersion);
            assertTrue(Arrays.equals(valBytes1, valBytes2));
        }

        row.putJson("json", String.format(jsonFmt, genLongString(65536)));
        Value val = table.createValue(row);
        byte[] valBytes = val.toByteArray();
        table.reserializeToOldValue(keyBytes, valBytes, oldVersion);
    }

    @Test
    public void testPartialRoundTrip() throws Exception {
        TableImpl table =
                TableBuilder.createTableBuilder("table")
                .addInteger("id")
                .addString("s")
                .primaryKey("id")
                .buildTable();

        RowImpl row = table.createRow();
        row.put("s", "row #1");
        roundTrip(row, true /* partial */);

    }

    /**
     * Test that serialization works properly after a serialization exception.
     * The problem involved the cached Encoder holding onto previous
     * output after a failure. [#27742]
     */
    @Test
    public void testSerializationCache() throws Exception {
        TableImpl table =
            TableBuilder.createTableBuilder("table")
            .addInteger("id")
            .addInteger("int")
            .primaryKey("id")
            .buildTable();

        class TestFieldValue extends IntegerValueImpl {
            private static final long serialVersionUID = 0;
            TestFieldValue(int value) {
                super(value);
            }

            @Override
            public int getInt() {
                throw new RuntimeException("Can't get int");
            }
        }

        TestFieldValue tv = new TestFieldValue(1);

        RowImpl row = table.createRow();
        row.put("id", 1);
        row.put("int", tv);
        try {
            table.createValueInternal(row, null, null);
            fail("Expected RuntimeException");
        } catch (RuntimeException re) {
            assertEquals("Can't get int", re.getMessage());
        }
        row.put("int", 5);

        Value v = table.createValueInternal(row, null, null);
        byte[] valBytes = v.toByteArray();

        Key key = table.createKey(row, false);
        KeySerializer keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);

        row = table.createRowFromBytes(keyBytes, valBytes, false);
        /* The bug in [#27742] caused '1' to appear here */
        assertEquals(5, row.get("int").getInt());
    }

    @Test
    public void testExceptions() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        NullValueImpl nullval = NullValueImpl.getInstance();
        try {
            FieldValueSerialization.writeNonNullFieldValue(nullval, false,
                    false, dos, SerialVersion.CURRENT);
            fail("Exception shoud throw for NullValueImpl");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        try {
            FieldValueSerialization.writeNonNullFieldValue(null, false, false,
                    dos, SerialVersion.CURRENT);
            fail("Exception should throw for null value");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        byte[] testArray1 = { 0, 1, 2, 3, 4, 5, 6, 7 };
        FixedBinaryDefImpl fixedBinDef = new FixedBinaryDefImpl("foo", 7, null);
        FixedBinaryValueImpl fixedBinValUnmatch = new FixedBinaryValueImpl(
                testArray1, fixedBinDef);
        try {
            FieldValueSerialization.writeNonNullFieldValue(
                    fixedBinValUnmatch.asFixedBinary(), false, false, dos,
                    SerialVersion.CURRENT);
            fail("Defined size does not match with values");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        EmptyValueImpl emptyval = EmptyValueImpl.getInstance();
        try {
            FieldValueSerialization.writeNonNullFieldValue(emptyval, false,
                    false, dos, SerialVersion.CURRENT);
            fail("Exception should throw for EmptyValueImpl");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        /* illegal state exception for default case */
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        try {
            AnyDefImpl defAny = new AnyDefImpl();
            ValueReader<FieldValueImpl> valReader = new FieldValueReaderImpl<FieldValueImpl>();
            FieldValueSerialization.readNonNullFieldValue(valReader, null,
                    defAny, null, in, SerialVersion.CURRENT);
            fail("ANY type not supported");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        byte[] bytesZeroLen = { 0 };
        bais = new ByteArrayInputStream(bytesZeroLen);
        in = new DataInputStream(bais);
        try {
            FieldValueSerialization.readNonNullFieldValue(null,
                    FieldDef.Type.NUMBER, in, SerialVersion.CURRENT);
            fail("Zero lengh for number type throw exception");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        /* test WriteTimestamp for empty and max length */
        TimestampDef tsDef = new TimestampDefImpl(9);
        TimestampValueImpl tsValEmpty = new TimestampValueImpl(tsDef, "");
        try {
            FieldValueSerialization.writeTimestamp(tsValEmpty, false, dos,
                    SerialVersion.CURRENT);
            fail("Should throw exception for empty timestamp value");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }

        String tsStringVal = "2016-07-21T20:47:15.9876543212016-07-21T20:47:15"
                + ".9876543212016-07-21T20:47:15.9876543212016-07-21T20:47:15.9"
                + "876543212016-07-21T20:47:15.987654321";
        byte[] tsByteArray = tsStringVal.getBytes();
        TimestampValueImpl tsValMax = new TimestampValueImpl(tsDef,
                tsByteArray);
        try {
            FieldValueSerialization.writeTimestamp(tsValMax, false, dos,
                    SerialVersion.CURRENT);
            fail("Timestamp value exceed max value throw exception");
        } catch (IOException | IllegalStateException e) {
            /* success */
        }
    }

    /*
     * Avro->NSON->Avro
     * Use various data types to make sure they round-trip correctly
     */
    @Test
    public void testNson() throws Exception {
        Map<String, Type> mrcounterFields = new HashMap<>();
        mrcounterFields.put("x.ci", oracle.kv.table.FieldDef.Type.INTEGER);
        mrcounterFields.put("x.y.cl", oracle.kv.table.FieldDef.Type.LONG);
        mrcounterFields.put("x.\"y.cl\"", oracle.kv.table.FieldDef.Type.NUMBER);
        TableBuilder builder =
            (TableBuilder) TableBuilder.createTableBuilder("nson")
            /* All id_xxx are PK fields */
            .addBoolean("id_bool")
            .addDouble("id_double")
            .addEnum("id_enum", new String[] {"id_e1", "id_e2", "id_e3"}, null)
            .addFloat("id_float")
            .addInteger("id_int")
            .addLong("id_long")
            .addNumber("id_number")
            .addString("id_str")
            .addTimestamp("id_time", 3)
            /* Below are value fields */
            .addJson("json1", mrcounterFields, null)
            .addJson("json2", null)
            .addJson("json3", null)
            .addString("string_val")
            .addLong("long_val")
            .addDouble("double_val")
            .addFloat("float_val")
            .addEnum("days", new String[] {"monday", "wednesday", "friday"},
                     null, null, null)
            .addNumber("number")
            .addTimestamp("timestamp", 5)
            .primaryKey("id_bool", "id_double", "id_enum", "id_float",
                        "id_int", "id_long", "id_number", "id_str", "id_time");
        builder.addField("record",
                         TableBuilder.createRecordBuilder("record")
                         .addString("city")
                         .addString("state")
                         .build());
        builder.addField("likes", TableBuilder.createArrayBuilder()
                         .addString().build());
        builder.addField("this_is_a_map", TableBuilder.createMapBuilder()
                         .addInteger().build());

        TableImpl table = builder.buildTable();
        RowImpl row = table.createRow();
        /* put PK fields */
        row.put("id_bool", true);
        row.put("id_double", 100.12);
        row.putEnum("id_enum", "id_e2");
        row.put("id_float", 12.34f);
        row.put("id_int", 30);
        row.put("id_long", Long.MAX_VALUE);
        row.put("id_number", FieldValueFactory.createNumber(
            new BigDecimal(
                "123456789012345678901234567890.1234567890123456789")));
        row.put("id_str", "abcde");
        Timestamp ts1 = Timestamp.from(Instant.now());
        row.put("id_time", ts1);

        /* set Value fields */
        row.put("string_val", "abcde");
        row.put("long_val", 600L);
        row.put("double_val", 1.234);
        row.put("float_val", 4.321f);
        row.putEnum("days", "monday");
        row.put("number", FieldValueFactory.createNumber(
            new BigDecimal(
                "123456789012345678901234567890.1234567890123456789")));
        Timestamp ts2 = Timestamp.from(Instant.now());
        row.put("timestamp", ts2);
        /*
         * Json structure:
         * a -> true
         * b -> 5567.7
         * x.string -> test
         * x.ci -> IntMRCounter
         * x.y.cl -> LongMRCounter
         * x."y.cl" -> NumberMRCounter
         */
        MapValue jsonMap = FieldDefImpl.Constants.mapJsonDef.createMap();
        jsonMap.put("a", true);
        jsonMap.put("b", 5567.7);
        MapValue xMap = FieldDefImpl.Constants.mapJsonDef.createMap();
        jsonMap.put("x", xMap);
        FieldValueImpl crdtField =
            FieldDefImpl.Constants.intMRCounterDef.createCRDTValue();
        crdtField.putMRCounterEntry(11,
            FieldDefImpl.Constants.integerDef.createInteger(1000));
        crdtField.putMRCounterEntry(22,
            FieldDefImpl.Constants.integerDef.createInteger(2000));
        xMap.put("ci", crdtField);

        crdtField = FieldDefImpl.Constants.numberMRCounterDef.createCRDTValue();
        crdtField.putMRCounterEntry(55,
            FieldDefImpl.Constants.numberMRCounterDef.createNumber(
                new BigDecimal("1111111111111111111111111111111111111111")));
        crdtField.putMRCounterEntry(66,
            FieldDefImpl.Constants.numberMRCounterDef.createNumber(
                new BigDecimal("2222222222222222222222222222222222222222")));
        xMap.put("y.cl", crdtField);
        xMap.put("string", "test");

        MapValue yMap = FieldDefImpl.Constants.mapJsonDef.createMap();
        xMap.put("y", yMap);
        crdtField = FieldDefImpl.Constants.longMRCounterDef.createCRDTValue();
        crdtField.putMRCounterEntry(33,
            FieldDefImpl.Constants.longMRCounterDef.
                                   createLong(Integer.MAX_VALUE));
        crdtField.putMRCounterEntry(44,
            FieldDefImpl.Constants.longMRCounterDef.
                                   createLong(Integer.MAX_VALUE + 1L));
        yMap.put("cl", crdtField);
        row.putInternal(row.getFieldPos("json1"), jsonMap, false);

        row.putJson("json2", "null");
        row.putNumber("json3", new BigDecimal(678));

        ArrayValue array = row.putArray("likes");
        array.add("array_value");
        MapValue map = row.putMap("this_is_a_map");
        map.put("number", 1);
        RecordValue record = row.putRecord("record");
        record.put("city", "bethel");
        record.put("state", "maine");

        Key key = table.createKey(row, false);
        KeySerializer keySerializer =
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        byte[] keyBytes = keySerializer.toByteArray(key);
        PrimaryKeyImpl pk = table.createPrimaryKeyFromKeyBytes(keyBytes);

        Value val = table.createValue(row, false);
        assertTrue(val != null);
        byte[] valBytes = val.toByteArray();

        BinaryKeyIterator keyIter = table.createBinaryKeyIterator(keyBytes);
        keyIter.reset();
        NioByteOutputStream out = NioByteOutputStream
            .createNioByteOutputStream();
        assertTrue(NsonUtil.createNsonFromKeyBytes(table, keyIter, out));
        byte[] nsonKey = getBytes(out);
        //System.out.println("NSON PK: " + Nson.toJsonString(nsonKey));

        out = NioByteOutputStream.createNioByteOutputStream();
        assertTrue(NsonUtil.createNsonFromValueBytes(table, valBytes,
                                                     true, out));
        byte[] nsonValue = getBytes(out);
        //System.out.println("NSON value: " + Nson.toJsonString(nsonValue));

        PrimaryKeyImpl pk1 =
            NsonUtil.createPrimaryKeyFromNsonBytes(table, nsonKey);
        //System.out.println("Key orig, new:\n" + pk + "\n" + pk1);
        Value val1 =
            NsonUtil.createValueFromNsonBytes(table, nsonValue, 0, false);
        //System.out.println("Value orig, new:\n" + val + "\n" + val1);

        assertTrue("Old and new PK should be the same", pk.equal(pk1));
        /* only compare the byte arrays, exclude format and region */
        assertTrue("Old and new Value should be the same",
                   compareValueArrays(val, val1));
    }

    /* only compare the serialized data, ignoring format and region */
    private boolean compareValueArrays(Value v1, Value v2) {
        return Arrays.equals(v1.getValue(), v2.getValue());
    }

    private String genLongString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + (i % 26)));
        }
        return sb.toString();
    }

    static void roundTripString(String s) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        SerializationUtil.writeString(out, CURRENT, s);

        byte[] bytes = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        String newString = SerializationUtil.readString(in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());
        assertEquals(s, newString);
    }

    static void roundTrip(FieldDef def) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        FieldDefSerialization.writeFieldDef(def, out, CURRENT);
        byte[] bytes = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        FieldDef newDef =
            FieldDefSerialization.readFieldDef(in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());
        assertEquals(def, newDef);
        assertEquals(def.hashCode(), newDef.hashCode());
    }

    static void roundTrip(FieldValue value) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        FieldValueSerialization.writeFieldValue(value, true, out, CURRENT);
        byte[] bytes = baos.toByteArray();
        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        FieldValue newValue =
            FieldValueSerialization.readFieldValue(null, in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());
        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                         newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization",
                         value, newValue);
        }
    }

    static void roundTrip2(FieldValue value) throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        FieldValueSerialization.writeFieldValue(value, false, out, CURRENT);
        byte[] bytes = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        FieldValue newValue = FieldValueSerialization.readFieldValue(
            value.getDefinition(), in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());

        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                         newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization",
                         value, newValue);
        }
    }

    static void roundTrip(RecordValue value, boolean partial)
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        FieldValueSerialization.writeRecord((RecordValueImpl)value, true,
                                            partial, out, CURRENT);
        byte[] bytes = baos.toByteArray();
        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        FieldValue newValue =
            FieldValueSerialization.readRecord(null, partial, in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());
        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                         newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization",
                         value, newValue);
        }
    }

    static void roundTrip(ArrayValue value) throws IOException {
        ArrayDefImpl arrayDefImpl = new ArrayDefImpl(
                FieldDefImpl.Constants.integerDef);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        FieldValueSerialization.writeArray((ArrayValueImpl) value, true, dos,
                SerialVersion.CURRENT);
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ArrayValueImpl newValue = new ArrayValueImpl(arrayDefImpl);
        ValueReader<FieldValueImpl> arrReader =
                new FieldValueReaderImpl<FieldValueImpl>();
        FieldValueSerialization.readArray(arrReader, null, null, in,
                SerialVersion.CURRENT);
        newValue = (ArrayValueImpl) arrReader.getValue();

        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                    newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization", value,
                    newValue);
        }
    }

    static void roundTrip(MapValue value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        FieldValueSerialization.writeMap((MapValueImpl) value, true, dos,
                SerialVersion.CURRENT);
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        MapValueImpl newValue = new MapValueImpl(
                FieldDefImpl.Constants.mapAnyDef);
        ValueReader<FieldValueImpl> mapReader =
                new FieldValueReaderImpl<FieldValueImpl>();
        FieldValueSerialization.readMap(mapReader, null, null, in,
                SerialVersion.CURRENT);
        newValue = (MapValueImpl) mapReader.getValue();

        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                    newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization", value,
                    newValue);
        }
    }

    static void roundTrip(EnumValue value) throws IOException {
        String enumsval[] = { "a", "b", "c" };
        EnumDefImpl defEnum = new EnumDefImpl("enumstr", enumsval);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        FieldValueSerialization.writeEnum((EnumValueImpl) value, false, dos,
                SerialVersion.CURRENT);
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        EnumValueImpl newValue = null;
        ValueReader<FieldValueImpl> enumReader =
                new FieldValueReaderImpl<FieldValueImpl>();
        FieldValueSerialization.readNonNullFieldValue(enumReader, null, defEnum,
                null, in, SerialVersion.CURRENT);
        newValue = (EnumValueImpl) enumReader.getValue();

        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                    newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization", value,
                    newValue);
        }
    }

    static void roundTrip(TimestampValue value, TimestampDef tsDef)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        FieldValueSerialization.writeTimestamp((TimestampValueImpl) value,
                false, dos, SerialVersion.CURRENT);
        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        TimestampValueImpl newValue = null;
        ValueReader<FieldValueImpl> tsReader =
                new FieldValueReaderImpl<FieldValueImpl>();
        FieldValueSerialization.readNonNullFieldValue(tsReader, null, tsDef,
                null, in, SerialVersion.CURRENT);
        newValue = (TimestampValueImpl) tsReader.getValue();

        if (value.isRow()) {
            /*
             * Use newValue as the expected value to avoid check in RowImpl for
             * table name.
             */
            assertEquals("Expected equal values after deserialization",
                    newValue, value);
        } else {
            assertEquals("Expected equal values after deserialization", value,
                    newValue);
        }
    }

    /*
     * Creates a random UTF-8 string for testing
     */
    private static String randomString(int length) {
      Random rand = new Random();
      StringBuffer sb = new StringBuffer(length);
      while (sb.length() < length) {
          char c = (char)(rand.nextInt() & Character.MAX_VALUE);
          if (Character.isDefined(c) && !Character.isSurrogate(c)) {
              sb.append(c);
          }
      }
      return sb.toString();
    }
}
