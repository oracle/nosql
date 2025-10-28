/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.proxy.ProxySerialization.readFieldValue;
import static oracle.nosql.proxy.ProxySerialization.writeFieldValue;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_ARRAY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BINARY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_DOUBLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_EMPTY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_JSON_NULL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_LONG;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NULL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NUMBER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.api.table.ValueReader;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.proxy.ProxySerialization.RowReaderImpl;
import oracle.nosql.proxy.ValueSerializer.RowSerializerImpl;
import oracle.nosql.proxy.util.TestBase;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

/**
 * Test serialization and deserialization on FieldValue on both driver and
 * proxy side.
 */
public class SerializationTest extends TestBase {

    /* Some const values for testing */
    private static final oracle.nosql.driver.values.NullValue nullValue =
        oracle.nosql.driver.values.NullValue.getInstance();

    private static final oracle.nosql.driver.values.JsonNullValue jsonNull =
        oracle.nosql.driver.values.JsonNullValue.getInstance();

    private static final oracle.nosql.driver.values.EmptyValue emptyValue =
        oracle.nosql.driver.values.EmptyValue.getInstance();

    private static final oracle.nosql.driver.values.BooleanValue trueValue =
        oracle.nosql.driver.values.BooleanValue.trueInstance();

    private static final oracle.nosql.driver.values.BinaryValue binaryValue =
        new oracle.nosql.driver.values.BinaryValue(new byte[]{(byte)0x0});

    private static final oracle.nosql.driver.values.DoubleValue doubleValue =
        new oracle.nosql.driver.values.DoubleValue(1.234567E6d);

    private static final oracle.nosql.driver.values.IntegerValue intValue =
        new oracle.nosql.driver.values.IntegerValue(9999999);

    private static final oracle.nosql.driver.values.LongValue longValue =
        new oracle.nosql.driver.values.LongValue(1000000000L);

    private static final oracle.nosql.driver.values.StringValue stringValue =
        new oracle.nosql.driver.values.StringValue("oracle nosql");

    private static final oracle.nosql.driver.values.TimestampValue
        timestampValue = new oracle.nosql.driver.values.TimestampValue(0);

    private static final oracle.nosql.driver.values.NumberValue numberValue =
        new oracle.nosql.driver.values.NumberValue(BigDecimal.ZERO);

    static {
        /* Configures Netty logging to use JDK logger */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
    }

    private final ByteBuf testBuf = Unpooled.buffer();

    /**
     * Test simple type values
     */
    @Test
    public void testSimpleTypeValues() {

        oracle.nosql.driver.values.FieldValue driverValue;

        /* Null */
        driverValue = nullValue;
        roundTrip(driverValue, FieldDefFactory.createStringDef());

        /* JsonNull */
        driverValue = jsonNull;
        roundTrip(driverValue, FieldDefFactory.createJsonDef());

        /* Binary */
        byte[][] bytesArray = new byte[][]{
            new byte[0],
            new byte[]{(byte)0, (byte)0},
            genByteArray(1024)
        };
        for (byte[] bytes : bytesArray) {
            roundTrip(new oracle.nosql.driver.values.BinaryValue(bytes),
                FieldDefFactory.createBinaryDef());
        }

        /* FixedBinary */
        roundTrip(new oracle.nosql.driver.values.BinaryValue(genByteArray(32)),
            FieldDefFactory.createFixedBinaryDef(32));

        /* Boolean */
        boolean[] booleans = new boolean[] {true, false};
        for (boolean val : booleans) {
            roundTrip(oracle.nosql.driver.values.BooleanValue.getInstance(val),
                FieldDefFactory.createBooleanDef());
        }

        /* Integer */
        int[] ints = new int[] {
            Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -123456789, 123456789
        };
        for (int val : ints) {
            roundTrip(new oracle.nosql.driver.values.IntegerValue(val),
                FieldDefFactory.createIntegerDef());
        }

        /* Long */
        long[] longs = new long[] {
            Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1234567890123456789L,
            1234567890123456789L
        };
        for (long val : longs) {
            roundTrip(new oracle.nosql.driver.values.LongValue(val),
                FieldDefFactory.createLongDef());
        }

        /* Float */
        float[] floats = new float[] {
            Float.MIN_VALUE, Float.MAX_VALUE, 0.0f, -1.1231421f, 132124.1f
        };
        for (float val : floats) {
            roundTrip(new oracle.nosql.driver.values.DoubleValue(val),
                FieldDefFactory.createFloatDef());
        }

        /* Double */
        double[] doubles = new double[] {
            Double.MIN_VALUE, Double.MAX_VALUE, 0.0d, -1.1231421132132132d,
            132124.132132132132d
        };
        for (double val : doubles) {
            roundTrip(new oracle.nosql.driver.values.DoubleValue(val),
                FieldDefFactory.createDoubleDef());
        }

        /* String */
        String[] strings = new String[] {"", genString(10), genString(1024)};
        for (String val : strings) {
            roundTrip(new oracle.nosql.driver.values.StringValue(val),
                FieldDefFactory.createStringDef());
        }

        /* Enum */
        final String[] enumValues = new String[]{"red", "yellow", "blue"};
        for (String val : enumValues) {
            roundTrip(new oracle.nosql.driver.values.StringValue(val),
                FieldDefFactory.createEnumDef(enumValues));
        }

        /* Timestamp */
        String datetime = "2017-07-15T15:18:59";
        for (int i = 0; i <= 9; i++) {
            String val = datetime;
            if (i > 0) {
                val += new String(".123456789").substring(0, i + 1);
            }
            roundTrip(new oracle.nosql.driver.values.TimestampValue(val),
                FieldDefFactory.createTimestampDef(i));
        }

        /* Number */
        for (int ival : ints) {
            BigDecimal val = BigDecimal.valueOf(ival);
            roundTrip(new oracle.nosql.driver.values.NumberValue(val),
                FieldDefFactory.createNumberDef());
        }

        for (long lval : longs) {
            BigDecimal val = BigDecimal.valueOf(lval);
            roundTrip(new oracle.nosql.driver.values.NumberValue(val),
                FieldDefFactory.createNumberDef());
        }

        for (float fval : floats) {
            BigDecimal val = BigDecimal.valueOf(fval);
            roundTrip(new oracle.nosql.driver.values.NumberValue(val),
                FieldDefFactory.createNumberDef());
        }

        for (double dval : doubles) {
            BigDecimal val = BigDecimal.valueOf(dval);
            roundTrip(new oracle.nosql.driver.values.NumberValue(val),
                FieldDefFactory.createNumberDef());
        }

        BigDecimal[] decs = new BigDecimal [] {
            BigDecimal.ZERO,
            new BigDecimal("1.23456789E+1024"),
            new BigDecimal(new BigInteger("9999999999"), Integer.MIN_VALUE + 10),
            new BigDecimal(new BigInteger("9999999999"), Integer.MAX_VALUE)
        };

        for (BigDecimal val : decs) {
            roundTrip(new oracle.nosql.driver.values.NumberValue(val),
                FieldDefFactory.createNumberDef());
        }
    }

    /*
     * Test on compatible types from Driver to proxy
     *     Driver type                          KV type
     *     -----------                          -------
     *     Long/Double/Decimal/String           Integer
     *     Integer/Double/Decimal/String        Long
     *     Integer/Long/Double/Decimal/String   Float
     *     Integer/Long/Decimal/String          Double
     *     Integer/Long/Double/String           Number
     *     String                               Boolean
     */
    @Test
    public void testCompatibleTypes()
        throws Exception {

        oracle.kv.table.FieldDef def;

        Random rand = new Random(System.currentTimeMillis());

        /*
         *  KV type: Integer
         *  Driver values: Long/Double/Decimal/String
         */
        def = FieldDefFactory.createIntegerDef();

        /* long to int */
        long[] longToInts = new long[] {
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            rand.nextInt()
        };
        for (long val : longToInts) {
            doWriteToProxy(new oracle.nosql.driver.values.LongValue(val),
                           def, def.createInteger((int)val));
        }

        /* double to int */
        double[] doubleToInts = new double[] {
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            1.23456789E8d
        };
        for (double val : doubleToInts) {
            doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(val),
                           def, def.createInteger((int)val));
        }

        /* decimal to int */
        int[] ints = new int[] {
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            rand.nextInt()
        };
        for (int val : ints) {
            doWriteToProxy(
                new oracle.nosql.driver.values.NumberValue(
                        BigDecimal.valueOf(val)), def, def.createInteger(val));
        }

        /* string to int */
        for (int val : ints) {
            doWriteToProxy(
                new oracle.nosql.driver.values.StringValue(String.valueOf(val)),
                def, def.createInteger(val));
        }

        /*
         *  KV type: Long
         *  Driver values: Integer/Double/Decimal/String
         */
        def = FieldDefFactory.createLongDef();

        /* int to long */
        int[] intToLongs = new int[] {
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            rand.nextInt()
        };
        for (int val : intToLongs) {
            doWriteToProxy(new oracle.nosql.driver.values.IntegerValue(val),
                           def, def.createLong(val));
        }

        /* double to long */
        double[] doubleToLongs = new double[] {
            -9.007199254740992E15,
            9.007199254740992E15,
            1.234567890123456E15d
        };
        for (double val : doubleToLongs) {
            doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(val),
                           def, def.createLong((long)val));
        }

        /* decimal to longs */
        long[] longs = new long[] {
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            0L,
            rand.nextLong()
        };
        for (long val : longs) {
            doWriteToProxy(
                new oracle.nosql.driver.values.NumberValue(
                    BigDecimal.valueOf(val)), def, def.createLong(val));
        }

        /* string to long */
        for (long val : longs) {
            doWriteToProxy(
                new oracle.nosql.driver.values.StringValue(String.valueOf(val)),
                def, def.createLong(val));
        }

        /*
         *  KV type: Float
         *  Driver values: Integer/Long/Double/Decimal/String
         */
        def = FieldDefFactory.createFloatDef();

        /* int to float */
        int[] intToFloats = new int[] {
            -10000000, 0, 10000000, Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        for (int val : intToFloats) {
            doWriteToProxy(new oracle.nosql.driver.values.IntegerValue(val),
                def, def.createFloat(val));
        }

        /* long to float */
        long[] longToFloats = new long[] {
            -10000000L, 0, 10000000L, Long.MIN_VALUE, Long.MAX_VALUE
        };
        for (long val : longToFloats) {
            doWriteToProxy(new oracle.nosql.driver.values.LongValue(val),
                def, def.createFloat(val));
        }

        /* double to float */
        float[] floats = new float[] {
            Float.MAX_VALUE, -Float.MAX_VALUE,
            Float.MIN_VALUE, -Float.MIN_VALUE,
            rand.nextFloat()
        };
        for (float val : floats) {
            doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(val),
                           def, def.createFloat(val));
        }

        /* decimal to float */
        for (float val : floats) {
            BigDecimal dec = BigDecimal.valueOf(val);
            doWriteToProxy(new oracle.nosql.driver.values.NumberValue(dec),
                           def, def.createFloat(val));
        }

        /* string to float */
        for (float val : floats) {
            String sval = Float.toString(val);
            doWriteToProxy(new oracle.nosql.driver.values.StringValue(sval),
                           def, def.createFloat(val));
        }

        /*
         *  KV type: Double
         *  Driver values: Integer/Long/Decimal/String
         */
        def = FieldDefFactory.createDoubleDef();

        /* int to double */
        int[] intToDoubles = new int[] {
            -10000000, 0, 10000000, Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        for (int val : intToDoubles) {
            doWriteToProxy(new oracle.nosql.driver.values.IntegerValue(val),
                           def, def.createDouble(val));
        }

        /* long to double */
        long[] longToDoubles = new long[] {
            -10000000L, 0, 10000000L, Long.MIN_VALUE, Long.MAX_VALUE
        };
        for (long val : longToDoubles) {
            doWriteToProxy(new oracle.nosql.driver.values.LongValue(val),
                           def, def.createDouble(val));
        }

        /* decimal to double */
        double[] doubles = new double[] {
            Double.MAX_VALUE, -Double.MAX_VALUE,
            Double.MIN_VALUE, -Double.MIN_VALUE,
            rand.nextDouble()
        };
        for (double val : doubles) {
            BigDecimal dec = BigDecimal.valueOf(val);
            doWriteToProxy(new oracle.nosql.driver.values.NumberValue(dec),
                           def, def.createDouble(val));
        }

        /* string to double */
        for (double val : doubles) {
            String sval = Double.toString(val);
            doWriteToProxy(new oracle.nosql.driver.values.StringValue(sval),
                           def, def.createDouble(val));
        }

        /*
         *  KV type: Number
         *  Driver values: Integer/Long/Double/String
         */
        def = FieldDefFactory.createNumberDef();

        /* int to number */
        for (int val : ints) {
            doWriteToProxy(new oracle.nosql.driver.values.IntegerValue(val),
                           def, def.createNumber(val));
        }

        /* long to number */
        for (long val : longs) {
            doWriteToProxy(new oracle.nosql.driver.values.LongValue(val),
                           def, def.createNumber(val));
        }

        /* double to number */
        for (double val : doubles) {
            doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(val),
                           def, def.createNumber(val));
        }

        /* string to number */
        String sval = "1.23456789E+1024";
        doWriteToProxy(new oracle.nosql.driver.values.StringValue(sval), def,
                       def.createNumber(new BigDecimal("1.23456789E+1024")));

        /* string to boolean */
        def = FieldDefFactory.createBooleanDef();
        sval = "true";
        doWriteToProxy(new oracle.nosql.driver.values.StringValue(sval),
                       def, def.createBoolean(true));

        sval = "abc";
        doWriteToProxy(new oracle.nosql.driver.values.StringValue(sval),
                       def, def.createBoolean(false));

        /*
         * Conversion fails.
         */

        /*
         * long/double/decimal to int
         */

        def = FieldDefFactory.createIntegerDef();

        /* long to int */
        long lval = (long)Integer.MAX_VALUE + 1;
        doWriteToProxy(new oracle.nosql.driver.values.LongValue(lval),
                       def, false /* shoudSucceed */);

        /* double to int */
        double dval = lval;
        doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(dval),
                       def, false /* shoudSucceed */);

        /* decimal to int */
        BigDecimal decVal = BigDecimal.valueOf(lval);
        doWriteToProxy(new oracle.nosql.driver.values.NumberValue(decVal),
                       def, false /* shoudSucceed */);

        /*
         * double/decimal to long
         */

        def = FieldDefFactory.createLongDef();

        /* double to long */
        dval = 9.2233720368547758E10d;
        doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(dval),
                       def, false /* shoudSucceed */);

        /* decimal to long */
        decVal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        doWriteToProxy(new oracle.nosql.driver.values.NumberValue(decVal),
                       def, false /* shoudSucceed */);

        /* double to float */
        dval = Double.MAX_VALUE;
        doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(dval),
                       FieldDefFactory.createFloatDef(),
                       false /* shoudSucceed */);

        /*
         * int/long/double/decimal to float
         */

        def = FieldDefFactory.createFloatDef();

        /* int to float */
        int ival = 123456789;
        doWriteToProxy(new oracle.nosql.driver.values.IntegerValue(ival),
                       def, false /* shoudSucceed */);

        /* long to float */
        lval = 1234567890123456789L;
        doWriteToProxy(new oracle.nosql.driver.values.LongValue(lval),
                       def, false /* shoudSucceed */);

        /* double to float */
        dval = Double.MAX_VALUE;
        doWriteToProxy(new oracle.nosql.driver.values.DoubleValue(dval),
                       def, false /* shoudSucceed */);

        /* decimal to float */
        decVal = BigDecimal.valueOf(123456789);
        doWriteToProxy(new oracle.nosql.driver.values.NumberValue(decVal),
                       def, false /* shoudSucceed */);

        /*
         * long/decimal to double
         */
        def = FieldDefFactory.createDoubleDef();

        /* long to double */
        lval = 1234567890123456789L;
        doWriteToProxy(new oracle.nosql.driver.values.LongValue(lval),
                       def, false /* shoudSucceed */);

        /* decimal to double */
        decVal = BigDecimal.valueOf(lval);
        doWriteToProxy(new oracle.nosql.driver.values.NumberValue(decVal),
                       def, false /* shoudSucceed */);

        /* Invalid string value for the target type*/
        String[] svals = new String[] {"_foo", "", "abc"};
        for (String val : svals) {
            oracle.nosql.driver.values.StringValue value =
                new oracle.nosql.driver.values.StringValue(val);
            doWriteToProxy(value, FieldDefFactory.createIntegerDef(),
                           false /* shoudSucceed */);
            doWriteToProxy(value, FieldDefFactory.createLongDef(),
                           false /* shoudSucceed */);
            doWriteToProxy(value, FieldDefFactory.createFloatDef(),
                           false /* shoudSucceed */);
            doWriteToProxy(value, FieldDefFactory.createDoubleDef(),
                           false /* shoudSucceed */);
            doWriteToProxy(value, FieldDefFactory.createNumberDef(),
                           false /* shoudSucceed */);
        }
    }

    @Test
    public void testArrayValue() {
        FieldDef arrayDef;
        oracle.nosql.driver.values.ArrayValue arrayValue;
        int numElements = 3;

        /* Array(String) */
        arrayDef = TableBuilder.createArrayBuilder().addString().build();
        arrayValue = new oracle.nosql.driver.values.ArrayValue(numElements);
        for (int i = 0; i < numElements; i++) {
            arrayValue.add("name_" + i);
        }
        roundTrip(arrayValue, arrayDef);

        /* Array(Map(Record(rid Integer, rname String)) */
        FieldDef recordDef = TableBuilder.createRecordBuilder("rec")
            .addInteger("rid").addString("rname").build();
        FieldDef mapDef =
            TableBuilder.createMapBuilder().addField(recordDef).build();
        arrayDef = TableBuilder.createArrayBuilder().addField(mapDef).build();

        oracle.nosql.driver.values.MapValue recValue;
        oracle.nosql.driver.values.MapValue mapValue;
        arrayValue = new oracle.nosql.driver.values.ArrayValue(numElements);
        for (int i = 0; i < numElements; i++) {
            mapValue = new oracle.nosql.driver.values.MapValue();
            for (int j = 0; j < numElements; j++) {
                recValue = new oracle.nosql.driver.values.MapValue();
                recValue.put("rid", j);
                recValue.put("rname", "name" + j);
                mapValue.put("key" + j, recValue);
            }
            arrayValue.add(mapValue);
        }
        roundTrip(arrayValue, arrayDef);

        /* Empty array */
        roundTrip(new oracle.nosql.driver.values.ArrayValue(), arrayDef);
    }

    @Test
    public void testMapValue() {
        FieldDef mapDef;
        oracle.nosql.driver.values.MapValue mapValue;
        int numElements = 3;

        /* Map(Long) */
        mapDef = TableBuilder.createMapBuilder().addLong().build();
        mapValue = new oracle.nosql.driver.values.MapValue(numElements);
        long[] lvals = new long[] {
            Long.MIN_VALUE, 0, Long.MAX_VALUE
        };
        for (int i = 0; i < lvals.length; i++) {
            mapValue.put("key" + i, lvals[i]);
        }
        roundTrip(mapValue, mapDef);

        /* Map(Array(Record(ri Integer, rf Float)) */
        mapDef = TableBuilder.createMapBuilder().addField
            (TableBuilder.createArrayBuilder().addField
                (TableBuilder.createRecordBuilder("rec")
                    .addInteger("ri")
                    .addFloat("rf")
                    .build())
                .build())
            .build();

        float[] fvals = new float[] {Float.MIN_VALUE, 1.23145f, Float.MAX_VALUE};
        oracle.nosql.driver.values.ArrayValue arrayValue;
        oracle.nosql.driver.values.MapValue recordValue;
        for (int i = 0; i < lvals.length; i++) {
            arrayValue = new oracle.nosql.driver.values.ArrayValue(fvals.length);
            int j = 0;
            for (float fval : fvals) {
                recordValue = new oracle.nosql.driver.values.MapValue();
                recordValue.put("ri", j++).put("rf", fval);
                arrayValue.add(recordValue);
            }
            mapValue.put("key" + i, arrayValue);
        }
        roundTrip(mapValue, mapDef);

        /* Empty map */
        roundTrip(new oracle.nosql.driver.values.MapValue(), mapDef);
    }

    @Test
    public void testRecordValue() {
        FieldDef recDef = TableBuilder.createRecordBuilder("rec")
            .addInteger("id")
            .addBinary("bi")
            .addFixedBinary("fbi", 30)
            .addBoolean("bl")
            .addDouble("d")
            .addFloat("f")
            .addLong("l")
            .addNumber("n")
            .addTimestamp("ts", 3)
            .addString("s")
            .addEnum("enum", new String[]{"red", "yellow", "blue"},  null)
            .addJson("json", null)
            .addField("array",
                TableBuilder.createArrayBuilder().addString().build())
            .addField("map",
                TableBuilder.createMapBuilder().addInteger().build())
            .addField("rec",
                TableBuilder.createRecordBuilder("rec").addInteger("ri")
                .addString("rs").build())
            .build();

        oracle.nosql.driver.values.ArrayValue arrayValue =
            new oracle.nosql.driver.values.ArrayValue();
        for (int i = 0; i < 3; i++) {
            arrayValue.add("string" + i);
        }

        oracle.nosql.driver.values.MapValue mapValue =
            new oracle.nosql.driver.values.MapValue();
        for (int i = 0; i < 3; i++) {
            mapValue.put("key" + i, i);
        }

        oracle.nosql.driver.values.MapValue recValue =
            new oracle.nosql.driver.values.MapValue();
        recValue.put("ri", 0).put("rs", "rs value");

        oracle.nosql.driver.values.MapValue recordValue =
            new oracle.nosql.driver.values.MapValue();
        recordValue.put("id", 1);
        recordValue.put("bi", genByteArray(10));
        recordValue.put("fbi", genByteArray(30));
        recordValue.put("bl", true);
        recordValue.put("d", 1.2321321321313131d);
        recordValue.put("f", 1.2321f);
        recordValue.put("l", 1234567890123456L);
        recordValue.put("n", BigDecimal.valueOf(1231321321313213L, 10000));
        recordValue.put("ts",
            new oracle.nosql.driver.values.TimestampValue(
                "2017-08-21T13:34:35.123"));
        recordValue.put("s", "this is a string");
        recordValue.put("enum", "blue");
        String json = "{\"a\": 1, \"b\": [23, 50, 60], \"map\":{\"m1\":6}}";
        recordValue.put("json", JsonUtils.createValueFromJson(json, null));
        recordValue.put("array", arrayValue);
        recordValue.put("map", mapValue);
        recordValue.put("rec", recValue);
        roundTrip(recordValue, recDef);

        /* All fields are filled with NullValue */
        recordValue = new oracle.nosql.driver.values.MapValue();
        recordValue.put("id", nullValue);
        recordValue.put("bi", nullValue);
        recordValue.put("fbi", nullValue);
        recordValue.put("bl", nullValue);
        recordValue.put("d", nullValue);
        recordValue.put("f", nullValue);
        recordValue.put("l", nullValue);
        recordValue.put("n", nullValue);
        recordValue.put("ts", nullValue);
        recordValue.put("s", nullValue);
        recordValue.put("enum", nullValue);
        recordValue.put("json", nullValue);
        recordValue.put("array", nullValue);
        recordValue.put("map", nullValue);
        recordValue.put("rec", nullValue);
        roundTrip(recordValue, recDef);

        /* Empty record */
        roundTrip(new oracle.nosql.driver.values.MapValue(), recDef);
    }

    @Test
    public void testDeserWithValueReader() {
        TableImpl table = TableBuilder.createTableBuilder("test")
            .addInteger("id")
            .addBinary("bi")
            .addFixedBinary("fbi", 30)
            .addBoolean("bl")
            .addDouble("d")
            .addFloat("f")
            .addLong("l")
            .addNumber("n")
            .addTimestamp("ts", 3)
            .addString("s")
            .addEnum("e", new String[]{"red", "yellow", "blue"},  null)
            .addJson("json", null)
            .addField("as",
                TableBuilder.createArrayBuilder().addString().build())
            .addField("mi",
                TableBuilder.createMapBuilder().addInteger().build())
            .addField("r",
                TableBuilder.createRecordBuilder("r")
                .addInteger("ri")
                .addString("rs")
                .build())
            .addField("a_r_ms",
                TableBuilder.createArrayBuilder("a_r_ms")
                .addField(TableBuilder.createRecordBuilder("r_ms")
                    .addInteger("ri")
                    .addField("rms",
                        TableBuilder.createMapBuilder("rms")
                        .addString()
                        .build())
                    .build())
                .build())
            .addField("r_m_al",
                TableBuilder.createRecordBuilder("r_m_al")
                .addInteger("ri")
                .addField("m_al",
                    TableBuilder.createMapBuilder("m_al")
                    .addField(TableBuilder.createArrayBuilder()
                        .addLong()
                        .build())
                    .build())
                .build())
            .primaryKey("id")
            .buildTable();

        Row row = table.createRow();
        row.put("id", 0);
        row.put("bi", genByteArray(50));
        row.putFixed("fbi",
            genByteArray(table.getField("fbi").asFixedBinary().getSize()));
        row.put("bl", true);
        row.put("d", Double.MAX_VALUE);
        row.put("f", Float.MAX_VALUE);
        row.put("l", Long.MAX_VALUE);
        row.putNumber("n",
            new BigDecimal("1234567890123456789012345678901234567890"));
        row.put("ts", new Timestamp(System.currentTimeMillis()));
        row.put("s", genString(100));
        row.putEnum("e", "blue");
        row.putJson("json",
            "{\"a\": 1, \"b\": [23, 50, 60], \"m\":{\"k1\":6,\"k2\":2}, " +
            "\"d\": null}");

        ArrayValue av = row.putArray("as");
        for (int i = 0; i < 3; i++) {
            av.add("av_" + i);
        }

        MapValue mv = row.putMap("mi");
        for (int i = 0; i < 3; i++) {
            mv.put("k" + i, i);
        }

        RecordValue rv = row.putRecord("r");
        rv.put("ri", 1);
        rv.put("rs", "rs");
        av = row.putArray("a_r_ms");
        for (int i = 0; i < 3; i++) {
            rv = av.addRecord();
            rv.put("ri", i);
            mv = rv.putMap("rms");
            for (int j = 0; j < 3; j++) {
                mv.put("k" + j, "ms_" + j);
            }
        }

        rv = row.putRecord("r_m_al");
        rv.put("ri", 1);
        mv = rv.putMap("m_al");
        for (int i = 0; i < 3; i++) {
            av = mv.putArray("k" + i);
            for (int j = 0; j < 3; j++) {
                av.add((long)j);
            }
        }
        doDeserializeWithValueReader(row);

        row = table.createRow();
        row.put("id", 0);
        ((RowImpl)row).addMissingFields();
        doDeserializeWithValueReader(row);
    }

    /**
     * Test invalid types for each target FieldDef.Type.
     */
    @Test
    public void testInvalidTypes() {

        final oracle.nosql.driver.values.FieldValue[] values =
            new oracle.nosql.driver.values.FieldValue[] {

            new oracle.nosql.driver.values.ArrayValue(),
            binaryValue,
            trueValue,
            doubleValue,
            intValue,
            longValue,
            new oracle.nosql.driver.values.MapValue(),
            stringValue,
            timestampValue,
            numberValue,
            jsonNull,
            nullValue,
            emptyValue
        };

        FieldDef def;
        int[] validTypes;

        /* Binary */
        def = FieldDefFactory.createBinaryDef();
        validTypes = new int[] {
            TYPE_BINARY, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY,
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Fixed Binary */
        def = FieldDefFactory.createFixedBinaryDef(1);
        validTypes = new int[] {
            TYPE_BINARY, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Boolean */
        def = FieldDefFactory.createBooleanDef();
        values[TYPE_STRING] = new oracle.nosql.driver.values.StringValue("true");
        validTypes = new int[] {
            TYPE_BOOLEAN, TYPE_STRING, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Float */
        def = FieldDefFactory.createFloatDef();
        values[TYPE_STRING] =
            new oracle.nosql.driver.values.StringValue("1.2345E-10f");
        validTypes = new int[] {
            TYPE_DOUBLE, TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Double */
        def = FieldDefFactory.createDoubleDef();
        values[TYPE_STRING] =
            new oracle.nosql.driver.values.StringValue("1.2345678E100d");
        validTypes = new int[] {
            TYPE_DOUBLE, TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Integer */
        def = FieldDefFactory.createIntegerDef();
        values[TYPE_STRING] = new oracle.nosql.driver.values.StringValue("123");
        validTypes = new int[] {
            TYPE_DOUBLE, TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Long */
        def = FieldDefFactory.createLongDef();
        values[TYPE_STRING] =
            new oracle.nosql.driver.values.StringValue("1234567890123");
        validTypes = new int[] {
            TYPE_DOUBLE, TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* String */
        def = FieldDefFactory.createStringDef();
        values[TYPE_STRING] = stringValue;
        validTypes = new int[] {
            TYPE_STRING, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Timestamp */
        def = FieldDefFactory.createTimestampDef(0);
        values[TYPE_STRING] =
            new oracle.nosql.driver.values.StringValue("1970-01-01");
        validTypes = new int[] {
            TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_TIMESTAMP,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Number */
        def = FieldDefFactory.createNumberDef();
        values[TYPE_STRING] =
            new oracle.nosql.driver.values.StringValue("1.213457E1024");
        validTypes = new int[] {
            TYPE_DOUBLE, TYPE_INTEGER, TYPE_LONG, TYPE_STRING, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Json */
        def = FieldDefFactory.createJsonDef();
        values[TYPE_STRING] = stringValue;
        validTypes = new int[] {
            TYPE_ARRAY, TYPE_BINARY, TYPE_BOOLEAN, TYPE_DOUBLE, TYPE_INTEGER,
            TYPE_LONG, TYPE_MAP, TYPE_STRING, TYPE_TIMESTAMP, TYPE_NUMBER,
            TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Array(Integer) */
        def = FieldDefFactory.createArrayDef(FieldDefFactory.createIntegerDef());
        validTypes = new int[] {
            TYPE_ARRAY, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Map(Integer) */
        def = FieldDefFactory.createMapDef(FieldDefFactory.createIntegerDef());
        validTypes = new int[] {
            TYPE_MAP, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);

        /* Record */
        def = TableBuilder.createRecordBuilder("rec").addInteger("rid").build();
        validTypes = new int[] {
            TYPE_MAP, TYPE_JSON_NULL, TYPE_NULL, TYPE_EMPTY
        };
        doWriteValuesToProxy(values, validTypes, def);
    }

    /**
     * Test Null, Json Null or Empty values
     */
    @Test
    public void testSpecialValues() throws Exception {

        oracle.nosql.driver.values.MapValue row;

        TableImpl table = TableBuilder.createTableBuilder("jsonTest")
            .addInteger("id")
            .addString("name")
            .addJson("json", null)
            .primaryKey("id")
            .buildTable();

        IndexImpl idx1 = new IndexImpl("idx1", table,
            Arrays.asList("json.children[].age", "json.address.zipcode", "name"),
            Arrays.asList(FieldDef.Type.STRING, FieldDef.Type.STRING, null),
            null);

        /*
         * Record value:
         *   {
         *       "id" : 1,
         *       "name" : null
         *       "json" : {
         *           "address" : {
         *               "city" : "San Fransisco",
         *               "state" : "CA",
         *               "street" : <json-null>
         *            },
         *            "phones" : ["400-123-4567", <json-null>, <json-null>],
         *            "children: [         *
         *              {"name": "Tomy", "school": <json-null>},
         *              <json-null>
         *            ]
         *            "email" : <json-null>
         *        },
         *   }
         */
        row = new oracle.nosql.driver.values.MapValue();
        row.put("id", 1);

        oracle.nosql.driver.values.MapValue jsonValue =
            new oracle.nosql.driver.values.MapValue();
        String json = "{\"city\":\"San Fransisco\",\"state\":\"CA\"," +
                      " \"street\":null}";
        jsonValue.put("address", JsonUtils.createValueFromJson(json, null))
                 .put("phones",
                     new oracle.nosql.driver.values.ArrayValue()
                         .add("400-123-4567")
                         .add(jsonNull)
                         .add(jsonNull))
                 .put("children",
                     new oracle.nosql.driver.values.ArrayValue()
                         .add(new oracle.nosql.driver.values.MapValue()
                             .put("name", "Tomy")
                             .put("school", jsonNull))
                         .add(jsonNull))
                 .put("email", jsonNull);
        row.put("json", jsonValue);
        row.put("name", nullValue);

        RecordValue expValue = table.createRowFromJson(row.toJson(), false);
        doWriteToProxy(row, expValue.getDefinition(), expValue);

        /*
         * Index key record:
         *   {
         *      "json.user[].name":<EMPTY>,
         *      "json.address.zipcode":null,
         *      "name":null
         *   }
         */
        row = new oracle.nosql.driver.values.MapValue();
        row.put("json.children[].age", emptyValue);
        row.put("json.address.zipcode", jsonNull);
        row.put("name", nullValue);

        IndexKey expKey = idx1.createIndexKey();
        expKey.putEMPTY("json.children[].age");
        expKey.putJsonNull("json.address.zipcode");
        expKey.putNull("name");

        doWriteToProxy(row, expKey.getDefinition(), expKey.asRecord());
    }

    /**
     * Test on Json type value.
     */
    @Test
    public void testJsonValue() {
        FieldDef def = TableBuilder.createRecordBuilder("jsonTest")
            .addInteger("id")
            .addJson("json", null)
            .build();

        oracle.nosql.driver.values.MapValue record =
            new oracle.nosql.driver.values.MapValue();
        oracle.nosql.driver.values.ArrayValue arrayValue =
            new oracle.nosql.driver.values.ArrayValue();
        oracle.nosql.driver.values.MapValue mapValue =
            new oracle.nosql.driver.values.MapValue();

        record.put("id", 0);

        /* Simple type values */
        record.put("json", trueValue);
        roundTrip(record, def);

        record.put("json", intValue);
        roundTrip(record, def);

        record.put("json", longValue);
        roundTrip(record, def);

        record.put("json", doubleValue);
        roundTrip(record, def);

        record.put("json", stringValue);
        roundTrip(record, def);

        record.put("json", numberValue);
        roundTrip(record, def);

        record.put("json", nullValue);
        roundTrip(record, def);

        record.put("json", jsonNull);
        roundTrip(record, def);

        record.put("json", binaryValue);
        roundTrip(record, def,
            JsonUtils.createValueFromJson(record.toJson(), null));

        /* Array */
        arrayValue.add(1).add(2).add(3).add(jsonNull);
        record.put("json", arrayValue);
        roundTrip(record, def);

        oracle.nosql.driver.values.MapValue nestedMap =
            new oracle.nosql.driver.values.MapValue();
        nestedMap.put("f", 1.123f).put("bool", false).put("jn", jsonNull);

        /* Map with nested array and map */

        mapValue.put("s", "string value")
                .put("i", 1234)
                .put("v", jsonNull)
                .put("bi", binaryValue)
                .put("array", arrayValue)
                .put("map", nestedMap);
        record.put("json", mapValue);
        roundTrip(record, def,
            JsonUtils.createValueFromJson(record.toJson(), null));

        String complexJson =
        "{"+
            "\"id\":0,"+
            "\"record\" : { \"int\" : 20, \"string\" : \"aef\"," +
            "               \"bool\" : true },"+
            "\"info\":"+
            "{"+
                "\"firstName\":\"first0\", \"lastName\":\"last0\",\"age\":10,"+
                "\"address\":"+
                "{"+
                "\"city\": \"San Fransisco\","+
                "\"state\"  : \"CA\","+
                "\"phones\" : [ { \"areacode\" : 408, \"number\" : 50," +
                               "  \"kind\" : \"home\" },"+
                               "{ \"areacode\" : 650, \"number\" : 51," +
                               "  \"kind\" : \"work\" },"+
                               "{ \"areacode\" : null, \"number\" : 52," +
                               "  \"kind\" : \"home\" },"+
                               "{ \"areacode\" : 510, \"number\" : 53," +
                               "  \"kind\" : \"home\" },"+
                               "{ \"areacode\" : 415, \"number\" : 54 },"+
                               "\"650-234-4556\","+
                               "650234455"+
                               "]"+
                               "},"+
                "\"children\":"+
                "{"+
                    "\"Anna\" : { \"age\" : 10, \"school\" : \"sch_1\"," +
                    " \"friends\" : [\"Anna\", \"John\", \"Maria\"]},"+
                    "\"Lisa\" : { \"age\" : 12," +
                    " \"friends\" : [\"Ada\"]},"+
                    "\"Mary\" : { \"age\" : 7,  \"school\" : \"sch_3\"," +
                    " \"friends\" : [\"Anna\", \"Mark\"]}"+
                "}"+
           "}"+
       "}";

        oracle.nosql.driver.values.FieldValue value;
        value = JsonUtils.createValueFromJson(complexJson, null);
        record.put("json", value);
        roundTrip(record, def);
    }

    /*
     * Test utility methods for numeric types conversion in ProxySerialization.
     */
    @Test
    public void testNumericConversion() {

        /* int to float */
        intToFloatTest();

        /* long to int */
        longToIntTest();

        /* long to float */
        longToFloatTest();

        /* long to double */
        longToDoubleTest();

        /* double to int */
        doubleToIntTest();

        /* double to long */
        doubleToLongTest();

        /* double to float */
        doubleToFloatTest();

        /* decimal to float */
        decimalToFloatTest();

        /* decimal to double */
        decimalToDoubleTest();
    }

    private void intToFloatTest() {
        int[] valid = new int[] {
            -8388608,
            -7654321,
            0,
            8388608,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE
        };
        for (int val : valid) {
            float fltVal = ProxySerialization.intToFloat(val);
            assertTrue(Float.compare(val, fltVal) == 0);
        }

        int[] invalid = new int[] {
            -123456789,
            123456789,
            Integer.MAX_VALUE - 1,
            Integer.MIN_VALUE + 1
        };
        for (int val : invalid) {
            try{
                ProxySerialization.intToFloat(val);
                fail("intToFloat should fail: " + val);
            } catch (Exception ex) {
            }
        }
    }

    private void longToIntTest() {
        long[] valid = new long[] {
            Integer.MIN_VALUE, Integer.MAX_VALUE, 0L, 123456789L
        };
        for (long val : valid) {
            int ival = ProxySerialization.longToInt(val);
            assertEquals(val, ival);
        }

        long[] invalid = new long[] {
            (long)Integer.MIN_VALUE - 1,
            (long)Integer.MAX_VALUE + 1,
            12345678901L
        };
        for (long val : invalid) {
            try{
                ProxySerialization.longToInt(val);
                fail("longToInt should fail: " + val);
            } catch (Exception ex){
            }
        }
    }

    private void longToFloatTest() {
        long[] valid = new long[] {
            -8388608,
            0,
            1234567,
            8388608,
            Long.MAX_VALUE,
            Long.MIN_VALUE
        };
        for (long val : valid) {
            float fltVal = ProxySerialization.longToFloat(val);
            assertTrue(Float.compare(val, fltVal) == 0);
        }

        long[] invalid = new long[] {
            -123456789,
            123456789,
            Long.MAX_VALUE - 1,
            Long.MIN_VALUE + 1
        };
        for (long val : invalid) {
            try{
                ProxySerialization.longToFloat(val);
                fail("longToFloat should fail: " + val);
            } catch (Exception ex) {
            }
        }
    }

    private void longToDoubleTest() {
        long[] valid = new long[] {
            0x80000000000000L,
            0xFF80000000000000L,
            0L,
            Long.MIN_VALUE,
            Long.MAX_VALUE
        };
        for (long val : valid) {
            double dblVal = ProxySerialization.longToDouble(val);
            assertTrue(Double.compare(dblVal, val) == 0);
        }
        long[] invalid = new long[] {
            0x80000000000001L,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE - 1
        };
        for (long val : invalid) {
            try{
                ProxySerialization.longToDouble(val);
                fail("longToDouble should fail: " + val);
            } catch (Exception ex){
            }
        }
    }

    private void doubleToIntTest() {
        double[] valid = new double[] {
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0.0d,
            1.23456789E8d,
            -1234.0d
        };
        for (double val : valid) {
            int intVal = ProxySerialization.doubleToInt(val);
            assertTrue(Double.compare(val, intVal) == 0);
        }

        double[] invalid = new double[] {
            1.23456789E7,
            (long)Integer.MAX_VALUE + 1,
            Float.MIN_VALUE,
            Double.MAX_VALUE
        };
        for (double val : invalid) {
            try{
                ProxySerialization.doubleToInt(val);
                fail("doubleToInt should fail: " + val);
            } catch (Exception ex){
            }
        }
    }

    private void doubleToLongTest() {
        double[] valid = new double[] {
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0.0d,
            1.234567890123456789E18d,
            -12345678901012.0d
        };
        for (double val : valid) {
            long longVal = ProxySerialization.doubleToLong(val);
            assertTrue(Double.compare(val, longVal) == 0);
        }

        double[] invalid = new double[] {
            1.2345678901234567E15,
            1.1d,
            Double.MIN_VALUE,
            Double.MAX_VALUE
        };
        for (double val : invalid) {
            try{
                ProxySerialization.doubleToLong(val);
                fail("doubleToLong should fail: " + val);
            } catch (Exception ex){
            }
        }
    }

    private void doubleToFloatTest() {
       float[] valid = new float[] {
           Float.NaN,
           Float.NEGATIVE_INFINITY,
           Float.POSITIVE_INFINITY,
           Float.MAX_VALUE,
           -Float.MAX_VALUE,
           Float.MIN_VALUE,
           -Float.MIN_VALUE,
           0f,
           1.23456E-14f
        };
        for (double val : valid) {
            float fltVal = ProxySerialization.doubleToFloat(val);
            assertTrue(Double.compare(val, fltVal) == 0);
        }

        double[] invalid = new double[] {
            Float.MAX_VALUE * 2d,
            Float.MIN_VALUE / 2d
        };
        for (double val : invalid) {
            try{
                ProxySerialization.doubleToFloat(val);
                fail("doubleToFloat should fail: " + val);
            } catch (Exception ex){
            }
        }
    }

    private void decimalToFloatTest() {
        BigDecimal[] valid = new BigDecimal[] {
            new BigDecimal("1.4E-45"),
            new BigDecimal("3.4028235E38"),
            BigDecimal.ZERO,
            BigDecimal.valueOf(Integer.MAX_VALUE),
            new BigDecimal("1.23456E-14")
        };
        for (BigDecimal val : valid) {
            ProxySerialization.decimalToFloat(val);
        }

        BigDecimal[] invalid = new BigDecimal[] {
            BigDecimal.valueOf(Double.MAX_VALUE)
        };
        for (BigDecimal val : invalid) {
            try{
                ProxySerialization.decimalToFloat(val);
                fail("decimalToFloat should fail: " + val);
            } catch (Exception ex) {
            }
        }
    }

    private void decimalToDoubleTest() {
        BigDecimal[] valid = new BigDecimal[] {
            new BigDecimal("1.234567890123456E15"),
            new BigDecimal("1.12345678"),
            BigDecimal.valueOf(Double.MIN_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE),
            BigDecimal.valueOf(Integer.MIN_VALUE),
            BigDecimal.valueOf(9223372036854774L),
            BigDecimal.ONE,
            new BigDecimal("1.234567890123456E309"), /* Infinity */
            new BigDecimal("-1.234567890123456E309") /* -Infinity */
        };
        for (BigDecimal val : valid) {
            ProxySerialization.decimalToDouble(val);
        }

        BigDecimal[] invalid = new BigDecimal[] {
            new BigDecimal("1.12345678E-325"),
            new BigDecimal("123456789012345678901234567890")
        };
        for (BigDecimal val : invalid) {
            try{
                ProxySerialization.decimalToDouble(val);
                fail("decimalToDouble should fail: " + val);
            } catch (Exception ex) {
            }
        }
    }

    @Test
    public void testSerializeWithValueSerializer() {
        TableImpl testTable = TableBuilder.createTableBuilder("foo")
            .addInteger("i")
            .addLong("l")
            .addString("s")
            .addDouble("d")
            .addFloat("f")
            .addBoolean("bl")
            .addBinary("bi")
            .addFixedBinary("fix", 20)
            .addNumber("n")
            .addTimestamp("ts", 3)
            .addField("mi",
                TableBuilder.createMapBuilder("mi").addInteger().build())
            .addField("as",
                TableBuilder.createArrayBuilder("as").addString().build())
            .addField("r", TableBuilder.createRecordBuilder("r")
                    .addInteger("ri")
                    .addString("rs")
                    .addField("rma",
                        TableBuilder.createMapBuilder("mai")
                            .addField(TableBuilder.createArrayBuilder("ai")
                                      .addInteger()
                                      .build())
                        .build())
                    .build())
            .addInteger("k1")
            .addInteger("m1")
            .addJson("json", null)
            .primaryKey("m1", "k1")
            .primaryKeySize("k1", 4)
            .shardKey("m1")
            .buildTable();

        oracle.nosql.driver.values.MapValue mv = initRow();
        doWriteRowToProxyWithValueSerializer(testTable, mv, true /* exact */);

        mv = new oracle.nosql.driver.values.MapValue();
        mv.put("m1", 1).put("k1", 2);
        doWriteRowToProxyWithValueSerializer(testTable, mv, true /* exact */);

        for (int pos = 0; pos < testTable.getFields().size(); pos++) {
            if (!testTable.isPrimKeyAtPos(pos)) {
                mv.put(testTable.getFields().get(pos), nullValue);
            }
        }
        doWriteRowToProxyWithValueSerializer(testTable, mv, true /* exact */);

        TableImpl jsonTable = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addJson("json", null)
            .primaryKey("id")
            .buildTable();

        mv = new oracle.nosql.driver.values.MapValue();
        mv.put("id", 1);

        mv.put("json", 1);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", "test");
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", true);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", genBytes(20));
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", Double.MAX_VALUE);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        BigDecimal dec =
            new BigDecimal("1234567890123456789012345678901234567890");
        mv.put("json", dec);
        RowImpl expRow = jsonTable.createRow();
        expRow.put("id", 1);
        expRow.putNumber("json", dec);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, expRow,
                                             true /* exact */,
                                             true /* shouldSucceed */);

        mv.put("json", new Timestamp(System.currentTimeMillis()));
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", jsonNull);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        mv.put("json", nullValue);
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        String json = "{\"i\":1," +
            "\"r\":{" +
                "\"s\":\"name\"," +
                "\"map\":{" +
                    "\"m1r\":{\"m1s\":\"a string value\", \"m1null\":null}," +
                    "\"m2a\":[\"a1\",\"a2\"]," +
                    "\"m3m\":{\"m3k1\":1, \"m3k2\":2}}," +
                "\"ar\":[-100, 0, -1234432443241431414]}, " +
            "\"nv\":null}";
        mv.put("json", JsonUtils.createValueFromJson(json, null));
        doWriteRowToProxyWithValueSerializer(jsonTable, mv, true /* exact */);

        /*
         * Invalid cases
         */

        testTable = TableBuilder.createTableBuilder("foo")
            .addInteger("i")
            .addString("s", null, false, "n/a")
            .addInteger("k1")
            .addInteger("m1")
            .primaryKey("m1", "k1")
            .primaryKeySize("k1", 4)
            .shardKey("m1")
            .buildTable();

        mv = new oracle.nosql.driver.values.MapValue();
        writeRowToProxyWithValueSerializer(testTable, mv,
                                           true /* exact */,
                                           false /* shouldSucceed */);

        mv.put("k1", 1);
        writeRowToProxyWithValueSerializer(testTable, mv,
                                           true /* exact */,
                                           false /* shouldSucceed */);

        mv.put("m1", 1);
        mv.put("i", "String");
        writeRowToProxyWithValueSerializer(testTable, mv,
                                           true /* exact */,
                                           false /* shouldSucceed */);

        mv.put("i", 1);
        mv.put("s", nullValue);
        writeRowToProxyWithValueSerializer(testTable, mv,
                                           true /* exact */,
                                           false /* shouldSucceed */);
    }

    @Test
    public void testWriteRowToProxy() {
        oracle.nosql.driver.values.MapValue mv;
        TableImpl table;
        String json;

         /* Case1: Row contains array(record) and map(record) */
        table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addField("address",
                TableBuilder.createRecordBuilder("address")
                    .addString("city")
                    .addString("state")
                    .addField("phones",
                        TableBuilder.createArrayBuilder()
                            .addField(
                                TableBuilder.createRecordBuilder("prec")
                                .addInteger("number")
                                .addInteger("areaCode")
                                .addEnum("kind",
                                    new String[]{"work", "home"}, null)
                                .build())
                            .build())
                    .build())
            .addField("children",
                TableBuilder.createMapBuilder()
                    .addField(TableBuilder.createRecordBuilder("crec")
                            .addField("friends",
                                TableBuilder.createArrayBuilder()
                                    .addString()
                                    .build())
                            .addString("school")
                            .addLong("age")
                            .build())
                .build())
            .primaryKey("id")
            .buildTable();

        json = "{ " +
            "  \"id\":0, " +
            "  \"firstName\":\"first0\", " +
            "  \"lastName\":\"last0\"," +
            "  \"age\":10," +
            "  \"address\": {" +
            "    \"city\": \"San Fransisco\"," +
            "    \"state\" :\"CA\"," +
            "    \"phones\":[" +
            "      { \"areacode\":408,\"number\":50, \"kind\":\"work\" }," +
            "      { \"areacode\":650,\"number\":51, \"kind\":\"work\" }," +
            "      { \"areacode\":650,\"number\":52, \"kind\":\"home\" }," +
            "      { \"areacode\":510,\"number\":53, \"kind\":\"home\" }," +
            "      { \"areacode\":415,\"number\":54, \"kind\":null }]" +
            "  }," +
            "  \"children\": {" +
            "    \"John\":{ \"age\":10, \"school\":\"sch_1\"," +
            "               \"friends\":[\"Anna\", \"John\", \"Maria\"]}," +
            "    \"Lisa\":{ \"age\":12, \"school\": null," +
            "               \"friends\":[\"Ada\"]}," +
            "    \"Mary\":{ \"age\":7, \"school\":\"sch_3\"," +
            "               \"friends\":[\"Anna\", \"Mark\"]}" +
            "  }" +
            "}";

        mv = (oracle.nosql.driver.values.MapValue)
             oracle.nosql.driver.values.MapValue.createFromJson(json, null);
        doWriteRowToProxyWithValueSerializer(table, mv, false /* exact */);

        /*
         * Case2: Row contains unknown fields, they should be ignored.
         */
        table = TableBuilder.createTableBuilder("foo")
                .addInteger("id")
                .addString("firstName")
                .addString("lastName")
                .addInteger("age")
                .addField("address",
                    TableBuilder.createRecordBuilder("address")
                        .addString("city")
                        .addString("state")
                        .addString("street")
                        .build())
                .addField("phones",
                     TableBuilder.createArrayBuilder()
                        .addField(TableBuilder.createRecordBuilder("prec")
                                .addString("areacode")
                                .addString("number")
                                .build())
                    .build())
                .addField("emails",
                    TableBuilder.createMapBuilder()
                    .addField(TableBuilder.createRecordBuilder("erec")
                            .addEnum("type",
                                     new String[] {"work", "other"}, null)
                            .addString("email")
                            .build())
                    .build())
                .primaryKey("id")
                .buildTable();

        json = "{" +
                "    \"id\": 0," +
                "    \"firstName\": \"first0\"," +
                "    \"lastName\": \"last0\"," +
                "    \"age\": 10," +
                "    \"invalidField0\": 0," +
                "    \"address\": {" +
                "        \"invalidField1\": 1," +
                "        \"street\": \"1 Oracle Way\"," +
                "        \"state\": \"CA\"," +
                "        \"city\": \"San Fransisco\"" +
                "    }," +
                "    \"phones\": [{" +
                "            \"number\": \"1231456\"," +
                "            \"invalidField2\": 2," +
                "            \"areacode\": \"781\"" +
                "        }," +
                "        {" +
                "            \"invalidField3\": 3," +
                "            \"areacode\": \"782\"," +
                "            \"number\": \"7654321\"" +
                "        }" +
                "    ]," +
                "    \"emails\": {" +
                "        \"email1\": {" +
                "            \"invalidField4\": 4," +
                "            \"email\": \"aaa@abc.com\"," +
                "            \"type\": \"work\"" +
                "        }," +
                "        \"email2\": {" +
                "            \"type\": \"other\"," +
                "            \"invalidField5\": 5," +
                "            \"email\": \"bbb@test.com\"" +
                "        }" +
                "    }" +
                "}";
        mv = (oracle.nosql.driver.values.MapValue)
                oracle.nosql.driver.values.MapValue.createFromJson(json, null);
        /*
         * Write to proxy using ValueSerializer to deserialize driver value
         */
        doWriteRowToProxyWithValueSerializer(table, mv, false /* exact */);

        /*
         * Write to proxy using readFieldValue() to deserialize driver value
         * to FieldValue
         */
        Row row = table.createRowFromJson(json, false);
        doWriteToProxy(mv, table.getRowDef(), row,
                       true /* shouldSucceed */);

        /*
         * Case3: key of map is empty string
         */
        table = TableBuilder.createTableBuilder("Boo")
                    .addInteger("id")
                    .addField("expenses",
                        TableBuilder.createMapBuilder()
                            .addInteger()
                            .build())
                    .primaryKey("id")
                    .buildTable();
        json = "{ \"id\":4, \"expenses\":{\"\":3, \"\\\"\":13}}";
        mv = (oracle.nosql.driver.values.MapValue)
                oracle.nosql.driver.values.MapValue.createFromJson(json, null);
        doWriteRowToProxyWithValueSerializer(table, mv, true /* exact */);
    }

    @Test
    public void testWriteArrayToRecord() {
        TableImpl table = TableBuilder.createTableBuilder("foo")
                .addInteger("id")
                .addString("name")
                .addInteger("age")
                .addField("address",
                    TableBuilder.createRecordBuilder("address")
                        .addString("city")
                        .addString("street")
                        .addInteger("buildingNo")
                        .build())
                .primaryKey("id")
                .buildTable();

        oracle.nosql.driver.values.ArrayValue rowArr;
        oracle.nosql.driver.values.MapValue rowMap;
        oracle.nosql.driver.values.ArrayValue addressArr;
        oracle.nosql.driver.values.MapValue addressMap;

        Row exp = table.createRow();
        exp.put("id", 1)
           .put("name", "Jack Wang")
           .put("age", 30)
           .put("address",
                table.getField("address").createRecord()
                    .put("city", "Burlington")
                    .put("street", "35 network drive")
                    .put("buildingNo", 95));

        addressArr = new oracle.nosql.driver.values.ArrayValue()
                        .add("Burlington")
                        .add("35 network drive")
                        .add(95);

        addressMap = new oracle.nosql.driver.values.MapValue()
                        .put("city", "Burlington")
                        .put("street", "35 network drive")
                        .put("buildingNo", 95);

        /*
         * Case1: Use array value for row, also use array for the nested record
         * "address".
         */
        rowArr = new oracle.nosql.driver.values.ArrayValue();
        rowArr.add(1)
              .add("Jack Wang")
              .add(30)
              .add(addressArr);
        doWriteRowToProxyWithValueSerializer(table, rowArr, exp, true, true);

        /*
         * Case2: Use array value for row, use map value for the nested
         *        record "address".
         */
        rowArr = new oracle.nosql.driver.values.ArrayValue();
        rowArr.add(1)
              .add("Jack Wang")
              .add(30)
              .add(addressMap);
        doWriteRowToProxyWithValueSerializer(table, rowArr, exp, true, true);

        /*
         * Case3: Use map value for row, use array value for the nested
         *        record "address".
         */
        rowMap = new oracle.nosql.driver.values.MapValue();
        rowMap.put("id", 1)
              .put("name", "Jack Wang")
              .put("age", 30)
              .put("address", addressArr);
        doWriteRowToProxyWithValueSerializer(table, rowArr, exp, true, true);

        /*
         * Case4: Test exact = false, use array value for row with an additional
         *        NULL value
         */
        rowArr = new oracle.nosql.driver.values.ArrayValue();
        rowArr.add(1)
              .add("Jack Wang")
              .add(30)
              .add(addressArr)
              .add(NullValue.getInstance());
        doWriteRowToProxyWithValueSerializer(table, rowArr, exp, false, true);

        /* Case5: use array value with "id" field only */
        rowArr = new oracle.nosql.driver.values.ArrayValue()
                    .add(new IntegerValue(1));
        exp = table.createRow();
        exp.put("id", 1);
        ((RowImpl)exp).addMissingFields();
        doWriteRowToProxyWithValueSerializer(table, rowArr, exp, false, true);

        /*
         * Negative case
         */

        /*
         * Case6: exact = true, array contains more or less elements than
         *        expected, expect 4 actual 1
         */
        rowArr = new oracle.nosql.driver.values.ArrayValue()
                    .add(1);
        doWriteRowToProxyWithValueSerializer(table, rowArr, null, true, false);

        /*
         * Case6: Invalid string for Integer: invalid
         */
        rowArr = new oracle.nosql.driver.values.ArrayValue()
                    .add("invalidForId");
        doWriteRowToProxyWithValueSerializer(table, rowArr, null, false, false);
    }

    private void doWriteRowToProxyWithValueSerializer(
            TableImpl table,
            oracle.nosql.driver.values.FieldValue value,
            boolean exact) {
        doWriteRowToProxyWithValueSerializer(table, value, exact, true);
    }

    private void doWriteRowToProxyWithValueSerializer(
            TableImpl table,
            oracle.nosql.driver.values.FieldValue value,
            boolean exact,
            boolean shouldSucceed) {
        doWriteRowToProxyWithValueSerializer(table, value, null /* expRow */,
                                             exact, shouldSucceed);
    }

    private void doWriteRowToProxyWithValueSerializer (
            TableImpl table,
            oracle.nosql.driver.values.FieldValue value,
            Row expRow,
            boolean exact,
            boolean shouldSucceed) {

        Row row = writeRowToProxyWithValueSerializer(table, value,
                                                     exact, shouldSucceed);
        if (shouldSucceed && expRow == null) {
            expRow = table.createRowFromJson(value.toJson(), false /* exact */);
            ((RowImpl)expRow).addMissingFields();
        }
        assertEquals(expRow, row);
    }

    private oracle.nosql.driver.values.MapValue initRow() {
        oracle.nosql.driver.values.MapValue row;
        row = new oracle.nosql.driver.values.MapValue();
        row.put("m1", 0);
        row.put("k1", 100);
        row.put("i", Integer.MAX_VALUE);
        row.put("l", Long.MAX_VALUE);
        row.put("s", "this a test string");
        row.put("d", Double.MAX_VALUE);
        row.put("f", Float.MAX_VALUE);
        row.put("bl", true);
        row.put("bi", genBytes(10));
        row.put("fix", genBytes(20));
        row.put("n", new BigDecimal("123456789012345678901234567890"));
        row.put("ts", new Timestamp(System.currentTimeMillis()));

        oracle.nosql.driver.values.MapValue mv =
            new oracle.nosql.driver.values.MapValue();
        for (int i = 0; i < 3; i++) {
            mv.put("k" + i, i);
        }
        row.put("mi", mv);

        oracle.nosql.driver.values.ArrayValue av =
            new oracle.nosql.driver.values.ArrayValue();
        for (int i = 0; i < 3; i++) {
            av.add("av" + i);
        }
        row.put("as", av);

        oracle.nosql.driver.values.MapValue rv =
            new oracle.nosql.driver.values.MapValue();
        rv.put("ri", 1);
        rv.put("rs", "rsv");

        mv = new oracle.nosql.driver.values.MapValue();
        for (int i = 0; i < 3; i++) {
            av = new oracle.nosql.driver.values.ArrayValue();
            for (int j = 0; j < 3; j++) {
                av.add(j * 100);
            }
            mv.put("k" + i, av);
        }
        rv.put("rma", mv);
        row.put("r", rv);
        String json = "{\"i\":1,\"r\":{\"s\":\"name\",\"ar\":[0,1]}," +
                      " \"a\":null}";
        row.put("json", JsonUtils.createValueFromJson(json, null));
        return row;
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    private void doWriteValuesToProxy (
        oracle.nosql.driver.values.FieldValue[] values,
        int[] validTypes,
        FieldDef fieldDef) {

        int j = 0;
        for (int i = 0; i < values.length; i++) {
            boolean isValid = (j < validTypes.length && i == validTypes[j]);
            doWriteToProxy(values[i], fieldDef, isValid);
            if (isValid) {
                j++;
            }
        }
    }

    /**
     * Value send from driver to proxy, then send back from proxy to driver.
     */
    private void roundTrip(oracle.nosql.driver.values.FieldValue driverValue,
                           FieldDef fieldDef) {
        roundTrip(driverValue, fieldDef, null);
    }

    private void roundTrip(oracle.nosql.driver.values.FieldValue driverValue,
                           FieldDef fieldDef,
                           oracle.nosql.driver.values.FieldValue expValue) {

        oracle.nosql.driver.values.FieldValue driverValue1;
        oracle.kv.table.FieldValue storeValue;

        storeValue = writeToProxy(driverValue, fieldDef,
                                  true /* shouldSucceed */);
        driverValue1 = writeToDriver(storeValue);

        /*
         * Verify the input value from driver with the the one read from
         * proxy. Use compareTo() instead of equals() because the latter
         * is not correct for BigDecimal (Number) types
         */
        oracle.nosql.driver.values.FieldValue exp =
            (expValue != null) ? expValue : driverValue;
        assertTrue("Wrong field value read\noriginal: " +
            driverValue.toJson() + "\nread: " + driverValue1.toJson(),
                   (driverValue1.compareTo(exp) == 0));
    }

    private void doDeserializeWithValueReader(Row row) {
        final ByteBuf buf = Unpooled.buffer();
        ByteOutputStream bos = new ByteOutputStream(buf);

        final TableImpl table = (TableImpl)row.getTable();
        final Version version =
            new Version(UUID.randomUUID(), 1, new RepNodeId(1, 1), 0x1L);
        final Value value = ((RowImpl)row).createValue();

        oracle.nosql.driver.values.FieldValue fval0 = null;
        oracle.nosql.driver.values.FieldValue fval1 = null;
        oracle.nosql.driver.util.ByteInputStream bis;

        /* Serialize row using ValueReader */
        RowReaderImpl reader = new RowReaderImpl(bos, row.getTable());
        for (String fname : table.getPrimaryKeyInternal()) {
            readKeyFieldValue(reader, fname, row.get(fname));
        }
        table.readRowFromValueVersion(reader, new ValueVersion(value, version));
        assertTrue(reader.done() > 0);
        assertEquals(reader.getVersion(), version);
        /* Deserialize to driver value */
        try {
            bis = new oracle.nosql.driver.util.NettyByteInputStream(buf);
            fval0 = BinaryProtocol.readFieldValue(bis);
        } catch (IOException ioe) {
            fail("Failed to deserialize fval0: " + ioe.getMessage());
        }

        /* Serialize row using ValueReader */
        buf.clear();
        bos = new ByteOutputStream(buf);
        try {
            writeFieldValue(bos, row);
            /* Deserialize to driver value */
            bis = new oracle.nosql.driver.util.NettyByteInputStream(buf);
            fval1 = BinaryProtocol.readFieldValue(bis);
        } catch (IOException ioe) {
            fail("Failed to deserialize fval1: " + ioe.getMessage());
        }

        assertEquals(fval0, fval1);
    }


    private static void readKeyFieldValue(ValueReader<?> reader,
                                          String fname,
                                          FieldValue value) {

        switch (value.getType()) {
        case BOOLEAN:
            reader.readBoolean(fname, value.asBoolean().get());
            break;
        case DOUBLE:
            reader.readDouble(fname, value.asDouble().get());
            break;
        case ENUM:
            reader.readEnum(fname, value.getDefinition(),
                value.asEnum().getIndex());
            break;
        case FLOAT:
            reader.readFloat(fname, value.asFloat().get());
            break;
        case INTEGER:
            reader.readInteger(fname, value.asInteger().get());
            break;
        case LONG:
            reader.readLong(fname, value.asLong().get());
            break;
        case STRING:
            reader.readString(fname, value.asString().get());
            break;
        case TIMESTAMP:
            reader.readTimestamp(fname, value.getDefinition(),
                ((TimestampValueImpl)value).getBytes());
            break;
        case NUMBER:
            reader.readNumber(fname, ((NumberValueImpl)value).getBytes());
            break;
        default:
            throw new IllegalStateException("Unexpected type: " +
                value.getType());
        }
    }

    /**
     * Serialize value on driver, deserialize value on proxy side.
     */
    private void doWriteToProxy(
            oracle.nosql.driver.values.FieldValue driverValue,
            FieldDef fieldDef,
            boolean shouldSucceed) {
        doWriteToProxy(driverValue, fieldDef, null, shouldSucceed);
    }

    private void doWriteToProxy(
            oracle.nosql.driver.values.FieldValue driverValue,
            FieldDef fieldDef,
            oracle.kv.table.FieldValue expValue) {
        doWriteToProxy(driverValue, fieldDef, expValue,
                       true /* shouldSucceed */);
    }

    private void doWriteToProxy(
             oracle.nosql.driver.values.FieldValue driverValue,
             FieldDef fieldDef,
             oracle.kv.table.FieldValue expValue,
             boolean shouldSucceed) {

        oracle.kv.table.FieldValue storeValue =
            writeToProxy(driverValue, fieldDef, shouldSucceed);
        if (expValue != null) {
            assertTrue(storeValue.equals(expValue));
        }
    }

    private oracle.kv.table.FieldValue
        writeToProxy(oracle.nosql.driver.values.FieldValue driverValue,
                     FieldDef fieldDef,
                     boolean shouldSucceed) {

        testBuf.clear();
        try {
            oracle.nosql.driver.util.ByteOutputStream bos =
                new oracle.nosql.driver.util.NettyByteOutputStream(testBuf);
            BinaryProtocol.writeFieldValue(bos, driverValue);
            ByteInputStream bis = new ByteInputStream(testBuf);
            oracle.kv.table.FieldValue storeValue =
                readFieldValue(bis, fieldDef, true, false);
            if (!shouldSucceed) {
                fail("Expect to catch IAE but not, driver value's type is " +
                    driverValue.getType() + ", kv type is " +
                    fieldDef.getType());
            }
            return storeValue;
        } catch (IOException e) {
            fail("Write value from driver to proxy failed: " + e);
        } catch (IllegalArgumentException iae) {
            if (shouldSucceed) {
                fail("Expect to succeed but fail: " + iae.getMessage() +
                    ", driver value's type is " + driverValue.getType() +
                    ", kv type is " + fieldDef.getType());
            }
        }
        return null;
    }

    private oracle.nosql.driver.values.FieldValue
        writeToDriver(oracle.kv.table.FieldValue storeValue) {

        testBuf.clear();
        try {
            ByteOutputStream bos = new ByteOutputStream(testBuf);
            writeFieldValue(bos, storeValue);
            oracle.nosql.driver.util.ByteInputStream bis =
                new oracle.nosql.driver.util.NettyByteInputStream(testBuf);
            return BinaryProtocol.readFieldValue(bis);
        } catch (IOException e) {
            fail("Write value from proxy to dirver failed: " + e);
        }
        return null;
    }

    private Row writeRowToProxyWithValueSerializer(
                TableImpl table,
                oracle.nosql.driver.values.FieldValue driverRow,
                boolean exact,
                boolean shouldSucceed) {

        try {
            testBuf.clear();
            oracle.nosql.driver.util.ByteOutputStream bos =
                new oracle.nosql.driver.util.NettyByteOutputStream(testBuf);
            BinaryProtocol.writeFieldValue(bos, driverRow);

            ByteInputStream bis = new ByteInputStream(testBuf);
            byte type = bis.readByte();
            if (driverRow instanceof oracle.nosql.driver.values.MapValue) {
                assertEquals(TYPE_MAP, type);
            } else if (driverRow instanceof
                       oracle.nosql.driver.values.ArrayValue){
                assertEquals(TYPE_ARRAY, type);
            }
            RowSerializer serializer =
                new RowSerializerImpl(bis, type, table, -1, -1, false, exact);
            Key key = table.createKeyInternal(serializer, false);
            Value value = table.createValueInternal(serializer);
            Row row = table.createRowFromBytes(key.toByteArray(),
                                               value.toByteArray(),
                                               false);
            if (!shouldSucceed) {
                fail("Expect to fail but succeed");
            }
            return row;
        } catch (IOException e) {
            fail("testRecordParser failed: " + e);
        } catch (RuntimeException re) {
            if (shouldSucceed) {
                fail("Expect to succeed but failed: " + re.getMessage());
            }
        }
        return null;
    }

    private byte[] genByteArray(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    private String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append('A' + (i % 26));
        }
        return sb.toString();
    }
}
