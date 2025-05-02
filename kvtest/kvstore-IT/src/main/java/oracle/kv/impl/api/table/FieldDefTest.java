/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.BinaryDef;
import oracle.kv.table.BooleanDef;
import oracle.kv.table.DoubleDef;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.FloatDef;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerDef;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.LongDef;
import oracle.kv.table.LongValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.StringDef;
import oracle.kv.table.StringValue;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;

import org.junit.Test;

/**
 * Test FieldDefImpl and its subclasses.  FieldDefImpl and related classes
 * comprise the "schema" for tables.  There are simple classes that represent a
 * single value such as Integer, and complex classes to represent multi-valued
 * objects such as maps and records.
 */
public class FieldDefTest extends TestBase {

    @SuppressWarnings("deprecation")
    @Test
    public void testInteger() {
        IntegerDef idef = new IntegerDefImpl();
        assertDefaults(idef, FieldDef.Type.INTEGER, true);
        assertNull(idef.getMin());
        assertNull(idef.getMax());
        assertEquals(encodingLength(idef), 0);
        idef.asInteger();

        IntegerDef idef1 = new IntegerDefImpl("test_integer", -75, 1000000);
        assertEquals(-75, (int) idef1.getMin());
        assertEquals(1000000, (int) idef1.getMax());
        assertTrue(idef1.isInteger());
        assertTrue(encodingLength(idef1) > 0);
        assertTrue(idef1.isNumeric());
        assertTrue(idef1.isAtomic());
        assertFalse(idef1.isComplex());

        assertTrue(idef1.equals(idef));

        /*
         * Test min/max range
         */
        @SuppressWarnings("unused")
        IntegerValue value = idef1.createInteger(1);
        try {
            value = idef1.createInteger(-76);
            fail();
        } catch (IllegalArgumentException iae) {
        }
        try {
            value = idef1.createInteger(1000001);
            fail();
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Json ser-de
         */
        testJson(idef1);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testLong() {
        LongDef ldef = new LongDefImpl();
        assertDefaults(ldef, FieldDef.Type.LONG, true);
        assertNull(ldef.getMin());
        assertNull(ldef.getMax());
        assertEquals(encodingLength(ldef), 0);
        ldef.asLong();

        /*
         * Non-default constructor
         */
        LongDef ldef1 = new LongDefImpl("test_long", -75L, 1000000L);
        assertTrue(ldef.equals(ldef1));
        assertEquals(-75L, (long) ldef1.getMin());
        assertEquals(1000000L, (long) ldef1.getMax());
        assertTrue(ldef1.isLong());
        assertTrue(encodingLength(ldef1) > 0);
        assertTrue(ldef1.isNumeric());
        assertTrue(ldef1.isAtomic());
        assertFalse(ldef1.isComplex());
        /*
         * Test min/max range
         */
        @SuppressWarnings("unused")
        LongValue value = ldef1.createLong(1);
        try {
            value = ldef1.createLong(-76L);
            fail();
        } catch (IllegalArgumentException iae) {
        }
        try {
            value = ldef1.createLong(1000001L);
            fail();
        } catch (IllegalArgumentException iae) {
        }
        testJson(ldef);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDouble() {
        DoubleDef ddef = new DoubleDefImpl();
        assertDefaults(ddef, FieldDef.Type.DOUBLE, true);
        assertNull(ddef.getMin());
        assertNull(ddef.getMax());
        ddef.asDouble();

        /*
         * Non-default constructor
         */
        DoubleDef ddef1 = new DoubleDefImpl("test_double", -75000.56, 1000000D);
        assertEquals(-75000.56, ddef1.getMin(), 0.0);
        assertEquals(1000000D, ddef1.getMax(), 0.0);
        assertTrue(ddef1.isDouble());
        assertTrue(ddef1.isNumeric());
        assertTrue(ddef1.isAtomic());
        assertFalse(ddef1.isComplex());
        DoubleDef newDef = new DoubleDefImpl("test_double",
                                             -75000.56, 1000000D);
        assertTrue(newDef.equals(ddef));
        assertTrue(ddef1.equals(ddef));

        /*
         * Test min/max range
         */
        @SuppressWarnings("unused")
        DoubleValue value = ddef1.createDouble(1D);
        try {
            value = ddef1.createDouble(-76000D);
            fail();
        } catch (IllegalArgumentException iae) {
        }
        try {
            value = ddef1.createDouble(1000001D);
            fail();
        } catch (IllegalArgumentException iae) {
        }
        testJson(newDef);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testFloat() {
        FloatDef fdef = new FloatDefImpl();
        assertDefaults(fdef, FieldDef.Type.FLOAT, true);
        assertNull(fdef.getMin());
        assertNull(fdef.getMax());
        fdef.asFloat();

        /*
         * Non-default constructor
         */
        FloatDef fdef1 = new FloatDefImpl("test_float", -75000.56F, 1000000F);
        assertEquals(-75000.56F, fdef1.getMin(), 0.0);
        assertEquals(1000000D, fdef1.getMax(), 0.0);
        assertTrue(fdef1.isFloat());
        assertTrue(fdef1.isNumeric());
        assertTrue(fdef1.isAtomic());
        assertFalse(fdef1.isComplex());
        FloatDef newDef = new FloatDefImpl("test_float",
                                             -75000.56F, 1000000F);
        assertTrue(newDef.equals(fdef1));
        assertTrue(fdef.equals(fdef1));

        /*
         * Test min/max range
         */
        @SuppressWarnings("unused")
        FloatValue value = fdef1.createFloat(1F);
        try {
            value = fdef1.createFloat(-76000F);
            fail();
        } catch (IllegalArgumentException iae) {
        }

        try {
            value = fdef1.createFloat(1000001F);
            fail();
        } catch (IllegalArgumentException iae) {
        }

        testJson(newDef);
    }

    @Test
    public void testBoolean() {
        BooleanDef boolDef = new BooleanDefImpl();
        assertDefaults(boolDef, FieldDef.Type.BOOLEAN, true);
        assertTrue(boolDef.isBoolean());
        assertFalse(boolDef.isNumeric());
        assertTrue(boolDef.isAtomic());
        assertFalse(boolDef.isComplex());
        boolDef.asBoolean();

        testJson(boolDef);
    }

    @Test
    public void testString() {
        StringDef stringDef = new StringDefImpl();
        assertDefaults(stringDef, FieldDef.Type.STRING, true);
        assertTrue(stringDef.isString());
        assertFalse(stringDef.isNumeric());
        assertTrue(stringDef.isAtomic());
        assertFalse(stringDef.isComplex());
        stringDef.asString();

        /*
         * Non-default constructor
         */
        stringDef = new StringDefImpl("test_string",
                                      "2012", "2012.12.30", true, true);

        assertTrue(stringDef.isValidKeyField());

        /*
         * Test min/max range
         */
        @SuppressWarnings("unused")
        StringValue value = stringDef.createString("2012");
        value = stringDef.createString("2012.12.30");
        try {
            value = stringDef.createString("2011");
            fail();
        } catch (IllegalArgumentException iae) {
        }
        try {
            value = stringDef.createString("2012.12.31");
            fail();
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Set min/max exclusive and test that
         */
        stringDef = new StringDefImpl("test_string",
                                      "2012", "2012.12.30", false, false);
        try {
            value = stringDef.createString("2012");
            fail();
        } catch (IllegalArgumentException iae) {
        }
        try {
            value = stringDef.createString("2012.12.30");
            fail();
        } catch (IllegalArgumentException iae) {
        }

        assertTrue(stringDef.equals(new StringDefImpl()));

        testJson(stringDef);
    }

    @Test
    public void testBinary() {
        BinaryDef binDef = new BinaryDefImpl();
        assertDefaults(binDef, FieldDef.Type.BINARY, false);
        assertTrue(binDef.isBinary());
        assertFalse(binDef.isNumeric());
        assertTrue(binDef.isAtomic());
        assertFalse(binDef.isComplex());
        binDef.asBinary();

        /*
         * Non-default constructor
         */
        binDef = new BinaryDefImpl("test_binary");

        assertFalse(binDef.isValidKeyField());
        testJson(binDef);
    }

    @Test
    public void testFixedBinary() {
        /*
         * Name and size are required
         */
        FixedBinaryDef fBinDef = new FixedBinaryDefImpl("fixed", 1);
        assertDefaults(fBinDef, FieldDef.Type.FIXED_BINARY, false);
        assertFalse(fBinDef.isBinary());
        assertTrue(fBinDef.isFixedBinary());
        assertFalse(fBinDef.isNumeric());
        assertTrue(fBinDef.isAtomic());
        assertFalse(fBinDef.isComplex());
        fBinDef.asFixedBinary();

        /*
         * Non-default constructor
         */
        fBinDef = new FixedBinaryDefImpl("fixed", 10);

        assertFalse(fBinDef.isValidKeyField());
        testJson(fBinDef);

        try {
            fBinDef = new FixedBinaryDefImpl(null, 10);
            fail("Name cannot be null");
        } catch (IllegalArgumentException iae) {}
        try {
            fBinDef = new FixedBinaryDefImpl("fixed", 0);
            fail("Size must be positive");
        } catch (IllegalArgumentException iae) {}
    }

    @SuppressWarnings("unused")
	@Test
    public void testEnum() {
        EnumDef enumDef = new EnumDefImpl("enum", new String[] {"a", "b", "c"});
        assertDefaults(enumDef, FieldDef.Type.ENUM, true);
        enumDef.asEnum();

        /*
         * Non-default constructor
         */
        enumDef = new EnumDefImpl("enum", new String[] {"a", "b", "c"},
                                  "non-default enum");
        assertTrue(enumDef.isValidKeyField());
        assertTrue(enumDef.isEnum());
        assertFalse(enumDef.isNumeric());
        assertTrue(enumDef.isAtomic());
        assertFalse(enumDef.isComplex());
        /*
         * Test invalid construction of a value
         */
        try {
            EnumValueImpl value = (EnumValueImpl) enumDef.createEnum("bad");
            fail();
        } catch (IllegalArgumentException iae) {
        }

        testJson(enumDef);

        /*
         * Try several bad enumeration strings.
         */
        try {
            new EnumDefImpl("enum", new String[] {"[a]", "b", "c"});
        } catch (IllegalArgumentException iae) {
        }
        try {
            new EnumDefImpl("enum", new String[] {"x-y", "b", "c"});
        } catch (IllegalArgumentException iae) {
        }
        try {
            new EnumDefImpl("enum", new String[] {"&x", "b", "c"});
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Underscore is a valid character, but not as a first character.
         */
        new EnumDefImpl("enum", new String[] {"x___y_", "b_", "a_c"});

        /*
         * Test evolution failure case where too many enumeration values are
         * added.  This won't happen until about 2044.
         * TODO: enable this if/when evolution of this sort is supported.
         *
        enumDef = new EnumDefImpl("enum", new String[] {"a", "b", "c"});

        int i = 0;
        for (i = 0; i < 10000; i++) {
            try {
                ((EnumDefImpl)enumDef).addValue(("a" + i));
            } catch (IllegalArgumentException iae) {
                assertTrue(i > 2000);
                break;
            }
        }
        assertTrue(i < 10000);
        */

        EnumDefImpl edi = new EnumDefImpl("enum",
                new String[] { "a", "b", "c" }, "non-default enum");
        String enumName = "abc";

        edi.setName(enumName);
        assertEquals(enumName, edi.getName());
        try {
            edi.setName(null);
            fail("Name should not be null for enum type");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            edi.setName("");
            fail("Name should not be empty for enum type");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            edi.createEnum(3);
            fail("Index should be inside range");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
    }

    @Test
    public void testArray() {

        /*
         * Use an array of Boolean to test
         */
        BooleanDefImpl boolDef = new BooleanDefImpl();
        ArrayDef arrayDef = new ArrayDefImpl(boolDef);
        assertDefaults(arrayDef, FieldDef.Type.ARRAY, false);
        assertFalse(arrayDef.isValidKeyField());
        assertTrue(arrayDef.isArray());
        assertFalse(arrayDef.isNumeric());
        assertFalse(arrayDef.isAtomic());
        assertTrue(arrayDef.isComplex());
        assertTrue(boolDef.equals(arrayDef.getElement()));
        ArrayDef newDef = new ArrayDefImpl(boolDef);
        assertTrue(newDef.equals(arrayDef));

        /* this should match */
        newDef = new ArrayDefImpl(new BooleanDefImpl());
        assertTrue(newDef.equals(arrayDef));

        /*
         * Test invalid insertion into the array
         */
        try {
            ArrayValue value = arrayDef.createArray();
            assertNotNull(value);
            value.add(1); /* fails because it's an array of Boolean */
            fail();
        } catch (IllegalArgumentException iae) {
        }

        testJson(newDef);
        newDef.asArray();

        FieldDefImpl arrayDefT1 = new ArrayDefImpl(
                FieldDefImpl.Constants.integerDef);
        FieldDefImpl arrayDefT2 = new ArrayDefImpl(
                FieldDefImpl.Constants.floatDef);
        FieldDefImpl unionDef = arrayDefT1.getUnionType(arrayDefT2);
        assertEquals(FieldDef.Type.ANY_JSON_ATOMIC,
                ((ArrayDefImpl) unionDef.asArray()).getElement().getType());

        IntegerDefImpl intDef = new IntegerDefImpl();
        FieldDefImpl recDefT1 = (RecordDefImpl) TableBuilder
                .createRecordBuilder("recint").addField("elem", intDef).build();
        assertEquals(FieldDef.Type.ANY,
                arrayDefT1.getUnionType(recDefT1).getType());
    }

    @Test
    public void testMap() {

        /*
         * Use an array of Boolean to test
         */
        BooleanDefImpl boolDef = new BooleanDefImpl();
        MapDef mapDef = new MapDefImpl(boolDef);
        assertDefaults(mapDef, FieldDef.Type.MAP, false);
        assertFalse(mapDef.isValidKeyField());
        assertTrue(mapDef.isMap());
        assertFalse(mapDef.isNumeric());
        assertFalse(mapDef.isAtomic());
        assertTrue(mapDef.isComplex());
        assertTrue(boolDef.equals(mapDef.getElement()));
        MapDef newDef = new MapDefImpl(boolDef);
        assertTrue(newDef.equals(mapDef));

        /* this should match */
        newDef = new MapDefImpl(new BooleanDefImpl());
        assertTrue(newDef.equals(mapDef));

        /*
         * Test invalid insertion into the map
         */
        try {
            MapValue value = mapDef.createMap();
            assertNotNull(value);
            value.put("foo", 1); /* fails because it's a map of Boolean */
            fail();
        } catch (IllegalArgumentException iae) {
        }
        testJson(newDef);
        newDef.asMap();
    }

    @Test
    public void testRecord() {
        /*
         * Use TableBuilder -- it's easier
         */
        RecordDef recordDef =
            (RecordDef) TableBuilder.createRecordBuilder("foo")
            .addInteger("int1")
            .addString("string1")
            .addBoolean("bool1", null).build();
        RecordDef recordDef1 =
            (RecordDef) TableBuilder.createRecordBuilder("foo")
            .addInteger("int2")
            .addString("string1")
            .addBoolean("bool1", null).build();
        RecordDef recordDef2 =
            (RecordDef) TableBuilder.createRecordBuilder("foo")
            .addInteger("int1")
            .addString("string1")
            .addString("bool1")
            .addBoolean("bool2", null).build();
        RecordDef recordDef3 =
            (RecordDef) TableBuilder.createRecordBuilder("foo")
            .addInteger("int1")
            .addString("string1")
            .addBoolean("bool1", null).build();

        assertDefaults(recordDef, FieldDef.Type.RECORD, false);
        assertFalse(recordDef.isValidKeyField());
        assertTrue(recordDef.isRecord());
        assertFalse(recordDef.isNumeric());
        assertFalse(recordDef.isAtomic());
        assertTrue(recordDef.isComplex());

        assertFalse(recordDef.equals(recordDef1));
        assertFalse(recordDef.equals(recordDef2));
        assertTrue("Should match", recordDef.equals(recordDef3));

        /*
         * Test invalid insertion into the record
         */
        try {
            RecordValue value = recordDef.createRecord();
            assertNotNull(value);
            value.put("int1", true); /* fails because int1 is Integer */
            fail();
        } catch (IllegalArgumentException iae) {
        }

        testJson(recordDef);
        recordDef.asRecord();

        FieldMap fieldMap = new FieldMap();
        try {
            @SuppressWarnings("unused")
            RecordDefImpl rdi = new RecordDefImpl(fieldMap, "description");
            fail("Record should have fields");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        fieldMap.put("num", new IntegerDefImpl(), false, null);
        try {
            @SuppressWarnings("unused")
            RecordDefImpl rdi = new RecordDefImpl(null, fieldMap,
                    "description");
            fail("name should not be null");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        RecordDefImpl rdiCount = new RecordDefImpl("AAA", fieldMap,
                "description");
        assertEquals(2, rdiCount.countTypes());

        String recName = "abc";
        RecordDefImpl rdi = (RecordDefImpl) TableBuilder
                .createRecordBuilder("foo").addInteger("int1").build();
        rdi.setName(recName);
        assertEquals(recName, rdi.getName());

        try {
            rdi.setName(null);
            fail("name should not be null for record type");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            rdi.setName("");
            fail("name should not be empty for record type");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        assertEquals(FieldDef.Type.INTEGER, rdi.findField("int1").getType());
    }

    /*
     * Increase code coverage -- exercise default methods in FieldDefImpl
     * that throw.
     */
    @Test
    public void testBadCasts() {
        FieldDefImpl def = FieldDefImpl.Constants.integerDef;
        int ival = 10;
        long lval = 10L;
        float fval = 10.0f;
        double dval = 10.0d;
        String sval = "ABC";
        byte barr[] = { 1, 2, 3 };

        try {
            def.asLong();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asString();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asDouble();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asFloat();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asBoolean();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asBinary();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asFixedBinary();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asMap();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asArray();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asRecord();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asAnyRecord();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asAny();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asAnyAtomic();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asAnyJsonAtomic();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asNumber();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asTimestamp();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asEnum();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            def.asJson();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.longDef.asInteger();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }


        /*
         * Now try to create types that will throw
         */
        try {
            def.createLong(0L);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createString("");
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createDouble(4.5);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createFloat(5.6F);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createBoolean(true);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createBinary(new byte[0]);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createFixedBinary(new byte[0]);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createMap();
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createArray();
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            def.createRecord();
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            FieldDefImpl.Constants.longDef.createInteger(1);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException cce) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createLong(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.longDef.createInteger(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.floatDef.createDouble(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.doubleDef.createFloat(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(barr);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumberFromIndexField(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(ival);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(lval);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(fval);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(dval);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createBoolean(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createTimestamp(barr);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createTimestamp(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createJsonNull();
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }

        try {
            FieldDefImpl.Constants.integerDef.createLong(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.longDef.createInteger(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.floatDef.createDouble(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.doubleDef.createFloat(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(barr);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumber(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createNumberFromIndexField(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createBoolean(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createTimestamp(barr);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            FieldDefImpl.Constants.integerDef.createTimestamp(sval);
            fail("Cast should have thrown");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * random additional code coverage cases
         */
        assertFalse(def.hasMin());
        assertFalse(def.hasMax());
        assertFalse(def.isEmpty());
        assertFalse(def.isWildcard());
        def.clone();
        assertFalse(FieldDefImpl.Constants.binaryDef.isValidKeyField());
        assertFalse(FieldDefImpl.Constants.binaryDef.isValidIndexField());
    }

    @Test
    public void testTimestamp() throws Exception {
        TimestampDefImpl tdef =
            new TimestampDefImpl(TimestampDefImpl.DEF_PRECISION);
        assertDefaults(tdef, FieldDef.Type.TIMESTAMP, true);
        tdef.asTimestamp();

        tdef = new TimestampDefImpl(9, "timestamp type");
        assertTrue(tdef.isTimestamp());
        assertTrue(tdef.isAtomic());
        assertFalse(tdef.isComplex());
        assertTrue(tdef.isValidIndexField());
        assertTrue(tdef.isValidKeyField());
        assertEquals(9, tdef.getPrecision());
        assertTrue(tdef.getDescription().equals("timestamp type"));

        TimestampDef newDef = new TimestampDefImpl(9, "timestamp type");
        assertTrue(newDef.equals(tdef));
        assertTrue(newDef.equals(tdef.clone()));

        testJson(tdef);

        /*
         * Try invalid precision.
         */
        try {
            tdef = new TimestampDefImpl(-1, null);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            tdef = new TimestampDefImpl(TimestampDefImpl.getMaxPrecision() + 1);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Test createTimestamp(Timestamp), createTimestamp(String) and
         * createTimestamp(long).
         */
        Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            TimestampUtils.createTimestamp(0, 1),
            TimestampUtils.createTimestamp(-1, 999999999),
            TimestampUtils.createTimestamp(System.currentTimeMillis()/1000,
                                           123456789),
            TimestampUtils.createTimestamp(-1 * System.currentTimeMillis()/1000,
                                           123456789),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE,
        };
        for (Timestamp ts : values) {
            for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
                tdef = new TimestampDefImpl(p);
                TimestampValueImpl v1 = tdef.createTimestamp(ts);
                Timestamp expValue = TimestampUtils.roundToPrecision(ts, p);
                assertTrue(v1.get().equals(expValue));

                TimestampValueImpl v2 = tdef.createTimestamp(v1.getBytes());
                TimestampValueImpl v3 =
                    tdef.createTimestamp(v1.formatForKey(tdef, 0));
                assertTrue(v1.equals(v2) && v2.equals(v3));
            }
        }

        /*
         * Test fromString()
         */
        final String[] dateStrs = new String[] {
            "2016-07-01T05:09:36",
            "2016-07-02T05:09:36.9",
            "2016-07-03T05:09:36.98",
            "2016-07-04T05:09:36.987",
            "2016-07-05T05:09:36.9876",
            "2016-07-06T05:09:36.98765",
            "2016-07-07T05:09:36.987654",
            "2016-07-08T05:09:36.9876543",
            "2016-07-09T05:09:36.98765432",
            "2016-07-10T05:09:36.987654321",
            "9999-12-31T23:59:59.999999999",
            "-6383-01-01T00:00:00",
            "-6383-01-01",
            "2016-07-10",
            "9999-12-31"
        };
        for (int p = 0; p <= TimestampDefImpl.MAX_PRECISION; p++) {
            tdef = new TimestampDefImpl(p);
            for (String str : dateStrs) {
                TimestampValue tsv = tdef.fromString(str);
                Timestamp ts1 = TimestampUtils.parseString(str);
                Timestamp expVal = TimestampUtils.roundToPrecision(ts1, p);
                assertTrue(tsv.get().equals(expVal));
            }
        }

        /*
         * Test TimestampDef.fromString(String timestampString,
         *                              String pattern,
         *                              boolean withZoneUTC)
         *  timestampString can be with UTC zone or local zone.
         */
        Timestamp ts =
            TimestampUtils.parseString("2016-07-26T08:18:23.123456789");

        String[] patterns = new String[] {
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.S",
            "yyyy-MM-dd'T'HH:mm:ss.SS",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS VV",
            "yyyy-MM-dd HH:mm:ss.SSSSSSSVV",
            "MMM dd yy HH-mm-ss-SSSSSSSSS VV",
        };

        for (int p = 0; p <= TimestampDefImpl.MAX_PRECISION; p++) {
            tdef = new TimestampDefImpl(p);
            final TimestampValue tsv = tdef.createTimestamp(ts);
            for (String pattern : patterns) {
                /*
                 * Timestamp string is with UTC zone,
                 * the default zone used for parsing is UTC.
                 */
                TimestampValue ts1 =
                    tdef.fromString(tsv.toString(pattern, true), pattern, true);
                /*
                 * Timestamp string is with local zone or contains Zone,
                 * the default zone used for parsing is local zone.
                 */
                TimestampValue ts2 =
                    tdef.fromString(tsv.toString(pattern, false),
                                    pattern, false);
                assertTrue(ts1.equals(ts2));

                /*
                 * Timestamp string is with UTC zone,
                 * the default zone used for parsing is local zone.
                 */
                TimestampValue ts3 =
                    tdef.fromString(tsv.toString(pattern, true),
                                    pattern, false);
                /*
                 * Timestamp string is with local zone,
                 * the default zone used for parsing is UTC.
                 */
                TimestampValue ts4 =
                    tdef.fromString(tsv.toString(pattern, false),
                                    pattern, true);
                if (pattern.contains("VV")) {
                    assertTrue(ts1.equals(ts3) && ts3.equals(ts4));
                } else {
                    assertTrue(Math.abs(ts1.get().getTime() -
                                        ts3.get().getTime()) ==
                               Math.abs(ts1.get().getTime() -
                                        ts4.get().getTime()));
                }
            }
        }

        /*
         * Test TimestampDef.fromString(String timestampString,
         *                              String pattern,
         *                              boolean withZoneUTC)
         *  timestampString can be with local zone or specified zone.
         */
        patterns = new String[] {
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS VV",
            "yyyy-MM-dd HH:mm:ss.SSSSSSSVV",
            "MMM dd yy HH-mm-ss-SSSSSSSSS VV",
            "yyyyy-MM-dd HH:mm:ss.SSSSSSSSS VV",
            "MMM d, uuuu HH:mm:ss VV"
        };

        String[] UTCZoneStrings = new String[] {
            "2016-07-26T08:18:23",
            "2016-07-26T08:18:23.123",
            "2016-07-26T08:18:23.123457 Z",
            "2016-07-26 08:18:23.1234568Z",
            "Jul 26 16 08-18-23-123456789 Z",
            "09999-12-31 23:59:59.999999999 Z",
            "Jan 1, -6383 00:00:00 Z"
        };

        String[] localOrOtherZones = new String[] {
            utcToLocalZone(UTCZoneStrings[0], patterns[0]),
            utcToLocalZone(UTCZoneStrings[1], patterns[1]),
            "2016-07-26T04:18:23.123457 America/New_York",
            "2016-07-26 16:18:23.1234568Asia/Shanghai",
            "Jul 26 16 13-48-23-123456789 Asia/Kolkata",
            "10000-01-01 07:59:59.999999999 Asia/Shanghai",
            "Jan 1, -6383 08:05:43 Asia/Shanghai"
        };

        TimestampDefImpl def = new TimestampDefImpl(9);
        for (int i = 0; i < patterns.length; i++) {
            final String pattern = patterns[i];
            TimestampValueImpl ts1 =
                def.fromString(UTCZoneStrings[i], pattern, true);
            TimestampValueImpl ts2 =
                def.fromString(localOrOtherZones[i], pattern, false);
            assertTrue(ts1.equals(ts2));
        }

        /*
         * Test TimestampDef.currentTimestamp()
         */
        Timestamp begin = new Timestamp(System.currentTimeMillis());
        List<Timestamp> list = new ArrayList<Timestamp>();
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            list.add(new TimestampDefImpl(p).currentTimestamp().get());
        }
        Timestamp min = TimestampUtils.roundToPrecision
                        (TimestampUtils.minusMillis(begin, 500), 0);
        Timestamp max = TimestampUtils.roundToPrecision
                       (TimestampUtils.plusMillis(begin, 500), 0);
        for (Timestamp val : list) {
            assertTrue(val.compareTo(min) >= 0 && val.compareTo(max) <= 0);
        }

        /*
         * Test bad timestamp string.
         */
        tdef = new TimestampDefImpl(3);
        try {
            tdef.fromString("1999-02");
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        try {
            tdef.fromString("Feb 06 1999 00:00:00");
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        /* Out of range */
        try {
            tdef.fromString("10000-01-01T00:00:00");
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
        try {
            tdef.fromString("-6384-12-31T23:59:59.999999999");
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
        try {
            tdef.fromString(null, TimestampDefImpl.DEF_STRING_FORMAT, true);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }

        AnyDefImpl anydef = new AnyDefImpl();
        AnyAtomicDefImpl anyatomicdef = new AnyAtomicDefImpl();
        TimestampDefImpl tdeflt = new TimestampDefImpl(3);
        assertTrue(tdef.isSubtype(anydef));
        assertTrue(tdef.isSubtype(anyatomicdef));
        assertTrue(tdef.isSubtype(tdeflt));
    }

    @Test
    public void testNumber() {
        NumberDefImpl bddef = new NumberDefImpl();
        assertDefaults(bddef, FieldDef.Type.NUMBER, true);
        bddef.asNumber();

        /*
         * Non-default constructor
         */
        bddef = new NumberDefImpl("test_Number");
        assertTrue(bddef.isNumber());
        assertTrue(bddef.isNumeric());
        assertTrue(bddef.isAtomic());
        assertFalse(bddef.isComplex());
        assertTrue(bddef.isValidIndexField());
        assertTrue(bddef.getDescription().equals("test_Number"));

        /*
         * Test some construction
         */
        @SuppressWarnings("unused")
        NumberValue value = bddef.createNumber(new BigDecimal(1));
        BigDecimal[] values = new BigDecimal[] {
            new BigDecimal(-76000),
            new BigDecimal(1000001),
            new BigDecimal(1000001.0F),
            new BigDecimal(755551.000001),
            new BigDecimal("755551000001576879000"),
        };

        for (BigDecimal v : values) {
            NumberValue dv1 = bddef.createNumber(v);

            byte[] bytes = NumberUtils.serialize(v);
            NumberValue dv2 = bddef.createNumber(bytes);
            assertTrue(dv2.equals(dv1));

            String str = SortableString.toSortable(bytes);
            NumberValue dv3 = bddef.createNumberFromIndexField(str);
            assertTrue(dv3.equals(dv1));

            FieldValue dv4 =
                FieldDefImpl.createValueFromString(v.toString(), bddef);
            assertTrue(dv4.equals(dv1));
        }

        int[] ints = new int[] {
           0, 1, -1, 32134, -321321, Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        for (int v : ints) {
            NumberValue dv1 = bddef.createNumber(v);
            NumberValue dv2 = bddef.createNumber(BigDecimal.valueOf(v));
            assertTrue(dv1.equals(dv2));
        }

        long[] longs = new long[] {
           -12313432232134L, 12313321321333L, Long.MIN_VALUE, Long.MAX_VALUE
        };
        for (long v : longs) {
            NumberValue dv1 = bddef.createNumber(v);
            NumberValue dv2 = bddef.createNumber(BigDecimal.valueOf(v));
            assertTrue(dv1.equals(dv2));
        }

        float[] floats = new float[] {
            0.0f, 1.0f, -1.1f, 32134.2131f, -321321.123f,
            -Float.MIN_VALUE, -Float.MAX_VALUE,
            Float.MIN_VALUE, Float.MAX_VALUE
        };
        for (float v : floats) {
            NumberValue dv1 = bddef.createNumber(v);
            NumberValue dv2 = bddef.createNumber(BigDecimal.valueOf(v));
            assertTrue(dv1.equals(dv2));
        }

        double[] doubles = new double[] {
            -32131234E-10, 32131234.32132132133123,
            -Double.MIN_VALUE, -Double.MAX_VALUE,
            Double.MIN_VALUE, Double.MAX_VALUE
        };
        for (double v : doubles) {
            NumberValue dv1 = bddef.createNumber(v);
            NumberValue dv2 = bddef.createNumber(BigDecimal.valueOf(v));
            assertTrue(dv1.equals(dv2));
        }

        testJson(bddef);
    }

    /**
     * Convert a timestamp string from UTC zone to Local time zone, the
     * fractional second of timestamp should be <= 3.
     */
    private static String utcToLocalZone(String timestampStr, String pattern)
        throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = sdf.parse(timestampStr);
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(d);
    }

    /**
     * Test the default values for FieldDef instances.
     */
    private void assertDefaults(final FieldDef def,
                                final FieldDef.Type type,
                                boolean keyOk) {

        /* description -- null */
        assertNull(def.getDescription());

        /* these two are type-dependent */
        assertTrue(def.isType(type));
        assertEquals(keyOk, def.isValidKeyField());
    }

    private int encodingLength(FieldDef def) {
        return ((FieldDefImpl)def).getEncodingLength();
    }

    /**
     * Convert the FieldDef object to JSON and back, and make sure
     * that the result is equal to the original object.
     */
    private void testJson(final FieldDef def) {

        String jsonString = ((FieldDefImpl) def).toJsonString();
        MapValue map =
            (MapValue) FieldValueFactory.createValueFromJson(jsonString);
        FieldDefImpl newDef = TableJsonUtils.fromJson(map);
        assertTrue(def.equals(newDef));
    }

    @Test
    public void testEmpty() {
        EmptyDefImpl emptydef = new EmptyDefImpl();
        IntegerDefImpl integerdef = new IntegerDefImpl();
        EmptyDefImpl emptydefother = emptydef.clone();
        assertEquals(emptydef, emptydefother);
        assertTrue(emptydef.isPrecise());
        assertTrue(emptydef.isSubtype(emptydefother));
        assertFalse(emptydef.isSubtype(integerdef));
    }

    @Test
    public void testAnyDef() {
        long lval = 10L;
        float fval = 10.10f;
        double dval = 10.10d;
        float delta = 0.0f;
        byte[] testArray = { 1, 2, 3, 4, 5, 6, 7 };
        boolean bval = true;
        AnyDefImpl anydef = new AnyDefImpl();
        AnyDefImpl anydefother = anydef.clone();
        LongDefImpl longDef = new LongDefImpl();
        assertEquals(lval, anydef.createLong(lval).get());
        assertEquals(fval, anydef.createFloat(fval).get(), delta);
        assertEquals(dval, anydef.createDouble(dval).get(), delta);
        assertEquals(testArray, anydef.createBinary(testArray).get());
        assertEquals(bval, anydef.createBoolean(bval).get());
        assertEquals(anydef, anydefother);
        assertEquals(FieldDefImpl.Constants.anyDef, anydef.asAny());
        assertTrue(anydef.isSubtype(anydefother));
        assertFalse(anydef.isSubtype(longDef));

        String tsValue = "2016-07-21T20:47:15.987654321Z";
        String json = "{\"a\": 1, \"b\": 2}";
        String retjson = "{\"a\":1,\"b\":2}";
        FieldValue anyVal = FieldDefImpl.createValueFromString(tsValue, anydef);
        assertEquals(tsValue, anyVal.asTimestamp().toString());
        anyVal = FieldDefImpl.createValueFromString(json, anydef);
        assertEquals(retjson, anyVal.toJsonString(false));
    }

    @Test
    public void testAnyAtomicDef() {
        AnyAtomicDefImpl aadi = new AnyAtomicDefImpl();
        FieldDefImpl fdi = new IntegerDefImpl();
        FieldDefImpl aadiOther = new AnyAtomicDefImpl();
        FieldDefImpl adi = new AnyDefImpl();
        assertFalse(aadi.isSubtype(fdi));
        assertTrue(aadi.isSubtype(adi));
        assertTrue(aadi.isSubtype(aadiOther));
    }

    @Test
    public void testAnyRecordDef() {
        AnyRecordDefImpl ardi = new AnyRecordDefImpl();
        AnyRecordDefImpl ardiClone = ardi.clone();
        FieldDefImpl fdi = new IntegerDefImpl();
        FieldDefImpl ardiOther = new AnyRecordDefImpl();
        FieldDefImpl adi = new AnyDefImpl();
        assertFalse(ardi.isSubtype(fdi));
        assertTrue(ardi.isSubtype(adi));
        assertTrue(ardi.isSubtype(ardiOther));
        assertTrue(ardiClone.equals(ardi.clone()));
    }

    @Test
    public void testJsonDef() {
        String jsonString = "";
        ArrayDefImpl arrayDefJson = new ArrayDefImpl(
                FieldDefImpl.Constants.jsonDef);
        ArrayValue jsonArray = arrayDefJson.createArray();
        try {
            jsonArray.addJson(0, jsonString);
            fail("empty string not allowed as it return null token");
        } catch (IllegalStateException e) {
            /* success */
        }

        /* return null for allMRCounterFields */
        byte[] bytesZeroLen = { 0 };
        ByteArrayInputStream bais = new ByteArrayInputStream(bytesZeroLen);
        DataInputStream in = new DataInputStream(bais);
        try {
            JsonDefImpl jdi = new JsonDefImpl(in,
                                              SerialVersion.MINIMUM);
            jdi.allMRCounterFields();
            assertEquals(null, jdi.allMRCounterFields());
            assertTrue(jdi.isComplex());
        } catch (IOException e) {
            /* success */
        }

        /* validate JSON-compatible values */
        byte[] btArray = { 1, 2, 3 };
        FixedBinaryDefImpl fBinDef = new FixedBinaryDefImpl("fixed", 1);
        FixedBinaryValueImpl fbvi = new FixedBinaryValueImpl(btArray, fBinDef);
        BinaryValueImpl bvi = new BinaryValueImpl(btArray);
        try {
            JsonDefImpl.validateJsonType(bvi);
            fail("BINARY type is not supported in JSON");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            JsonDefImpl.validateJsonType(fbvi);
            fail("FIXED_BINARY type is not supported in JSON");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        String enums1[] = { "a", "b", "c" };
        EnumDef def1 = new EnumDefImpl("enum1", enums1);
        EnumValueImpl evi = new EnumValueImpl(def1, enums1[0]);
        try {
            JsonDefImpl.validateJsonType(evi);
            fail("ENUM type is not supported in JSON");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        RecordDef recordDef = (RecordDef) TableBuilder
                .createRecordBuilder("foo").addInteger("int").build();
        RecordValueImpl rvi = new RecordValueImpl(recordDef);
        try {
            JsonDefImpl.validateJsonType(rvi);
            fail("RECORD type is not supported in JSON");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        ArrayValueImpl aviJson = new ArrayValueImpl(arrayDefJson);
        JsonDefImpl.validateJsonType(aviJson);

        ArrayDefImpl arrayDefInt = new ArrayDefImpl(
                FieldDefImpl.Constants.integerDef);
        ArrayValueImpl aviInt = new ArrayValueImpl(arrayDefInt);
        JsonDefImpl.validateJsonType(aviInt);

        MapDefImpl mapDefJson = new MapDefImpl(FieldDefImpl.Constants.jsonDef);
        MapValueImpl mviJson = new MapValueImpl(mapDefJson);
        JsonDefImpl.validateJsonType(mviJson);

        MapDefImpl mapDefNum = new MapDefImpl(FieldDefImpl.Constants.numberDef);
        MapValueImpl mviNum = new MapValueImpl(mapDefNum);
        JsonDefImpl.validateJsonType(mviNum);

        IntegerValueImpl ivi = new IntegerValueImpl(10);
        JsonDefImpl.validateJsonType(ivi);

        ArrayDefImpl arrayDefAny = new ArrayDefImpl(
                FieldDefImpl.Constants.anyDef);
        ArrayValueImpl aviAny = new ArrayValueImpl(arrayDefAny);
        try {
            JsonDefImpl.validateJsonType(aviAny);
            fail("Type is not supported");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* createNumber */
        int ival = 10;
        long lval = 10L;
        float fval = 10.0f;
        double dval = 10.0d;
        float delta = 0.0f;
        JsonDefImpl jdiNum = new JsonDefImpl();
        NumberValueImpl numVal = (NumberValueImpl) jdiNum.createNumber(ival);
        BigDecimal bdVal = (BigDecimal) NumberUtils
                .deserialize(numVal.getBytes());
        assertEquals(ival, bdVal.intValue());
        numVal = (NumberValueImpl) jdiNum.createNumber(lval);
        bdVal = (BigDecimal) NumberUtils.deserialize(numVal.getBytes());
        assertEquals(lval, bdVal.longValue());
        numVal = (NumberValueImpl) jdiNum.createNumber(fval);
        bdVal = (BigDecimal) NumberUtils.deserialize(numVal.getBytes());
        assertEquals(fval, bdVal.floatValue(), delta);
        numVal = (NumberValueImpl) jdiNum.createNumber(dval);
        bdVal = (BigDecimal) NumberUtils.deserialize(numVal.getBytes());
        assertEquals(dval, bdVal.doubleValue(), delta);
    }
}
