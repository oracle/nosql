/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TimestampDef;

import org.junit.Test;

/**
 * Test simple subclasses of FieldValue.  Complex types are tested in another
 * class.
 */
@SuppressWarnings("unlikely-arg-type")
public class FieldValueTest extends TestBase {
    static IntegerValueImpl intNotEqual = new IntegerValueImpl(10);
    static LongValueImpl longNotEqual = new LongValueImpl(10);
    static DoubleValueImpl doubleNotEqual = new DoubleValueImpl(10.0);
    static FloatValueImpl floatNotEqual = new FloatValueImpl(10.0F);
    static NumberValueImpl decimalNotEqual =
        new NumberValueImpl(new BigDecimal(15));
    static StringValueImpl stringNotEqual = new StringValueImpl("12345");
    static BooleanValueImpl booleanNotEqual = BooleanValueImpl.falseValue;

    @Test
    public void testInteger() {
        int values[] =
            {Integer.MIN_VALUE, -1, 0, 1, 1000000, Integer.MAX_VALUE};
        IntegerValueImpl lastVal = null;
        for (int i : values) {
            IntegerValueImpl val = new IntegerValueImpl(i);
            assertEquals(i, val.get());
            assertTrue(val.getType() == FieldDef.Type.INTEGER);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(intNotEqual));
            assertFalse(val.equals(longNotEqual));
            String keyFormat = val.formatForKey(null);
            assertTrue(new IntegerValueImpl(keyFormat).equals(val));
            assertEquals(val.compareTo
                         (new IntegerValueImpl(keyFormat)), 0);
            assertTrue(val.isInteger());
            assertFalse(val.isMap());
            assertTrue(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            assertFalse(val.isNumber());
            assertFalse(val.isFixedBinary());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException e) {
                /* success */
            }
            if (lastVal != null) {
                assertTrue(val.compareTo(lastVal) > 0);
            }
            lastVal = val;

            /* test toString() */
            assertTrue(Integer.parseInt(lastVal.toString()) == i);
        }

        /* test clone and getDefinition methods */
        int ival = 99;
        IntegerDefImpl intDef = new IntegerDefImpl(null, 0, 100);
        IntegerRangeValue val = new IntegerRangeValue(ival, intDef);
        IntegerRangeValue valother = val.clone();
        assertEquals(val, valother);
        assertEquals(FieldDefImpl.Constants.integerDef, val.getDefinition());

        IntegerValueImpl valImpl = new IntegerValueImpl(ival);
        try {
            valImpl.asPrimaryKey();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        try {
            valImpl.asIndexKey();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        try {
            valImpl.asRow();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        /* test compareTo method*/
        FieldValueImpl fieldValImpl = FieldDefImpl.Constants.integerDef
                .createInteger(ival);
        FieldValueImpl other = fieldValImpl.clone();
        assertEquals(fieldValImpl, other);
        assertEquals(fieldValImpl.compareTo(other), 0);

        try {
            fieldValImpl.size();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }

        try {
            fieldValImpl.getMap();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }

        /* test compareKeyValues method */
        float fval = 99.0f;
        FieldValueImpl fviint = FieldDefImpl.Constants.integerDef
                .createInteger(ival);
        FieldValueImpl fvifloat = FieldDefImpl.Constants.floatDef
                .createFloat(fval);
        int icheck = FieldValueImpl.compareKeyValues(fviint, fvifloat);
        assertEquals(0, icheck);

        BigDecimal bdval = new BigDecimal(99);
        FieldValueImpl fviNumber = FieldDefImpl.Constants.numberDef
                .createNumber(bdval);
        icheck = FieldValueImpl.compareKeyValues(fviint, fviNumber);
        assertEquals(0, icheck);

        /* test equal method */
        NullJsonValueImpl njvi = NullJsonValueImpl.getInstance();
        assertFalse(fviint.equal(njvi));
        assertTrue(fviint.equal(fvifloat));
        fval = 100.0f;
        fvifloat.setFloat(fval);
        assertFalse(fviint.equal(fvifloat));
        long lval = 99;
        FieldValueImpl fvilong = FieldDefImpl.Constants.longDef
                .createLong(lval);
        assertTrue(fviint.equal(fvilong));
        lval = 100;
        fvilong.setLong(lval);
        assertFalse(fviint.equal(fvilong));
        double dval = 99.0d;
        FieldValueImpl fvidouble = FieldDefImpl.Constants.doubleDef
                .createDouble(dval);
        assertTrue(fviint.equal(fvidouble));
        dval = 100.0d;
        fvidouble.setDouble(dval);
        assertFalse(fviint.equal(fvidouble));
        assertTrue(fviint.equal(fviNumber));
        BigDecimal bdvalue = new BigDecimal(100);
        FieldValueImpl fviNum = FieldDefImpl.Constants.numberDef
                .createNumber(bdvalue);
        assertFalse(fviint.equal(fviNum));
        FieldValueImpl fvistring = FieldDefImpl.Constants.stringDef
                .createString("ABC");
        assertFalse(fviint.equal(fvistring));
    }

    @Test
    public void testLong() {
        long values[] =
            {Long.MIN_VALUE, -1, 0, 1, 1000000000L, Long.MAX_VALUE};
        LongValueImpl lastVal = null;
        for (long l : values) {
            LongValueImpl val = new LongValueImpl(l);
            assertEquals(l, val.get());
            assertTrue(val.getType() == FieldDef.Type.LONG);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(longNotEqual));
            assertFalse(val.equals(intNotEqual));
            String keyFormat = val.formatForKey(null);
            assertTrue(new LongValueImpl(keyFormat).equals(val));
            assertEquals(val.compareTo
                         (new LongValueImpl(keyFormat)), 0);
            assertTrue(val.isLong());
            assertFalse(val.isMap());
            assertTrue(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException e) {
                /* success */
            }
            if (lastVal != null) {
                assertTrue(val.compareTo(lastVal) > 0);
            }
            lastVal = val;

            /* test toString() */
            assertTrue(Long.parseLong(lastVal.toString()) == l);
        }

        /* test clone and getDefinition methods */
        long lVal = 1000000000L;
        LongDefImpl lDef = new LongDefImpl(null, Long.MIN_VALUE,
                Long.MAX_VALUE);
        LongRangeValue lRangeVal = new LongRangeValue(lVal, lDef);
        LongRangeValue valother = lRangeVal.clone();
        assertEquals(lRangeVal, valother);
        assertEquals(FieldDefImpl.Constants.longDef, lRangeVal.getDefinition());

        /* test compareKeyValues method */
        long lval = 99;
        float fval = 99.0f;
        FieldValueImpl fvilong = FieldDefImpl.Constants.longDef
                .createLong(lval);
        FieldValueImpl fvifloat = FieldDefImpl.Constants.floatDef
                .createFloat(fval);
        int icheck = FieldValueImpl.compareKeyValues(fvilong, fvifloat);
        assertEquals(0, icheck);

        FieldValueImpl fviboolean = BooleanDefImpl.Constants.booleanDef
                .createBoolean(true);
        try {
            icheck = FieldValueImpl.compareKeyValues(fvilong, fviboolean);
            fail("Values are non-comparable");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        BigDecimal bdval = new BigDecimal(99);
        FieldValueImpl fviNumber = FieldDefImpl.Constants.numberDef
                .createNumber(bdval);
        icheck = FieldValueImpl.compareKeyValues(fvilong, fviNumber);
        assertEquals(0, icheck);

        /* test equal method */
        int ival = 99;
        FieldValueImpl fviint = FieldDefImpl.Constants.integerDef
                .createInteger(ival);
        assertTrue(fvilong.equal(fviint));
        ival = 100;
        fviint.setInt(ival);
        assertFalse(fvilong.equal(fviint));

        assertTrue(fvilong.equal(fvifloat));
        fval = 100.0f;
        fvifloat.setFloat(fval);
        assertFalse(fvilong.equal(fvifloat));

        double dval = 99.0d;
        FieldValueImpl fvidouble = FieldDefImpl.Constants.doubleDef
                .createDouble(dval);
        assertTrue(fvilong.equal(fvidouble));
        dval = 100.0d;
        fvidouble.setDouble(dval);
        assertFalse(fvilong.equal(fvidouble));

        assertTrue(fvilong.equal(fviNumber));
        BigDecimal bdvalue = new BigDecimal(100);
        FieldValueImpl fviNum = FieldDefImpl.Constants.numberDef
                .createNumber(bdvalue);
        assertFalse(fvilong.equal(fviNum));

        FieldValueImpl fvistring = FieldDefImpl.Constants.stringDef
                .createString("ABC");
        assertFalse(fvilong.equal(fvistring));
    }

    @Test
    public void testDouble() {
        double values[] = {Double.MIN_VALUE, Double.MAX_VALUE,
                           -1.0, 0.0, 1.0, 1000000000.0,
                           Double.MIN_NORMAL,
                           Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY};
        DoubleValueImpl lastVal = null;
        for (double d : values) {
            DoubleValueImpl val = new DoubleValueImpl(d);
            assertEquals(d, val.get(), 0.0);
            assertTrue(val.getType() == FieldDef.Type.DOUBLE);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(doubleNotEqual));
            assertFalse(val.equals(intNotEqual));
            String keyFormat = val.formatForKey(null);
            assertTrue(new DoubleValueImpl(keyFormat).equals(val));
            assertEquals(val.compareTo
                         (new DoubleValueImpl(keyFormat)), 0);
            assertTrue(val.isDouble());
            assertFalse(val.isMap());
            assertTrue(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException e) {
                /* success */
            }
            if (lastVal != null) {
                if (val.get() < lastVal.get()) {
                    assertTrue(val.compareTo(lastVal) < 0);
                } else {
                    assertTrue(val.compareTo(lastVal) > 0);
                }
            }
            lastVal = val;

            /* test toString() */
            assertTrue(Double.parseDouble(lastVal.toString()) == d);
        }
        DoubleValueImpl valNan = new DoubleValueImpl(Double.NaN);
        DoubleValueImpl val1 = new DoubleValueImpl(1F);
        String valStringNan = valNan.formatForKey(null);
        DoubleValueImpl newValNan = new DoubleValueImpl(valStringNan);
        assertTrue(valNan.equals(newValNan));
        assertFalse(valNan.equals(val1));

        /* test clone and getDefinition methods */
        Double dval = 1000000000.0;
        DoubleDefImpl dDef = new DoubleDefImpl(null, Double.MIN_VALUE,
                Double.MAX_VALUE);
        DoubleRangeValue dRangeVal = new DoubleRangeValue(dval, dDef);
        DoubleRangeValue valother = dRangeVal.clone();
        assertEquals(dRangeVal, valother);
        assertEquals(FieldDefImpl.Constants.doubleDef,
                dRangeVal.getDefinition());

        /* test compareKeyValues method */
        double dvalue = 99.0d;
        float fvalue = 99.0f;
        int icomp = -1;
        FieldValueImpl fvidouble = FieldDefImpl.Constants.doubleDef
                .createDouble(dvalue);
        FieldValueImpl fvifloat = FieldDefImpl.Constants.floatDef
                .createFloat(fvalue);
        icomp = FieldValueImpl.compareKeyValues(fvidouble, fvifloat);
        assertEquals(0, icomp);
        BigDecimal bdval = new BigDecimal(99);
        FieldValueImpl fviNumber = FieldDefImpl.Constants.numberDef
                .createNumber(bdval);
        icomp = FieldValueImpl.compareKeyValues(fvidouble, fviNumber);
        assertEquals(0, icomp);

        /* test equal method */
        assertTrue(fvidouble.equal(fvifloat));
        fvalue = 101.0f;
        fvifloat.setFloat(fvalue);
        assertFalse(fvidouble.equal(fvifloat));
        BigDecimal bd = new BigDecimal(dvalue);
        FieldValueImpl fvinumber = new NumberValueImpl(bd);
        assertTrue(fvidouble.equal(fvinumber));
        dvalue = 100.0d;
        BigDecimal bdother = new BigDecimal(dvalue);
        fvinumber.setDecimal(bdother);
        assertFalse(fvidouble.equal(fvinumber));
        FieldValueImpl fvistring = FieldDefImpl.Constants.stringDef
                .createString("ABC");
        assertFalse(fvidouble.equal(fvistring));
    }

    @Test
    public void testNumber() {

        BigDecimal values[] = {
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.TEN,
            new BigDecimal(-1.23456),
            new BigDecimal(1.23456),
            new BigDecimal(Integer.MIN_VALUE),
            new BigDecimal(Integer.MIN_VALUE + 1),
            new BigDecimal(Integer.MAX_VALUE - 1),
            new BigDecimal(Integer.MAX_VALUE),
            new BigDecimal(Long.MIN_VALUE),
            new BigDecimal(Long.MIN_VALUE + 1),
            new BigDecimal(Long.MAX_VALUE - 1),
            new BigDecimal(Long.MAX_VALUE),
            new BigDecimal(-1 * Float.MAX_VALUE),
            new BigDecimal(Float.MAX_VALUE),
            new BigDecimal(-1 * Double.MAX_VALUE),
            new BigDecimal(Double.MAX_VALUE),
            new BigDecimal("-456678494949494949494949494.57687"),
            new BigDecimal("456678494949494949494949494.57687"),
        };

        Map<BigDecimal, NumberValueImpl> map =
           new TreeMap<BigDecimal, NumberValueImpl>();

        for (BigDecimal bd : values) {
            map.put(bd, new NumberValueImpl(bd));
        }

        NumberValueImpl lastVal = null;
        for (Entry<BigDecimal, NumberValueImpl> e : map.entrySet()) {
            BigDecimal bd = e.getKey();
            NumberValueImpl val = e.getValue();
            assertTrue(bd.compareTo(val.get()) == 0);
            assertTrue(val.get().equals(val.getDecimal()));
            assertTrue(val.getType() == FieldDef.Type.NUMBER);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(decimalNotEqual));
            assertFalse(val.equals(intNotEqual));
            assertTrue(val.isNumber());
            assertFalse(val.isMap());
            assertTrue(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException ex) {
                /* success */
            }
            if (lastVal != null) {
                assertTrue(val.get().compareTo(lastVal.get()) > 0);
            }
            lastVal = val;

            /* test toString() */
            assertTrue(new BigDecimal(lastVal.toString()).compareTo(bd) == 0);

            /* test equals(), compareTo() */
            assertTrue(val.equals(new NumberValueImpl(val.get())));
            assertTrue(val.compareTo(new NumberValueImpl(val.get())) == 0);
        }

        /* test castAsXxx() methods */

        /* castAsInt() */
        NumberValueImpl dv;
        int[] ivals = new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};
        for (int ival:  ivals) {
            dv = new NumberValueImpl(new BigDecimal(ival));
            assertTrue(ival == dv.castAsInt());
            assertTrue(ival == dv.castAsLong());
            assertTrue(ival == dv.castAsFloat());
            assertTrue(ival == dv.castAsDouble());

            IntegerValueImpl intVal = new IntegerValueImpl(ival);
            assertTrue(dv.compareTo(intVal) == 0);
        }

        /* castAsInt() does not cause overflow */
        BigDecimal[] bds = new BigDecimal[] {
            new BigDecimal(Integer.MIN_VALUE).subtract(BigDecimal.ONE),
            new BigDecimal(Integer.MAX_VALUE).add(BigDecimal.ONE),
        };
        for (BigDecimal v : bds) {
            dv = new NumberValueImpl(v);
            try {
                dv.castAsInt();
            } catch (IllegalArgumentException iae) {
                fail("Expected to not throw exception");
            }
        }

        /* test castAsLong() */
        long[] lvals = new long[] {Long.MIN_VALUE,
                                   Long.valueOf(Integer.MIN_VALUE) - 1,
                                   Long.valueOf(Integer.MAX_VALUE) + 1,
                                   Long.MAX_VALUE};
        for (long lval : lvals) {
            dv = new NumberValueImpl(new BigDecimal(lval));
            try {
                dv.castAsInt();
            } catch (IllegalArgumentException iae) {
                fail("Expected to not throw exception");
            }
            assertTrue(lval == dv.castAsLong());
            assertTrue(lval == dv.castAsFloat());
            assertTrue(lval == dv.castAsDouble());

            LongValueImpl longVal = new LongValueImpl(lval);
            assertTrue(dv.compareTo(longVal) == 0);
        }

        /* castAsLong() does not cause overflow */
        bds = new BigDecimal[] {
            new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE),
            new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE),
        };
        for (BigDecimal v : bds) {
            dv = new NumberValueImpl(v);
            try {
                dv.castAsLong();
            } catch (IllegalArgumentException iae) {
                fail("Expected to not throw exception");
            }
        }

        /* test castAsFloat() */
        float[] fvals = new float[] {1.1f, -1.1f
                                     -1 * Float.MAX_VALUE,
                                     -1 * Float.MIN_VALUE,
                                     Float.MIN_VALUE,
                                     Float.MAX_VALUE};
        for (float fval : fvals) {
            dv = new NumberValueImpl(BigDecimal.valueOf(fval));
            try {
                dv.castAsInt();
                dv.castAsLong();
            } catch (IllegalArgumentException iae) {
                fail("Expected to not throw exception");
            }
            assertTrue(fval == dv.castAsFloat());
            assertTrue(fval == dv.castAsDouble());

            FloatValueImpl fltVal = new FloatValueImpl(fval);
            assertTrue(dv.compareTo(fltVal) == 0);
        }

        /* test castAsDouble() */
        double[] dvals = new double[] {-1 * Double.MAX_VALUE,
                                       -1 * Double.MIN_VALUE,
                                       Double.MIN_VALUE,
                                       Double.MAX_VALUE};
        for (double dval : dvals) {
            dv = new NumberValueImpl(BigDecimal.valueOf(dval));
            try {
                dv.castAsInt();
                dv.castAsLong();
            } catch (IllegalArgumentException iae) {
                fail("Expected to not throw exception");
            }

            float fval = dv.castAsFloat();
            if (dval == -1 * Double.MAX_VALUE) {
                assertTrue(fval == Float.NEGATIVE_INFINITY);
            } else if (dval == Double.MAX_VALUE) {
                assertTrue(fval == Float.POSITIVE_INFINITY);
            } else {
                assertTrue(fval == 0.0f);
            }
            assertTrue(dval == dv.castAsDouble());

            DoubleValueImpl dblVal = new DoubleValueImpl(dval);
            assertTrue(dv.compareTo(dblVal) == 0);
        }

        /* test formatForKey() */
        bds = new BigDecimal[] {
            BigDecimal.ZERO,
            BigDecimal.ONE,
            new BigDecimal("1234"),
            new BigDecimal("-1234"),
            BigDecimal.valueOf(Long.MIN_VALUE + 1),
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.valueOf(Long.MAX_VALUE-1),
            BigDecimal.valueOf(Long.MAX_VALUE),
            BigDecimal.valueOf(-1 * Double.MAX_VALUE),
            BigDecimal.valueOf(-0.5 * Double.MAX_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE - 0.0000001),
            BigDecimal.valueOf(Double.MAX_VALUE),
            new BigDecimal("-456678494949494949494949494.57687"),
            new BigDecimal("456678494949494949494949494.57687")
        };
        BigDecimal prevVal = bds[0];
        for (int i = 1; i < bds.length; i++) {
            BigDecimal bd = bds[i];
            String prevStr = new NumberValueImpl(prevVal).formatForKey(null);
            String str = new NumberValueImpl(bd).formatForKey(null);
            if (bd.compareTo(prevVal) > 0) {
                assertTrue(str.compareTo(prevStr) > 0);
            } else if (bd.compareTo(prevVal) < 0) {
                assertTrue(str.compareTo(prevStr) < 0);
            } else {
                assertTrue(str.compareTo(prevStr) == 0);
            }
            prevVal = bd;
        }

        /* test getNextValue() */
        for (BigDecimal bd : bds) {
            NumberValueImpl val = new NumberValueImpl(bd);
            NumberValueImpl nextVal = (NumberValueImpl)val.getNextValue();
            assertTrue(nextVal.compareTo(val) > 0);

            NumberValueImpl incVal =
                new NumberValueImpl(bd.add(new BigDecimal("1E-10000")));
            assertTrue(incVal.compareTo(nextVal) > 0);
        }

        BigDecimal bd = new BigDecimal("1234");
        FieldValueImpl fvinumber = new NumberValueImpl(bd);
        FieldValueImpl fvistring = new StringValueImpl("ABC");
        assertFalse(fvinumber.equal(fvistring));

        try {
            fvinumber.compareTo(fvistring);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
            /* success */
        }
    }

    @Test
    public void testFloat() {
        float values[] = {Float.MIN_VALUE, Float.MAX_VALUE,
                           -1.0F, 0.0F, 1.0F, 1000000000.0F,
                           Float.MIN_NORMAL,
                           Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY};
        FloatValueImpl lastVal = null;
        for (float f : values) {
            FloatValueImpl val = new FloatValueImpl(f);
            assertEquals(f, val.get(), 0.0F);
            assertTrue(val.getType() == FieldDef.Type.FLOAT);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(floatNotEqual));
            assertFalse(val.equals(intNotEqual));
            String keyFormat = val.formatForKey(null);
            assertTrue(new FloatValueImpl(keyFormat).equals(val));
            assertEquals(val.compareTo
                         (new FloatValueImpl(keyFormat)), 0);
            assertTrue(val.isFloat());
            assertFalse(val.isMap());
            assertTrue(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException e) {
                /* success */
            }
            if (lastVal != null) {
                if (val.get() < lastVal.get()) {
                    assertTrue(val.compareTo(lastVal) < 0);
                } else {
                    assertTrue(val.compareTo(lastVal) > 0);
                }
            }
            lastVal = val;

            /* test toString() */
            assertTrue(Float.parseFloat(lastVal.toString()) == f);
        }
        FloatValueImpl valNan = new FloatValueImpl(Float.NaN);
        FloatValueImpl val1 = new FloatValueImpl(1F);
        String valStringNan = valNan.formatForKey(null);
        FloatValueImpl newValNan = new FloatValueImpl(valStringNan);
        assertTrue(valNan.equals(newValNan));
        assertFalse(valNan.equals(val1));

        /* test clone and getDefinition methods */
        Float fval = 1000000000.0F;
        FloatDefImpl fDef = new FloatDefImpl(null, Float.MIN_VALUE,
                Float.MAX_VALUE);
        FloatRangeValue fRangeVal = new FloatRangeValue(fval, fDef);
        FloatRangeValue valother = fRangeVal.clone();
        assertEquals(fRangeVal, valother);
        assertEquals(FieldDefImpl.Constants.floatDef,
                fRangeVal.getDefinition());

        /* test compareKeyValues method */
        int icheck = -1;
        float fvalue = 99.0f;
        int ival = 99;
        FieldValueImpl fvifloat = FloatDefImpl.Constants.floatDef
                .createFloat(fvalue);
        FieldValueImpl fviint = FloatDefImpl.Constants.integerDef
                .createInteger(ival);
        icheck = FieldValueImpl.compareKeyValues(fvifloat, fviint);
        assertEquals(0, icheck);

        long lval = 99;
        FieldValueImpl fvilong = LongDefImpl.Constants.longDef.createLong(lval);
        icheck = FieldValueImpl.compareKeyValues(fvifloat, fvilong);
        assertEquals(0, icheck);

        double dval = 99.0d;
        FieldValueImpl fvidouble = DoubleDefImpl.Constants.doubleDef
                .createDouble(dval);
        icheck = FieldValueImpl.compareKeyValues(fvifloat, fvidouble);
        assertEquals(0, icheck);

        BigDecimal bdval = new BigDecimal(99);
        FieldValueImpl fviNumber = NumberDefImpl.Constants.numberDef
                .createNumber(bdval);
        icheck = FieldValueImpl.compareKeyValues(fvifloat, fviNumber);
        assertEquals(0, icheck);

        FieldValueImpl fviboolean = BooleanDefImpl.Constants.booleanDef
                .createBoolean(true);
        try {
            icheck = FieldValueImpl.compareKeyValues(fvifloat, fviboolean);
            fail("Values are non-comparable");
        } catch (IllegalArgumentException e) {
            /* success */
        }

        /* test equal */
        assertTrue(fvifloat.equal(fviint));
        ival = 100;
        fviint.setInt(ival);
        assertFalse(fvifloat.equal(fviint));
        assertTrue(fvifloat.equal(fvilong));
        lval = 100;
        fvilong.setLong(lval);
        assertFalse(fvifloat.equal(fvilong));
        float f_value = 99.0f;
        FieldValueImpl fvi_float = FloatDefImpl.Constants.floatDef
                .createFloat(f_value);
        assertTrue(fvifloat.equal(fvi_float));
        f_value = 100.0f;
        fvi_float.setFloat(f_value);
        assertFalse(fvifloat.equal(fvi_float));
        assertTrue(fvifloat.equal(fvidouble));
        dval = 100.0d;
        fvidouble.setDouble(dval);
        assertFalse(fvifloat.equal(fvidouble));
        assertTrue(fvifloat.equal(fviNumber));
        BigDecimal bdvalue = new BigDecimal(100);
        FieldValueImpl fviNum = NumberDefImpl.Constants.numberDef
                .createNumber(bdvalue);
        assertFalse(fvifloat.equal(fviNum));

        int expint = 1000000000;
        long explong = 1000000000;
        FloatValueImpl fvi = new FloatValueImpl(fval);
        assertEquals(expint, fvi.castAsInt());
        assertEquals(explong, fvi.castAsLong());
        fvi.setFloat(f_value);
        f_value = 99f;
        fvi.setFloat(f_value);
        assertEquals(bdval, fvi.castAsDecimal());
        NumberValueImpl nvi = new NumberValueImpl(bdval);
        assertEquals(nvi, fvi.castAsNumber());
        fvi.setFloat(Float.MIN_VALUE);
        assertEquals(fvi, fvi.getMinimumValue());
        try {
            fvi.compareTo(nvi);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        f_value = 55f;
        fvi.setFloat(f_value);
        FloatValueImpl fviother = new FloatValueImpl(f_value);
        assertEquals(fvi.hashCode(), fviother.hashCode());
        FieldValueImpl fvistring = FieldDefImpl.Constants.stringDef
                .createString("ABC");
        assertFalse(fvi.equal(fvistring));
    }

    @Test
    public void testBoolean() {
        BooleanValueImpl val = BooleanValueImpl.trueValue;
        assertEquals(true, val.get());
        assertTrue(val.getType() == FieldDef.Type.BOOLEAN);
        assertTrue(val.clone().equals(val));
        assertFalse(val.equals(booleanNotEqual));
        assertFalse(val.equals(intNotEqual));
        assertTrue(val.isBoolean());
        assertFalse(val.isMap());
        assertFalse(val.isNumeric());
        assertTrue(val.isAtomic());
        assertFalse(val.isComplex());
        try {
            val.asEnum();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        assertEquals(val.compareTo(BooleanValueImpl.trueValue), 0);
        /* true is > false in terms of compareTo */
        assertEquals(val.compareTo(BooleanValueImpl.falseValue), 1);

        /* test toString() */
        assertTrue(Boolean.parseBoolean(val.toString()) == val.get());

        String keyVal = val.formatForKey(null);
        assertTrue(keyVal.length() == 1);
        BooleanValueImpl newVal = BooleanValueImpl.create(keyVal);
        assertEquals(newVal, val);

        /* round-trip false */
        val = BooleanValueImpl.create(false);
        keyVal = val.formatForKey(null);
        assertTrue(keyVal.length() == 1);
        newVal = BooleanValueImpl.create(keyVal);
        assertEquals(newVal, val);
    }

    @Test
    public void testBinary() {
        byte[] testArray = { 1, 2, 3, 4, 5, 6, 7};
        byte[] testArray1 = { 0, 2, 3, 4, 5, 6, 7};
        BinaryValueImpl val = new BinaryValueImpl(testArray);
        BinaryValue val1 = new BinaryValueImpl(testArray1);
        assertTrue(Arrays.equals(testArray, val.get()));
        assertTrue(val.getType() == FieldDef.Type.BINARY);
        assertTrue(val.clone().equals(val));
        assertFalse(val.equals(val1));
        assertFalse(val.equals(intNotEqual));
        assertTrue(val.isBinary());
        assertFalse(val.isMap());
        assertFalse(val.isNumeric());
        assertTrue(val.isAtomic());
        assertFalse(val.isComplex());
        try {
            val.asEnum();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        try {
        	val.formatForKey(null);
            fail("Binary is not allowed as a key");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* toString round trip */
        BinaryDefImpl bdi = new BinaryDefImpl();
        String s = val.toString();
        BinaryValueImpl bvi = bdi.fromString(s);
        assertTrue(bvi.equals(val));

        try {
            val.castAsString();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        try {
            FieldValueImpl fvlBin = new BinaryValueImpl(testArray);
            fvlBin.getMinimumValue();
            fail("Type does not implement getMinimumValue");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            FieldValueImpl fvlBin = new BinaryValueImpl(testArray);
            fvlBin.getNextValue();
            fail("Type does not implement getNextValue");
        } catch (IllegalArgumentException e) {
            /* success */
        }

        byte[] testArray2 = { 0, 2, 3, 4, 5, 6, 7 };
        FieldValueImpl fvlBin = new BinaryValueImpl(testArray1);
        FieldValueImpl fvlBinother = new BinaryValueImpl(testArray2);
        assertEquals(0, fvlBin.compareTo(fvlBinother));

        FieldValueImpl fviBin = new BinaryValueImpl(testArray);
        FieldValueImpl fviBinSame = new BinaryValueImpl(testArray);
        FieldValueImpl fviBinother = new BinaryValueImpl(testArray1);
        FixedBinaryDefImpl def = new FixedBinaryDefImpl("foo", 7, null);
        FixedBinaryValueImpl fviFixBin = def.createFixedBinary(testArray);
        FixedBinaryValueImpl fviFixBinOther = def.createFixedBinary(testArray1);
        FieldValueImpl fviInt = FieldDefImpl.Constants.integerDef
                .createInteger(10);
        assertTrue(fviBin.equal(fviBinSame));
        assertFalse(fviBin.equal(fviBinother));
        assertTrue(fviBin.equal(fviFixBin));
        assertFalse(fviBin.equal(fviFixBinOther));
        assertFalse(fviBin.equal(fviInt));
        assertEquals(0, val.compareTo(fviBin));
        assertEquals(-1, val.compareTo(fviBinother));
        testArray = null;
        try {
            @SuppressWarnings("unused")
            BinaryValueImpl bviZero = new BinaryValueImpl(testArray);
            fail("null not allowed for binary value");
        } catch (IllegalArgumentException iae) {
            /* suceess */
        }
    }

    @Test
    public void testFixedBinary() {
        byte[] testArray = { 1, 2, 3, 4, 5, 6, 7};
        byte[] testArray1 = { 0, 1, 2, 3, 4, 5, 6, 7};
        FixedBinaryDefImpl def = new FixedBinaryDefImpl("foo", 7, null);
        FixedBinaryValueImpl val = def.createFixedBinary(testArray);
        try {
            val = def.createFixedBinary(testArray1);
            fail("Set should have failed");
        } catch (IllegalArgumentException iae) {
        }
        try {
            def.createFixedBinary(testArray1);
            fail("Create should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /* toString round trip */
        String s = val.toString();
        FixedBinaryValueImpl bvi = def.fromString(s);
        assertTrue(bvi.equals(val));

        assertFalse(val.isNumeric());
        assertTrue(val.isAtomic());
        assertFalse(val.isComplex());

        /* test equal method */
        FixedBinaryDefImpl fixedBinDef = new FixedBinaryDefImpl("foo", 7, null);
        FixedBinaryValueImpl fixedBinValUnmatch = new FixedBinaryValueImpl(
                testArray1, fixedBinDef);
        FieldValueImpl fviFixBin = new FixedBinaryValueImpl(testArray,
                fixedBinDef);
        FieldValueImpl fviFixBinOther = new FixedBinaryValueImpl(testArray,
                fixedBinDef);
        BinaryValueImpl fviBin = new BinaryValueImpl(testArray);
        BinaryValueImpl fviBinOther = new BinaryValueImpl(testArray1);
        FieldValueImpl fviInt = FieldDefImpl.Constants.integerDef
                .createInteger(10);
        assertTrue(fviFixBin.equal(fviFixBinOther));
        assertFalse(fviFixBin.equal(fixedBinValUnmatch));
        assertTrue(fviFixBin.equal(fviBin));
        assertFalse(fviFixBin.equal(fviBinOther));
        assertFalse(fviFixBin.equal(fviInt));

        FixedBinaryDefImpl fixedBinNewDef = new FixedBinaryDefImpl("foo1", 8,
                null);
        FixedBinaryValueImpl fixedBinValOther = new FixedBinaryValueImpl(
                testArray1, fixedBinDef);
        FixedBinaryValueImpl fixedBinNewVal = new FixedBinaryValueImpl(
                testArray1, fixedBinNewDef);
        assertTrue(fixedBinValUnmatch.equals(fixedBinValOther));
        assertFalse(fixedBinNewVal.equals(fixedBinValOther));
        assertFalse(val.equals(fixedBinValOther));
        assertFalse(fixedBinValUnmatch.equals(fviInt));
        assertEquals(0, val.compareTo(fixedBinValUnmatch));
        try {
            fixedBinValUnmatch.compareTo(fviInt);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
    }

    @Test
    public void testString() {
        String values[] = {"01234", "", "123456789"};
        for (String s : values) {
            StringValueImpl val = new StringValueImpl(s);
            assertEquals(s, val.get());
            assertTrue(val.getType() == FieldDef.Type.STRING);
            assertTrue(val.clone().equals(val));
            assertFalse(val.equals(stringNotEqual));
            assertFalse(val.equals(intNotEqual));
            assertEquals(val.compareTo(val.clone()), 0);
            String keyFormat = val.formatForKey(null);
            assertTrue(new StringValueImpl(keyFormat).equals(val));
            assertTrue(val.isString());
            assertFalse(val.isMap());
            assertFalse(val.isNumeric());
            assertTrue(val.isAtomic());
            assertFalse(val.isComplex());
            try {
                val.asEnum();
                fail("Cast should have thrown");
            } catch (ClassCastException e) {
                /* success */
            }
            if (!val.get().equals("")) {
                assertTrue(val.compareTo(new StringValueImpl("00")) > 0);
                assertTrue(val.compareTo(new StringValueImpl("70")) < 0);
            }
        }

        String sVal = "abc";
        StringDefImpl sDef = new StringDefImpl(null);
        StringRangeValue sRangeVal = new StringRangeValue(sVal, sDef);
        StringRangeValue valother = sRangeVal.clone();
        assertEquals(sRangeVal, valother);
        assertEquals(FieldDefImpl.Constants.stringDef,
                sRangeVal.getDefinition());

        StringValueImpl val = new StringValueImpl(sVal);
        try {
            val.castAsLong();
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        String strval = "a";
        String enums1[] = { "a", "b", "c" };
        EnumDef def1 = new EnumDefImpl("enum1", enums1);
        FieldValueImpl fvistring = StringDefImpl.Constants.stringDef
                .createString(strval);
        FieldValueImpl fvienum = new EnumValueImpl(def1, enums1[0]);
        int icomp = FieldValueImpl.compareKeyValues(fvistring, fvienum);
        assertEquals(0, icomp);
    }

    @SuppressWarnings("unused")
	@Test
    public void testEnum() {
        String enums1[] = {"a", "b", "c"};
        String enums2[] = {"z1_", "z2_", "z3_"};
        EnumDef def1 = new EnumDefImpl("enum1", enums1);
        EnumDef def2 = new EnumDefImpl("enum2", enums2);
        assertFalse(def1.equals(def2));
        for (String s1 : enums1) {
            for (String s2 : enums2) {
                EnumValueImpl val1 = new EnumValueImpl(def1, s1);
                EnumValue val2 = new EnumValueImpl(def2, s2);
                assertEquals(s1, val1.get());
                assertEquals(s2, val2.get());
                assertTrue(val1.getType() == FieldDef.Type.ENUM);
                assertTrue(val2.getType() == FieldDef.Type.ENUM);
                assertTrue(val1.clone().equals(val1));
                assertTrue(val2.clone().equals(val2));
                assertFalse(val1.equals(val2));
                assertFalse(val1.equals(intNotEqual));
                assertEquals(val1.compareTo(val1.clone()), 0);
                String keyFormat = val1.formatForKey(null);
                assertTrue(EnumValueImpl.createFromKey(def1, keyFormat)
                           .equals(val1));
                assertFalse(val1.isMap());
                assertFalse(val1.isNumeric());
                assertTrue(val1.isAtomic());
                assertFalse(val1.isComplex());
                try {
                    val1.asMap();
                    fail("Cast should have thrown");
                } catch (ClassCastException e) {
                    /* success */
                }
                try {
                    new EnumValueImpl(def1, "x");
                    fail("Construction should fail");
                } catch (IllegalArgumentException iae) {
                    /* success */
                }
                assertTrue(val1.compareTo(new EnumValueImpl(def1, "a")) >= 0);
                assertTrue(val1.compareTo(new EnumValueImpl(def1, "c")) <= 0);
                assertTrue(val1.toString().equals(s1));
                assertTrue(val2.toString().equals(s2));
            }
        }

        String strval = "a";
        FieldValueImpl fvienum = new EnumValueImpl(def1, enums1[0]);
        FieldValueImpl fvistring = StringDefImpl.Constants.stringDef
                .createString(strval);
        int icomp = FieldValueImpl.compareKeyValues(fvienum, fvistring);
        assertEquals(0, icomp);

        FieldValueImpl fviInt = FieldDefImpl.Constants.integerDef
                .createInteger(10);
        EnumValueImpl enumVal = new EnumValueImpl(def1, "a");
        try {
            enumVal.compareTo(fviInt);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        try {
            EnumValueImpl fviEnum = new EnumValueImpl(null, "a");
            fail("Enum Def should not be null");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            EnumValueImpl fviEnum = new EnumValueImpl(def1, null);
            fail("Enum Value should not be null");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
    }

    /*
     * This needs fleshing out.  Initially it's just trying out the array
     * add and List get methods on ArrayValue.
     */
    @Test
    public void testArray() {
        int vals[] = {1, 2, 3, 4, 5};
        int valsOther[] = {1, 2, 3, 4};
        long lArrayVal[] = {1L, 2L, 3L, 4L};
        float fArrayVal[] = {1.1f, 2.1f, 3.1f};
        double dArrayVal[] = {1.1d, 2.1d, 3.1d};
        String sarrayVal[] = {"A", "B", "C"};
        boolean barrayVal[] = {true, false, true};
        byte btarrVal[][] = {{1,2}, {3,4}, {5,6}};
        Timestamp tsarrVal[] = new Timestamp[] {
                TimestampUtils.parseString("2016-07-21T20:47:15.987654321"),
                TimestampUtils.parseString("-2016-07-21T20:47:15.987654321"),
            };
        BigDecimal bigdecVal[] = {
                BigDecimal.ZERO,
                BigDecimal.ONE,
                BigDecimal.TEN,
            };

        IntegerDefImpl idef = new IntegerDefImpl();
        ArrayDefImpl arrayDef = new ArrayDefImpl(idef);
        ArrayValue val = arrayDef.createArray();
        val.add(vals);
        List<FieldValue> list = val.toList();
        assertTrue(list.size() == vals.length);
        assertTrue(list.get(1).asInteger().get() == 2);
        val.set(0, 5);
        assertTrue(list.size() == vals.length);
        assertTrue(list.get(0).asInteger().get() == 5);
        assertFalse(val.isNumeric());
        assertFalse(val.isAtomic());
        assertTrue(val.isComplex());
        /* TODO: expand to test other simple data types */

        /* Test nested Number type */
        NumberDefImpl numberDef = new NumberDefImpl();
        arrayDef = new ArrayDefImpl(numberDef);
        val = arrayDef.createArray();
        int ival = 1;
        val.addNumber(ival);
        checkNumberValue(val.get(0).asNumber(), BigDecimal.valueOf(ival));

        long lval = 99999999999L;
        val.addNumber(lval);
        checkNumberValue(val.get(1).asNumber(), BigDecimal.valueOf(lval));

        float fval = -3.21431f;
        val.addNumber(fval);
        checkNumberValue(val.get(2).asNumber(), BigDecimal.valueOf(fval));

        double dval = 332131.21378001d;
        val.addNumber(dval);
        checkNumberValue(val.get(3).asNumber(), BigDecimal.valueOf(dval));

        BigDecimal bd = new BigDecimal("3.3213121378001321313E-1024");
        val.addNumber(bd);
        checkNumberValue(val.get(4).asNumber(), bd);

        /* test add methods */
        LongDefImpl ldef = new LongDefImpl();
        ArrayDefImpl arrayDefLong = new ArrayDefImpl(ldef);
        ArrayValue valLong = arrayDefLong.createArray();
        valLong.add(lArrayVal);
        List<FieldValue> listLong = valLong.toList();
        assertTrue(listLong.size() == lArrayVal.length);
        ArrayValue valLongOther = arrayDefLong.createArray();
        valLongOther.add(lArrayVal);
        assertEquals(valLong.hashCode(), valLongOther.hashCode());

        FloatDefImpl fdef = new FloatDefImpl();
        ArrayDefImpl arrayDefFloat = new ArrayDefImpl(fdef);
        ArrayValue valFloat = arrayDefFloat.createArray();
        valFloat.add(fArrayVal);
        List<FieldValue> listFloat = valFloat.toList();
        assertTrue(listFloat.size() == fArrayVal.length);

        DoubleDefImpl ddef = new DoubleDefImpl();
        ArrayDefImpl arrayDefDouble = new ArrayDefImpl(ddef);
        ArrayValue valDouble = arrayDefDouble.createArray();
        valDouble.add(dArrayVal);
        List<FieldValue> listDouble = valDouble.toList();
        assertTrue(listDouble.size() == dArrayVal.length);

        StringDefImpl sdef = new StringDefImpl();
        ArrayDefImpl arrayDefString = new ArrayDefImpl(sdef);
        ArrayValue valString = arrayDefString.createArray();
        valString.add(sarrayVal);
        List<FieldValue> listString = valString.toList();
        assertTrue(listString.size() == sarrayVal.length);

        BooleanDefImpl bdef = new BooleanDefImpl();
        ArrayDefImpl arrayDefBoolean = new ArrayDefImpl(bdef);
        ArrayValue valBoolean = arrayDefBoolean.createArray();
        valBoolean.add(barrayVal);
        List<FieldValue> listBoolean = valBoolean.toList();
        assertTrue(listBoolean.size() == barrayVal.length);

        BinaryDefImpl bindef = new BinaryDefImpl();
        ArrayDefImpl arrayDefBytearray = new ArrayDefImpl(bindef);
        ArrayValue valBytearray = arrayDefBytearray.createArray();
        valBytearray.add(btarrVal);
        List<FieldValue> listBytearray = valBytearray.toList();
        assertTrue(listBytearray.size() == btarrVal.length);

        TimestampDefImpl tsdef = new TimestampDefImpl(9);
        ArrayDefImpl arrayDefTimestamp = new ArrayDefImpl(tsdef);
        ArrayValue valTimestamp = arrayDefTimestamp.createArray();
        valTimestamp.add(tsarrVal);
        List<FieldValue> listTimestamp = valTimestamp.toList();
        assertTrue(listTimestamp.size() == tsarrVal.length);

        val = arrayDef.createArray();
        val.addNumber(vals);
        List<FieldValue> listIntNum = val.toList();
        assertTrue(listIntNum.size() == vals.length);

        valLong = arrayDef.createArray();
        valLong.addNumber(lArrayVal);
        List<FieldValue> listLongNum = valLong.toList();
        assertTrue(listLongNum.size() == lArrayVal.length);

        valFloat = arrayDef.createArray();
        valFloat.addNumber(fArrayVal);
        List<FieldValue> listFloatNum = valFloat.toList();
        assertTrue(listFloatNum.size() == fArrayVal.length);

        valDouble = arrayDef.createArray();
        valDouble.addNumber(dArrayVal);
        List<FieldValue> listDoubleNum = valDouble.toList();
        assertTrue(listDoubleNum.size() == dArrayVal.length);

        ArrayValue valBigDecimal = arrayDef.createArray();
        valBigDecimal.addNumber(bigdecVal);
        List<FieldValue> listBigDecimal = valBigDecimal.toList();
        assertTrue(listBigDecimal.size() == bigdecVal.length);

        FixedBinaryDefImpl fixbindef = new FixedBinaryDefImpl("fixed", 2);
        ArrayDefImpl arrayDefFixBytearray = new ArrayDefImpl(fixbindef);
        ArrayValue valFixBytearray = arrayDefFixBytearray.createArray();
        valFixBytearray.addFixed(btarrVal);
        List<FieldValue> listFixBytearray = valBytearray.toList();
        assertTrue(listFixBytearray.size() == btarrVal.length);

        EnumDefImpl enumdef = new EnumDefImpl("enum", sarrayVal);
        ArrayDefImpl arrayDefEnum = new ArrayDefImpl(enumdef);
        ArrayValue valEnum = arrayDefEnum.createArray();
        valEnum.addEnum(sarrayVal);
        List<FieldValue> listEnumArray = valEnum.toList();
        assertTrue(listEnumArray.size() == sarrayVal.length);

        String str1 = "{\"a\":1}";
        String str2 = "{\"a\":2}";
        String str3 = "{\"a\":3}";
        String str4 = "{\"a\":10}";
        JsonDefImpl jsondef = new JsonDefImpl();
        ArrayDefImpl arrayDefJson = new ArrayDefImpl(jsondef);
        ArrayValue jsonArray = arrayDefJson.createArray();
        jsonArray.addJson(0, str1);
        jsonArray.addJson(1, str2);
        jsonArray.addJson(2, str3);
        String valactual = jsonArray.get(1).toJsonString(false);
        assertEquals(str2, valactual);
        jsonArray.setJson(0, str4);
        valactual = jsonArray.get(0).toJsonString(false);
        assertEquals(str4, valactual);

        double dVal1 = 1.1d;
        double dVal2 = 2.1d;
        double dVal3 = 3.1d;
        double dVal4 = 4.1d;
        double delta = 0.1d;
        valDouble = arrayDefDouble.createArray();
        valDouble.add(0, dVal1);
        valDouble.add(1, dVal2);
        valDouble.add(2, dVal3);
        double dvalactual = valDouble.get(1).asDouble().get();
        assertEquals(dVal2, dvalactual, delta);
        valDouble.set(0, dVal4);
        dvalactual = valDouble.get(0).asDouble().get();
        assertEquals(dVal4, dvalactual, delta);

        long lVal1 = 1L;
        long lVal2 = 2L;
        long lVal3 = 3L;
        long lVal4 = 4L;
        valLong = arrayDefLong.createArray();
        valLong.add(0, lVal1);
        valLong.add(1, lVal2);
        valLong.add(2, lVal3);
        long lvalactual = valLong.get(1).asLong().get();
        assertEquals(lVal2, lvalactual);
        valLong.set(0, lVal4);
        lvalactual = valLong.get(0).asLong().get();
        assertEquals(lVal4, lvalactual);

        String strval1 = "A";
        String strval2 = "B";
        String strval3 = "C";
        String strval4 = "D";
        valString = arrayDefString.createArray();
        valString.add(0, strval1);
        valString.add(1, strval2);
        valString.add(2, strval3);
        String svalactual = valString.get(1).asString().get();
        assertEquals(strval2, svalactual);
        valString.set(0, strval4);
        svalactual = valString.get(0).asString().get();
        assertEquals(strval4, svalactual);

        float fval1 = 1.1f;
        float fval2 = 2.1f;
        float fval3 = 3.1f;
        float fdelta = 0.1f;
        valFloat = arrayDefFloat.createArray();
        valFloat.add(0, fval1);
        valFloat.add(1, fval2);
        valFloat.add(2, fval3);
        float fvalactual = valFloat.get(1).asFloat().get();
        assertEquals(fval2, fvalactual, fdelta);

        boolean bval1 = true;
        boolean bval2 = false;
        boolean bval3 = true;
        boolean bval4 = false;
        valBoolean = arrayDefBoolean.createArray();
        valBoolean.add(0, bval1);
        valBoolean.add(1, bval2);
        valBoolean.add(2, bval3);
        boolean bvalactual = valBoolean.get(1).asBoolean().get();
        assertEquals(bval2, bvalactual);
        valBoolean.set(0, bval4);
        bvalactual = valBoolean.get(0).asBoolean().get();
        assertEquals(bval4, bvalactual);

        Timestamp ts1 = TimestampUtils
                .parseString("2016-07-21T20:47:15.987654321");
        Timestamp ts2 = TimestampUtils
                .parseString("2016-07-22T20:47:15.987654321");
        Timestamp ts3 = TimestampUtils
                .parseString("2016-07-23T20:47:15.987654321");
        Timestamp ts4 = TimestampUtils
                .parseString("2016-07-24T20:47:15.987654321");
        valTimestamp = arrayDefTimestamp.createArray();
        valTimestamp.add(0, ts1);
        valTimestamp.add(1, ts2);
        valTimestamp.add(2, ts3);
        Timestamp tsvalactual = valTimestamp.get(1).asTimestamp().get();
        assertEquals(ts2, tsvalactual);
        valTimestamp.set(0, ts4);
        tsvalactual = valTimestamp.get(0).asTimestamp().get();
        assertEquals(ts4, tsvalactual);

        /* test addNumber methods */
        int ival1 = 1;
        int ival2 = 2;
        int ival3 = 3;
        int ival4 = 4;
        ArrayValue arrintnum = arrayDef.createArray();
        arrintnum.addNumber(0, ival1);
        arrintnum.addNumber(1, ival2);
        arrintnum.addNumber(2, ival3);
        BigDecimal bdactualvalint = arrintnum.get(1).asNumber().get();
        assertEquals(ival2, bdactualvalint.intValue());
        arrintnum.setNumber(0, ival4);
        bdactualvalint = arrintnum.get(0).asNumber().get();
        assertEquals(ival4, bdactualvalint.intValue());

        float fval4 = 4.1f;
        ArrayValue arrvalfloat = arrayDef.createArray();
        arrvalfloat.addNumber(0, fval1);
        arrvalfloat.addNumber(1, fval2);
        arrvalfloat.addNumber(2, fval3);
        BigDecimal bdactualval = arrvalfloat.get(1).asNumber().get();
        assertEquals(fval2, bdactualval.floatValue(), fdelta);
        arrvalfloat.setNumber(0, fval4);
        bdactualval = arrvalfloat.get(0).asNumber().get();
        assertEquals(fval4, bdactualval.floatValue(), fdelta);

        long lval1 = 1;
        long lval2 = 2;
        long lval3 = 3;
        long lval4 = 4;
        ArrayValue arrlongnum = arrayDef.createArray();
        arrlongnum.addNumber(0, lval1);
        arrlongnum.addNumber(1, lval2);
        arrlongnum.addNumber(2, lval3);
        BigDecimal bdactualvallong = arrlongnum.get(1).asNumber().get();
        assertEquals(lval2, bdactualvallong.longValue());
        arrlongnum.setNumber(0, ival4);
        bdactualvallong = arrlongnum.get(0).asNumber().get();
        assertEquals(lval4, bdactualvallong.intValue());

        ArrayValue arrdoublenum = arrayDef.createArray();
        arrdoublenum.addNumber(0, dVal1);
        arrdoublenum.addNumber(1, dVal2);
        arrdoublenum.addNumber(2, dVal3);
        BigDecimal bdactualvaldouble = arrdoublenum.get(1).asNumber().get();
        assertEquals(dVal2, bdactualvaldouble.doubleValue(), delta);
        arrdoublenum.setNumber(0, dVal4);
        bdactualvaldouble = arrdoublenum.get(0).asNumber().get();
        assertEquals(dVal4, bdactualvaldouble.doubleValue(), delta);

        BigDecimal bd1 = new BigDecimal("1.3213121378001321313E-1024");
        BigDecimal bd2 = new BigDecimal("2.3213121378001321313E-1024");
        BigDecimal bd3 = new BigDecimal("3.3213121378001321313E-1024");
        BigDecimal bd4 = new BigDecimal("4.3213121378001321313E-1024");
        ArrayValue arrbigdecimalnum = arrayDef.createArray();
        arrbigdecimalnum.addNumber(0, bd1);
        arrbigdecimalnum.addNumber(1, bd2);
        arrbigdecimalnum.addNumber(2, bd3);
        BigDecimal bdactualvalbd = arrbigdecimalnum.get(1).asNumber().get();
        assertEquals(bd2.doubleValue(), bdactualvalbd.doubleValue(), delta);
        arrbigdecimalnum.setNumber(0, bd4);
        bdactualvalbd = arrbigdecimalnum.get(0).asNumber().get();
        assertEquals(bd4.doubleValue(), bdactualvalbd.doubleValue(), delta);

        /* fastExternalNotSupported */
        IntegerDefImpl idefImpl = new IntegerDefImpl();
        ArrayDefImpl arrayDefImpl = new ArrayDefImpl(idefImpl);
        ArrayValueImpl arrValImpl = new ArrayValueImpl(arrayDefImpl);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        try {
            arrValImpl.writeFastExternal(dos, SerialVersion.CURRENT);
            fail("FastExternal serialization not supported");
        } catch (IllegalStateException e) {
            /* success */
        }

        FieldValueImpl fvi = new IntegerValueImpl(ival);
        assertFalse(arrValImpl.equal(fvi));

        arrValImpl.add(vals);
        ArrayValueImpl arrValImplLong = new ArrayValueImpl(arrayDefLong);
        arrValImplLong.add(lArrayVal);
        assertFalse(arrValImpl.equal(arrValImplLong));
        ArrayValueImpl arrValImplFloat = new ArrayValueImpl(arrayDefFloat);
        arrValImplFloat.add(fArrayVal);
        ArrayValueImpl arrValImplDouble = new ArrayValueImpl(arrayDefDouble);
        arrValImplDouble.add(dArrayVal);
        assertFalse(arrValImplFloat.equal(arrValImplDouble));

        arrValImplFloat = FieldDefImpl.Constants.jsonDef.createArray();
        arrValImplFloat.add(0, fval1);
        double dvalActual = arrValImplFloat.get(0).asDouble().get();
        assertEquals(fval1, dvalActual, fdelta);

        arrValImplLong = FieldDefImpl.Constants.jsonDef.createArray();
        arrValImplLong.add(0, lval1);
        arrValImplLong.set(0, lval2);
        long lvalActual = arrValImplLong.get(0).asLong().get();
        assertEquals(lval2, lvalActual);

        try {
            arrValImplDouble.compareTo(arrValImplLong);
            fail("Definition should be same");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            IntegerValueImpl ivi = new IntegerValueImpl(ival);
            arrValImplDouble.compareTo(ivi);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        ArrayValueImpl aviIntOther = new ArrayValueImpl(arrayDefImpl);
        aviIntOther.add(valsOther);
        assertEquals(1, arrValImpl.compareTo(aviIntOther));
        assertFalse(arrValImplLong.equals(arrValImplFloat));

        ArrayValueImpl arrValImplOther = arrValImplLong.clone();
        arrValImplOther.set(0, 1000L);
        assertFalse(arrValImplLong.equals(arrValImplOther));

        String jsonInput = "[{\"a\":10},{\"b\":11}]";
        JsonDefImpl jsondefImpl = new JsonDefImpl();
        ArrayDefImpl arrayDefJsonImpl = new ArrayDefImpl(jsondefImpl);
        ArrayValueImpl arrJsonVal = arrayDefJsonImpl.createArray();
        Reader reader = new StringReader(jsonInput);
        ComplexValueImpl.createFromJson(arrJsonVal, reader, true, true);
        assertEquals(2, arrJsonVal.size());
        assertEquals("[{\"a\":10}, {\"b\":11}]",
                arrJsonVal.toList().toString());

        try {
            Reader readerNew = new StringReader("a");
            ComplexValueImpl.createFromJson(arrJsonVal, readerNew, true, true);
            fail("Can not parse provided invalid json");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        try {
            String jsonBad = "{\"a\":10},{\"b\":11}]";
            Reader readerBad = new StringReader(jsonBad);
            ComplexValueImpl.createFromJson(arrJsonVal, readerBad, true, true);
            fail("Can not parse provided invalid json");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
    }

    @Test
    public void testMap() {
        int vals[] = {1, 2, 3, 4, 5};
        int valsother[] = {1, 2, 3, 4, 5, 6};
        int valsdiff[] = { 7, 8, 9, 1, 2, 3 };
        IntegerDefImpl idef = new IntegerDefImpl();
        MapDefImpl mapDef = new MapDefImpl(idef);
        MapValue val = mapDef.createMap();
        for (int i = 0; i < vals.length; i++) {
            val.put("value" + i, vals[i]);
        }
        assertTrue(val.getFields().size() == vals.length);
        assertTrue(val.get("value2").asInteger().get() == vals[2]);
        val.put("value2", 100);
        assertTrue(val.get("value2").asInteger().get() == 100);

        assertTrue(val.compareTo(val.clone()) == 0);

        assertFalse(val.isNumeric());
        assertFalse(val.isAtomic());
        assertTrue(val.isComplex());

        /* Test nested Number type */
        NumberDefImpl numberDef = new NumberDefImpl();
        mapDef = new MapDefImpl(numberDef);
        val = mapDef.createMap();
        int ival = 1;
        val.putNumber("value1", ival);
        checkNumberValue(val.get("value1").asNumber(),
                         BigDecimal.valueOf(ival));

        long lval = 99999999999L;
        val.putNumber("value2", lval);
        checkNumberValue(val.get("value2").asNumber(),
                         BigDecimal.valueOf(lval));

        float fval = -3.21431f;
        val.putNumber("value3", fval);
        checkNumberValue(val.get("value3").asNumber(),
                         BigDecimal.valueOf(fval));

        double dval = 332131.21378001d;
        val.putNumber("value4", dval);
        checkNumberValue(val.get("value4").asNumber(),
                         BigDecimal.valueOf(dval));

        BigDecimal bd = new BigDecimal("3.3213121378001321313E-1024");
        val.putNumber("value5", bd);
        checkNumberValue(val.get("value5").asNumber(), bd);

        MapValueImpl mvi = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < vals.length; i++) {
            mvi.put("value" + i, vals[i]);
        }
        FieldValueImpl fvi = new IntegerValueImpl(ival);
        assertFalse(mvi.equal(fvi));

        MapValueImpl mviother = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < valsother.length; i++) {
            mviother.put("value" + i, valsother[i]);
        }
        assertFalse(mvi.equal(mviother));
        MapValueImpl mvisame = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < valsother.length; i++) {
            mvisame.put("value" + i, valsother[i]);
        }
        assertTrue(mviother.equal(mvisame));

        MapValueImpl mvidiff = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < valsdiff.length; i++) {
            mvidiff.put("value" + i, valsdiff[i]);
        }
        assertFalse(mviother.equal(mvidiff));

        try {
            mvi.compareTo(fvi);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        try {
            mviother.compareTo(val);
            fail("Definition mismatch for both map");
        } catch (IllegalArgumentException e) {
            /* success */
        }

        assertEquals(mviother.hashCode(), mvisame.hashCode());
        assertEquals(-1, mviother.compareTo(mvidiff));
        /* value different */
        assertNotEquals(0, mviother.compareTo(mvidiff));
        /* key different */
        MapValueImpl mvikeydiff = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < valsdiff.length; i++) {
            mvikeydiff.put("value1" + i, valsdiff[i]);
        }
        assertNotEquals(0, mvikeydiff.compareTo(mvidiff));
        /* more keys */
        MapValueImpl valImpl = FieldDefImpl.Constants.mapAnyDef.createMap();
        for (int i = 0; i < vals.length; i++) {
            valImpl.put("value" + i, vals[i]);
        }
        assertEquals(1, mviother.compareTo(valImpl));
        assertEquals(-1, valImpl.compareTo(mviother));
    }

    @Test
    public void testRecord() {
        int iVal = 100;
        String sVal = "string value";
        boolean bVal = true;

        RecordDef recordDef =
            (RecordDef) TableBuilder.createRecordBuilder("foo")
            .addInteger("int")
            .addString("string")
            .addBoolean("bool", null).build();

        RecordValue val = recordDef.createRecord();
        val.put("int", iVal);
        val.put("string", sVal);
        val.put("bool", bVal);

        assertTrue(val.getFieldNames().size() == 3);
        assertTrue(val.get("int").asInteger().get() == iVal);
        assertTrue(val.get("string").asString().get() == sVal);
        assertTrue(val.get("bool").asBoolean().get() == bVal);

        val.put("int", iVal + 100);
        assertTrue(val.get("int").asInteger().get() == iVal + 100);
        val.putNull("string");
        assertTrue(val.get("string").isNull());

        assertTrue(val.compareTo(val.clone()) == 0);

        assertFalse(val.isNumeric());
        assertFalse(val.isAtomic());
        assertTrue(val.isComplex());

        /* Test nested Number type */
        recordDef = (RecordDef) TableBuilder.createRecordBuilder("foo")
                    .addNumber("intVal")
                    .addNumber("longVal")
                    .addNumber("floatVal")
                    .addNumber("doubleVal")
                    .addNumber("bigDecimalVal").build();

        val = recordDef.createRecord();

        int ival = 1;
        val.putNumber("intVal", ival);

        long lval = 99999999999L;
        val.putNumber("longVal", lval);

        float fval = -3.21431f;
        val.putNumber("floatVal", fval);

        double dval = 332131.21378001d;
        val.putNumber("doubleVal", dval);

        BigDecimal bd = new BigDecimal("3.3213121378001321313E-1024");
        val.putNumber("bigDecimalVal", bd);

        checkNumberValue(val.get("intVal").asNumber(),
                         BigDecimal.valueOf(ival));
        checkNumberValue(val.get("longVal").asNumber(),
                         BigDecimal.valueOf(lval));
        checkNumberValue(val.get("floatVal").asNumber(),
                         BigDecimal.valueOf(fval));
        checkNumberValue(val.get("doubleVal").asNumber(),
                         BigDecimal.valueOf(dval));
        checkNumberValue(val.get("bigDecimalVal").asNumber(), bd);

        /* test equal method */
        double dValue = 10.0d;
        long lVal = 10;
        RecordDef recordDef1 =
                (RecordDef) TableBuilder.createRecordBuilder("foo")
                .addInteger("int")
                .addString("string")
                .addBoolean("bool", null).build();
        RecordValueImpl rec1 = new RecordValueImpl(recordDef1);
        rec1.put("int", iVal);
        rec1.put("string", sVal);
        rec1.put("bool", bVal);
        RecordDef recordDef2 =
                (RecordDef) TableBuilder.createRecordBuilder("foo")
                .addInteger("int")
                .addString("string")
                .addBoolean("bool", null)
                .addDouble("double").build();
        RecordValueImpl rec2 = new RecordValueImpl(recordDef2);
        rec2.put("int", iVal);
        rec2.put("string", sVal);
        rec2.put("bool", bVal);
        rec2.put("double", dValue);
        RecordDef recordDef3 =
                (RecordDef) TableBuilder.createRecordBuilder("foo")
                .addInteger("int")
                .addLong("long")
                .addBoolean("bool", null).build();
        RecordValueImpl rec3 = new RecordValueImpl(recordDef3);
        rec3.put("int", iVal);
        rec3.put("long", lVal);
        rec3.put("bool", bVal);
        assertFalse(rec1.equal(rec3));
        RecordValueImpl rec4 = new RecordValueImpl(recordDef3);
        rec4.put("int", iVal);
        lVal=50;
        rec4.put("long", lVal);
        rec4.put("bool", bVal);
        assertFalse(rec1.equal(rec3));

        FieldValueImpl fvi = new IntegerValueImpl(ival);
        assertFalse(rec1.equal(fvi));
        assertFalse(rec1.equal(rec2));
        assertFalse(rec3.equal(rec4));
        lVal=10;
        rec4.put("long", lVal);
        assertTrue(rec3.equal(rec4));
    }

    private void checkNumberValue(FieldValue fval, BigDecimal bd) {
        assertTrue(fval.isNumber());
        assertTrue(fval.asNumber().get().compareTo(bd) == 0);
    }

    @Test
    public void testPutNestedRangeValue() {

        /* Integer type */
        FieldDefImpl elemDef = new IntegerDefImpl(null, 0, 100);
        FieldDef valDef = new IntegerDefImpl();
        FieldValue value = valDef.createInteger(10);
        FieldValue invalidValue = valDef.createInteger(101);
        doPutFieldValue(elemDef, value, invalidValue);

        /* Long type */
        elemDef = new LongDefImpl(null, 0L, 100L);
        valDef = new LongDefImpl();
        value = valDef.createLong(10);
        invalidValue = valDef.createLong(101);
        doPutFieldValue(elemDef, value, invalidValue);

        /* Float type */
        elemDef = new FloatDefImpl(null, 0.0f, 100.0f);
        valDef = new FloatDefImpl();
        value = valDef.createFloat(10f);
        invalidValue = valDef.createFloat(100.1f);
        doPutFieldValue(elemDef, value, invalidValue);

        /* Double type */
        elemDef = new DoubleDefImpl(null, 0.0d, 100.0d);
        valDef = new DoubleDefImpl();
        value = valDef.createDouble(10d);
        invalidValue = valDef.createDouble(100.1d);
        doPutFieldValue(elemDef, value, invalidValue);

        /* String type */
        elemDef = new StringDefImpl(null, "aaa", "baa", true, false);
        valDef = new StringDefImpl();
        value = valDef.createString("abc");
        invalidValue = valDef.createString("baa");
        doPutFieldValue(elemDef, value, invalidValue);
    }

    private void doPutFieldValue(FieldDefImpl elemDef,
                                 FieldValue value,
                                 FieldValue invalidValue) {

        ArrayDefImpl arrayDef = new ArrayDefImpl(elemDef);
        MapDefImpl mapDef = new MapDefImpl(elemDef);
        RecordDefImpl recDef =
            (RecordDefImpl)TableBuilder.createRecordBuilder("rec")
                                       .addField("elem", elemDef).build();

        ArrayValue av = arrayDef.createArray();
        MapValue mv = mapDef.createMap();
        RecordValue rv = recDef.createRecord();

        av.add(value);
        assertTrue(av.size() == 1);

        mv.put("key1", value);
        assertTrue(mv.size() == 1 && mv.get("key1") != null);

        rv.put("elem", value);
        assertTrue(rv.get("elem") != null);

        try {
            av.add(invalidValue);
            fail("Expect to catch IAE but not when add the value to array: " +
                 invalidValue.toJsonString(true));
        } catch (IllegalArgumentException ignored) {
        } //succeed

        try {
            mv.put("key1", invalidValue);
            fail("Expect to catch IAE but not when put the value to map: " +
                    invalidValue.toJsonString(true));
        } catch (IllegalArgumentException ignored) {
        } //succeed

        try {
            rv.put("elem", invalidValue);
            fail("Expect to catch IAE but not when put the value to record: " +
                 invalidValue.toJsonString(true));
        } catch (IllegalArgumentException ignored) {
        } //succeed
    }

    @Test
    public void testPutCastedValueToRecord() {
        final RecordDefImpl recDef =
                (RecordDefImpl)TableBuilder.createRecordBuilder("rec")
                                           .addLong("l")
                                           .addFloat("f")
                                           .addDouble("d")
                                           .addNumber("n")
                                           .addTimestamp("ts", 6)
                                           .addJson("j", null).build();
        final RecordValue rv = recDef.createRecord();

        final int i = 123;
        FieldValueImpl val = new IntegerDefImpl().createInteger(i);
        rv.put("l", val);
        assertTrue(rv.get("l").asLong().get() == i);
        rv.put("n", val);
        assertTrue(rv.get("n").asNumber().get().intValue() == i);
        rv.put("j", val);
        assertTrue(rv.get("j").asInteger().get() == i);

        final long l = 1232131321414L;
        val = new LongDefImpl().createLong(l);
        rv.put("n", val);
        assertTrue(rv.get("n").asNumber().get().longValue() == l);
        rv.put("j", val);
        assertTrue(rv.get("j").asLong().get() == l);

        final float f = 3.1415926f;
        val = new FloatDefImpl().createFloat(f);
        rv.put("d", val);
        assertTrue(rv.get("d").asDouble().get() == f);
        rv.put("n", val);
        assertTrue(rv.get("n").asNumber().get().floatValue() == f);
        rv.put("j", val);
        assertTrue(rv.get("j").asDouble().get() == f);

        final double d = 3.1415926535897926f;
        val = new DoubleDefImpl().createDouble(d);
        rv.put("n", val);
        assertTrue(rv.get("n").asNumber().get().doubleValue() == d);
        rv.put("j", val);
        assertTrue(rv.get("j").asDouble().get() == d);

        final BigDecimal bd = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE),
                                             Integer.MAX_VALUE);
        val = new NumberDefImpl().createNumber(bd);
        rv.put("j", val);
        assertTrue(rv.get("j").asNumber().get().compareTo(bd) == 0);

        final String s = "this is string value.";
        val = new StringDefImpl().createString(s);
        rv.put("j", val);
        assertTrue(rv.get("j").asString().get() == s);

        final Boolean b = true;
        val = new BooleanDefImpl().createBoolean(b);
        rv.put("j", val);
        assertTrue(rv.get("j").asBoolean().get() == b);

        /* Test casting TimestampValue to the given precision */
        final String tsStr = "2019-06-17T10:42:06.123456789";
        final Timestamp ts = TimestampUtils.parseString(tsStr);
        final int defPrec = recDef.getFieldDef("ts").asTimestamp()
                                .getPrecision();

        for (int prec = 0; prec <= defPrec; prec++) {
            val = new TimestampDefImpl(prec).createTimestamp(ts);
            rv.put("ts", val);
            Timestamp ts1 = rv.get("ts").asTimestamp().get();
            Timestamp exp = TimestampUtils.roundToPrecision(ts,
                                Math.min(prec, defPrec));
            assertEquals(exp, ts1);
        }
    }

    /*
     * Increase code coverage -- exercise default methods in FieldValueImpl
     * that throw.
     */
    @Test
    public void testBadCasts() {
        FieldValueImpl val =
            FieldDefImpl.Constants.integerDef.createInteger(1);

        try {
            val.asLong();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asString();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asDouble();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asFloat();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asBoolean();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asBinary();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asFixedBinary();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asMap();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asArray();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asRecord();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.longDef.createLong(0L).asInteger();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asTimestamp();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asNumber();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }

        /*
         * Now try to create types that will throw
         */
        try {
            val.setLong(0L);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setString("");
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setDouble(4.5);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setFloat(5.6F);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setBoolean(true);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.longDef.createLong(1L).setInt(1);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            FieldDefImpl.Constants.booleanDef.createBoolean(false).getLong();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getString();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getDouble();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getFloat();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getBoolean();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getEnumString();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setEnum("foo");
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getElement(1);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getElement("foo");
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getElement("foo");
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getBytes();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setTimestamp(new Timestamp(0));
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getTimestamp();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.setDecimal(new BigDecimal(0));
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getDecimal();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }

        FieldValueImpl fvlDouble = FieldDefImpl.Constants.doubleDef
                .createDouble(1);
        try {

            fvlDouble.getInt();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            fvlDouble.getMRCounterMap();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.incrementMRCounter(fvlDouble, 1);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.decrementMRCounter(fvlDouble, 1);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            int regid=1;
            val.putMRCounterEntry(regid, fvlDouble);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getNumberBytes();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.getTimestampBytes();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asRecordValueSerializer();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asMapValueSerializer();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.asArrayValueSerializer();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.convertMRCounterToPlainValue();
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
        try {
            val.castToOtherMRCounter(FieldDefImpl.Constants.longMRCounterDef);
            fail("Cast should have thrown");
        } catch (ClassCastException cce) {
        }
    }

    @Test
    public void testTimestamp() {
        Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            TimestampUtils.parseString("2016-07-21T20:47:15.987654321"),
            TimestampUtils.parseString("-2016-07-21T20:47:15.987654321"),
            TimestampUtils.createTimestamp(-1, 999999999),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE,
        };

        for (Timestamp ts : values) {
            for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
                Timestamp exp = TimestampUtils.roundToPrecision(ts, p);
                TimestampDefImpl def = new TimestampDefImpl(p);
                TimestampValueImpl val = def.createTimestamp(ts);
                assertTrue(val.get().equals(exp));
                assertTrue(val.isTimestamp());
                assertFalse(val.isNumeric());
                assertTrue(val.isAtomic());
                assertFalse(val.isComplex());
                assertTrue(val.getType() == FieldDef.Type.TIMESTAMP);

                assertTrue(val.clone().equals(val));
                assertFalse(val.equals(longNotEqual));

                /* Serialization and deserialization */
                assertTrue(def.createTimestamp(val.getBytes()).equals(val));

                /* Convert to sortable string */
                String keyFormat = val.formatForKey(def, 0);
                assertTrue(def.createTimestamp(keyFormat).equals(val));
                assertEquals(0, val.compareTo(def.createTimestamp(keyFormat)));

                /* Test TimestampValue.castAsLong() */
                assertTrue(val.castAsLong() == exp.getTime());
                /* Test TimestmapValue.castAsString() */
                assertTrue(val.castAsString().equals(val.toString()));

                /* Test TimestampValue.setTimestamp()/getTimestamp()*/
                Timestamp newVal = new Timestamp(new Date().getTime());
                newVal.setNanos(123456789);
                assertTrue(val.getTimestamp().equals(exp));
                val.setTimestamp(newVal);
                exp = TimestampUtils.roundToPrecision(newVal, p);
                assertTrue(val.getTimestamp().equals(exp));
            }
        }

        /* Test compareTo() */
        Timestamp[] sorted = new Timestamp[] {
            TimestampUtils.parseString("-6383-01-01T00:00:00"),
            TimestampUtils.parseString("1969-12-31T23:00:00"),
            TimestampUtils.parseString("1969-12-31T23:59:59"),
            TimestampUtils.parseString("1969-12-31T23:59:59.000000001"),
            TimestampUtils.parseString("1969-12-31T23:59:59.000999999"),
            TimestampUtils.parseString("1969-12-31T23:59:59.9"),
            TimestampUtils.parseString("1969-12-31T23:59:59.99"),
            TimestampUtils.parseString("1969-12-31T23:59:59.999"),
            TimestampUtils.parseString("1969-12-31T23:59:59.999999999"),
            TimestampUtils.parseString("1970-01-01T00:00:00"),
            TimestampUtils.parseString("1970-01-01T00:00:00.000000001"),
            TimestampUtils.parseString("1970-01-01T00:00:00.000000009"),
            TimestampUtils.parseString("1970-01-01T00:00:00.001"),
            TimestampUtils.parseString("1970-01-01T00:00:00.001000001"),
            TimestampUtils.parseString("1970-01-01T01:00:00"),
            TimestampUtils.parseString("9999-12-31T23:59:59.999999999"),
        };
        TimestampValueImpl last = null;
        for (Timestamp v : sorted) {
            TimestampValueImpl tsv = new TimestampDefImpl(9).createTimestamp(v);
            assertTrue(tsv.get().equals(v));
            if (last != null) {
                assertTrue(tsv.compareTo(last) > 0);
            }
            last = tsv;
        }

        /* Test min and max value */
        Timestamp tsMin = TimestampUtils.minusNanos
            (TimestampDefImpl.MIN_VALUE, 1);
        Timestamp tsMax = TimestampUtils.plusNanos
            (TimestampDefImpl.MAX_VALUE, 1);
        for (int p = 0; p < TimestampDefImpl.MAX_PRECISION; p++) {
            try {
                new TimestampDefImpl(p).createTimestamp(tsMin);
                fail("Expected to catch IllegalArgumentException but not");
            } catch (IllegalArgumentException iae) {
            }

            try {
                new TimestampDefImpl(p).createTimestamp(tsMax);
                fail("Expected to catch IllegalArgumentException but not");
            } catch (IllegalArgumentException iae) {
            }
        }

        /* Test TimestampValueImpl.nextValue() */
        values = new Timestamp[] {
            TimestampUtils.minusMillis(TimestampUtils.roundToPrecision
                                       (TimestampDefImpl.MAX_VALUE, 0), 3000),
            TimestampUtils.minusMillis(TimestampUtils.roundToPrecision
                                       (TimestampDefImpl.MAX_VALUE, 1), 300),
            TimestampUtils.minusMillis(TimestampUtils.roundToPrecision
                                       (TimestampDefImpl.MAX_VALUE, 2), 30),
            TimestampUtils.minusMillis(TimestampUtils.roundToPrecision
                                       (TimestampDefImpl.MAX_VALUE, 3), 3),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 4), 300000),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 5), 30000),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 6), 3000),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 7), 300),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 8), 30),
            TimestampUtils.minusNanos(TimestampUtils.roundToPrecision
                                      (TimestampDefImpl.MAX_VALUE, 9), 3),
        };
        for (int p = 0; p <= TimestampDefImpl.MAX_PRECISION; p++) {
            Timestamp ts = values[p];
            TimestampValueImpl tsv = new TimestampDefImpl(p).createTimestamp(ts);
            for (int i = 0; i < 3; i++) {
                tsv = (TimestampValueImpl)tsv.getNextValue();
                assertTrue(tsv != null);
            }
            assertTrue(tsv.getNextValue() == null);
        }

        /* Test TimestampValueImpl.getMinimumValue() */
        for (int p = 0; p <= TimestampDefImpl.MAX_PRECISION; p++) {
            TimestampValueImpl tsv =
                (TimestampValueImpl)new TimestampDefImpl(p)
                    .createTimestamp(new Timestamp(0))
                    .getMinimumValue();
            Timestamp exp =
                TimestampUtils.roundToPrecision(TimestampDefImpl.MIN_VALUE, p);
            assertTrue(tsv.get().equals(exp));
        }

        /* Bad casting */
        TimestampValueImpl val =
            new TimestampDefImpl(3).createTimestamp(new Timestamp(0));
        try {
            val.castAsInt();
            fail("Expected to catch ClassCastException: castAsInt()");
        } catch (ClassCastException cce) {
        }
        try {
            val.castAsFloat();
            fail("Expected to catch ClassCastException: castAsFloat()");
        } catch (ClassCastException cce) {
        }
        try {
            val.castAsDouble();
            fail("Expected to catch ClassCastException: castAsDouble()");
        } catch (ClassCastException cce) {
        }
        try {
            val.castAsNumber();
            fail("Expected to catch ClassCastException: castAsNumber()");
        } catch (ClassCastException cce) {
        }
        try {
            val.castAsDecimal();
            fail("Expected to catch ClassCastException: castAsDecimal()");
        } catch (ClassCastException cce) {
        }

        /* Test getXXX() methods */
        val = new TimestampDefImpl(9)
                .createTimestamp(TimestampDefImpl.MAX_VALUE);
        assertTrue(val.getYear() == 9999);
        assertTrue(val.getMonth() == 12);
        assertTrue(val.getDay() == 31);
        assertTrue(val.getHour() == 23);
        assertTrue(val.getMinute() == 59);
        assertTrue(val.getSecond() == 59);
        assertTrue(val.getNano() == 999999999);

        val = new TimestampDefImpl(9)
                .createTimestamp(TimestampDefImpl.MIN_VALUE);
        assertTrue(val.getYear() == -6383);
        assertTrue(val.getMonth() == 1);
        assertTrue(val.getDay() == 1);
        assertTrue(val.getHour() == 0);
        assertTrue(val.getMinute() == 0);
        assertTrue(val.getSecond() == 0);
        assertTrue(val.getNano() == 0);

        /* test equal method */
        Timestamp ts = TimestampUtils
                .parseString("2016-07-21T20:47:15.987654321");
        TimestampDef tsDef = new TimestampDefImpl(9);
        TimestampValueImpl tsval = new TimestampValueImpl(tsDef, ts);
        Timestamp tsother = TimestampUtils
                .parseString("2016-08-21T20:47:15.987654321");
        FieldValueImpl fvitsSame = new TimestampValueImpl(tsDef, ts);
        FieldValueImpl fvitsother = new TimestampValueImpl(tsDef, tsother);
        FieldValueImpl fviInt = FieldDefImpl.Constants.integerDef
                .createInteger(10);
        assertTrue(fvitsSame.equal(tsval));
        assertFalse(fvitsSame.equal(fvitsother));
        assertFalse(fvitsSame.equal(fviInt));

        BigDecimal bd = new BigDecimal("1234");
        FieldValueImpl fvinumber = new NumberValueImpl(bd);

        try {
            fvitsSame.compareTo(fvinumber);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
        TimestampValueImpl tsi = new TimestampValueImpl(tsDef, ts);
        Timestamp tsnull = null;
        try {
            tsi.setTimestamp(tsnull);
            fail("Timestamp shold not be null");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        TimestampDef tsDefPrec = new TimestampDefImpl(5);
        TimestampValueImpl tsiPrec = new TimestampValueImpl(tsDefPrec, ts);
        try {
            tsiPrec.castToPrecision(5);
            fail("Precision mismatch");
        } catch (AssertionError e) {
            /* success */
        }
        try {
            tsiPrec.roundDownToPrecision(7);
            fail("This timestamp's precision less than targe precision");
        } catch (AssertionError e) {
            /* success */
        }
        try {
            tsiPrec.roundUpToPrecision(7);
            fail("This timestamp's precision less than targe precision");
        } catch (AssertionError e) {
            /* success */
        }
        TimestampValueImpl tsiPrecOther = new TimestampValueImpl(tsDefPrec, ts);
        assertEquals(tsiPrec.hashCode(), tsiPrecOther.hashCode());
    }

    @Test
    public void testCompareNullValues() {
        final FieldValue[] sorted = new FieldValue[] {
             new IntegerDefImpl().createInteger(0),
             EmptyValueImpl.getInstance(),
             NullJsonValueImpl.getInstance(),
             NullValueImpl.getInstance()
        };

        for (int i = 0; i < sorted.length; i++) {
            FieldValue v1 = sorted[i];
            for (int j = 0; j < sorted.length; j++) {
                FieldValue v2 = sorted[j];
                int ret = (i == j) ? 0 : (i < j) ? -1 : 1;
                assertTrue(FieldValueImpl.compareFieldValues(v1, v2) == ret);
            }
        }

        /* test Unsupported operation */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        NullValueImpl nvi = NullValueImpl.getInstance();
        try {
            nvi.writeFastExternal(dos, SerialVersion.CURRENT);
            fail("Unsupported for Null value");
        } catch (IllegalStateException e) {
            /* success */
        }

        try {
            nvi.getType();
            fail("Unsupported for Null value");
        } catch (UnsupportedOperationException e) {
            /* success */
        }

        try {
            nvi.getDefinition();
            fail("Unsupported for Null value");
        } catch (UnsupportedOperationException e) {
            /* success */
        }

        /* test clone, compareTo ... methods */
        NullValueImpl nviOther = nvi.clone();
        assertEquals(nvi, nviOther);
        assertEquals(nvi.hashCode(), nviOther.hashCode());

        IntegerValueImpl iviVal = new IntegerValueImpl(0);
        try {
            nvi.compareTo(iviVal);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
    }

    @Test
    public void testEmpty() {
        EmptyValueImpl emptyval = EmptyValueImpl.getInstance();
        EmptyValueImpl emptyvalOther = emptyval.clone();
        assertEquals(emptyval, emptyvalOther);
        assertEquals(0, emptyval.compareTo(emptyvalOther));
        IntegerValueImpl iviVal = new IntegerValueImpl(10);
        try {
            emptyval.compareTo(iviVal);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }

        int iret = FieldValueImpl.compareKeyValues(emptyval, iviVal);
        assertEquals(1, iret);
    }

    @Test
    public void testNullJson() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        NullJsonValueImpl njvi = NullJsonValueImpl.getInstance();
        try {
            njvi.writeFastExternal(dos, SerialVersion.CURRENT);
            fail("Unsupported for Null value");
        } catch (IllegalStateException e) {
            /* success */
        }

        NullJsonValueImpl njviOther = NullJsonValueImpl.getInstance();
        assertEquals(0, njvi.compareTo(njviOther));

        IntegerValueImpl iviVal = new IntegerValueImpl(0);
        try {
            njvi.compareTo(iviVal);
            fail("Cast should have thrown");
        } catch (ClassCastException e) {
            /* success */
        }
    }
}
