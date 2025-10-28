/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

public class NumericTest extends ProxyTestBase {

    final String tableName = "numericTest";
    final String createTableDdl =
        "create table if not exists numericTest (" +
            "id integer, " +
            "i integer, " +
            "l long, " +
            "f float," +
            "d double, " +
            "n number, " +
            "primary key(id))";

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tableOperation(handle, createTableDdl,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
    }

    @Override
    public void tearDown() throws Exception {
        deleteTable(tableName);
        super.tearDown();
    }

    /**
     * Test put numeric values, values are parsed from JSON.
     */
    @Test
    public void testPutWithJson() {

        final int intVal = 123456;
        final long longVal = 987654321012345678L;
        final float fltVal = 1.234567f;
        final double dblVal = 9.8765432123456d;

        int[] ints = new int[] {
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0,
            intVal,
        };

        long [] longs = new long[] {
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            longVal,
        };

        float[] flts = new float[] {
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            0.0f,
            fltVal
        };

        double[] dbls = new double[] {
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            0.0d,
            dblVal
        };

        BigInteger bint =
            new BigInteger("98765432109876543210987654321098765432109876543210");
        BigDecimal[] decs = new BigDecimal[] {
            new BigDecimal(bint, -1024),
            new BigDecimal(bint, 1024),
            BigDecimal.ZERO,
            BigDecimal.valueOf(longVal)
        };

        for (int i = 0; i < ints.length; i++) {
            runPutGetTest(ints[i], longs[i], flts[i], dbls[i], decs[i], false);
            runPutGetTest(ints[i], longs[i], flts[i], dbls[i], decs[i], true);
        }
    }

    private void runPutGetTest(int i, long l, float f, double d, BigDecimal dec,
                               boolean numericAsNumber) {

        final MapValue key = new MapValue().put("id", 1);
        final MapValue row = new MapValue()
            .put("id", 1)
            .put("i", i)
            .put("l", l)
            .put("f", f)
            .put("d", d)
            .put("n", dec);

        final JsonOptions jsonOpts =
            new JsonOptions().setNumericAsNumber(numericAsNumber);
        String jsonStr = row.toJson(jsonOpts);

        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValueFromJson(jsonStr, jsonOpts);
        PutResult putRet = handle.put(putReq);
        assertTrue(putRet != null && putRet.getVersion() != null);

        GetRequest getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(key);
        GetResult getRet = handle.get(getReq);
        assertTrue(getRet != null && getRet.getValue() != null);

        MapValue value = getRet.getValue();
        assertType(value.get("i"), Type.INTEGER);
        assertTrue("Wrong value of \"i\"", value.getInt("i") == i);

        assertType(value.get("l"), Type.LONG);
        assertTrue("Wrong value of \"l\"", value.getLong("l") == l);

        assertType(value.get("f"), Type.DOUBLE);
        assertTrue("Wrong value of \"f\"", value.getDouble("f") == f);

        assertType(value.get("d"), Type.DOUBLE);
        assertTrue("Wrong value of \"d\"", value.getDouble("d") == d);

        assertType(value.get("n"), Type.NUMBER);
        assertTrue("Wrong value of \"n\"",
            value.getNumber("n").compareTo(dec) == 0);
    }

    /**
     * Put numeric values with other compatible type values.
     */
    @Test
    public void testCompatibleTypes() {

        final MapValue value = new MapValue().put("id", 1);
        Map<String, Object> expValues = new HashMap<String, Object>();

        /*
         * Target KV field type: Integer
         * Value types: LONG, DOUBLE, NUMBER
         */
        String fname = "i";

        /* Use LONG for Integer type */
        final long longToIntOK = Integer.MAX_VALUE;
        final long longToIntFail = (long)Integer.MAX_VALUE + 1;

        value.put(fname, longToIntOK);
        expValues.put(fname, (int)longToIntOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, longToIntFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use DOUBLE for Integer type */
        final double doubleToIntOK = 1.2345678E7d;
        final double doubleToIntFail = -1.1d;

        value.put(fname, doubleToIntOK);
        expValues.put(fname, (int)doubleToIntOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, doubleToIntFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use NUMBER for Integer type */
        final BigDecimal decimalToIntOK = BigDecimal.valueOf(Integer.MIN_VALUE);
        final BigDecimal decimalToIntFail = BigDecimal.valueOf(Long.MAX_VALUE);

        value.put(fname, decimalToIntOK);
        expValues.put(fname, decimalToIntOK.intValue());
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, decimalToIntFail);
        putAsOtherNuermicTypeTest(value, false);

        /*
         * Target KV field type: Long
         * Value types: INTEGER, DOUBLE, NUMBER
         */
        expValues.clear();
        value.remove(fname);
        fname = "l";

        /* Use INTEGER for Long type */
        final int intToLongOK = Integer.MAX_VALUE;

        value.put(fname, intToLongOK);
        expValues.put(fname, (long)intToLongOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        /* Use DOUBLE for Long type */
        final double doubleToLongOK = 1.234567890123E12d;
        final double doubleToLongFail = -1.1d;

        value.put(fname, doubleToLongOK);
        expValues.put(fname, (long)doubleToLongOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, doubleToLongFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use NUMBER for Long type */
        final BigDecimal decimalToLongOK = BigDecimal.valueOf(Long.MAX_VALUE);
        final BigDecimal decimalToLongFail = new BigDecimal("1234567890.1");

        value.put(fname, decimalToLongOK);
        expValues.put(fname, decimalToLongOK.longValue());
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, decimalToLongFail);
        putAsOtherNuermicTypeTest(value, false);

        /*
         * Target KV field type: Float
         * Value types: INTEGER, LONG, DOUBLE, NUMBER
         */
        expValues.clear();
        value.remove(fname);
        fname = "f";

        /* Use INTEGER for Float type */
        final int intToFloatOK = 16777216;
        final int intToFloatFail = 16777217;

        value.put(fname, intToFloatOK);
        expValues.put(fname, (double)intToFloatOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, intToFloatFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use LONG for Float type */
        final long longToFloatOK = Long.MAX_VALUE;
        final long longToFloatFail = Long.MAX_VALUE - 1;

        value.put(fname, longToFloatOK);
        expValues.put(fname, (double)longToFloatOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, longToFloatFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use DOUBLE for Float type */
        final double doubleToFloatOK = -Float.MAX_VALUE;
        final double doubleToFloatFail = Double.MAX_VALUE;

        value.put(fname, doubleToFloatOK);
        expValues.put(fname, doubleToFloatOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, doubleToFloatFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use NUMBER for Float type */
        float flt = 1.23456E2f;
        final BigDecimal decimalToFloatOK = BigDecimal.valueOf(flt);
        final BigDecimal decimalToFloatFail =
            BigDecimal.valueOf(Double.MAX_VALUE);

        value.put(fname, decimalToFloatOK);
        expValues.put(fname, (double)flt);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, decimalToFloatFail);
        putAsOtherNuermicTypeTest(value, false);

        /*
         * Target KV field type: Double
         * Value types: INTEGER, LONG, NUMBER
         */
        expValues.clear();
        value.remove(fname);
        fname = "d";

        /* Use INTEGER for Double type */
        final int intToDoubleOK = Integer.MAX_VALUE;

        value.put(fname, intToDoubleOK);
        expValues.put(fname, (double)intToDoubleOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        /* Use LONG for Double type */
        final long longToDoubleOK = Long.MAX_VALUE;
        final long longToDoubleFail = Long.MAX_VALUE - 1;

        value.put(fname, longToDoubleOK);
        expValues.put(fname, (double)longToDoubleOK);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, longToDoubleFail);
        putAsOtherNuermicTypeTest(value, false);

        /* Use NUMBER for Double type */
        double dbl = Double.MAX_VALUE;
        final BigDecimal decimalToDoubleOK = BigDecimal.valueOf(dbl);
        final BigDecimal decimalToDoubleFail =
            BigDecimal.valueOf(Long.MAX_VALUE - 1);

        value.put(fname, decimalToDoubleOK);
        expValues.put(fname, dbl);
        putAsOtherNuermicTypeTest(value, true, expValues);

        value.put(fname, decimalToDoubleFail);
        putAsOtherNuermicTypeTest(value, false);

        /*
         * Target KV field type: Number
         * Value types: INTEGER, LONG, DOUBLE
         */
        expValues.clear();
        value.remove(fname);
        fname = "n";

        /* Use INTEGER for Number type */
        final int intToNumberOK = Integer.MAX_VALUE;
        value.put(fname, intToNumberOK);
        expValues.put(fname, BigDecimal.valueOf(intToNumberOK));
        putAsOtherNuermicTypeTest(value, true, expValues);

        /* Use LONG for Number type */
        final long longToNumberOK = Long.MAX_VALUE;
        value.put(fname, longToNumberOK);
        expValues.put(fname, BigDecimal.valueOf(longToNumberOK));
        putAsOtherNuermicTypeTest(value, true, expValues);

        /* Use DOUBLE for Number type */
        final double doubleToNumberOK = Double.MAX_VALUE;
        value.put(fname, doubleToNumberOK);
        expValues.put(fname, BigDecimal.valueOf(doubleToNumberOK));
        putAsOtherNuermicTypeTest(value, true, expValues);
    }

    /*
     * Test Get/Delete op with a key parsed from JSON.
     */
    @Test
    public void testGetDeleteWithKeyFromJson() {
        String tabName = "tableWithNumberKey";
        String ddl = "create table if not exists " + tabName + "(" +
            "pk number, " +
            "str string, " +
            "primary key(pk))";

        tableOperation(handle, ddl,
            new TableLimits(1000, 1000, 50),
            TableResult.State.ACTIVE, 10000);

        JsonOptions optNumericAsNumber =
            new JsonOptions().setNumericAsNumber(true);

        BigDecimal bd = new BigDecimal("123456789012345678901234567890");
        /* this used to fail but with driver 5.4.11 it works */
        runGetDeleteTest(tabName, bd, null, true);
        runGetDeleteTest(tabName, bd, optNumericAsNumber, true);

        bd = BigDecimal.valueOf(Integer.MAX_VALUE);
        runGetDeleteTest(tabName, bd, null, true);
        runGetDeleteTest(tabName, bd, optNumericAsNumber, true);

        bd = BigDecimal.valueOf(Long.MAX_VALUE);
        runGetDeleteTest(tabName, bd, null, true);
        runGetDeleteTest(tabName, bd, optNumericAsNumber, true);

        bd = BigDecimal.valueOf(Float.MAX_VALUE);
        runGetDeleteTest(tabName, bd, null, true);
        runGetDeleteTest(tabName, bd, optNumericAsNumber, true);

        bd = BigDecimal.valueOf(Double.MAX_VALUE);
        runGetDeleteTest(tabName, bd, null, true);
        runGetDeleteTest(tabName, bd, optNumericAsNumber, true);
    }

    private void runGetDeleteTest(String tname,
                                  BigDecimal bd,
                                  JsonOptions jsonOpts,
                                  boolean expSucceed) {

        /* Put a row */
        MapValue mapVal = new MapValue()
            .put("pk", bd)
            .put("str", "strdata");
        PutRequest putReq = new PutRequest()
            .setTableName(tname)
            .setValue(mapVal);
        PutResult putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());

        mapVal = new MapValue().put("pk", bd);
        String pkJson = mapVal.toJson(jsonOpts);

        /*
         * Get the row, the key is parsed from JSON string with the
         * specified options.
         */
        GetRequest getReq = new GetRequest()
            .setKeyFromJson(pkJson, jsonOpts)
            .setTableName(tname);
        GetResult getRes = handle.get(getReq);
        if (expSucceed) {
            assertNotNull(getRes.getValue());
        } else {
            assertNull(getRes.getValue());
        }

        /*
         * Delete the row, the key is parsed from JSON string with the
         * specified options.
         */
        DeleteRequest delReq = new DeleteRequest()
            .setKeyFromJson(pkJson, jsonOpts)
            .setTableName(tname);
        DeleteResult delRes = handle.delete(delReq);
        assertTrue(expSucceed == delRes.getSuccess());
    }

    private void putAsOtherNuermicTypeTest(MapValue value,
                                           boolean shouldSucceed) {

        putAsOtherNuermicTypeTest(value, shouldSucceed, null);
    }

    private void putAsOtherNuermicTypeTest(MapValue value,
                                           boolean shouldSucceed,
                                           Map<String, Object> expValues) {

        runPutAsOtherNuermicTypeTest(value, false, shouldSucceed, expValues);
        runPutAsOtherNuermicTypeTest(value, true, shouldSucceed, expValues);
    }

    private void runPutAsOtherNuermicTypeTest(MapValue value,
                                              boolean numericAsNumber,
                                              boolean shouldSucceed,
                                              Map<String, Object> expValues){

        final JsonOptions jsonOpts =
            new JsonOptions().setNumericAsNumber(numericAsNumber);
        final String jsonStr = value.toJson(jsonOpts);
        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValueFromJson(jsonStr, jsonOpts);
        try {
            PutResult putRet = handle.put(putReq);
            if (shouldSucceed) {
                assertTrue("Put failed",
                           putRet != null && putRet.getVersion() != null);
                MapValue key = new MapValue().put("id", 1);
                GetRequest getReq = new GetRequest()
                    .setTableName(tableName)
                    .setKey(key);
                GetResult getRet = handle.get(getReq);
                assertTrue(getRet != null);
                if (expValues != null) {
                    checkValue(getRet.getValue(), expValues);
                }
            } else {
                fail("Put should have failed");
            }
        } catch (Exception ex) {
            if (shouldSucceed) {
                fail("Put failed: " + ex.getMessage());
            }
            //System.out.println(ex.getMessage());
        }
    }

    private void checkValue(MapValue value, Map<String, Object> expValues) {

        for (Entry<String, Object> e : expValues.entrySet()) {
            String fname = e.getKey();
            Object fval = e.getValue();

            if (fval instanceof Integer) {
                FieldValue fieldValue = value.get(fname);
                assertType(fieldValue, Type.INTEGER);
                assertTrue(fieldValue.getInt() == (int)fval);
            } else if (fval instanceof Long) {
                FieldValue fieldValue = value.get(fname);
                assertType(fieldValue, Type.LONG);
                assertTrue(fieldValue.getLong() == (long)fval);
            } else if (fval instanceof Double) {
                FieldValue fieldValue = value.get(fname);
                assertType(fieldValue, Type.DOUBLE);
                assertTrue(fieldValue.getDouble() == (double)fval);
            } else if (fval instanceof BigDecimal) {
                FieldValue fieldValue = value.get(fname);
                assertType(fieldValue, Type.NUMBER);
                assertTrue(
                    fieldValue.getNumber().compareTo((BigDecimal)fval) == 0);
            } else {
                fail("Unexpected value: " + fval);
            }
        }
    }

    private void assertType(FieldValue value, FieldValue.Type type) {
        assertTrue("Wrong type. expect " + type + " actual " + value.getType(),
            value.getType() == type);
    }
}
