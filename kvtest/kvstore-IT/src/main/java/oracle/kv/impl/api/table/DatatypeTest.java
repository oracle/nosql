/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;

import org.junit.Test;

/**
 * Datatype test
 */

public class DatatypeTest extends TableTestBase{

    private static enum GenType{
        Min, Max, Random;
    }
    private final String FieldId = "id";
    private final String FieldInt = "intField";
    private final String FieldLong = "longField";
    private final String FieldDouble = "doubleField";
    private final String FieldFloat = "floatField";
    private final String FieldString = "stringField";
    private final String FieldBoolean = "booleanField";
    private final String FieldBinary = "binaryField";
    private final String FieldFixedBinary = "fixedBinaryField";
    private final String FieldEnum = "enumField";
    private final String FieldTimestamp = "timestampField";
    private final String FieldNumber = "numberField";
    private final String FieldMap = "mapField";
    private final String FieldArray = "arrayField";
    private final String FieldRecord = "recordField";

    private final double DELTA = 1e-15;

    /* Basic put/get test for simple data types. */
    @Test
    public void testBasicPutGet()
        throws Exception {

        final String tblName = "DtTable";
        final String[] enumValues = {"TYPE1", "TYPE2", "TYPE3", "TYPE4"};
        final int fixedLen = 20;
        final TableBuilderBase tb =
            TableBuilder.createTableBuilder(
                tblName, "test data type", null)
            .addLong(FieldId)
            .addInteger(FieldInt)
            .addLong(FieldLong)
            .addDouble(FieldDouble)
            .addFloat(FieldFloat)
            .addString(FieldString)
            .addBoolean(FieldBoolean, null)
            .addBinary(FieldBinary)
            .addFixedBinary(FieldFixedBinary, fixedLen)
            .addEnum(FieldEnum, enumValues, null, null, null)
            .addTimestamp(FieldTimestamp, 3)
            .addNumber(FieldNumber)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        final TableImpl table = getTable(tblName);
        final int bytesLen = 20;
        final int stringLen = 20;
        final HashMap<Long, HashMap<String, Object>> keyValueMap =
            new HashMap<Long, HashMap<String, Object>>();

        /* Put MAX/MIN/Random values for each type. */
        for (GenType type: GenType.values()) {
            HashMap<String, Object> valuesMap =
                genValues(type, stringLen, bytesLen, fixedLen, enumValues);
            putTableValues(table, valuesMap);
            keyValueMap.put((Long)valuesMap.get(FieldId), valuesMap);
        }
        getByPKAndVerifyValues(table, keyValueMap);
    }

    /* Test on default value for all data types. */
    @Test
    public void testDefaultValue()
        throws Exception {

        final String tblName = "DtTable";
        final int intDef = 100;
        final long longDef = -1000L;
        final double doubleDef = 1.10123134E-110;
        final float floatDef = (float)1.10123134E-10;
        final String stringDef = "this is default value test";
        final boolean booleanDef = true;
        final String[] enumValues =
            new String[]{"TYPE1", "TYPE2", "TYPE3", "TYPE4"};
        final String enumDef = enumValues[enumValues.length - 1];
        final Timestamp timestampDef = new Timestamp(0);
        final BigDecimal decimal = BigDecimal.ZERO;
        final int fixedLen = 256;

        RecordBuilder rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addLong(FieldLong);
        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addString();
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addInteger();

        /* Case1: defaultValue = xxxDef */
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt, null, null, intDef)
            .addLong(FieldLong, null, null, longDef)
            .addDouble(FieldDouble, null, null, doubleDef)
            .addFloat(FieldFloat, null, null, floatDef)
            .addString(FieldString, null, null, stringDef)
            .addBoolean(FieldBoolean, null, null, booleanDef)
            .addEnum(FieldEnum, enumValues, null, null, enumDef)
            .addBinary(FieldBinary, null)
            .addFixedBinary(FieldFixedBinary, fixedLen)
            .addTimestamp(FieldTimestamp, 9, null, null, timestampDef)
            .addNumber(FieldNumber, null, null, decimal)
            .addField(FieldMap, mb.build())
            .addField(FieldArray, ab.build())
            .addField(FieldRecord, rb.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert a new row without putting value to not-primary-key fields. */
        TableImpl table = getTable(tblName);
        Row row = table.createRow();
        row.put(FieldId, 1L);
        try {
            tableImpl.put(row, null, null);
        } catch (Exception e) {
            fail("Expected to be OK but get exception: " + e.getMessage());
        }

        /* Retrieve the row and verify the values. */
        PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1L);
        Row retRow = tableImpl.get(key, null);
        assertEquals(intDef, retRow.get(FieldInt).asInteger().get());
        assertEquals(longDef, retRow.get(FieldLong).asLong().get());
        assertEquals(
            doubleDef, retRow.get(FieldDouble).asDouble().get(), DELTA);
        assertEquals(
            floatDef, retRow.get(FieldFloat).asFloat().get(), (float)DELTA);
        assertTrue(stringDef.equals(retRow.get(FieldString).asString().get()));
        assertEquals(booleanDef, retRow.get(FieldBoolean).asBoolean().get());
        assertTrue(enumDef.equals(retRow.get(FieldEnum).asEnum().get()));
        assertTrue(retRow.get(FieldBinary).isNull());
        assertTrue(retRow.get(FieldFixedBinary).isNull());
        assertTrue(timestampDef.equals
                   (retRow.get(FieldTimestamp).asTimestamp().get()));
        assertTrue(decimal.compareTo
                       (retRow.get(FieldNumber).asNumber().get()) == 0);
        assertTrue(retRow.get(FieldMap).isNull());
        assertTrue(retRow.get(FieldArray).isNull());
        assertTrue(retRow.get(FieldRecord).isNull());

        /*
         * case 1.1: insert and then read a row with a field whose
         * value is explicitly set to NULL.
         */
        row = table.createRow();
        row.put(FieldId, 1L);
        row.putNull(FieldInt);
        try {
            tableImpl.put(row, null, null);
        } catch (Exception e) {
            fail("Expected to be OK but get exception: " + e.getMessage());
        }

        key = table.createPrimaryKey();
        key.put(FieldId, 1L);
        retRow = tableImpl.get(key, null);
        assert(retRow.get(FieldInt).isNull());

        removeTable(null, tblName, true);

        /* Case2: nullable = false, defaultValue = xxxDef */
        rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addLong(FieldLong);

        mb = TableBuilder.createMapBuilder();
        mb.addString();

        ab = TableBuilder.createArrayBuilder();
        ab.addInteger();

        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt, null, false, intDef)
            .addLong(FieldLong, null, false, longDef)
            .addDouble(FieldDouble, null, false, doubleDef)
            .addFloat(FieldFloat, null, false, floatDef)
            .addString(FieldString, null, false, stringDef)
            .addBoolean(FieldBoolean, null, false, booleanDef)
            .addEnum(FieldEnum, enumValues, null, false, enumDef)
            .addTimestamp(FieldTimestamp, 3, null, false, timestampDef)
            .addNumber(FieldNumber, null, false, decimal)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert a new row without putting value to not-primary-key fields. */
        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        tableImpl.put(row, null, null);

        /* Retrieve the row and verify the values. */
        key = table.createPrimaryKey();
        key.put(FieldId, 1L);
        retRow = tableImpl.get(key, null);
        assertEquals(intDef, retRow.get(FieldInt).asInteger().get());
        assertEquals(longDef, retRow.get(FieldLong).asLong().get());
        assertEquals(
            doubleDef, retRow.get(FieldDouble).asDouble().get(), DELTA);
        assertEquals(
            floatDef, retRow.get(FieldFloat).asFloat().get(), (float)DELTA);
        assertTrue(stringDef.equals(retRow.get(FieldString).asString().get()));
        assertEquals(booleanDef, retRow.get(FieldBoolean).asBoolean().get());
        assertTrue(enumDef.equals(retRow.get(FieldEnum).asEnum().get()));
        assertTrue(timestampDef.equals(retRow.get(FieldTimestamp)
                                       .asTimestamp().get()));
        assertTrue(decimal.compareTo
                   (retRow.get(FieldNumber).asNumber().get()) == 0);

        /* Case3: nullable = true, defaultValue = null */
        removeTable(null, tblName, true);
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt, null, true, null)
            .addLong(FieldLong, null, true, null)
            .addDouble(FieldDouble, null, true, null)
            .addFloat(FieldFloat, null, true, null)
            .addString(FieldString, null, true, null)
            .addBoolean(FieldBoolean, null, true, null)
            .addEnum(FieldEnum, enumValues, null, true, null)
            .addBinary(FieldBinary, null)
            .addFixedBinary(FieldFixedBinary, fixedLen)
            .addTimestamp(FieldTimestamp, 3)
            .addNumber(FieldNumber)
            .addField(FieldArray, ab.build())
            .addField(FieldMap, mb.build())
            .addField(FieldRecord, rb.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert a new row without putting value to fields
         * except PK field. */
        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 3L);
        tableImpl.put(row, null, null);

        /* Retrieve the row and verify the values. */
        key = table.createPrimaryKey();
        key.put(FieldId, 3L);
        retRow = tableImpl.get(key, null);
        tableImpl.put(row, null, null);
        assertTrue(retRow.get(FieldInt).isNull());
        assertTrue(retRow.get(FieldLong).isNull());
        assertTrue(retRow.get(FieldDouble).isNull());
        assertTrue(retRow.get(FieldFloat).isNull());
        assertTrue(retRow.get(FieldString).isNull());
        assertTrue(retRow.get(FieldBoolean).isNull());
        assertTrue(retRow.get(FieldEnum).isNull());
        assertTrue(retRow.get(FieldBinary).isNull());
        assertTrue(retRow.get(FieldFixedBinary).isNull());
        assertTrue(retRow.get(FieldTimestamp).isNull());
        assertTrue(retRow.get(FieldNumber).isNull());
        assertTrue(retRow.get(FieldMap).isNull());
        assertTrue(retRow.get(FieldArray).isNull());
        assertTrue(retRow.get(FieldRecord).isNull());
    }

    /* Test on Nullable property. */
    @Test
    public void testNullableProp()
        throws Exception {

        final String tblName = "DtTable";
        final String[] enumValues =
            new String[]{"TYPE1", "TYPE2", "TYPE3", "TYPE4"};

        /* Set all the fields nullable property to true. */
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addInteger();

        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addInteger();

        RecordBuilder rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addInteger(FieldInt);

        TableBuilderBase tb =
            TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt, null, true, null)
            .addLong(FieldLong, null, true, null)
            .addDouble(FieldDouble, null, true, null)
            .addFloat(FieldFloat, null, true, null)
            .addString(FieldString, null, true, null)
            .addBoolean(FieldBoolean, null, true, null)
            .addBinary(FieldBinary, null)
            .addFixedBinary(FieldFixedBinary, 256)
            .addEnum(FieldEnum, enumValues, null, true, null)
            .addTimestamp(FieldTimestamp, 3, null, true, null)
            .addNumber(FieldNumber, null, true, null)
            .addField(FieldArray, ab.build())
            .addField(FieldMap, mb.build())
            .addField(FieldRecord, rb.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        TableImpl table = getTable(tblName);
        Row row = table.createRow();
        row.put(FieldId, 1L);
        row.putNull(FieldInt);
        row.putNull(FieldLong);
        row.putNull(FieldDouble);
        row.putNull(FieldFloat);
        row.putNull(FieldString);
        row.putNull(FieldBoolean);
        row.putNull(FieldBinary);
        row.putNull(FieldFixedBinary);
        row.putNull(FieldEnum);
        row.putNull(FieldTimestamp);
        row.putNull(FieldNumber);
        row.putNull(FieldArray);
        row.putNull(FieldMap);
        row.putNull(FieldRecord);
        try {
            tableImpl.put(row, null, null);
        } catch (IllegalCommandException ice) {
            ice.printStackTrace();
            fail("Expected to be OK but get exception: " + ice.getMessage());
        }

        /* Get the row and verify values for each type */
        PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1L);
        Row retRow = tableImpl.get(key, null);
        assertTrue(retRow.get(FieldInt).isNull());
        assertTrue(retRow.get(FieldLong).isNull());
        assertTrue(retRow.get(FieldDouble).isNull());
        assertTrue(retRow.get(FieldFloat).isNull());
        assertTrue(retRow.get(FieldString).isNull());
        assertTrue(retRow.get(FieldBoolean).isNull());
        assertTrue(retRow.get(FieldBinary).isNull());
        assertTrue(retRow.get(FieldFixedBinary).isNull());
        assertTrue(retRow.get(FieldEnum).isNull());
        assertTrue(retRow.get(FieldTimestamp).isNull());
        assertTrue(retRow.get(FieldNumber).isNull());
        assertTrue(retRow.get(FieldArray).isNull());
        assertTrue(retRow.get(FieldMap).isNull());
        assertTrue(retRow.get(FieldRecord).isNull());
        removeTable(null, tblName, true);

        /* Integer, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt, null, false, 1)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldInt);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Long, nullable = false  */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addLong(FieldLong, null, false, 1L)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldLong);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Double, nullable = false  */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addDouble(FieldDouble, null, false, 1.0)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldDouble);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Float, nullable = false  */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addFloat(FieldFloat, null, false, 1.0F)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldFloat);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* String, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addString(FieldString, null, false, "1")
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldString);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Boolean, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addBoolean(FieldBoolean, null, false, true)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldBoolean);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Enum, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addEnum(FieldEnum, enumValues, null, false, enumValues[0])
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldEnum);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Timestamp, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addTimestamp(FieldTimestamp, 9, null, false, new Timestamp(0))
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldTimestamp);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
        removeTable(null, tblName, true);

        /* Number, nullable = false */
        tb = TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addNumber(FieldNumber, null, false, BigDecimal.ZERO)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        table = getTable(tblName);
        row = table.createRow();
        row.put(FieldId, 1L);
        try {
            row.putNull(FieldNumber);
            fail("Expected to get exception but OK.");
        } catch (IllegalArgumentException ice) {
        }
    }

    /*  Index on supported data types. */
    @Test
    public void testIndexing()
        throws Exception {

        final String tblName = "DtTable";
        final String[] enumValues =
            new String[]{"TYPE1", "TYPE2", "TYPE3", "TYPE4"};
        final String idxFieldInt = "indexFldInt";
        final String idxFieldLong = "indexFldLong";
        final String idxFieldDouble = "indexFldDouble";
        final String idxFieldFloat = "indexFldFloat";
        final String idxFieldString = "indexFldString";
        final String idxFieldEnum = "indexFldEnum";
        final String idxFieldTimestamp = "indexFldTimestamp";
        final String idxFieldNumber = "indexFldNumber";
        final int nRec = 10;
        final int stringLen = 20;

        /* create table */
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(tblName, null, null)
            .addLong(FieldId)
            .addInteger(FieldInt)
            .addLong(FieldLong)
            .addDouble(FieldDouble)
            .addFloat(FieldFloat)
            .addString(FieldString)
            .addBoolean(FieldBoolean, null)
            .addEnum(FieldEnum, enumValues, null, null, null)
            .addTimestamp(FieldTimestamp, 3)
            .addNumber(FieldNumber)
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* create indexes */
        TableImpl table = getTable(tblName);
        addIndex(table, idxFieldInt, new String[]{FieldInt}, true);
        addIndex(table, idxFieldLong, new String[]{FieldLong}, true);
        addIndex(table, idxFieldDouble, new String[]{FieldDouble}, true);
        addIndex(table, idxFieldFloat, new String[]{FieldFloat}, true);
        addIndex(table, idxFieldString, new String[]{FieldString}, true);
        addIndex(table, idxFieldEnum, new String[]{FieldEnum}, true);
        addIndex(table, idxFieldTimestamp, new String[]{FieldTimestamp}, true);
        addIndex(table, idxFieldNumber, new String[]{FieldNumber}, true);
        table = getTable(tblName);

        /* insert nRec rows */
        HashMap<Long, HashMap<String, Object>> kvMap =
            new HashMap<Long, HashMap<String, Object>> ();
        for (int i = 0; i < nRec; i++) {
            HashMap<String, Object> valuesMap =
                genValues(GenType.Random, stringLen, null, null, enumValues);
            putTableValues(table, valuesMap);
            kvMap.put((Long)valuesMap.get(FieldId), valuesMap);
        }

        /* Retrieve row by primary key and verify values */
        getByPKAndVerifyValues(table, kvMap);

        /* Retrieve row by index key and verify values */
        Map<String, Index> indexes = table.getIndexes();
        for (Map.Entry<Long, HashMap<String, Object>> kvEntry:
            kvMap.entrySet()) {
            HashMap<String, Object> valuesMap = kvEntry.getValue();
            for (Map.Entry<String, Index> idxEntry: indexes.entrySet()) {
                IndexImpl index = (IndexImpl)idxEntry.getValue();
                IndexKey key = index.createIndexKey();
                for (String field: index.getFields()) {
                    FieldDef def = table.getField(field);
                    Object value = valuesMap.get(field);
                    switch(def.getType()) {
                    case INTEGER:
                        key.put(field, ((Integer)value).intValue());
                        break;
                    case LONG:
                        key.put(field, ((Long)value).longValue());
                        break;
                    case DOUBLE:
                        key.put(field, ((Double)value).doubleValue());
                        break;
                    case FLOAT:
                        key.put(field, ((Float)value).floatValue());
                        break;
                    case STRING:
                        key.put(field, ((String)value));
                        break;
                    case ENUM:
                        key.putEnum(field, ((String)value));
                        break;
                    case TIMESTAMP:
                        key.put(field, ((Timestamp)value));
                        break;
                    case NUMBER:
                        key.putNumber(field, ((BigDecimal)value));
                        break;
                    default:
                        fail("unsupported field type for index.");
                    }
                }
                getRowByIndexKeyAndVerify(key, FieldId, kvMap);
            }
        }
    }

    private void getRowByIndexKeyAndVerify(IndexKey key, String pKeyFieldName,
        HashMap<Long, HashMap<String, Object>> kvMap) {

        TableIterator<Row> itr = tableImpl.tableIterator(key, null, null);
        boolean isMatched = false;
        while(itr.hasNext()) {
            Row row = itr.next();
            /*Verify index key values.*/
            for (String fname: key.getIndex().getFields()) {
                assertTrue(key.get(fname).compareTo(row.get(fname)) == 0);
            }

            PrimaryKey pKey = row.createPrimaryKey();
            Long pKeyValue = pKey.get(pKeyFieldName).asLong().get();
            HashMap<String, Object> valuesMap = kvMap.get(pKeyValue);
            if (!verifyRowValues(
                (TableImpl)key.getIndex().getTable(), row, valuesMap)) {
                isMatched = false;
            }
            isMatched = true;
        }
        itr.close();
        if (!isMatched) {
            fail("Failed to verify the row getting by index key:" +
                key.toJsonString(false));
        }
    }

    private void putTableValues(TableImpl table, Map<String, Object> map) {

        Row row = table.createRow();

        for (FieldMapEntry fme :  table.getFieldMap().getFieldProperties()) {

            String fname = fme.getFieldName();
            try {
                if (map.containsKey(fname)) {
                    Object value = map.get(fname);
                    if (value instanceof Integer) {
                        row.put(fname, (Integer)value);
                    } else if (value instanceof Long) {
                        row.put(fname, (Long)value);
                    } else if (value instanceof Double) {
                        row.put(fname, (Double)value);
                    } else if (value instanceof Float) {
                        row.put(fname, (Float)value);
                    } else if (value instanceof byte[]) {
                        if (fme.getFieldDef().isBinary()) {
                            row.put(fname, (byte[])value);
                        } else {
                            row.putFixed(fname, (byte[])value);
                        }
                    } else if (value instanceof Boolean) {
                        row.put(fname, (Boolean)value);
                    } else if (value instanceof String) {
                        if (fme.getFieldDef().isEnum()) {
                            row.putEnum(fname, (String)value);
                        } else {
                            row.put(fname, (String)value);
                        }
                    } else if (value instanceof Timestamp) {
                        row.put(fname, (Timestamp)value);
                    } else if (value instanceof BigDecimal) {
                        row.putNumber(fname, (BigDecimal)value);
                    } else {
                        fail("Unsupported value type.");
                    }
                } else {
                    row.putNull(fname);
                }
            } catch (Exception e) {
                fail("Expected to be OK but get exception: " +
                     fname + ", " + e);
            }
        }
        try {
            tableImpl.put(row, null, null);
        } catch (Exception e) {
            fail("Put should be OK but get expception: " + e);
        }
    }

    private void getByPKAndVerifyValues(TableImpl table,
                               HashMap<Long, HashMap<String, Object>> kvMap) {

        PrimaryKey key = table.createPrimaryKey();
        for (Map.Entry<Long, HashMap<String, Object>> entry: kvMap.entrySet()) {
            long id = entry.getKey();
            Map<String, Object> valuesMap = entry.getValue();
            key.put(FieldId, id);
            Row row = tableImpl.get(key, null);
            verifyRowValues(table, row, valuesMap);
        }
    }

    private boolean verifyRowValues(TableImpl table, Row row,
                                    Map<String, Object> valuesMap) {

        for (FieldMapEntry fme :  table.getFieldMap().getFieldProperties()) {

            String fname = fme.getFieldName();
            FieldDef fieldDef = fme.getFieldDef();

            if (valuesMap.containsKey(fname)) {
                Object value = valuesMap.get(fname);
                if (value instanceof Integer) {
                    assertTrue(fieldDef.isInteger());
                    assertEquals(((Integer)valuesMap.get(fname)).intValue(),
                                 row.get(fname).asInteger().get());
                } else if (value instanceof Long) {
                    assertTrue(fieldDef.isLong());
                    assertEquals(((Long)valuesMap.get(fname)).longValue(),
                                 row.get(fname).asLong().get());
                } else if (value instanceof Double) {
                    assertTrue(fieldDef.isDouble());
                    assertEquals(
                        ((Double)valuesMap.get(fname)).doubleValue(),
                        row.get(fname).asDouble().get(), DELTA);
                } else if (value instanceof Float) {
                    assertTrue(fieldDef.isFloat());
                    assertEquals(
                        ((Float)valuesMap.get(fname)).floatValue(),
                        row.get(fname).asFloat().get(), (float)DELTA);
                } else if (value instanceof byte[]) {
                    if (fieldDef.isBinary()) {
                        assertTrue(
                            Arrays.equals((byte[])valuesMap.get(fname),
                                          row.get(fname).asBinary().get()));
                    } else {
                        assertTrue(
                            Arrays.equals((byte[])valuesMap.get(fname),
                                         row.get(fname).asFixedBinary().get()));
                    }
                } else if (value instanceof Boolean) {
                    assertTrue(fieldDef.isBoolean());
                    assertEquals(
                        ((Boolean)valuesMap.get(fname)).booleanValue(),
                        row.get(fname).asBoolean().get());
                } else if (value instanceof String) {
                    if (fieldDef.isEnum()) {
                        assertTrue(((String)valuesMap.get(fname))
                                   .equals(row.get(fname).asEnum().get()));
                    } else {
                        assertTrue(((String)valuesMap.get(fname))
                                   .equals(row.get(fname).asString().get()));
                    }
                } else if (value instanceof Timestamp) {
                    assertTrue(fieldDef.isTimestamp());
                    Timestamp exp =
                        roundTimestamp((Timestamp)valuesMap.get(fname),
                                       fieldDef.asTimestamp().getPrecision());
                    assertTrue(exp.equals(row.get(fname).asTimestamp().get()));
                } else if (value instanceof BigDecimal) {
                    assertTrue(fieldDef.isNumber());
                    BigDecimal exp = ((BigDecimal)valuesMap.get(fname));
                    BigDecimal actual = row.get(fname).asNumber().get();
                    assertTrue(actual.compareTo(exp) == 0);
                } else {
                    fail("Unsupported value type: " + value.getClass());
                }
            } else {
                fail("Cannot find the value of field in kvMap : " + fname);
            }
        }
        return true;
    }

    private ArrayBuilder createArrayBuilderEnumField(String name,
                                                     String[] values) {
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        return (ArrayBuilder)ab.addEnum(name, values, null);
    }

    private MapBuilder createMapBuilderEnumField(String name, String[] values) {
        MapBuilder ab = TableBuilder.createMapBuilder();
        return (MapBuilder)ab.addEnum(name, values, null);
    }

    private ArrayBuilder createArrayBuilderFixedField(String name, int size) {
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        return (ArrayBuilder)ab.addFixedBinary(name, size);
    }

    private MapBuilder createMapBuilderFixedField(String name, int size) {
        MapBuilder ab = TableBuilder.createMapBuilder();
        return (MapBuilder)ab.addFixedBinary(name, size);
    }


    /* Basic put/get on field: Array{simple types} */
    @Test
    public void testArrayFieldPutGet()
        throws Exception {

        final String tblname = "DtTable";
        final String ArrayFieldInt = "arrayFieldInt";
        final String ArrayFieldLong = "arrayFieldLong";
        final String ArrayFieldDouble = "arrayFieldDouble";
        final String ArrayFieldFloat = "arrayFieldFloat";
        final String ArrayFieldString = "arrayFieldString";
        final String ArrayFieldBoolean = "arrayFieldBoolean";
        final String ArrayFieldBinary = "arrayFieldBinary";
        final String ArrayFieldFixedBinary = "arrayFieldFixedBinary";
        final String ArrayFieldEnum = "arrayFieldEnum";
        final String ArrayFieldTimestamp = "arrayFieldTimestamp";
        final String ArrayFieldNumber = "arrayFieldNumber";
        final String[] enumValues =
            new String[]{"TYPE1", "TYPE2", "TYPE3", "TYPE4"};
        final int fixedLen = 256;

        /* Array{Integer} */
        ArrayBuilder arrayBuilderInt =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addInteger();
        /* Array{Long} */
        ArrayBuilder arrayBuilderLong =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addLong();
        /* Array{String} */
        ArrayBuilder arrayBuilderString =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addString();
        /* Array{Double} */
        ArrayBuilder arrayBuilderDouble =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addDouble();
        /* Array{Float} */
        ArrayBuilder arrayBuilderFloat =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addFloat();
        /* Array{Boolean} */
        ArrayBuilder arrayBuilderBoolean =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addBoolean();
        /* Array{Binary} */
        ArrayBuilder arrayBuilderBinary =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
            .addBinary(null);
        /* Array{fixed Binary} */
        ArrayBuilder arrayBuilderFixedBinary =
            createArrayBuilderFixedField(FieldFixedBinary, fixedLen);
        /* Array{Enum} */
        ArrayBuilder arrayBuilderEnum =
            createArrayBuilderEnumField(FieldEnum, enumValues);
        /* Array{Timestamp} */
        ArrayBuilder arrayBuilderTimestamp =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addTimestamp(9);
        /* Array{Number} */
        ArrayBuilder arrayBuilderDecimal =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
                .addNumber();
        /* Create table includes all above array{simple type} field. */
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(
                tblname, "test array type", null)
                    .addInteger(FieldId)
                    .addField(ArrayFieldInt, arrayBuilderInt.build())
                    .addField(ArrayFieldLong, arrayBuilderLong.build())
                    .addField(ArrayFieldDouble, arrayBuilderDouble.build())
                    .addField(ArrayFieldFloat, arrayBuilderFloat.build())
                    .addField(ArrayFieldString, arrayBuilderString.build())
                    .addField(ArrayFieldBoolean, arrayBuilderBoolean.build())
                    .addField(ArrayFieldBinary, arrayBuilderBinary.build())
                    .addField(ArrayFieldFixedBinary,
                              arrayBuilderFixedBinary.build())
                    .addField(ArrayFieldEnum, arrayBuilderEnum.build())
                    .addField(ArrayFieldTimestamp,
                              arrayBuilderTimestamp.build())
                    .addField(ArrayFieldNumber, arrayBuilderDecimal.build())
                    .primaryKey(FieldId)
                    .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        final TableImpl table = getTable(tblname);
        final RowImpl row = table.createRow();
        row.put(FieldId, 1);
        ArrayValue arrayValueInt = row.putArray(ArrayFieldInt);
        ArrayValue arrayValueLong = row.putArray(ArrayFieldLong);
        ArrayValue arrayValueDouble = row.putArray(ArrayFieldDouble);
        ArrayValue arrayValueFloat = row.putArray(ArrayFieldFloat);
        ArrayValue arrayValueString = row.putArray(ArrayFieldString);
        ArrayValue arrayValueBoolean = row.putArray(ArrayFieldBoolean);
        ArrayValue arrayValueBinary = row.putArray(ArrayFieldBinary);
        ArrayValue arrayValueFixedBinary = row.putArray(ArrayFieldFixedBinary);
        ArrayValue arrayValueEnum = row.putArray(ArrayFieldEnum);
        ArrayValue arrayValueTimestamp = row.putArray(ArrayFieldTimestamp);
        ArrayValue arrayValueDecimal = row.putArray(ArrayFieldNumber);

        /* Insert a row, put nItems values to each array field.*/
        final int nItem = 5;
        final int bytesLen = 128;
        final int strLen = 20;
        ArrayList<HashMap<String, Object>> valueMapList =
            new ArrayList<HashMap<String, Object>>();
        for (int i = 0; i < nItem; i++) {
            HashMap<String, Object> valuesMap =
                genValues(GenType.Random, strLen, bytesLen,
                          fixedLen, enumValues);
            if (i == 0) {
                arrayValueAddValueIncorrectType(arrayValueInt, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueLong, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueDouble, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueFloat, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueString, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueBoolean, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueBinary, valuesMap);
                arrayValueAddValueIncorrectType(
                    arrayValueFixedBinary, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueEnum, valuesMap);
                arrayValueAddValueIncorrectType(arrayValueTimestamp,
                                                 valuesMap);
                arrayValueAddValueIncorrectType(arrayValueDecimal, valuesMap);
            } else {
                arrayValueInt.add((Integer)valuesMap.get(FieldInt));
                arrayValueLong.add((Long)valuesMap.get(FieldLong));
                arrayValueDouble.add((Double)valuesMap.get(FieldDouble));
                arrayValueFloat.add((Float)valuesMap.get(FieldFloat));
                arrayValueString.add((String)valuesMap.get(FieldString));
                arrayValueBoolean.add((Boolean)valuesMap.get(FieldBoolean));
                arrayValueBinary.add((byte[])valuesMap.get(FieldBinary));
                arrayValueFixedBinary.addFixed(
                    (byte[])valuesMap.get(FieldFixedBinary));
                arrayValueEnum.addEnum((String)valuesMap.get(FieldEnum));
                arrayValueTimestamp.add
                    ((Timestamp)valuesMap.get(FieldTimestamp));
                arrayValueDecimal.addNumber((BigDecimal)valuesMap.get(FieldNumber));
            }
            valueMapList.add(i, valuesMap);
        }
        try {
            tableImpl.put(row, null, null);
        } catch (Exception e) {
            fail("Put should be OK but get expception: " + e);
        }

        /* Retrieve row and verify the row values */
        PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1);
        Row retRow = tableImpl.get(key, null);
        arrayValueInt = (ArrayValue)retRow.get(ArrayFieldInt);
        arrayValueLong = (ArrayValue)retRow.get(ArrayFieldLong);
        arrayValueDouble = (ArrayValue)retRow.get(ArrayFieldDouble);
        arrayValueFloat = (ArrayValue)retRow.get(ArrayFieldFloat);
        arrayValueString = (ArrayValue)retRow.get(ArrayFieldString);
        arrayValueBoolean = (ArrayValue)retRow.get(ArrayFieldBoolean);
        arrayValueBinary = (ArrayValue)retRow.get(ArrayFieldBinary);
        arrayValueFixedBinary = (ArrayValue)retRow.get(ArrayFieldFixedBinary);
        arrayValueEnum = (ArrayValue)retRow.get(ArrayFieldEnum);
        arrayValueTimestamp = (ArrayValue)retRow.get(ArrayFieldTimestamp);
        arrayValueDecimal = (ArrayValue)retRow.get(ArrayFieldNumber);

        assertEquals(valueMapList.size(), arrayValueInt.size());
        assertEquals(valueMapList.size(), arrayValueLong.size());
        assertEquals(valueMapList.size(), arrayValueDouble.size());
        assertEquals(valueMapList.size(), arrayValueFloat.size());
        assertEquals(valueMapList.size(), arrayValueString.size());
        assertEquals(valueMapList.size(), arrayValueBoolean.size());
        assertEquals(valueMapList.size(), arrayValueBinary.size());
        assertEquals(valueMapList.size(), arrayValueFixedBinary.size());
        assertEquals(valueMapList.size(), arrayValueEnum.size());
        assertEquals(valueMapList.size(), arrayValueTimestamp.size());
        assertEquals(valueMapList.size(), arrayValueDecimal.size());

        for (int i = 0; i < valueMapList.size(); i++) {
            HashMap<String, Object> valuesMap = valueMapList.get(i);
            assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                         arrayValueInt.get(i).asInteger().get());
            assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                         arrayValueLong.get(i).asLong().get());
            assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                         arrayValueDouble.get(i).asDouble().get(), DELTA);
            assertEquals(((Float)valuesMap.get(FieldFloat)).floatValue(),
                         arrayValueFloat.get(i).asFloat().get(), (float)DELTA);
            assertTrue(((String)valuesMap.get(FieldString))
                        .equals(arrayValueString.get(i).asString().get()));
            assertEquals(((Boolean)valuesMap.get(FieldBoolean)).booleanValue(),
                         arrayValueBoolean.get(i).asBoolean().get());
            assertTrue(
                Arrays.equals((byte[])valuesMap.get(FieldBinary),
                              arrayValueBinary.get(i).asBinary().get()));
            assertTrue(
                Arrays.equals((byte[])valuesMap.get(FieldFixedBinary),
                           arrayValueFixedBinary.get(i).asFixedBinary().get()));
            assertTrue(((String)valuesMap.get(FieldEnum))
                       .equals(arrayValueEnum.get(i).asEnum().get()));
            assertTrue(((Timestamp)valuesMap.get(FieldTimestamp))
                       .equals(arrayValueTimestamp.get(i).asTimestamp().get()));
            assertTrue(((BigDecimal)valuesMap.get(FieldNumber))
                       .compareTo(arrayValueDecimal.get(i).asNumber().get())
                           == 0);
        }
    }

    private void arrayValueAddValueIncorrectType(ArrayValue arrayValue,
                                            HashMap<String, Object> valuesMap) {
        FieldDef fieldDef = arrayValue.getDefinition().getElement();

        try {
            arrayValue.add((Integer)valuesMap.get(FieldInt));
            if (!fieldDef.isInteger()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isInteger()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((Long)valuesMap.get(FieldLong));
            if (!fieldDef.isLong()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isLong()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((String)valuesMap.get(FieldString));
            if (!fieldDef.isString()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isString()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((Double)valuesMap.get(FieldDouble));
            if (!fieldDef.isDouble()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isDouble()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((Float)valuesMap.get(FieldFloat));
            if (!fieldDef.isFloat()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isFloat()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((Boolean)valuesMap.get(FieldBoolean));
            if (!fieldDef.isBoolean()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isBoolean()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((byte[])valuesMap.get(FieldBinary));
            if (!fieldDef.isBinary()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isBinary()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.addFixed((byte[])valuesMap.get(FieldFixedBinary));
            if (!fieldDef.isFixedBinary()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isFixedBinary()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.addEnum((String)valuesMap.get(FieldEnum));
            if (!fieldDef.isEnum()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isEnum()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.add((Timestamp)valuesMap.get(FieldTimestamp));
            if (!fieldDef.isTimestamp()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isTimestamp()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            arrayValue.addNumber((BigDecimal)valuesMap.get(FieldNumber));
            if (!fieldDef.isNumber()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isNumber()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }
    }

    /* Test on array{array}, array{map}, array{record} */
    @Test
    public void testArrayFieldNestedPutGet()
        throws Exception {

        final String tblName = "DtTable";
        final String FieldArrayArray = "arrayField_array";
        final String FieldArrayMap = "arrayField_map";
        final String FieldArrayRecord = "arrayField_record";

        RecordBuilder rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addString(FieldString);
        rb.addDouble(FieldDouble);

        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addInteger();

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addLong();

        /* array{array} */
        ArrayBuilder afb1 = TableBuilder.createArrayBuilder();
        afb1.addField(ab.build());

        /* array{map} */
        ArrayBuilder afb2 = TableBuilder.createArrayBuilder();
        afb2.addField(mb.build());

        /* array{record} */
        ArrayBuilder afb3 = TableBuilder.createArrayBuilder();
        afb3.addField(rb.build());

        /* create table {integer, array{array}, array{map}, array{record}} */
        TableBuilderBase tb = TableBuilder.createTableBuilder(
            tblName, null, null)
            .addInteger(FieldId)
            .addField(FieldArrayArray, afb1.build())
            .addField(FieldArrayMap, afb2.build())
            .addField(FieldArrayRecord, afb3.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert a row. */
        TableImpl table = getTable(tblName);
        RowImpl row = table.createRow();
        row.put(FieldId, 1);
        ArrayValue av1 = row.putArray(FieldArrayArray);
        ArrayValue av2 = row.putArray(FieldArrayMap);
        ArrayValue av3 = row.putArray(FieldArrayRecord);

        final HashMap<String, Object> valuesMap =
            genValues(GenType.Random, 20, null, null, null);
        ArrayValue av1_array = av1.addArray();
        MapValue av2_mapValue = av2.addMap();
        RecordValue av3_recValue = av3.addRecord();
        av1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        av2_mapValue.put((String)valuesMap.get(FieldString),
                         ((Integer)valuesMap.get(FieldInt)).intValue());
        av3_recValue.put(FieldString, (String)valuesMap.get(FieldString));
        av3_recValue.put(FieldDouble,
                         ((Double)valuesMap.get(FieldDouble)).doubleValue());
        tableImpl.put(row, null, null);

        /* Get row and verify values */
        final PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1);
        RowImpl retRow = (RowImpl)tableImpl.get(key, null);
        av1_array = retRow.get(FieldArrayArray).asArray().get(0).asArray();
        assertEquals(1, av1_array.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     av1_array.get(0).asLong().get());
        av2_mapValue = retRow.get(FieldArrayMap).asArray().get(0).asMap();
        assertEquals(1, av2_mapValue.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     av2_mapValue.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        av3_recValue = retRow.get(FieldArrayRecord).asArray().get(0).asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(av3_recValue.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                     av3_recValue.get(FieldDouble).asDouble().get(), DELTA);
    }

    /* Basic put/get on field: map{simple types} */
    @Test
    public void testMapFieldPutGet()
        throws Exception {

        final String tblname = "DtTable";
        final String MapFieldInt = "mapFieldInt";
        final String MapFieldLong = "mapFieldLong";
        final String MapFieldDouble = "mapFieldDouble";
        final String MapFieldFloat = "mapFieldFloat";
        final String MapFieldString = "mapFieldString";
        final String MapFieldBoolean = "mapFieldBoolean";
        final String MapFieldBinary = "mapFieldBinary";
        final String MapFieldFixedBinary = "mapFieldFixedBinary";
        final String MapFieldEnum = "mapFieldEnum";
        final String[] enumValues =
            new String[]{"TYPE1", "TYPE2", "TYPE3", "TYPE4"};
        final String MapFieldTimestamp = "mapTimestamp";
        final String MapFieldNumber = "mapNumber";
        final int fixedLen = 128;

        /* map {Integer} */
        MapBuilder mapBuilderInt =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addInteger();
        /* map {Long} */
        MapBuilder mapBuilderLong =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addLong();
        /* map {String} */
        MapBuilder mapBuilderString =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addString();
        /* map {Double} */
        MapBuilder mapBuilderDouble =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addDouble();
        /* map {Float} */
        MapBuilder mapBuilderFloat =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addFloat();
        /* map {Boolean} */
        MapBuilder mapBuilderBoolean =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addBoolean();
        /* map {Binary} */
        MapBuilder mapBuilderBinary =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addBinary(null);
        /* map {FixedBinary} */
        MapBuilder mapBuilderFixedBinary =
            createMapBuilderFixedField(FieldFixedBinary, fixedLen);
        /* map {Enum} */
        MapBuilder mapBuilderEnum =
            createMapBuilderEnumField(FieldEnum, enumValues);
        /* map {Timestamp} */
        MapBuilder mapBuilderTimestamp =
            (MapBuilder)TableBuilder.createMapBuilder()
                .addTimestamp(9);
        /* map {Number} */
        MapBuilder mapBuilderDecimal =
            (MapBuilder)TableBuilder.createMapBuilder().addNumber();
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(
                tblname, "test map type", null)
                    .addInteger(FieldId)
                    .addField(MapFieldInt, mapBuilderInt.build())
                    .addField(MapFieldLong, mapBuilderLong.build())
                    .addField(MapFieldDouble, mapBuilderDouble.build())
                    .addField(MapFieldFloat, mapBuilderFloat.build())
                    .addField(MapFieldString, mapBuilderString.build())
                    .addField(MapFieldBoolean, mapBuilderBoolean.build())
                    .addField(MapFieldBinary, mapBuilderBinary.build())
                    .addField(MapFieldFixedBinary,
                              mapBuilderFixedBinary.build())
                    .addField(MapFieldEnum, mapBuilderEnum.build())
                    .addField(MapFieldTimestamp, mapBuilderTimestamp.build())
                    .addField(MapFieldNumber, mapBuilderDecimal.build())
                    .primaryKey(FieldId)
                    .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* insert a row */
        final TableImpl table = getTable(tblname);
        final RowImpl row = table.createRow();
        MapValue mapValueInt = row.putMap(MapFieldInt);
        MapValue mapValueLong = row.putMap(MapFieldLong);
        MapValue mapValueDouble = row.putMap(MapFieldDouble);
        MapValue mapValueFloat = row.putMap(MapFieldFloat);
        MapValue mapValueString = row.putMap(MapFieldString);
        MapValue mapValueBoolean = row.putMap(MapFieldBoolean);
        MapValue mapValueBinary = row.putMap(MapFieldBinary);
        MapValue mapValueFixedBinary = row.putMap(MapFieldFixedBinary);
        MapValue mapValueEnum = row.putMap(MapFieldEnum);
        MapValue mapValueTimestamp = row.putMap(MapFieldTimestamp);
        MapValue mapValueDecimal = row.putMap(MapFieldNumber);

        final int nItem = 5;
        final int bytesLen = 128;
        final int strLen = 20;
        ArrayList<HashMap<String, Object>> valueMapList =
            new ArrayList<HashMap<String, Object>>();
        row.put(FieldId, 1);
        for (int i = 0; i < nItem; i++) {
            HashMap<String, Object> valuesMap =
                genValues(GenType.Random, strLen,
                          bytesLen, fixedLen, enumValues);
            String mapField = (String)valuesMap.get(FieldString) + i;
            if (i == 0) {
                mapValueAddValueIncorrectType(
                    mapValueInt, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueLong, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueDouble, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueFloat, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueString, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueBoolean, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueBinary, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueFixedBinary, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueEnum, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueTimestamp, mapField, valuesMap);
                mapValueAddValueIncorrectType(
                    mapValueDecimal, mapField, valuesMap);
            } else {
                mapValueInt.put(mapField, (Integer)valuesMap.get(FieldInt));
                mapValueLong.put(mapField, (Long)valuesMap.get(FieldLong));
                mapValueDouble.put(
                    mapField, (Double)valuesMap.get(FieldDouble));
                mapValueFloat.put(
                    mapField, (Float)valuesMap.get(FieldFloat));
                mapValueString.put(
                    mapField, (String)valuesMap.get(FieldString));
                mapValueBoolean.put(
                    mapField, (Boolean)valuesMap.get(FieldBoolean));
                mapValueBinary.put(
                    mapField, (byte[])valuesMap.get(FieldBinary));
                mapValueFixedBinary.putFixed(
                    mapField, (byte[])valuesMap.get(FieldFixedBinary));
                mapValueEnum.putEnum(
                    mapField, (String)valuesMap.get(FieldEnum));
                mapValueTimestamp.put(
                    mapField, (Timestamp)valuesMap.get(FieldTimestamp));
                mapValueDecimal.putNumber(
                    mapField, (BigDecimal)valuesMap.get(FieldNumber));
            }
            valueMapList.add(i, valuesMap);
        }
        try {
            tableImpl.put(row, null, null);
        } catch (Exception e) {
            fail("Put should be OK but get expception: " + e);
        }

        /* Retrieve row and verify the row values */
        PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1);
        Row retRow = tableImpl.get(key, null);
        mapValueInt = (MapValue)retRow.get(MapFieldInt);
        mapValueLong = (MapValue)retRow.get(MapFieldLong);
        mapValueDouble = (MapValue)retRow.get(MapFieldDouble);
        mapValueFloat = (MapValue)retRow.get(MapFieldFloat);
        mapValueString = (MapValue)retRow.get(MapFieldString);
        mapValueBoolean = (MapValue)retRow.get(MapFieldBoolean);
        mapValueBinary = (MapValue)retRow.get(MapFieldBinary);
        mapValueFixedBinary = (MapValue)retRow.get(MapFieldFixedBinary);
        mapValueEnum = (MapValue)retRow.get(MapFieldEnum);
        mapValueTimestamp = (MapValue)retRow.get(MapFieldTimestamp);
        mapValueDecimal = (MapValue)retRow.get(MapFieldNumber);

        assertEquals(valueMapList.size(), mapValueInt.size());
        assertEquals(valueMapList.size(), mapValueLong.size());
        assertEquals(valueMapList.size(), mapValueDouble.size());
        assertEquals(valueMapList.size(), mapValueFloat.size());
        assertEquals(valueMapList.size(), mapValueString.size());
        assertEquals(valueMapList.size(), mapValueBoolean.size());
        assertEquals(valueMapList.size(), mapValueBinary.size());
        assertEquals(valueMapList.size(), mapValueFixedBinary.size());
        assertEquals(valueMapList.size(), mapValueEnum.size());
        assertEquals(valueMapList.size(), mapValueTimestamp.size());
        assertEquals(valueMapList.size(), mapValueDecimal.size());

        for (int i = 0; i < valueMapList.size(); i++) {
            HashMap<String, Object> valuesMap = valueMapList.get(i);
            String mapField = (String)valuesMap.get(FieldString) + i;
            assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                         mapValueInt.get(mapField).asInteger().get());
            assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                         mapValueLong.get(mapField).asLong().get());
            assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                         mapValueDouble.get(mapField).asDouble().get(), DELTA);
            assertEquals(((Float)valuesMap.get(FieldFloat)).floatValue(),
                         mapValueFloat.get(mapField).asFloat().get(),
                         (float)DELTA);
            assertTrue(((String)valuesMap.get(FieldString))
                        .equals(mapValueString.get(mapField).asString().get()));
            assertEquals(((Boolean)valuesMap.get(FieldBoolean)).booleanValue(),
                         mapValueBoolean.get(mapField).asBoolean().get());
            assertTrue(
                Arrays.equals((byte[])valuesMap.get(FieldBinary),
                              mapValueBinary.get(mapField).asBinary().get()));
            assertTrue(
                Arrays.equals((byte[])valuesMap.get(FieldFixedBinary),
                              mapValueFixedBinary.get(mapField)
                              .asFixedBinary().get()));
            assertTrue(((String)valuesMap.get(FieldEnum))
                       .equals(mapValueEnum.get(mapField).asEnum().get()));
            assertTrue(((Timestamp)valuesMap.get(FieldTimestamp))
                       .equals(mapValueTimestamp.get(mapField)
                               .asTimestamp().get()));
            assertTrue(((BigDecimal)valuesMap.get(FieldNumber))
                       .compareTo(mapValueDecimal.get(mapField)
                                 .asNumber().get()) == 0);
        }
    }

    private void mapValueAddValueIncorrectType(
        MapValue mapValue,
        String mapField,
        HashMap<String, Object> valuesMap) {

        FieldDef fieldDef = mapValue.getDefinition().getElement();

        try {
            mapValue.put(mapField, (Integer)valuesMap.get(FieldInt));
            if (!fieldDef.isInteger()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isInteger()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (Long)valuesMap.get(FieldLong));
            if (!fieldDef.isLong()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isLong()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (String)valuesMap.get(FieldString));
            if (!fieldDef.isString()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isString()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (Double)valuesMap.get(FieldDouble));
            if (!fieldDef.isDouble()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isDouble()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (Float)valuesMap.get(FieldFloat));
            if (!fieldDef.isFloat()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isFloat()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (Boolean)valuesMap.get(FieldBoolean));
            if (!fieldDef.isBoolean()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isBoolean()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (byte[])valuesMap.get(FieldBinary));
            if (!fieldDef.isBinary()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isBinary()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.putFixed(mapField,
                (byte[])valuesMap.get(FieldFixedBinary));
            if (!fieldDef.isFixedBinary()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isFixedBinary()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.putEnum(mapField, (String)valuesMap.get(FieldEnum));
            if (!fieldDef.isEnum()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isEnum()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.put(mapField, (Timestamp)valuesMap.get(FieldTimestamp));
            if (!fieldDef.isTimestamp()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isTimestamp()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }

        try {
            mapValue.putNumber(mapField,
                               (BigDecimal)valuesMap.get(FieldNumber));
            if (!fieldDef.isNumber()) {
                fail("Excepted to get IllegalArgumentException but OK.");
            }
        } catch (Exception e) {
            if (fieldDef.isNumber()) {
                fail("Excepted to be OK but get exception:" + e.getMessage());
            }
        }
    }

    /* Test on map{array}, map{map}, map{record} */
    @Test
    public void testMapFieldNestedPutGet()
        throws Exception {

        final String tblName = "DtTable";
        final String FieldMapArray = "mapField_array";
        final String FieldMapMap = "mapField_map";
        final String FieldMapRecord = "mapField_record";

        RecordBuilder rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addString(FieldString);
        rb.addDouble(FieldDouble);

        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addInteger();

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addLong();

        /* map{array} */
        MapBuilder mfb1 = TableBuilder.createMapBuilder();
        mfb1.addField(ab.build());

        /* map{map} */
        MapBuilder mfb2 = TableBuilder.createMapBuilder();
        mfb2.addField(mb.build());

        /* map{record} */
        MapBuilder mfb3 = TableBuilder.createMapBuilder();
        mfb3.addField(rb.build());

        /* create table {integer, map{array}, map{map}, map{record}} */
        TableBuilderBase tb = TableBuilder.createTableBuilder(
            tblName, null, null)
            .addInteger(FieldId)
            .addField(FieldMapArray, mfb1.build())
            .addField(FieldMapMap, mfb2.build())
            .addField(FieldMapRecord, mfb3.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert a row */
        TableImpl table = getTable(tblName);
        RowImpl row = table.createRow();
        row.put(FieldId, 1);
        MapValue mv1 = row.putMap(FieldMapArray);
        MapValue mv2 = row.putMap(FieldMapMap);
        MapValue mv3 = row.putMap(FieldMapRecord);

        final HashMap<String, Object> valuesMap =
            genValues(GenType.Random, 20, null, null, null);
        final String mapField = "mapField";
        ArrayValue mv1_array = mv1.putArray(mapField);
        MapValue mv2_mapValue = mv2.putMap(mapField);
        RecordValue mv3_recValue = mv3.putRecord(mapField);
        mv1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        mv2_mapValue.put((String)valuesMap.get(FieldString),
                         ((Integer)valuesMap.get(FieldInt)).intValue());
        mv3_recValue.put(FieldString, (String)valuesMap.get(FieldString));
        mv3_recValue.put(FieldDouble,
                         ((Double)valuesMap.get(FieldDouble)).doubleValue());
        tableImpl.put(row, null, null);

        /* Get row and verify values */
        final PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1);
        final RowImpl retRow = (RowImpl)tableImpl.get(key, null);
        mv1_array = retRow.get(FieldMapArray).asMap().get(mapField).asArray();
        assertEquals(1, mv1_array.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     mv1_array.get(0).asLong().get());
        mv2_mapValue = retRow.get(FieldMapMap).asMap().get(mapField).asMap();
        assertEquals(1, mv2_mapValue.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     mv2_mapValue.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        mv3_recValue =
            retRow.get(FieldMapRecord).asMap().get(mapField).asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(mv3_recValue.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                     mv3_recValue.get(FieldDouble).asDouble().get(), DELTA);
    }

    /* Test on record{simple types} */
    @Test
    public void testRecordFieldPutGet()
        throws Exception {

        final String tblName = "DtTable";
        final String[] enumValues = new String[] {"TYPE1","TYPE2","TYPE3"};
        final int fixedLen = 256;
        /*
         * Create a table contains a record field
         * record {Integer, Long, Double, String, Binary, FixedBinary,
         *         Boolean, Enum}
         */
        RecordBuilder rb =
            (RecordBuilder)TableBuilder.createRecordBuilder(FieldRecord)
            .addInteger(FieldInt)
            .addLong(FieldLong)
            .addDouble(FieldDouble)
            .addFloat(FieldFloat)
            .addString(FieldString)
            .addBinary(FieldBinary)
            .addFixedBinary(FieldFixedBinary, fixedLen)
            .addBoolean(FieldBoolean, null)
            .addEnum(FieldEnum, enumValues, null, null, null)
            .addTimestamp(FieldTimestamp, 9)
            .addNumber(FieldNumber);

        TableBuilderBase tb =
            TableBuilder.createTableBuilder(tblName, null, null)
            .addInteger(FieldId)
            .addField(FieldRecord, rb.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        /* Insert nRec rows */
        final TableImpl table = getTable(tblName);
        final int nRec = 5;
        final int bytesLen = 128;
        final int strLen = 20;
        RowImpl row = table.createRow();
        ArrayList<HashMap<String, Object>> valuesMapList =
            new ArrayList<HashMap<String, Object>>();
        for (int i = 0; i < nRec; i++) {
            HashMap<String, Object> valuesMap =
                genValues(GenType.Random, strLen,
                          bytesLen, fixedLen, enumValues);
            row.put(FieldId, i);
            RecordValue rv = row.putRecord(FieldRecord);
            rv.put(FieldInt, ((Integer)valuesMap.get(FieldInt)).intValue());
            rv.put(FieldLong, ((Long)valuesMap.get(FieldLong)).longValue());
            rv.put(FieldDouble,
                ((Double)valuesMap.get(FieldDouble)).doubleValue());
            rv.put(FieldFloat,
                ((Float)valuesMap.get(FieldFloat)).floatValue());
            rv.put(FieldString, (String)valuesMap.get(FieldString));
            rv.put(FieldBoolean,
                ((Boolean)valuesMap.get(FieldBoolean)).booleanValue());
            rv.put(FieldBinary, (byte[])valuesMap.get(FieldBinary));
            rv.putFixed(FieldFixedBinary,
                        (byte[])valuesMap.get(FieldFixedBinary));
            rv.putEnum(FieldEnum, (String)valuesMap.get(FieldEnum));
            rv.put(FieldTimestamp, (Timestamp)valuesMap.get(FieldTimestamp));
            rv.putNumber(FieldNumber, (BigDecimal)valuesMap.get(FieldNumber));
            valuesMapList.add(valuesMap);
            tableImpl.put(row, null, null);
        }

        /* Retrieve rows and verify values for each field. */
        for (int i = 0; i < nRec; i++) {
            HashMap<String, Object> valuesMap = valuesMapList.get(i);
            PrimaryKey key = table.createPrimaryKey();
            key.put(FieldId, i);
            RowImpl retRow = (RowImpl)tableImpl.get(key, null);
            RecordValue rv = retRow.get(FieldRecord).asRecord();
            assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                         rv.get(FieldInt).asInteger().get());
            assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                         rv.get(FieldLong).asLong().get());
            assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                         rv.get(FieldDouble).asDouble().get(), DELTA);
            assertEquals(((Float)valuesMap.get(FieldFloat)).floatValue(),
                         rv.get(FieldFloat).asFloat().get(), (float)DELTA);
            assertTrue(((String)valuesMap.get(FieldString))
                       .equals(rv.get(FieldString).asString().get()));
            assertEquals(((Boolean)valuesMap.get(FieldBoolean)).booleanValue(),
                         rv.get(FieldBoolean).asBoolean().get());
            assertTrue(
                Arrays.equals((byte[])valuesMap.get(FieldBinary),
                              rv.get(FieldBinary).asBinary().get()));
            assertTrue(((String)valuesMap.get(FieldEnum))
                        .equals(rv.get(FieldEnum).asEnum().get()));
            assertTrue(((Timestamp)valuesMap.get(FieldTimestamp))
                       .equals(rv.get(FieldTimestamp).asTimestamp().get()));
            assertTrue(((BigDecimal)valuesMap.get(FieldNumber))
                       .compareTo(rv.get(FieldNumber).asNumber().get()) == 0);
        }
    }

    /* Test on record{map, array, record} */
    @Test
    public void testRecordFieldNestedPutGet()
        throws Exception {

        final String tblName = "DtTable";
        final String FieldRecordAll = "FieldRecordAll";

        RecordBuilder rb = TableBuilder.createRecordBuilder(FieldRecord);
        rb.addString(FieldString);
        rb.addDouble(FieldDouble);
        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addInteger();
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addLong();

        /* Record{array, map, record} */
        RecordBuilder rb1 = TableBuilder.createRecordBuilder(FieldRecordAll);
        rb1.addField(FieldMap, mb.build());
        rb1.addField(FieldArray, ab.build());
        rb1.addField(FieldRecord, rb.build());

        /* create table {integer, Record{array, map, record}} */
        TableBuilderBase tb = TableBuilder.createTableBuilder(
            tblName, null, null)
            .addInteger(FieldId)
            .addField(FieldRecordAll, rb1.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        addTable((TableBuilder)tb, true);

        TableImpl table = getTable(tblName);
        RowImpl row = table.createRow();
        row.put(FieldId, 1);

        final HashMap<String, Object> valuesMap =
            genValues(GenType.Random, 20, null, null, null);
        RecordValue rv = row.putRecord(FieldRecordAll);
        ArrayValue rv_av = rv.putArray(FieldArray);
        MapValue rv_mv = rv.putMap(FieldMap);
        RecordValue rv_rv = rv.putRecord(FieldRecord);

        rv_av.add(((Long)valuesMap.get(FieldLong)).longValue());
        rv_mv.put((String)valuesMap.get(FieldString),
                  ((Integer)valuesMap.get(FieldInt)).intValue());
        rv_rv.put(FieldString, (String)valuesMap.get(FieldString));
        rv_rv.put(
            FieldDouble, ((Double)valuesMap.get(FieldDouble)).doubleValue());
        tableImpl.put(row, null, null);

        /* Get row and verify values */
        final PrimaryKey key = table.createPrimaryKey();
        key.put(FieldId, 1);
        RowImpl retRow = (RowImpl)tableImpl.get(key, null);
        rv = retRow.get(FieldRecordAll).asRecord();
        rv_av = rv.get(FieldArray).asArray();
        assertEquals(1, rv_av.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     rv_av.get(0).asLong().get());
        rv_mv = rv.get(FieldMap).asMap();
        assertEquals(1, rv_mv.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     rv_mv.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        rv_rv = rv.get(FieldRecord).asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(rv_rv.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                    rv_rv.get(FieldDouble).asDouble().get(), DELTA);
    }

    private long lastSeed = 0L;
    private long getRandomSeed() {
        long seed = System.currentTimeMillis();
        if (lastSeed == seed) {
            seed++;
        }
        lastSeed = seed;
        return seed;
    }

    private HashMap<String, Object> genValues(GenType type,
                                              Integer stringLen,
                                              Integer bytesLen,
                                              Integer fixedLen,
                                              String[] enumValues) {
        HashMap<String, Object> values = new HashMap<String, Object>();
        Long seed = getRandomSeed();
        final Random rand = new Random(seed);
        values.put(FieldId, seed);
        switch(type) {
        case Min:
            values.put(FieldInt, Integer.MIN_VALUE);
            values.put(FieldLong, Long.MIN_VALUE);
            values.put(FieldDouble, Double.MIN_VALUE);
            values.put(FieldFloat, Float.MIN_VALUE);
            values.put(FieldTimestamp, TimestampDefImpl.MIN_VALUE);
            break;
        case Max:
            values.put(FieldInt, Integer.MAX_VALUE);
            values.put(FieldLong, Long.MAX_VALUE);
            values.put(FieldDouble, Double.MAX_VALUE);
            values.put(FieldFloat, Float.MAX_VALUE);
            values.put(FieldTimestamp, TimestampDefImpl.MAX_VALUE);
            break;
        case Random:
            values.put(FieldInt, rand.nextInt());
            values.put(FieldLong, rand.nextLong());
            values.put(FieldDouble, rand.nextDouble());
            values.put(FieldFloat, rand.nextFloat());
            values.put(FieldTimestamp, randTimestamp(rand));
            break;
        default:
            fail("Cannot get there");
        }
        values.put(FieldBoolean, rand.nextBoolean());
        if (bytesLen != null) {
            byte[] bytes = new byte[bytesLen];
            rand.nextBytes(bytes);
            values.put(FieldBinary, bytes);
        }
        if (fixedLen != null) {
            byte[] bytes = new byte[fixedLen];
            rand.nextBytes(bytes);
            values.put(FieldFixedBinary, bytes);
        }
        if (stringLen != null) {
            values.put(FieldString, randomString(rand, stringLen));
        }
        if (enumValues != null) {
            int iEnum = Math.abs(rand.nextInt()) % enumValues.length;
            values.put(FieldEnum, enumValues[iEnum]);
        }
        values.put(FieldNumber, randomDecimal(rand));
        return values;
    }

    private BigDecimal randomDecimal(Random rand) {
        return new BigDecimal(BigInteger.valueOf(rand.nextLong()),
                              rand.nextInt());
    }

    private String randomString(Random rand, int len) {
       final String AB =
           "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
       StringBuilder sb = new StringBuilder(len);
       for( int i = 0; i < len; i++ ) {
        sb.append(AB.charAt(rand.nextInt(AB.length())));
    }
       return sb.toString();
    }

    private Timestamp randTimestamp(Random rand) {
        long millis = TimestampDefImpl.MIN_VALUE.getTime() +
                      (long)(rand.nextDouble() *
                             (TimestampDefImpl.MAX_VALUE.getTime() -
                              TimestampDefImpl.MIN_VALUE.getTime()));
        int nanos = (int)((Math.abs(millis) % 1000) * 1000000 +
                    rand.nextInt(999999));
        return TimestampUtils.createTimestamp(millis/1000,  nanos);
    }

    private Timestamp roundTimestamp(Timestamp ts, int precision) {
        return TimestampUtils.roundToPrecision(ts, precision);
    }
}
