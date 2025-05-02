/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import oracle.kv.TestBase;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import org.junit.Test;

public class ComplexFieldTest extends TestBase{

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
    private final String FieldMap = "mapField";
    private final String FieldArray = "arrayField";
    private final String FieldRecord = "recordField";

    private final double DELTA = 1e-15;

    /*
     * Test on field: Array{simple types}
     */
    @Test
    public void testArrayFieldPutGet() {

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
        /* Array{fxied Binary} */
        ArrayBuilder arrayBuilderFixedBinary =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
            .addFixedBinary(FieldFixedBinary, fixedLen);
        /* Array{Enum} */
        ArrayBuilder arrayBuilderEnum =
            (ArrayBuilder)TableBuilder.createArrayBuilder()
            .addEnum(FieldEnum, enumValues, null);

        /* Create table includes all above array{simple type} field. */
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(
                null, tblname, "test array type", null, null)
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
                    .primaryKey(FieldId)
                    .shardKey(FieldId);
        final TableImpl table = tb.buildTable();

        /* Create a row, put nItems values to each array field.*/
        final int nItem = 5;
        final int bytesLen = 128;
        final int strLen = 20;

        /*
         * Put on array field using:
         * RecordValue putArray(String name, Iterable<?> list)
         * RecordValue putArray(String name, Object[] array)
         */
        final RowImpl row = table.createRow();
        row.put(FieldId, 1);
        Integer[] intArray = new Integer[nItem];
        Long[] longArray = new Long[nItem];
        List<Double> doubleArray = new ArrayList<Double>();
        List<Float> floatArray = new ArrayList<Float>();
        List<String> stringArray = new ArrayList<String>();
        List<Boolean> booleanArray = new ArrayList<Boolean>();
        List<byte[]> binaryArray = new ArrayList<byte[]>();
        List<byte[]> fixedBinaryArray = new ArrayList<byte[]>();
        List<String> enumArray = new ArrayList<String>();
        ArrayList<HashMap<String, Object>> valueMapList =
            new ArrayList<HashMap<String, Object>>();
        for (int i = 0; i < nItem; i++) {
            HashMap<String, Object> valuesMap =
                genValues(strLen, bytesLen, fixedLen, enumValues);
            intArray[i] = (Integer)valuesMap.get(FieldInt);
            longArray[i] = (Long)valuesMap.get(FieldLong);
            doubleArray.add((Double)valuesMap.get(FieldDouble));
            floatArray.add((Float)valuesMap.get(FieldFloat));
            stringArray.add((String)valuesMap.get(FieldString));
            booleanArray.add((Boolean)valuesMap.get(FieldBoolean));
            binaryArray.add((byte[])valuesMap.get(FieldBinary));
            fixedBinaryArray.add((byte[])valuesMap.get(FieldFixedBinary));
            enumArray.add((String)valuesMap.get(FieldEnum));
            valueMapList.add(i, valuesMap);
        }
        /* When field type doesn't match with value type, throw exception */
        try {
            row.putArray(ArrayFieldInt, longArray);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        try {
            row.putArray(ArrayFieldFloat, doubleArray);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        row.putArray(ArrayFieldInt, intArray);
        row.putArray(ArrayFieldLong, longArray);
        row.putArray(ArrayFieldDouble, doubleArray);
        row.putArray(ArrayFieldFloat, floatArray);
        row.putArray(ArrayFieldString, stringArray);
        row.putArray(ArrayFieldBoolean, booleanArray);
        row.putArray(ArrayFieldBinary, binaryArray);
        row.putArray(ArrayFieldFixedBinary, fixedBinaryArray);
        row.putArray(ArrayFieldEnum, enumArray);

        /*
         * Put on array field using:
         * RecordValue putArray(String name)
         */
        final RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        ArrayValue arrayValueInt = row2.putArray(ArrayFieldInt);
        ArrayValue arrayValueLong = row2.putArray(ArrayFieldLong);
        ArrayValue arrayValueDouble = row2.putArray(ArrayFieldDouble);
        ArrayValue arrayValueFloat = row2.putArray(ArrayFieldFloat);
        ArrayValue arrayValueString = row2.putArray(ArrayFieldString);
        ArrayValue arrayValueBoolean = row2.putArray(ArrayFieldBoolean);
        ArrayValue arrayValueBinary = row2.putArray(ArrayFieldBinary);
        ArrayValue arrayValueFixedBinary = row2.putArray(ArrayFieldFixedBinary);
        ArrayValue arrayValueEnum = row2.putArray(ArrayFieldEnum);
        for (int i = 0; i < valueMapList.size(); i++) {
            HashMap<String, Object> valuesMap = valueMapList.get(i);
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
        }

        /* Verify */
        assertTrue(row.equals(row2));

        arrayValueInt = (ArrayValue)row.get(ArrayFieldInt);
        arrayValueLong = (ArrayValue)row.get(ArrayFieldLong);
        arrayValueDouble = (ArrayValue)row.get(ArrayFieldDouble);
        arrayValueFloat = (ArrayValue)row.get(ArrayFieldFloat);
        arrayValueString = (ArrayValue)row.get(ArrayFieldString);
        arrayValueBoolean = (ArrayValue)row.get(ArrayFieldBoolean);
        arrayValueBinary = (ArrayValue)row.get(ArrayFieldBinary);
        arrayValueFixedBinary = (ArrayValue)row.get(ArrayFieldFixedBinary);
        arrayValueEnum = (ArrayValue)row.get(ArrayFieldEnum);

        assertEquals(valueMapList.size(), arrayValueInt.size());
        assertEquals(valueMapList.size(), arrayValueLong.size());
        assertEquals(valueMapList.size(), arrayValueDouble.size());
        assertEquals(valueMapList.size(), arrayValueFloat.size());
        assertEquals(valueMapList.size(), arrayValueString.size());
        assertEquals(valueMapList.size(), arrayValueBoolean.size());
        assertEquals(valueMapList.size(), arrayValueBinary.size());
        assertEquals(valueMapList.size(), arrayValueFixedBinary.size());
        assertEquals(valueMapList.size(), arrayValueEnum.size());

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
        }
    }

    /*
     * Test on array{array}, array{map}, array{record}
     */
    @Test
    public void testArrayFieldNestedPutGet() {

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
            null, tblName, null, null, null)
            .addInteger(FieldId)
            .addField(FieldArrayArray, afb1.build())
            .addField(FieldArrayMap, afb2.build())
            .addField(FieldArrayRecord, afb3.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        final TableImpl table = tb.buildTable();

        /*
         * Create a row, put on array field using:
         * RecordValue putArray(String name, Iterable<?> list)
         */
        RowImpl row = table.createRow();
        row.put(FieldId, 1);
        List<List<Long>> av1 = new ArrayList<List<Long>>();
        List<Map<String, Integer>> av2 = new ArrayList<Map<String, Integer>>();
        List<Map<String, Object>> av3 = new ArrayList<Map<String, Object>>();
        final HashMap<String, Object> valuesMap =
            genValues(20, null, null, null);
        List<Long> av1_array = new ArrayList<Long>();
        Map<String, Integer> av2_mapValue = new HashMap<String, Integer>();
        Map<String, Object> av3_recValue = new HashMap<String, Object>();
        av1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        av2_mapValue.put((String)valuesMap.get(FieldString),
                         ((Integer)valuesMap.get(FieldInt)).intValue());
        av3_recValue.put(FieldString, valuesMap.get(FieldString));
        av3_recValue.put(FieldDouble, valuesMap.get(FieldDouble));
        av1.add(av1_array);
        av2.add(av2_mapValue);
        av3.add(av3_recValue);
        row.putArray(FieldArrayArray, av1);
        row.putArray(FieldArrayMap, av2);
        row.putArray(FieldArrayRecord, av3);

        /*
         * Create a row, put on array field using:
         * RecordValue putArray(String name)
         */
        final RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        ArrayValue avVal1 = row2.putArray(FieldArrayArray);
        ArrayValue avVal2 = row2.putArray(FieldArrayMap);
        ArrayValue avVal3 = row2.putArray(FieldArrayRecord);
        ArrayValue avVal1_array = avVal1.addArray();
        MapValue avVal2_mapValue = avVal2.addMap();
        RecordValue avVal3_recValue = avVal3.addRecord();
        avVal1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        avVal2_mapValue.put((String)valuesMap.get(FieldString),
                            ((Integer)valuesMap.get(FieldInt)).intValue());
        avVal3_recValue.put(FieldString, (String)valuesMap.get(FieldString));
        avVal3_recValue.put(FieldDouble,
                            ((Double)valuesMap.get(FieldDouble)).doubleValue());

        /* Verify */
        assertTrue(row.equals(row2));

        ArrayValue av1_get = row.get(FieldArrayArray).asArray().get(0).
                             asArray();
        assertEquals(1, av1_get.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     av1_get.get(0).asLong().get());
        MapValue av2_get = row.get(FieldArrayMap).asArray().get(0).asMap();
        assertEquals(1, av2_get.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     av2_get.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        RecordValue av3_get = row.get(FieldArrayRecord).asArray().get(0).
                              asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(av3_get.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                     av3_get.get(FieldDouble).asDouble().get(), DELTA);
    }

    /*
     * Test on field: map{simple types}
     */
    @Test
    public void testMapFieldPutGet() {

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
            (MapBuilder)TableBuilder.createMapBuilder()
            .addFixedBinary(FieldFixedBinary, fixedLen);
        /* map {Enum} */
        MapBuilder mapBuilderEnum =
            (MapBuilder)TableBuilder.createMapBuilder()
            .addEnum(FieldEnum, enumValues, null);
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(
                null, tblname, "test map type", null, null)
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
                    .primaryKey(FieldId)
                    .shardKey(FieldId);
        final TableImpl table = tb.buildTable();

        /* Create a row, put nItems values to each array field.*/
        final int nItem = 5;
        final int bytesLen = 128;
        final int strLen = 20;

        /*
         * Put on map field using:
         * RecordValue putMap(String fieldName, Map<String, ?> map)
         */
        final RowImpl row = table.createRow();
        row.put(FieldId, 1);
        Map<String, Integer> intMap = new HashMap<String, Integer>();
        Map<String, Long> longMap = new HashMap<String, Long>();
        Map<String, Double> doubleMap = new HashMap<String, Double>();
        Map<String, Float> floatMap = new HashMap<String, Float>();
        Map<String, String> stringMap = new HashMap<String, String>();
        Map<String, Boolean> booleanMap = new HashMap<String, Boolean>();
        Map<String, byte[]> binaryMap = new HashMap<String, byte[]>();
        Map<String, byte[]> fixedBinaryMap = new HashMap<String, byte[]>();
        Map<String, String> enumMap = new HashMap<String, String>();
        ArrayList<HashMap<String, Object>> valueMapList =
            new ArrayList<HashMap<String, Object>>();
        for (int i = 0; i < nItem; i++) {
            HashMap<String, Object> valuesMap =
                genValues(strLen, bytesLen, fixedLen, enumValues);
            String mapField = (String)valuesMap.get(FieldString) + i;
            intMap.put(mapField, (Integer)valuesMap.get(FieldInt));
            longMap.put(mapField, (Long)valuesMap.get(FieldLong));
            doubleMap.put(mapField, (Double)valuesMap.get(FieldDouble));
            floatMap.put(mapField, (Float)valuesMap.get(FieldFloat));
            stringMap.put(mapField, (String)valuesMap.get(FieldString));
            booleanMap.put(mapField, (Boolean)valuesMap.get(FieldBoolean));
            binaryMap.put(mapField, (byte[])valuesMap.get(FieldBinary));
            fixedBinaryMap.put(
                mapField, (byte[])valuesMap.get(FieldFixedBinary));
            enumMap.put(mapField, (String)valuesMap.get(FieldEnum));
            valueMapList.add(i, valuesMap);
        }
        /* When field type doesn't match with value type, throw exception */
        try {
            row.putMap(MapFieldInt, longMap);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        try {
            row.putMap(MapFieldFloat, doubleMap);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        row.putMap(MapFieldInt, intMap);
        row.putMap(MapFieldLong, longMap);
        row.putMap(MapFieldDouble, doubleMap);
        row.putMap(MapFieldFloat, floatMap);
        row.putMap(MapFieldString, stringMap);
        row.putMap(MapFieldBoolean, booleanMap);
        row.putMap(MapFieldBinary, binaryMap);
        row.putMap(MapFieldFixedBinary, fixedBinaryMap);
        row.putMap(MapFieldEnum, enumMap);

        /*
         * Put on map field using:
         * RecordValue putMap(String fieldName)
         */
        final RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        MapValue mapValueInt = row2.putMap(MapFieldInt);
        MapValue mapValueLong = row2.putMap(MapFieldLong);
        MapValue mapValueDouble = row2.putMap(MapFieldDouble);
        MapValue mapValueFloat = row2.putMap(MapFieldFloat);
        MapValue mapValueString = row2.putMap(MapFieldString);
        MapValue mapValueBoolean = row2.putMap(MapFieldBoolean);
        MapValue mapValueBinary = row2.putMap(MapFieldBinary);
        MapValue mapValueFixedBinary = row2.putMap(MapFieldFixedBinary);
        MapValue mapValueEnum = row2.putMap(MapFieldEnum);
        for (int i = 0; i < valueMapList.size(); i++) {
            HashMap<String, Object> valuesMap = valueMapList.get(i);
            String mapField = (String)valuesMap.get(FieldString) + i;
            mapValueInt.put(mapField, (Integer)valuesMap.get(FieldInt));
            mapValueLong.put(mapField, (Long)valuesMap.get(FieldLong));
            mapValueDouble.put(mapField, (Double)valuesMap.get(FieldDouble));
            mapValueFloat.put(mapField, (Float)valuesMap.get(FieldFloat));
            mapValueString.put(mapField, (String)valuesMap.get(FieldString));
            mapValueBoolean.put(mapField, (Boolean)valuesMap.get(FieldBoolean));
            mapValueBinary.put(mapField, (byte[])valuesMap.get(FieldBinary));
            mapValueFixedBinary.putFixed(
                mapField, (byte[])valuesMap.get(FieldFixedBinary));
            mapValueEnum.putEnum(mapField, (String)valuesMap.get(FieldEnum));
        }

        /* Verify */
        assertTrue(row.equals(row2));

        mapValueInt = (MapValue)row.get(MapFieldInt);
        mapValueLong = (MapValue)row.get(MapFieldLong);
        mapValueDouble = (MapValue)row.get(MapFieldDouble);
        mapValueFloat = (MapValue)row.get(MapFieldFloat);
        mapValueString = (MapValue)row.get(MapFieldString);
        mapValueBoolean = (MapValue)row.get(MapFieldBoolean);
        mapValueBinary = (MapValue)row.get(MapFieldBinary);
        mapValueFixedBinary = (MapValue)row.get(MapFieldFixedBinary);
        mapValueEnum = (MapValue)row.get(MapFieldEnum);

        assertEquals(valueMapList.size(), mapValueInt.size());
        assertEquals(valueMapList.size(), mapValueLong.size());
        assertEquals(valueMapList.size(), mapValueDouble.size());
        assertEquals(valueMapList.size(), mapValueFloat.size());
        assertEquals(valueMapList.size(), mapValueString.size());
        assertEquals(valueMapList.size(), mapValueBoolean.size());
        assertEquals(valueMapList.size(), mapValueBinary.size());
        assertEquals(valueMapList.size(), mapValueFixedBinary.size());
        assertEquals(valueMapList.size(), mapValueEnum.size());

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
        }
    }

    /*
     * Test on map{array}, map{map}, map{record}
     */
    @Test
    public void testMapFieldNestedPutGet() {

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
            null, tblName, null, null, null)
            .addInteger(FieldId)
            .addField(FieldMapArray, mfb1.build())
            .addField(FieldMapMap, mfb2.build())
            .addField(FieldMapRecord, mfb3.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        TableImpl table = tb.buildTable();

        /*
         * Create a row, put on map field using:
         * RecordValue putMap(String fieldName, Map<String, ?> map)
         */
        RowImpl row = table.createRow();
        row.put(FieldId, 1);
        Map<String, List<Long>> mv1 = new HashMap<String, List<Long>>();
        Map<String, Map<String, Integer>> mv2 =
            new HashMap<String, Map<String, Integer>>();
        Map<String, Map<String, Object>> mv3 =
            new HashMap<String, Map<String,Object>>();
        final HashMap<String, Object> valuesMap =
            genValues(20, null, null, null);
        final String mapField = "mapField";
        List<Long> mv1_array = new ArrayList<Long>();
        Map<String, Integer> mv2_mapValue = new HashMap<String, Integer>();
        Map<String, Object> mv3_recValue = new HashMap<String, Object>();
        mv1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        mv2_mapValue.put((String)valuesMap.get(FieldString),
                         ((Integer)valuesMap.get(FieldInt)).intValue());
        mv3_recValue.put(FieldString, valuesMap.get(FieldString));
        mv3_recValue.put(FieldDouble, valuesMap.get(FieldDouble));
        mv1.put(mapField, mv1_array);
        mv2.put(mapField, mv2_mapValue);
        mv3.put(mapField, mv3_recValue);
        row.putMap(FieldMapArray, mv1);
        row.putMap(FieldMapMap, mv2);
        row.putMap(FieldMapRecord, mv3);

        /*
         * Create a row, put on map field using:
         * RecordValue putMap(String fieldName)
         */
        RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        MapValue mvVal1 = row2.putMap(FieldMapArray);
        MapValue mvVal2 = row2.putMap(FieldMapMap);
        MapValue mvVal3 = row2.putMap(FieldMapRecord);
        ArrayValue mvVal1_array = mvVal1.putArray(mapField);
        MapValue mvVal2_mapValue = mvVal2.putMap(mapField);
        RecordValue mvVal3_recValue = mvVal3.putRecord(mapField);
        mvVal1_array.add(((Long)valuesMap.get(FieldLong)).longValue());
        mvVal2_mapValue.put((String)valuesMap.get(FieldString),
                            ((Integer)valuesMap.get(FieldInt)).intValue());
        mvVal3_recValue.put(FieldString, (String)valuesMap.get(FieldString));
        mvVal3_recValue.put(FieldDouble,
                            ((Double)valuesMap.get(FieldDouble)).doubleValue());
        /* Verify */
        assertTrue(row.equals(row2));

        ArrayValue mv1_get = row.get(FieldMapArray).asMap().get(mapField)
            .asArray();
        assertEquals(1, mv1_get.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     mv1_get.get(0).asLong().get());
        MapValue mv2_get = row.get(FieldMapMap).asMap().get(mapField).asMap();
        assertEquals(1, mv2_get.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     mv2_get.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        RecordValue mv3_get =
            row.get(FieldMapRecord).asMap().get(mapField).asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(mv3_get.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                     mv3_get.get(FieldDouble).asDouble().get(), DELTA);
    }

    /*
     * Test on record{simple types}
     */
    @Test
    public void testRecordFieldPutGet() {

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
            .addEnum(FieldEnum, enumValues, null, null, null);
        TableBuilderBase tb =
            TableBuilder.createTableBuilder(null, tblName, null, null, null)
            .addInteger(FieldId)
            .addField(FieldRecord, rb.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        final TableImpl table = tb.buildTable();

        /*
         * Create a row, put on record field using:
         * RecordValue putRecord(String fieldName, Map<String, ?> map)
         */
        final int bytesLen = 128;
        final int strLen = 20;
        RowImpl row = table.createRow();
        HashMap<String, Object> valuesMap =
            genValues(strLen, bytesLen, fixedLen, enumValues);
        row.put(FieldId, 1);
        Map<String, Object> rv = new HashMap<String, Object>();
        /* When field type doesn't match with value type, throw exception */
        rv.put(FieldInt, "1");
        try {
            row.putRecord(FieldRecord, rv);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        rv.put(FieldInt, 1L);
        try {
            row.putRecord(FieldRecord, rv);
            fail("Excepted to get Exception but OK.");
        } catch (IllegalArgumentException e) {
        }
        rv.put(FieldInt, valuesMap.get(FieldInt));
        rv.put(FieldLong, valuesMap.get(FieldLong));
        rv.put(FieldDouble, valuesMap.get(FieldDouble));
        rv.put(FieldFloat, valuesMap.get(FieldFloat));
        rv.put(FieldString, valuesMap.get(FieldString));
        rv.put(FieldBoolean, valuesMap.get(FieldBoolean));
        rv.put(FieldBinary, valuesMap.get(FieldBinary));
        rv.put(FieldFixedBinary, valuesMap.get(FieldFixedBinary));
        rv.put(FieldEnum, valuesMap.get(FieldEnum));
        row.putRecord(FieldRecord, rv);

        /*
         * Put on record field using:
         * RecordValue putRecord(String fieldName)
         */
        RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        RecordValue rvVal = row2.putRecord(FieldRecord);
        rvVal.put(FieldInt, ((Integer)valuesMap.get(FieldInt)).intValue());
        rvVal.put(FieldLong, ((Long)valuesMap.get(FieldLong)).longValue());
        rvVal.put(FieldDouble,
                  ((Double)valuesMap.get(FieldDouble)).doubleValue());
        rvVal.put(FieldFloat,
                  ((Float)valuesMap.get(FieldFloat)).floatValue());
        rvVal.put(FieldString, (String)valuesMap.get(FieldString));
        rvVal.put(FieldBoolean,
                  ((Boolean)valuesMap.get(FieldBoolean)).booleanValue());
        rvVal.put(FieldBinary, (byte[])valuesMap.get(FieldBinary));
        rvVal.putFixed(FieldFixedBinary,
                       (byte[])valuesMap.get(FieldFixedBinary));
        rvVal.putEnum(FieldEnum, (String)valuesMap.get(FieldEnum));

        /* Verify */
        assertTrue(row.equals(row2));

        rvVal = row.get(FieldRecord).asRecord();
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     rvVal.get(FieldInt).asInteger().get());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     rvVal.get(FieldLong).asLong().get());
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                     rvVal.get(FieldDouble).asDouble().get(), DELTA);
        assertEquals(((Float)valuesMap.get(FieldFloat)).floatValue(),
                     rvVal.get(FieldFloat).asFloat().get(), (float)DELTA);
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(rvVal.get(FieldString).asString().get()));
        assertEquals(((Boolean)valuesMap.get(FieldBoolean)).booleanValue(),
                     rvVal.get(FieldBoolean).asBoolean().get());
        assertTrue(
            Arrays.equals((byte[])valuesMap.get(FieldBinary),
                          rvVal.get(FieldBinary).asBinary().get()));
        assertTrue(((String)valuesMap.get(FieldEnum))
                    .equals(rvVal.get(FieldEnum).asEnum().get()));
    }

    /*
     * Test on record{map, array, record}
     */
    @Test
    public void testRecordFieldNestedPutGet() {

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
            null, tblName, null, null, null)
            .addInteger(FieldId)
            .addField(FieldRecordAll, rb1.build())
            .primaryKey(FieldId)
            .shardKey(FieldId);
        TableImpl table = tb.buildTable();

        /*
         * Create a row, put on record field using:
         * RecordValue putRecord(String fieldName, Map<String, ?> map)
         */
        RowImpl row = table.createRow();
        row.put(FieldId, 1);
        final HashMap<String, Object> valuesMap =
            genValues(20, null, null, null);
        Map<String, Object> rv = new HashMap<String, Object>();
        List<Long> rv_av = new ArrayList<Long>();
        Map<String, Integer> rv_mv = new HashMap<String, Integer>();
        Map<String, Object> rv_rv = new HashMap<String, Object>();
        rv_av.add(((Long)valuesMap.get(FieldLong)).longValue());
        rv_mv.put((String)valuesMap.get(FieldString),
                  ((Integer)valuesMap.get(FieldInt)).intValue());
        rv_rv.put(FieldString, valuesMap.get(FieldString));
        rv_rv.put(FieldDouble, valuesMap.get(FieldDouble));
        rv.put(FieldArray, rv_av);
        rv.put(FieldMap, rv_mv);
        rv.put(FieldRecord, rv_rv);
        row.putRecord(FieldRecordAll, rv);

        /*
         * Put on record field using:
         * RecordValue putRecord(String fieldName)
         */
        RowImpl row2 = table.createRow();
        row2.put(FieldId, 1);
        RecordValue rvVal = row2.putRecord(FieldRecordAll);
        ArrayValue rvVal_av = rvVal.putArray(FieldArray);
        MapValue rvVal_mv = rvVal.putMap(FieldMap);
        RecordValue rvVal_rv = rvVal.putRecord(FieldRecord);

        rvVal_av.add(((Long)valuesMap.get(FieldLong)).longValue());
        rvVal_mv.put((String)valuesMap.get(FieldString),
                     ((Integer)valuesMap.get(FieldInt)).intValue());
        rvVal_rv.put(FieldString, (String)valuesMap.get(FieldString));
        rvVal_rv.put(FieldDouble,
            ((Double) valuesMap.get(FieldDouble)).doubleValue());

        /* Verify */
        assertTrue(row.equals(row2));

        RecordValue rv_get = row.get(FieldRecordAll).asRecord();
        ArrayValue rv_av_get = rv_get.get(FieldArray).asArray();
        assertEquals(1, rv_av_get.size());
        assertEquals(((Long)valuesMap.get(FieldLong)).longValue(),
                     rv_av_get.get(0).asLong().get());
        MapValue rv_mv_get = rv_get.get(FieldMap).asMap();
        assertEquals(1, rv_mv_get.size());
        assertEquals(((Integer)valuesMap.get(FieldInt)).intValue(),
                     rv_mv_get.get(((String)valuesMap.get(FieldString)))
                     .asInteger().get());
        RecordValue rv_rv_get = rv_get.get(FieldRecord).asRecord();
        assertTrue(((String)valuesMap.get(FieldString))
                   .equals(rv_rv_get.get(FieldString).asString().get()));
        assertEquals(((Double)valuesMap.get(FieldDouble)).doubleValue(),
                    rv_rv_get.get(FieldDouble).asDouble().get(), DELTA);
    }

    /*
     * Make sure Maps are case-sensitive.
     */
    @Test
    public void testMapCaseSensitive() {
        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addInteger();
        MapDef mapDef = mb.build();

        MapValue map = mapDef.createMap();

        map.put("a", 1);
        map.put("A", 2);
        map.put("abcde", 6);
        map.put("ABcdE", 7);
        assertTrue(map.size() == 4);
        assertNotNull(map.get("a"));
        assertNotNull(map.get("A"));
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

    private HashMap<String, Object> genValues(Integer stringLen,
                                              Integer bytesLen,
                                              Integer fixedLen,
                                              String[] enumValues) {
        HashMap<String, Object> values = new HashMap<String, Object>();
        Long seed = getRandomSeed();
        final Random rand = new Random(seed);
        values.put(FieldId, seed);
        values.put(FieldInt, rand.nextInt());
        values.put(FieldLong, rand.nextLong());
        values.put(FieldDouble, rand.nextDouble());
        values.put(FieldFloat, rand.nextFloat());
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
        return values;
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
}
