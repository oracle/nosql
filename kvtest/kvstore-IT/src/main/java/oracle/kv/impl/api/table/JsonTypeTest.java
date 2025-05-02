/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.TestBase;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import org.junit.Test;


public class JsonTypeTest extends TestBase {

    TableImpl userTable;

    final static boolean verbose = false;

    @Test
    public void basic() throws Exception {
        final EnumDefImpl enumDef =
            new EnumDefImpl("test",
                            new String[] {"a", "b", "c"});
        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name")
            .addJson("json", null)
            .build();

        RecordValue val = recordDef.createRecord();
        val.put("name", "joe");

        /*
         * Integer
         */
        val.put("json", 1);
        SerializationTest.roundTrip(val);

        /*
         * Array
         */
        ArrayValue array = val.putArray("json");
        array.add(5);
        array.add(4.6);
        array.add("this is a string");
        SerializationTest.roundTrip(val);

        /*
         * Nested array
         */
        ArrayValue nestedArray = array.addArray();
        nestedArray.add("str").add(6L).add(true)
            .add(FieldValueFactory.createValueFromJson(
                     "{\"a\":\"b\", \"b\":1234, \"c\":[1,2,3,4,5]}"));
        SerializationTest.roundTrip(val);

        /*
         * Homogenous array
         */
        ArrayValueImpl hArray = FieldDefImpl.Constants.jsonDef.createArray();
        hArray.add(5);
        hArray.add(6);
        hArray.add(7);
        SerializationTest.roundTrip(hArray);

        /*
         * Used the FieldValue to creat a map, which will be a Map<JSON>
         */
        MapValue map = val.putMap("json");
        map.put("anint", 4);
        map.put("adouble", 5.6);
        map.put("astring", "this is a string");
        map.put("json", FieldValueFactory.createValueFromJson(
                     "{\"a\":\"b\", \"b\":1234, \"c\":[1,2,3,4,5]}"));
        SerializationTest.roundTrip(val);

        /*
         * Nested Map<JSON>
         */
        MapValue nestedMap = map.putMap("nestedMap");
        nestedMap.put("nestedInt", 5);
        nestedMap.put("nestedString", "s");
        SerializationTest.roundTrip(val);

        /*
         * Try a number of fixed-schema types and verify that they fail
         * to be put into JSON.
         */
        recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name").build();
        RecordValue record = recordDef.createRecord().put("name", "joe");
        try {
            nestedArray.add(record);
            fail("Add should have failed");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedMap.put("x", record);
            fail("Put should have failed");
        } catch (IllegalArgumentException iae) {}

        /*
         * Same thing with a fixed-schema map
         */
        MapDefImpl mapDef =
            FieldDefFactory.createMapDef(FieldDefImpl.Constants.integerDef);
        map = mapDef.createMap().put("i", 1);

        try {
            val.put("json", FieldDefImpl.Constants.mapAnyDef.createMap());
            fail("Should not be able to put map of ANY in JSON field");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedMap.put("x", FieldDefImpl.Constants.mapAnyDef.createMap());
            fail("Put should have failed");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedArray.add(FieldDefImpl.Constants.mapAnyDef.createMap());
            fail("Add should have failed");
        } catch (IllegalArgumentException iae) {}

        try {
            val.put("json", FieldDefImpl.Constants.arrayAnyDef.createArray());
            fail("Should not be able to put an array of ANY in JSON field");
        } catch (IllegalArgumentException iae) {}

        /*
         * Try some invalid simpler types
         */
        try {
            nestedArray.add(new byte[1]);
            fail("Should not be able to insert binary into JSON");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedMap.put("x", new byte[1]);
            fail("Should not be able to insert binary into JSON");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedArray.add(enumDef.createEnum("a"));
            fail("Should not be able to insert enum into JSON");
        } catch (IllegalArgumentException iae) {}

        try {
            nestedMap.put("x", enumDef.createEnum("a"));
            fail("Should not be able to insert enum into JSON");
        } catch (IllegalArgumentException iae) {}
    }

    /*
     * Test various cases handling JSON null in maps, arrays, and
     * otherwise.
     * o JSON null in map, array of JSON
     * o disallow SQL null in map array of JSON
     * o construction of JSON null
     */
    @Test
    public void testJsonNull() throws Exception {
        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name")
            .addJson("json", null)
            .build();

        /* JSON null in map */
        RecordValue rval = recordDef.createRecord();
        rval.put("name", "a");
        MapValue map = rval.putMap("json");
        map.putJsonNull("nullField");
        assertJsonNull(map.get("nullField"));
        SerializationTest.roundTrip(rval);

        /* JSON null in array */
        ArrayValue array  = rval.putArray("json");
        array.addJsonNull();
        array.addJsonNull(0);
        array.setJsonNull(1);
        array.add(FieldValueFactory.createJsonNull());
        for (int i = 0; i < 3; i++) {
            assertJsonNull(array.get(i));
        }
        SerializationTest.roundTrip(rval);

        /* direct JSON null usage */
        rval.put("json", FieldValueFactory.createJsonNull());
        SerializationTest.roundTrip(rval);

        /*
         * This will also work because the "json" field in the record value is
         * allowed to be null (SQL null).
         */
        rval.put("json", NullValueImpl.getInstance());
        SerializationTest.roundTrip(rval);

        /* Test SQL null, which should fail */
        try {
            map.put("somefield", NullValueImpl.getInstance());
            fail("SQL null should fail");
        } catch (IllegalArgumentException iae) {
        }
        try {
            array.add(NullValueImpl.getInstance());
            fail("SQL null should fail");
        } catch (IllegalArgumentException iae) {
        }
    }

    static private void assertJsonNull(FieldValue val) {
        assertTrue("Should be JSON null", val.isJsonNull());
        assertFalse("Should be not be SQL null", val.isNull());
    }

    @Test
    public void testHomogeneousArrays() throws Exception {

        FieldValue stringValue = FieldValueFactory.createString("s");
        FieldValue intValue = FieldValueFactory.createInteger(5000);
        FieldValue floatValue = FieldValueFactory.createFloat(5000f);

        /*
         * Homogenous array, receiver does not have type info
         */
        ArrayValueImpl hArray = FieldDefImpl.Constants.jsonDef.createArray();
        hArray.add(5);
        hArray.add(6);
        hArray.add(7);
        assertTrue(hArray.isHomogeneous());
        assertTrue(hArray.getHomogeneousType().isInteger());
        SerializationTest.roundTrip(hArray);

        hArray.add(intValue);
        hArray.set(0, intValue);
        hArray.set(2, 3);
        hArray.add(1, intValue);
        assertTrue(hArray.isHomogeneous());
        assertTrue(hArray.getHomogeneousType().isInteger());
        SerializationTest.roundTrip(hArray);

        hArray.add("not_like_the_others");
        assertFalse(hArray.isHomogeneous());
        assertTrue(hArray.getElementDef().isJson());
        SerializationTest.roundTrip(hArray);

        /* test that float is converted to double */
        hArray = FieldDefImpl.Constants.jsonDef.createArray();
        hArray.add(5f);
        hArray.add(6f);
        hArray.add(7f);
        assertTrue(hArray.isHomogeneous());
        assertTrue(hArray.getHomogeneousType().isDouble());
        SerializationTest.roundTrip(hArray);

        hArray.add(floatValue);
        hArray.set(0, floatValue);
        hArray.set(2, 3f);
        hArray.add(1, floatValue);
        assertTrue(hArray.isHomogeneous());
        assertTrue(hArray.getHomogeneousType().isDouble());
        SerializationTest.roundTrip(hArray);

        hArray.add("not_like_the_others");
        assertFalse(hArray.isHomogeneous());
        assertTrue(hArray.getElementDef().isJson());
        SerializationTest.roundTrip(hArray);

        /*
         * Homogenous array, receiver expects ARRAY(JSON)
         */
        hArray = FieldDefImpl.Constants.jsonDef.createArray();
        hArray.add(5);
        hArray.add(6);
        hArray.add(7);

        SerializationTest.roundTrip2(hArray);

        hArray.add(1, stringValue);
        assertFalse(hArray.isHomogeneous());

        SerializationTest.roundTrip2(hArray);

        /*
         * Try with an empty json array
         */
        hArray = FieldDefImpl.Constants.jsonDef.createArray();
        assertFalse(hArray.isHomogeneous());
        assertTrue(hArray.getElementDef().isJson());
        SerializationTest.roundTrip(hArray);
        SerializationTest.roundTrip2(hArray);

        /*
         * Array of ANY
         */
        hArray = FieldDefImpl.Constants.anyDef.createArray();
        hArray.add(5);
        hArray.add(6);
        hArray.add(7);
        assertTrue(hArray.isHomogeneous());
        assertTrue(hArray.getHomogeneousType().isInteger());
        SerializationTest.roundTrip(hArray);
        hArray.add("not_like_the_others");
        assertFalse(hArray.isHomogeneous());
        assertTrue(hArray.getElementDef().isAny());
        SerializationTest.roundTrip(hArray);

        /*
         * Try with an empty ANY array
         */
        hArray = FieldDefImpl.Constants.anyDef.createArray();
        assertFalse(hArray.isHomogeneous());
        assertTrue(hArray.getElementDef().isAny());
        SerializationTest.roundTrip(hArray);
        SerializationTest.roundTrip2(hArray);
    }

    @Test
    public void testJsonInput() throws Exception {

        final String json1 = "{\"a\": 1, \"b\": null, \"bool\": true," +
            "\"map\": {\"m1\": 5}, \"ar\" : [1,2.7,3]}";

        final String json2 = "{\"a\": 1, \"b\": 2, \"map\":{\"m1\":6}}";
        final String json3 = "[1,2.7,null,3]";
        final String json4 = "null";
        final String json5 = "6";
        final String json6 =
            "{\"a\": 92345678901234}";
        final String json7 = "{\"a\": 1, \"b\": 2, \"A\":3}";
        final String json8 = "{\"a\": 1, \"a\": 2, \"A\":3}";
        final String json9 =
            "{\"a\": 9234567890123456789012345777777555555555888888}";
        final String json10 = "923456789012345678901234577777755.5555555888888";

        /* bad json */
        final String bjson1 = "{\"a\": [}";
        final String bjson2 = null;

        final String[] jsonDocs = {json1, json2, json3, json4,
                                   json5, json6, json7, json8,
                                   json9, json10};
        /* TBD, when Number is supported in query
           json9, json10}; */

        final String[] badJsonDocs = {bjson1, bjson2};

        /*
         * Standalone case to test case-sensitivity
         */
        FieldValue val1 = FieldValueFactory.createValueFromJson(
            FieldDefImpl.Constants.jsonDef, json7);
        assertTrue(((MapValue) val1).size() == 3);

        for (String json : jsonDocs) {
            FieldValue val = FieldValueFactory.createValueFromJson(
                FieldDefImpl.Constants.jsonDef, json);
            SerializationTest.roundTrip(val);
        }

        for (String json : badJsonDocs) {
            try {
                FieldValueFactory.createValueFromJson(
                    FieldDefImpl.Constants.jsonDef, json);
                fail("JSON should have failed: " + json);
            } catch (IllegalArgumentException iae) {}
        }

        /*
         * TODO: generate JSON and implement a JSON equality method. String
         * comparisons are not helpful. The issue will be round-tripping of
         * numbers.
         */
    }

    /*
     * Tests some odd strings that are JSON and/or not JSON and make sure that
     * the JSON processing in the system (ours and Jackson) does the right thing.
     *
     * If additional, odd use cases can be created, they can be added here.
     */
    @Test
    public void testJsonInjection() throws Exception {

        final String json1 = "{\"a\": 1, \"b\": null, \"bool\": true," +
            "\"map\": {\"m1\": 5}, \"ar\" : [1,2.7,3]}";

        final String json2 = "{\"a\": 1, \"b\": 2, \"map\":{\"m1\":6}}";
        final String json3 = "[1,2.7,null,3]";
        final String json4 = "null";
        final String json5 = "{\"a\": \"1, \'\"b\": 2, \"map\":{\"m1\":6}}";

        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        sb.append("\"");
        sb.append("\'");
        sb.append('"');
        sb.append(120);
        sb.append((char)12);
        final String json6 = sb.toString();
        final String[] strings = {json1, json2, json3, json4, json5, json6};

        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name")
            .addString("jsonInString")
            .addJson("json", null)
            .build();


        RecordValue rval = recordDef.createRecord();
        rval.put("name", "a_name");
        rval.put("json", 4);
        for (String s : strings) {
            /* put the string in a String field */
            rval.put("jsonInString", s);
            /* put the string in a JSON field, but as a String */
            rval.put("json", s);
            SerializationTest.roundTrip(rval);
            roundTripAsJson(rval);
        }
    }

    /*
     * Test that the record can be turned into JSON and back without issues
     */
    private static void roundTripAsJson(RecordValue record) {
        String jsonString = record.toString();
        RecordValue newRecord =
            FieldValueFactory.createValueFromJson(record.getDefinition(),
                                                  jsonString).asRecord();
        assertEquals("Records should be equal", newRecord, record);
    }

    /*
     * NOTE: at this time (4.2 development) there is no public mechanism for
     * creating or defining a collection (map or array) of. The mechanism tested
     * here is entirely internal. The code is in place for the possibility that
     * the query engine might need these or future enhancement of the API.
     */
    @Test
    public void testMapAny() throws Exception {
        MapValue map = FieldDefImpl.Constants.mapAnyDef.createMap();
        map.put("a", 1);
        map.put("b", "str");

        /* add a record */
        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name").build();
        RecordValue record = recordDef.createRecord().put("name", "joe");
        map.put("record", record);
    }

    /*
     * See NOTE on testMapAny about support for collections of ANY
     */
    @Test
    public void testArrayAny() throws Exception {
        ArrayValue array = FieldDefImpl.Constants.arrayAnyDef.createArray();
        array.add(1);
        array.add("str");

        /* add a record */
        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("record")
            .addString("name").build();
        RecordValue record = recordDef.createRecord().put("name", "joe");
        array.add(record);

        /* add a JSON type to the array */
        final String json = "{\"a\": 1, \"b\": 2, \"map\":{\"m1\":6}}";
        FieldValue jsonVal =
            FieldValueFactory.createValueFromJson(
                FieldDefImpl.Constants.jsonDef, json);
        array.add(jsonVal);
    }

    /* not private to avoid warnings */
    static void verbose(String output) {
        if (verbose) {
            System.out.println(output);
        }
    }
}
