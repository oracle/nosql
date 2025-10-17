/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static oracle.nosql.proxy.protocol.JsonProtocol.ON_DEMAND;
import static oracle.nosql.proxy.protocol.JsonProtocol.PROVISIONED;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.TimestampValue;
import oracle.nosql.common.JsonBuilder;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.JsonProtocol;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonArray;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonObject;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonPayload;

/**
 * Test JSON payload parser and builder
 */
public class JsonPayloadTest {

    @Test
    public void testJsonPayload() throws Exception {
        int id = 1;
        boolean ifNotExists = true;
        String compartmentId = "ocid1.compartment.oc1..aaaaaaaagaqos5k";
        String ddlStatement = "create table if not exists foo1(" +
                                "id integer, name string, age integer, " +
                                "info json, primary key(id))";
        int maxReadUnits = 100;
        int maxWriteUnits = 200;
        int maxStorageInGBs = 3;
        String capacityMode = PROVISIONED;
        String[] states = new String[] {
            "CREATING", "ACTIVE", "UPDATING"
        };

        String fmt = "{" +
            "  \"id\": %s, " +
            "  \"ifNotExists\": %s, " +
            "  \"compartmentId\": %s," +
            "  \"ddlStatement\": %s," +
            "  \"tableLimits\": {" +
            "    \"maxReadUnits\": %s," +
            "    \"maxWriteUnits\": %s," +
            "    \"maxStorageInGBs\": %s," +
            "    \"capacityMode\": %s" +
            "  }," +
            "  \"state\": [%s, %s, %s]" +
            "}";

        String[] jsons = new String[3];

        jsons[0] = String.format(fmt, String.valueOf(id),
                                 String.valueOf(ifNotExists),
                                 appendQuotes(compartmentId),
                                 appendQuotes(ddlStatement),
                                 String.valueOf(maxReadUnits),
                                 String.valueOf(maxWriteUnits),
                                 String.valueOf(maxStorageInGBs),
                                 appendQuotes(capacityMode),
                                 appendQuotes(states[0]),
                                 appendQuotes(states[1]),
                                 appendQuotes(states[2]));

        /* Field values are all string */
        jsons[1] = String.format(fmt, appendQuotes(String.valueOf(id)),
                                 appendQuotes(String.valueOf(ifNotExists)),
                                 appendQuotes(compartmentId),
                                 appendQuotes(ddlStatement),
                                 appendQuotes(String.valueOf(maxReadUnits)),
                                 appendQuotes(String.valueOf(maxWriteUnits)),
                                 appendQuotes(String.valueOf(maxStorageInGBs)),
                                 appendQuotes(capacityMode),
                                 appendQuotes(states[0]),
                                 appendQuotes(states[1]),
                                 appendQuotes(states[2]));

        /* Field name in lower case */
        jsons[2] = String.format(fmt.toLowerCase(), String.valueOf(id),
                                 String.valueOf(ifNotExists),
                                 appendQuotes(compartmentId),
                                 appendQuotes(ddlStatement),
                                 String.valueOf(maxReadUnits),
                                 String.valueOf(maxWriteUnits),
                                 String.valueOf(maxStorageInGBs),
                                 appendQuotes(capacityMode),
                                 appendQuotes(states[0]),
                                 appendQuotes(states[1]),
                                 appendQuotes(states[2]));

        JsonPayload pl;
        for (String json : jsons) {
            validateJson(json);
            pl = new JsonPayload(json);
            while (pl.hasNext()) {
                if (pl.isField("id")) {
                    assertEquals(id, pl.readInt());
                } else if (pl.isField("ifNotExists")) {
                    assertEquals(ifNotExists, pl.readBool());
                } else if (pl.isField("compartmentId")) {
                    assertEquals(compartmentId, pl.readString());
                } else if (pl.isField("ddlStatement")) {
                    assertEquals(ddlStatement, pl.readString());
                } else if (pl.isField("tableLimits")) {
                    JsonObject jo = pl.readObject();
                    while (jo.hasNext()) {
                        if (jo.isField("maxReadUnits")) {
                            assertEquals(maxReadUnits, jo.readInt());
                        } else if (jo.isField("maxWriteUnits")) {
                            assertEquals(maxWriteUnits, jo.readInt());
                        } else if (jo.isField("maxStorageInGBs")) {
                            assertEquals(maxStorageInGBs, jo.readInt());
                        } else if (jo.isField("capacityMode")) {
                            assertEquals(capacityMode, jo.readString());
                        } else {
                            fail("Unexpected field: " + jo.getCurrentField());
                        }
                    }
                } else if (pl.isField("state")) {
                    JsonArray ja = pl.readArray();
                    int i = 0;
                    while (ja.hasNext()) {
                        String value = ja.readString();
                        if (i > 3) {
                            fail("Unexpected value: " + value);
                        }
                        assertEquals(states[i++], value);
                    }
                    assertEquals(3, i);
                } else {
                    fail("Unexpected field: " + pl.getCurrentField());
                }
            }
            pl.close();
        }
    }

    @Test
    public void testJsonPayloadSkipValue() throws Exception {
        final String[] allFields = new String[] {
            "name", "ifNotExists", "compartmentId", "count",
            "tableLimits", "state"
        };
        final String payload = "{" +
            "  \"name\": \"users\", " +
            "  \"ifNotExists\": true, " +
            "  \"compartmentId\": \"testCompartment\"," +
            "  \"count\": 1, " +
            "  \"tableLimits\": {" +
            "    \"maxReadUnits\": 100," +
            "    \"maxWriteUnits\": 100," +
            "    \"maxStorageInGBs\": 1" +
            "  }," +
            "  \"state\": [\"ACTIVE\", \"INACTIVE\"]" +
            "}";
        validateJson(payload);

        JsonPayload pl = new JsonPayload(payload);

        /* Skip all fields */
        List<String> fields = new ArrayList<>();
        while (pl.hasNext()) {
            fields.add(pl.getCurrentField());
            pl.skipValue();
        }
        pl.close();

        assertEquals(Arrays.asList(allFields), fields);

        /* Skip unknown fields */
        pl = new JsonPayload(payload);
        while (pl.hasNext()) {
            if (pl.isField("name")) {
                assertEquals("users", pl.readString());
            } else if (pl.isField("ifNotExists")) {
                assertEquals(true, pl.readBool());
            } else if (pl.isField("compartmentId")) {
                assertEquals("testCompartment", pl.readString());
            } else {
                pl.skipValue();
            }
        }
        pl.close();
    }

    @Test
    public void testEmptyObjectArray() {
        JsonBuilder jb = JsonBuilder.create();
        String json = jb.startObject("o")
                           .startArray("a1")
                              .startArray(null)
                              .endArray()
                           .endArray()
                           .append("id", 1)
                        .endObject()
                        .startArray("a")
                           .startObject(null)
                               .startArray("a1")
                               .endArray()
                               .append("id", 1)
                           .endObject()
                        .endArray()
                        .append("id", 1)
                        .toString();
        validateJson(json);
    }

    @Test
    public void testTypeCasting() throws Exception {
        JsonPayload pl;

        /* Integer */
        String json = "{\"field\": 1}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        assertEquals("field", pl.getCurrentField());
        assertEquals(1, pl.readInt());
        assertEquals("1", pl.readString());
        try {
            pl.readBool();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readArray();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readObject();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        pl.close();

        /* Boolean */
        json = "{\"field\": false}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        assertEquals(false, pl.readBool());
        assertEquals("false", pl.readString());
        try {
            pl.readInt();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readArray();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readObject();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        pl.close();

        /* Integer overflow */
        json = "{\"field\": " + Long.MAX_VALUE + "}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        try {
            pl.readInt();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        pl.close();

        /* String */
        json = "{\"field\": \"abc\"}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        assertEquals(false, pl.readBool());
        assertEquals("abc", pl.readString());
        try {
            pl.readInt();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readArray();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readObject();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        pl.close();

        json = "{\"field\": \"100\"}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        assertEquals(100, pl.readInt());
        pl.close();

        /* Array type */
        json = "{\"field\": [1, 2, 3]}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        try {
            pl.readBool();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readString();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readInt();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readObject();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        JsonArray ja = pl.readArray();
        int i = 0;
        while (ja.hasNext()) {
            assertEquals(++i, ja.readInt());
        }
        pl.close();

        /* Object type */
        json = "{\"field\": {\"key\":1}}";
        pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        try {
            pl.readBool();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readString();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readInt();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        try {
            pl.readArray();
            fail("Expect to fail but not");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
        JsonObject jo = pl.readObject();
        assertTrue(jo.hasNext());
        assertEquals("key", jo.getCurrentField());
        assertEquals(1, jo.readInt());
        pl.close();
    }

    @Test
    public void testNullValue() throws IOException {
        String json = "{\"i\": null, " +
                       "\"s\": null, " +
                       "\"b\": null, " +
                       "\"ar\": null, " +
                       "\"obj\":null}";
        JsonPayload pl = new JsonPayload(json);
        while (pl.hasNext()) {
            if (pl.isField("i")) {
                assertEquals(0, pl.readInt());
            } else if (pl.isField("b")) {
                assertFalse(pl.readBool());
            } else if (pl.isField("s")) {
                assertNull(pl.readString());
            } else if (pl.isField("ar")) {
                assertNull(pl.readArray());
            } else if (pl.isField("obj")) {
                assertNull(pl.readObject());
            } else {
                fail("Unexpected field: " + pl.getCurrentField());
            }
        }
		pl.close();
    }

    /* Test JsonPayload.readValueAsJson() */
    @Test
    public void testReadValueAsJson() throws Exception {
        String json = "{\n" +
            "  \"compartmentId\": \"ocid.iad.xxx\"," +
            "  \"id\": 1," +
            "  \"phone\": [123, 456]," +
            "  \"name\": \"Jack Ma\"," +
            "  \"address\": {" +
            "    \"info\": {" +
            "      \"state\":\"MA\"," +
            "      \"city\":\"Burlington\"," +
            "      \"street\":\"35 network drive\"" +
            "    }," +
            "    \"type\": \"WORK\"," +
            "    \"zipCode\":\"01803\"," +
            "    \"lot\": 123456.7," +
            "    \"inUsed\": true" +
            "  }," +
            "  \"option\": \"IF_ABSENT\"," +
            "  \"isGetReturnRow\": false," +
            "  \"timeoutInMs\": 5000" +
            "}";

        JsonPayload pl = new JsonPayload(json);
        runReadValueAsJsonTest(pl);
        pl.close();

        ByteBuf buf = Unpooled.wrappedBuffer(json.getBytes());
        pl = new JsonPayload(new ByteInputStream(buf));
        runReadValueAsJsonTest(pl);
        pl.close();
    }

    private void runReadValueAsJsonTest(JsonPayload pl) throws IOException {
        String addressJson = null;
        String field = null;
        int n = 0;
        while (pl.hasNext()) {
            field = pl.getCurrentField();
            assertNotNull(field);
            if (field.equals("address")) {
                addressJson = pl.readValueAsJson();
            } else if (field.equals("phone")) {
                assertEquals("[123, 456]", pl.readValueAsJson());
            } else {
                assertNotNull(pl.readValue());
            }
            n++;
        }
        assertEquals(8, n);
        pl.close();

        assertNotNull(addressJson);
        pl = new JsonPayload(addressJson);
        assertTrue(pl.hasNext());
        assertEquals("info", pl.getCurrentField());
        assertTrue(pl.readValueAsJson()
                    .contains("\"street\":\"35 network drive\""));

        assertTrue(pl.hasNext());
        assertEquals("type", pl.getCurrentField());
        assertEquals("\"WORK\"", pl.readValueAsJson());

        assertTrue(pl.hasNext());
        assertEquals("zipCode", pl.getCurrentField());
        assertEquals("\"01803\"", pl.readValueAsJson());

        assertTrue(pl.hasNext());
        assertEquals("lot", pl.getCurrentField());
        assertEquals("123456.7", pl.readValueAsJson());

        assertTrue(pl.hasNext());
        assertEquals("inUsed", pl.getCurrentField());
        assertEquals("true", pl.readValueAsJson());

        assertFalse(pl.hasNext());
        pl.close();
    }

    private String appendQuotes(String str) {
        return "\"" + str + "\"";
    }

    private void validateJson(String json) {
        try {
            JsonUtils.parseJsonNode(json);
        } catch (Exception ex) {
            fail("Invalid json: " + json);
        }
    }

    @Test
    public void testJsonBuilder() throws IOException {

        int id = -1;
        boolean ifNotExists = true;
        String compartmentId = "ocid1.compartment.oc1..aaaaaaaagaqos5k";
        String ddlStatement = "create table if not exists foo1(\n" +
                                "\tid integer, \n" +
                                "\tname string default \"n/a\", \n" +
                                "age integer default -1,\n " +
                                "info json,\n" +
                                "primary key(id))";
        int maxReadUnits = 100;
        int maxWriteUnits = 200;
        int maxStorageInGBs = 3;
        String capacityMode = ON_DEMAND;
        String[] states = new String[] {
            "CREATING", "ACTIVE", "UPDATING"
        };

        JsonBuilder jb = JsonBuilder.create();
        jb.append("id", id);
        jb.append("ifNotExists", ifNotExists);
        jb.append("compartmentId", compartmentId);
        jb.append("ddlStatement", ddlStatement);
        jb.startObject("tableLimits");
        jb.append("maxReadUnits", maxReadUnits);
        jb.append("maxWriteUnits", maxWriteUnits);
        jb.append("maxStorageInGBs", maxStorageInGBs);
        jb.append("capacityMode", capacityMode);
        jb.endObject();
        jb.startArray("states");
        for (String state : states) {
            jb.append(state);
        }
        jb.endArray();

        String json = jb.toString();
        validateJson(json);

        JsonPayload pl = new JsonPayload(json);
        while(pl.hasNext()) {
            if (pl.isField("id")) {
                assertEquals(id, pl.readInt());
            } else if (pl.isField("ifNotExists")) {
                assertEquals(ifNotExists, pl.readBool());
            } else if (pl.isField("compartmentId")) {
                assertEquals(compartmentId, pl.readString());
            } else if (pl.isField("ddlStatement")) {
                assertEquals(ddlStatement, pl.readString());
            } else if (pl.isField("tableLimits")) {
                JsonObject jo = pl.readObject();
                while (jo.hasNext()) {
                    if (jo.isField("maxReadUnits")) {
                        assertEquals(maxReadUnits, jo.readInt());
                    } else if (jo.isField("maxWriteUnits")) {
                        assertEquals(maxWriteUnits, jo.readInt());
                    } else if (jo.isField("maxStorageInGBs")) {
                        assertEquals(maxStorageInGBs, jo.readInt());
                    } else if (jo.isField("capacityMode")) {
                        assertEquals(capacityMode, jo.readString());
                    } else {
                        fail("Unexpected field: " + jo.getCurrentField());
                    }
                }
            } else if (pl.isField("states")) {
                JsonArray ja = pl.readArray();
                int i = 0;
                while (ja.hasNext()) {
                    assertTrue(i < states.length);
                    assertEquals(states[i++], ja.readString());
                }
                assertEquals(states.length, i);
            } else {
                fail("Unexpected field: " + pl.getCurrentField());
            }
        }

        pl.close();
    }

    @Test
    public void testEscapeCharactor() throws Exception {
        final String value = "\tt\\\bb\"c\"\r\n";

        JsonBuilder jb = JsonBuilder.create();
        jb.append("key", value);
        String json = jb.toString();
        validateJson(json);

        JsonPayload pl = new JsonPayload(json);
        assertTrue(pl.hasNext());
        assertEquals("key", pl.getCurrentField());
        String value1 = pl.readString();
        assertEquals(value, value1);
        pl.close();
    }

    @Test
    public void testTagsToJson() throws Exception {
        /* freeform tags: Map<String, String> */
        Map<String, String> freeformTags = new HashMap<String, String>();
        freeformTags.put("createBy", "OracleNosql");
        freeformTags.put("accountType", "IAMUser");

        String json = JsonProtocol.tagsToJson(freeformTags);
        Map<?, ?> tags = JsonUtils.readValue(json, Map.class);
        assertEquals(freeformTags, tags);

        /*
         * predefined tags: Map<String, Map<String, Object>>
         *                  Object can be Integer, String or Boolean
         */
        Map<String, Map<String, Object>> definedTags = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put("Standby", true);
        props.put("Purpose", "WebTier");
        definedTags.put("Operations", props);
        props = new HashMap<>();
        props.put("Operator", "user1");
        props.put("number", 10.0);
        definedTags.put("ZOperations", props);

        json = JsonProtocol.tagsToJson(definedTags);
        tags = JsonUtils.readValue(json, Map.class);
        assertEquals(definedTags, tags);
    }

    /**
     * Test JsonProtocol.buildFieldValue(), it used to output FieldValue to Json.
     */
    @Test
    public void testBuildFieldValue() {
        TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addString("name")
            .addBoolean("valid")
            .addBinary("photo")
            .addFixedBinary("code", 16)
            .addLong("count")
            .addFloat("height")
            .addDouble("income")
            .addEnum("color", new String[]{"red", "yellow", "blue"}, null)
            .addNumber("storage")
            .addTimestamp("time", 3)
            .addField("ar",
                      TableBuilder.createArrayBuilder("ar")
                          .addField(TableBuilder.createRecordBuilder("rec")
                                        .addInteger("ari")
                                        .addString("ars")
                                        .build())
                             .build())
            .addField("ma", TableBuilder.createMapBuilder("ma")
                                .addField(TableBuilder.createArrayBuilder()
                                            .addInteger()
                                            .build())
                            .build())
            .addField("rma",
                      TableBuilder.createRecordBuilder("rma")
                          .addInteger("rid")
                          .addField("rm", TableBuilder.createMapBuilder()
                                            .addString()
                                            .build())
                          .addField("ra", TableBuilder.createArrayBuilder()
                                            .addTimestamp(6)
                                            .build())
                          .build())
            .primaryKey("id")
            .buildTable();

        /* Test null values */
        RowImpl row = table.createRow();
        row.put("id", 1);
        row.addMissingFields();
        roundTrip(row);

        row.clear();
        row.put("id", 2);
        row.put("name","name1");
        row.put("valid", true);
        row.put("photo", "this is a phone".getBytes());
        row.putFixed("code", genBytes(16));
        row.put("count", 1234567890123456789L);
        row.put("height", (float)12.3);
        row.put("income", 11313213.123412414);
        row.putEnum("color", "blue");
        row.putNumber("storage", new BigDecimal("9999999999999999999999"));
        row.put("time", new Timestamp(System.currentTimeMillis()));

        ArrayValue av = row.putArray("ar");
        RecordValue rv = av.addRecord();
        rv.put("ari", -1000);
        rv.put("ars", "hello ndcs");
        rv = av.addRecord();
        rv.put("ari", 1001);
        rv.put("ars", "hello oci");

        MapValue mv = row.putMap("ma");
        av = mv.putArray("k1");
        av.add(1).add(200).add(-200);

        RecordValue rma = row.putRecord("rma");
        rma.put("rid", 1);
        mv = rma.putMap("rm");
        mv.put("k1", "v1").put("k2", "v2").put("k3", "");
        av = rma.putArray("ra");
        long ms = System.currentTimeMillis();
        for (int i = 0; i < 3; i++ ) {
            Timestamp ts = new Timestamp(ms + i);
            av.add(ts);
        }
        roundTrip(row);

        /* Test timestamp string format */
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ts.setNanos(987654321);
        TimestampValue tsv;
        TimestampValue tsv1;
        String dateStr;
        for (int i = 0; i < 10; i++) {
            JsonBuilder jb = JsonBuilder.create();
            tsv = FieldValueFactory.createTimestamp(ts, i);

            /* Build Json string for TimestampValue */
            JsonProtocol.buildFieldValue(jb, null, tsv);
            dateStr = jb.toString();

            /* Remove '{' and '}' surrounded the timestamp string */
            dateStr = dateStr.substring(1, dateStr.length() - 1);
            assertTrue(dateStr.endsWith("Z\""));

            /* Create TimestampValue from the Json String */
            tsv1 = FieldValueFactory.createValueFromJson(
                    tsv.getDefinition(), dateStr).asTimestamp();
            assertEquals(tsv, tsv1);
        }

        /* Test json null */
        table = TableBuilder.createTableBuilder("foo")
                    .addInteger("id")
                    .addJson("info", null)
                    .primaryKey("id")
                    .buildTable();
        row = table.createRow();
        row.put("id", 1);
        row.putJson("info",
                    "{" +
                      "\"name\": null, " +
                      "\"phones\":[123, null, 456], " +
                      "\"addresses\": [" +
                        "{\"city\": \"boston\", \"zipcode\": null}, " +
                        "null," +
                        "{\"city\": null, \"zipcode\":01803}" +
                      "]" +
                    "}");
        roundTrip(row);

        /*
         * Test the 3 double special values NaN, Infinity and -Infinity are
         * strings in the returning json of JsonProtocol.buildFieldValue().
         */
        table = TableBuilder.createTableBuilder("foo")
                .addInteger("id")
                .addDouble("d0")
                .addDouble("d1")
                .addDouble("d2")
                .addDouble("d3")
                .primaryKey("id")
                .buildTable();
        row = table.createRow();
        row.put("id", 1)
            .put("d0", Double.MAX_VALUE)
            .put("d1", Double.NaN)
            .put("d2", Double.POSITIVE_INFINITY)
            .put("d3", Double.NEGATIVE_INFINITY);

        JsonBuilder jb = JsonBuilder.create();
        JsonProtocol.buildFieldValue(jb, null, row);
        String json = jb.toString();

        assertTrue(json.contains("\"d0\":" + Double.MAX_VALUE));
        assertTrue(json.contains("\"d1\":\"NaN\""));
        assertTrue(json.contains("\"d2\":\"Infinity\""));
        assertTrue(json.contains("\"d3\":\"-Infinity\""));
    }

    /*
     * This test is in response to Jira NOSQL-8154 that requires timestamps
     * to be accepted in RFC 3339 Nano format and reject certain strings with
     * a message that mentions RFC 3339. The strings used come from the Jira
     * and associated Confluence document.
     */
    @Test
    public void testParseTimestamp() {
        String[] strings = new String[] {
            "2020-07-01T13:01:25.123",
            "2020-07-01T13:01:25.123Z",
            "2020-07-01T10:01:25.123-03:00",
            "2020-07-01T13:01:25.123456789",
            "2020-07-01T13:01:25.123456789Z",
            "2020-07-01T21:01:25.123456789+08:00"
        };

        String[] goodStrings = new String[] {
            "2020-07-14t02:40:00z",
            "2020-01-01T12:00:27.87+00:20",
            "2020-01-01T12:00:27.873+00:20",
            "2020-01-01T12:00:27.873834+00:20",
            "2020-01-01T12:00:27.873834939+00:20"
        };

        String[] badStrings = new String[] {
            "1937-01-01T12",
            "1937-01-01T12:",
            "1937-01-01T12Z",
            "1937-01-01T12:Z",
            "1937-01-01T12+20:00",
            "1937-01-01T12:+20:00",
            "1937-01-01T12Z+20:00",
            "1937-01-01T12:Z+20:00"
        };

        long expEpochMs = 1593608485123L;
        for (String ts : strings) {
            assertEquals(expEpochMs, RestDataService.parseTimestamp(ts));
        }

        for (String ts : goodStrings) {
            RestDataService.parseTimestamp(ts);
        }

        for (String ts : badStrings) {
            try {
                RestDataService.parseTimestamp(ts);
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("not in RFC"));
            }
        }
    }

    private void roundTrip(Row row) {
        JsonBuilder jb = JsonBuilder.create();
        JsonProtocol.buildFieldValue(jb, null, row);
        String json = jb.toString();

        /* Remove outmost '{' and '}' */
        json = json.substring(1, json.length() - 1);

        Row row1 = row.getTable().createRowFromJson(json, true);
        assertEquals(row, row1);
    }

    private static byte[] genBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }
}
