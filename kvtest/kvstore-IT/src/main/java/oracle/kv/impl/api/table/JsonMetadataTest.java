/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import oracle.kv.impl.api.table.TableImpl.JsonFormatter;
import oracle.kv.table.Index;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This test exercises the simple extension to the JSON-based TableImpl
 * format to include child tables, optionally. This is internal use only
 * at this time.
 */
public class JsonMetadataTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    /*
     * Child table format
     */
    @Test
    public void testChild()
        throws Exception {

        /*
         * Add nested types to exercise JSON serialization
         */
        final String parentDDL =
            "create table parent(id integer, primary key(id), " +
            "ts timestamp(3), " +
            "fb binary(8), " +
            "sArray array(integer), " +
            "en enum(a,b,c,d,e), " +
            "sMap map(string), " +
            "cArray array(map(integer)), " +
            "cMap map(array(string)), " +
            "rec record(map map(string), rec1 record(name string), ar array(timestamp(4))) " +
            ")";
        final String child1DDL =
            "create table parent.child1(name string, primary key(name))";
        final String child2DDL =
            "create table parent.child2(age integer, primary key(age))";
        final String child3DDL =
            "create table parent.child1.child3(city string, primary key(city))";
        final String child4DDL =
            "create table parent.child2.child4(state string, primary key(state))";

        executeDdl(parentDDL);
        executeDdl(child1DDL, true, true);
        executeDdl(child2DDL, true, true);
        executeDdl(child3DDL, true, true);
        executeDdl(child4DDL, true, true);

        TableImpl parent = getTable("parent");
        assertNotNull(parent);
        TableImpl child1 = getTable("parent.child1");
        assertNotNull(child1);
        TableImpl child2 = getTable("parent.child2");
        assertNotNull(child2);
        TableImpl child3 = getTable("parent.child1.child3");
        assertNotNull(child3);
        TableImpl child4 = getTable("parent.child2.child4");
        assertNotNull(child4);
        String parentString = parent.toJsonString(true, true, null);

        TableImpl newParent = TableJsonUtils.fromJsonString(parentString, null);
        assertEquals(parent, newParent);

        String childString = child4.toJsonString(true, false, null);
        TableImpl newChild = TableJsonUtils.fromJsonString(childString,
                                                           child2);
        assertEquals(child4, newChild);
    }

    /*
     * Tests the to/from JSON round tripping of default values in schema
     */
    @Test
    public void testDefaultRoundTrip()
        throws Exception {

        /*
         * Use some regions to put them into the JSON to test round-trip
         */
        executeDdl("set local region myregion");
        executeDdl("create region remoteRegion1");
        executeDdl("create region remoteRegion2");
        executeDdl("create table foo(id integer, primary key(id), " +
                   "age integer default -1, " +
                   "l1 long default 1234567891011, " +
                   "d1 double default 1.234567891011, " +
                   "b1 boolean default true, " +
                   "n1 number default 123456.5678, " +
                   "e1 enum(a, b, c, d) default c, " +
                   "s1 string default 'abcde', " +
                   "t1 timestamp(1) default 7891011) in regions myregion",
                   getNamespace(),
                   true, /* shouldSucceed */
                   /*
                    * Specify noMRTable=true so that executeDdl doesn't add
                    * region arguments in MR table mode since the DDL already
                    * specifies remote regions
                    */
                   true);

        TableImpl table = getTable("foo");
        String json = table.toJsonString(true);
        TableImpl newTable = TableJsonUtils.fromJsonString(json, null);
        assertEquals(table, newTable);
    }

    @Test
    public void testOldJson() throws Exception {
        final String oldJson="{" +
            "\"json_version\" : 1," +
            "\"type\" : \"table\"," +
            "\"name\" : \"vehicleTable\"," +
            "\"namespace\" : \"exampleId\"," +
            "\"ttl\" : \"1 DAYS\"," +
            "\"shardKey\" : [ \"type\", \"make\", \"model\" ]," +
            "\"primaryKey\" : [ \"type\", \"make\", \"model\", \"class\" ]," +
            "\"limits\" : [ {" +
            "\"readLimit\" : 200," +
            "\"writeLimit\" : 100, "+
            "\"sizeLimit\" : 1, " +
            "\"indexKeySizeLimit\" : 64" +
            "} ], " +
            "\"fields\" : [ {" +
            "\"name\" : \"type\", " +
                "\"type\" : \"STRING\", " +
                "\"nullable\" : false, " +
                "\"default\" : null" +
            "}, {" +
                "\"name\" : \"make\", " +
                "\"type\" : \"STRING\", " +
                "\"nullable\" : false, " +
                "\"default\" : null" +
            "}, {" +
                "\"name\" : \"model\", " +
                "\"type\" : \"STRING\", " +
                "\"nullable\" : false, " +
                "\"default\" : null" +
            "}, {" +
                "\"name\" : \"class\", " +
                "\"type\" : \"STRING\", " +
                "\"nullable\" : false, " +
                "\"default\" : null" +
            "}, {" +
                "\"name\" : \"color\", " +
                "\"type\" : \"STRING\", " +
                "\"nullable\" : true, " +
                "\"default\" : null" +
            "} ]" +
            "}";
        TableImpl oldTable = TableJsonUtils.fromJsonString(oldJson, null);
        String newJson = oldTable.toJsonString(true);
        TableImpl newTable = TableJsonUtils.fromJsonString(newJson, null);
        assertEquals(oldTable, newTable);
    }

    @Test
    public void testEnumFixBinary() {
        String ddl = "CREATE TABLE foo (" +
                        "id INTEGER, " +
                        "e ENUM(red, yellow, blue), " +
                        "f BINARY(8), " +
                        "ae ARRAY(ENUM(A,B,C)), " +
                        "me MAP(ENUM(D,E)), " +
                        "af ARRAY(BINARY(16)), " +
                        "mf MAP(BINARY(32)), " +
                        "PRIMARY KEY(id))";
        executeDdl(ddl);

        TableImpl table = getTable("foo");
        String json = table.toJsonString(false);

        TableImpl newTable = TableJsonUtils.fromJsonString(json, null);
        assertEquals(table, newTable);
    }

    @Test
    public void testIdentity() throws Exception {
        String ddl = "CREATE TABLE foo (" +
                     "id INTEGER GENERATED ALWAYS AS IDENTITY, " +
                     "name STRING, " +
                     "PRIMARY KEY(id))";
        executeDdl(ddl);
        roundTrip("foo");

        ddl = "ALTER TABLE foo(MODIFY id GENERATED BY DEFAULT AS IDENTITY)";
        executeDdl(ddl);
        roundTrip("foo");

        ddl = "ALTER TABLE foo(MODIFY id GENERATED BY DEFAULT ON NULL " +
                              "AS IDENTITY)";
        executeDdl(ddl);
        roundTrip("foo");


        ddl = "ALTER TABLE foo(MODIFY id GENERATED BY DEFAULT ON NULL " +
                              "AS IDENTITY (MAXVALUE 99 CYCLE))";
        executeDdl(ddl);
        roundTrip("foo");

        ddl = "ALTER TABLE foo(MODIFY id GENERATED BY DEFAULT ON NULL " +
                              "AS IDENTITY (START WITH 0 INCREMENT BY 2 " +
                                           "MAXVALUE 100 CACHE 5 CYCLE))";
        executeDdl(ddl);
        roundTrip("foo");

        ddl = "ALTER TABLE foo(MODIFY id GENERATED BY DEFAULT ON NULL " +
                              "AS IDENTITY (START WITH -1 INCREMENT BY -2 " +
                                           "MINVALUE -100 CACHE 5 NO CYCLE))";
        executeDdl(ddl);
        roundTrip("foo");

        removeTable(null, "foo", true);
        ddl = "CREATE TABLE foo (" +
                   "id LONG GENERATED ALWAYS AS IDENTITY, " +
                   "name STRING, " +
                   "PRIMARY KEY(id))";
        executeDdl(ddl);
        roundTrip("foo");

        removeTable(null, "foo", true);
        ddl = "CREATE TABLE foo (" +
                "id NUMBER GENERATED BY DEFAULT AS IDENTITY, " +
                "name STRING, " +
                "PRIMARY KEY(id))";
        executeDdl(ddl);
        roundTrip("foo");
    }

    @Test
    public void testIndexWithNullsUnique() {
        String ddl = "CREATE TABLE foo (" +
                        "id INTEGER, " +
                        "name STRING, " +
                        "age INTEGER, " +
                        "phone STRING, " +
                        "PRIMARY KEY(id))";
        executeDdl(ddl);

        ddl = "CREATE INDEX idxAge ON foo(age)";
        executeDdl(ddl);

        ddl = "CREATE INDEX idxName ON foo(name) WITH NO NULLS";
        executeDdl(ddl);

        ddl = "CREATE INDEX idxPhone ON foo(phone) " +
              "WITH NO NULLS WITH UNIQUE KEYS PER ROW";
        executeDdl(ddl);

        roundTrip("foo");
    }

    @Test
    public void testJsonIndexFieldSpecialChars() {
        String ddl = "CREATE TABLE foo (" +
                        "id INTEGER, " +
                        "info JSON, " +
                        "PRIMARY KEY(id))";
        executeDdl(ddl);

        ddl = "CREATE INDEX idx1 ON foo (" +
                    "info.\"_a\"[].key as STRING, " +
                    "info.\"b\\\"c\" as INTEGER)";
        executeDdl(ddl);

        roundTrip("foo");
    }

    @Test
    public void testFunctionalIndex() {
        String ddl = "CREATE TABLE foo (" +
                "id INTEGER, " +
                "i INTEGER, " +
                "s STRING, " +
                "ts TIMESTAMP(3), " +
                "j JSON, " +
                "PRIMARY KEY(id))";
        executeDdl(ddl);

        String[] indexDdls = new String[] {
            "CREATE INDEX idx1 ON foo(" +
                 "length(s), " +
                 "substring(s, 1, 2), " +
                 "power(i, 2), " +
                 "timestamp_round(ts, 2), " +
                 "modification_time()) " +
            "WITH NO NULLS WITH UNIQUE KEYS PER ROW",

            "CREATE INDEX idx2 ON foo (" +
                 "replace(j.s as STRING, \"nosql\", \"NoSQL\"), " +
                 "trunc(j.n.d as Double, 3), " +
                 "substring(j.m[].values().name as AnyAtomic, 3), " +
                 "power(j.\"#\".a.\"@\" as Long, 2)," +
                 "j.v as AnyAtomic)"
        };

        for (String indexDdl : indexDdls) {
            executeDdl(indexDdl);
        }

        roundTrip("foo");
    }

    private void roundTrip(String tableName) {
        roundTrip(tableName, null);
        for (int v = 1; v <= JsonFormatter.CURRENT_VERSION; v++) {
            roundTrip(tableName, v);
        }
    }

    private void roundTrip(String tableName, Integer jsonVersion) {
        TableImpl table = getTable(tableName);
        String json;
        if (jsonVersion != null) {
            json = table.toJsonString(true, jsonVersion);
        } else {
            json = table.toJsonString(true);
        }
        TableImpl newTable = TableJsonUtils.fromJsonString(json, null);
        assertEquals(table, newTable);
        if (table.hasIdentityColumn()) {
            assertEquals(table.getIdentitySequenceDef(),
                         newTable.getIdentitySequenceDef());
        }

        assertEquals(table.getIndexes().size(), newTable.getIndexes().size());
        for (Map.Entry<String, Index> e : table.getIndexes().entrySet()) {
            String name = e.getKey();
            Index idx = table.getIndexes().get(name);
            Index newIdx = newTable.getIndexes().get(name);
            /*
             * Set index status, this is to use IndexImpl.equals() to compare
             * index properties but ignore the difference on status.
             *
             * The index status is not included in table JSON representation,
             * IndexImpl constructed from JSON is with TRANSIENT status, and the
             * status of index of original table is READY.
             */
            ((IndexImpl)newIdx).setStatus(((IndexImpl)idx).getStatus());
            assertEquals(idx, newIdx);
        }
    }
}
