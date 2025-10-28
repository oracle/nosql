/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static oracle.kv.impl.api.table.TableTestBase.makeIndexTypeList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.MapValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.junit.Test;

/*
 * Transient testing of Index serialization/deserialization, as well as
 * testing of index iterator and range operations outside of the context
 * of the store.
 */
public class IndexSerializationTest extends TestBase {

    final String anonMapKey = MapValue.ANONYMOUS;
    private final Random rand = new Random(System.currentTimeMillis());

    private final int maxKeys = 1000;

    private enum NullType {
        SQL_NULL,
        JSON_NULL,
        EMPTY
    }

    @Test
    public void basicSerDeser() {
        runBasicSerDeser(false /*enableNull*/);
        runBasicSerDeser(true  /*enableNull*/);
    }

    /* Ignore warnings about Index.createIndexKey */
    @SuppressWarnings("deprecation")
    private void runBasicSerDeser(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        final String[] enumValues = {"friend", "family", "colleage"};
        TableImpl userTable = TableBuilder.createTableBuilder("Foo",
                                                              "Table of Users",
                                                              null)
            .addLong("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addFloat("height")
            .addDouble("totalTime")
            .addEnum("type", enumValues, null)
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        IndexImpl firstNameIndex =
            new IndexImpl("first", userTable,
                          makeIndexList("id", "firstName"),
                          null,
                          null);
        userTable.addIndex(firstNameIndex);
        IndexImpl ageIndex = new IndexImpl("age", userTable,
                                           makeIndexList("age"),
                                           null,
                                           null);
        userTable.addIndex(ageIndex);

        IndexImpl miscIndex =
            new IndexImpl("misc", userTable,
                          makeIndexList("height", "type",
                                        "lastName", "totalTime"),
                          null,
                          null);
        userTable.addIndex(miscIndex);

        JEMap firstNameIndexMap = new JEMap();
        JEMap ageIndexMap = new JEMap();
        JEMap miscIndexMap = new JEMap();

        for (int i = 0; i < 400; i++) {

            RowImpl row = addUserRow(userTable, i,
                                     getFirstName(i, enableNull),
                                     getLastName(i, enableNull),
                                     getAge(i, enableNull),
                                     getHeight(i, enableNull),
                                     getType(i, enumValues, enableNull),
                                     getTotalTime(i, enableNull));

            IndexKeyImpl key = firstNameIndex.createIndexKey(row);
            /* ikey should have the indexed values but nothing else */
            assertTrue(key.get("firstName").equals(row.get("firstName")));
            assertTrue(key.get("id").equals(row.get("id")));
            try {
                key.get("lastName");
                fail("lastName should not exist");
            } catch (IllegalArgumentException iae) {}

            byte[] serialized = firstNameIndex.serializeIndexKey(key);
            assertTrue(serialized != null);
            firstNameIndexMap.put(serialized, key);

            key = ageIndex.createIndexKey(row);
            serialized = ageIndex.serializeIndexKey(key);
            assertTrue(serialized != null);
            ageIndexMap.put(serialized, key);

            key = miscIndex.createIndexKey(row);
            serialized = miscIndex.serializeIndexKey(key);
            assertTrue(serialized != null);
            miscIndexMap.put(serialized, key);
        }

        firstNameIndexMap.validate();
        ageIndexMap.validate();
        miscIndexMap.validate();
    }

    /*
     * Test various composite indexes and ranges.  Shard key does not affect
     * indexes so that's not used here.
     */
    @Test
    public void testComposite() {
        runTestComposite(false /*enableNull*/);
        runTestComposite(true  /*enableNull*/);
    }

    private void runTestComposite(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        /*
         * Build a table that can be used to create a number of different
         * indexes.
         */
        final TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addInteger("int1")
            .addInteger("int2")
            .addInteger("int3")
            .addString("s1")
            .addString("s2")
            .addString("s3")
            .addDouble("d1")
            .addDouble("d2")
            .addDouble("d3")
            .addEnum("enum", new String[]{"a", "b", "c"}, null, null, null)
            .primaryKey("id")
            .buildTable();

        /*
         * Now some indexes
         */
        IndexImpl indexIntString =
            new IndexImpl("int_string", table,
                          makeIndexList("int1", "s1"), null, null);
        table.addIndex(indexIntString);

        IndexImpl indexSSS =
            new IndexImpl("s_s_s", table,
                          makeIndexList("s1", "s2", "s3"), null, null);
        table.addIndex(indexSSS);

        IndexImpl indexStringEnum =
            new IndexImpl("s_enum", table,
                          makeIndexList("s1", "enum"), null, null);
        table.addIndex(indexStringEnum);

        IndexImpl indexEnumString =
            new IndexImpl("enum_s", table,
                          makeIndexList("enum", "s1"), null, null);
        table.addIndex(indexEnumString);

        /*
         * Try an invalid index with the same field used more than once.
         * NOTE: this wasn't fixed until after 3.0.9.
         */
        try {
            @SuppressWarnings("unused")
            IndexImpl indexStringBad =
                new IndexImpl("s_enum", table,
                              makeIndexList("s1", "enum", "s1"), null, null);
            fail("Index creation should have failed");
        } catch (IllegalCommandException ice) {}

        /*
         * Create ranges and keys and make some assertions
         */

        /*
         * Create a range
         */
        FieldRange fieldRange = indexIntString.createFieldRange("int1")
            .setStart(1, true)
            .setEnd(3, false);
        IndexRange range = new IndexRange(indexIntString.createIndexKey(),
                                          fieldRange.createMultiRowOptions(),
                                          null);
        /*
         * Create values and compare
         */
        IndexKeyImpl indexKey = indexIntString.createIndexKey();
        indexKey.put("int1", 1);
        indexKey.put("s1", "row1");
        assertTrue(range.inRange(indexKey));
        indexKey.put("int1", 2);
        assertTrue(range.inRange(indexKey));
        indexKey.put("int1", 3);
        assertFalse(range.inRange(indexKey));
        indexKey.put("int1", 0);
        assertFalse(range.inRange(indexKey));
        if (enableNull) {
            indexKey.clear();
            indexKey.putNull("int1");
            assertFalse(range.inRange(indexKey));

            indexKey.put("int1", 1);
            indexKey.putNull("s1");
            assertTrue(range.inRange(indexKey));
        }

        /*
         * Create a range with a prefix, start, no end
         */
        indexKey.clear();
        indexKey.put("int1", 2);
        fieldRange = indexIntString.createFieldRange("s1").setStart("b", true);
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);
        indexKey.clear();
        indexKey.put("int1", 1);
        assertFalse(range.inRange(indexKey));
        indexKey.put("int1", 2);
        assertFalse(range.inRange(indexKey));
        indexKey.put("s1", "a");
        assertFalse(range.inRange(indexKey));
        indexKey.put("s1", "b");
        assertTrue(range.inRange(indexKey));
        indexKey.put("int1", 3);
        assertFalse(range.inRange(indexKey));
        if (enableNull) {
            indexKey.clear();
            indexKey.putNull("int1");
            assertFalse(range.inRange(indexKey));

            indexKey.put("int1", 2);
            indexKey.putNull("s1");
            assertFalse(range.inRange(indexKey));
        }

        /*
         * String index
         */
        indexKey = indexSSS.createIndexKey();
        indexKey.put("s1", "defmnox");
        fieldRange = indexSSS.createFieldRange("s2").setStart("hij", true);
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);
        indexKey.clear();

        indexKey.put("s2", "hij");
        indexKey.put("s3", "x");
        /* prefix needs to be exact match */
        indexKey.put("s1", "defmnoxy");
        assertFalse(range.inRange(indexKey));
        indexKey.put("s1", "defmno");
        assertFalse(range.inRange(indexKey));
        indexKey.put("s1", "def");
        assertFalse(range.inRange(indexKey));
        indexKey.put("s1", "defmnox");
        assertTrue(range.inRange(indexKey));
        indexKey.put("s2", "hi");
        assertFalse(range.inRange(indexKey));
        if (enableNull) {
            indexKey.putNull("s1");
            assertFalse(range.inRange(indexKey));

            indexKey.put("s1", "defmnox");
            indexKey.putNull("s2");
            assertFalse(range.inRange(indexKey));

            indexKey.put("s2", "hij");
            assertTrue(range.inRange(indexKey));

            indexKey.putNull("s3");
            assertTrue(range.inRange(indexKey));
        }

        /*
         * More String index, use end range
         */
        fieldRange = indexSSS.createFieldRange("s2")
            .setStart("hij", true).setEnd("mno", false);
        indexKey.clear();
        indexKey.put("s1", "a");
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);
        indexKey.put("s2", "mno");
        assertFalse(range.inRange(indexKey));
        indexKey.put("s2", "mn");
        assertTrue(range.inRange(indexKey));
        indexKey.put("s2", "mnoa");
        assertFalse(range.inRange(indexKey));
        if (enableNull) {
            indexKey.putNull("s2");
            assertFalse(range.inRange(indexKey));
        }

        /*
         * String, Enum
         * Exact match on s1, range on enum
         */
        indexKey = indexStringEnum.createIndexKey();
        indexKey.put("s1", "row1");
        fieldRange = indexStringEnum.createFieldRange("enum")
            .setStartEnum("a", true).setEndEnum("b", true);
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);
        indexKey.clear();
        indexKey.putEnum("enum", "a");
        indexKey.put("s1", "row1");
        assertTrue(range.inRange(indexKey));
        indexKey.putEnum("enum", "b");
        assertTrue(range.inRange(indexKey));
        if (enableNull) {
            indexKey.putNull("enum");
            assertFalse(range.inRange(indexKey));
        }

        /*
         * Enum, String
         * range on enum, first field in index
         */
        indexKey = indexEnumString.createIndexKey();
        fieldRange = indexEnumString.createFieldRange("enum")
            .setStartEnum("a", true).setEndEnum("b", true);
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);
        indexKey.putEnum("enum", "a");
        indexKey.put("s1", "row1");
        assertTrue(range.inRange(indexKey));
        indexKey.putEnum("enum", "b");
        assertTrue(range.inRange(indexKey));
        if (enableNull) {
            indexKey.clear();
            indexKey.putNull("enum");
            assertFalse(range.inRange(indexKey));

            indexKey.putEnum("enum", "a");
            indexKey.putNull("s1");
            assertTrue(range.inRange(indexKey));
        }

        /*
         * Illegal start/end order for enum.
         */
        try {
            fieldRange = indexEnumString.createFieldRange("enum")
                .setStartEnum("b", true).setEndEnum("a", true);
            fail("Illegal range");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * TODO: more indexes and ranges using other types
         */
    }

    @Test
    public void testIndexRangeUpperBound() {
        runTestUpperBound(IndexImpl.INDEX_VERSION_V0);
        runTestUpperBound(IndexImpl.INDEX_VERSION_V1);
    }

    private void runTestUpperBound(int serialVersion) {

        setIndexSerialVersion(serialVersion);

        TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addInteger("i")
            .addField("a", TableBuilder.createArrayBuilder()
                                       .addInteger()
                                       .build())
            .addField("r", TableBuilder.createRecordBuilder("r")
                                       .addInteger("ri")
                                       .addString("rs")
                                       .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl indexArray =
            new IndexImpl("indexArray", table,
                          makeIndexList("a[]"), null, null);
        table.addIndex(indexArray);

        IndexImpl indexRecRiRs =
            new IndexImpl("indexRecRiRs", table,
                          makeIndexList("r.ri", "r.rs"), null, null);

        /* Range on indexArray */

        IndexKeyImpl indexKey = indexArray.createIndexKey();
        FieldRange fieldRange = indexArray.createFieldRange("a[]")
                                          .setStart(0, true);
        IndexRange range = new IndexRange(indexKey,
                                          fieldRange.createMultiRowOptions(),
                                          null);

        indexKey.put("a[]", Integer.MAX_VALUE);
        assertTrue(range.inRange(indexKey));

        indexKey.putEMPTY("a[]");
        assertFalse(range.inRange(indexKey));

        indexKey.putNull("a[]");
        assertFalse(range.inRange(indexKey));

        /* Range on indexRecRiRs */

        indexKey = indexRecRiRs.createIndexKey();
        indexKey.put("r.ri", 10);
        fieldRange = indexRecRiRs.createFieldRange("r.rs")
                                 .setStart("abc", false);
        range = new IndexRange(indexKey,
                               fieldRange.createMultiRowOptions(),
                               null);

        indexKey.put("r.rs", "abc");
        assertFalse(range.inRange(indexKey));
        indexKey.put("r.rs", "abc ");
        assertTrue(range.inRange(indexKey));

        indexKey.putEMPTY("r.rs");
        assertFalse(range.inRange(indexKey));

        indexKey.putNull("r.rs");
        assertFalse(range.inRange(indexKey));
    }

    /**
     * Use the JEMap below to create an index to test some limitations and
     * implementation of inclusive/exclusive range semantics.
     */
    @Test
    public void testInclusive() {
        runTestInclusive(false /*enableNull*/);
        runTestInclusive(true  /*enableNull*/);

        runTestInclusiveNotNull(false /*enableNull*/);
        runTestInclusiveNotNull(true /*enableNull*/);

        runTestInclusiveJsonField();
    }

    private void runTestInclusive(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        /*
         * Build a table that can be used to create a number of different
         * indexes.
         */
        final TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addInteger("int1")
            .addInteger("int2")
            .addInteger("int3")
            .addString("s1")
            .addString("s2")
            .addString("s3")
            .addDouble("d1")
            .addDouble("d2")
            .addDouble("d3")
            .addNumber("n")
            .addEnum("enum", new String[]{"a", "b", "c"}, null, null, null)
            .primaryKey("id")
            .buildTable();

        /*
         * Composite, 3-integer index
         */
        IndexImpl index =
            new IndexImpl("int3", table,
                          makeIndexList("int1", "int2", "int3"), null, null);
        table.addIndex(index);

        IndexImpl idx_s_e =
            new IndexImpl("idx_s_e", table,
                          makeIndexList("s1", "enum"), null, null);
        table.addIndex(idx_s_e);

        IndexImpl idx_n_e =
            new IndexImpl("idx_n_e", table,
                          makeIndexList("n", "enum"), null, null);
        table.addIndex(idx_n_e);

        /*
         * Pretend the JEMap is an index and add some values to it
         */
        int edgeCount = 4; /* how close to min/max/zero to add values */
        JEMap map = new JEMap();
        IndexKeyImpl key = index.createIndexKey();
        for (int i = 0; i < edgeCount; i++) {
            key.put("int1", Integer.MIN_VALUE + i);
            for (int j = 0; j < edgeCount; j++) {
                key.put("int2", Integer.MIN_VALUE + j);
                add(map, index, key, edgeCount);
                key.put("int2", 0 + j);
                add(map, index, key, edgeCount);
                key.put("int2", Integer.MAX_VALUE - j);
                add(map, index, key, edgeCount);
            }
            key.put("int1", 0 + i);
            for (int j = 0; j < edgeCount; j++) {
                key.put("int2", Integer.MIN_VALUE + j);
                add(map, index, key, edgeCount);
                key.put("int2", 0 + j);
                add(map, index, key, edgeCount);
                key.put("int2", Integer.MAX_VALUE - j);
                add(map, index, key, edgeCount);
            }
        }

        map.validate();

        /*
         * Try some things
         */
        key.clear();
        key.put("int1", 1);
        key.put("int2", Integer.MAX_VALUE - 1);

        map.clear();
        IndexKeyImpl nextKey = key.clone();
        int num = 5;
        for (int i = 0; i < num; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            key = nextKey.clone();
            map.put(index.serializeIndexKey(nextKey), key);
        }
        assertEquals(map.size(), num);
        map.validate();

        /*
         * Increase the key that contains max values, key.incrementIndexKey()
         * should return false.
         */
        key.clear();
        key.put("int1", Integer.MAX_VALUE);
        key.put("int2", Integer.MAX_VALUE);
        nextKey = key.clone();
        if (enableNull) {
            for (int i = 0; i < 3; i++) {
                assertTrue(nextKey.incrementIndexKey());
                assertTrue(key.compareTo(nextKey) < 0);
                key = nextKey.clone();
            }

            key.clear();
            key.putNull("int1");
            key.put("int2", Integer.MAX_VALUE);
            nextKey = key.clone();

            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            assertTrue(nextKey.get("int1").isNull() &&
                       nextKey.get("int2").isEMPTY());

            key = nextKey.clone();
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            assertTrue(nextKey.get("int1").isNull() &&
                       nextKey.get("int2").isNull());
            assertFalse(nextKey.incrementIndexKey());
        } else {
            assertFalse(nextKey.incrementIndexKey());
        }

        key = idx_s_e.createIndexKey();
        EnumDefImpl edef = ((EnumDefImpl)table.getField("enum").asEnum());
        key.put("s1", "");
        key.put("enum", edef.createEnum(0));
        nextKey = key.clone();
        map.clear();
        num = edef.getValues().length + 1;
        for (int i = 0; i < num; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            key = nextKey.clone();
            map.put(idx_s_e.serializeIndexKey(nextKey), key);
        }
        assertEquals(map.size(), num);
        map.validate();

        if (enableNull) {
            key.clear();
            key.putNull("s1");
            key.put("enum", edef.createEnum(edef.getValues().length - 1));
            nextKey = key.clone();
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            assertTrue(nextKey.get("enum").isEMPTY());

            key = nextKey.clone();
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(nextKey.get("enum").isNull());
            assertFalse(nextKey.incrementIndexKey());
        }

        /* Test the index on NUMBER and ENUM fields */
        key = idx_n_e.createIndexKey();
        key.putNumber("n", 0);
        nextKey = key.clone();
        assertTrue(nextKey.incrementIndexKey());
        assertTrue(key.compareTo(nextKey) < 0);
        key.putNumber("n", 0.000000000000001);
        assertTrue(key.compareTo(nextKey) > 0);

        key.putNumber("n", Long.MIN_VALUE);
        nextKey = key.clone();
        assertTrue(nextKey.incrementIndexKey());
        assertTrue(key.compareTo(nextKey) < 0);
        key.putNumber("n", Long.MIN_VALUE + 1);
        assertTrue(key.compareTo(nextKey) > 0);

        key.putNumber("n", Double.MAX_VALUE);
        nextKey = key.clone();
        assertTrue(nextKey.incrementIndexKey());
        key.putNumber("n", new BigDecimal("1.79769313486231570000001E+308"));
        assertTrue(key.compareTo(nextKey) > 0);

        key.put("enum", edef.createEnum(0));
        key.putNumber("n", 0);
        nextKey = key.clone();
        map.clear();
        num = edef.getValues().length + 1;
        for (int i = 0; i < num; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            key = nextKey.clone();
            map.put(idx_n_e.serializeIndexKey(nextKey), key);
        }
        assertEquals(map.size(), num);
        map.validate();

        /* TODO: more use cases */
    }

    private void runTestInclusiveNotNull(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        /*
         * Build a table that can be used to create a number of different
         * indexes.
         */
        final TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id", null, false, 0)
            .addInteger("intNa")
            .addInteger("intNN", null, false, 0)
            .addString("sNa")
            .addString("sNN", null, false, "")
            .addEnum("enumNa", new String[]{"a", "b", "c"}, null, null, null)
            .addEnum("enumNN", new String[]{"a", "b", "c"}, null, false, "a")
            .primaryKey("id")
            .buildTable();

        IndexImpl idx_ina_inn =
            new IndexImpl("idx_ina_inn", table,
                          makeIndexList("intNa", "intNN"), null, null);
        table.addIndex(idx_ina_inn);

        IndexImpl idx_sna_ena =
            new IndexImpl("idx_sna_ena", table,
                          makeIndexList("sNa", "enumNa"), null, null);
        table.addIndex(idx_sna_ena);

        IndexImpl idx_ina_enn =
            new IndexImpl("idx_ina_enn", table,
                          makeIndexList("intNa", "enumNN"), null, null);
        table.addIndex(idx_ina_enn);

        /*
         * Pretend the JEMap is an index and add some values to it
         */
        JEMap map = new JEMap();
        IndexKeyImpl key = idx_ina_inn.createIndexKey();
        key.put("intNa", 1);
        key.put("intNN", Integer.MAX_VALUE - 1);
        IndexKeyImpl nextKey = key.clone();

        int num = 5;
        for (int i = 0; i < num; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            key = nextKey.clone();
            map.put(idx_ina_inn.serializeIndexKey(nextKey), key);
        }
        assertEquals(map.size(), num);
        map.validate();

        /*
         * Increase the key that contains max values, key.incrementIndexKey()
         * should return false.
         */
        key.clear();
        key.put("intNa", Integer.MAX_VALUE);
        key.put("intNN", Integer.MAX_VALUE);
        nextKey = key.clone();
        if (enableNull) {
            map.clear();
            for (int i = 0; i < 3; i++) {
                assertTrue(nextKey.incrementIndexKey());
                assertTrue(key.compareTo(nextKey) < 0);
                key = nextKey.clone();
                map.put(idx_ina_inn.serializeIndexKey(nextKey), key);
            }
            assertEquals(map.size(), 3);
            map.validate();

            key.clear();
            key.putNull("intNa");
            key.put("intNN", Integer.MAX_VALUE);
            nextKey = key.clone();
            assertFalse(nextKey.incrementIndexKey());
        } else {
            assertFalse(nextKey.incrementIndexKey());
        }

        key = idx_sna_ena.createIndexKey();
        EnumDefImpl edef = ((EnumDefImpl)table.getField("enumNa").asEnum());
        key.put("sNa", "");
        key.put("enumNa", edef.createEnum(0));
        nextKey = key.clone();

        map.clear();
        num = edef.getValues().length + 1;
        for (int i = 0; i < num; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            if (i == (edef.getValues().length - 1)) {
                if (enableNull) {
                    assertTrue(nextKey.get("enumNa").isEMPTY());
                } else {
                    assertTrue(nextKey.get("enumNa")
                                   .equals(edef.createEnum(0)));
                }
            }
            key = nextKey.clone();
            map.put(idx_sna_ena.serializeIndexKey(nextKey), key);
        }
        assertEquals(map.size(), num);
        map.validate();

        if (enableNull) {
            key.clear();
            key.putNull("sNa");
            key.put("enumNa", edef.createEnum(edef.getValues().length - 1));
            nextKey = key.clone();
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            assertTrue(nextKey.get("enumNa").isEMPTY());

            key = nextKey.clone();
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            assertTrue(nextKey.get("enumNa").isNull());
            assertFalse(nextKey.incrementIndexKey());
        }

        key = idx_ina_enn.createIndexKey();
        key.put("intNa", Integer.MAX_VALUE);
        key.put("enumNN", edef.createEnum(edef.getValues().length - 1));
        nextKey = key.clone();
        if (enableNull) {
            map.clear();
            for (int i = 0; i < edef.getValues().length; i++) {
                assertTrue(nextKey.incrementIndexKey());
                assertTrue(key.compareTo(nextKey) < 0);
                key = nextKey.clone();
                map.put(idx_ina_enn.serializeIndexKey(nextKey), key);
            }
            assertEquals(map.size(), edef.getValues().length);
            map.validate();

            key.clear();
            key.putNull("intNa");
            key.put("enumNN", edef.createEnum(edef.getValues().length - 1));
            nextKey = key.clone();
            assertFalse(nextKey.incrementIndexKey());
        } else {
            assertFalse(nextKey.incrementIndexKey());
        }
    }

    private void runTestInclusiveJsonField() {
        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(true);

        /*
         * Build a table that can be used to create a number of different
         * indexes.
         */
        final TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id", null, false, 0)
            .addJson("json", null)
            .primaryKey("id")
            .buildTable();

        IndexImpl idx_long_bool =
            new IndexImpl("idx_long", table,
                          makeIndexList("json.bool", "json.long"),
                          makeIndexTypeList(FieldDef.Type.BOOLEAN,
                                            FieldDef.Type.LONG), null);
        table.addIndex(idx_long_bool);

        IndexKeyImpl key = idx_long_bool.createIndexKey();
        key.put("json.bool", Boolean.TRUE);
        key.put("json.long", Long.MAX_VALUE - 3);
        IndexKeyImpl nextKey = key.clone();

        for (int i = 0; i < 3; i++) {
            assertTrue(nextKey.incrementIndexKey());
            assertTrue(key.compareTo(nextKey) < 0);
            key = nextKey.clone();
        }

        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.long").isEMPTY());
        assertTrue(key.compareTo(nextKey) < 0);
        key = nextKey.clone();

        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.long").isJsonNull());
        assertTrue(key.compareTo(nextKey) < 0);
        key = nextKey.clone();

        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.long").isNull());
        assertTrue(key.compareTo(nextKey) < 0);
        key = nextKey.clone();

        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.bool").isEMPTY());
        assertTrue(nextKey.get("json.long").asLong().get() == Long.MIN_VALUE);
        assertTrue(key.compareTo(nextKey) < 0);

        nextKey.putNull("json.long");
        key = nextKey.clone();
        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.bool").isJsonNull());
        assertTrue(nextKey.get("json.long").asLong().get() == Long.MIN_VALUE);
        assertTrue(key.compareTo(nextKey) < 0);
        key = nextKey.clone();

        nextKey.putNull("json.long");
        key = nextKey.clone();
        assertTrue(nextKey.incrementIndexKey());
        assertTrue(nextKey.get("json.bool").isNull());
        assertTrue(nextKey.get("json.long").asLong().get() == Long.MIN_VALUE);
        assertTrue(key.compareTo(nextKey) < 0);
        key = nextKey.clone();

        nextKey.putNull("json.long");
        key = nextKey.clone();
        assertFalse(nextKey.incrementIndexKey());
    }

    /**
     * Test indexes on nested complex types.
     */
    @Test
    public void testNested() {
        runTestNested(false /*enableNull*/);
        runTestNested(true  /*enableNull*/);
    }

    private void runTestNested(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        final TableImpl table = TableBuilder.createTableBuilder("NestedTable")
            .addInteger("id")
            .addString("first")
            .addString("last")
            /* Record */
            .addField("address", TableBuilder.createRecordBuilder("address")
                      .addInteger("number")
                      .addInteger("zip")
                      .addString("street")
                      .addString("city")
                      .build())
            /* Map of integer */
            .addField("map", TableBuilder.createMapBuilder()
                      .addInteger().build())
            /* Map of record (email addresses), keyed by type */
            .addField("email", TableBuilder.createMapBuilder()
                      .addField(TableBuilder.createRecordBuilder("email")
                                .addString("address")
                                .addString("isp")
                                .build())
                      .build())
            /* Map of array of integer */
            .addField("arrayMap", TableBuilder.createMapBuilder()
                      .addField(TableBuilder.createArrayBuilder()
                                .addInteger().build())
                      .build())
            /* Array of record of jobs */
            .addField("recArray", TableBuilder.createArrayBuilder()
                      .addField(TableBuilder.createRecordBuilder("jobs")
                                .addString("title")
                                .addString("company")
                                .addField("phones",
                                          TableBuilder.createMapBuilder()
                                          .addString().build())
                                .build())
                      .build())
            .primaryKey("id")
            .buildTable();

        /*
         * Composite index on last, address.city, address.zip.
         * Change case to exercise case-insensitivity.
         */
        IndexImpl index =
            new IndexImpl("last_city", table,
                          makeIndexList("laSt", "Address.City", "address.zip"),
                          null, null);
        table.addIndex(index);

        /* Index on map of integer */
        IndexImpl mapIndex =
            new IndexImpl("map_a", table, makeIndexList("map.a"), null, null);
        table.addIndex(mapIndex);

        /* Try an illegal string that might be supported in the future */
        try {
            IndexImpl escMapIndex =
                new IndexImpl("map_escape", table,
                              makeIndexList("map.a\\\\.b"), null, null);
            table.addIndex(escMapIndex);
            fail("Index creation should have failed");
        } catch (IllegalCommandException ice) {}

        /* Index on the isp in the "home" map entry in the email address map */
        IndexImpl emailIndex =
            new IndexImpl("homeEmail", table,
                          makeIndexList("email.home.isp"), null, null);
        table.addIndex(emailIndex);

        /* Index on the "a" field of map of array of integer */
        IndexImpl mapOfArrayIndex =
            new IndexImpl("mapOfArray", table,
                          makeIndexList("arrayMap.a[]"), null, null);
        table.addIndex(mapOfArrayIndex);
        assertTrue(mapOfArrayIndex.isMultiKey());

        /* Index on the company field of the array of job records */
        IndexImpl arrayIndex =
            new IndexImpl("arrayIndex", table,
                          makeIndexList("recArray[].company"), null, null);
        table.addIndex(arrayIndex);
        assertTrue(arrayIndex.isMultiKey());

        /* Index on the company field of the array of job records */
        IndexImpl arrayOfMapIndex =
            new IndexImpl("arrayOfMapIndex", table,
                          makeIndexList("recArray[].phones.home"), null, null);
        table.addIndex(arrayOfMapIndex);
        assertTrue(arrayOfMapIndex.isMultiKey());

        IndexKeyImpl key = index.createIndexKey();
        key.put("last", "Jones");
        key.put("address.city", "Chicago");
        key.put("address.zip", 12345);
        key.validate();

        /*
         * Try serialization/deserialization.
         */
        byte[] serialized = index.serializeIndexKey(key);
        IndexKeyImpl ikey = fromIndexKey(index, serialized, false);
        assertTrue(key.equals(ikey));
        if (enableNull) {

            key.clear();
            key.putNull("last");
            key.putNull("address.city");
            key.putNull("address.zip");
            serialized = index.serializeIndexKey(key);
            ikey = fromIndexKey(index, serialized, true);
            key.validate();
            assertTrue(key.equals(ikey));

            key.clear();
            key.putNull("last");
            serialized = index.serializeIndexKey(key);
            ikey = fromIndexKey(index, serialized, true);
            key.validate();
            assertTrue(key.equals(ikey));

            key.putNull("address.city");
            serialized = index.serializeIndexKey(key);
            ikey = fromIndexKey(index, serialized, true);
            key.validate();
            assertTrue(key.equals(ikey));

            key.putNull("address.zip");
            serialized = index.serializeIndexKey(key);
            ikey = fromIndexKey(index, serialized, true);
            key.validate();
            assertTrue(key.equals(ikey));
        }

        /* map index */
        key = mapIndex.createIndexKey();
        key.put("map.a", 1);
        serialized = mapIndex.serializeIndexKey(key);
        ikey = fromIndexKey(mapIndex, serialized, false);
        assertTrue(key.equals(ikey));

        if (enableNull) {
            key.clear();
            key.putNull("map.a");
            serialized = mapIndex.serializeIndexKey(key);
            ikey = fromIndexKey(mapIndex, serialized, true);
            assertTrue(key.equals(ikey));
            key.validate();
        }

        /* map of record index */
        key = emailIndex.createIndexKey();
        key.put("email.home.isp", "fastisp");
        serialized = emailIndex.serializeIndexKey(key);
        ikey = fromIndexKey(emailIndex, serialized, false);
        assertTrue(key.equals(ikey));
        if (enableNull) {
            key.clear();
            key.putNull("email.home.isp");
            serialized = emailIndex.serializeIndexKey(key);
            ikey = fromIndexKey(emailIndex, serialized, true);
            assertTrue(key.equals(ikey));
            key.validate();
        }

        /* map of array index */
        key = mapOfArrayIndex.createIndexKey();

        key.put("arrayMap.a[]", 1);
        serialized = mapOfArrayIndex.serializeIndexKey(key);
        ikey = fromIndexKey(mapOfArrayIndex, serialized, false);
        assertTrue(key.equals(ikey));
        if (enableNull) {
            key.clear();
            key.putNull("arrayMap.a[]");
            serialized = mapOfArrayIndex.serializeIndexKey(key);
            ikey = fromIndexKey(mapOfArrayIndex, serialized, true);
            assertTrue(key.equals(ikey));
            key.validate();
        }

        /* array of jobs index */
        key = arrayIndex.createIndexKey();
        key.put("recArray[].company", "oracle");
        serialized = arrayIndex.serializeIndexKey(key);
        assertTrue(serialized != null && serialized.length > 0);
        ikey = fromIndexKey(arrayIndex, serialized, false);
        assertTrue(key.equals(ikey));
        if (enableNull) {
            key.clear();
            key.putNull("recArray[].company");
            serialized = arrayIndex.serializeIndexKey(key);
            ikey = fromIndexKey(arrayIndex, serialized, true);
            assertTrue(key.equals(ikey));
            key.validate();
        }

        /* array of phone of jobs records */
        key = arrayOfMapIndex.createIndexKey();
        key.put("recArray[].phones.home", "1234567");
        serialized = arrayOfMapIndex.serializeIndexKey(key);
        ikey = fromIndexKey(arrayOfMapIndex, serialized, true);
        assertTrue(key.equals(ikey));
        if (enableNull) {
            key.clear();
            key.putNull("recArray[].phones.home");
            serialized = arrayOfMapIndex.serializeIndexKey(key);
            ikey = fromIndexKey(arrayOfMapIndex, serialized, true);
            assertTrue(key.equals(ikey));
            key.validate();
        }

        /*
         * Some illegal index fields.  IndexKeyImpl can validate nested fields
         * during the put.  It does not currently validate order of fields so
         * it's possible to fool it; however, bad index keys will always
         * generate an exception when used in a TableAPI call, which validates
         * fully-constructed IndexKeys.
         */
        key = emailIndex.createIndexKey();
        try {
            /* "work" not indexed */
            key.put("email.work.isp", "fastisp");
            fail("Index key put should have failed");
        } catch (IllegalArgumentException iae) {}

        key = mapIndex.createIndexKey();
        try {
            /* "b" not indexed */
            key.put("map.b", 1);
            fail("Index key put should have failed");
        } catch (IllegalArgumentException iae) {}

        key = index.createIndexKey();
        try {
            /* "street" not indexed */
            key.put("address.street", "elm");
            fail("Index key put should have failed");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void mapIndexes() throws Exception {
        runTestMapIndexes(false /*enableNull*/);
        runTestMapIndexes(true  /*enableNull*/);
    }

    private void runTestMapIndexes(boolean enableNull) {

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        final TableImpl table = TableBuilder.createTableBuilder("MapTable")
            .addInteger("id")
            /* Map of integer */
            .addField("map", TableBuilder.createMapBuilder()
                      .addInteger().build())
            /* Map of record (addresses) */
            .addField("addresses", TableBuilder.createMapBuilder()
                      .addField(TableBuilder.createRecordBuilder("address")
                                .addInteger("number")
                                .addInteger("zip")
                                .addString("street")
                                .addString("city")
                                .build())
                      .build())
            .addField("maprecord", TableBuilder.createRecordBuilder("maprecord")
                      .addField("map", TableBuilder.createMapBuilder()
                                .addLong().build())
                      .build())
            /* Array of record of jobs */
            .addField("recArray", TableBuilder.createArrayBuilder()
                      .addField(TableBuilder.createRecordBuilder("jobs")
                                .addString("title")
                                .addString("company")
                                .build())
                      .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl index = null;
        IndexKeyImpl key = null;

        /*
         * Try bad index on the map value which is a record, which is not
         * indexable.
         */
        try {
            index = new IndexImpl("mapRecord", table,
                                  makeIndexList("addresses.keys()",
                                                "addresses.values()"), null, null);
            fail("Index creation should have failed");
        } catch (IllegalCommandException ice) {
        }

        /*
         * Index on a field in the map's record.
         */
        index = new IndexImpl("mapRecord", table,
                              makeIndexList("addresses.valuEs().street",
                                            "addresses.valueS().city", "id"), null, null);
        table.addIndex(index);
        key = index.createIndexKey();
        key.put("addresses.vAlues().city", "boston");
        key.put("addresses.valUes().street", "elm");
        key.validate();

        index = new IndexImpl("mapKeyValue", table,
                              makeIndexList("map.Keys()", "map.Values()"),
                              null, null);
        table.addIndex(index);

        key = index.createIndexKey();
        key.put("map.kEys()", "common");
        key.put("map.VaLues()", 13);
        key.validate();
        key.put("map.Values()", 20);
        key.validate();

        RowImpl row = table.createRow() ;
        MapValue mapVal = row.putMap("map");
        for (int i = 0; i < 10; i++) {
            mapVal.put(("key" + i), i).put("common", i + 10);
        }

        List<byte[]> keys = index.extractIndexKeys(row, maxKeys);
        assert(keys.size() == 11);
    }

    @Test
    public void testArrayIndexWithNull() {
        testArrayStringIndexes(false);
        testArrayStringIndexes(true);
        testArrayRecordIndexes(false);
        testArrayRecordIndexes(true);
        testArrayMapIndexes(false);
        testArrayMapIndexes(true);
    }

    private void testArrayStringIndexes(boolean enableNull) {

        setIndexEnableNullSupport(enableNull);

        TableImpl table = TableBuilder.createTableBuilder("TestArrayString")
            .addInteger("id")
            .addInteger("i")
            .addField("as", TableBuilder.createArrayBuilder()
                            .addString().build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxArray = new IndexImpl("idx_as", table,
                                           makeIndexList("as[]"), null, null);
        table.addIndex(idxArray);

        IndexImpl idxArrayInt = new IndexImpl("idx_as_i", table,
                                              makeIndexList("as[]", "i"),
                                              null, null);
        table.addIndex(idxArrayInt);

        IndexImpl idxIntArray = new IndexImpl("idx_i_as", table,
                                              makeIndexList("i", "as[]"),
                                              null, null);
        table.addIndex(idxIntArray);

        String[] jsons = new String[] {
            "{\"id\":1,\"i\":1,\"as\":[\"as1\"]}",
            "{\"id\":2,\"i\":2,\"as\":[\"\"]}",
            "{\"id\":3,\"i\":3,\"as\":null}",
            "{\"id\":4,\"i\":null,\"as\":[\"\"]}",
            "{\"id\":5,\"i\":null,\"as\":[]}",
            "{\"id\":6,\"i\":null,\"as\":null}"
        };

        JEMap idxArrayMap = new JEMap();
        JEMap idxArrayIntMap = new JEMap();
        JEMap idxIntArrayMap = new JEMap();
        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);

            putKeysToJEMap(idxArray, row, idxArrayMap);
            putKeysToJEMap(idxArrayInt, row, idxArrayIntMap);
            putKeysToJEMap(idxIntArray, row, idxIntArrayMap);
        }

        idxArrayMap.validate();
        int expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxArrayMap.size());

        idxArrayIntMap.validate();
        expNum = enableNull ? 6 : 2;
        assertEquals(expNum, idxArrayIntMap.size());

        idxIntArrayMap.validate();
        expNum = enableNull ? 6 : 2;
        assertEquals(expNum, idxArrayIntMap.size());
    }

    private void testArrayRecordIndexes(boolean enableNull) {

        setIndexEnableNullSupport(enableNull);

        TableImpl table =
            TableBuilder.createTableBuilder("TestArrayRecordString")
            .addInteger("id")
            .addInteger("i")
            .addField("arec", TableBuilder.createArrayBuilder()
                      .addField(TableBuilder.createRecordBuilder("rec1")
                                .addInteger("rid1")
                                .addString("rs1")
                                .build())
                      .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxArecRid1 = new IndexImpl("idxArecRid1", table,
                                              makeIndexList("arec[].rid1"),
                                              null, null);
        table.addIndex(idxArecRid1);

        IndexImpl idxIArecRid1Rs1 =
            new IndexImpl("idxIArecRid1Rs1", table,
                          makeIndexList("i", "arec[].rid1", "arec[].rs1"),
                          null, null);
        table.addIndex(idxIArecRid1Rs1);

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"arec\":[{\"rid1\":1,\"rs1\":\"rs1\"}]}",
            "{\"id\":2,\"i\":2,\"arec\":[{\"rid1\":2,\"rs1\":null}]}",
            "{\"id\":3,\"i\":3,\"arec\":[{\"rid1\":null,\"rs1\":null}]}",
            "{\"id\":4,\"i\":4,\"arec\":null}",
            "{\"id\":5,\"i\":5,\"arec\":[]}",
            "{\"id\":6,\"i\":null,\"arec\":[]}",
            "{\"id\":7,\"i\":null,\"arec\":null}"
        };

        JEMap idxArecRid1Map = new JEMap();
        JEMap idxIArecRid1Rs1Map = new JEMap();
        for (String json : jsons) {
            Row row = table.createRowFromJson(json, false);
            putKeysToJEMap(idxArecRid1, row, idxArecRid1Map);
            putKeysToJEMap(idxIArecRid1Rs1, row, idxIArecRid1Rs1Map);
        }

        idxArecRid1Map.validate();
        int expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxArecRid1Map.size());

        idxIArecRid1Rs1Map.validate();
        expNum = enableNull ? 7 : 1;
        assertEquals(expNum, idxIArecRid1Rs1Map.size());
    }

    private void testArrayMapIndexes(boolean enableNull) {
        setIndexEnableNullSupport(enableNull);

        TableImpl table =
            TableBuilder.createTableBuilder("TestArrayMap")
            .addInteger("id")
            .addInteger("i")
            .addField("amap", TableBuilder.createArrayBuilder()
                      .addField(TableBuilder.createMapBuilder()
                                .addInteger()
                                .build())
                      .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxAmapKey1 = new IndexImpl("idxAmapKey1", table,
                                              makeIndexList("amap[].key1"),
                                              null, null);
        table.addIndex(idxAmapKey1);

        IndexImpl idxIAmapKey2 =
            new IndexImpl("idxIAmapKey2", table,
                          makeIndexList("i", "amap[].key2"), null, null);
        table.addIndex(idxIAmapKey2);

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"amap\":[{\"key1\":1,\"key2\":2}]}",
            "{\"id\":2,\"i\":null,\"amap\":[{\"key1\":2,\"key2\":3}]}",
            "{\"id\":3,\"i\":3,\"amap\":[{\"key4\":4,\"key5\":5}]}",
            "{\"id\":4,\"i\":4,\"amap\":[]}",
            "{\"id\":5,\"i\":5,\"amap\":null}",
            "{\"id\":6,\"i\":null,\"amap\":null}",
            "{\"id\":7,\"i\":null,\"amap\":[]}"
        };

        JEMap idxAmapKey1Map = new JEMap();
        JEMap idxIAmapKey2Map = new JEMap();

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, false);
            putKeysToJEMap(idxAmapKey1, row, idxAmapKey1Map);
            putKeysToJEMap(idxIAmapKey2, row, idxIAmapKey2Map);
        }

        idxAmapKey1Map.validate();
        int expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxAmapKey1Map.size());

        idxIAmapKey2Map.validate();
        expNum = enableNull ? 7 : 1;
        assertEquals(expNum, idxIAmapKey2Map.size());
    }

    @Test
    public void testMapIndexWithNull() {
        testMapStringIndexes(false);
        testMapStringIndexes(true);
        testMapRecordIndexes(false);
        testMapRecordIndexes(true);
    }

    private void testMapStringIndexes(boolean enableNull) {

        setIndexEnableNullSupport(enableNull);

        TableImpl table = TableBuilder.createTableBuilder("TestMapString")
            .addInteger("id")
            .addInteger("i")
            .addField("ms", TableBuilder.createMapBuilder()
                            .addString().build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxKeyOfMs = new IndexImpl("idx_keyof_ms", table,
                                             makeIndexList("ms.keys()"),
                                             null, null);
        table.addIndex(idxKeyOfMs);

        IndexImpl idxElementOfMs = new IndexImpl("idx_elementof_ms", table,
                                                 makeIndexList("ms.valUEs()"),
                                                 null, null);
        table.addIndex(idxElementOfMs);

        IndexImpl idxMsKey1 = new IndexImpl("idx_ms_key1", table,
                                             makeIndexList("ms.key1"),
                                            null, null);
        table.addIndex(idxMsKey1);

        IndexImpl idxKeyElementOfMs =
            new IndexImpl("idx_key_element_ms", table,
                          makeIndexList("ms.values()", "ms.keYs()"), null, null);
        table.addIndex(idxKeyElementOfMs);

        IndexImpl idxMisc =
            new IndexImpl("idx_idxMisc", table,
                          makeIndexList("i", "ms.keys()", "ms.values()"),
                          null, null);
        table.addIndex(idxMisc);

        String[] jsons = new String[] {
            "{\"id\":1,\"i\":1,\"ms\":{\"key1\":\"val11\",\"key2\":\"val12\"}}",
            "{\"id\":2,\"i\":null,\"ms\":null}",
            "{\"id\":3,\"i\":null,\"ms\":{\"key1\":\"val31\",\"key2\":\"val32\"}}",
            "{\"id\":4,\"i\":4,\"ms\":null}",
            "{\"id\":5,\"i\":5,\"ms\":{}}"
        };

        JEMap idxKeyOfMsMap = new JEMap();
        JEMap idxElementOfMsMap = new JEMap();
        JEMap idxMsKey1Map = new JEMap();
        JEMap idxKeyElementOfMsMap = new JEMap();
        JEMap idxMiscMap = new JEMap();

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);
            putKeysToJEMap(idxKeyOfMs, row, idxKeyOfMsMap);
            putKeysToJEMap(idxElementOfMs, row, idxElementOfMsMap);
            putKeysToJEMap(idxMsKey1, row, idxMsKey1Map);
            putKeysToJEMap(idxKeyElementOfMs, row, idxKeyElementOfMsMap);
            putKeysToJEMap(idxMisc, row, idxMiscMap);
        }

        int expNum;

        idxKeyOfMsMap.validate();
        expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxKeyOfMsMap.size());

        idxElementOfMsMap.validate();
        expNum = enableNull ? 6 : 4;
        assertEquals(expNum, idxElementOfMsMap.size());

        idxMsKey1Map.validate();
        expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxMsKey1Map.size());

        idxKeyElementOfMsMap.validate();
        expNum = enableNull ? 6 : 4;
        assertEquals(expNum, idxKeyElementOfMsMap.size());

        idxMiscMap.validate();
        expNum = enableNull ? 7 : 2;
        assertEquals(expNum, idxMiscMap.size());
    }

    private void testMapRecordIndexes(boolean enableNull) {

        setIndexEnableNullSupport(enableNull);

        TableImpl table = TableBuilder.createTableBuilder("TestMapRecord")
            .addInteger("id")
            .addInteger("i")
            .addField("ms", TableBuilder.createMapBuilder()
                            .addField(TableBuilder.createRecordBuilder("rec")
                                      .addString("rs").build())
                            .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxKeyOfMs =
            new IndexImpl("idx_keyof_ms", table,
                          makeIndexList("ms.keys()"),
                          null, null);
        table.addIndex(idxKeyOfMs);

        IndexImpl idxElementOfMsRs =
            new IndexImpl("idx_elementof_ms_rs", table,
                          makeIndexList("ms.vALues().rs"),
                          null, null);
        table.addIndex(idxElementOfMsRs);

        IndexImpl idxMisc =
            new IndexImpl("idx_misc", table,
                          makeIndexList("i", "ms.keyS()", "ms.valueS().rs"),
                          null, null);
        table.addIndex(idxMisc);

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"ms\":{\"key1\":{\"rs\":\"rs11\"}," +
                                      "\"key2\":{\"rs\":\"rs12\"}}}",
            "{\"id\":2,\"i\":2, \"ms\":{\"key1\":{\"rs\":\"rs21\"}}}",
            "{\"id\":3,\"i\":null,\"ms\":{\"key1\":{\"rs\":\"rs31\"}," +
                                         "\"key2\":{\"rs\":\"rs32\"}}}",
            "{\"id\":4,\"i\":4,\"ms\":null}",
            "{\"id\":5,\"i\":null,\"ms\":{}}",
            "{\"id\":6,\"i\":null,\"ms\":null}",
        };

        JEMap idxKeyOfMsMap = new JEMap();
        JEMap idxElementOfMsRsMap = new JEMap();
        JEMap idxMiscMap = new JEMap();

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);

            putKeysToJEMap(idxKeyOfMs, row, idxKeyOfMsMap);
            putKeysToJEMap(idxElementOfMsRs, row, idxElementOfMsRsMap);
            putKeysToJEMap(idxMisc, row, idxMiscMap);
        }

        idxKeyOfMsMap.validate();
        int expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxKeyOfMsMap.size());

        idxElementOfMsRsMap.validate();
        expNum = enableNull ? 7 : 5;
        assertEquals(expNum, idxElementOfMsRsMap.size());

        idxMiscMap.validate();
        expNum = enableNull ? 8 : 3;
        assertEquals(expNum, idxMiscMap.size());
    }

    @Test
    public void testRecordIndexesWithNull() {
        testRecordStringIndexes(false);
        testRecordStringIndexes(true);
        testRecordNestedArrayIndexes(false);
        testRecordNestedArrayIndexes(true);
        testRecordMapIndexes(false);
        testRecordMapIndexes(true);
    }

    private void testRecordStringIndexes(boolean enableNull){

        setIndexEnableNullSupport(enableNull);

        TableImpl table = TableBuilder.createTableBuilder("TestRecordString")
            .addInteger("id")
            .addInteger("i")
            .addField("rec", TableBuilder.createRecordBuilder("rec")
                             .addInteger("rid")
                             .addString("rs")
                             .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxRecRid = new IndexImpl("idx_rec_rid", table,
                                              makeIndexList("rec.rid"),
                                              null, null);
        table.addIndex(idxRecRid);

        IndexImpl idxRecRs = new IndexImpl("idx_rec_rs", table,
                                              makeIndexList("rec.rs"),
                                              null, null);
        table.addIndex(idxRecRs);

        IndexImpl idxMisc =
            new IndexImpl("idx_misc", table,
                          makeIndexList("i", "rec.rs", "rec.rid"),
                          null, null);
        table.addIndex(idxMisc);

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"rec\":{\"rid\":1,\"rs\":\"rs1\"}}",
            "{\"id\":2,\"i\":2,\"rec\":{\"rid\":2,\"rs\":null}}",
            "{\"id\":3,\"i\":3,\"rec\":{\"rid\":null,\"rs\":\"rs2\"}}",
            "{\"id\":4,\"i\":4,\"rec\":{\"rid\":null,\"rs\":null}}",
            "{\"id\":5,\"i\":null,\"rec\":{\"rid\":null,\"rs\":null}}",
            "{\"id\":6,\"i\":null,\"rec\":null}"
        };

        JEMap idxRecRidMap = new JEMap();
        JEMap idxRecRsMap = new JEMap();
        JEMap idxMiscMap = new JEMap();

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);
            putKeysToJEMap(idxRecRid, row, idxRecRidMap);
            putKeysToJEMap(idxRecRs, row, idxRecRsMap);
            putKeysToJEMap(idxMisc, row, idxMiscMap);
        }

        idxRecRidMap.validate();
        int expNum = enableNull ? 3 : 2;
        assertEquals(expNum, idxRecRidMap.size());

        idxRecRsMap.validate();
        expNum = enableNull ? 3 : 2;
        assertEquals(expNum, idxRecRsMap.size());

        idxMiscMap.validate();
        expNum = enableNull ? 5 : 1;
        assertEquals(expNum, idxMiscMap.size());
    }

    private void testRecordNestedArrayIndexes(boolean enableNull){

        setIndexEnableNullSupport(enableNull);

        /*
         * rec0 RECORD(rid1 integer, rec1 RECORD(ras2 ARRAY(string)))
         */
        TableImpl table = TableBuilder.createTableBuilder("TestRecordArray")
            .addInteger("id")
            .addInteger("i")
            .addField("rec0",
                      TableBuilder.createRecordBuilder("rec0")
                      .addInteger("rid1")
                      .addField("rec1",
                                TableBuilder.createRecordBuilder("rec1")
                                .addField("ras2",
                                          TableBuilder.createArrayBuilder()
                                          .addString()
                                          .build())
                                .build())
                       .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxRas2 = new IndexImpl("idx_ras2", table,
                                          makeIndexList("rec0.rec1.ras2[]"),
                                          null, null);
        table.addIndex(idxRas2);

        IndexImpl idxRid1Ras2 =
            new IndexImpl("idx_rid1_ras2", table,
                          makeIndexList("rec0.rid1", "rec0.rec1.ras2[]"),
                          null, null);
        table.addIndex(idxRid1Ras2);

        IndexImpl idxIRas2Rid1 =
            new IndexImpl("idx_i_ras2_rid1", table,
                           makeIndexList("i", "rec0.rec1.ras2[]", "rec0.rid1"),
                           null, null);
        table.addIndex(idxIRas2Rid1);

        JEMap idxRas2Map = new JEMap();
        JEMap idxRid1Ras2Map = new JEMap();
        JEMap idxIRas2Rid1Map = new JEMap();

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"rec0\":{\"rid1\":1," +
                "\"rec1\":{\"ras2\":[\"as11\",\"as12\"]}}}",
            "{\"id\":2,\"i\":2,\"rec0\":{\"rid1\":2," +
                "\"rec1\":{\"ras2\":null}}}",
            "{\"id\":3,\"i\":3,\"rec0\":{\"rid1\":3," +
                "\"rec1\":{\"ras2\":[\"\"]}}}",
            "{\"id\":4,\"i\":4,\"rec0\":{\"rid1\":null," +
                "\"rec1\":{\"ras2\":[\"\"]}}}",
            "{\"id\":5,\"i\":null,\"rec0\":{\"rid1\":null," +
                "\"rec1\":{\"ras2\":null}}}",
            "{\"id\":6,\"i\":null,\"rec0\":{\"rid1\":null," +
                "\"rec1\":{\"ras2\":[\"as61\"]}}}",
        };

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);
            putKeysToJEMap(idxRas2, row, idxRas2Map);
            putKeysToJEMap(idxRid1Ras2, row, idxRid1Ras2Map);
            putKeysToJEMap(idxIRas2Rid1, row, idxIRas2Rid1Map);
        }

        idxRas2Map.validate();
        int expNum = enableNull ? 5 : 4;
        assertEquals(expNum, idxRas2Map.size());

        idxRid1Ras2Map.validate();
        expNum = enableNull ? 7 : 3;
        assertEquals(expNum, idxRid1Ras2Map.size());

        idxIRas2Rid1Map.validate();
        expNum = enableNull ? 7 : 3;
        assertEquals(expNum, idxIRas2Rid1Map.size());
    }

    private void testRecordMapIndexes(boolean enableNull) {

        setIndexEnableNullSupport(enableNull);

        TableImpl table = TableBuilder.createTableBuilder("TestRecordMap")
            .addInteger("id")
            .addInteger("i")
            .addField("rec0",
                      TableBuilder.createRecordBuilder("rec0")
                      .addInteger("rid1")
                      .addField("rms1",
                                TableBuilder.createMapBuilder()
                                .addString()
                                .build())
                       .build())
            .primaryKey("id")
            .buildTable();

        IndexImpl idxElementOfRms1 = new IndexImpl("idx_elementof_rms1", table,
                                          makeIndexList("rec0.rms1.valUes()"),
                                          null, null);
        table.addIndex(idxElementOfRms1);

        IndexImpl idxKeyOfElementOfRms1 =
            new IndexImpl("idx_keyof_elementof_rms1", table,
                          makeIndexList("rec0.rms1.VALUES()",
                                        "rec0.rms1.KEYS()"),
                          null, null);
        table.addIndex(idxKeyOfElementOfRms1);

        IndexImpl idxRms1Key1 = new IndexImpl("idx_rms1_key1", table,
                                              makeIndexList("rec0.rms1.key1"),
                                              null, null);
        table.addIndex(idxRms1Key1);

        IndexImpl idxIKey1ElementOfRms1 =
            new IndexImpl("idx_rms2_key1_elementof", table,
                          makeIndexList("i", "rec0.rms1.key1",
                                        "rec0.rms1.vaLUes()"),
                          null, null);
        table.addIndex(idxIKey1ElementOfRms1);

        JEMap idxElementOfRms1Map = new JEMap();
        JEMap idxKeyElementOfRms1Map = new JEMap();
        JEMap idxKey1Rms1Map = new JEMap();
        JEMap idxIKey1ElementOfRms1Map = new JEMap();

        String[] jsons = {
            "{\"id\":1,\"i\":1,\"rec0\":{\"rid1\":1," +
                "\"rms1\":{\"key1\":\"val1\",\"key2\":\"val2\"}}}",
            "{\"id\":2,\"i\":null,\"rec0\":{\"rid1\":1," +
                "\"rms1\":{\"key1\":\"val1\",\"key2\":\"val2\"}}}",
            "{\"id\":3,\"i\":3,\"rec0\":{\"rid1\":1,\"rms1\":null}}",
            "{\"id\":4,\"i\":null,\"rec0\":{\"rid1\":1,\"rms1\":{}}}",
            "{\"id\":5,\"i\":null,\"rec0\":{\"rid1\":1,\"rms1\":null}}"
        };

        for (String json : jsons) {
            Row row = table.createRowFromJson(json, true);

            putKeysToJEMap(idxElementOfRms1, row, idxElementOfRms1Map);
            putKeysToJEMap(idxKeyOfElementOfRms1, row, idxKeyElementOfRms1Map);
            putKeysToJEMap(idxRms1Key1, row, idxKey1Rms1Map);
            putKeysToJEMap(idxIKey1ElementOfRms1, row, idxIKey1ElementOfRms1Map);
        }

        idxElementOfRms1Map.validate();
        int expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxElementOfRms1Map.size());

        idxKeyElementOfRms1Map.validate();
        expNum = enableNull ? 4 : 2;
        assertEquals(expNum, idxKeyElementOfRms1Map.size());

        idxKey1Rms1Map.validate();
        expNum = enableNull ? 3 : 1;
        assertEquals(expNum, idxKey1Rms1Map.size());

        idxIKey1ElementOfRms1Map.validate();
        expNum = enableNull ? 7 : 2;
        assertEquals(expNum, idxIKey1ElementOfRms1Map.size());
    }

    /**
     * Test the serialization and deserialization of non-null fields:
     *  1. Creates 2 table "contacts1" and "contacts2":
     *      - Table "contacts1" contains the fields that are nullable.
     *      - Table "contacts2" contains the simple fields that not not-null,
     *        complex fields are nullable.
     *  2. Create multiple indexes:
     *      - Index on single simple fields.
     *      - Index on multiple fields.
     *      - Index on map or array.
     *  3. Create rows for above 2 tables, do below:
     *     for each row
     *     do
     *         for each index of 2 tables
     *         do
     *             1.extract index key bytes and compare the number of bytes.
     *             2.Use IndexImpl.deserializeIndexKey() to deserialize the
     *               index key from bytes and compare index keys.
     *             3.Use IndexImpl.rowFromIndexKey() to deserialize the
     *               index key to a row and compare rows.
     *             4.Use JEMap to store key bytes and Index Key of index of
     *               table contacts2 to verify sorting.
     *         end loop
     *     end loop
     */
    @Test
    public void testSerDeserNullablity() {
        final boolean enableNull = true;

        /* Set if enable null value supported in index key */
        setIndexEnableNullSupport(enableNull);

        final String[] enumValues = {"friend", "family", "colleage"};
        TableImpl tableNullable = TableBuilder.createTableBuilder("contacts1")
            .addLong("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addFloat("height")
            .addDouble("totalTime")
            .addEnum("type", enumValues, null)
            .addField("address",
                      TableBuilder.createRecordBuilder("address")
                          .addString("city")
                          .addString("street")
                          .build())
            .addField("email",
                      TableBuilder.createArrayBuilder("email")
                          .addString().build())
            .addField("phone",
                      TableBuilder.createMapBuilder("phone")
                          .addString().build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        TableImpl tableNotNull = TableBuilder.createTableBuilder("contacts2")
            .addLong("id")
            .addString("firstName", null, false, "na")
            .addString("lastName", null, false, "na")
            .addInteger("age", null, false, -1)
            .addFloat("height", null, false, 0.0f)
            .addDouble("totalTime", null, false, 0.0d)
            .addEnum("type", enumValues, null, false, enumValues[0])
            .addField("address",
                      TableBuilder.createRecordBuilder("address")
                          .addString("city", null, false, "na")
                          .addString("street", null, false, "na")
                          .build())
            .addField("email",
                      TableBuilder.createArrayBuilder("email")
                          .addString().build())
            .addField("phone",
                      TableBuilder.createMapBuilder("phone")
                          .addString().build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        String[] idxFields = new String[] {
            "firstName", "lastName", "age", "height", "totalTime", "type"
        };

        for (int i = 0; i < idxFields.length; i++) {
            String field = idxFields[i];
            List<String> fieldList = makeIndexList(field);
            tableNullable.addIndex(createIndex(tableNullable, fieldList));
            tableNotNull.addIndex(createIndex(tableNotNull, fieldList));
        }

        List<String> fieldList = makeIndexList("lastName", "age", "height",
                                               "address.city", "id");
        tableNullable.addIndex(createIndex(tableNullable, fieldList));
        tableNotNull.addIndex(createIndex(tableNotNull, fieldList));

        fieldList = makeIndexList("firstName", "height",
                                  "phone.keys()", "phone.vAlues()");
        tableNullable.addIndex(createIndex(tableNullable, fieldList));
        tableNotNull.addIndex(createIndex(tableNotNull, fieldList));

        fieldList = makeIndexList("age", "email[]");
        tableNullable.addIndex(createIndex(tableNullable, fieldList));
        tableNotNull.addIndex(createIndex(tableNotNull, fieldList));

        int numIndexes = tableNullable.getIndexes().size();
        assertEquals(numIndexes, tableNotNull.getIndexes().size());

        JEMap[] jeMaps = new JEMap[numIndexes];
        for (int i = 0; i < jeMaps.length; i++) {
            jeMaps[i] = new JEMap();
        }

        for (int i = 0; i < 50; i++) {

            RowImpl rowY = addContactRow(tableNullable, i, enumValues);
            RowImpl rowN = addContactRow(tableNotNull, i, enumValues);

            int ind = 0;
            for (Entry<String, Index> entry:
                    tableNullable.getIndexes().entrySet()) {

                String idxName = entry.getKey();
                IndexImpl idxY = (IndexImpl)entry.getValue();
                IndexImpl idxN = (IndexImpl)tableNotNull.getIndex(idxName);
                assertTrue(idxN != null);
                assertTrue(idxY.isMultiKey() == idxN.isMultiKey());

                JEMap jeMap = jeMaps[ind++];
                if (idxY.isMultiKey()) {
                    assertTrue(idxN.isMultiKey());

                    List<byte[]> listY = idxY.extractIndexKeys(rowY, maxKeys);
                    List<byte[]> listN = idxN.extractIndexKeys(rowN, maxKeys);
                    assertEquals(listY.size(), listN.size());

                    for (int n = 0; n < listY.size(); n++) {
                       verifyIndexKeys(idxY, listY.get(n),
                                       idxN, listN.get(n), jeMap);
                    }

                } else {
                    byte[] keyBytesY = idxY.extractIndexKey(rowY);
                    assert(keyBytesY != null);
                    byte[] keyBytesN = idxN.extractIndexKey(rowN);
                    assert(keyBytesN != null);
                    verifyIndexKeys(idxY, keyBytesY, idxN, keyBytesN, jeMap);
                }
            }
        }

        for (JEMap map : jeMaps) {
            map.validate();
        }
    }

    private IndexImpl createIndex(TableImpl table, List<String> fields) {
        return new IndexImpl(getIndexName(fields), table, fields, null, null);
    }

    /**
     * Returns a valid index name based on the field names.
     */
    private String getIndexName(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("idx");
        for (String field : fields) {
            sb.append("_");
            sb.append(field.replace(".", "_")
                          .replace("(", "_").replace(")","_")
                          .replace("[", "").replace("]",""));
        }
        return sb.toString();
    }

    private void verifyIndexKeys(IndexImpl idxY, byte[] keyBytesY,
                                 IndexImpl idxN, byte[] keyBytesN,
                                 JEMap jeMap) {

        assertEquals(getNumOfNotNullFields(idxN),
                     keyBytesY.length - keyBytesN.length);

        IndexKeyImpl iKeyY = idxY.deserializeIndexKey(keyBytesY, false);
        IndexKeyImpl iKeyN = idxN.deserializeIndexKey(keyBytesN, false);

        assertEquals(iKeyY.getNumFields(), iKeyN.getNumFields());

        for (String field: idxY.getFields()) {
            FieldValue valY = iKeyY.get(field);
            FieldValue valN = iKeyN.get(field);
            assertTrue(FieldValueImpl.compareFieldValues(valY, valN) == 0);
        }

        RecordValueImpl rowY1 = idxY.getIndexKeyDef().createRecord();
        idxY.rowFromIndexEntry(rowY1, null, keyBytesY);

        RecordValueImpl rowN1 = idxN.getIndexKeyDef().createRecord();
        idxN.rowFromIndexEntry(rowN1, null, keyBytesN);

        assertEquals(rowY1.getNumFields(), rowN1.getNumFields());

        for (IndexField field : idxY.getIndexFields()) {
            String path = field.getPathName();

            FieldValue valY = rowY1.get(path);
            assertTrue(valY != null);

            FieldValue valN = rowN1.get(path);
            assertTrue(valN != null);

            if (valY.isRecord()) {
                assertTrue(valN.isRecord());
                assertTrue(valY.toJsonString(true)
                           .equalsIgnoreCase(valN.toJsonString(true)));
            } else {
                assertTrue(valY.compareTo(valN) == 0);
            }
        }

        jeMap.put(keyBytesN, idxN.deserializeIndexKey(keyBytesN, false));
    }

    /**
     * Returns the number of nullable index fields.
     */
    private int getNumOfNotNullFields(IndexImpl index) {
        TableImpl table = index.getTable();
        int cnt = 0;
        for (IndexField iField : index.getIndexFields()) {
            if (iField.isComplex() ||
                table.isKeyComponent(iField.getPathName()) ||
                table.isNullable(iField.getPathName())) {
                continue;
            }
            cnt++;
        }
        return cnt;
    }

    /* Ignore warnings about Index.createIndexKey */
    @SuppressWarnings("deprecation")
    @Test
    public void testTimestampFields() {
        TableImpl table = TableBuilder.createTableBuilder("TimestampTable")
            .addInteger("id")
            .addTimestamp("ts0", 0)
            .addTimestamp("ts1", 1)
            .addTimestamp("ts2", 2)
            .addTimestamp("ts3", 3)
            .addTimestamp("ts4", 4)
            .addTimestamp("ts5", 5)
            .addTimestamp("ts6", 6)
            .addTimestamp("ts7", 7)
            .addTimestamp("ts8", 8)
            .addTimestamp("ts9", 9)
            .addField("rec", TableBuilder.createRecordBuilder("rec")
                              .addTimestamp("ts1", 1)
                              .addTimestamp("ts3", 3)
                              .addTimestamp("ts9", 9)
                              .build())
            .addField("arr", TableBuilder.createArrayBuilder("arr")
                              .addTimestamp("ts6", 6)
                              .build())
            .addField("map", TableBuilder.createMapBuilder("map")
                              .addTimestamp("ts2", 2)
                              .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        IndexImpl idx_ts0 = new IndexImpl("idx_ts0", table,
                                          makeIndexList("ts0"), null, null);
        table.addIndex(idx_ts0);

        IndexImpl idx_ts1 = new IndexImpl("idx_ts1", table,
                                          makeIndexList("ts1"), null, null);
        table.addIndex(idx_ts1);

        IndexImpl idx_ts2 = new IndexImpl("idx_ts2", table,
                                          makeIndexList("ts2"), null, null);
        table.addIndex(idx_ts2);

        IndexImpl idx_ts3 = new IndexImpl("idx_ts3", table,
                                          makeIndexList("ts3"), null, null);
        table.addIndex(idx_ts3);

        IndexImpl idx_ts4 = new IndexImpl("idx_ts4", table,
                                          makeIndexList("ts4"), null, null);
        table.addIndex(idx_ts4);

        IndexImpl idx_ts5 = new IndexImpl("idx_ts5", table,
                                          makeIndexList("ts5"), null, null);
        table.addIndex(idx_ts5);

        IndexImpl idx_ts6 = new IndexImpl("idx_ts6", table,
                                          makeIndexList("ts6"), null, null);
        table.addIndex(idx_ts6);

        IndexImpl idx_ts7 = new IndexImpl("idx_ts7", table,
                                          makeIndexList("ts7"), null, null);
        table.addIndex(idx_ts7);

        IndexImpl idx_ts8 = new IndexImpl("idx_ts8", table,
                                          makeIndexList("ts8"), null, null);
        table.addIndex(idx_ts8);

        IndexImpl idx_ts9 = new IndexImpl("idx_ts9", table,
                                          makeIndexList("ts9"), null, null);
        table.addIndex(idx_ts9);

        IndexImpl idx_rec_ts0 =
            new IndexImpl("idx_rec_ts1_ts3_ts9", table,
                          makeIndexList("rec.ts1", "rec.ts3", "rec.ts9"), null, null);
        table.addIndex(idx_rec_ts0);

        IndexImpl idx_arr_ts1 = new IndexImpl("idx_arr_ts6", table,
                                              makeIndexList("arr[]"), null, null);
        table.addIndex(idx_arr_ts1);

        IndexImpl idx_map_ts3 = new IndexImpl("idx_map_ts2", table,
                                              makeIndexList("map.vALUes()"),
                                              null, null);
        table.addIndex(idx_map_ts3);

        IndexImpl idx_ts0_ts3_ts6_ts9 =
            new IndexImpl("idx_ts0_ts3_ts6_ts9", table,
                          makeIndexList("ts0", "ts3", "ts6", "ts9"), null, null);
        table.addIndex(idx_ts0_ts3_ts6_ts9);

        int nRows = 100;
        JEMap[] jeMaps = new JEMap[table.getIndexes().size()];
        for (int i = 0; i < nRows; i++) {
            Timestamp ts = genTimestamp(i, nRows);
            RowImpl row = addTimestampTableRow(table, i, ts);
            int idx = 0;
            for (String name : table.getIndexes().keySet()) {
                IndexImpl index = (IndexImpl) table.getIndexes().get(name);
                JEMap jeMap = jeMaps[idx];
                if (jeMap == null) {
                    jeMap = new JEMap();
                    jeMaps[idx] = jeMap;
                }
                if (index.isMultiKey()) {
                    byte[] key = table.createKey(row, false).toByteArray();
                    byte[] data = table.createValue(row).toByteArray();
                    List<byte[]> indexKeys =
                        index.extractIndexKeys(key, data, 0, 0, 0, 0, false, maxKeys);
                    for (byte[] buf : indexKeys) {
                        IndexKeyImpl idxKey = index.deserializeIndexKey(buf,
                                                                        false);
                        jeMap.put(buf, idxKey.clone());
                    }
                } else {
                    IndexKeyImpl key = index.createIndexKey(row);
                    byte[] serialized = index.serializeIndexKey(key);
                    jeMap.put(serialized, key.clone());
                }
                idx++;
            }
        }

        for (int i = 0; i < jeMaps.length; i++) {
            jeMaps[i].validate();
        }
    }

    /**
     * NYI: test for generic index.
     */
    //@Test
    public void testJsonSerialization() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addJson("json", null)
            .addJson("json1", null)
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        table.addIndex(new IndexImpl("jsonindex", table,
                                     makeIndexList("json", "json1"),
                                     null, null));
        IndexImpl index = (IndexImpl) table.getIndex("jsonindex");
        IndexKeyImpl ikey = index.createIndexKey();
        ikey.put("json", 1);
        ikey.put("json1", "10");

        byte[] previous = index.serializeIndexKey(ikey);
        for (int i = (Integer.MAX_VALUE - 10000); i < Integer.MAX_VALUE; i++) {
            ikey.put("json", i);
            ikey.put("json1", i-10);
            //ikey.putNull("json1");
            byte[] serialized = index.serializeIndexKey(ikey);

            assertTrue(IndexImpl.compareUnsignedBytes(previous, serialized) < 0);
            previous = serialized;
	}
    }

    @Test
    public void testJsonTypedIndexSerialization() {
        setIndexEnableNullSupport(true);

        runJsonFieldAsTypeTest();
        runJsonSubFieldAsTypeTest();

        runRecordJsonSubFieldAsTypeTest();

        runArrayOfJsonFieldAsTypeTest();
        runArrayOfJsonSubFieldAsTypeTest();

        runMapOfJsonFieldAsTypeTest();
        runMapOfJsonSubFieldAsTypeTest();

        testInvalidFieldPath();

        runMultiKeyJsonKeysTest();
    }

    /**
     * Typed index on json field
     */
    private void runJsonFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addJson("json", null)
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        String indexName = "idxJson";
        String fieldName = "json";
        String fmt = "{\"json\":%s}";
        int num = 100;

        String[] nulls = formatJsons(fmt, new String[]{null, "null"});
        NullType[] expNullTypes = new NullType[]{
            NullType.SQL_NULL,
            NullType.JSON_NULL
        };

        runTypedIndexKeyTest(table, indexName, fieldName, num,
                             fmt, fmt, nulls, expNullTypes);
    }

    /**
     * Typed index on json.sub field
     */
    private void runJsonSubFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addJson("json", null)
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        String indexName = "indexJsonSub";
        String fieldName = "json.sub";
        String jsonFmt = "{\"json\": %s}";
        String jsonSubFmt = "{\"json\": {\"sub\": %s}}";
        int num = 100;

        String[] nulls = formatJsons(jsonFmt, new String[] {
            null,
            "null",
            "{\"sub\":null}",
            "{}",
            "{\"name\":\"test\"}"
        });

        NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.JSON_NULL,
            NullType.EMPTY,
            NullType.EMPTY
        };

        runTypedIndexKeyTest(table, indexName, fieldName, num,
                             jsonSubFmt, jsonSubFmt,
                             nulls, expNullTypes);
    }

    /**
     * Typed index on rec.json.sub field
     */
    private void runRecordJsonSubFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("rec",
                      TableBuilder.createRecordBuilder("rec")
                      .addJson("json", null).build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        String indexName = "indexRecJsonSub";
        String fieldName = "rec.json.sub";
        String jsonFmt = "{\"rec\":{\"json\": %s}}";
        String jsonSubFmt = "{\"rec\":{\"json\": {\"sub\": %s}}}";
        int num = 100;

        String[] nulls = formatJsons(jsonFmt, new String[] {
            null,
            "null",
            "{\"sub\":null}",
            "{}",
            "{\"name\":\"test\"}"
        });
        NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.JSON_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        runTypedIndexKeyTest(table, indexName, fieldName, num, jsonSubFmt,
                             jsonSubFmt, nulls, expNullTypes);
    }

    /**
     * Typed index on array(json) field
     */
    private void runArrayOfJsonFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("a", TableBuilder.createArrayBuilder().addJson().build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        final String indexName = "idxArray";
        final String fieldName = "a[]";
        final String arrayFmt = "{\"a\":%s}";
        final String arraySubFmt = "{\"a\":[%s]}";
        final String[] nulls = formatJsons(arrayFmt, new String[] {
            null, "null", "[]", "[null]"
        });
        final NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.JSON_NULL
        };

        runNestedTypedIndexKeyTest(table, indexName, fieldName, 20,
                                   arraySubFmt, nulls, expNullTypes,
                                   new JsonStringFormatter() {
            @Override
            public String createJsonString(String[] values) {
                return formatJsonString(arraySubFmt, null, values);
            }
        });
    }

    /**
     * Typed index on array(json).sub field
     */
    private void runArrayOfJsonSubFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("a", TableBuilder.createArrayBuilder().addJson().build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        final String indexName = "idxArray";
        final String fieldName = "a[].sub";
        final String arrayFmt = "{\"a\":%s}";
        final String arraySubFmt = "{\"a\":[%s]}";
        final String elemFmt = "{\"sub\":%s}";
        final String arrayElementFmt = "{\"a\":[{\"sub\":%s}]}";
        final String[] nulls = formatJsons(arrayFmt, new String[] {
            null,
            "null",
            "[]",
            "[null]",
            "[{\"sub\":null,\"b\":true}]",
            "[{\"d\":1.1}]",
            "[{}]"
        });
        final NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.JSON_NULL,
            NullType.EMPTY,
            NullType.EMPTY
        };

        runNestedTypedIndexKeyTest(table, indexName, fieldName, 20,
                                   arrayElementFmt, nulls, expNullTypes,
                                   new JsonStringFormatter() {
            @Override
            public String createJsonString(String[] values) {
                return formatJsonString(arraySubFmt, elemFmt, values);
            }
        });
    }

    /**
     * Typed index on map(json).values() and map(json).keys()
     */
    private void runMapOfJsonFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("m", TableBuilder.createMapBuilder().addJson().build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        final String indexName = "idxMap";
        final String mapKeysField = "m.keys()";
        final String mapValuesField = "m.values()";
        final String mapFmt = "{\"m\":%s}";
        final String mapSubFmt = "{\"m\":{%s}}";
        final String elemFmt = "\"%s\":%s";
        final String mapValueFmt = "{\"m\":{\"key\":%s}}";
        final String[] nulls = formatJsons(mapFmt, new String[] {
            null, "null", "{}"
        });
        final NullType[] expBytesForNulls = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY
        };

        runNestedTypedIndexKeyTest(table, indexName, mapValuesField, 20,
                                   mapValueFmt, nulls, expBytesForNulls,
                                   new JsonStringFormatter() {
            @Override
            public String createJsonString(String[] values) {
                return formatJsonString(mapSubFmt, elemFmt, values, true);
            }
        });

        /* Index on table(m.keys()) */
        String[] values = genStrings(20, 100);
        String jsonString = formatJsonString(mapSubFmt, elemFmt, values, true);
        runTestNestedJsonAsType(table, indexName, mapKeysField,
                                null /* FieldDef.Type */,
                                jsonString, values.length,
                                null, null, null);
    }

    /**
     * Typed index on map(json).values().sub
     */
    private void runMapOfJsonSubFieldAsTypeTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("m", TableBuilder.createMapBuilder().addJson().build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        final String indexName = "idxMap";
        final String fieldName = "m.values().sub";
        final String mapFmt = "{\"m\":%s}";
        final String mapSubFmt = "{\"m\":{%s}}";
        final String elemFmt = "\"%s\":{\"sub\":%s}";
        final String mapJsonSubValueFmt = "{\"m\":{\"key\":{\"sub\":%s}}}";
        final String[] nulls = formatJsons(mapFmt, new String[] {
            null,
            "null",
            "{}",
            "{\"key\":null}",
            "{\"key\":{\"sub\":null,\"b\":true}}",
            "{\"key\":{\"d\":1.1}}",
            "{\"key\":{}}"
        });
        final NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.JSON_NULL,
            NullType.EMPTY,
            NullType.EMPTY
        };

        runNestedTypedIndexKeyTest(table, indexName, fieldName, 20,
                                   mapJsonSubValueFmt, nulls, expNullTypes,
                                   new JsonStringFormatter() {
            @Override
            public String createJsonString(String[] values) {
                return formatJsonString(mapSubFmt, elemFmt, values, true);
            }
        });
    }

    private void testInvalidFieldPath() {
        TableBuilder builder = TableBuilder.createTableBuilder("json");
        builder.addInteger("id")
            .addField("r", TableBuilder.createRecordBuilder("r")
                               .addJson("json", null).build())
            .addField("a", TableBuilder.createArrayBuilder("a")
                               .addJson("json", null).build())
            .addField("m", TableBuilder.createMapBuilder("m")
                               .addJson("json", null).build())
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        IndexImpl index =
            addTypedIndex(table, "idxRJsonLong",
                          makeIndexList("r.json.a.l"),
                          makeIndexTypeList(FieldDef.Type.LONG));

        String[] invalidJsons = new String[] {
            "{\"id\":102,\"r\":{\"json\":[\"d\"]}}",
            "{\"id\":104,\"r\":{\"json\":{\"a\":[]}}}",
            "{\"id\":105,\"r\":{\"json\":{\"a\":{\"l\":[]}}}}",
            "{\"id\":106,\"r\":{\"json\":{\"a\":{\"l\":\"s\"}}}}",
            "{\"id\":107,\"r\":{\"json\":{\"a\":{\"l\":{\"a\":\"v\"}}}}}",
        };
        testJsonAsTypeInvalid(index, invalidJsons);

        index = addTypedIndex(table, "idxAJaJbJl",
                              makeIndexList("a[].ja.jb.jl"),
                              makeIndexTypeList(FieldDef.Type.LONG));
        invalidJsons = new String[] {
            "{\"id\":101,\"a\":[{\"ja\":[]}]}",
            "{\"id\":104,\"a\":[{\"ja\":{\"jb\":[]}}]}",
            "{\"id\":106,\"a\":[{\"ja\":{\"jb\":{\"jl\":\"d1\"}}}, " +
            "                   {\"ja\":{\"jb\":{\"jl\":1231}}}]}",
            "{\"id\":107,\"a\":[{\"ja\":{\"jb\":{\"jl\":{\"d1\":1}}}}]}",
        };
        testJsonAsTypeInvalid(index, invalidJsons);

        index = addTypedIndex(table, "idxMValuesABL",
                              makeIndexList("m.values().a.b.l"),
                              makeIndexTypeList(FieldDef.Type.LONG));
        invalidJsons = new String[] {
                "{\"id\":101,\"m\":{\"k1\":[]}}",
                "{\"id\":103,\"m\":{\"k1\":{\"a\":[]}}}",
                "{\"id\":105,\"m\":{\"k1\":{\"a\":{\"b\":[]}}}}",
                "{\"id\":106,\"m\":{\"k1\":{\"a\":{\"b\":{\"l\":\"s\"}}}}}",
                "{\"id\":107,\"m\":{\"k1\":{\"a\":{\"b\":{\"l\":[\"s\"]}}}}}",
                "{\"id\":108,\"m\":{\"k1\":{\"a\":{\"b\":{\"l\":{\"s\":1}}}}}}",
            };
        testJsonAsTypeInvalid(index, invalidJsons);

        index = addTypedIndex(table, "idxMK1ABL",
                              makeIndexList("m.k1.a.b.l"),
                              makeIndexTypeList(FieldDef.Type.LONG));
        testJsonAsTypeInvalid(index, invalidJsons);
    }

    private void runMultiKeyJsonKeysTest() {
        TableBuilder builder = TableBuilder.createTableBuilder("testJson");
        TableImpl table = builder.addInteger("id")
                                 .addJson("json", null)
                                 .primaryKey("id")
                                 .buildTable();

        /* Index on table(json.keys()} */
        String[] jsons = new String[] {
            "{\"id\":1,\"json\":{\"l\":1,\"s\":\"str\",\"n\":null}}",
            "{\"id\":2,\"json\":{\"a\":[1,2],\"r\":{\"ri\":1, \"rs\":\"s\"}}}",
            "{\"id\":3}",
            "{\"id\":4,\"json\":{}}",
            "{\"id\":5,\"json\":null}",
        };

        String[] nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",

            "{\"id\":101,\"json\":1}",
            "{\"id\":102,\"json\":\"test\"}",
            "{\"id\":102,\"json\":true}",
        };

        NullType[] expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        String[] invalidJsons = new String[] { "{\"id\":100,\"json\":[]}" };

        runTestJsonAsType(table, "idxJsonKeys", "json.keys()", null,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(json.values() as long) */
        jsons = new String[] {
            "{\"id\":1,\"json\":{\"l\":1,\"s\":2}}",
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
        };

        nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",

            "{\"id\":103,\"json\":1}"
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":100,\"json\":{\"l\":1,\"s\":\"S\"}}",
            "{\"id\":101,\"json\":{\"l\":1,\"s\":{\"A\":1}}}",
            "{\"id\":102,\"json\":[1]}"
        };

        runTestJsonAsType(table, "idxJsonValues", "json.values()",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(json.a.b.c.keys()} */
        jsons = new String[] {
            "{\"id\":1,\"json\":{\"a\":{\"l\":1,\"s\":\"String\"}}}",
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
            "{\"id\":5,\"json\":{\"b\":\"s\"}}",
            "{\"id\":6,\"json\":{\"a\":null}}",
            "{\"id\":7,\"json\":{\"a\":{}}}",
            "{\"id\":8,\"json\":{\"a\":{\"b\":{\"c\":{\"c1\":1,\"c2\":\"v2\",\"c3\":[true, false]}}}}}"
        };

        nulls = new String[] {
            "{\"id\":1,\"json\":{\"a\":{\"l\":1,\"s\":\"String\"}}}",
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
            "{\"id\":5,\"json\":{\"b\":\"s\"}}",
            "{\"id\":6,\"json\":{\"a\":null}}",
            "{\"id\":7,\"json\":{\"a\":{}}}",

            "{\"id\":101,\"json\":1}",
            "{\"id\":103,\"json\":{\"a\":1}}",
            "{\"id\":105,\"json\":{\"a\":{\"b\":1}}}",
            "{\"id\":106,\"json\":{\"a\":{\"b\":{\"c\":1}}}}",
        };

        expNullTypes = new NullType[] {
             NullType.EMPTY,
             NullType.SQL_NULL,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
             NullType.EMPTY,
        };

        invalidJsons = new String[] {
            "{\"id\":100,\"json\":[]}",
            "{\"id\":102,\"json\":{\"a\":[]}}",
            "{\"id\":104,\"json\":{\"a\":{\"b\":[]}}}",
            "{\"id\":107,\"json\":{\"a\":{\"b\":{\"c\":[]}}}}"
        };

        runTestJsonAsType(table, "idxJsonABCKeys", "json.a.b.c.keys()", null,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(json.values().l as long) */
        jsons = new String[] {
            "{\"id\":1,\"json\":{\"a\":{\"l\":1},\"s\":{\"l\":2}}}",
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
            "{\"id\":5,\"json\":{\"a\":{\"b\":1}}}",

            "{\"id\":102,\"json\":{\"a\":1,\"s\":{\"l\":2}}}",
        };

        nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
            "{\"id\":5,\"json\":{\"a\":{\"a\":1}}}",

            "{\"id\":101,\"json\":1}",
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":103,\"json\":{\"a\":[1]}}",
            "{\"id\":104,\"json\":{\"a\":{\"l\":{\"a\":1}}}}",
            "{\"id\":100,\"json\":[1]}"
        };

        runTestJsonAsType(table, "idxJsonValuesL", "json.values().l",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* json.a.values() as long) */
        jsons = new String[] {
            "{\"id\":1,\"json\":{\"a\":{\"l\":1,\"s\":2}}}",
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",
            "{\"id\":5,\"json\":{\"a\":{\"a\":100}}}",
        };

        nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"json\":{}}",
            "{\"id\":4,\"json\":null}",

            "{\"id\":101,\"json\":1}",
            "{\"id\":102,\"json\":{\"a\":1}}",
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":104,\"json\":{\"a\":{\"k\":true}}}",
            "{\"id\":105,\"json\":{\"a\":{\"l\":1,\"k\":[1]}}}",
            "{\"id\":106,\"json\":{\"a\":{\"l\":{\"s\":\"s\"},\"k\":[1]}}}",
            "{\"id\":100,\"json\":[1]}",
            "{\"id\":103,\"json\":{\"a\":[1]}}"
        };

        runTestJsonAsType(table, "idxJsonValuesL", "json.a.values()",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table (json[] as long) */
        jsons = new String[] {
            "{\"id\":1}",
            "{\"id\":2, \"json\":[1,2]}",
            "{\"id\":3, \"json\":[]}",
            "{\"id\":4, \"json\":null}",

            "{\"id\":100, \"json\":1}"
        };

        nulls = new String[] {
            "{\"id\":1}",
            "{\"id\":3, \"json\":[]}",
            "{\"id\":4, \"json\":null}",
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.JSON_NULL
        };

        invalidJsons = new String[] {
           "{\"id\":100, \"json\":{\"a\":1}}",
           "{\"id\":100, \"json\":[\"s\", \"s1\"]}",
        };

        runTestJsonAsType(table, "idxJsonArray", "json[]",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on (json[].l as long) */
        jsons = new String[] {
            "{\"id\":1}",
            "{\"id\":2, \"json\":[{\"l\":1},{\"l\":2}]}",
            "{\"id\":3, \"json\":[{\"a\":1}]}",
            "{\"id\":4, \"json\":[]}",
            "{\"id\":5, \"json\":null}",
        };

        nulls = new String[] {
            "{\"id\":1}",
            "{\"id\":3, \"json\":[{\"a\":1}]}",
            "{\"id\":4, \"json\":[]}",
            "{\"id\":5, \"json\":null}",

            "{\"id\":100, \"json\":{\"a\":1}}",
            "{\"id\":100, \"json\":1}",
            "{\"id\":100, \"json\":[\"s\", \"s1\"]}"
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":100, \"json\":[{\"l\":true}]}",
            "{\"id\":100, \"json\":[{\"l\":[1]}]}",
            "{\"id\":100, \"json\":[{\"l\":{\"a\":1}}]}",
        };

        runTestJsonAsType(table, "idxJsonArray", "json[].l",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(r.json.a.keys()) */
        builder = TableBuilder.createTableBuilder("testJson");
        table = builder.addInteger("id")
                .addField("r", TableBuilder.createRecordBuilder("r")
                                           .addJson("json", null).build())
                .primaryKey("id")
                .buildTable();

        jsons = new String[] {
            "{\"id\":1,\"r\":{\"json\":{\"a\":{\"l\":1,\"s\":{\"b\":\"b1\"}}}}}",
            "{\"id\":2,\"r\":{}}",
            "{\"id\":3,\"r\":null}",
            "{\"id\":4}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":null}}",
            "{\"id\":7,\"r\":{\"json\":{\"a\":null}}}",
            "{\"id\":8,\"r\":{\"json\":{\"b\":null}}}",
            "{\"id\":9,\"r\":{\"json\":{\"a\":{}}}}",
        };

        nulls = new String[] {
            "{\"id\":2,\"r\":{}}",
            "{\"id\":3,\"r\":null}",
            "{\"id\":4}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":null}}",
            "{\"id\":7,\"r\":{\"json\":{\"a\":null}}}",
            "{\"id\":8,\"r\":{\"json\":{\"b\":null}}}",
            "{\"id\":9,\"r\":{\"json\":{\"a\":{}}}}",

            "{\"id\":101,\"r\":{\"json\":1}}",
            "{\"id\":103,\"r\":{\"json\":{\"a\":1}}}",
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
        };

        invalidJsons = new String[] { 
            "{\"id\":100,\"r\":{\"json\":[]}}",
            "{\"id\":102,\"r\":{\"json\":{\"a\":[]}}}"
        };

        runTestJsonAsType(table, "idxRJsonAKeys", "r.json.a.keys()", null,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table (r.json.values() as long) */
        jsons = new String[] {
            "{\"id\":1,\"r\":{\"json\":{\"a\":1, \"b\":2}}}",
            "{\"id\":2}",
            "{\"id\":3,\"r\":{}}",
            "{\"id\":4,\"r\":null}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":null}}",
        };

        nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"r\":{}}",
            "{\"id\":4,\"r\":null}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":null}}",

            "{\"id\":100,\"r\":{\"json\":1}}"
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":102,\"r\":{\"json\":{\"a\":\"s\"}}}",
            "{\"id\":103,\"r\":{\"json\":{\"a\":[]}}}",
            "{\"id\":104,\"r\":{\"json\":{\"a\":{\"l\":1}}}}",
            "{\"id\":101,\"r\":{\"json\":[1]}}"
        };

        runTestJsonAsType(table, "idxRJsonAKeys", "r.json.values()",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table (r.json.r1.a[].l as long) */
        jsons = new String[] {
            "{\"id\":1,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":1},{\"s\":2,\"l\":3}]}}}}",
            "{\"id\":2}",
            "{\"id\":3,\"r\":null}",
            "{\"id\":4,\"r\":{\"json\":null}}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":{\"r1\":null}}}",
            "{\"id\":7,\"r\":{\"json\":{\"r1\":{}}}}",
            "{\"id\":8,\"r\":{\"json\":{\"s1\":{\"a\":[]}}}}",
            "{\"id\":9,\"r\":{\"json\":{\"r1\":{\"a\":null}}}}",
            "{\"id\":10,\"r\":{\"json\":{\"r1\":{\"a\":[]}}}}",
            "{\"id\":11,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":null}]}}}}",
            "{\"id\":12,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l1\":1}]}}}}",
        };

        nulls = new String[] {
            "{\"id\":2}",
            "{\"id\":3,\"r\":null}",
            "{\"id\":4,\"r\":{\"json\":null}}",
            "{\"id\":5,\"r\":{\"json\":{}}}",
            "{\"id\":6,\"r\":{\"json\":{\"r1\":null}}}",
            "{\"id\":7,\"r\":{\"json\":{\"r1\":{}}}}",
            "{\"id\":8,\"r\":{\"json\":{\"s1\":{\"a\":[]}}}}",
            "{\"id\":9,\"r\":{\"json\":{\"r1\":{\"a\":null}}}}",
            "{\"id\":10,\"r\":{\"json\":{\"r1\":{\"a\":[]}}}}",
            "{\"id\":11,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":null}]}}}}",
            "{\"id\":12,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l1\":1}]}}}}",

            "{\"id\":101,\"r\":{\"json\":1}}",
            "{\"id\":103,\"r\":{\"json\":{\"r1\":1}}}",
            "{\"id\":104,\"r\":{\"json\":{\"r1\":{\"a\":{}}}}}",
            "{\"id\":105,\"r\":{\"json\":{\"r1\":{\"a\":1}}}}",
            "{\"id\":106,\"r\":{\"json\":{\"r1\":{\"a\":[1]}}}}",

            "{\"id\":100,\"r\":{\"json\":[]}}",
            "{\"id\":102,\"r\":{\"json\":{\"r1\":[]}}}",
        };

        expNullTypes = new NullType[] {
            NullType.SQL_NULL,
            NullType.SQL_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.JSON_NULL,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,
            NullType.EMPTY,

            NullType.EMPTY,
            NullType.EMPTY
        };

        invalidJsons = new String[] {
            "{\"id\":107,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":\"s\"}]}}}}",
            "{\"id\":108,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":[]}]}}}}",
            "{\"id\":109,\"r\":{\"json\":{\"r1\":{\"a\":[{\"l\":{}}]}}}}",
        };

        runTestJsonAsType(table, "idxRJsonR1AL", "r.json[].r1[].a[].l",
                          FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);
    }

    /**
     * Test typed json index key serialization.
     */
    private void runTypedIndexKeyTest(TableImpl table, String indexName,
                                      String fieldName, int numValues,
                                      String fmtJson, String fmtInvalidJson,
                                      String[] nulls,
                                      NullType[] expNullTypes) {

        /* Index on table(<fieldName> as long) */
        String[] jsons = formatJsons(fmtJson, genLongs(numValues));
        String[] invalidJsons = formatJsons(fmtInvalidJson,
                                            getInvalidJsons(FieldDef.Type.LONG));
        runTestJsonAsType(table, indexName, fieldName, FieldDef.Type.LONG,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(<fieldName> as double)*/
        jsons = formatJsons(fmtJson, genDoubles(numValues));
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.DOUBLE));
        runTestJsonAsType(table, indexName, fieldName, FieldDef.Type.DOUBLE,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(<fieldName> as number)*/
        jsons = formatJsons(fmtJson, genBigDecimals(numValues));
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.NUMBER));
        runTestJsonAsType(table, indexName, fieldName, FieldDef.Type.NUMBER,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(<fieldName> as string) */
        jsons = formatJsons(fmtJson, genStrings(numValues, 100));
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.STRING));
        runTestJsonAsType(table, indexName, fieldName, FieldDef.Type.STRING,
                          jsons, nulls, expNullTypes, invalidJsons);

        /* Index on table(<fieldName> as boolean) */
        jsons = formatJsons(fmtJson, genBooleans());
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.BOOLEAN));
        runTestJsonAsType(table, indexName, fieldName, FieldDef.Type.BOOLEAN,
                          jsons, nulls, expNullTypes, invalidJsons);
    }

    private interface JsonStringFormatter{
        String createJsonString(String[] values);
    }

    private void runNestedTypedIndexKeyTest(TableImpl table, String indexName,
                                            String fieldName, int numKeys,
                                            String fmtInvalidJson,
                                            String[] nulls,
                                            NullType[] expNullTypes,
                                            JsonStringFormatter fmt) {

        /* Index on table(<fieldName> as long)*/
        String[] values = genLongs(numKeys);
        String jsonString = fmt.createJsonString(values);
        String[] invalidJsons = formatJsons(fmtInvalidJson,
                                            getInvalidJsons(FieldDef.Type.LONG));
        runTestNestedJsonAsType(table, indexName, fieldName,
                                FieldDef.Type.LONG,
                                jsonString, values.length,
                                nulls, expNullTypes,
                                invalidJsons);

        /* Index on table(<fieldName> as double)*/
        values = genDoubles(numKeys);
        jsonString = fmt.createJsonString(values);
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.DOUBLE));
        runTestNestedJsonAsType(table, indexName, fieldName,
                                FieldDef.Type.DOUBLE,
                                jsonString, values.length,
                                nulls, expNullTypes,
                                invalidJsons);

        /* Index on table(<fieldName> as number)*/
        values = genBigDecimals(numKeys);
        jsonString = fmt.createJsonString(values);
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.NUMBER));
        runTestNestedJsonAsType(table, indexName, fieldName,
                                FieldDef.Type.NUMBER,
                                jsonString, values.length,
                                nulls, expNullTypes,
                                invalidJsons);

        /* Index on table(<fieldName> as string)*/
        values = genStrings(numKeys, 100);
        jsonString = fmt.createJsonString(values);
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.STRING));
        runTestNestedJsonAsType(table, indexName, fieldName,
                                FieldDef.Type.STRING,
                                jsonString, numKeys,
                                nulls, expNullTypes,
                                invalidJsons);

        /* Index on table(<fieldName> as boolean) */
        values = genBooleans();
        jsonString = fmt.createJsonString(values);
        invalidJsons = formatJsons(fmtInvalidJson,
                                   getInvalidJsons(FieldDef.Type.BOOLEAN));
        runTestNestedJsonAsType(table, indexName, fieldName,
                                FieldDef.Type.BOOLEAN,
                                jsonString, values.length,
                                nulls, expNullTypes,
                                invalidJsons);
    }

    /**
     * Returns the bytes that represents the null values in Json index key.
     */
    private byte[] getNullBytes(NullType type) {
        switch (type) {
            case SQL_NULL:
                return new byte[]{unsigned(IndexImpl.NULL_INDICATOR_V1)};
            case JSON_NULL:
                return new byte[]{unsigned(IndexImpl.JSON_NULL_INDICATOR)};
            case EMPTY:
                return new byte[]{unsigned(IndexImpl.EMPTY_INDICATOR)};
            default:
                fail("Unexpected json null type: " + type);
        }
        return null;
    }

    /**
     * Returns invalid JSON values for the given type.
     */
    private String[] getInvalidJsons(FieldDef.Type type) {
        switch(type) {
            case LONG:
                return new String[] {
                    "-9223372036854775809",
                    "9223372036854775808",
                    "1.1",
                    "\"jack jones\"",
                    "false",
                    "[10,20]",
                    "{}"
                };
            case DOUBLE:
                return new String[] {
                    "\"jack jones\"",
                    "false",
                    "[10,20]",
                    "{}"
                };
            case NUMBER:
                return new String[] {
                    "\"jack jones\"",
                    "false",
                    "[10,20]",
                    "{}"
                };
            case STRING:
                return new String[] {
                    "true",
                    "1234.1",
                    "[10,20]",
                    "{}"
                };
            case BOOLEAN:
                return new String[] {
                    "\"jack jones\"",
                    "1234",
                    "[10,20]",
                    "{}"
                };
            default:
                fail("Invalid type for typed JSON index");
        }
        return null;
    }

    private String[] formatJsons(String fmt, String[] jsonValues) {
        String[] jsons = new String[jsonValues.length];
        for (int i = 0; i < jsonValues.length; i++) {
            String s = jsonValues[i];
            if (s != null) {
                jsons[i] = String.format(fmt, jsonValues[i]);
            } else {
                jsons[i] = null;
            }
        }
        return jsons;
    }

    private String formatJsonString(String fmt,
                                    String elemFmt,
                                    String[] elemValues) {
        return formatJsonString(fmt, elemFmt, elemValues, false);
    }

    private String formatJsonString(String fmt,
                                    String elemFmt,
                                    String[] elemValues,
                                    boolean hasKey) {

        if (elemValues == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elemValues.length; i++) {
            String value = elemValues[i];
            if (sb.length() > 0) {
                sb.append(",");
            }

            String e;
            if (elemFmt != null) {
                if (hasKey) {
                    e = String.format(elemFmt, "key" + i, value);
                } else {
                    e = String.format(elemFmt, value);
                }
            } else {
                e = value;
            }
            sb.append(e);
        }
        return String.format(fmt, sb.toString());
    }

    private void runTestJsonAsType(TableImpl table,
                                   String indexName,
                                   String indexFieldName,
                                   FieldDef.Type type,
                                   String[] jsons,
                                   String[] nulls,
                                   NullType[] expNullTypes,
                                   String[] invalidJsonValues) {

        IndexImpl index = addTypedIndex(table, indexName,
                                        makeIndexList(indexFieldName),
                                        ((type != null) ?
                                          makeIndexTypeList(type) : null));
        assertTrue(index != null);
        testJsonAsType(index, jsons);
        testJsonAsTypeNulls(index, nulls, expNullTypes);
        testJsonAsTypeInvalid(index, invalidJsonValues);
        table.removeIndex(indexName);
    }

    private void runTestNestedJsonAsType(TableImpl table,
                                         String indexName,
                                         String indexFieldName,
                                         FieldDef.Type type,
                                         String jsonString,
                                         int numJsonValues,
                                         String[] nulls,
                                         NullType[] expNullTypes,
                                         String[] invalidJsonValues) {

        List<FieldDef.Type> types = (type != null) ?
                                    makeIndexTypeList(type) : null;
        IndexImpl index = addTypedIndex(table, indexName,
                                        makeIndexList(indexFieldName), types);
        assertTrue(index != null);
        testJsonAsType(index, jsonString, numJsonValues);
        if (nulls != null) {
            testJsonAsTypeNulls(index, nulls, expNullTypes);
        }
        if (invalidJsonValues != null) {
            testJsonAsTypeInvalid(index, invalidJsonValues);
        }
        table.removeIndex(indexName);
    }

    private IndexImpl addTypedIndex(TableImpl table, String indexName,
                                    List<String> fields,
                                    List<FieldDef.Type> types) {

        table.addIndex(new IndexImpl(indexName, table, fields, types, null));
        return (IndexImpl)table.getIndex(indexName);
    }

    private void testJsonAsType(IndexImpl index, String... jsons) {

        JEMap map = new JEMap();
        for (int i = 0; i < jsons.length; i++) {
            Row row = createRowWithJsonField(index.getTable(), i, jsons[i]);
            putKeysToJEMap(index, row, map);
        }
        map.validate();
    }

    private void testJsonAsType(IndexImpl index, String json, int numKeys) {

        JEMap map = new JEMap();
        Row row = createRowWithJsonField(index.getTable(), 0, json);
        putKeysToJEMap(index, row, map);
        map.validate();
        assertTrue(map.size() == numKeys);
    }

    private void testJsonAsTypeNulls(IndexImpl index,
                                     String[] jsons,
                                     NullType[] expNullTypes) {

        for (int i = 0; i < jsons.length; i++) {
            Row row = createRowWithJsonField(index.getTable(), i, jsons[i]);
            byte[] bytes;
            if (index.isMultiKey()) {
                bytes = index.extractIndexKeys((RowImpl)row, maxKeys).get(0);
            } else {
                bytes = index.extractIndexKey((RowImpl)row);
            }

            byte[] expBytes = getNullBytes(expNullTypes[i]);
            assertTrue(bytes.length == expBytes.length);
            /*
            if (!Arrays.equals(bytes, expBytes)) {

                System.out.println("json doc " + i + ": " + jsons[i]);

                System.out.println("Actual key bytes: ");
                for (Byte b : bytes) {
                    System.out.println(b + ", ");
                }

                System.out.println("Expected key bytes: ");
                for (Byte b : expBytes) {
                    System.out.println(b + ", ");
                }
            }
            */
            assertTrue(Arrays.equals(bytes, expBytes));
        }
    }

    private Row createRowWithJsonField(TableImpl table, int id, String json) {

        final RowImpl row = table.createRow();
        row.put("id", id);

        if (json != null) {
            ComplexValueImpl.createFromJson(
                row,
                new ByteArrayInputStream(json.getBytes()),
                false/*exact*/);
        } else {
            row.addMissingFields();
        }

        return row;
    }

    public void testJsonAsTypeInvalid(IndexImpl index, String[] invalidJsons) {
        for (String s : invalidJsons) {
            Row row = index.getTable().createRowFromJson(s, false);
            row.put("id", 0);
            try {
                if (index.isMultiKey()) {
                    index.extractIndexKeys((RowImpl)row, maxKeys);
                } else {
                    index.extractIndexKey((RowImpl)row);
                }
                fail("Expected to catch IAE but not");
            } catch (IllegalArgumentException ignored) {
            }
        }
    }

    private byte unsigned(byte b) {
        return (byte)(b ^ 0x80);
    }

    private String[] genLongs(int num) {
        String[] values = new String[num];
        for (int i = 0; i < num; i++) {
            long l;
            switch (i) {
            case 0:
                l = Long.MIN_VALUE;
                break;
            case 1:
                l = Long.MAX_VALUE;
                break;
            case 2:
                l = 0;
                break;
            default:
                l = rand.nextLong();
            }
            values[i] = String.valueOf(l);
        }
        return values;
    }

    private String[] genDoubles(int num) {
        String[] values = new String[num];
        for (int i = 0; i < num; i++) {
            double d;
            switch(i) {
            case 0:
                d = Double.MIN_VALUE;
                break;
            case 1:
                d = Double.MAX_VALUE;
                break;
            case 2:
                d = -Double.MIN_VALUE;
                break;
            case 3:
                d = -Double.MAX_VALUE;
                break;
            case 4:
                d = 0.0;
                break;
            default:
                d = rand.nextDouble();
            }
            values[i] = String.valueOf(d);
        }
        return values;
    }

    private String[] genBigDecimals(int num) {
        String[] values = new String[num];
        for (int i = 0; i < num; i++) {
            BigDecimal bd;
            switch(i % 2) {
            case 0:
                bd = BigDecimal.valueOf(rand.nextLong());
                break;
            default:
                bd = BigDecimal.valueOf(rand.nextDouble());
                break;
            }
            values[i] = bd.toString();
        }
        return values;
    }

    private String[] genBooleans() {
        return new String[]{String.valueOf(true), String.valueOf(false)};
    }

    private String[] genStrings(int num, int maxLen) {
        Set<String> values = new HashSet<String>();
        for (int i = 0; i < num; i++) {
            if (!values.add(genString(maxLen))) {
                i--;
            }
        }
        return values.toArray(new String[num]);
    }

    private String genString(int maxLen) {
        int len = rand.nextInt(maxLen - 1) + 1;
        int start = rand.nextInt(26);
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        for (int i = 0; i < len; i++) {
            char ch = (char)('A' + (start + i) % 26);
            sb.append(ch);
        }
        sb.append("\"");
        return sb.toString();
    }

    private RowImpl addTimestampTableRow(Table table, int i, Timestamp ts) {
        RowImpl row = (RowImpl) table.createRow();
        row.put("id", i);
        row.put("ts0", ts);
        row.put("ts1", ts);
        row.put("ts2", ts);
        row.put("ts3", ts);
        row.put("ts4", ts);
        row.put("ts5", ts);
        row.put("ts6", ts);
        row.put("ts7", ts);
        row.put("ts8", ts);
        row.put("ts9", ts);
        row.putRecord("rec").put("ts1", ts).put("ts3", ts).put("ts9", ts);
        row.putArray("arr").add(ts);
        row.putMap("map").put("key1", ts);
        return row;
    }

    private Timestamp genTimestamp(int i, int nRows) {
        if (i == nRows / 2) {
            return TimestampDefImpl.MIN_VALUE;
        }
        if (i == nRows / 2  + 1) {
            return TimestampDefImpl.MAX_VALUE;
        }
        Timestamp ts = new Timestamp(System.currentTimeMillis() + i * 3600000);
        ts.setNanos(987654321);
        return ts;
    }

    /* Ignore warnings about Index.createIndexKey */
    @SuppressWarnings("deprecation")
    @Test
    public void testNumberFields() {
        TableImpl table = TableBuilder.createTableBuilder("DecimalTable")
            .addInteger("id")
            .addNumber("dec1")
            .addNumber("dec2")
            .addField("rec", TableBuilder.createRecordBuilder("rec")
                              .addInteger("rid")
                              .addNumber("dec1")
                              .build())
            .addField("arr", TableBuilder.createArrayBuilder("arr")
                              .addNumber()
                              .build())
            .addField("map", TableBuilder.createMapBuilder("map")
                              .addNumber()
                              .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        IndexImpl idx_dec1 = new IndexImpl("idx_dec1", table,
                                           makeIndexList("dec1"), null, null);
        table.addIndex(idx_dec1);

        IndexImpl idx_dec1_dec2 = new IndexImpl("idx_dec1_dec2", table,
                                          makeIndexList("dec1", "dec2"), null, null);
        table.addIndex(idx_dec1_dec2);

        IndexImpl idx_misc_recDec1_recRid_desc1 =
            new IndexImpl("idx_rec_dec_rid", table,
                          makeIndexList("rec.dec1", "rec.rid", "dec1"), null, null);
        table.addIndex(idx_misc_recDec1_recRid_desc1);

        IndexImpl idx_arr_dec = new IndexImpl("idx_arr_dec", table,
                                              makeIndexList("arr[]"), null, null);
        table.addIndex(idx_arr_dec);

        IndexImpl idx_map_dec = new IndexImpl("idx_map_dec", table,
                                              makeIndexList("map.valueS()"),
                                              null, null);
        table.addIndex(idx_map_dec);

        IndexImpl idx_misc =
            new IndexImpl("idx_misc", table,
                          makeIndexList("map.valueS()", "dec1", "rec.dec1"),
                          null, null);
        table.addIndex(idx_misc);

        int nRows = 100;
        JEMap[] jeMaps = new JEMap[table.getIndexes().size()];
        for (int i = 0; i < nRows; i++) {
            Row row = addDecimalTableRow(table, i);
            int idx = 0;
            for (String name : table.getIndexes().keySet()) {
                IndexImpl index = (IndexImpl) table.getIndexes().get(name);
                JEMap jeMap = jeMaps[idx];
                if (jeMap == null) {
                    jeMap = new JEMap();
                    jeMaps[idx] = jeMap;
                }
                if (index.isMultiKey()) {
                    byte[] key = table.createKey(row, false).toByteArray();
                    byte[] data = table.createValue(row).toByteArray();
                    List<byte[]> indexKeys =
                        index.extractIndexKeys(key, data, 0, 0, 0, 0, false, maxKeys);
                    for (byte[] buf : indexKeys) {
                        IndexKeyImpl idxKey = index.deserializeIndexKey(buf,
                                                                        false);
                        jeMap.put(buf, idxKey.clone());
                    }
                } else {
                    IndexKeyImpl key = index.createIndexKey(row);
                    byte[] serialized = index.serializeIndexKey(key);
                    jeMap.put(serialized, key.clone());
                }
                idx++;
            }
        }

        for (int i = 0; i < jeMaps.length; i++) {
            jeMaps[i].validate();
        }
    }

    private BigDecimal genDecimal(int i) {
        switch (i % 5) {
        case 0:
            return BigDecimal.valueOf(rand.nextInt());
        case 1:
            return BigDecimal.valueOf(rand.nextLong());
        case 2:
            return BigDecimal.valueOf(rand.nextFloat());
        case 3:
            return BigDecimal.valueOf(rand.nextDouble());
        default:
            return new BigDecimal(BigInteger.valueOf(rand.nextLong()),
                                  rand.nextInt());
        }
    }

    private Row addDecimalTableRow(Table table, int i) {
        Row row = table.createRow();
        row.put("id", i);
        row.putNumber("dec1", genDecimal(i++));
        row.putNumber("dec2", genDecimal(i++));
        row.putRecord("rec").put("rid", i)
                            .putNumber("dec1", genDecimal(i++));
        row.putArray("arr").addNumber(genDecimal(i++))
                           .addNumber(genDecimal(i++))
                           .addNumber(genDecimal(i++));
        row.putMap("map").putNumber("key1", genDecimal(i++))
                         .putNumber("key2", genDecimal(i++))
                         .putNumber("key3", genDecimal(i++));
        return row;
    }

    @Test
    public void testEmptyValue() {
        runEmptyValueTest(IndexImpl.INDEX_VERSION_V0);
        runEmptyValueTest(IndexImpl.INDEX_VERSION_V1);
    }

    private void runEmptyValueTest(int serialVersion) {

        setIndexSerialVersion(serialVersion);

        /*
         * create table(id integer,
         *              as array(string),
         *              ms map(string),
         *              r  record(rid integer,
         *                        rs string,
         *                        ra array(record(ri integer, rs string)),
         *                        rm map(record(ri integer, rs string))))
         */
        TableBuilder builder = (TableBuilder)
            TableBuilder.createTableBuilder("testTable")
                .addInteger("id")
                .addField("as",
                          TableBuilder.createArrayBuilder().addString().build())
                .addField("ms",
                          TableBuilder.createMapBuilder().addString().build())
                .addField("r",
                     TableBuilder.createRecordBuilder("r")
                         .addInteger("rid")
                         .addString("rs")
                         .addField("ra",
                                   TableBuilder.createArrayBuilder().addField
                                       (TableBuilder.createRecordBuilder("rar")
                                            .addInteger("ri")
                                            .addString("rs")
                                            .build())
                                       .build())
                         .addField("rm",
                                   TableBuilder.createMapBuilder().addField
                                       (TableBuilder.createRecordBuilder("rmr")
                                            .addInteger("ri")
                                            .addString("rs")
                                            .build())
                                   .build())
                      .build())
                .primaryKey("id");
        TableImpl table = builder.buildTable();

        IndexImpl idxAs = new IndexImpl("idxAs", table,
                                        makeIndexList("as[]"), null, null);
        table.addIndex(idxAs);

        IndexImpl idxMs = new IndexImpl("idxMs", table,
                                        makeIndexList("ms.keys()",
                                                      "ms.values()",
                                                      "ms.aa"), null, null);
        table.addIndex(idxMs);

        IndexImpl idxRecRa = new IndexImpl("idxRecRA", table,
                                           makeIndexList("r.ra[].ri",
                                                         "r.ra[].rs"),
                                           null, null);
        table.addIndex(idxRecRa);

        IndexImpl idxRecRm = new IndexImpl("idxRecRm", table,
                                           makeIndexList("r.rm.keys()",
                                                         "r.rm.values().rs",
                                                         "r.rm.key1.rs"),
                                           null, null);
        table.addIndex(idxRecRm);

        /* Index on testTable(as[]) */
        String[] jsons = new String[] {
            "{\"id\":1}",
            "{\"id\":2, \"as\":null}",
            "{\"id\":3, \"as\":[]}"
        };
        String[] expKeyStrings = new String[] {
            "{\"as[]\":null}",
            "{\"as[]\":null}",
            "{\"as[]\":\"EMPTY\"}",
        };
        runIndexKeySerDeSer(table, idxAs, jsons, expKeyStrings);

        /* Index on testTable(ms.keys(), ms.values(), ms.aa) */
        jsons = new String[] {
            "{\"id\":1}",
            "{\"id\":2, \"ms\":null}",
            "{\"id\":3, \"ms\":{}}",
            "{\"id\":4, \"ms\":{\"k1\":\"v1\"}}",
        };
        expKeyStrings = new String[] {
            "{\"ms.keys()\":null,\"ms.values()\":null,\"ms.aa\":null}",
            "{\"ms.keys()\":null,\"ms.values()\":null,\"ms.aa\":null}",
            "{\"ms.keys()\":\"EMPTY\",\"ms.values()\":\"EMPTY\",\"ms.aa\":\"EMPTY\"}",
            "{\"ms.keys()\":\"k1\",\"ms.values()\":\"v1\",\"ms.aa\":\"EMPTY\"}",
        };
        runIndexKeySerDeSer(table, idxMs, jsons, expKeyStrings);

        /* Index on testTable(r.ra[].ri, r.ra[].rs) */
        jsons = new String[] {
            "{\"id\":1, \"r\":{\"ra\":null}}",
            "{\"id\":2, \"r\":{\"ra\":[]}}",
            "{\"id\":3, \"r\":{\"ra\":[{\"ri\":1}]}}",
            "{\"id\":4, \"r\":{\"ra\":[{\"rs\":\"ss\"}]}}",
        };
        expKeyStrings = new String[] {
            "{\"r.ra[].ri\":null,\"r.ra[].rs\":null}",
            "{\"r.ra[].ri\":\"EMPTY\",\"r.ra[].rs\":\"EMPTY\"}",
            "{\"r.ra[].ri\":1,\"r.ra[].rs\":null}",
            "{\"r.ra[].ri\":null,\"r.ra[].rs\":\"ss\"}",
        };
        runIndexKeySerDeSer(table, idxRecRa, jsons, expKeyStrings);

        /* Index on testTable(r.rm.keys(), r.rm.values().rs, r.rm.key1.rs) */
        jsons = new String[] {
            "{\"id\":1, \"r\":{\"rm\":null}}",
            "{\"id\":2, \"r\":{\"rm\":{}}}",
            "{\"id\":3, \"r\":{\"rm\":{\"key2\":{\"rs\":\"s2\"}}}}",
            "{\"id\":4, \"r\":{\"rm\":{\"key1\":{\"rs\":\"s1\"}}}}",
        };
        expKeyStrings = new String[] {
            "{\"r.rm.keys()\":null," +
             "\"r.rm.values().rs\":null," +
             "\"r.rm.key1.rs\":null}",

            "{\"r.rm.keys()\":\"EMPTY\"," +
             "\"r.rm.values().rs\":\"EMPTY\"," +
             "\"r.rm.key1.rs\":\"EMPTY\"}",

             "{\"r.rm.keys()\":\"key2\"," +
             "\"r.rm.values().rs\":\"s2\"," +
             "\"r.rm.key1.rs\":\"EMPTY\"}",

             "{\"r.rm.keys()\":\"key1\"," +
             "\"r.rm.values().rs\":\"s1\"," +
             "\"r.rm.key1.rs\":\"s1\"}"
        };
        runIndexKeySerDeSer(table, idxRecRm, jsons, expKeyStrings);
    }

    private void runIndexKeySerDeSer(Table table, IndexImpl index,
                                     String[] jsons,
                                     String[] expKeyStrings) {

        assert(jsons.length == expKeyStrings.length);

        for (int i = 0; i < jsons.length; i++) {

            RowImpl row = (RowImpl)table.createRow();

            /*
             * The following call will fill-in any missing fields with
             * their default values.
             */
            ComplexValueImpl.createFromJson(
                row,
                new ByteArrayInputStream(jsons[i].getBytes()),
                false/*exact*/);

            List<byte[]> list = index.extractIndexKeys(row, maxKeys);
            assertTrue(list.size() == 1);

            IndexKeyImpl key = index.deserializeIndexKey(list.get(0), false);
            String expKeyStr =
                (index.getIndexVersion() == IndexImpl.INDEX_VERSION_V1) ?
                expKeyStrings[i] : expKeyStrings[i].replace("\"EMPTY\"", "null");

            String keyJson = key.toJsonString(false);

            if (!keyJson.equals(expKeyStr)) {
                System.out.println("Keys do not match.\nexpected key = " +
                                   expKeyStr + "\nactual key = " + keyJson);
            }

            assertTrue(keyJson.equals(expKeyStr));
        }
    }

    private void setIndexSerialVersion(int version) {
        System.setProperty(IndexImpl.INDEX_SERIAL_VERSION,
                           String.valueOf(version));
    }

    private void putKeysToJEMap(IndexImpl index, Row row, JEMap map) {

        if (index.isMultiKey()) {

            List<byte[]> serkeys = index.extractIndexKeys((RowImpl)row,
                                                          maxKeys);
            if (serkeys == null) {
                return;
            }

            for (byte[] serkey : serkeys) {
                IndexKeyImpl key = index.deserializeIndexKey(serkey, false);
                map.put(serkey, key);
            }

        } else {
            byte[] serkey = index.extractIndexKey((RowImpl)row);
            if (serkey == null) {
                return;
            }

            IndexKeyImpl key = index.deserializeIndexKey(serkey, false);
            map.put(serkey, key);
        }
    }

    void add(JEMap map, IndexImpl index, IndexKeyImpl key, int count) {
        for (int i = 0; i < count; i++) {
            key.put("int3", Integer.MIN_VALUE + i);
            map.put(index.serializeIndexKey(key), key.clone());
            key.put("int3", 0 - i);
            map.put(index.serializeIndexKey(key), key.clone());
            key.put("int3", 0 + i);
            map.put(index.serializeIndexKey(key), key.clone());
            key.put("int3", Integer.MAX_VALUE - i);
            map.put(index.serializeIndexKey(key), key.clone());
        }
    }

    private IndexKeyImpl fromIndexKey(IndexImpl index, byte[] bytes,
                                      boolean allowPartial) {
        return index.deserializeIndexKey(bytes, allowPartial);
    }

    /**
     * A local class that maps byte[] to IndexKeyImpl and uses the JE byte comparator
     * as a comparator.  This is used to emulate ordering in a JE database without
     * actually creating a database.
     */
    static class JEMap implements Comparator<byte[]> {
        final TreeMap<byte[], IndexKeyImpl> map;

        JEMap() {
            map = new TreeMap<byte[], IndexKeyImpl>(this);
        }

        void put(byte[] k, IndexKeyImpl v) {
            map.put(k, v);
        }

        IndexKeyImpl get(byte[] k) {
            return map.get(k);
        }

        boolean exists(byte[] k) {
            return map.containsKey(k);
        }

        Map<byte[], IndexKeyImpl> getMap() {
            return map;
        }

        int size() {
            return map.entrySet().size();
        }

        void validate() {
            IndexKeyImpl last = null;
            for (IndexKeyImpl ikey : map.values()) {
                if (last != null) {
                    assertTrue(last.compareTo(ikey) < 0);
                }
                last = ikey;
            }
        }

        /**
         * Comparator<byte[]>
         */
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compareBytes(o1, o2);
        }

        void clear() {
            map.clear();
        }
    }

    static int compareBytes(byte[] checkKey, byte[] borderKey) {
        return IndexImpl.compareUnsignedBytes(checkKey, borderKey);
    }

    static private RowImpl addUserRow(Table table, long id, String first,
                                      String last, Integer age, Float height,
                                      String type, Double totalTime) {
        RowImpl row = (RowImpl) table.createRow();
        row.put("id", id);
        if (first != null) {
            row.put("firstName", first);
        } else {
            row.putNull("firstName");
        }

        if (last != null) {
            row.put("lastName", last);
        } else {
            row.putNull("lastName");
        }

        if (age != null) {
            row.put("age", age);
        } else {
            row.putNull("age");
        }

        if (height != null) {
            row.put("height", height);
        } else {
            row.putNull("height");
        }

        if (type != null) {
            row.putEnum("type", type);
        } else {
            row.putNull("type");
        }

        if (totalTime != null) {
            row.put("totalTime", totalTime);
        } else {
            row.putNull("totalTime");
        }
        return row;
    }

    static RowImpl addContactRow(Table table, int id, String[] types) {
        RowImpl row = (RowImpl)table.createRow();
        row.put("id", (long)id);
        row.put("firstName", getFirstName(id, false));
        row.put("lastName", getLastName(id, false));
        row.put("age", getAge(id, false));
        row.put("height", getHeight(id, false));
        row.put("totalTime", getTotalTime(id, false));
        row.putEnum("type", getType(id, types, false));
        if (id % 3 == 0) {
            row.putNull("address");
        } else {
            row.putRecord("address")
                .put("city", "city" + id)
                .put("street", "stree" + id);
        }
        if (id % 6 == 0) {
            row.putNull("email");
        } else {
            row.putArray("email")
                .add("emailA" + id)
                .add("emailB" + id);
        }
        if (id % 9 == 0) {
            row.putNull("phone");
        } else {
            row.putMap("phone")
                .put("home", "phoneA" + id)
                .put("work", "phongB" + id);
        }
        return row;
    }

    static String getFirstName(int id, boolean enabledNull) {
        int mode = id % 10;
        if (enabledNull && mode == 1) {
            return null;
        } else if (mode == 2) {
            return "";
        }
        return "firstName" + id;
    }

    static String getLastName(int id, boolean enabledNull) {
        int mode = (id + 1) % 10;
        if (enabledNull && mode == 1) {
            return null;
        } else if (mode == 2) {
            return "";
        }
        return "lastName" + id;
    }

    static Integer getAge(int id, boolean enabledNull) {
        int mode = (id + 2) % 10;
        if (enabledNull && mode == 1) {
            return null;
        } else if (mode == 2) {
            return 0;
        } else if (mode == 3) {
            return Integer.MAX_VALUE;
        } else if (mode == 4) {
            return Integer.MIN_VALUE;
        }
        return id % 80;
    }

    static Float getHeight(int id, boolean enabledNull) {
        int mode = (id + 3) % 10;
        if (enabledNull && mode == 1) {
            return null;
        } else if (mode == 2) {
            return 0.0f;
        } else if (mode == 3) {
            return Float.MAX_VALUE;
        } else if (mode == 4) {
            return Float.MIN_VALUE;
        }
        float ret = id * 0.9999f;
        return ((id % 2) == 0 ? ret : -1 * ret);
    }

    static String getType(int id, String[] values, boolean enabledNull) {
        int mode = (id + 4) % 10;
        if (enabledNull && mode == 1) {
            return null;
        }
        return values[id % values.length];
    }

    static Double getTotalTime(int id, boolean enabledNull) {
        int mode = (id + 5) % 10;
        if (enabledNull && mode == 1) {
            return null;
        } else if (mode == 2) {
            return 0.0d;
        } else if (mode == 3) {
            return Double.MAX_VALUE;
        } else if (mode == 4) {
            return Double.MIN_VALUE;
        }
        double ret = id * 1.00000001d;
        return ((id % 2) == 0 ? ret : -1 * ret);
    }

    static void setIndexEnableNullSupport(boolean enabled) {
        System.setProperty(IndexImpl.INDEX_NULL_DISABLE,
                           String.valueOf(!enabled));
    }
}
