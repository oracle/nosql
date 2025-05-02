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

import oracle.kv.FaultException;
import oracle.kv.KeySizeLimitException;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class LimitsTest extends TableTestBase {
    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after MRTable is put on cloud.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp();
    }

    @Test
    public void testIndexKeyLimits() throws Exception {
        String ddl = "create table users (" +
            "id integer, " +
            "name String, " +
            "friends array(String), " +
            "relatives map(String), " +
            "primary key(id))";

        String indexDdl = "create index name on users(name)";
        String arrayIndexDdl = "create index array on users(friends[])";
        String mapIndexDdl = "create index map on users(relatives.values())";

        String jsonRow = "{\"id\":1, \"name\": \"joe\"," +
            "\"friends\":[], " +
            "\"relatives\":{}}";

        TableLimits limits = new TableLimits(500,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             32); /* index key size */
        createTable(ddl, limits);
        executeDdl(indexDdl);
        executeDdl(arrayIndexDdl);
        executeDdl(mapIndexDdl);

        Table table = tableImpl.getTable("users");
        Row row = table.createRowFromJson(jsonRow, false);

        /*
         * Initial put should consume 4k write:
         *  1 -- the row
         *  1 for each index. Empty maps and arrays still consume a single
         * entry, indicating an empty index.
         */
        Result result =
            tableImpl.putInternal((RowImpl)row, null, null);
        assertNotNull(result.getNewVersion());
        assertEquals(4, result.getWriteKB());

        /*
         * Start near the key size limit and add entries for the 2
         * complex indexes.
         *
         * JE doesn't rewrite existing entries, so each new put only
         * counts the basic record plus the single additional index key
         */
        int elemSize = 29;
        try {
            for (; elemSize <= 33; elemSize++) {
                ArrayValue array = row.get("friends").asArray();
                String element = createLongName('a', elemSize);
                array.add(element);
                MapValue map = row.get("relatives").asMap();
                map.put(Integer.toString(elemSize), element);
                result =
                    tableImpl.putInternal((RowImpl)row, null, null);

                /*
                 * The first write to the empty map/array indexes adds
                 * 2 writes for removal of the empty index key records.
                 * In general these will be 4:
                 * 2 -- row rewrite (1 delete, 1 add)
                 * 1 for each index -- one new element/index key added
                 */
                int expectedWrite = (elemSize == 29 ? 6 : 4);
                assertEquals(expectedWrite, result.getWriteKB());
                assertEquals(0, result.getReadKB());
                if (elemSize > 32) {
                    fail("Index key size limit should have been reached");
                }
            }
        } catch (KeySizeLimitException e) {
            if (elemSize <= 31) {
                fail("Index key size limit was not reached: " + elemSize);
            }
        }
    }

    /*
     * Tests specific index size limits using string fields in the index
     * to validate that the extra string length indicator is not included
     * in the size enforcement
     */
    @Test
    public void testIndexKeyLimits1() throws Exception {
        String ddl = "create table users (" +
            "id String, " +
            "name String, " +
            "s1 String, " +
            "s2 String, " +
            "primary key(shard(id),name))";

        /*
         * Index on primary key fields do not include an extra "null" indicator
         * in the entry so they work up to the full limits. Nullable (not
         * primary key) fields include the extra indicator...
         */
        String indexDdl = "create index idname on users(id,name)";
        String indexDdl1 = "create index s1s2 on users(s1,s2)";

        String jsonRow = "{\"id\":\"1\", \"name\": \"1\"}";

        TableLimits limits = new TableLimits(500,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             TableLimits.NO_LIMIT,
                                             64); /* index key size */
        createTable(ddl, limits);
        executeDdl(indexDdl);
        executeDdl(indexDdl1);

        Table table = tableImpl.getTable("users");
        Row row = table.createRowFromJson(jsonRow, false);
        String element = createLongName('a', 32);
        row = table.createRow();
        row.put("id", element);
        row.put("s1", element);
        boolean exceptionSeen = false;
        for (int i = 1; i <=35; i++) {
            element = createLongName('a', i);
            row.put("name", element);
            row.put("s2", element);
            try {
                tableImpl.putInternal((RowImpl)row, null, null);
                assertTrue(i <= 32);
            } catch (Exception e) {
                assertEquals(i, 33);
                exceptionSeen = true;
                break;
            }
        }
        assertTrue(exceptionSeen);
    }

    /**
     * Prepare of DML statements is counted so queries should be throttled
     */
    @Test
    public void testDMLThrottling() throws Exception {

        final String ddl = "create table users (" +
            "id integer, " +
            "name String, " +
            "primary key(id))";

        /* use select and update */
        final String query = "select * from users";
        final String updQuery = "update users u set name = 'x' where id = 1";

        createTable(ddl, null);

        testPrepare(query, true);
        testPrepare(updQuery, true);
    }

    /**
     * Prepare of DDL statements is NOT counted.
     */
    @Test
    public void testDDLThrottling() throws Exception {

        final String ddl = "create table users (" +
            "id integer, " +
            "name String, " +
            "primary key(id))";

        /* use a few different DDL statements */
        final String indexQ = "create index if not exists idx on users(name)";
        final String alterQ = "alter table users USING TTL 3 days";
        final String dropQ = "drop index if exists no_index on users";

        createTable(ddl, null);

        testPrepare(indexQ, false);
        testPrepare(alterQ, false);
        testPrepare(dropQ, false);
    }

    private void testPrepare(String query, boolean shouldBeThrottled)
        throws Exception {

        TableImpl t = (TableImpl)tableImpl.getTable("users");

        /* remove any limits */
        setTableLimits(t, new TableLimits(TableLimits.NO_LIMIT,
                                          TableLimits.NO_LIMIT,
                                          10));

        loopPrepare(query);

        /* Set write throughput limits but no read */
        setTableLimits(t, new TableLimits(TableLimits.NO_LIMIT, 1, 10));

        loopPrepare(query);

        /* Set a large read limit */
        setTableLimits(t, new TableLimits(100, 1, 10));

        loopPrepare(query);

        /*
         * Set a small enough read limit that the prepare should fail after
         * only a couple of executions. (min. query cost is 2KB)
         */
        setTableLimits(t, new TableLimits(1, 1, 10));

        try {
            loopPrepare(query);
        } catch (FaultException expected) {
            if (!shouldBeThrottled) {
                fail("Query shouldn't have been throttled");
            }
            return;
        }
        if (shouldBeThrottled) {
            fail("Query was not throttled");
        }
    }


    private void loopPrepare(String query) {
        for (int i = 0; i < 20; i++) {
            prepare(query);
        }
    }
}
