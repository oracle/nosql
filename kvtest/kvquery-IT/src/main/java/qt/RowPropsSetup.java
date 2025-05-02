/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Properties;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import qt.framework.QTDefaultImpl;
import qt.framework.QTest;

/**
 * Setup implementation for case rowprops.
 *
 * The rowprops tests the partition()/shard() and other row properties related
 * SQL functions, the results of parition()/shard() functions are related to
 * key distribution of the test table "foo". Although the rowprop test is always
 * run first, its table id can be also variable depended on the number of system
 * table.
 *
 * To make sure the table will be assigned with a "fixed" id, dummy tables will
 * be created before create the test tables. The start table id of test tables
 * can be specified in test.config using property "table-xxx-id".
 * e.g. table-foo-id = 22. If table with the specified id already existed in the
 * store or 1st dummy table was assigned with id > target table id, then test
 * run failed.
 */
public class RowPropsSetup extends QTDefaultImpl{
    private final static String TABLE_ID_PREFIX = "table-";
    private final static String TABLE_ID_SUFFIX = "-id";
    private final static String DUMMY_TABLE_PREFIX = "before_rowprops";

    @Override
    public void setConfigProperties(Properties properties) {
        super.setConfigProperties(properties);
        if (!configProperties.containsKey("before-ddl-file")) {
            configProperties.setProperty("before-ddl-file", "before.ddl");
        }
        if (!configProperties.containsKey("after-ddl-file")) {
            configProperties.setProperty("after-ddl-file", "after.ddl");
        }
    }

    @Override
    public void before() {
        opts.verbose("Run Before: RowProps");

        String tableName = null;
        int targetId = 0;

        /*
         * Read property "table-xxx-id=<n>" that specifies the table id of
         * the target table "xxx".
         */
        String key;
        for (Map.Entry<Object, Object> e : configProperties.entrySet()) {
            key = ((String) e.getKey()).toLowerCase();

            if (key.startsWith(TABLE_ID_PREFIX) &&
                key.endsWith(TABLE_ID_SUFFIX)) {

                tableName = key.substring(TABLE_ID_PREFIX.length(),
                                (key.length() - TABLE_ID_SUFFIX.length()));
                assertTrue(tableName.length() > 0);

                targetId = Integer.parseInt((String)e.getValue());
                opts.verbose(key + "=" + targetId);

                break;
            }
        }

        /*
         * Create dummy tables to make sure the target table will be assigned
         * with the id specified.
         */
        if (targetId > 0) {
            if (QTest.store.getTableAPI().getTableById(targetId) != null) {
                fail("The table with id = " + targetId + " already exists");
            }
            createDummyTablesToId(targetId - 1);
        }

        /* Run ddl statements in before.ddl */
        super.before();

        /* Verify the id of target table */
        if (targetId > 0) {
            long id = ((TableImpl)QTest.store.getTableAPI()
                        .getTable(tableName)).getId();
            assertEquals("target table id, expected="+ targetId +
                         ", actual=" + id, targetId, id);
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: RowProps");
        super.after();
        dropDummyTables();
    }

    private void createDummyTablesToId(int targetId) {
        final String ddl = "create table if not exists %s(k string, " +
                           "primary key(k))";
        final TableAPI tableAPI = QTest.store.getTableAPI();

        long id = 0;
        String name;
        boolean first = true;
        do {
            name = DUMMY_TABLE_PREFIX + (id + 1);
            executeStatement(String.format(ddl, name));

            id = ((TableImpl)tableAPI.getTable(name)).getId();
            opts.verbose("Created dummy table " + name + ", table id = " + id);
            if (first) {
                if (id > targetId) {
                    fail("The start table id " + (targetId + 1)  +
                         " is too low, the table id of 1st dummy table '" +
                         name + "' is " + id);
                }
                first = false;
            }
        } while (id < targetId);
    }

    private void dropDummyTables() {
        for (Map.Entry<String, Table> e:
             QTest.store.getTableAPI().getTables().entrySet()) {
            if (e.getKey().startsWith(DUMMY_TABLE_PREFIX)) {
                executeStatement("drop table " + e.getKey());
            }
        }
    }
}
