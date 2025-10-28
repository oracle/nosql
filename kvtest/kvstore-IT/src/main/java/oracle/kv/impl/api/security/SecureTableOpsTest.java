/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.security;

import static oracle.kv.impl.api.security.OpAccessCheckTestUtils.testDeniedTableDeleteOps;
import static oracle.kv.impl.api.security.OpAccessCheckTestUtils.testValidTableDeleteOps;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.*;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReturnValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.security.KVStorePrivilege.PrivilegeType;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the table data operation access check.  In this test, we modify the
 * privileges of the testing user by changing the granted privileges of the
 * role granted to the testing user.
 */
public class SecureTableOpsTest extends SecureTestBase {

    private static final String SUPER_USER = "super";
    private static final String SUPER_USER_PWD = "NoSql00__super123";

    private static final String TEST_USER = "test";
    private static final String TEST_USER_PWD = "NoSql00__test123";

    private static final String TEST_TABLE = "test_table";
    private static final String TEST_CHILD_TABLE = "test_table.child";
    private static final String TEST_TABLE_B = "test_table_b";
    private static final String TEST_TTL_TABLE = "test_ttl_table";
    private static final String TEST_TABLE_DEF =
        " (id integer, name string, primary key(id))";
    private static final String TEST_CHILD_TABLE_DEF =
        " (email string, address string, primary key (email))";
    private static final String TEST_TABLE_B_DEF =
        " (id integer, name string, salary integer, primary key(id))";

    private static final String TEST_TABLE_INDEX = "test_name";
    private static final String TEST_TABLE_INDEX_FIELD = "name";

    private static final String TEST_TABLE_ROW_JSON_STR =
        "{\"id\":1, \"name\":\"jim\"}";

    private static final String TEST_CHILD_TABLE_INDEX = "test_child_addr";
    private static final String TEST_CHILD_TABLE_INDEX_FIELD = "address";

    private static final String TEST_CHILD_TABLE_ROW_JSON_STR =
        "{\"id\":1, \"email\":\"test@\", \"address\":\"earth\"}";

    private static final String TEST_TABLE_B_ROW_JSON_STR =
        "{\"id\":1, \"name\":\"jim\", \"salary\":3000}";
    private static final String NS = "ns";
    private static final String NS_TEST_TABLE = NS + ":" + TEST_TABLE;
    private static final String NS_TEST_CHILD_TABLE =
        NS + ":" + TEST_CHILD_TABLE;
    private static final String NS_TEST_TTL_TABLE = NS + ":" + TEST_TTL_TABLE;

    private static final Map<String, String> tableRowMap =
        new HashMap<String, String>();

    static {
        tableRowMap.put(TEST_CHILD_TABLE, TEST_CHILD_TABLE_ROW_JSON_STR);
        tableRowMap.put(TEST_TABLE, TEST_TABLE_ROW_JSON_STR);
        tableRowMap.put(TEST_TTL_TABLE, TEST_TABLE_ROW_JSON_STR);
        tableRowMap.put(TEST_TABLE_B, TEST_TABLE_B_ROW_JSON_STR);
        tableRowMap.put(NS_TEST_TABLE, TEST_TABLE_ROW_JSON_STR);
        tableRowMap.put(NS_TEST_CHILD_TABLE, TEST_CHILD_TABLE_ROW_JSON_STR);
        tableRowMap.put(NS_TEST_TTL_TABLE, TEST_TABLE_ROW_JSON_STR);
    }

    private static final String TEST_ROLE = "testrole";

    private static KVStore superUserStore;
    private static KVStore testUserStore;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(SUPER_USER, SUPER_USER_PWD, true /* admin */));
        users.add(new SecureUser(TEST_USER, TEST_USER_PWD, false /* admin */));
        startup();

        initStores();
        prepareTest();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        closeStores();
        shutdown();
    }

    @Override
    protected void clearTestDirectory() {
        /* do not clean test directory */
    }

    @Test
    public void testTableOwnership() throws Exception {
        /* Add some data to test_table, which is owned by super_user */
        Row row = createOneRowForTable(TEST_TABLE);
        TableAPI testUserTableAPI = testUserStore.getTableAPI();
        IndexKey idxKey =
            row.getTable().getIndex(TEST_TABLE_INDEX).createIndexKey();

        /*
         * Non-owner user could not do any access operation on TEST_TABLE without
         * explicit grant
         */
        try {
            populateTableWithOneRow(superUserStore, row);

            final TableImpl testTableImpl =
                (TableImpl) testUserTableAPI.getTable(TEST_TABLE);
            assertNotEquals(testTableImpl.getOwner().name(), TEST_USER);

            testDeniedInsertOps(testUserStore, row);
            testDeniedReadOps(testUserStore, row, idxKey);
            testDeniedDeleteOps(testUserStore, row);
        } finally {
            cleanOneRowFromTable(superUserStore, row);
        }

        /*
         * Build another table using account of test_user, and tests that all
         * access operations are valid even without any grant.
         */
        final String anotherTable = "another_table";
        try {
            grantPrivToRole(TEST_ROLE, CREATE_ANY_TABLE, CREATE_ANY_INDEX);
            execStatement(testUserStore,
                          "create table " + anotherTable +
                          " (id integer, idxfield string, primary key(id))");
            execStatement(testUserStore,
                          "create index idx1 on " + anotherTable +
                          " (idxfield)");
            final Table table = testUserTableAPI.getTable(anotherTable);
            row = table.createRow();
            row.put("id", 1);
            row.put("idxfield", "t1");
            idxKey = table.getIndex("idx1").createIndexKey();

            testValidInsertOps(row);

            populateTableWithOneRow(superUserStore, row);

            testValidReadOps(testUserStore, row, idxKey);
            testValidDeleteOps(row);
        } finally {
            /* clean up */
            execStatement(testUserStore,
                          "drop table if exists " + anotherTable);
            revokePrivFromRole(TEST_ROLE, CREATE_ANY_TABLE, CREATE_ANY_INDEX);
        }
    }

    @Test
    public void testTableReadPermission() throws Exception {
        final Row row = createOneRowForTable(TEST_TABLE);
        final IndexKey idxKey =
            row.getTable().getIndex(TEST_TABLE_INDEX).createIndexKey();

        populateTableWithOneRow(superUserStore, row);

        try {
            /* Removed all privileges enabling table read */
            revokePrivFromRole(TEST_ROLE, READ_ANY_TABLE, READ_ANY);
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, READ_TABLE);

            /* all read ops are denied */
            testDeniedReadOps(testUserStore, row, idxKey);

            /* test read_table */
            try {
                grantPrivToRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
                testValidReadOps(testUserStore, row, idxKey);
            } finally {
                revokePrivFromRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
                testDeniedReadOps(testUserStore, row, idxKey);
            }

            /* test read_any_table */
            try {
                grantPrivToRole(TEST_ROLE, READ_ANY_TABLE);
                testValidReadOps(testUserStore, row, idxKey);
            } finally {
                revokePrivFromRole(TEST_ROLE, READ_ANY_TABLE);
                testDeniedReadOps(testUserStore, row, idxKey);
            }

            /* test read_any */
            try {
                grantPrivToRole(TEST_ROLE, READ_ANY);
                testValidReadOps(testUserStore, row, idxKey);
            } finally {
                revokePrivFromRole(TEST_ROLE, READ_ANY);
                testDeniedReadOps(testUserStore, row, idxKey);
            }
        } finally {
            cleanOneRowFromTable(superUserStore, row);
        }
    }

    @Test
    public void testTableInsertPermission() throws Exception {
        final Row row = createOneRowForTable(TEST_TABLE);

        /* Removed all privileges enabling table insert */
        revokePrivFromRole(TEST_ROLE, WRITE_ANY, INSERT_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);

        /* all read ops are denied */
        testDeniedInsertOps(testUserStore, row);

        /* test insert_table */
        try {
            grantPrivToRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            testValidInsertOps(row);
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, row);
        }

        /* test insert_any_table */
        try {
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE);
            testValidInsertOps(row);
        } finally {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, row);
        }

        /* test write_any */
        try {
            grantPrivToRole(TEST_ROLE, WRITE_ANY);
            testValidInsertOps(row);
        } finally {
            revokePrivFromRole(TEST_ROLE, WRITE_ANY);
            testDeniedInsertOps(testUserStore, row);
        }
    }

    private static String RMD = "{\"row metadata\":1}";

    @Test
    public void testTableRowMetadata() throws Exception {
        final Row row = createOneRowForTable(NS_TEST_TABLE);
        final PrimaryKey pk = row.createPrimaryKey();
        pk.setRowMetadata(RMD);

        /* Removed all privileges */
        revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE, DELETE_TABLE);

        row.setRowMetadata(RMD);
        /* all inserts and deletes with row_metadata ops are denied */
        testDeniedInsertOps(testUserStore, row);
        testDeniedTableDeleteOps(testUserStore, pk);

        /* tests table privileges */
        try {
            /* grant INSERT_TABLE, insert with row_metadata should work */
            grantPrivToRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE);
            testValidInsertOps(row);
            testDeniedTableDeleteOps(testUserStore, pk);
            revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE);

            /* grant DELETE_TABLE, delete with row_metadata should work */
            grantPrivToRole(TEST_ROLE, NS_TEST_TABLE, DELETE_TABLE);
            testDeniedInsertOps(testUserStore, row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, DELETE_TABLE);

            /*
             * grant INSERT_TABLE and DELETE_TABLE, both insert and
             * delete with row_metadata should work
             */
            grantPrivToRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE,
                            DELETE_TABLE);
            testValidInsertOps(row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE,
                               DELETE_TABLE);
        } finally {
            revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, INSERT_TABLE,
                               DELETE_TABLE);
            testDeniedInsertOps(testUserStore, row);
            testDeniedTableDeleteOps(testUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
        }

        /* test namespace privileges */
        try {
            /* grant INSERT_IN_NAMESPACE, insert with row_metadata should work */
            grantNsPrivToRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);
            testValidInsertOps(row);
            testDeniedTableDeleteOps(testUserStore, pk);
            revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);

            /* grant DELETE_IN_NAMESPACE, delete with row_metadata should work */
            grantNsPrivToRole(TEST_ROLE, NS, DELETE_IN_NAMESPACE);
            testDeniedInsertOps(testUserStore, row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokeNsPrivFromRole(TEST_ROLE, NS, DELETE_IN_NAMESPACE);

            /*
             * grant INSERT_IN_NAMESPACE and DELETE_IN_NAMESPACE, both insert
             * and delete with row_metadata should work
             */
            grantNsPrivToRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                DELETE_IN_NAMESPACE);
            testValidInsertOps(row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                DELETE_IN_NAMESPACE);
        } finally {
            revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                DELETE_IN_NAMESPACE);
            testDeniedInsertOps(testUserStore, row);
            testDeniedDeleteOps(testUserStore, row);
            cleanOneRowFromTable(superUserStore, row);
        }

        /* test system privileges */
        try {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);

            /* grant INSERT_ANY_TABLE, insert with row_metadata should work */
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE);
            testValidInsertOps(row);
            testDeniedTableDeleteOps(testUserStore, pk);
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE);

            /* grant DELETE_ANY_TABLE, delete with row_metadata should work */
            grantPrivToRole(TEST_ROLE, DELETE_ANY_TABLE);
            testDeniedInsertOps(testUserStore, row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokePrivFromRole(TEST_ROLE, DELETE_ANY_TABLE);

            /*
             * grant INSERT_ANY_TABLE and DELETE_ANY_TABLE, both insert and
             * delete with row_metadata should work
             */
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
            testValidInsertOps(row);
            populateTableWithOneRow(superUserStore, row);
            testValidTableDeleteOps(testUserStore, superUserStore, pk);
            cleanOneRowFromTable(superUserStore, row);
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
        } finally {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
            testDeniedInsertOps(testUserStore, row);
            testDeniedDeleteOps(testUserStore, row);
            cleanOneRowFromTable(superUserStore, row);
        }
    }

    @Test
    public void testTableDeletePermission() throws Exception {
        final Row row = createOneRowForTable(TEST_TABLE);

        populateTableWithOneRow(superUserStore, row);

        try {
            /* Removed all privileges enabling table delete */
            revokePrivFromRole(TEST_ROLE, WRITE_ANY, DELETE_ANY_TABLE);
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE);

            /* all delete ops are denied */
            testDeniedDeleteOps(testUserStore, row);

            /* test delete_table */
            try {
                grantPrivToRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE);
                testValidDeleteOps(row);
            } finally {
                revokePrivFromRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE);
                testDeniedDeleteOps(testUserStore, row);
            }

            /* test delete_any_table */
            try {
                grantPrivToRole(TEST_ROLE, DELETE_ANY_TABLE);
                testValidDeleteOps(row);
            } finally {
                revokePrivFromRole(TEST_ROLE, DELETE_ANY_TABLE);
                testDeniedDeleteOps(testUserStore, row);
            }

            /* test write_any */
            try {
                grantPrivToRole(TEST_ROLE, WRITE_ANY);
                testValidDeleteOps(row);
            } finally {
                revokePrivFromRole(TEST_ROLE, WRITE_ANY);
                testDeniedDeleteOps(testUserStore, row);
            }
        } finally {
            cleanOneRowFromTable(superUserStore, row);
        }
    }

    @Test
    public void testMultiDelete() throws Exception {
        final Row parentRow = createOneRowForTable(NS_TEST_TABLE);
        final Row childRow = createOneRowForTable(NS_TEST_CHILD_TABLE);
        final PrimaryKey parentKey = parentRow.createPrimaryKey();
        final PrimaryKey childKey = childRow.createPrimaryKey();

        populateTableWithOneRow(superUserStore, parentRow);
        populateTableWithOneRow(superUserStore, childRow);

        revokePrivFromRole(TEST_ROLE, WRITE_ANY, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, NS_TEST_TABLE, DELETE_TABLE);
        revokePrivFromRole(TEST_ROLE, NS_TEST_CHILD_TABLE, DELETE_TABLE);

        try {
            testMultiDeleteInternal(parentRow, childRow, parentKey, childKey,
                                    DELETE_TABLE, DELETE_IN_NAMESPACE,
                                    DELETE_ANY_TABLE);
        } finally {
            cleanOneRowFromTable(superUserStore, parentRow);
            cleanOneRowFromTable(superUserStore, childRow);
        }
    }

    private void testMultiDeleteInternal(Row parentRow,
                                         Row childRow,
                                         PrimaryKey parentKey,
                                         PrimaryKey childKey,
                                         KVStorePrivilegeLabel tablePriv,
                                         KVStorePrivilegeLabel nsPriv,
                                         KVStorePrivilegeLabel sysPriv)
        throws Exception {

        final String parentTable = parentRow.getTable().getFullNamespaceName();
        final String childTable = childRow.getTable().getFullNamespaceName();

        testDeniedMultiDelete(parentKey, childKey);

        grantPrivToRole(TEST_ROLE, parentTable, tablePriv);
        try {
            testDeniedMultiDeleteChild(parentKey, childKey);
        } finally {
            revokePrivFromRole(TEST_ROLE, parentTable, tablePriv);
        }

        grantPrivToRole(TEST_ROLE, childTable, tablePriv);
        try {
            testDeniedMultiDeleteParent(parentKey, childKey);
        } finally {
            revokePrivFromRole(TEST_ROLE, childTable, tablePriv);
        }

        grantPrivToRole(TEST_ROLE, parentTable, tablePriv);
        grantPrivToRole(TEST_ROLE, childTable, tablePriv);
        try {
            testValidMultiDelete(parentRow, childRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, parentTable, tablePriv);
            revokePrivFromRole(TEST_ROLE, childTable, tablePriv);
        }

        grantNsPrivToRole(TEST_ROLE, NS, nsPriv);
        try {
            testValidMultiDelete(parentRow, childRow);
        } finally {
            revokeNsPrivFromRole(TEST_ROLE, NS, nsPriv);
        }

        grantPrivToRole(TEST_ROLE, sysPriv);
        try {
            testValidMultiDelete(parentRow, childRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, sysPriv);
        }
    }

    private void testDeniedMultiDelete(PrimaryKey parentKey,
                                       PrimaryKey childKey) {
        final TableAPI testTableAPI = testUserStore.getTableAPI();
        new OpAccessCheckTestUtils.TestDeniedExecution() {
            @Override
            void perform() {
                testTableAPI.multiDelete(parentKey, null, null);
            }
        }.exec();
        new OpAccessCheckTestUtils.TestDeniedExecution() {
            @Override
            void perform() {
                testTableAPI.multiDelete(childKey, null, null);
            }
        }.exec();
    }

    private void testDeniedMultiDeleteChild(PrimaryKey parentKey,
                                            PrimaryKey childKey) {
        final MultiRowOptions includeChild = new MultiRowOptions(
            null, null, Arrays.asList(childKey.getTable()));
        new OpAccessCheckTestUtils.TestDeniedExecution() {
            @Override
            void perform() {
                testUserStore.getTableAPI().multiDelete(
                    parentKey, includeChild, null);
            }
        }.exec();
    }

    private void testDeniedMultiDeleteParent(PrimaryKey parentKey,
                                             PrimaryKey childKey) {
        final MultiRowOptions includeParent = new MultiRowOptions(
            null, Arrays.asList(parentKey.getTable()), null);
        new OpAccessCheckTestUtils.TestDeniedExecution() {
            @Override
            void perform() {
                testUserStore.getTableAPI().multiDelete(
                    childKey, includeParent, null);
            }
        }.exec();
    }

    private void testValidMultiDelete(Row parentRow, Row childRow) {
        final PrimaryKey parentKey = parentRow.createPrimaryKey();
        final PrimaryKey childKey = childRow.createPrimaryKey();
        final TableAPI testTableAPI = testUserStore.getTableAPI();
        final MultiRowOptions includeParent = new MultiRowOptions(
            null, Arrays.asList(parentRow.getTable()), null);
        final MultiRowOptions includeChild = new MultiRowOptions(
            null, null, Arrays.asList(childRow.getTable()));
        try {
            assertEquals(2, testTableAPI.multiDelete(
                parentKey, includeChild,null));
        } finally {
            populateTableWithOneRow(superUserStore, parentRow);
            populateTableWithOneRow(superUserStore, childRow);
        }
        try {
            assertEquals(2, testTableAPI.multiDelete(
                childKey, includeParent, null));
        } finally {
            populateTableWithOneRow(superUserStore, parentRow);
            populateTableWithOneRow(superUserStore, childRow);
        }
    }

    @Test
    public void testPrivOnParentAndChildTable() throws Exception {
        final Row row = createOneRowForTable(TEST_CHILD_TABLE);
        final IndexKey idxKey =
            row.getTable().getIndex(TEST_CHILD_TABLE_INDEX)
                .createIndexKey();

        /* Test table read */
        revokePrivFromRole(TEST_ROLE, READ_ANY, READ_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_CHILD_TABLE, READ_TABLE);

        try {
            populateTableWithOneRow(superUserStore, row);

            /* Read privilege on parent table does not apply to child tables */
            grantPrivToRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
            testDeniedReadOps(testUserStore, row, idxKey);

            /*
             * Should be OK to read child table with read privileges on both
             * parent and child tables
             */
            grantPrivToRole(TEST_ROLE, TEST_CHILD_TABLE, READ_TABLE);
            testValidReadOps(testUserStore, row, idxKey);

            /*
             * Read privilege on only child table is not sufficient to read
             * the child table
             */
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
            grantPrivToRole(TEST_ROLE, TEST_CHILD_TABLE, READ_TABLE);
            testDeniedReadOps(testUserStore, row, idxKey);

        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
            revokePrivFromRole(TEST_ROLE, TEST_CHILD_TABLE, READ_TABLE);
            cleanOneRowFromTable(superUserStore, row);
        }

        /* Test table insert */
        revokePrivFromRole(TEST_ROLE, WRITE_ANY, INSERT_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_CHILD_TABLE, INSERT_TABLE);

        try {
            /* Insert privilege on parent table does not apply to child table */
            grantPrivToRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, row);

            /* Should be OK with insert on both child and parent table */
            grantPrivToRole(TEST_ROLE, TEST_CHILD_TABLE, INSERT_TABLE);
            testValidInsertOps(row);

            /* Insert priv on only child table is not sufficient */
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, row);

            /* Should be OK with read on parent table */
            grantPrivToRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
            testValidInsertOps(row);

        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE,
                READ_TABLE);
            revokePrivFromRole(TEST_ROLE, TEST_CHILD_TABLE, INSERT_TABLE);
        }

        revokePrivFromRole(TEST_ROLE, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_CHILD_TABLE, DELETE_TABLE);

        try {
            populateTableWithOneRow(superUserStore, row);

            /* Delete privilege on parent table does not apply to child table*/
            grantPrivToRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE);
            testDeniedDeleteOps(testUserStore, row);

            /* Should be OK with delete priv on both child and parent table */
            grantPrivToRole(TEST_ROLE, TEST_CHILD_TABLE, DELETE_TABLE);
            testValidDeleteOps(row);

            /* Delete priv on only child table is not sufficient */
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE);
            testDeniedDeleteOps(testUserStore, row);

            /* Should be OK with read priv on parent table */
            grantPrivToRole(TEST_ROLE, TEST_TABLE, READ_TABLE);
            testValidDeleteOps(row);

        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, DELETE_TABLE,
                READ_TABLE);
            cleanOneRowFromTable(superUserStore, row);
        }
    }

    @Test
    public void testBypassTableAccessCheck()
        throws Exception {

        try {
            grantPrivToRole(TEST_ROLE, TEST_TABLE,
                            READ_TABLE, INSERT_TABLE, DELETE_TABLE);
            final Table testTable =
                testUserStore.getTableAPI().getTable(TEST_TABLE);
            final Table testTableB =
                testUserStore.getTableAPI().getTable(TEST_TABLE_B);
            assertNotNull(testTableB);
            final String rowJson = tableRowMap.get(TEST_TABLE_B);
            assertNotNull(rowJson);

            /* Build a row using test table B */
            final RowImpl row =
                (RowImpl)testTableB.createRowFromJson(rowJson, true);

            /* Identify id of test table */
            final long testTableId = ((TableImpl) testTable).getId();
            final KVStoreImpl storeImpl = (KVStoreImpl) testUserStore;

            /*
             * Exercise internal single-operation execution methods,
             * each method pass id of test table that test user have permission
             * to access but key in test table B.
             */
            final DeniedOp[] ops = new DeniedOp[] {
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        doPutOp(storeImpl,
                                InternalOperation.OpCode.PUT,
                                row.getPrimaryKey(false),
                                row.createValue(),
                                null,
                                testTableId);
                    }
                },
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        doPutOp(storeImpl,
                                InternalOperation.OpCode.PUT_IF_ABSENT,
                                row.getPrimaryKey(false),
                                row.createValue(),
                                null,
                                testTableId);
                    }
                },
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        doPutOp(storeImpl,
                                InternalOperation.OpCode.PUT_IF_VERSION,
                                row.getPrimaryKey(false),
                                row.createValue(),
                                OpAccessCheckTestUtils.dummyVer,
                                testTableId);
                    }
                },
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        storeImpl.getInternal(row.getPrimaryKey(false),
                                              testTableId,
                                              null /* Consistency */,
                                              0 /* Timeout */,
                                              null /* Timeout unit */);
                    }
                },
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        storeImpl.deleteInternal(row.getPrimaryKey(false),
                                                 null /* ReturnValueVersion */,
                                                 null /* Durability */,
                                                 0 /* Timeout */,
                                                 null /* Timeout Unit*/,
                                                 testTableId);
                    }
                },
                new DeniedOp() {

                    @Override
                    public void execute() throws FaultException {
                        storeImpl.deleteIfVersionInternal(
                            row.getPrimaryKey(false),
                            OpAccessCheckTestUtils.dummyVer,
                            null /* ReturnValueVersion */,
                            null /* Durability */,
                            0 /* Timeout */,
                            null /* Timeout Unit*/,
                            testTableId);
                    }
                }
            };

            new DeniedOpExecution() {

                @Override
                void assertException(UnauthorizedException uae) {
                    tableIdMismatchFailure(uae);
                }
            }.perform(ops);

            /*
             * Drop test table b so that the key passed in cannot identify
             * a table
             */
            execStatement(superUserStore, "drop table " + TEST_TABLE_B);

            new DeniedOpExecution() {

                @Override
                void assertException(UnauthorizedException uae) {
                    tableNotFoundFailure(uae);
                }
            }.perform(ops);
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE,
                               READ_TABLE, INSERT_TABLE, DELETE_TABLE);
        }
    }

    private void doPutOp(KVStoreImpl storeImpl,
                         InternalOperation.OpCode op,
                         Key key,
                         Value value,
                         Version version,
                         long tableId) {

        storeImpl.doPutInternal(
            storeImpl.makePutRequest(op,
                                     key,
                                     value,
                                     ReturnValueVersion.Choice.NONE,
                                     tableId,
                                     null,
                                     0,
                                     null,
                                     null,
                                     false,
                                     version),
            null);
    }

    @Test
    public void testTTLTableAccessControl() throws Exception {
        testTTLAccessControl(TEST_TTL_TABLE, TEST_TABLE);
    }

    @Test
    public void testTTLNamespaceAccessControl() throws Exception {
        testTTLAccessControl(NS_TEST_TTL_TABLE, NS_TEST_TABLE);
    }

    private void testTTLAccessControl(String ttlTable, String nonTtlTable)
        throws Exception {

        Row ttlRow = createOneRowForTable(ttlTable);
        Row nonTTLRow = createOneRowForTable(nonTtlTable);

        /* Removed all privileges enabling table insert */
        revokePrivFromRole(TEST_ROLE, WRITE_ANY,
                           INSERT_ANY_TABLE, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, ttlTable,
                           INSERT_TABLE, DELETE_TABLE);
        revokePrivFromRole(TEST_ROLE, nonTtlTable,
                           INSERT_TABLE, DELETE_TABLE);
        revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                             DELETE_IN_NAMESPACE);

        /* all insert ops are denied */
        testDeniedInsertOps(testUserStore, ttlRow);

        /*
         * test insert_table only, all insert ops against a table
         * having ttl defined are denied
         */
        testDeniedTTLWithoutDelete(ttlRow, nonTTLRow);

        /*
         * test insert_table only, all insert ops with explicitly set ttl
         * as zero against a table having default ttl defined are passed
         */
        ttlRow.setTTL(TimeToLive.DO_NOT_EXPIRE);
        testValidNoTTL(ttlRow);

        /*
         * test insert_table and delete_table, all insert ops against a table
         * having ttl defined are passed
         */
        ttlRow = createOneRowForTable(ttlTable);
        testValidTTLWithDelete(ttlRow);

        /*
         * test insert_table only, try perform inserts with explicitly TTL
         * setting against a table having TTL default defined.
         */
        ttlRow.setTTL(TimeToLive.ofDays(10));
        testDeniedTTLWithoutDelete(ttlRow, nonTTLRow);

        /*
         * test insert_table and delete_table, try perform inserts with
         * explicitly TTL setting against a table having TTL default passed.
         */
        testValidTTLWithDelete(ttlRow);

        /*
         * test insert_table only, try perform inserts without TTL setting
         * against a table not having TTL default defined are passed.
         */
        nonTTLRow = createOneRowForTable(nonTtlTable);
        testValidNoTTL(nonTTLRow);

        /*
         * test insert_table only, try perform inserts with TTL as zero setting
         * against a table not having TTL default defined are passed.
         */
        nonTTLRow.setTTL(TimeToLive.ofDays(0));
        testValidNoTTL(nonTTLRow);

        /*
         * test insert_table only, try perform inserts with TTL setting against
         * a table not having TTL default defined are denied.
         */
        ttlRow = createOneRowForTable(ttlTable);
        ttlRow.setTTL(TimeToLive.ofDays(10));
        testDeniedTTLWithoutDelete(ttlRow, nonTTLRow);

        /*
         * test insert_table and delete_table only, try perform inserts with
         * TTL setting against a table not having TTL default defined are passed.
         */
        testValidTTLWithDelete(ttlRow);
    }

    private void testDeniedTTLWithoutDelete(Row ttlRow, Row nonTTLRow)
        throws Exception {
        String ttlTable = ttlRow.getTable().getFullNamespaceName();
        String nonTTLTable = nonTTLRow.getTable().getFullNamespaceName();
        try {
            grantPrivToRole(TEST_ROLE, ttlTable, INSERT_TABLE);
            testDeniedTTLInsertOps(testUserStore, ttlRow);

            grantPrivToRole(TEST_ROLE, nonTTLTable, INSERT_TABLE);
            testValidInsertOps(nonTTLRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, ttlTable, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, ttlRow);

            revokePrivFromRole(TEST_ROLE, nonTTLTable, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, nonTTLRow);
        }
        if (ttlRow.getTable().getNamespace().equals(NS)) {
            try {
                grantNsPrivToRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);
                testDeniedTTLInsertOps(testUserStore, ttlRow);
                testValidInsertOps(nonTTLRow);
            } finally {
                revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);
                testDeniedInsertOps(testUserStore, ttlRow);
                testDeniedInsertOps(testUserStore, nonTTLRow);
            }
        }
        try {
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedTTLInsertOps(testUserStore, ttlRow);

            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE);
            testValidInsertOps(nonTTLRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, ttlRow);

            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, nonTTLRow);
        }
        try {
            grantPrivToRole(TEST_ROLE, WRITE_ANY);
            testValidInsertOps(ttlRow);
            grantPrivToRole(TEST_ROLE, WRITE_ANY);
            testValidInsertOps(nonTTLRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, WRITE_ANY);
            testDeniedInsertOps(testUserStore, ttlRow);
            revokePrivFromRole(TEST_ROLE, WRITE_ANY);
            testDeniedInsertOps(testUserStore, nonTTLRow);
        }
    }

    private void testValidNoTTL(Row nonTtlRow) throws Exception {
        String table = nonTtlRow.getTable().getFullNamespaceName();
        try {
            grantPrivToRole(TEST_ROLE, table, INSERT_TABLE);
            testValidInsertOps(nonTtlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, table, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, nonTtlRow);
        }
        if (nonTtlRow.getTable().getNamespace().equals(NS)) {
            try {
                grantNsPrivToRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);
                testValidInsertOps(nonTtlRow);
            } finally {
                revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE);
                testDeniedInsertOps(testUserStore, nonTtlRow);
            }
        }
        try {
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE);
            testValidInsertOps(nonTtlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, nonTtlRow);
        }
        try {
            grantPrivToRole(TEST_ROLE, WRITE_ANY);
            testValidInsertOps(nonTtlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, WRITE_ANY);
            testDeniedInsertOps(testUserStore, nonTtlRow);
        }
    }

    private void testValidTTLWithDelete(Row ttlRow) throws Exception {
        String table = ttlRow.getTable().getFullNamespaceName();
        try {
            grantPrivToRole(TEST_ROLE, table, INSERT_TABLE, DELETE_TABLE);
            testValidInsertOps(ttlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, table, INSERT_TABLE, DELETE_TABLE);
            testDeniedInsertOps(testUserStore, ttlRow);
        }
        if (ttlRow.getTable().getNamespace().equals(NS)) {
            try {
                grantNsPrivToRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                                  DELETE_IN_NAMESPACE);
                testValidInsertOps(ttlRow);
            } finally {
                revokeNsPrivFromRole(TEST_ROLE, NS, INSERT_IN_NAMESPACE,
                                     DELETE_IN_NAMESPACE);
                testDeniedInsertOps(testUserStore, ttlRow);
            }
        }
        try {
            grantPrivToRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
            testValidInsertOps(ttlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, INSERT_ANY_TABLE, DELETE_ANY_TABLE);
            testDeniedInsertOps(testUserStore, ttlRow);
        }
        try {
            grantPrivToRole(TEST_ROLE, WRITE_ANY);
            testValidInsertOps(ttlRow);
        } finally {
            revokePrivFromRole(TEST_ROLE, WRITE_ANY);
            testDeniedInsertOps(testUserStore, ttlRow);
        }
    }

    @Test
    public void testBulkPutTTLAccessControl()
        throws Exception {

        /*
         * Test against a table without default TTL setting.
         */
        Row row = createOneRowForTable(TEST_TABLE);

        /* Removed all privileges enabling table insert */
        revokePrivFromRole(TEST_ROLE, WRITE_ANY,
                           INSERT_ANY_TABLE, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_TABLE,
                           INSERT_TABLE, DELETE_TABLE);
        testDeniedInsertOps(testUserStore, row);

        Table testTable = superUserStore.getTableAPI().getTable(TEST_TABLE);
        TableAPI superUserTableAPI = superUserStore.getTableAPI();
        assertNotNull(testTable);

        Row ttlRow = testTable.createRow();
        ttlRow.put("id", 2);
        ttlRow.put("name", "bob");
        ttlRow.setTTL(TimeToLive.ofDays(10));

        TableAPI tableAPI = testUserStore.getTableAPI();
        OpAccessCheckTestUtils.TestRowStream stream =
            new OpAccessCheckTestUtils.TestRowStream(row, ttlRow);

        /*
         * Stream has two rows, one with valid TTL, the other not, without
         * DELETE_TABLE privilege, the operation should fail.
         */
        try {
            grantPrivToRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            tableAPI.put(Collections.singletonList(stream), null);
            fail("expected");
        } catch (FaultException fe) {
            assertTrue(fe.getCause() instanceof UnauthorizedException);
            assertTrue(stream.isCaughtException());
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, row);
        }

        try {
            grantPrivToRole(TEST_ROLE, TEST_TABLE,
                            INSERT_TABLE, DELETE_TABLE);
            tableAPI.put(Collections.singletonList(stream), null);
            assertTrue(stream.isCompleted());
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TABLE,
                               INSERT_TABLE, DELETE_TABLE);
            testDeniedInsertOps(testUserStore, row);
            superUserTableAPI.delete(row.createPrimaryKey(), null, null);
            superUserTableAPI.delete(ttlRow.createPrimaryKey(), null, null);
        }

        /*
         * Test against a table with default TTL setting.
         */
        ttlRow = createOneRowForTable(TEST_TTL_TABLE);

        /* Removed all privileges enabling table insert */
        revokePrivFromRole(TEST_ROLE, WRITE_ANY,
                           INSERT_ANY_TABLE, DELETE_ANY_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_TTL_TABLE,
                           INSERT_TABLE, DELETE_TABLE);
        revokePrivFromRole(TEST_ROLE, TEST_TABLE,
                           INSERT_TABLE, DELETE_TABLE);

        /* all insert ops are denied */
        testDeniedInsertOps(testUserStore, ttlRow);
        testTable = superUserStore.getTableAPI().getTable(TEST_TABLE);
        assertNotNull(testTable);

        row = testTable.createRow();
        row.put("id", 2);
        row.put("name", "bob");
        row.setTTL(TimeToLive.DO_NOT_EXPIRE);
        stream = new OpAccessCheckTestUtils.TestRowStream(row, ttlRow);

        /*
         * Stream has two rows, one with valid default TTL, the other has TTL
         * as 0, without DELETE_TABLE privilege, the operation should fail.
         */
        try {
            grantPrivToRole(TEST_ROLE, TEST_TTL_TABLE, INSERT_TABLE);
            tableAPI.put(Collections.singletonList(stream), null);
            fail("expected");
        } catch (FaultException fe) {
            assertTrue(fe.getCause() instanceof UnauthorizedException);
            assertTrue(stream.isCaughtException());
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TTL_TABLE, INSERT_TABLE);
            testDeniedInsertOps(testUserStore, row);
        }

        try {
            grantPrivToRole(TEST_ROLE, TEST_TTL_TABLE,
                            INSERT_TABLE, DELETE_TABLE);
            tableAPI.put(Collections.singletonList(stream), null);
            assertTrue(stream.isCompleted());
        } finally {
            revokePrivFromRole(TEST_ROLE, TEST_TTL_TABLE,
                               INSERT_TABLE, DELETE_TABLE);
            testDeniedInsertOps(testUserStore, row);
            superUserTableAPI.delete(row.createPrimaryKey(), null, null);
            superUserTableAPI.delete(ttlRow.createPrimaryKey(), null, null);
        }
    }

    private void testValidInsertOps(Row row) throws Exception {
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testValidTableInsertOps(
            testUserStore, superUserStore, row);

        OpAccessCheckTestUtils.testValidPutOps(
            testUserStore, superUserStore, kvKey, getKVValueFromRow(row));
    }

    private void testValidDeleteOps(Row row) throws Exception {
        final PrimaryKey key = row.createPrimaryKey();
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();

        OpAccessCheckTestUtils.testValidTableDeleteOps(
            testUserStore, superUserStore, key);

        OpAccessCheckTestUtils.testValidDeleteOps(
            testUserStore, superUserStore, kvKey, getKVValueFromRow(row));
    }

    private void testDeniedTTLInsertOps(KVStore store, Row row)
        throws Exception {

        OpAccessCheckTestUtils.testDeniedTableInsertOps(store, row);

        /*
         * Using KV API to access keyspace of a table having ttl defined,
         * there would be no valid ttl specified, so these operations do
         * not need delete_table privilege.
         */
        final Key kvKey =
            TableKey.createKey(row.getTable(), row, false).getKey();
        OpAccessCheckTestUtils.testValidPutOps(
            testUserStore, superUserStore, kvKey, getKVValueFromRow(row));
    }

    /*
     * Prepares test tables and roles.
     */
    private static void prepareTest() throws Exception {
        execStatement(superUserStore, "create namespace " + NS);
        execStatement(superUserStore,
                      "create table " + TEST_TABLE + TEST_TABLE_DEF);
        execStatement(superUserStore,
                      "create index " + TEST_TABLE_INDEX + " on " +
                      TEST_TABLE + " (" + TEST_TABLE_INDEX_FIELD + ")");
        execStatement(superUserStore,
                      "create table " + TEST_CHILD_TABLE +
                      TEST_CHILD_TABLE_DEF);
        execStatement(superUserStore,
                      "create index " + TEST_CHILD_TABLE_INDEX + " on " +
                      TEST_CHILD_TABLE +
                      " (" + TEST_CHILD_TABLE_INDEX_FIELD + ")");
        execStatement(superUserStore,
                      "create table " + TEST_TABLE_B + TEST_TABLE_B_DEF);
        execStatement(superUserStore,
                      "create table " + TEST_TTL_TABLE + TEST_TABLE_DEF +
                      " using ttl 5 days");

        execStatement(superUserStore, "create role testrole");
        execStatement(superUserStore,
                      "grant " + TEST_ROLE + " to user " + TEST_USER);
        execStatement(superUserStore,
                      "grant readwrite to user " + SUPER_USER);

        final ExecuteOptions options = new ExecuteOptions().setNamespace(NS);
        execStatement(superUserStore,
                      "create table " + TEST_TABLE + TEST_TABLE_DEF,
                      options);
        execStatement(superUserStore,
                      "create table " + TEST_CHILD_TABLE +
                      TEST_CHILD_TABLE_DEF,
                      options);
        execStatement(superUserStore,
                      "create table " + TEST_TTL_TABLE + TEST_TABLE_DEF +
                      " using ttl 5 days",
                      options);
    }

    /* Creates a row for test table */
    private static Row createOneRowForTable(String table) {
        final Table testTable =
            superUserStore.getTableAPI().getTable(table);
        assertNotNull(testTable);

        final String rowJson = tableRowMap.get(table);
        assertNotNull(rowJson);
        return testTable.createRowFromJson(rowJson, true);
    }

    private static void populateTableWithOneRow(KVStore store,
                                                Row row) {
        assertNotNull(store.getTableAPI().put(row, null, null));
    }

    private static void cleanOneRowFromTable(KVStore store, Row row) {
        final PrimaryKey key = row.createPrimaryKey();
        store.getTableAPI().delete(key, null, null);
    }

    private static void revokePrivFromRole(String role,
                                           KVStorePrivilegeLabel... sysPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : sysPriv) {
            execStatement(superUserStore,
                          "revoke " + label + " from " + role);
            assertRoleHasNoPriv(role, label.toString());
        }
    }

    private static void revokePrivFromRole(String role,
                                           String table,
                                           KVStorePrivilegeLabel... tablePriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : tablePriv) {
            execStatement(superUserStore,
                          "revoke " + label + " on " + table + " from " +
                          role);
            assertRoleHasNoPriv(role, toTablePrivStr(label, table));
        }
    }

    private static void revokeNsPrivFromRole(String role,
                                             String ns,
                                             KVStorePrivilegeLabel... nsPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : nsPriv) {
            execStatement(superUserStore,
                          "revoke " + label + " on NAMESPACE " + ns +
                          " from " + role);
            assertRoleHasNoPriv(role, toNamespacePrivStr(label, ns));
        }
    }

    private static void grantPrivToRole(String role,
                                        KVStorePrivilegeLabel... sysPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : sysPriv) {
            execStatement(superUserStore,
                          "grant " + label + " to " + role);
            assertRoleHasPriv(role, label.toString());
        }
    }

    private static void grantPrivToRole(String role,
                                        String table,
                                        KVStorePrivilegeLabel... tablePriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : tablePriv) {
            execStatement(superUserStore,
                          "grant " + label + " on " + table + " to " +
                          role);
            assertRoleHasPriv(role, toTablePrivStr(label, table));
        }
    }

    private static void grantNsPrivToRole(String role,
                                          String ns,
                                          KVStorePrivilegeLabel... nsPriv)
        throws Exception {
        for (KVStorePrivilegeLabel label : nsPriv) {
            execStatement(superUserStore,
                          "grant " + label + " on NAMESPACE " + ns +
                          " to " + role);
            assertRoleHasPriv(role, toNamespacePrivStr(label, ns));
        }
    }

    private static void assertRoleHasNoPriv(String role,
                                            String privStr) {
        final StatementResult result =
            superUserStore.executeSync("show role " + role);
        assertThat(result.getResult(), not(containsString(privStr)));
    }

    private static void assertRoleHasPriv(String role,
                                          String privStr) {
        final StatementResult result =
            superUserStore.executeSync("show role " + role);
        assertThat(result.getResult(), containsString(privStr));
    }

    private static String toTablePrivStr(KVStorePrivilegeLabel tablePriv,
                                         String table) {
        assertEquals(tablePriv.getType(), PrivilegeType.TABLE);
        return String.format("%s(%s)", tablePriv, table);
    }

    /* Keep this method around in case we need it some time */
    @SuppressWarnings("unused")
    private static String toNamespacePrivStr(KVStorePrivilegeLabel nsPriv,
                                             String namespace) {
        assertEquals(nsPriv.getType(), PrivilegeType.NAMESPACE);
        return String.format("%s(%s)", nsPriv, namespace);
    }

    private static void initStores() {
        try {
            final LoginCredentials superCreds =
                new PasswordCredentials(SUPER_USER,
                                        SUPER_USER_PWD.toCharArray());
            final LoginCredentials testCreds =
                new PasswordCredentials(TEST_USER,
                                        TEST_USER_PWD.toCharArray());

            superUserStore = createStore.getSecureStore(superCreds);
            testUserStore = createStore.getSecureStore(testCreds);
        } catch (Exception e) {
            fail("unexpected exception in user login: " + e);
        }
    }

    private static void closeStores() {
        if (superUserStore != null) {
            superUserStore.close();
        }

        if (testUserStore != null) {
            testUserStore.close();
        }
    }

    private static void tableIdMismatchFailure(UnauthorizedException fe) {
        assertThat("table id not match", fe.getMessage(),
            containsString("differs from table id in key"));
    }

    private static void tableNotFoundFailure(UnauthorizedException uae) {
        assertThat("table not found from key", uae.getMessage(),
            containsString("Key does not identify a table"));
    }

    private interface DeniedOp {
        void execute() throws FaultException;
    }

    private static abstract class DeniedOpExecution {
        void perform(DeniedOp... ops) {
            for (DeniedOp op : ops) {
                try {
                    op.execute();
                    fail("Expected UnauthorizedException");
                } catch (UnauthorizedException fe) {
                    assertException(fe);
                }
            }
        }

        abstract void assertException(UnauthorizedException fe);
    }
}
