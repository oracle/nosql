/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.security;

import static oracle.kv.impl.security.KVStorePrivilegeLabel.DELETE_ANY_TABLE;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.INSERT_ANY_TABLE;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.READ_ANY;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.READ_ANY_TABLE;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.WRITE_ANY;
import static oracle.kv.impl.security.KVStorePrivilegeLabel.WRITE_SYSTEM_TABLE;
import static oracle.kv.impl.security.RoleInstance.WRITESYSTABLE_NAME;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;

import oracle.kv.KVStore;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SystemTableTest extends SecureTestBase {

    private static final String SUPER_USER = "super";
    private static final String SUPER_USER_PWD = "NoSql00__super123";

    private static final String TEST_USER = "test";
    private static final String TEST_USER_PWD = "NoSql00__test123";

    private static final String TEST_TABLE = PartitionStatsLeaseDesc.TABLE_NAME;
    private static final String TEST_ROLE = "testrole";
    private static final String SUPER_ROLE = "superrole";

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
    public void testSystemTableOpsPermission() throws Exception {
        waitForTable(superUserStore.getTableAPI(), TEST_TABLE);
        changeRNParameter("rnStatisticsEnabled", "false");
        assertEquals(totalRows(), 0);

        /* Populate one row */
        Row row = populateOneRow();
        Row newRow = createOneRow(testUserStore, 2);
        assertNotNull(superUserStore.getTableAPI().put(row, null, null));

        try {
            grantPrivToRole(superUserStore, TEST_ROLE, READ_ANY, WRITE_ANY);

            testValidReadOps(testUserStore, row, null);
            testDeniedDeleteOps(testUserStore, row);
            testDeniedInsertOps(testUserStore, newRow);
        } finally {
            revokePrivFromRole(superUserStore, TEST_ROLE, READ_ANY, WRITE_ANY);
            testDeniedReadOps(testUserStore, row, null);
            testDeniedInsertOps(testUserStore, newRow);
            testDeniedDeleteOps(testUserStore, row);
        }

        /* test read_any_table */
        try {
            grantPrivToRole(superUserStore, TEST_ROLE, READ_ANY_TABLE);
            testValidReadOps(testUserStore, row, null);
        } finally {
            revokePrivFromRole(superUserStore, TEST_ROLE, READ_ANY_TABLE);
            testDeniedReadOps(testUserStore, row, null);
        }

        /* test insert_any_table */
        try {

            /* INSERT_ANY_TABLE privilege */
            grantPrivToRole(superUserStore, TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, newRow);

            /* INSERT_ANY_TABLE and WRITE_SYSTEM_TABLE privileges */
            grantPrivToRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            testValidInsertOps(testUserStore, superUserStore, newRow);

            /* INSERT_ANY_TABLE privilege, WRITESYSTABLE role to role */
            revokePrivFromRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to role " +
                          TEST_ROLE);
            testValidInsertOps(testUserStore, superUserStore, newRow);

            /* INSERT_ANY_TABLE privilege, WRITESYSTABLE role to user */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to user " +
                          TEST_USER);
            testValidInsertOps(testUserStore, superUserStore, newRow);

            /* WRITESYSTABLE role to user */
            revokePrivFromRole(superUserStore, TEST_ROLE, INSERT_ANY_TABLE);
            testDeniedInsertOps(testUserStore, newRow);

            /* WRITESYSTABLE role to role */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from user " +
                          TEST_USER);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to role " +
                          TEST_ROLE);
            testDeniedInsertOps(testUserStore, newRow);

            /* WRITE_SYSTEM_TABLE privilege */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            grantPrivToRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            testDeniedInsertOps(testUserStore, newRow);
        } finally {
            revokePrivFromRole(superUserStore, TEST_ROLE, INSERT_ANY_TABLE);
            revokePrivFromRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from user " +
                          TEST_USER);
        }

        /* test delete_any_table */
        try {

            /* DELETE_ANY_TABLE privilege */
            grantPrivToRole(superUserStore, TEST_ROLE, DELETE_ANY_TABLE);
            testDeniedDeleteOps(testUserStore, row);

            /* DELETE_ANY_TABLE and WRITE_SYSTEM_TABLE privileges */
            grantPrivToRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            testValidDeleteOps(testUserStore, superUserStore, row);

            /* DELETE_ANY_TABLE and WRITESYSTABLE role to role */
            revokePrivFromRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to role " +
                          TEST_ROLE);
            testValidDeleteOps(testUserStore, superUserStore, row);

            /* DELETE_ANY_TABLE privilege, WRITESYSTABLE role to user */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to user " +
                          TEST_USER);
            testValidDeleteOps(testUserStore, superUserStore, row);

            /* WRITESYSTABLE role to user */
            revokePrivFromRole(superUserStore, TEST_ROLE, DELETE_ANY_TABLE);
            testDeniedDeleteOps(testUserStore, row);

            /* WRITESYSTABLE role to role */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from user " +
                          TEST_USER);
            execStatement(superUserStore,
                          "grant " + WRITESYSTABLE_NAME + " to role " +
                          TEST_ROLE);
            testDeniedDeleteOps(testUserStore, row);

            /* WRITE_SYSTEM_TABLE privilege */
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            grantPrivToRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            testDeniedDeleteOps(testUserStore, row);
        } finally {
            revokePrivFromRole(superUserStore, TEST_ROLE, DELETE_ANY_TABLE);
            revokePrivFromRole(superUserStore, TEST_ROLE, WRITE_SYSTEM_TABLE);
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from role " +
                          TEST_ROLE);
            execStatement(superUserStore,
                          "revoke " + WRITESYSTABLE_NAME + " from user " +
                          TEST_USER);
        }

        /* test write_any */
        try {
            grantPrivToRole(superUserStore, TEST_ROLE, WRITE_ANY);
            testDeniedDeleteOps(testUserStore, row);
        } finally {
            revokePrivFromRole(superUserStore, TEST_ROLE, WRITE_ANY);
        }
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

    /*
     * Prepares test role.
     */
    private static void prepareTest() throws Exception {
        execStatement(superUserStore, "create role " + TEST_ROLE);
        execStatement(superUserStore,
                      "grant " + TEST_ROLE + " to user " + TEST_USER);
        execStatement(superUserStore, "create role " + SUPER_ROLE);
        execStatement(superUserStore,
                      "grant " + WRITE_SYSTEM_TABLE + " to " + SUPER_ROLE);
        execStatement(superUserStore,
                      "grant " + SUPER_ROLE + " to user " + SUPER_USER);
        execStatement(superUserStore,
                      "grant readwrite to user " + SUPER_USER);
    }

    private static void changeRNParameter(String paramName, String paramValue)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(paramName, paramValue);

        int p = createStore.getAdmin().
            createChangeAllParamsPlan("change rn parameter", null, map);
        createStore.getAdmin().approvePlan(p);
        createStore.getAdmin().executePlan(p, false);
        createStore.getAdmin().awaitPlan(p, 0, null);
    }

    private static Row populateOneRow() {
        Row row = createOneRow(superUserStore, 1);
        assertNotNull(superUserStore.getTableAPI().put(row, null, null));
        return row;
    }

    private static Row createOneRow(KVStore store, int rowId) {
        Row row = store.getTableAPI().getTable(TEST_TABLE).createRow();
        row.put("partitionId", rowId);
        row.put(PartitionStatsLeaseDesc.COL_NAME_LAST_UPDATE, "2015");
        row.put(PartitionStatsLeaseDesc.COL_NAME_LEASE_DATE, "2016");
        row.put(PartitionStatsLeaseDesc.COL_NAME_LEASE_RN, "rg1-rn1");
        return row;
    }

    private int totalRows() {
        TableAPI tableAPI = superUserStore.getTableAPI();
        Table testTable = tableAPI.getTable(TEST_TABLE);
        TableIterator<Row> iter = tableAPI.tableIterator(
            testTable.createPrimaryKey(), null, null);
        int rowNum =  0;
        while (iter.hasNext()) {
            iter.next();
            rowNum++;
        }
        iter.close();
        return rowNum;
    }
}
