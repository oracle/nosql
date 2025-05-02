/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.security;

import static oracle.kv.impl.param.ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT;
import static oracle.kv.impl.param.ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL;
import static oracle.kv.impl.param.ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT;
import static oracle.kv.util.DDLTestUtils.execQueryStatement;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.StatementResult;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadata.NamespaceImpl;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecurityDdlTest extends SecureTestBase {

    private static final String ADMIN_NAME = "adminUser";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static final String TEST_USER_NAME = "jack";
    private static final String TEST_USER_B_NAME = "bob";
    private static final String TEST_ROLE_NAME = "test_role";
    private static final String TEST_ROLE_B_NAME = "test_roleB";
    private static final String TEST_USER_PW = "NoSql00__1234";
    private static final String EMPLOYEE_TABLE = "employee";
    private static final String MANAGER_TABLE = "manager";
    private static final String CORP_NAMESPACE = "corp";
    private static final String CREATE_USER =
        "CREATE USER %s IDENTIFIED BY \"%s\"";
    private static KVStore adminStore;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));
        startup();
        adminStore = loginKVStoreUser(ADMIN_NAME, ADMIN_PW);

        /* Add test tables */
        addTestTables();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {
        shutdown();
    }

    /**
     * Notes: This test did not call super setUp method to clean test directory.
     */
    @Override
    public void setUp()
        throws Exception {

        /*
         * The CreateStore model executes the SNA in the same process as
         * the client.  When we run a tests that deliberately botches the
         * client transport config, we need to repair that config before
         * running additional tests.
         */
        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();

        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
    }

    @Test
    public void testUserBasic()
        throws Exception {

        final String newPassword = "NoSql00__12345";
        final String wrongPassword = "incorrect";
        final String nonExistentUser = "ghost";
        final int accountLockoutTimeout = 5;
        final int accountLockoutThreshold = 5;
        final int accountLockoutInterval = 1;

        setAuthPolicy(GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT + "=" + "" +
                accountLockoutThreshold + ";" +
                GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL + "=" +
                "" + accountLockoutInterval + " SECONDS" + ";" +
                GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT + "=" +
                "" + accountLockoutTimeout + " SECONDS");

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME);

        /* Drop a non-existing user */
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\" PASSWORD EXPIRE");
        loginFailed(TEST_USER_NAME, TEST_USER_PW);

        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + newPassword + "\" RETAIN CURRENT PASSWORD");
        KVStore userStore = loginKVStoreUser(TEST_USER_NAME, newPassword);
        userStore = loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);

        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
            " CLEAR RETAINED PASSWORD");
        loginFailed(TEST_USER_NAME, TEST_USER_PW);

        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
            " ACCOUNT LOCK");
        loginFailed(TEST_USER_NAME, newPassword);

        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
            " ACCOUNT UNLOCK");
        userStore = loginKVStoreUser(TEST_USER_NAME, newPassword);
        execStatement(userStore, "ALTER USER " + TEST_USER_NAME +
            " PASSWORD EXPIRE");
        loginFailed(TEST_USER_NAME, newPassword);

        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
            " PASSWORD LIFETIME 15 S");
        userStore = loginKVStoreUser(TEST_USER_NAME, newPassword);
        try {
            Thread.sleep(15 * 1000 + 100);
        } catch (InterruptedException ie)/* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        loginFailed(TEST_USER_NAME, newPassword);

        /* Login store with incorrect password to make account lockout */
        for (int i = 0; i < accountLockoutThreshold; i++) {
            loginFailed(TEST_USER_NAME, wrongPassword);
        }

        /* Renew password */
        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
                " PASSWORD LIFETIME 30 D");

        /* Login failed because account lockout but not password expire */
        try {
            loginKVStoreUser(TEST_USER_NAME, newPassword);
        } catch (AuthenticationFailureException afe) {
            assertThat("Account locked", afe.getMessage(),
                       containsString("User account is locked"));
        }

        try {
            Thread.sleep(accountLockoutTimeout * 1000 + 1000);
        } catch (InterruptedException ie)/* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Login succeed */
        loginKVStoreUser(TEST_USER_NAME, newPassword);

        /* Set password never expire */
        execStatement(adminStore, "ALTER USER " + TEST_USER_NAME +
                " PASSWORD LIFETIME 0 D");

        /* Login succeed */
        loginKVStoreUser(TEST_USER_NAME, newPassword);

        /* Login with non-existent users */
        loginFailed(nonExistentUser, newPassword);

        /* Grant role to non-existent user */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT readwrite TO USER " + nonExistentUser);
            }
        }.exec(IllegalArgumentException.class);

        /* Grant table access privilege to user directly, which should fail */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT INSERT_TABLE ON " + EMPLOYEE_TABLE +
                    " TO " + TEST_USER_NAME);
            }
        }.exec(IllegalArgumentException.class);

        execStatement(adminStore, "DROP USER " + TEST_USER_NAME);
    }

    @Test
    public void testUserPasswordPolicies() throws Exception {
        setLeastCheckerPolicies();
        execStatement(adminStore, "CREATE USER test IDENTIFIED BY \"123456\"");
        setPasswordPolicy("passwordProhibited=welcome1,nosql");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"nosql\"");
            }
        }.exec(IllegalArgumentException.class);
        setLeastCheckerPolicies();
        setPasswordPolicy("passwordNotUserName=true");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"test\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordNotStoreName=true");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \""+
                    createStore.getAdmin().getStoreName() +"\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordRemember=2");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"previous1\"");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"previous2\"");
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"previous1\"");
            }
        }.exec(IllegalArgumentException.class);
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"previous2\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMinSpecial=5");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"!_!_+\"");
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"#$%!\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMinDigit=5");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"09876\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"0987\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMinLower=5");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"gjhkfUUU7777__==\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"dsdsUUU7777____\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMinUpper=5");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"UUUUUdsdsc7777__==\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"dsUUdsUU7777____\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMinLength=5");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"UUd _\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"   4\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMaxLength=10");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"UU ds3d _\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"ds 00ds)_DDD)\"");
            }
        }.exec(IllegalArgumentException.class);

        setLeastCheckerPolicies();
        setPasswordPolicy("passwordMaxLength=10;" +
                          "passwordProhibited=welcomewelcome,nosql;" +
                          "passwordMinLength=9");
        execStatement(adminStore,
            "ALTER USER test IDENTIFIED BY \"welcomewel\"");
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "ALTER USER test IDENTIFIED BY \"nosql\"");
            }
        }.exec(IllegalArgumentException.class);
    }

    private void setLeastCheckerPolicies() throws RemoteException {
        setPasswordPolicy("passwordComplexityCheck=true; " +
                          "passwordMinLength=1; " +
                          "passwordMaxLength=100; " +
                          "passwordMinUpper=0; " +
                          "passwordMinLower=0; " +
                          "passwordMinDigit=0; " +
                          "passwordMinSpecial=0; " +
                          "passwordRemember=0; " +
                          "passwordNotUserName=false; " +
                          "passwordNotStoreName=false; " +
                          "passwordProhibited= ");
    }

    @Test
    public void testRoleBasic()
        throws Exception {

        final String emName = "employee";
        final String mgName = "manager";
        final String testKey = "/person01";
        final String testValue = "bob";
        final String unknownRole = "unknwon";
        final String ns1 = "ns001";
        final String table1 = NameUtils.makeQualifiedName(ns1, "table001");
        final String table2 = "table002";
        final String createTableFmt =
            "CREATE TABLE %s (id INTEGER, PRIMARY KEY(id))";

        execStatement(adminStore, "DROP ROLE " + emName);
        execStatement(adminStore, "DROP ROLE " + mgName);
        execStatement(adminStore, "DROP TABLE IF EXISTS " + table2);
        execStatement(adminStore, "DROP TABLE IF EXISTS " + table1);
        execStatement(adminStore, "DROP NAMESPACE IF EXISTS " + ns1);

        execStatement(adminStore, "GRANT readwrite TO USER " + ADMIN_NAME);
        addTestKVData(adminStore, testKey, testValue);
        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        final KVStore emStore = loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        execStatement(adminStore, "CREATE ROLE " + emName);
        execStatement(adminStore, "CREATE ROLE " + mgName);

        execStatement(adminStore, "CREATE NAMESPACE " + ns1);
        execStatement(adminStore, String.format(createTableFmt, table1));
        execStatement(adminStore, String.format(createTableFmt, table2));

        /* Grant role to role */
        execStatement(adminStore,
            "GRANT " + emName + " TO ROLE " + mgName);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT " + mgName + " TO ROLE " + mgName);
            }
        }.exec(IllegalArgumentException.class);

        /* circular granting should be forbidden */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT " + mgName + " TO ROLE " + emName);
            }
        }.exec(IllegalArgumentException.class);

        /* Could not grant role to system-builtin role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT " + emName + " TO readonly");
            }
        }.exec(IllegalArgumentException.class);

        /* Could not grant unknown role to other role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT " + unknownRole + " TO ROLE " + mgName);
            }
        }.exec(IllegalArgumentException.class);

        /* Could not revoke privilege from system-builtin role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "REVOKE SYSOPER, SYSVIEW FROM sysadmin");
            }
        }.exec(IllegalArgumentException.class);

        /* Revoke role from role */
        execStatement(adminStore,
            "REVOKE " + emName + " FROM ROLE " + mgName);

        /* Could not revoke unknown role from other role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "REVOKE " + unknownRole + " FROM ROLE " + mgName);
            }
        }.exec(IllegalArgumentException.class);

        execStatement(adminStore,
            "GRANT " + emName + " TO USER " + TEST_USER_NAME);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                emStore.get(Key.fromString(testKey));
            }
        }.exec(UnauthorizedException.class);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(emStore,
                    "GRANT SYSOPER, READ_ANY TO " + emName);
            }
        }.exec(UnauthorizedException.class);

        /*
         * System privileges can not be used as table privileges, and vice
         * versa
         */
        for (final KVStorePrivilegeLabel label :
                KVStorePrivilegeLabel.values()) {
            switch (label.getType()) {
            case SYSTEM:
                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON " + EMPLOYEE_TABLE +
                            " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON " + EMPLOYEE_TABLE +
                            " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON NAMESPACE " +
                                CORP_NAMESPACE +
                                " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON NAMESPACE " +
                                CORP_NAMESPACE +
                                " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON NAMESPACE " + ns1 +
                            " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON NAMESPACE " + ns1 +
                            " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);
                break;

            case TABLE:
                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON NAMESPACE " + ns1 +
                            " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON NAMESPACE " + ns1 +
                            " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);
                break;

            case NAMESPACE:
                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON " + table1 +
                            " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON " + table1 +
                            " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "GRANT " + label + " ON " + table2 +
                            " TO " + mgName);
                    }
                }.exec(IllegalArgumentException.class);

                new ExpectErrorExecution() {
                    @Override
                    void doStatement() throws Exception {
                        execStatement(adminStore,
                            "REVOKE " + label + " ON " + table2 +
                            " FROM " + mgName);
                    }
                }.exec(IllegalArgumentException.class);
                break;

            case OBJECT:
                /* no OBJECT privs right now */
            }
        }

        /* Grant system privilege to role */
        execStatement(adminStore,
            "GRANT SYSOPER, READ_ANY TO " + emName);

        execStatement(emStore,
            "GRANT SYSOPER, READ_ANY TO " + emName);
        assertNotNull(emStore.get(Key.fromString(testKey)).getValue());

        /* Revoke system privilege from role */
        execStatement(adminStore,
            "REVOKE READ_ANY, SYSOPER FROM " + emName);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                emStore.get(Key.fromString(testKey));
            }
        }.exec(UnauthorizedException.class);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(emStore,
                    "REVOKE " + emName + " FROM USER " + TEST_USER_NAME);
            }
        }.exec(UnauthorizedException.class);

        /* Revoke role from user */
        execStatement(adminStore,
            "REVOKE " + emName + " FROM USER " + TEST_USER_NAME);

        final TableAPI tableImpl = emStore.getTableAPI();
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                addTableData(tableImpl, tableImpl.getTable(EMPLOYEE_TABLE),
                             2, "jim", "green");
            }
        }.exec(UnauthorizedException.class);

        /*
         * Grant object privilege of other table manager to role, the user
         * having this role still cannot write the table employee
         */
        execStatement(adminStore,
            "GRANT INSERT_TABLE ON " + MANAGER_TABLE + " TO " + emName);
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                addTableData(tableImpl, tableImpl.getTable(EMPLOYEE_TABLE),
                             2, "jim", "green");
            }
        }.exec(UnauthorizedException.class);

        /* Grant object privilege to role */
        execStatement(adminStore,
            "GRANT INSERT_TABLE ON " + EMPLOYEE_TABLE + " TO " + emName);

        /* Grant object privilege on unknown table to role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "GRANT INSERT_TABLE ON " + "unknown" + " TO " + emName);
            }
        }.exec(IllegalArgumentException.class);

        /* Revoke object privilege on unknown table from role */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(adminStore,
                    "REVOKE INSERT_TABLE ON " + "unknown" + " FROM " + emName);
            }
        }.exec(IllegalArgumentException.class);

        execStatement(adminStore,
            "GRANT " + emName + " TO USER " + TEST_USER_NAME);
        addTableData(tableImpl, tableImpl.getTable(EMPLOYEE_TABLE),
                     2, "jim", "green");

        /* Revoke object privilege from role */
        execStatement(adminStore,
            "REVOKE INSERT_TABLE ON " + EMPLOYEE_TABLE + " FROM " + emName);

        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                addTableData(tableImpl, tableImpl.getTable(EMPLOYEE_TABLE),
                             3, "bob", "smith");
            }
        }.exec(UnauthorizedException.class);

        /* Drop table manager for later test */
        execStatement(adminStore,
            "REVOKE INSERT_TABLE ON " + MANAGER_TABLE + " FROM " + emName);
        execStatement(adminStore, "DROP TABLE " + MANAGER_TABLE);

        /* Grant CREATE_ANY_TABLE to role employee */
        execStatement(adminStore, "GRANT CREATE_ANY_TABLE TO " + emName);

        /* Now user jack is allowed to add table, re-add table manager */
        addManagerTable(emStore);

        /*
         * Revoke CREATE_ANY_TABLE from role employee, and drop table manager
         * and user jack
         */
        execStatement(adminStore, "REVOKE CREATE_ANY_TABLE FROM " + emName);
        execStatement(adminStore, "DROP TABLE " + MANAGER_TABLE);
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME);

        /* Recreate user jack */
        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
                      " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        /* Jack should not be able to add new table */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                addManagerTable(emStore);
            }
        }.exec(UnauthorizedException.class);


        /* Test if "show role <name>" works with a case-insensitive name */
        execStatement(adminStore, "SHOW ROLE " + mgName);
        execStatement(adminStore, "SHOW ROLE " + mgName.toUpperCase());
        final String tmpRoleName =
            mgName.substring(0, 1).toUpperCase() + mgName.substring(1);
        execStatement(adminStore, "SHOW AS JSON ROLE " + tmpRoleName);
        /* system built-in roles */
        execStatement(adminStore, "SHOW ROLE readONLY");
        execStatement(adminStore, "SHOW AS JSON ROLE SYSADMIN");

        /* Drop role */
        execStatement(adminStore, "DROP role " + mgName);
        execStatement(adminStore, "DROP role " + mgName);
        execStatement(adminStore, "DROP role " + emName);
        execStatement(adminStore, "DROP role sysdba");
    }

    @Test
    public void testExternalUserSyntaxOnly() throws Exception {
        final String externalUser = "Frank/host@test.com";
        final String internalUser = "Frank";
        final String internalUserPass = "NoSql00__123456";
        final String emptyUser = "";
        /* Create external user */
        execStatement(adminStore, "CREATE USER \"" + externalUser +
            "\" IDENTIFIED EXTERNALLY");
        /* Alter external user */
        execStatement(adminStore, "ALTER USER \"" + externalUser +
            "\" ACCOUNT LOCK");
        /* Show external user */
        execStatement(adminStore, "SHOW USER \"" + externalUser +
            "\"");
        /* Grant role to user */
        execStatement(adminStore, "GRANT readwrite TO USER \"" + externalUser +
            "\"");
        /* Revoke role from user */
        execStatement(adminStore, "REVOKE readwrite FROM USER \"" + externalUser +
            "\"");
        /* Create internal user */
        execStatement(adminStore, "CREATE USER " + internalUser +
            " IDENTIFIED BY \"" + internalUserPass + "\"");
        /* Alter internal user */
        execStatement(adminStore, "ALTER USER " + internalUser +
            " ACCOUNT LOCK");
        /* Show internal user */
        execStatement(adminStore, "SHOW USER " + internalUser);

        /* Cannot create empty user */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + emptyUser +
                    "\" IDENTIFIED EXTERNALLY");
            }
        }.exec(IllegalArgumentException.class);
        /* Create external user with different artributes */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + externalUser +
                    "\" IDENTIFIED EXTERNALLY ADMIN");
            }
        }.exec(IllegalArgumentException.class);
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + externalUser +
                    "\" IDENTIFIED EXTERNALLY ACCOUNT UNLOCK");
            }
        }.exec(IllegalArgumentException.class);
        /* Cannot do any password related option for external user */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + externalUser +
                    "\" IDENTIFIED EXTERNALLY PASSWORD EXPIRE");
            }
        }.exec(IllegalArgumentException.class);
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + externalUser +
                    "\" IDENTIFIED EXTERNALLY PASSWORD LIFETIME 15 S");
            }
        }.exec(IllegalArgumentException.class);
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "ALTER USER \"" + externalUser +
                    "\" PASSWORD EXPIRE");
            }
        }.exec(IllegalArgumentException.class);
        /*
         * Cannot create an internal user with name in external user name
         * convention, which must be a string with quoted.
         */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER \"" + "incorrectname" +
                    "\" IDENTIFIED BY \"" + internalUserPass + "\"");
            }
        }.exec(IllegalArgumentException.class);
        /*
         * Cannot create a external user whose input name in internal user
         * naming convention.
         */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE USER " + "incorrectname" +
                    " IDENTIFIED EXTERNALLY");
            }
        }.exec(IllegalArgumentException.class);

        /* Drop external user */
        execStatement(adminStore, "DROP USER \"" + externalUser +
            "\"");
        /* Drop internal user */
        execStatement(adminStore, "DROP USER " + internalUser);
    }

    @Test
    public void testCascadeDropUser()
        throws Exception {

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
                      " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "GRANT dbadmin TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
                      " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
                      " TO USER " + TEST_USER_B_NAME);
        execStatement(adminStore, "GRANT SYSOPER TO " + TEST_ROLE_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        final KVStore testStoreB =
            loginKVStoreUser(TEST_USER_B_NAME, TEST_USER_PW);

        /*
         * There are two tables created in static setup.
         */

        assertEquals(countUserTables(tableAPI), 2);

        /*
         * Add 12 test tables whose owner is test user.  Also include indexes.
         * Includes 4 top level tables with child and grandchild tables each.
         */
        List<String> tables = addTestTablesComplex(testStore, 12, 3, true, null);
        assertEquals(countUserTables(tableAPI), 2 + tables.size());


        /* Add six test roles */
        addTestRoles(adminStore, 6, TEST_USER_NAME);

        /* Grant table privileges of test tables to test roles respectively */
        grantTablePrivsToRoles(adminStore, tables, 6);

        /* Dropping a user who owns tables must specify cascade option */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB, "DROP USER " + TEST_USER_NAME);
            }
        }.exec(IllegalArgumentException.class);

        /*
         * Caller that does not have DROP_ANY_TABLE and DROP_ANY_INDEX
         * privileges cannot drop a user with cascade.
         */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB,
                    "DROP USER " + TEST_USER_NAME + " CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_TABLE TO " + TEST_ROLE_NAME);
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB,
                    "DROP USER " + TEST_USER_NAME + " CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_INDEX TO " + TEST_ROLE_NAME);
        execStatement(testStoreB, "DROP USER " + TEST_USER_NAME + " CASCADE");

        /*
         * Drop test user whose tables should be removed. The table
         * privilege on these tables are also removed from any roles
         */

        assertEquals(countUserTables(tableAPI), 2);
        StatementResult result;
        result = adminStore.executeSync("show users");
        assertThat(result.getResult(), not(containsString(TEST_USER_NAME)));
        for (int i = 0; i < 6; i++) {
            result = adminStore.executeSync("show role role" + i);
            for(int j = 0; j < tables.size(); j++) {
                assertThat(result.getResult(), not(containsString(
                        "table" + j)));
            }
        }

        /* Add test tables again */

        tables = addTestTablesWithIndex(adminStore, 6);
        assertEquals(countUserTables(tableAPI), 8);

        /* Grant table privileges of test tables to test roles respectively */
        grantTablePrivsToRoles(adminStore, tables, 6);

        /* Drop test tables */
        for (int i = 0; i < 6; i++) {
            execStatement(adminStore, "DROP TABLE table" + i);
        }

        assertEquals(countUserTables(tableAPI), 2);

        /* Table privileges on these tables should be removed from roles */
        for (int i = 0; i < 6; i++) {
            result = adminStore.executeSync("show role role" + i);
            assertThat(result.getResult(), not(containsString("table" + i)));
        }
    }

    @Test
    public void testCascadeDropUserNs()
        throws Exception {

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "GRANT dbadmin TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_B_NAME);
        execStatement(adminStore, "GRANT SYSOPER TO " + TEST_ROLE_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        final KVStore testStoreB =
            loginKVStoreUser(TEST_USER_B_NAME, TEST_USER_PW);

        /* There is only the "sysdefault" namespace */
        assertEquals(1, tableAPI.listNamespaces().size());
        /*
         * There are two tables created in static setup.
         */
        assertEquals(2, countUserTables(tableAPI));

        /* Add 2 namespaces whose owner is test user */
        addTestNamespaces(testStore, 2);
        assertEquals(3, tableAPI.listNamespaces().size());

//        addTestTablesInNs(testStore, 3, "ns0");
//        addTestTablesInNs(testStore, 3, "ns1");
//        assertEquals(13, tableAPI.getTables().size());

        List<String> tables1 = addTestTablesComplex(testStore, 6, 2, true, "ns0");
        List<String> tables2 = addTestTablesComplex(testStore, 6, 3, true, "ns1");
        /* Combine into one list */
        List<String> tables = new ArrayList<>();
        tables.addAll(tables1);
        tables.addAll(tables2);
        assertEquals(countUserTables(tableAPI), 2 + tables.size());

        /* Add six test roles */
        addTestRoles(adminStore, 6, TEST_USER_NAME);

        /* Grant table privileges of test tables to test roles respectively */
        grantTablePrivsToRoles(adminStore, tables, 6);

        /* Dropping a user who owns tables must specify cascade option */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB, "DROP USER " + TEST_USER_NAME);
            }
        }.exec(IllegalArgumentException.class);

        /*
         * Caller that does not have DROP_ANY_TABLE and DROP_ANY_INDEX
         * privileges cannot drop a user with cascade.
         */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB,
                    "DROP USER " + TEST_USER_NAME + " CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_TABLE TO " + TEST_ROLE_NAME);
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB,
                    "DROP USER " + TEST_USER_NAME + " CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT DROP_ANY_INDEX TO " + TEST_ROLE_NAME);
        execStatement(testStoreB, "DROP USER " + TEST_USER_NAME + " CASCADE");

        /* Drop test user whose namespaces and tables should be removed. The
         * table privilege on these tables are also removed from any roles */
        assertEquals(1, tableAPI.listNamespaces().size());
        assertEquals(2, countUserTables(tableAPI));
        StatementResult result;
        result = adminStore.executeSync("show users");
        assertThat(result.getResult(), not(containsString(TEST_USER_NAME)));
        for (int i = 0; i < 6; i++) {
            result = adminStore.executeSync("show role role" + i);
            for(int j = 0; j < tables1.size(); j++) {
                assertThat(result.getResult(), not(containsString(
                        "table" + j)));
            }
        }

        /* Add test namespaces and tables again */
        addTestNamespaces(adminStore, 2);
        tables.clear();
        tables.addAll(addTestTablesInNs(adminStore, 3, "ns0"));
        tables.addAll(addTestTablesInNs(adminStore, 3, "ns1"));
        assertEquals(3, tableAPI.listNamespaces().size());
        // assertEquals(13, tableAPI.getTables().size());

        assertEquals(countUserTables(tableAPI), 2 + tables.size());

        /* Grant table privileges of test tables to test roles respectively */
        grantTablePrivsToRoles(adminStore, tables, 6);

        /* Drop test tables */
        for (int i = 0; i < 3; i++) {
            execStatement(adminStore, "DROP TABLE ns0:table" + i);
            execStatement(adminStore, "DROP TABLE ns1:table" + i);
        }
        execStatement(adminStore, "DROP NAMESPACE ns0");
        execStatement(adminStore, "DROP NAMESPACE ns1");
        assertEquals(1, tableAPI.listNamespaces().size());
        assertEquals(2, countUserTables(tableAPI));

        /* Table privileges on these tables should be removed from roles */
        for (int i = 0; i < 6; i++) {
            result = adminStore.executeSync("show role role" + i);
            assertThat(result.getResult(), not(containsString("table" +
                    (i % 3))));
            execStatement(adminStore, "DROP ROLE role" + i);
        }
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME);
    }

    @Test
    public void testDropNsCascade()
        throws Exception {

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "GRANT dbadmin TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_B_NAME);
        execStatement(adminStore, "GRANT SYSOPER TO " + TEST_ROLE_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        final KVStore testStoreB =
            loginKVStoreUser(TEST_USER_B_NAME, TEST_USER_PW);

        /* There is only the "sysdefault" namespace */
        assertEquals(1, tableAPI.listNamespaces().size());
        /*
         * There are two tables are test created in static setup.
         */
        assertEquals(2, countUserTables(tableAPI));

        /* Add 2 namespaces whose owner is test user */
        addTestNamespaces(testStore, 2);
        assertEquals(3, tableAPI.listNamespaces().size());

        // addTestTablesInNs(testStore, 3, "ns0");
        // addTestTablesInNs(testStore, 3, "ns1");
        // assertEquals(13, tableAPI.getTables().size());

        List<String> tables1 = addTestTablesComplex(testStore, 6, 3, true, "ns0");
        List<String> tables2 = addTestTablesComplex(testStore, 6, 2, true, "ns1");
        List<String> tables = new ArrayList<>();
        tables.addAll(tables1);
        tables.addAll(tables2);
        assertEquals(countUserTables(tableAPI), 2 + tables.size());

        /* Add six test roles */
        addTestRoles(adminStore, 6, TEST_USER_B_NAME);

        /* Grant table privileges of test tables to test roles respectively */
        grantTablePrivsToRoles(adminStore, tables, 6);

        /* Dropping a user who owns tables must specify cascade option */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB, "DROP USER " + TEST_USER_NAME);
            }
        }.exec(IllegalArgumentException.class);

        /*
         * Caller that does not have DROP_ANY_NAMESPACE
         * privilege cannot drop a user with cascade.
         */
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB, "DROP NAMESPACE ns0 CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_TABLE TO " + TEST_ROLE_NAME);
        new ExpectErrorExecution() {

            @Override
            void doStatement() throws Exception {
                execStatement(testStoreB, "DROP NAMESPACE ns0 CASCADE");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        execStatement(testStoreB, "DROP NAMESPACE ns0 CASCADE");
        assertEquals(2, tableAPI.listNamespaces().size());
        // assertEquals(10, tableAPI.getTables().size());

        assertEquals(countUserTables(tableAPI), 2 + tables2.size());


        execStatement(testStoreB, "DROP NAMESPACE ns1 CASCADE");
        assertEquals(1, tableAPI.listNamespaces().size());
        // assertEquals(7, tableAPI.getTables().size());

        assertEquals(countUserTables(tableAPI), 2);

        for (int i = 0; i < 6; i++) {
            StatementResult result = adminStore.executeSync(
                    "show role role" + i);
            for(int j = 0; j < tables1.size(); j++) {
                assertThat(result.getResult(), not(containsString(
                        "table" + j)));
            }
        }

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);
    }


    @Test
    public void testInAnyNamespace()
        throws Exception {
        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        //execStatement(adminStore, "GRANT dbadmin TO USER " + TEST_USER_NAME);
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_B_NAME +
            " TO USER " + TEST_USER_B_NAME);
        //execStatement(adminStore, "GRANT SYSOPER TO " + TEST_ROLE_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);

        /* Error if TEST_USER wants to add a new namespace */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "CREATE NAMESPACE fooNs.com");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT CREATE_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        // remember the current namespaces
        Collection<String> namespaces = tableAPI.listNamespaces();
        assertNotNull(namespaces);
        int previousNamespaces = namespaces.size();
        assertTrue(previousNamespaces >= 1);
        assertTrue(namespaces.contains("sysdefault"));

        /* Add 2 test namespaces whose owner is test user ns0 and ns1 */
        addTestNamespaces(testStore, 2);

        // check the newly added namespaces
        TableAPI tableAPI2 = adminStore.getTableAPI();
        namespaces = tableAPI2.listNamespaces();
        assertEquals(2, namespaces.size() - previousNamespaces);
        assertTrue(namespaces.contains("ns0"));
        assertTrue(namespaces.contains("ns1"));

        // 3 namespaces sysdefault, ns0 and ns1
        assertEquals(3, namespaces.size());

        execStatement(adminStore, "GRANT CREATE_ANY_TABLE TO " +
            TEST_ROLE_NAME);

        /* Add 4 test tables whose owner is test user */
        addTestTablesInNs(testStore, 2, "ns0");
        assertEquals(2, tableAPI.getTables("ns0").size());

        addTestTablesInNs(testStore, 2, "ns1");
        assertEquals(2, tableAPI.getTables("ns1").size());

        // write in ns0:table0 and ns1:table0
        TableAPI testApi = testStore.getTableAPI();
        Table testNs0Table0 = testApi.getTable("ns0", "table0");
        Table testNs1Table0 = testApi.getTable("ns1", "table0");

        addTableData(testApi, testNs0Table0, 1, "ns0:table0-f", "ns0:table0-l");
        addTableData(testApi, testNs1Table0, 1, "ns1:table0-f", "ns1:table0-l");

        // read from ns0:table0 and ns1:table0
        PrimaryKey pk = testNs0Table0.createPrimaryKey();
        pk.put(0, 1);
        Row r = readTableData(testApi, testNs0Table0, 1);
        assertEquals("ns0:table0-f", r.get("firstName").asString().get());
        assertEquals("ns0:table0-l", r.get("lastName").asString().get());

        pk = testNs1Table0.createPrimaryKey();
        pk.put(0, 1);
        r = readTableData(testApi, testNs1Table0, 1);
        assertEquals("ns1:table0-f", r.get("firstName").asString().get());
        assertEquals("ns1:table0-l", r.get("lastName").asString().get());

        // check select works
        execQueryStatement(testStore, "SELECT * FROM ns0:table0", 1);
        execQueryStatement(testStore, "SELECT * FROM ns0:table1", 0);
        execQueryStatement(testStore, "SELECT * FROM ns1:table0", 1);
        execQueryStatement(testStore, "SELECT * FROM ns1:table1", 0);

        // check case-insensitive namespace name
        execQueryStatement(testStore, "SELECT * FROM Ns0:table0", 1);
        execQueryStatement(testStore, "SELECT * FROM nS0:table0", 1);
        execQueryStatement(testStore, "SELECT * FROM NS0:table0", 1);

        assertEquals("ns0", testApi.getTable("Ns0", "table0").getNamespace());
        assertEquals("ns0", testApi.getTable("nS0", "table0").getNamespace());
        assertEquals("ns0", testApi.getTable("NS0", "table0").getNamespace());

        // try to drop ns0, should fail because there are no drop privs
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "DROP NAMESPACE ns0");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        // try to drop ns0, should fail because there are tables in it
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "DROP NAMESPACE ns0");
            }
        }.exec(IllegalArgumentException.class);

        // drop tables
        execStatement(testStore, "DROP TABLE ns0:table0");
        execStatement(testStore, "DROP TABLE ns0:table1");
        execStatement(testStore, "DROP TABLE ns1:table0");
        execStatement(testStore, "DROP TABLE ns1:table1");

        // drop namespaces
        execStatement(testStore, "DROP NAMESPACE ns0");
        execStatement(testStore, "DROP NAMESPACE ns1");

        // cleanup roles and users
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_B_NAME);

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
    }

    @Test
    public void testDataInNamespace()
        throws Exception {

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        //execStatement(adminStore, "GRANT dbadmin TO USER " + TEST_USER_NAME);
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_B_NAME +
            " TO USER " + TEST_USER_B_NAME);
        //execStatement(adminStore, "GRANT SYSOPER TO " + TEST_ROLE_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);
        final KVStore testBStore =
            loginKVStoreUser(TEST_USER_B_NAME, TEST_USER_PW);

        execStatement(adminStore,
            "GRANT CREATE_ANY_NAMESPACE, DROP_ANY_NAMESPACE  TO " +
            TEST_ROLE_NAME);

        /* Add 2 test namespaces whose owner is test user */
        addTestNamespaces(testStore, 2);

        Set<String> nses = adminStore.getTableAPI().listNamespaces();
        assertNotNull(nses);
        assertEquals(3, nses.size());
        assertTrue(nses.contains("sysdefault"));
        assertTrue(nses.contains("ns0"));
        assertTrue(nses.contains("ns1"));

        execStatement(adminStore, "GRANT CREATE_ANY_TABLE TO " +
            TEST_ROLE_NAME);

        /* Add 4 test tables whose owner is test user */
        addTestTablesInNs(testStore, 2, "ns0");
        assertEquals(2, tableAPI.getTables("ns0").size());

        addTestTablesInNs(testStore, 2, "ns1");
        assertEquals(2, tableAPI.getTables("ns1").size());

        TableAPI testApi = testStore.getTableAPI();
        Table testTable0 = testApi.getTable("ns0", "table0");

        addTableData(testApi, testTable0, 1, "f", "l");


        /**
         *  Test WRITE[INSERT/DELETE]_IN_NAMESPACE
         */
        TableAPI testBApi = testBStore.getTableAPI();
        Table testBNs0Table0 = testApi.getTable("ns0", "table0");

        /* Error if TEST_USER_B wants to write in ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                addTableData(testBApi, testBNs0Table0, 2, "f", "l");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT INSERT_IN_NAMESPACE ON NAMESPACE ns0 " +
            "TO " + TEST_ROLE_B_NAME);

        addTableData(testBApi, testBNs0Table0, 2, "f", "l");

        /* Error if TEST_USER_B wants to read from ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                readTableData(testBApi, testBNs0Table0, 2);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT READ_IN_NAMESPACE ON NAMESPACE ns0 " +
            "TO " + TEST_ROLE_B_NAME);

        readTableData(testBApi, testBNs0Table0, 2);
        readIterTableData(testBApi, testBNs0Table0, 2);

        /* Error if TEST_USER_B wants to delete from ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                deleteTableData(testBApi, testBNs0Table0, 2);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DELETE_IN_NAMESPACE ON NAMESPACE ns0" +
            " TO " + TEST_ROLE_B_NAME);

        deleteTableData(testBApi, testBNs0Table0, 2);

        execStatement(adminStore, "REVOKE DELETE_IN_NAMESPACE ON NAMESPACE " +
            "ns0 FROM " + TEST_ROLE_B_NAME);

        /* Error if TEST_USER_B wants to delete from ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                deleteTableData(testBApi, testBNs0Table0, 2);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "REVOKE READ_IN_NAMESPACE ON NAMESPACE ns0" +
            " FROM " + TEST_ROLE_B_NAME);

        /* Error if TEST_USER_B wants to read in ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                readTableData(testBApi, testBNs0Table0, 2);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "REVOKE INSERT_IN_NAMESPACE ON NAMESPACE " +
            "ns0 FROM " + TEST_ROLE_B_NAME);

        /* Error if TEST_USER_B wants to write in ns0:table0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                addTableData(testBApi, testBNs0Table0, 2, "f", "l");
            }
        }.exec(UnauthorizedException.class);

        execStatement(testStore, "DROP TABLE ns0:table0");
        execStatement(testStore, "DROP TABLE ns0:table1");
        execStatement(testStore, "DROP TABLE ns1:table0");
        execStatement(testStore, "DROP TABLE ns1:table1");

        execStatement(testStore, "DROP NAMESPACE ns0");
        execStatement(testStore, "DROP NAMESPACE ns1");

        // cleanup roles and users
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
    }

    @Test
    public void testDDLInNamespace()
        throws Exception {

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_NAME);

        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_B_NAME +
            " TO USER " + TEST_USER_B_NAME);

        final TableAPI tableAPI = adminStore.getTableAPI();
        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);

        /*
         * There are two tables already created in static setup.
         */
        assertEquals(2, countUserTables(tableAPI));

        /* Initial namespace only */
        assertEquals(1, tableAPI.listNamespaces().size());

        /* Error if TEST_USER wants to add a new namespace */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "CREATE NAMESPACE fooNs. ora . cle");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT CREATE_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        /* Add test namespace whose owner is test user */
        execStatement(testStore, "CREATE NAMESPACE ns0");

        assertEquals(2, tableAPI.listNamespaces().size());

        execStatement(adminStore, "GRANT CREATE_ANY_TABLE TO " +
            TEST_ROLE_NAME);

        final KVStore testBStore =
            loginKVStoreUser(TEST_USER_B_NAME, TEST_USER_PW);

        /**
         *  Test CREATE_TABLE_IN_NAMESPACE
         */
        String createStmt =
            "CREATE TABLE ns0:table11 (" +
                "id INTEGER, " +
                "firstName STRING, " +
                "lastName STRING, " +
                "PRIMARY KEY (id))";
        String dropStmt =
            "DROP TABLE ns0:table11";
        /* Error if TEST_USER_B wants to create new table in ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, createStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT CREATE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 TO " + TEST_ROLE_B_NAME);


        execStatement(testBStore, createStmt);
        execStatement(testBStore, dropStmt);
        // create the table by userA
        execStatement(testStore, createStmt);

        /**
         *  Test EVOLVE_TABLE_IN_NAMESPACE
         */
        String alterStmt =
            "ALTER TABLE ns0:table11 (ADD middleName STRING)";
        /* Error if TEST_USER_B wants to alter a table in ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, alterStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT EVOLVE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 TO " + TEST_ROLE_B_NAME);

        execStatement(testBStore, alterStmt);

        /**
         *  Test CREATE_INDEX_IN_NAMESPACE
         */
        String createIdxStmt =
            "CREATE INDEX secDdlTable11Idx on ns0:table11 (middleName)";
        /* Error if TEST_USER_B wants to alter a table in ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, createIdxStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT CREATE_INDEX_IN_NAMESPACE ON " +
            "NAMESPACE ns0 TO " + TEST_ROLE_B_NAME);

        execStatement(testBStore, createIdxStmt);

        /**
         *  Test DROP_INDEX_IN_NAMESPACE
         */
        String dropIdxStmt =
            "DROP INDEX secDdlTable11Idx ON ns0:table11";
        /* Error if TEST_USER_B wants to alter a table in ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, dropIdxStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_INDEX_IN_NAMESPACE ON " +
            "NAMESPACE ns0 TO " + TEST_ROLE_B_NAME);

        execStatement(testBStore, dropIdxStmt);

        /**
         *  Test DROP_TABLE_IN_NAMESPACE
         */
        /* Error if TEST_USER_B wants to alter a table in ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, dropStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 TO " + TEST_ROLE_B_NAME);

        execStatement(testBStore, dropStmt);

        String dropNsStmt = "DROP NAMESPACE ns0";
        /* Insufficient rights, needs DROP_ANY_NAMESPACE privilege */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testBStore, dropNsStmt);
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        /* Error because testRoleB still has privs on ns0 */
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, dropNsStmt);
            }
        }.exec(IllegalArgumentException.class);

        execStatement(adminStore, "REVOKE CREATE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 FROM " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "REVOKE EVOLVE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 FROM " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "REVOKE CREATE_INDEX_IN_NAMESPACE ON " +
            "NAMESPACE ns0 FROM " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "REVOKE DROP_INDEX_IN_NAMESPACE ON " +
            "NAMESPACE ns0 FROM " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "REVOKE DROP_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE ns0 FROM " + TEST_ROLE_B_NAME);

        execStatement(testStore, dropNsStmt);


        /* Check GRANT/REVOKE MODIFY_IN_NAMESPACE */
        execStatement(adminStore, "CREATE NAMESPACE n");

        execStatement(adminStore, "GRANT MODIFY_IN_NAMESPACE ON " +
            "NAMESPACE n TO " + TEST_ROLE_NAME);
        StatementResult result = adminStore.executeSync("show role " +
            TEST_ROLE_NAME);
        assertTrue(result.isSuccessful());
        assertTrue(result.getResult().contains("CREATE_INDEX_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("CREATE_TABLE_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("EVOLVE_TABLE_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("DROP_INDEX_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("DROP_TABLE_IN_NAMESPACE(n)"));

        execStatement(adminStore, "REVOKE MODIFY_IN_NAMESPACE ON " +
            "NAMESPACE n FROM " + TEST_ROLE_NAME);
        result = adminStore.executeSync("show role " +
            TEST_ROLE_NAME);
        assertTrue(result.isSuccessful());
        assertFalse(result.getResult().contains("CREATE_INDEX_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("CREATE_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("EVOLVE_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("DROP_INDEX_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("DROP_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("MODIFY_IN_NAMESPACE(n)"));

        /* CHECK GRANT/REVOKE ALL */
        execStatement(adminStore, "GRANT ALL ON " +
            "NAMESPACE n TO " + TEST_ROLE_NAME);
        result = adminStore.executeSync("show role " +
            TEST_ROLE_NAME);
        assertTrue(result.isSuccessful());
        assertTrue(result.getResult().contains("CREATE_INDEX_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("CREATE_TABLE_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("EVOLVE_TABLE_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("DROP_INDEX_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("DROP_TABLE_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("READ_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("INSERT_IN_NAMESPACE(n)"));
        assertTrue(result.getResult().contains("DELETE_IN_NAMESPACE(n)"));

        execStatement(adminStore, "REVOKE ALL ON " +
            "NAMESPACE n FROM " + TEST_ROLE_NAME);
        result = adminStore.executeSync("show role " +
            TEST_ROLE_NAME);
        assertTrue(result.isSuccessful());
        assertFalse(result.getResult().contains("CREATE_INDEX_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("CREATE_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("EVOLVE_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("DROP_INDEX_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("DROP_TABLE_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("MODIFY_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("READ_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("INSERT_IN_NAMESPACE(n)"));
        assertFalse(result.getResult().contains("DELETE_IN_NAMESPACE(n)"));

        execStatement(adminStore, "DROP NAMESPACE n CASCADE");

        // cleanup roles and users
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);

        execStatement(adminStore, "DROP USER " + TEST_USER_B_NAME + " CASCADE");
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
    }

    /**
     * Test "GRANT/REVOKE *_IN_NAMESPACE ON NAMESPACE namespaceName"
     * <p>
     * Negative cases:
     * <ul>
     * <li>GRANT/REVOKE *_IN_NAMESPACE ON NAMESPACE tableName ...</li>
     * <li>GRANT/REVOKE *_IN_NAMESPACE ON tableName ...</li>
     * <li>GRANT/REVOKE *_IN_NAMESPACE ON namespaceName ...</li>
     * </ul>
     * Positive cases:
     * <ul>
     * <li>GRANT/REVOKE *_IN_NAMESPACE ON NAMESPACE namespaceName ...</li>
     * </ul>
     * </p>
     */
    @Test
    public void testGrantRevokeNsPrivs() throws Exception {

        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE USER " + TEST_USER_B_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");

        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_B_NAME);
        execStatement(adminStore, "GRANT CREATE_ANY_NAMESPACE, " +
            "DROP_ANY_NAMESPACE, CREATE_ANY_TABLE TO " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT CREATE_ANY_NAMESPACE, " +
            "DROP_ANY_NAMESPACE, CREATE_ANY_TABLE TO " + TEST_ROLE_B_NAME);

        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME + " TO USER " +
            TEST_USER_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_B_NAME + " TO USER " +
            TEST_USER_B_NAME);

        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);

        execStatement(testStore, "CREATE NAMESPACE Ns001");
        execStatement(testStore,
            "CREATE TABLE Ns001:T1 (id string, primary key(id))");
        execStatement(testStore,
            "CREATE TABLE T2 (id string, primary key(id))");

        final String[] NS_PRIVS = {
            "CREATE_TABLE_IN_NAMESPACE",
            "DROP_TABLE_IN_NAMESPACE",
            "EVOLVE_TABLE_IN_NAMESPACE",
            "CREATE_INDEX_IN_NAMESPACE",
            "DROP_INDEX_IN_NAMESPACE",
            "MODIFY_IN_NAMESPACE",
            "READ_IN_NAMESPACE",
            "INSERT_IN_NAMESPACE",
            "DELETE_IN_NAMESPACE",
        };

        // Test "GRANT *_IN_NAMESPACE ON ..."
        final String msg1 = "%s should not have been granted the privilege: %s";
        final String msg2 = "%s should have been granted the privilege: %s";

        // Case 1: GRANT *_IN_NAMESPACE ON NAMESPACE <tableName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "GRANT " + nsPriv +
                        " ON NAMESPACE Ns001:T1 TO " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "GRANT " + nsPriv +
                        " ON NAMESPACE T2 TO " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        StatementResult stmtRes =
            adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        String result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            assertFalse(String.format(msg1, TEST_ROLE_B_NAME, nsPriv),
                result.contains(nsPriv));
        }

        // Case 2: GRANT *_IN_NAMESPACE ON <tableName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "GRANT " + nsPriv +
                        " ON Ns001:T1 TO " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "GRANT " + nsPriv +
                        " ON T2 TO " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }
        stmtRes = adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            assertFalse(String.format(msg1, TEST_ROLE_B_NAME, nsPriv),
                result.contains(nsPriv));
        }

        // Case 3: GRANT *_IN_NAMESPACE ON <namespaceName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "GRANT " + nsPriv +
                        " ON Ns001 TO " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }
        stmtRes = adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            // MODIFY_IN_NAMESPACE is an alias for CREATE/DROP/EVOLVE
            if ("MODIFY_IN_NAMESPACE".equals(nsPriv))
                continue;
            assertFalse(String.format(msg1, TEST_ROLE_B_NAME, nsPriv),
                result.contains(nsPriv));
        }

        // Case 4: GRANT *_IN_NAMESPACE ON NAMESPACE <namespaceName> ...
        for (String nsPriv : NS_PRIVS) {
            execStatement(adminStore, "GRANT " + nsPriv +
                " ON NAMESPACE Ns001 TO " + TEST_ROLE_B_NAME);
        }
        stmtRes = adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            if ("MODIFY_IN_NAMESPACE".equals(nsPriv))
                continue;
            assertTrue(String.format(msg2, TEST_ROLE_B_NAME, nsPriv+ "(Ns001)"),
                result.contains(nsPriv + "(Ns001)"));
        }

        // Test "REVOKE *_IN_NAMESPACE ON ..."
        final String msg3 = "%s privilege should not have been revoked from %s";
        final String msg4 = "%s privilege should have been revoked from %s";

        // Case 5: REVOKE *_IN_NAMESPACE ON NAMESPACE <tableName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "REVOKE " + nsPriv +
                        " ON NAMESPACE Ns001:T1 FROM " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "REVOKE " + nsPriv +
                        " ON NAMESPACE T2 FROM " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        // Case 6: REVOKE *_IN_NAMESPACE ON <tableName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "REVOKE " + nsPriv +
                        " ON Ns001:T1 FROM " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "REVOKE " + nsPriv +
                        " ON T2 FROM " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }

        // Case 7: REVOKE *_IN_NAMESPACE ON <namespaceName> ...
        for (String nsPriv : NS_PRIVS) {
            new ExpectErrorExecution() {
                @Override
                void doStatement() throws Exception {
                    execStatement(adminStore, "REVOKE " + nsPriv +
                        " ON Ns001 FROM " + TEST_ROLE_B_NAME);
                }
            }.exec(IllegalArgumentException.class);
        }
        stmtRes = adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            if ("MODIFY_IN_NAMESPACE".equals(nsPriv))
                continue;
            assertTrue(String.format(msg3, nsPriv, TEST_ROLE_B_NAME),
                result.contains(nsPriv));
        }

        // Case 8: REVOKE *_IN_NAMESPACE ON NAMESPACE <namespaceName> ...
        for (String nsPriv : NS_PRIVS) {
            execStatement(adminStore, "REVOKE " + nsPriv +
                " ON NAMESPACE Ns001 FROM " + TEST_ROLE_B_NAME);
        }
        stmtRes = adminStore.executeSync("SHOW ROLE " + TEST_ROLE_B_NAME);
        assertTrue(stmtRes.isSuccessful());
        result = stmtRes.getResult();
        for (String nsPriv : NS_PRIVS) {
            if ("MODIFY_IN_NAMESPACE".equals(nsPriv))
                continue;
            assertFalse(
                String.format(msg4, nsPriv + "(Ns001)", TEST_ROLE_B_NAME),
                result.contains(nsPriv + "(Ns001)"));
        }

        // close the test store handle
        testStore.close();
    }

    @Test
    public void testSysdefaultNamespace()
        throws Exception {
        final TableAPI tableAPI = adminStore.getTableAPI();

        assertTrue(NameUtils.isInternalInitialNamespace(null));
        assertFalse(NameUtils.isInternalInitialNamespace(""));

        Collection<String> nses = tableAPI.listNamespaces();
        assertEquals(1, nses.size());
        assertTrue(nses.contains("sysdefault"));
        assertEquals(1, nses.size());
        assertEquals("sysdefault", nses.iterator().next());

        // Get the tables in the "sysdefault" namespace.
        Map<String, Table>sysdefaultTables = tableAPI.getTables("sysdefault");
        assertNotNull(sysdefaultTables);

        assertTrue(sysdefaultTables.size() > 0);
        Table et = tableAPI.getTable("sysdefault", "employee");
        assertNotNull(et);
        assertEquals("sysdefault", et.getNamespace());

        Table et2 = tableAPI.getTable("employee");
        assertNotNull(et);
        assertTrue(et.equals(et2));

        // null gets "sysdefault" namespace too
        Map<String, Table> nullTables = tableAPI.getTables(null);
        assertNotNull(nullTables);
        assertTrue(nullTables.size() > 0);

        et = tableAPI.getTable("employee");
        assertNotNull(et);
        assertEquals("sysdefault", et.getNamespace());

        // check CREATE/DROP/change privs for "sysdefault" namespace
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "CREATE NAMESPACE sysdefault");
            }
        }.exec(IllegalArgumentException.class);

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "DROP NAMESPACE sysdefault");
            }
        }.exec(IllegalArgumentException.class);

        // check privileges on "sysdefault" namespace
        execStatement(adminStore, "CREATE USER " + TEST_USER_NAME +
            " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "GRANT " + TEST_ROLE_NAME +
            " TO USER " + TEST_USER_NAME);

        final KVStore testStore =
            loginKVStoreUser(TEST_USER_NAME, TEST_USER_PW);

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "CREATE TABLE sysdefault:t1(id INTEGER, " +
                    "firstName STRING, lastName STRING, PRIMARY KEY (id))");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "REVOKE CREATE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE sysdefault FROM " + TEST_ROLE_NAME);

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "CREATE TABLE sysdefault:t2(id INTEGER, " +
                    "firstName STRING, lastName STRING, PRIMARY KEY (id))");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT CREATE_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE sysdefault TO " + TEST_ROLE_NAME);

        execStatement(testStore, "CREATE TABLE sysdefault:t3(id INTEGER, " +
            "firstName STRING, lastName STRING, PRIMARY KEY (id))");
        execStatement(testStore, "DROP TABLE sysdefault:t3");


        execStatement(adminStore, "GRANT CREATE_ANY_TABLE TO " +
            TEST_ROLE_NAME);

        execStatement(testStore, "CREATE TABLE sysdefault:t4(id INTEGER, " +
            "firstName STRING, lastName STRING, PRIMARY KEY (id))");
        execStatement(testStore, "DROP TABLE sysdefault:t4");


        // DROP TABLE
        execStatement(adminStore, "CREATE TABLE sysdefault:t5(id INTEGER, " +
            "firstName STRING, lastName STRING, PRIMARY KEY (id))");

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "DROP TABLE sysdefault:t5");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "REVOKE DROP_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE sysdefault FROM " + TEST_ROLE_NAME);

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "DROP TABLE sysdefault:t5");
            }
        }.exec(UnauthorizedException.class);

        execStatement(adminStore, "GRANT DROP_TABLE_IN_NAMESPACE ON " +
            "NAMESPACE sysdefault TO " + TEST_ROLE_NAME);

        execStatement(testStore, "DROP TABLE sysdefault:t5");


        execStatement(adminStore, "CREATE TABLE sysdefault:t6(id INTEGER, " +
            "firstName STRING, lastName STRING, PRIMARY KEY (id))");
        execStatement(adminStore, "GRANT DROP_ANY_TABLE TO " +
            TEST_ROLE_NAME);

        execStatement(testStore, "DROP TABLE sysdefault:t6");

        // Test dropping sysdefault namespace
        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(testStore, "DROP namespace sysdefault");
            }
        }.exec(UnauthorizedException.class);

        new ExpectErrorExecution() {
            @Override
            void doStatement() throws Exception {
                execStatement(adminStore, "DROP namespace sysdefault");
            }
        }.exec(IllegalArgumentException.class);

        // cleanup roles and users
        execStatement(adminStore, "DROP ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore, "DROP USER " + TEST_USER_NAME + " CASCADE");
    }

    @Test
    public void testConcurrentNsDdl() throws Exception {

        execStatement(adminStore, "CREATE ROLE " + TEST_ROLE_NAME);
        execStatement(adminStore,
            "GRANT CREATE_ANY_NAMESPACE, DROP_ANY_NAMESPACE TO " +
            TEST_ROLE_NAME);

        final String ns = "ConcurrentNsDdl";
        final int threadCnt = 10;
        final String[] testUsers = new String[threadCnt];
        final KVStore[] testStores = new KVStore[threadCnt];

        /* test 'create namespace' operations concurrently */
        String ddl = "CREATE NAMESPACE " + ns;
        final ExecutorService service = Executors.newFixedThreadPool(threadCnt);
        final List<Future<Integer>> list =
            new ArrayList<Future<Integer>>(threadCnt);
        for (int i = 0; i < threadCnt; i++) {
            testUsers[i] = TEST_USER_NAME + i;
            execStatement(adminStore, "DROP USER " + testUsers[i]);
            execStatement(adminStore,
                String.format(CREATE_USER, testUsers[i], TEST_USER_PW));
            execStatement(adminStore,
                "GRANT " + TEST_ROLE_NAME + " TO USER " + testUsers[i]);
            testStores[i] = loginKVStoreUser(testUsers[i], TEST_USER_PW);
            final Future<Integer> future = service.submit(
                new DdlTask(testStores[i], ddl, i));
            list.add(future);
        }

        /* call internal APIs to get the owner of specified namespace */
        final TableAPIImpl tableAPI = ((TableAPIImpl) adminStore.getTableAPI());
        TableMetadata md = tableAPI.getTableMetadata();
        NamespaceImpl nsImpl = md.getNamespace(ns);
        assertNotNull("Namespace '" + ns + "' should have been created.",
            nsImpl);
        assertNotNull("Owner of namespace '" + ns + "' should not be null.",
            nsImpl.getOwner());

        int result = 0;
        for (int i = 0; i < threadCnt; i++) {
            final int taskId = list.get(i).get();
            /* failed task for invalid task */
            if (taskId < 0 || taskId >= testUsers.length) {
                continue;
            }
            ++result;
            assertEquals("Owner of namespace '" + ns + "' is incorrect!",
                testUsers[taskId], nsImpl.getOwner().name());
        }
        assertEquals("At most 1 out of " + threadCnt + " concurrent \"" +
            ddl + "\" operations could succeed",
            1, result);

        /* test 'drop namespace' operations concurrently */
        ddl = "DROP NAMESPACE " + ns;
        list.clear();
        for (int i = 0; i < threadCnt; i++) {
            testUsers[i] = TEST_USER_NAME + i;
            final Future<Integer> future = service.submit(
                new DdlTask(testStores[i], ddl, (i + 1)));
            list.add(future);
        }

        result = 0;
        for (int i = 0; i < threadCnt; i++) {
            final int taskId = list.get(i).get();
            if (taskId < 0 || taskId >= testUsers.length) {
                continue;
            }
            ++result;
        }
        assertEquals("At most 1 out of " + threadCnt + " concurrent \"" +
            ddl + "\" operations could succeed",
            1, result);

        final Set<String> namespaces = tableAPI.listNamespaces();
        assertNotNull(namespaces);
        assertFalse("Namespace '" + ns + "' should have been dropped",
            namespaces.contains(ns));

        /* clean up */
        service.shutdown();
        for (String user : testUsers) {
            execStatement(adminStore, "DROP USER " + user);
        }

        for (KVStore store : testStores) {
            if (store != null) {
                store.close();
            }
        }

    }

    @Test
    public void testShowCommand()
        throws Exception {

        /* Create 6 users and roles for testing */
        for (int i = 0; i < 6; i++) {
            execStatement(adminStore, "CREATE USER testshow" + i +
                " IDENTIFIED BY \"" + TEST_USER_PW + "\"");
            execStatement(adminStore, "CREATE ROLE testshow" + i);
        }

        /* Test show users in JSON output */
        String results =
            adminStore.executeSync("SHOW AS JSON USERS").getResult();
        ObjectNode jsonResults = JsonUtils.parseJsonObject(results);
        Iterable<JsonNode> array = JsonUtils.getArray(jsonResults, "users");
        int found = 0;
        for (JsonNode user : array) {
            String userName = JsonUtils.getAsText(user, "name");
            if (userName.contains("testshow")) {
                found++;
            }
        }
        assertTrue(found == 6);

        /* Test show roles in JSON output */
        found = 0;
        results = adminStore.executeSync("SHOW AS JSON ROLES").getResult();
        jsonResults = JsonUtils.parseJsonObject(results);
        array = JsonUtils.getArray(jsonResults, "roles");
        for (JsonNode role : array) {
            String roleName = JsonUtils.getAsText(role, "name");
            if (roleName.contains("testshow")) {
                found++;
            }
        }
        assertTrue(found == 6);

        /* Test show single user */
        execStatement(adminStore, "GRANT readwrite TO USER testshow0");
        results = adminStore.executeSync(
            "SHOW AS JSON USER testshow0").getResult();
        jsonResults = JsonUtils.parseJsonObject(results);
        assertTrue(jsonResults.get("name").asText().equals("testshow0"));
        assertTrue(jsonResults.get("enabled").asBoolean());
        assertTrue(jsonResults.get("retain-passwd").asText().equals("inactive"));
        Iterable<JsonNode> grantedRoles =
            JsonUtils.getArray(jsonResults, "granted-roles");
        Set<String> roles = new HashSet<>();
        for (JsonNode role : grantedRoles) {
            roles.add(role.asText());
        }
        assertTrue(roles.size() == 2);
        assertTrue(roles.contains("readwrite"));
        assertTrue(roles.contains("public"));
        roles.clear();

        /* Test show single role */
        execStatement(adminStore, "GRANT CREATE_ANY_INDEX TO testshow0");
        execStatement(adminStore,
            "GRANT READ_TABLE on " + MANAGER_TABLE + " TO testshow0");
        execStatement(adminStore, "GRANT readwrite TO ROLE testshow0");
        execStatement(adminStore, "GRANT testshow1 TO ROLE testshow0");
        results = adminStore.executeSync(
            "SHOW AS JSON ROLE testshow0").getResult();
        jsonResults = JsonUtils.parseJsonObject(results);
        assertTrue(jsonResults.get("name").asText().equals("testshow0"));
        assertTrue(jsonResults.get("assignable").asBoolean());
        assertFalse(jsonResults.get("readonly").asBoolean());
        grantedRoles =
            JsonUtils.getArray(jsonResults, "granted-roles");
        for (JsonNode role : grantedRoles) {
            roles.add(role.asText());
        }
        assertTrue(roles.size() == 2);
        assertTrue(roles.contains("readwrite"));
        assertTrue(roles.contains("testshow1"));
        Set<String> privs = new HashSet<>();
        Iterable<JsonNode> grantedPrivs =
            JsonUtils.getArray(jsonResults, "granted-privileges");
        for (JsonNode priv : grantedPrivs) {
            privs.add(priv.asText());
        }
        assertTrue(privs.size() == 2);
        assertTrue(privs.contains("CREATE_ANY_INDEX"));
        assertTrue(privs.contains("READ_TABLE(" + MANAGER_TABLE + ")"));
    }

    private void loginFailed(String userName, String password) {
        try {
            loginKVStoreUser(userName, password);
            fail("expected exception");
        } catch (AuthenticationFailureException afe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    private static void addTestTables()
        throws Exception {

        Table employee = addEmployeeTable(adminStore);
        addTableData(adminStore.getTableAPI(), employee, 1, "jack", "jefer");
        addManagerTable(adminStore);
    }

    private static Table addEmployeeTable(KVStore store)
        throws Exception {

        execStatement(store,
            "CREATE TABLE " + EMPLOYEE_TABLE +
            " COMMENT \"employee table\" (" +
            "id INTEGER, " +
            "firstName STRING, " +
            "lastName STRING, " +
            "PRIMARY KEY (id))");

        /* Add few data into table */
        TableAPI tableImpl = store.getTableAPI();
        Table employee = tableImpl.getTable(EMPLOYEE_TABLE);
        return employee;
    }

    private static Table addManagerTable(KVStore store)
        throws Exception {

        execStatement(store,
            "CREATE TABLE " + MANAGER_TABLE +
            " COMMENT \"manager table\" (" +
            "id INTEGER, " +
            "firstName STRING, " +
            "lastName STRING, " +
            "PRIMARY KEY (id))");

        /* Add few data into table */
        TableAPI tableImpl = store.getTableAPI();
        Table manager = tableImpl.getTable(MANAGER_TABLE);
        return manager;
    }

    private static void addTestNamespaces(KVStore store, int num)
        throws Exception {

        for (int i = 0 ; i < num; i++) {
            execStatement(store,
                "CREATE NAMESPACE ns" + i + " ");
        }
    }

    private static List<String> addTestTablesComplex(KVStore store, int num,
        int nestLevel, boolean addIndexes, String ns)
        throws Exception {
        String tableName = null;
        int nl = 0;
        List<String> tableNames = new ArrayList<>(num);
        for(int i = 0; i < num; i++) {
            if (tableName == null) {
                tableName = (ns == null ? "table" : ns + ":table") + i;
            } else {
                tableName += ".table" + i;
            }
            final String colSfx = (nl == 0) ? "" : Integer.toString(nl);
            execStatement(store, String.format(
                    "CREATE TABLE %s COMMENT \"test table\" (" +
                    "id%s INTEGER, " +
                    "firstName%s STRING, " +
                    "lastName%s STRING, " +
                    "PRIMARY KEY (id%s))",
                    tableName, colSfx, colSfx, colSfx, colSfx));
            if (addIndexes) {
                execStatement(store, String.format(
                        "CREATE INDEX index%d ON %s(firstname%s)",
                        i, tableName, colSfx));
            }
            tableNames.add(tableName);
            if ((++nl) == nestLevel) {
                nl = 0;
                tableName = null;
            }
        }
        return tableNames;
    }

    private static void grantTablePrivsToRoles(KVStore store,
            List<String> tableNames,
            int numRoles) throws Exception {
        final String[] TABLE_PRIVS = { "READ_TABLE",
                "DELETE_TABLE",
                "INSERT_TABLE",
                "EVOLVE_TABLE",
                "CREATE_INDEX",
                "DROP_INDEX"
        };
        final boolean reuseRoles = tableNames.size() >= numRoles;
        int privIdx = 0;
        int roleIdx = 0;
        int tableIdx = 0;
        while(true) {
            execStatement(store,
                    String.format("GRANT %s ON %s TO role%d",
                            TABLE_PRIVS[privIdx],
                            tableNames.get(tableIdx),
                            roleIdx));
            privIdx = (privIdx + 1) % TABLE_PRIVS.length;
            roleIdx = (roleIdx + 1) % numRoles;
            if (reuseRoles) {
                if ((++tableIdx) == tableNames.size()) {
                    break;
                }
                roleIdx = (roleIdx + 1) % numRoles;
            } else {
                if ((++roleIdx) == numRoles) {
                    break;
                }
                tableIdx = (tableIdx + 1) % tableNames.size();
            }
        }
    }

    /**
     * Gets the number of user tables only. System tables are not counted.
     */
    private static int countUserTables(TableAPI tableAPI, String ns) {
        final TableMetadata md = ((TableAPIImpl)tableAPI).getTableMetadata();
        int count = 0;
        for (String tableName : md.listTables(ns, true)) {
            if (!tableName.startsWith(TableImpl.SYSTEM_TABLE_PREFIX)) {
                count++;
            }
        }
        return count;
    }

    private static int countUserTables(TableAPI tableAPI) {
        return countUserTables(tableAPI, null);
    }

    private static List<String> addTestTablesInNs(KVStore store, int num, String ns)
        throws Exception {
        return addTestTablesComplex(store, num, 1, false, ns);
    }

    private static List<String> addTestTablesWithIndex(KVStore store, int num)
        throws Exception {
        return addTestTablesComplex(store, num, 1, true, null);
    }

    private static void addTestRoles(KVStore store, int num, String user)
        throws Exception {

        for (int i = 0 ; i < num; i++) {
            execStatement(store, "CREATE ROLE role" + i);
            if (user != null) {
                execStatement(store, "GRANT role" + i + " TO USER " + user);
            }
        }
    }

    private static void addTableData(TableAPI tableImpl, Table table, int id,
                                     String firstName, String lastName) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("firstName", firstName);
        row.put("lastName", lastName);
        tableImpl.put(row, null, null);
    }

    private static Row readTableData(TableAPI tableImpl, Table table, int id) {
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        return tableImpl.get(pk, null);
    }

    private static Row readIterTableData(TableAPI tableImpl, Table table,
        int id) {
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        TableIterator<Row> it = tableImpl.tableIterator(pk, null, null);
        assertNotNull(it);
        assertEquals(true, it.hasNext());
        return it.next();
    }

    private static boolean deleteTableData(TableAPI tableImpl, Table table, int id)
    {
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        return tableImpl.delete(pk, null, null);
    }

    private static void addTestKVData(KVStore store, String key, String value) {
        store.put(Key.fromString(key), Value.createValue((value.getBytes())));
    }

    /**
     * Run a code snippet and expect a specified error.
     */
    private abstract class ExpectErrorExecution {

        void exec(Class<?> expectedError) {
            try {
                doStatement();
                fail("Expected " + expectedError.getSimpleName());
            } catch (Exception e) {
                if (! expectedError.isInstance(e) ) {
                    fail("Didn't expect " + e);
                }
            }
        }

        abstract void doStatement() throws Exception;
    }

    private static class DdlTask implements Callable<Integer> {
        private final KVStore store;
        private final String ddl;
        private final int taskId;

        DdlTask(KVStore store, String ddl, int taskId) {
            this.store = store;
            this.ddl = ddl;
            this.taskId = taskId;
        }

        @Override
        public Integer call() throws Exception
        {
            try {
                execStatement(store, ddl);
                return taskId;
            } catch (Exception ex) {
                return -1;
            }
        }
    }
}
