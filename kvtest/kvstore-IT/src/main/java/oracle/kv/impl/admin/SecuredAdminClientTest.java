/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.logging.Level.INFO;
import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.TRANSPORT_PROPERTY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.sna.ManagedService;
import oracle.kv.impl.sna.StorageNodeBasic;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import org.junit.Test;

/**
 * Use to test the CommandShell command-line client against a secured kvstore.
 */
public class SecuredAdminClientTest extends AdminClientTestBase {

    /* Users will be created in the system */
    private static final String USER = "user";
    private static final String PASS = "NoSql00__pass";
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASS = "NoSql00__adminpass";
    private static final String USER2 = "user2";
    private static final String PASS2 = "NoSql00__pass2";
    private static final String ANONYMOUS = "anonymous";

    private static final String TOPO = "testTopo";
    private static final String POOL = "testPool";
    private static final String JLINE_DISABLE = "oracle.kv.shell.jline.disable";
    private static final String SYS_CONSOLE_DISABLE =
        "oracle.kv.shell.sys.console.disable";

    /* Security configuration */
    private static final String SEC_DIR =
        TestUtils.getTestDir() + File.separator + "security";
    private static final String SEC_FILE =
        TestUtils.getTestDir() + File.separator + "secfile";

    /* Table used in the table admin and data command test */
    private static final String USER_TABLE_NAME = "user";
    private static final TableBuilder USER_TABLE =
        (TableBuilder) TableBuilder.createTableBuilder(USER_TABLE_NAME)
                       .addInteger("id")
                       .addString("name")
                       .addInteger("age")
                       .primaryKey("id");
    private static final String EMPLOYEE_TABLE_NAME = "employee";
    private static final TableBuilder EMPLOYEE_TABLE =
        (TableBuilder)TableBuilder.createTableBuilder(EMPLOYEE_TABLE_NAME)
                      .addInteger("id")
                      .addString("name")
                      .addInteger("salary")
                      .primaryKey("id");

    /* Admin command return message */
    private static final String ACCESS_DENIED = "Insufficient access rights";
    private static final String PLAN_SUCCEED = "ended successfully";

    private String testUser;

    public SecuredAdminClientTest() {
        super(true);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createPasswordStore();
        createSecurityFile();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testDropUser()
        throws Exception {

        initAdminForUserCmds();

        /* Add several users */
        assertCLICommandResult(
            "Add another admin", ADMIN_USER,
            "plan create-user -name admin2 -admin " +
            "-password NoSql00__adminpass2 -wait",
            "ended successfully");

        assertCLICommandResult(
            "Add another common user", ADMIN_USER,
            "plan create-user -name user2 -password NoSql00__pass2 -wait",
            "ended successfully");

        /* Test drop users */
        assertCLICommandResult("Missing name", ADMIN_USER,
                               "plan drop-user -wait",
                               "Missing required argument");

        assertCLICommandResult("Not allow dropping self", ADMIN_USER,
                               "plan drop-user -name admin -wait",
                               "A current online user cannot drop itself");

        assertCLICommandResult("Drop another admin", ADMIN_USER,
                               "plan drop-user -name admin2 -wait",
                               "ended successfully");

        assertCLICommandResult("Drop a non-existing user", ADMIN_USER,
                               "plan drop-user -name admin2 -wait",
                               "ended successfully");

        assertCLICommandResult("Non-admin user cannot perform dropping",
                               USER, "plan drop-user -name user2 -wait",
                               ACCESS_DENIED);

        assertCLICommandResult("Drop a non-admin user", ADMIN_USER,
                               "plan drop-user -name user2 -wait",
                               "ended successfully");

        assertCLICommandResult("Drop user operation is idempotent",
                               ADMIN_USER, "plan drop-user -name user2 -wait",
                               "ended successfully");
    }

    @Test
    public void testUserPermission()
        throws Exception {

        initAdminForUserCmds();

        /* Verify limited permission of non-admin users */

        assertCLICommandResult("Non-admin user cannot create users", USER,
                               "plan create-user -name u2 " +
                               "-password NoSql00__1 -wait",
                               ACCESS_DENIED);

        assertCLICommandResult("Non-admin user cannot change others", USER,
                               "plan change-user -name admin -disable -wait",
                               "Admin privilege is required");

        assertCLICommandResult("Non-admin user cannot drop users", USER,
                               "plan drop-user -name user -wait",
                               ACCESS_DENIED);

        assertCLICommandResult("Non-admin user can change itself", USER,
                               "plan change-user -name user -disable -wait",
                               "ended successfully");

        /* Verify privileged permission of admin users */

        assertCLICommandResult(
            "Admin can create users", ADMIN_USER,
            "plan create-user -name u2 -admin -password NoSql00__1 -wait",
            "ended successfully");

        assertCLICommandResult("Admin can change others", ADMIN_USER,
                               "plan change-user -name u2 -disable -wait",
                               "ended successfully");

        assertCLICommandResult("Admin user can drop users", ADMIN_USER,
                               "plan drop-user -name u2 -wait",
                               "ended successfully");
    }

    @Test
    public void testCLILogin()
        throws Exception {
        deployStoreWithoutUsers();

        final String EMPTY_CMD = Shell.eol;
        assertCLICommandResult("Login admin as anonymous", ANONYMOUS,
            EMPTY_CMD, "Logged in to Admin as anonymous");

        assertCLICommandResult("Cannot login to Admin with empty security MD",
            USER, EMPTY_CMD,
            "Login failed: No user data exists");

        assertCLICommandResult(
            "Cannot login store with empty security MD",
            USER,
            "-store " + kvstoreName + " " + EMPTY_CMD /* connect to store */,
            "Login failed: No user data exists, only anonymous login is" +
                " allowed.");

        /* Create a user */
        assertCLICommandResult(
            "Add admin user", ANONYMOUS,
            String.format("plan create-user -name %s -admin -password %s -wait",
                ADMIN_USER, ADMIN_PASS),
            "ended successfully");

        assertCLICommandResult("Login only admin as ADMIN_USER", ADMIN_USER,
            EMPTY_CMD, "Logged in to Admin as admin");

        assertCLICommandResult(
            "Store and admin login share login info. when no admin-username",
            ADMIN_USER, "-store " + kvstoreName + " " + EMPTY_CMD /* connect to store */,
            "Logged in to Admin as admin");

        System.setIn(new ByteArrayInputStream(new byte[] {-1}));
        System.setProperty(JLINE_DISABLE, "true");
        System.setProperty(SYS_CONSOLE_DISABLE, "true");
        try {
            assertCLICommandResult(
                "Store and admin login share login info. when store user is " +
                    "same with admin user", ADMIN_USER, "-store " + kvstoreName +
                    " -admin-username admin " + EMPTY_CMD, "Admin localhost:" +
                    sna1.getRegistryPort() + " requires authentication.");
        } finally {
            System.setIn(System.in);
            System.setProperty(JLINE_DISABLE, "false");
            System.setProperty(SYS_CONSOLE_DISABLE, "false");
        }
    }

    @Test
    public void testTrustCLILogin()
        throws Exception {

        final String EMPTY_CMD = Shell.eol;
        final File secDir = new File(
            TestUtils.getTestDir(), FileNames.SECURITY_CONFIG_DIR);
        deployStoreWithoutUsers();

        assertCLICommandResult(
            "Login admin with store security dir", null,
            CommandParser.STORE_SECURITY_DIR_FLAG + " " +
             secDir.getAbsolutePath() + " " + EMPTY_CMD,
            "Logged in to Admin with store SSL credentials");

        assertCLICommandResult(
            "Login admin and store with store security dir", null,
            CommandParser.STORE_SECURITY_DIR_FLAG + " " +
            secDir.getAbsolutePath() + " -store " + kvstoreName +
             " "  + EMPTY_CMD,
            "Logged in to Admin and store with store SSL credentials");

        assertCLICommandResult(
            "Login admin with invalid store security dir", null,
            CommandParser.STORE_SECURITY_DIR_FLAG + " " +
             TestUtils.getTestDir() + " " + EMPTY_CMD,
            "Unable to login with store SSL credentials");

        sna1.shutdown(true, true);
        assertCLICommandResult(
            "Login admin when SN is down", null,
            CommandParser.STORE_SECURITY_DIR_FLAG + " " +
                secDir.getAbsolutePath() + " " + EMPTY_CMD,
            "Unable to login with store SSL credentials, " +
             "cannot connect to any admin");
    }

    @Test
    public void testPlanOwnerShip() throws Exception {
        initAdminForUserCmds();

        final CommandServiceAPI csForAdmin = getAdmin(ADMIN_USER);
        final CommandServiceAPI csForUser1 = getAdmin(USER);
        final CommandServiceAPI csForUser2 = getAdmin(USER2);

        /* Record how may plans has been executed so far */
        int numOfPlan =
            csForAdmin.getPlanRange(1 /* start id */, 20 /* howmany */).size();

        /* Non-SYSVIEW users could not see plans with null owner */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                /* Plan 1 is created by anonymous and thus has no owner */
                csForUser1.getPlanById(1);
            }
        });

        final int user1PlanId = csForUser1.createChangeUserPlan(
            "Change user attempt", USER, true, null, false, false);
        numOfPlan++;

        /* Users with SYSVIEW can see all plans */
        final Plan user1Plan = csForAdmin.getPlanById(user1PlanId);
        final String u1Name = user1Plan.getOwner().name();
        assertEquals(u1Name, USER);

        final Plan nullOwnerPlan = csForAdmin.getPlanById(1);
        assertNull(nullOwnerPlan.getOwner());

        /* Non-SYSVIEW users are unable to get plans created by others. */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForUser2.getPlanById(user1PlanId);
            }
        });

        /* Non-SYSVIEW users are unable to get plan status created by others. */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForUser2.getPlanStatus(user1PlanId, 0L /* options */,
                                         false /* json */);
            }
        });

        /* SYSVIEW users can see all plans */
        final Map<Integer, Plan> plansOfAdmin =
            csForAdmin.getPlanRange(1 /* start id */, 20 /* howmany */);
        assertEquals(plansOfAdmin.size(), numOfPlan);
        assertTrue(plansOfAdmin.containsKey(user1PlanId));

        /* SYSVIEW users can see ids of all plans in range. */
        final int[] planIdsOfAdmin =
            csForAdmin.getPlanIdRange(0L, System.currentTimeMillis(), 20);
        assertEquals(planIdsOfAdmin[1], numOfPlan);

        /* Non-SYSVIEW users can only see their own plans in range. */
        final Map<Integer, Plan> plansOfUser1 =
            csForUser1.getPlanRange(1 /* start id */, 10 /* howmany */);
        assertEquals(plansOfUser1.size(), 1);
        assertTrue(plansOfUser1.containsKey(user1PlanId));

        /* Non-SYSVIEW users can only see ids of their own plans in range. */
        final int[] planIdsOfUser1 =
            csForUser1.getPlanIdRange(0L, System.currentTimeMillis(), 10);
        assertEquals(planIdsOfUser1[1], 1); /* Can see only one plan */
        assertEquals(planIdsOfUser1[0], user1PlanId);

        /* Non-SYSVIEW users who has no plans cannot see any plan */
        final Map<Integer, Plan> plansOfUser2 =
            csForUser2.getPlanRange(1 /* start id */, 10 /* howmany */);
        assertEquals(plansOfUser2.size(), 0);

        /* Non-SYSVIEW users who has no plans cannot see any plan */
        final int[] planIdsOfUser2 =
            csForUser2.getPlanIdRange(0L, System.currentTimeMillis(), 10);
        assertEquals(planIdsOfUser2[0], 0); /* Cannot see any plan */

        /* Users are unable to operate plans created by others. */
        testPlanOpsDenied(csForUser2, user1PlanId);

        /*
         * Checks SYSOPER users can operate plans created by other users. Never
         * mind the results, just check the permission, because permission
         * check precedes the execution of plan operations.
         */
        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForAdmin.approvePlan(user1PlanId);
            }
        });

        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForAdmin.executePlan(user1PlanId, false);
            }
        });

        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForAdmin.awaitPlan(user1PlanId, 1, TimeUnit.SECONDS);
            }
        });

        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForAdmin.cancelPlan(user1PlanId);
            }
        });

        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForAdmin.interruptPlan(user1PlanId);
            }
        });

        /*
         * Checks that when a new user is added with the same name of a removed
         * user, the new user could not access or operate the plans created by
         * that removed user.
         */

        assertCLICommandResult("Drop user", ADMIN_USER,
                               "plan drop-user -name user -wait",
                               "ended successfully");

        assertCLICommandResult(
            "Add the same user again", ADMIN_USER,
            "plan create-user -name user -password " + PASS +
            " -wait", PLAN_SUCCEED);

        /* USER should have a different uId now, re-login */
        final CommandServiceAPI csForNewUser1 = getAdmin(USER);

        /* Could not access */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                csForNewUser1.getPlanById(user1PlanId);
            }
        });

        /* Could not operate plans created by old USER */
        testPlanOpsDenied(csForNewUser1, user1PlanId);
    }

    @Test
    public void testShowUsers() throws Exception {
        initSecuredAdmin();

        /* Without any users or roles, getUsersDescription return null */
        final CommandServiceAPI csForAnonymous = getAnonymousAdmin();
        assertNull(csForAnonymous.getUsersDescription());

        /*
         * Created a role, security metadata is not null. Now
         * getUsersDescription return a empty map, since no users are created.
         */
        assertStatementDDLCommandResult("testDDLCreateRole", null,
            "CREATE ROLE role_test",
            "Statement completed successfully");
        assertNotNull(csForAnonymous.getUsersDescription());

        /*
         * Create three users.
         */
        createUsers();
        final CommandServiceAPI csForAdmin = getAdmin(ADMIN_USER);
        final CommandServiceAPI csForUser1 = getAdmin(USER);

        /* SYSVIEW user can see all users */
        Map<String, UserDescription> users = csForAdmin.getUsersDescription();
        assertEquals(3, users.size());

        /* Non-SYSVIEW user cannot see other users */
        users = csForUser1.getUsersDescription();
        assertNotNull(users.get(USER));
        assertNull(users.get(ADMIN_USER));
        assertNull(users.get(USER2));
    }

    /*
     * Checks users could not operate their own plans if the required
     * privileges of the plans have been revoked from the users.
     */
    @Test
    public void testPlanPrivilegeCheckEnforced() throws Exception {
        initAdminForUserCmds();

        assertCLICommandResult("Grant sysadmin to user", ADMIN_USER,
                               roleCommand("grant", USER, "sysadmin"),
                               PLAN_SUCCEED);

        CommandServiceAPI csForUser1 = getAdmin(USER);

        /*
         * Creates a createUserPlan, which needs SYSOPER privilege belonging
         * to sysadmin role.
         */
        final int planId =
            csForUser1.createCreateUserPlan("Create user", "test",
                                            true /*isEnabled */,
                                            false /* isAdmin */,
                                            PASS.toCharArray());
        csForUser1.approvePlan(planId);

        /* Revokes the sysadmin role from USER */
        assertCLICommandResult("Revoke sysadmin from user", ADMIN_USER,
                               roleCommand("revoke", USER, "sysadmin"),
                               PLAN_SUCCEED);

        /* USER can still access the plan */
        csForUser1.getPlanById(planId);

        /*
         * Because the SYSOPER privilege has been removed, the following
         * operations should fail since the CreateUserPlan requires SYSOPER.
         */
        testPlanOpsDenied(csForUser1, planId);
    }

    private void testPlanOpsDenied(final CommandServiceAPI cs,
                                   final int planId)
        throws Exception {

        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {

                cs.approvePlan(planId);
            }
        });

        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                cs.awaitPlan(planId, 1, TimeUnit.SECONDS);
            }
        });

        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                cs.executePlan(planId, false);
            }
        });

        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                cs.cancelPlan(planId);
            }
        });

        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                cs.interruptPlan(planId);
            }
        });
    }

    /** Test the 'plan create-user' command. **/
    @Test
    public void testCreateUser()

            throws Exception {

        initSecuredAdmin();

        assertCLICommandResult("Missing name",
                "plan create-user -admin -wait",
                "Missing required argument");

        assertCLICommandResult("Missing password",
                "plan create-user -name test -admin -password",
                "Flag -password requires an argument");

        assertCLICommandResult("First user should be admin",
                "plan create-user -name user " +
                        "-password NoSql00__1 -wait",
                "first user in the store must be -admin");

        assertCLICommandResult(
                "First user should be enabled",
                "plan create-user -name user -admin " +
                        "-password NoSql00__1 -disable -wait",
                "first user in the store must be -admin and enabled");

        assertCLICommandResult(
                "Create first user successfully",
                "plan create-user -name admin -admin " +
                        "-password NoSql00__adminpass -wait",
                "ended successfully");

        assertCLICommandResult("Show user",
                ADMIN_USER,
                "show users",
                "name=admin");

        assertCLICommandResult(
                "Create user operation is idempotent",
                ADMIN_USER,
                "plan create-user -name admin -admin -password NoSql00__adminpass -wait",
                "ended successfully");

        assertCLICommandResult(
                "Created user existed",
                ADMIN_USER,
                "plan create-user -name admin -password NoSql00__adminpass -wait",
                "admin already exists but has admin setting of true");

        assertCLICommandResult(
                "Created user existed 2",
                ADMIN_USER,
                "plan create-user -name admin -admin -disable " +
                        "-password NoSql00__adminpass -wait",
                "admin already exists but has enabled state of true");

        assertCLICommandResult(
                "Created user existed 3",
                ADMIN_USER,
                "plan create-user -name admin -admin -password NoSql00__2 -wait",
                "admin already exists but has different password");

        assertCLICommandResult(
                "Create other user successfully",
                ADMIN_USER,
                "plan create-user -name user -password NoSql00__1 -wait",
                "ended successfully");

        assertCLICommandResult("Show user",
                ADMIN_USER,
                "show users",
                "name=user");
    }

    /** Test the 'plan change-user' command. */
    @Test
    public void testChangeUser()
            throws Exception {

        initSecuredAdmin();

        assertCLICommandResult("Missing name",
                "plan change-user -set-password -wait",
                "Missing required argument");

        assertCLICommandResult("User does not exist",
                "plan change-user -name shadow -disable -wait",
                "User with name shadow does not exist");

        assertCLICommandResult(
                "Create first user successfully",
                "plan create-user -name admin -admin -password NoSql00__adminpass -wait",
                "ended successfully");

        assertCLICommandResult("Nothing changed",
                ADMIN_USER,
                "plan change-user -name admin -wait",
                "Nothing changed for user admin");

        assertCLICommandResult("Could not disable the only admin",
                ADMIN_USER,
                "plan change-user -name admin -disable -wait",
                "Cannot disable the last enabled admin user");

        assertCLICommandResult(
                "Missing -set-password",
                ADMIN_USER,
                "plan change-user -name admin -retain-current-password -wait",
                "Option -retain-current-password is only valid in conjunction " +
                        "with -set-password");

        assertCLICommandResult(
                "Missing -set-password",
                ADMIN_USER,
                "plan change-user -name admin -password -wait",
                "Option -password is only valid in conjunction with " +
                        "-set-password");

        assertCLICommandResult(
                "Missing password",
                ADMIN_USER,
                "plan change-user -name admin -set-password -password",
                "Flag -password requires an argument");

        assertCLICommandResult(
                "Create user",
                ADMIN_USER,
                "plan create-user -name admin2 -admin -password NoSql00__1 -wait",
                "ended successfully");

        assertCLICommandResult(
                "Change password",
                ADMIN_USER,
                "plan change-user -name admin2 -set-password " +
                        "-password NoSql00__2 -wait",
                "ended successfully");

        assertCLICommandResult(
                "Change and retain password",
                ADMIN_USER,
                "plan change-user -name admin2 -set-password -password " +
                        "NoSql00__3 " +
                        "-retain-current-password -wait",
                "ended successfully");

        assertCLICommandResult(
                "Show user to confirm the password is retained",
                ADMIN_USER,
                "show user -name admin2",
                "retain-passwd=active");

        assertCLICommandResult(
                "Could not retain password",
                ADMIN_USER,
                "plan change-user -name admin2 -set-password -password NoSql00__3 " +
                        "-retain-current-password -wait",
                "Could not retain password");

        assertCLICommandResult(
                "Clear retain password",
                ADMIN_USER,
                "plan change-user -name admin2 -clear-retained-password -wait",
                "ended successfully");

        assertCLICommandResult(
                "Show user to confirm the retained password is clear",
                ADMIN_USER,
                "show user -name admin2",
                "retain-passwd=inactive");
    }

    @Test
    public void testGrantRole()
        throws Exception {

        initAdminForUserCmds();

        assertCLICommandResult(
            "Missing user", ADMIN_USER,
            "plan grant -wait",
            "Missing required argument");

        assertCLICommandResult(
            "Missing user value", ADMIN_USER,
            "plan grant -user",
            "Flag -user requires an argument");

        assertCLICommandResult(
            "Missing role", ADMIN_USER,
            "plan grant -user root -wait",
            "Missing required argument");

        assertCLICommandResult(
            "Missing role value", ADMIN_USER,
            "plan grant -user root -role",
            "Flag -role requires an argument");

        assertCLICommandResult(
            "Non-sysadmin user cannot grant role to other user", USER,
            roleCommand("grant", ADMIN_USER, "readOnly"),
            ACCESS_DENIED);

        assertCLICommandResult(
            "Sysadmin user can grant role to other user", ADMIN_USER,
            roleCommand("grant", USER, "readonly"),
            PLAN_SUCCEED);

        assertCLICommandResult(
            "Grant role is idempotent", ADMIN_USER,
            roleCommand("grant", USER, "readonly"),
            PLAN_SUCCEED);

        assertCLICommandResult(
            "Sysadmin user can grant multiple roles to other user",
                ADMIN_USER,
            roleCommand("grant", USER, "writeonly", "dbadmin"),
            PLAN_SUCCEED);

        assertCLICommandResult(
            "Grant invalid role to user", ADMIN_USER,
            roleCommand("grant", USER, "root"),
            "does not exist");

        /*
         * Specify multiple role names to verify if validation can work in
         * this case.
         */
        assertCLICommandResult(
            "Grant invalid role to user", ADMIN_USER,
            roleCommand("grant", USER, "readonly", "root"),
            "does not exist");

        assertCLICommandResult(
            "Grant unassignable role public to user", ADMIN_USER,
            roleCommand("grant", USER, "public", "readonly"),
            "cannot be granted or revoked");

        assertCLICommandResult(
            "Grant invisible role internal to user", ADMIN_USER,
            roleCommand("grant", USER, "readonly", "INTERNAL"),
            "cannot be granted or revoked");
    }

    @Test
    public void testRevokeRole()
        throws Exception {

        initAdminForUserCmds();

        assertCLICommandResult(
            "Missing user", ADMIN_USER,
            "plan revoke",
            "Missing required argument");

        assertCLICommandResult(
            "Missing user value", ADMIN_USER,
            "plan revoke -user",
            "Flag -user requires an argument");

        assertCLICommandResult(
            "Missing role", ADMIN_USER,
            "plan revoke -user root -wait",
            "Missing required argument");

        assertCLICommandResult(
            "Missing role value", ADMIN_USER,
            "plan revoke -user root -role",
            "Flag -role requires an argument");

        assertCLICommandResult(
            "Non-sysadmin user cannot revoke role from other user", USER,
            roleCommand("revoke", ADMIN_USER, "readonly"),
                        ACCESS_DENIED);

        assertCLICommandResult(
            "Grant readwrite and dbadmin to user for testing",
                       ADMIN_USER,
            roleCommand("grant", USER, "dbadMIN", "readwrite"),
                        PLAN_SUCCEED);

        assertCLICommandResult(
            "Sysadmin user can revoke role from other user",
                       ADMIN_USER,
            roleCommand("revoke", USER, "reAdwrite", "dbadmin"),
                        PLAN_SUCCEED);

        assertCLICommandResult(
            "Revoke role user don't have", ADMIN_USER,
            roleCommand("revoke", USER, "readwrite"), PLAN_SUCCEED);

        assertCLICommandResult(
            "Revoke invalid role from user", ADMIN_USER,
            roleCommand("revoke", USER, "root"),
            "does not exist");

        assertCLICommandResult(
            "Revoke unassignable role public from user", ADMIN_USER,
            roleCommand("revoke", USER, "public"),
            "cannot be granted or revoked");

        assertCLICommandResult(
            "Revoke invisible role internal from user", ADMIN_USER,
            roleCommand("revoke", USER, "INTERNAL"),
            "cannot be granted or revoked");

        assertCLICommandResult(
            "Revoke sysadmin from last syadmin user", ADMIN_USER,
            roleCommand("revoke", ADMIN_USER, "sysaDmin"),
            "Cannot revoke sysadmin role");
    }

    /*
     * General permission check test for CLI commands.
     *
     * This test examine whether the permission check work correctly when
     * execute the commands need SYSOPER, SYSDBA, SYSVIEW privilege
     * respectively.
     */
    @Test
    public void testCLICommandsPermissionCheck()
        throws Exception {

        /* Deploy 1x1 store with one Admin */
        deployStoreWithoutUsers();
        createUsers();

        final CommandServiceAPI adminCs = getAdmin(ADMIN_USER);

        /* Add user table and schema for test */
        addTable(USER_TABLE, true /* should succeed */, adminCs);

        /*
         * Login with common user who only has public role. Each command
         * execution will fail and expect to get unauthorized exception.
         */
         runSysoperCommands(USER, false /* no SYSOPER */);
         runSysviewCommand(USER, false /* no SYSVIEW */);
         runSysdbaCommands(USER, false /* no SYSDBA */);
         runSysdbaCommands(USER2, false /* no SYSDBA */);

        /*
         * Grant dbadmin role to user2. All commands need SYSDBA privilege
         * should be able to pass permission check, the rest of commands cannot.
         */
        grantRoles(ADMIN_USER, USER2, "dbadmin");
        runSysdbaCommands(USER2, true /* has SYSDBA */);
        runSysoperCommands(USER2, false /* no SYSOPER */);
        runSysviewCommand(USER2, false /* no SYSVIEW */);

        /*
         * Grant sysadmin role to user. All commands in combo should be able to
         * pass permission check.
         */
        grantRoles(ADMIN_USER, USER, "sysadmin");

        /* Add user table for test, because runSysdbaCommands removed it */
        addTable(USER_TABLE, true, /* should succeed */ adminCs);
        runSysoperCommands(USER, true /* has SYSOPER */);
        runSysviewCommand(USER, true /* has SYSVIEW */);
        runSysdbaCommands(USER, true /* has SYSDBA */);

        /*
         * Revoke sysadmin from user and dbadmin from user2. They cannot
         * execute commands in combo then.
         */
        revokeRoles(ADMIN_USER, USER, "sysadmin");
        revokeRoles(ADMIN_USER, USER2, "dbadmin");

        /* Add user table for test, because previous test removed it */
        addTable(USER_TABLE, true, /* should succeed */ adminCs);
        runSysoperCommands(USER, false /* no SYSOPER */);
        runSysdbaCommands(USER, false /* no SYSDBA */);
        runSysviewCommand(USER, false /* no SYSVIEW */);

        runSysoperCommands(USER2, false /* no SYSOPER */);
        runSysdbaCommands(USER2, false /* no SYSDBA */);
        runSysviewCommand(USER2, false /* no SYSVIEW */);
    }

    /**
     * Attempt to run commands need SYSOPER privilege.
     */
    private void runSysoperCommands(final String user,
                                    final boolean privGranted)
        throws Exception {

        /*
         * To make sure each plan can finish successfully, some of them should
         * follow particular sequence:
         * deploy sn2 zone1
         * deploy admin2 on sn2
         * remove admin2
         * remove sn2
         */
        final String SNAPSHOT = "testBackup";
        final String NEW_POOL = "newPool";
        final String NEW_TOPO = "newTopo";

        /* Succeed without any output */
        assertCLICommandResult("Create pool " + NEW_POOL, user,
            "pool create -name " + NEW_POOL,
           privGranted ? "" : ACCESS_DENIED);

        assertCLICommandResult("Join sn1 to pool " + NEW_POOL, user,
            "pool join -name " + NEW_POOL + " -sn sn1",
            privGranted ? "Added Storage Node" : ACCESS_DENIED);

        assertCLICommandResult("Create topology " + NEW_TOPO, user,
            String.format("topology create -name %s -pool %s -partitions %d",
            NEW_TOPO, POOL, 10),
            privGranted ? "Created: " + NEW_TOPO : ACCESS_DENIED);

        /*
         * Should succeed if has SYSOPER privilege, though TOPO has been
         * deployed because of plan's idempotence.
         */
        assertCLICommandResult("Deploy topology " + TOPO, user,
            "plan deploy-topology -wait -name " + TOPO,
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        /* Succeed without any output */
        assertCLICommandResult("Change policy hideUserData", user,
            "change-policy -params hideUserData=false",
            privGranted ? "" : ACCESS_DENIED);

        /*
         * Test create and remove zone. Create the zone for testing with admin
         * user first, so that the zone existence check of remove-zone command
         * won't fail before the security check is performed.
         */
        try {
            assertCLICommandResult("Deploy zoneToRemove", ADMIN_USER,
                "plan deploy-zone -wait -name zoneToRemove -rf 1",
                PLAN_SUCCEED);
            assertCLICommandResult("Deploy zoneToRemove", user,
                "plan deploy-zone -wait -name zoneToRemove -rf 1",
                privGranted ? PLAN_SUCCEED : ACCESS_DENIED);
            assertCLICommandResult("Remove zoneToRemove", user,
                "plan remove-zone -znname zoneToRemove -wait",
                privGranted ? PLAN_SUCCEED : ACCESS_DENIED);
        } finally {
            if (!privGranted) {
                assertCLICommandResult("Remove zoneToRemove", ADMIN_USER,
                    "plan remove-zone -znname zoneToRemove -wait",
                    PLAN_SUCCEED);
            }
        }

        assertCLICommandResult("Deploy sn2 in zn1", user,
            String.format("plan deploy-sn -wait -host %s -port %s -zn %s",
            sna2.getHostname(), String.valueOf(sna2.getRegistryPort()), "zn1"),
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Deploy admin2 on sn2", user,
            "plan deploy-admin -wait -sn sn2",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Change parameters sessionTimeout", user,
            "plan change-parameters -wait -security " +
            "-params sessionTimeout=300_s",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        /* The directory is checked for existence on the SN. */
        assertCLICommandResult("Change sn2 storagedir", user,
            "plan change-storagedir -wait -sn sn2 -add -storagedir /",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        /*
         * Test remove-admin and remove-sn. When testing user doesn't have
         * SYSOPER privilege, removing admin1 and sn1 to verify the output
         * of security check, because sn2 and admin2 are only deployed when
         * testing user have the privilege.
         */
        final String adminToRemove = privGranted ? "admin2" : "admin1";
        final String snToRemove = privGranted ? "sn2" : "sn1";

        /*
         * Use force since only 2 Admins in store so that this plan cannot
         * normally execute
         */
        assertCLICommandResult("Remove " + adminToRemove, user,
            "plan remove-admin -admin " + adminToRemove + " -force -wait",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        /* Remove sn need SNA down */
        if (privGranted) {
            sna2.shutdown(true, false);
        }
        assertCLICommandResult("Remove " + snToRemove, user,
            "plan remove-sn -sn " + snToRemove + " -wait",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Create user root", user,
            "plan create-user -name root -password NoSql00__123 -wait",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Grant sysadmin to root", user,
            roleCommand("grant", "root", "sysadmin"),
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Revoke sysadmin from root", user,
            roleCommand("revoke", "root", "sysadmin"),
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Drop user root", user,
            "plan drop-user -wait -name root",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Stop rg1-rn1", user,
            "plan stop-service -service rg1-rn1 -wait -force",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Start rg1-rn1", user,
            "plan start-service -service rg1-rn1 -wait",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("Create snapshot", user,
            "snapshot create -name " + SNAPSHOT,
            privGranted ? "Created data snapshot" : ACCESS_DENIED);

        assertCLICommandResult("Remove snapshot", user,
            "snapshot remove -name " + SNAPSHOT,
            privGranted ? "Removed snapshot" : ACCESS_DENIED);

        assertCLICommandResult("Change repfactor of " + NEW_TOPO, user,
            String.format("topology change-repfactor -name %s -pool %s " +
            "-rf 2 -zn dc1", NEW_TOPO, NEW_POOL),
            privGranted ? "Changed replication factor" : ACCESS_DENIED);

        assertCLICommandResult("Delete " + NEW_TOPO, user,
            "topology delete -name " + NEW_TOPO,
            privGranted ? "Removed " + NEW_TOPO : ACCESS_DENIED);

        assertCLICommandResult("Clone current topology", user,
            "topology clone -current -name " + NEW_TOPO,
            privGranted ? "Created " + NEW_TOPO : ACCESS_DENIED);

        assertCLICommandResult("topology preview", user,
            "topology preview -name " + NEW_TOPO,
            privGranted ? "No differences in topologies" : ACCESS_DENIED);

        assertCLICommandResult("topology rebalance", user,
            String.format("topology rebalance -name %s -pool %s",
            NEW_TOPO, NEW_POOL), privGranted ? "Rebalanced" : ACCESS_DENIED);

        assertCLICommandResult("topology redistribute", user,
             String.format("topology redistribute -name %s -pool %s",
             NEW_TOPO, NEW_POOL), privGranted ? "Redistributed" : ACCESS_DENIED);

        assertCLICommandResult("topology validate", user,
            "topology validate",
            privGranted ? "Validation" : ACCESS_DENIED);

        /* Succeed without any output */
        assertCLICommandResult("pool remove", user,
            "pool remove -name " + NEW_POOL,
            privGranted ? "" : ACCESS_DENIED);
    }

    /**
     * Attempt to run commands need SYSDBA privilege.
     */
    private void runSysdbaCommands(final String user,
                                   final boolean privGranted)
        throws Exception {

        assertCLICommandResult("plan add-index", user,
            "plan add-index -name idx0 -wait -table " +
            USER_TABLE_NAME + " -field name",
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("plan remove-index", user,
            "plan remove-index -name idx0 -wait -table " +
                       USER_TABLE_NAME,
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);

        assertCLICommandResult("plan remove-table", user,
            "plan remove-table -wait -name " + USER_TABLE_NAME,
            privGranted ? PLAN_SUCCEED : ACCESS_DENIED);
    }

    /**
     * Attempt to run commands need SYSVIEW privilege.
     */
    private void runSysviewCommand(final String user,
                                   final boolean privGranted)
        throws Exception {

        assertCLICommandResult("ping", user,
            "ping", privGranted ? "Pinging components" :
                        ACCESS_DENIED);

        assertCLICommandResult("List topology", user,
            "topology list", privGranted ? TOPO :
                        ACCESS_DENIED);

        assertCLICommandResult("topology view", user,
            "topology view -name " + TOPO,
            privGranted ? "numPartitions" : ACCESS_DENIED);

        assertCLICommandResult("show admins", user,
            "show admins", privGranted ? "admin" :
                        ACCESS_DENIED);

        assertCLICommandResult("show topology", user,
            "show topology", privGranted ? "numPartitions" :
                        ACCESS_DENIED);

        assertCLICommandResult("show upgrade-order", user,
            "show upgrade-order",
            privGranted ? "Calculating upgrade order" : ACCESS_DENIED);

        assertCLICommandResult("show zones", user,
            "show zones", privGranted ? "zn:" : ACCESS_DENIED);

        assertCLICommandResult("show security parameters", user,
            "show parameters -security",
            privGranted ? "sessionTimeout" : ACCESS_DENIED);

        assertCLICommandResult("show policy", user,
            "show parameters -policy",
            privGranted ? "rnCachePercent" : ACCESS_DENIED);

        assertCLICommandResult("show rn parameters", user,
            "show parameters -service rg1-rn1",
            privGranted ? "repNodeId" : ACCESS_DENIED);

        assertCLICommandResult("show sn parameters", user,
            "show parameters -service sn1",
            privGranted ? "storageNodeId" : ACCESS_DENIED);

        assertCLICommandResult("show admin parameters", user,
            "show parameters -service sn1",
            privGranted ? "storageNodeId" : ACCESS_DENIED);

        assertCLICommandResult("show pools", user,
            "show pools", privGranted ? "AllStorageNodes" :
                        ACCESS_DENIED);

        assertCLICommandResult("show perf", user,
            "show perf", privGranted ? "Cumulative" : ACCESS_DENIED);

        assertCLICommandResult(
            "show tls-credentials", user, "show tls-credentials",
            privGranted ? "Installed credentials status" : ACCESS_DENIED);

        /* show plans covered in plan ownership test */
    }

    /*
     * Test table add and evolve commands permission check.
     */
    @Test
    public void testTableCLICommandPermissionCheck()
        throws Exception {

        deployStoreWithoutUsers();
        createUsers();
        final CommandServiceAPI csForUser = getAdmin(USER);
        final CommandServiceAPI csForUser2 = getAdmin(USER2);

        /*
         * Login with common user only with public role to build a table
         * and add, which is expected fail.
         */
        testDeniedOperation(new Operation() {

            @Override
            public void execute()
                throws Exception {

                addTable(USER_TABLE, false, csForUser);
            }
        });

        /* Grant dbadmin to user and re-create table, which should pass. */
        grantRoles(ADMIN_USER, USER, "dbadmin");
        testValidOperation(new Operation() {

            @Override
            public void execute()
                throws Exception {

                addTable(USER_TABLE, true, csForUser);
            }
        });

        /*
         * Login with common user2 only with public role to create table,
         * which should fail.
         */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                addTable(EMPLOYEE_TABLE, false, csForUser2);
            }
        });

        /* Grant sysadmin to user2 and re-create table, which should pass. */
        grantRoles(ADMIN_USER, USER2, "sysadmin");
        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                addTable(EMPLOYEE_TABLE, true, csForUser2);
            }
        });

        /* Revoke dbadmin from user */
        revokeRoles(ADMIN_USER, USER, "dbadmin");

        /* Revoke sysadmin from user2 */
        revokeRoles(ADMIN_USER, USER2, "sysadmin");

        KVStore store = getStore(USER);
        TableAPI table = store.getTableAPI();
        final TableEvolver evolver1 = TableEvolver.createTableEvolver(
            table.getTable(USER_TABLE_NAME));
        evolver1.addInteger("an_integer");
        evolver1.evolveTable();
        final TableEvolver evolver2 = TableEvolver.createTableEvolver(
            table.getTable(EMPLOYEE_TABLE_NAME));
        evolver2.removeField("salary");
        evolver2.evolveTable();
        /*
         * Login with common user only with public role to evolve table,
         * which should fail.
         */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                evolveTable(evolver1, false, csForUser2);
            }
        });

        /*
         * Login with common user2 only with public role to evolve table,
         * which should fail.
         */
        testDeniedOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                evolveTable(evolver2, false, csForUser);
            }
        });

        /*
         * Grant dbadmin to user, sysadmin to user2 and execute evolve table
         * plan, which should pass permission check and succeed.
         */
        grantRoles(ADMIN_USER, USER, "dbadmin");
        grantRoles(ADMIN_USER, USER2, "sysadmin");
        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                evolveTable(evolver1, true, csForUser2);
            }
        });
        testValidOperation(new Operation() {

            @Override
            public void execute() throws Exception {
                evolveTable(evolver2, true, csForUser2);
            }
        });

        store.close();
    }

    /*
     * Test the anonymous login user can run DDL command.
     */
    @Test
    public void testAnonymousLoginDDLCommands()
        throws Exception {
        deployStoreWithoutUsers();
        /*
         * Typically the anonymous login is used to create the first user, after
         * which anonymous logins are no longer permitted. We don't recommend
         * user to use anonymous login to do table DDL, but the current DDL
         * implementation is allowing this, we cover some table cases here.
         */
        assertStatementDDLCommandResult("testCreateTable", null,
            "CREATE TABLE T1 (ID1 INTEGER, ID2 INTEGER, " +
                       "PRIMARY KEY(ID1))",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testAlterTable", null,
            "ALTER TABLE T1 (ADD ID3 INTEGER)",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testShowTable", null,
            "SHOW TABLE T1", "T1");
        assertStatementDDLCommandResult("testDescTable", null,
            "DESC AS JSON TABLE T1", "ID3");
        assertStatementDDLCommandResult("testDropTable", null,
            "DROP TABLE T1",
             "Statement completed successfully");
        assertStatementDDLCommandResult("testCreateTable2", null,
            "CREATE TABLE T1 (ID1 INTEGER, " +
                    "ID2 INTEGER, PRIMARY KEY(ID1))",
            "Statement completed successfully");
        /* Security related DDL */
        assertStatementDDLCommandResult("testDDLCreateRole",
                null,
            "CREATE ROLE role_test",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testDDLGrantPrivToRole",
                null,
            "GRANT readwrite TO ROLE role_test",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testDDLDropRole", null,
            "DROP ROLE role_test",
             "Statement completed successfully");
        /* Command fail cases */
        assertStatementDDLCommandResult("testDDLCreateUserFail",
                null,
            "CREATE USER TEST IDENTIFIED BY \""+ ADMIN_PASS +"\"",
            "The first user in the store must be " +
                    "-admin and enabled");
        assertStatementDDLCommandResult("testConvertDMLToDDLFail",
                null,
            "SELECT * FROM T1", "Problem parsing");
        assertStatementDDLCommandResult("testIllegalStatement",
                null,
            "CREATE TABLE FAIL_TABLE (-ABCXYZ)",
            "Error handling command");
        /* Create the first user, end using anonymous */
        assertStatementDDLCommandResult("testDDLCreateUser", null,
            "CREATE USER " + ADMIN_USER + " IDENTIFIED BY \"" +
            ADMIN_PASS +"\" ADMIN",
                "Statement completed successfully");
        /* Use the new user to run DDL, need to connect to store */
        assertStoreStatementDDLCommandResult(
                "testAdminUserCreateTable",
            ADMIN_USER,
            "CREATE TABLE T2 (ID1 INTEGER, ID2 INTEGER, " +
                    "PRIMARY KEY(ID1))",
            "Statement completed successfully");
        assertStoreStatementDDLCommandResult("testAdminUserShowTable",
            ADMIN_USER, "SHOW TABLE T2", "T2");
        assertStoreStatementDDLCommandResult("testAdminUserCreateRole",
            ADMIN_USER,
            "CREATE ROLE role_test2",
            "Statement completed successfully");
        assertStoreStatementDDLCommandResult("testAdminUserGrantRole",
            ADMIN_USER, "GRANT role_test2 TO USER " + ADMIN_USER,
            "Statement completed successfully");
    }

    /*
     * Test the anonymous login user can run DDL command against bootstrap admin.
     */
    @Test
    public void testBootstrapAdminDDLCommands()
        throws Exception {

        runCLICommand("configure", "-name", kvstoreName);

        /* Security related DDL */
        assertStatementDDLCommandResult("testDDLCreateRole", null,
            "CREATE ROLE role_test",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testDDLGrantPrivToRole",
                null,
            "GRANT readwrite TO ROLE role_test",
            "Statement completed successfully");
        assertStatementDDLCommandResult("testDDLDropRole", null,
            "DROP ROLE role_test",
             "Statement completed successfully");

        /* Command fail cases */
        assertStatementDDLCommandResult("testDDLCreateUserFail",
                null,
            "CREATE USER TEST IDENTIFIED BY \""+ ADMIN_PASS +"\"",
            "The first user in the store must be -admin " +
                    "and enabled");
        assertStatementDDLCommandResult("testIllegalStatement",
                null,
            "CREATE TABLE FAIL_TABLE (-ABCXYZ)",
            "Error handling command");

        /* Create the first user, end using anonymous */
        assertStatementDDLCommandResult("testDDLCreateUser", null,
            "CREATE USER " + ADMIN_USER + " IDENTIFIED BY \"" +
            ADMIN_PASS +"\" ADMIN",
           "Statement completed successfully");

        /* verify topology can be still deployed with the first user */
        testUser = ADMIN_USER;
        runCLICommand("plan", "deploy-zone", "-name",
                      "testzone", "-rf", "1",  "-wait");
        runCLICommand("plan", "deploy-sn", "-zn", "1", "-host",
                      sna1.getHostname(), "-port",
                      String.valueOf(sna1.getRegistryPort()),
                      "-wait");
        runCLICommand("plan", "deploy-admin", "-sn", "1", "-wait");
        runCLICommand("pool", "create", "-name", POOL);
        runCLICommand("pool", "join", "-name", POOL, "-sn", "1");
        runCLICommand("topology",
                      "create",
                      "-name",
                      TOPO,
                      "-pool",
                      POOL,
                      "-partitions",
                      "10"  /* n partitions */);

        assertCLICommandResult("Deploy topo", ADMIN_USER,
            String.format("plan deploy-topology -name %s -wait", TOPO),
            "ended successfully");

        /* Use the new user to run DDL, need to connect to store */
        assertStoreStatementDDLCommandResult(
            "testAdminUserCreateTable", ADMIN_USER,
            "CREATE TABLE T2 (ID1 INTEGER, ID2 INTEGER, " +
                      "PRIMARY KEY(ID1))",
            "Statement completed successfully");
        assertStoreStatementDDLCommandResult("testAdminUserShowTable",
            ADMIN_USER, "SHOW TABLE T2", "T2");
        assertStoreStatementDDLCommandResult("testAdminUserCreateRole",
            ADMIN_USER,
            "CREATE ROLE role_test2",
            "Statement completed successfully");
        assertStoreStatementDDLCommandResult("testAdminUserGrantRole",
            ADMIN_USER, "GRANT role_test2 TO USER " + ADMIN_USER,
            "Statement completed successfully");
    }

    /*
     * Test KV data CLI commands permission check.
     */
    @Test
    public void testKvDataCLICommandPermissionCheck()
        throws Exception {

        final String PUT1 = "put kv -key /user/01 -value Bob";
        final String GET1 = "get kv -key /user/01";
        final String PUT2 = "put kv -key /user/02 -value Tom";
        final String DEL2 = "delete kv -key /user/02 -all";
        final String[] SINGLE_KEY_OPS = new String[] { PUT1, GET1, PUT2 };

        /* Command output message */
        final String PUT_SUCCEED = "Operation successful";
        final String DEL_SUCCEED = "Key deleted";
        final String MULTI_KEY_OP_FAILD = "0 Key";
        final String MULTI_OP_FAILD = "0 Record";

        /* Total number of records in the store */
        int totalRecords = 0;

        deployStoreWithoutUsers();
        createUsers();

        /* Grant admin user readwrite role */
        grantRoles(ADMIN_USER, ADMIN_USER, "readwrite");
        assertDataCLICommandResult(
            "Put user01 succeed", ADMIN_USER, PUT1, PUT_SUCCEED);
        totalRecords++;

        /* All read and write commmand cannot pass permission check */
        for (String command : SINGLE_KEY_OPS) {
            assertDataCLICommandResult(
                command + " Operation fail", USER, command,
                          ACCESS_DENIED);
        }

        assertDataCLICommandResult(
            DEL2 + " Operation fail", USER, DEL2, MULTI_KEY_OP_FAILD);

        /* Grant readonly to user */
        grantRoles(ADMIN_USER, USER, "readonly");

        /* Only read commands can pass permission check */
        assertDataCLICommandResult("Get user01 succeed", USER, GET1,
                "Bob");
        assertDataCLICommandResult(
            "Put user02 fail", USER, PUT2, ACCESS_DENIED);
        assertDataCLICommandResult(
            "Delete user02 fail", USER, DEL2, MULTI_KEY_OP_FAILD);

        /* Grant writeonly to user */
        grantRoles(ADMIN_USER, USER, "writeonly");

        /* All read and write command should pass permission check */
        assertDataCLICommandResult("Get user01 succeed", USER, GET1,
                "Bob");
        assertDataCLICommandResult(
            "Put user02 succeed", USER, PUT2, PUT_SUCCEED);
        assertDataCLICommandResult(
            "Delete user02 succeed", USER, DEL2, DEL_SUCCEED);

        for (int i = 2; i < 10; i++) {
            assertDataCLICommandResult("Put record " + i, USER,
                String.format("put kv -key %s -value %s",
                "/user/0" + i, "bob" + i), PUT_SUCCEED);
                 totalRecords++;
        }

        assertDataCLICommandResult("Get all user data", USER,
            "get kv -key /user -all", totalRecords +
                        " Records returned");

        /* Revoke readonly and writeonly from user */
        revokeRoles(ADMIN_USER, USER, "writeonly", "readonly");

        /* All read and write command cannot pass permission check */
        assertDataCLICommandResult(
            "Get user01 fail", USER, GET1, ACCESS_DENIED);
        assertDataCLICommandResult(
            "Get all data fail", USER, "get kv -key " +
                        "/user -all", MULTI_OP_FAILD);
        assertDataCLICommandResult(
            "Put user02 fail", USER, PUT2, ACCESS_DENIED);
        assertDataCLICommandResult(
            "Delete user02 fail", USER, DEL2, MULTI_KEY_OP_FAILD);
        assertDataCLICommandResult("Multi delete fail", USER,
            String.format("delete kv -key /user -all"), MULTI_KEY_OP_FAILD);

        /* Grant readwrite to user */
        grantRoles(ADMIN_USER, USER, "readwrite");

        /* All read and write command should pass permission check */
        assertDataCLICommandResult("Get user01 succeed", USER, GET1,
                "Bob");
        assertDataCLICommandResult(
            "Delete user01 succeed", USER,
            "delete kv -key /user/01", DEL_SUCCEED);
        totalRecords -= 1;
        assertDataCLICommandResult("Multi delete succeed", USER,
            String.format("delete kv -key /user -all"),
            totalRecords + " Keys deleted");

        assertDataCLICommandResult("Put succeed", USER, PUT1,
                                   PUT_SUCCEED);
        assertDataCLICommandResult("Get succeed", USER, GET1,
                "Bob");
    }

    /*
     * Test table data CLI command permission check.
     */
    @Test
    public void testTableDataCLICommandPermissionCheck()
        throws Exception {

        final String GET = "get table -name " + USER_TABLE_NAME;
        final String DELETE =
            "delete table -name " + USER_TABLE_NAME + " -field id -value 1";
        final String AGGREGATE =
            "aggregate table -name " + USER_TABLE_NAME + " -count";
        final String GET_SUCCEED = " rows returned";
        final String DEL_SUCCEED = " rows deleted";
        final String DEL_ONE_ROW = "1 row deleted";

        int totalRows = 0;

        deployStoreWithoutUsers();
        createUsers();

        final CommandServiceAPI adminCs = getAdmin(ADMIN_USER);

        /* Add table for test */
        addTable(USER_TABLE, true, /* should succeed */ adminCs);

        /* Add index for user table */
        assertCLICommandResult(
            "Add index", ADMIN_USER,
            "plan add-index -name idx0 -table " + USER_TABLE_NAME +
            " -field name -wait",
            "ended successfully");

        /* Shell connect store */
        final CommandShell shell = connectStore(USER);

        /* Build table row record */
        final Map<String, String> value = new HashMap<String, String>();
        value.put("id", "1");
        value.put("name", "bob");

        /* Put table data */
        testTablePutOperation(shell, value, false /* write not granted */);
        assertDataCLICommandResult("Get fail", USER, GET,
                ACCESS_DENIED);
        assertDataCLICommandResult("Delete fail", USER, DELETE,
                ACCESS_DENIED);

        /* Grant readonly to user */
        grantRoles(ADMIN_USER, USER, "readonly");

        testTablePutOperation(shell, value, false /* write not granted */);
        assertDataCLICommandResult("Get succeed", USER, GET,
                "0 row returned");
        assertDataCLICommandResult(
            "Aggregate succeed", USER, AGGREGATE,
                "Row count: 0");
        assertDataCLICommandResult("Delete fail", USER, DELETE,
                ACCESS_DENIED);

        /* Grant writeonly to user */
        grantRoles(ADMIN_USER, USER, "writeonly");
        testTablePutOperation(shell, value, true /* write granted */);
        totalRows++;

        /* Add 9 rows data */
        for (int i = 2; i < 10; i++) {
            value.clear();
            value.put("id", "" + i);
            value.put("name", "bob" + i);
            testTablePutOperation(shell, value, true /* write granted */);
            totalRows++;
        }
        assertDataCLICommandResult("Get succeed", USER, GET,
                GET_SUCCEED);
        assertDataCLICommandResult(
            "Aggregate succeed", USER, AGGREGATE,
                "Row count: " + totalRows);
        assertDataCLICommandResult("Multi get succeed", USER,
            String.format("get table -name %s -field %s -start %d -end %s",
            USER_TABLE_NAME, "id", 2, 4), 3 + GET_SUCCEED);
        assertDataCLICommandResult(
            "Delete succeed", USER, DELETE, DEL_ONE_ROW);
        assertDataCLICommandResult("Multi delete succeed", USER,
            String.format("delete table -name %s -field %s -start %d -end %s",
            USER_TABLE_NAME, "id", 1, 5), 4 + DEL_SUCCEED);
        totalRows -= 5;

        value.clear();
        value.put("id", "1");
        value.put("name", "bob");

        /* Revoke readonly and writeonly from user */
        revokeRoles(ADMIN_USER, USER, "readonly", "writeonly");
        testTablePutOperation(shell, value, false /* write not granted */);
        assertDataCLICommandResult("Get fail", USER, GET,
                ACCESS_DENIED);
        assertDataCLICommandResult("Delete fail", USER, DELETE,
                ACCESS_DENIED);

        /* Grant readwrite to user */
        grantRoles(ADMIN_USER, USER, "readwrite");
        testTablePutOperation(shell, value, true /* write granted */);
        totalRows++;
        assertDataCLICommandResult("Get succeed", USER, GET,
                GET_SUCCEED);
        assertDataCLICommandResult(
            "Aggregate succeed", USER, AGGREGATE,
                "Row count: " + totalRows);
        assertDataCLICommandResult("Delete succeed", USER, DELETE,
                DEL_ONE_ROW);
    }

    /*
     * Utility method to run table put command for testTableDataCLICommand test.
     * Only can used to add new row in user table.
     */
    private void testTablePutOperation(final CommandShell shell,
                                       final Map<String, String> addValues,
                                       final boolean writeGranted)
        throws Exception {

        shell.execute("put table -name " + USER_TABLE_NAME);
        String message;
        for (Entry<String, String> value : addValues.entrySet()) {
            message = execute(shell.getCurrentCommand(),
                getAddValueArgs(value.getKey(), value.getValue()), shell);
            assertNullString(message);
        }
        message = execute(shell.getCurrentCommand(),
                          new String[] { "table", "exit" }, shell);
        final String contains =
            writeGranted ? "Operation successful" : ACCESS_DENIED;
        assertThat("Table Put Data", message, containsString(contains));
    }

    /*
     * Utility method to generate add-field command.
     */
    private String[] getAddValueArgs(final String field, final String value) {
        return new String[] {
            "table", "add-value", "-field", field, "-value", value };
    }

    /**
     * Test using the SNA disable-services command on an unregistered, secure
     * store.
     */
    @Test
    public void testSNADisableServicesUnregistered()
        throws Exception {

        /* Disable services after shutting down unregistered SNA */
        sna1.shutdown(true, true);
        StorageNodeBasic.doMain(logger,
                                "-root", sna1.getBootstrapDir(),
                                "-config", sna1.getBootstrapFile(),
                                "-shutdown",
                                "-disable-services");

        /* Disable services after removing security configuration file */
        File securityConfigFile =
            new File(sna1.getSecurityDir(), sna1.getSecurityConfigFile());
        assertTrue(securityConfigFile.delete());
        StorageNodeBasic.doMain(logger,
                                "-root", sna1.getBootstrapDir(),
                                "-config", sna1.getBootstrapFile(),
                                "-shutdown",
                                "-disable-services");
    }

    /**
     * Test using the SNA disable-services command on a registered, secure
     * store.
     */
    @Test
    public void testSNADisableServicesRegistered()
        throws Exception {

        deployStoreWithoutUsers();
        sna1.shutdown(true, true);
        StorageNodeBasic.doMain(logger,
                                "-root", sna1.getBootstrapDir(),
                                "-config", sna1.getBootstrapFile(),
                                "-shutdown",
                                "-disable-services");
        StorageNodeTestBase.startSNA(
            sna1.getBootstrapDir(), sna1.getBootstrapFile(), false,
                false);

        /* Confirm that the services do not start */
        assertEquals("Number of managed processes",
                     0,
                     ManagedService.findManagedProcesses(
                         kvstoreName, "-root " +
                                     sna1.getBootstrapDir(),
                         logger).size());
    }

    @Test
    public void testUserPasswordComplexityPolicies()
        throws Exception {

        deployStoreWithoutUsers();
        createUsers();

        testUser = ADMIN_USER;

        setMinimumCheckerPolicy();

        assertStoreStatementDDLCommandResult(
            "testCreateUser",
            ADMIN_USER,
            "CREATE USER test IDENTIFIED BY \"123456\"",
            "Statement completed successfully");

        assertStoreStatementDDLCommandResult(
            "testAlterUser",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"654321\"",
            "Statement completed successfully");

        runCLICommand("change-policy", "-params", "passwordMinLength=10");

        assertStoreStatementDDLCommandResult(
            "testMinLenViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"123456789\"",
            "Password must have at least 10 characters");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordMaxLength=8");

        assertStoreStatementDDLCommandResult(
            "testMaxLenViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"123456789\"",
            "Password must have at most 8 characters");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordMinUpper=5");

        assertStoreStatementDDLCommandResult(
            "testMinUpperViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"UUUUuuuu\"",
            "Password must have at least 5 upper case letters");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordMinLower=5");

        assertStoreStatementDDLCommandResult(
            "testMinLowerViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"llllLLLL\"",
            "Password must have at least 5 lower case letters");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordMinDigit=5");

        assertStoreStatementDDLCommandResult(
            "testMinDigitViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"1234UULL\"",
            "Password must have at least 5 digit numbers");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordMinSpecial=5",
            "passwordAllowedSpecial=!^_*@-#~");

        assertStoreStatementDDLCommandResult(
            "testMinSpecialViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"123UL_*!_\"",
            "Password must have at least 5 special characters");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params", "passwordRemember=2");

        assertStoreStatementDDLCommandResult(
            "testMinRememberViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"Prev1\"",
            "Statement completed successfully");

        assertStoreStatementDDLCommandResult(
            "testMinRememberViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"Prev2\"",
            "Statement completed successfully");

        assertStoreStatementDDLCommandResult(
            "testMinRememberViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"Prev1\"",
            "Password must not be one of the previous " +
                    "2 remembered passwords");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params",
                "passwordNotUserName=true");

        assertStoreStatementDDLCommandResult(
            "testNotUserNameViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"test\"",
            "Password must not be the same as the user name, " +
                    "the user " +
            "name reversed, or the user name with the numbers " +
            "1-100 appended.");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params",
                "passwordNotStoreName=true");

        assertStoreStatementDDLCommandResult(
            "testNotStoreNameViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \""+ kvstoreName +"\"",
            "Password must not be the same as the store name, " +
                    "the store " +
            "name reversed, or the store name with the numbers " +
            "1-100 appended.");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params",
                      "passwordProhibited=welcome1,nosql");

        assertStoreStatementDDLCommandResult(
            "testProhibitedViolation",
            ADMIN_USER,
            "ALTER USER test IDENTIFIED BY \"welcome1\"",
            "Password must not be the word in the " +
                    "prohibited list");

        setMinimumCheckerPolicy();

        runCLICommand("change-policy", "-params",
                      "passwordProhibited=welcome1,nosql",
                      "passwordMinDigit=5");

        assertStoreStatementDDLCommandResult(
                "testMultipleViolation",
                ADMIN_USER,
                "ALTER USER test IDENTIFIED BY \"welcome1\"",
                "Password must have at least " +
                        "5 digit numbers\n  " +
                "Password must not be the word in the prohibited list");
    }

    private void setMinimumCheckerPolicy() throws Exception {
        /*
         * Set the minimum requirements.
         */
        runCLICommand("change-policy", "-params",
                      "passwordComplexityCheck=true",
                      "passwordMinLength=1",
                      "passwordMaxLength=100",
                      "passwordMinUpper=0",
                      "passwordMinLower=0",
                      "passwordMinDigit=0",
                      "passwordMinSpecial=0",
                      "passwordRemember=0",
                      "passwordNotUserName=false",
                      "passwordNotStoreName=false");
    }

    /*
     * Assert whether given message is null.
     */
    private void assertNullString(final String message) {
        assertTrue("The return message is expected to be null: " +
                    ", but get :" + message, message == null);
    }

    private void addTable(final TableBuilder builder,
                          final boolean shouldSucceed,
                          final CommandServiceAPI cs)
        throws Exception {

        builder.validate();
        int planId = cs.createAddTablePlan
            ("AddTable",
             builder.getNamespace(),
             builder.getName(),
             (builder.getParent() != null ?
             builder.getParent().getFullName() : null),
             builder.getFieldMap(),
             builder.getPrimaryKey(),
             builder.getPrimaryKeySizes(),
             builder.getShardKey(),
             null, // ttl
             null, // limits
             builder.isR2compatible(),
             0,
             builder.getJsonCollection(),
             builder.getMRCounters(),
             null  /* description */);
        execPlan(cs, shouldSucceed, planId, "AddTable");
    }

    private void evolveTable(final TableEvolver evolver,
                             final boolean shouldSucceed,
                             final CommandServiceAPI cs)
        throws Exception {

        TableImpl table = evolver.getTable();
        int planId = cs.createEvolveTablePlan("EvolveTable",
                                              table.getInternalNamespace(),
                                              table.getFullName(),
                                              evolver.getTableVersion(),
                                              table.getFieldMap(),
                                              table.getDefaultTTL(),
                                              table.getRemoteRegions());
        execPlan(cs, shouldSucceed, planId, "EvolveTable");
    }

    /**
     * Use given shell to execute command and return the execution result.
     *
     * If execution of shell command throw shell exception, simply output the
     * message of exception.
     */
    String execute(final ShellCommand command,
                   final String[] args,
                   final Shell shell) {
        try {
            return command.execute(args, shell);
        } catch (ShellException se) {
            return se.getMessage();
        }
    }

    /**
     * Builds a role plan command.
     *
     * @param op operation of "revoke" or "grant"
     * @param user operated user
     * @param roles roles to operate
     * @return a role plan command
     */
    private String roleCommand(final String op,
                               final String user,
                               final String... roles) {
        StringBuilder sb = new StringBuilder();
        sb.append("plan " + op);
        sb.append(" -user " + user);

        for (String role : roles) {
            sb.append(" -role " + role);
        }
        sb.append(" -wait");

        return sb.toString();
    }

    private void grantRoles(final String loginUser,
                            final String user,
                            final String... roles)
        throws Exception {

        assertCLICommandResult(
           "Grant role to " + user, loginUser,
           roleCommand("grant", user, roles),
           "ended successfully");
    }

    private void revokeRoles(final String loginUser,
                             final String user,
                             final String... roles)
        throws Exception {

        assertCLICommandResult(
            "Revoke role to " + user, loginUser,
            roleCommand("revoke", user, roles),
            "ended successfully");
    }

    /**
     * Call command service to execute plan command.
     *
     * @param cs Command service
     * @param shouldSucceed if true then the plan should succeed.
     * @param planId id of plan
     * @param planName name of plan
     */
    private void execPlan(final CommandServiceAPI cs,
                          final boolean shouldSucceed,
                          final int planId,
                          final String planName) {
        try {
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            Plan.State state = cs.awaitPlan(planId, 0, null);
            if (shouldSucceed) {
                assertTrue(state == Plan.State.SUCCEEDED);
            } else {
                assertTrue(state == Plan.State.ERROR);
            }
        } catch (Exception e) {
            if (shouldSucceed) {
                fail("Execute " + planName + " fail");
            }
        }
    }

    /**
     * Runs CLI command and asserts the match of expected and actual results.
     * Users need to be specified to run the command.
     *
     * @param testName1 name of the test
     * @param user name of users to login to admin and run command
     * @param command command
     * @param expectedSubstring expected substring
     */
    private void assertCLICommandResult(final String testName1,
                                        final String user,
                                        final String command,
                                        final String expectedSubstring)
        throws Exception {

        testUser = user;
        assertCLICommandResult(testName1, command, expectedSubstring);
    }

    /**
     * Run data CLI command and asserts the match of expected and actual output
     * results. User need to be specificed to run the command.
     *
     * @param testName1
     *            name of the test
     * @param user
     *            name of user who login in admin, store and run command
     * @param command
     *            command
     * @param expectedSubstring
     *            expected substring
     */
    void assertDataCLICommandResult(final String testName1,
                                    final String user,
                                    final String command,
                                    final String expectedSubstring)
        throws Exception {

        testUser = user;
        final String result = runDataCLICommand(command.split(" "));
        assertThat(testName1, result, containsString(expectedSubstring));
        logger.info(testName1 + ":\n" + result);
    }

    void assertStatementDDLCommandResult(final String testName1,
                                        final String user,
                                        final String statement,
                                        final String expectedSubstring)
        throws Exception {
        testUser = user;
        final String result = runCLICommand("execute", statement);
        assertThat(testName1, result, containsString(expectedSubstring));
        logger.info(testName1 + ":\n" + result);
    }

    void assertStoreStatementDDLCommandResult(final String testName1,
                                              final String user,
                                              final String statement,
                                              final String expectedSubstring)
        throws Exception {
        testUser = user;
        final String result = runDataCLICommand("execute", statement);
        assertThat(testName1, result, containsString(expectedSubstring));
        logger.info(testName1 + ":\n" + result);
    }

    /*
     * Initializes a secured admin without creating any user.
     */
    private void initSecuredAdmin()
        throws Exception {

        runCLICommand(new String[] { "configure", "-name", kvstoreName });
        runCLICommand(new String[] { "plan", "deploy-zone", "-name",
                                     "testzone", "-rf", "1",  "-wait" });
        runCLICommand(new String[] { "plan", "deploy-sn", "-zn", "1", "-host",
                                     sna1.getHostname(), "-port",
                                     String.valueOf(sna1.getRegistryPort()),
                                     "-wait" });
        runCLICommand(new String[] { "plan", "deploy-admin", "-sn", "1",
                                     "-wait" });
    }

    /*
     * Deploys a secured store without creating any user.
     */
    private void deployStoreWithoutUsers()
        throws Exception {

        initSecuredAdmin();

        runCLICommand(new String[] { "pool", "create", "-name", POOL });
        runCLICommand(new String[] { "pool", "join", "-name", POOL,
                                     "-sn", "1" });
        runCLICommand(new String[] { "topology",
                                     "create",
                                     "-name",
                                     TOPO,
                                     "-pool",
                                     POOL,
                                     "-partitions",
                                     "10"  /* n partitions */ });

        assertCLICommandResult("Deploy topo", ANONYMOUS,
            String.format("plan deploy-topology -name %s -wait", TOPO),
            "ended successfully");
    }

    /*
     * Initializes a secured admin and create users for testing user
     * manipulating command.
     */
    private void initAdminForUserCmds()
        throws Exception {

        initSecuredAdmin();
        createUsers();
    }

    private void createUsers()
        throws Exception {

        /* Create some users */
        assertCLICommandResult(
            "Add admin user", ANONYMOUS,
            String.format("plan create-user -name %s -admin -password %s -wait",
                          ADMIN_USER, ADMIN_PASS),
            "ended successfully");

        assertCLICommandResult(
            "Add a common user", ADMIN_USER,
            String.format("plan create-user -name %s -password %s -wait",
                          USER, PASS),
            "ended successfully");

        assertCLICommandResult(
            "Add a common user", ADMIN_USER,
            String.format("plan create-user -name %s -password %s -wait",
                          USER2, PASS2),
            "ended successfully");
    }

    /**
     * Provides CLI prefix arguments for secured store.
     *
     * @see oracle.kv.impl.admin.AdminClientTest#cliPrefixArgs()
     */
    @Override
    String[] cliPrefixArgs() {
        final String[] args = { "-host", "localhost", "-port", adminPort,
                                "-security", SEC_FILE };
        if ((testUser == null) || testUser.equals(ANONYMOUS)) {
            return args;
        }

        final String[] userArgs = { "-username", testUser };
        final String[] fullArgs = new String[args.length + userArgs.length];
        System.arraycopy(args, 0, fullArgs, 0, args.length);
        System.arraycopy(userArgs, 0, fullArgs, args.length,
                userArgs.length);
        return fullArgs;
    }

    /**
     * Creates a security file for login to admin.
     */
    private void createSecurityFile() throws FileNotFoundException {
        final File secFile = new File(SEC_FILE);
        final FileOutputStream fos = new FileOutputStream(secFile);
        final PrintStream ps = new PrintStream(fos);
        ps.println(AUTH_PWDFILE_PROPERTY + "=" + TestUtils.getTestDir() +
                   File.separator + "pwdfile");
        ps.println(TRANSPORT_PROPERTY + "=" + "ssl");
        ps.println(SSL_TRUSTSTORE_FILE_PROPERTY + "=" + SEC_DIR +
                   File.separator + "client.trust");
        ps.close();
        assertTrue(secFile.exists());
    }

    /**
     * Creates a password store for auto login of client tests. The filestore
     * is used currently.
     */
    private void createPasswordStore()
        throws Exception {
        final File pwdFile = new File(TestUtils.getTestDir(), "pwdfile");
        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.filestore.FileStoreManager");
        final PasswordStore pwdfile = pwdMgr.getStoreHandle(pwdFile);
        pwdfile.create(null /* auto login */);
        pwdfile.setSecret(ADMIN_USER, ADMIN_PASS.toCharArray());
        pwdfile.setSecret(USER, PASS.toCharArray());
        pwdfile.setSecret(USER2, PASS2.toCharArray());
        pwdfile.save();
        assertTrue(pwdFile.exists());
    }

    /**
     * Tries to login admin using specified username and get the admin command
     * service API.
     *
     * @param loginUser name of login user
     * @return CommandServiceAPI of admin
     */
    private CommandServiceAPI getAdmin(final String loginUser)
        throws Exception {

        final KVStoreLogin adminLogin = new KVStoreLogin(loginUser, SEC_FILE);
        adminLogin.loadSecurityProperties();
        final LoginCredentials loginCreds =
            adminLogin.makeShellLoginCredentials();
        adminLogin.prepareRegistryCSF();

        final int port = Integer.valueOf(adminPort);
        final LoginManager adminLoginMgr = KVStoreLogin.getAdminLoginMgr(
            "localhost", port, loginCreds, logger);
        if (adminLoginMgr == null) {
            throw new IllegalStateException(
                "Could not login admin as " + loginUser);
        }
        return RegistryUtils.getAdmin("localhost", port, adminLoginMgr,
                                      logger);
    }

    /**
     * Tries to login admin with anonymous user.
     */
    private CommandServiceAPI getAnonymousAdmin()
        throws Exception {

        final KVStoreLogin adminLogin = new KVStoreLogin(null, SEC_FILE);
        adminLogin.loadSecurityProperties();
        adminLogin.prepareRegistryCSF();
        final AdminLoginManager alm =
                new AdminLoginManager(null, true, logger);
        final int port = Integer.valueOf(adminPort);
        alm.bootstrap("localhost", port, null);

        return RegistryUtils.getAdmin("localhost", port, alm, logger);
    }

    /**
     * Call CommandShell.main to run data CLI command, and return any output
     * it prints as a String.
     *
     * @param args The command line arguments
     * @return The command's output
     * @throws Exception
     */
    String runDataCLICommand(String... args)
        throws Exception {

        logger.log(INFO, "runDataCLICommand {0}", Arrays.toString(args));
        try {
            String prefixArgs[] = cliPrefixArgs();

            /* Concatenate the prefix and given arg arrays. */
            String fullArgs[] = new String[prefixArgs.length + args.length];
            System.arraycopy(prefixArgs, 0, fullArgs, 0,
                             prefixArgs.length);
            System.arraycopy(args, 0, fullArgs, prefixArgs.length,
                             args.length);

            /* Arrange to capture stdout. */
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            PrintStream cmdOut = new PrintStream(outStream);
            /* Run the command. */
            CommandShell shell = new CommandShell(System.in, cmdOut);
            shell.parseArgs(fullArgs);
            shell.openStore(sna1.getHostname(),
                            sna1.getRegistryPort(),
                            kvstoreName,
                            testUser,
                            SEC_FILE);
            shell.start();

            logger.log(INFO, "runDataCLICommand result: {0}", outStream);

            return outStream.toString();

        } catch (Exception e) {
            logger.log(INFO, "runDataCLICommand throws", e);
            throw e;
        } catch (Error e) {
            logger.log(INFO, "runDataCLICommand throws", e);
            throw e;
        }
    }

    /**
     * Tries to login admin using specified username and get the command shell.
     *
     * @param userName
     * @return command shell
     */
    private CommandShell connectStore(String userName)
        throws Exception {

        final CommandShell shell;
        try {
            /* Arrange to capture stdout. */
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            PrintStream cmdOut = new PrintStream(outStream);
            shell = new CommandShell(System.in, cmdOut);
            shell.openStore("localhost",
                            Integer.parseInt(adminPort),
                            kvstoreName,
                            userName, SEC_FILE);
        } catch (Exception e) {
            logger.log(INFO, "getCommandShell throws", e);
            throw e;
        } catch (Error e) {
            logger.log(INFO, "getCommandShell throws", e);
            throw e;
        }
        return shell;
    }

    /**
     * Tries to get store handle.
     */
    private KVStore getStore(String loginUser)
        throws Exception {

        final KVStoreLogin storeLogin = new KVStoreLogin(loginUser, SEC_FILE);
        storeLogin.loadSecurityProperties();
        final LoginCredentials loginCreds =
            storeLogin.makeShellLoginCredentials();
        storeLogin.prepareRegistryCSF();
        KVStoreConfig kvstoreConfig = new KVStoreConfig(
            kvstoreName, "localhost" + ":" +
                Integer.parseInt(adminPort));
        kvstoreConfig.setSecurityProperties(storeLogin.getSecurityProperties());
        return KVStoreFactory.getStore(kvstoreConfig, loginCreds,
                                      null);
    }

    private void testDeniedOperation(final Operation command)
        throws Exception {

        try {
            command.execute();
            fail("expected exception");
        } catch(ShellException se) {
            if (! (se.getCause() instanceof UnauthorizedException)) {
                fail("unexpected exception");
            }
        } catch (UnauthorizedException uae) /* CHECKSTYLE:OFF */{
        } /* CHECKSTYLE:ON */
    }

    private void testValidOperation(final Operation command) {
        /*
         * Do not mind the results, just check the permission, since permission
         * check precedes the execution of plan operations.
         */
        try {
            command.execute();
        } catch(ShellException se) {
            if (se.getCause() instanceof UnauthorizedException) {
                fail("unexpected exception");
            }
        } catch (UnauthorizedException uae) {
            fail("unexpected exception");
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    interface Operation {
        public void execute() throws Exception;
    }
}
