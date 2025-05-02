/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Collections.singleton;
import static oracle.kv.util.shell.Shell.eol;
import static oracle.kv.util.shell.Shell.eolt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.PlanCommand.DeployDCSub;
import oracle.kv.impl.admin.client.PlanCommand.EnableRequestsSub;
import oracle.kv.impl.admin.client.PlanCommand.PlanSubCommand;
import oracle.kv.impl.admin.client.PlanCommand.RemoveDatacenterSub;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;


import org.easymock.IExpectationSetters;
import org.junit.Test;

/**
 * Verifies the functionality and error paths of the PlanCommand class.  Note
 * that although the CLI test framework verifies many of the same aspects of
 * PlanCommand as this unit test, the tests from the CLI test framework do not
 * contribute to the unit test coverage measure that is automatically computed
 * nightly. Thus, the intent of this test class is to provide additional unit
 * test coverage for the PlanCommand class that will be automatically measured
 * nightly.
 */
public class PlanCommandTest extends TestBase {

    private final ParameterMap pMap = new ParameterMap();
    private final AdminParams adminParams = new AdminParams(pMap);

    private static final int ADMIN_ID = 99;
    private static final AuthContext NULL_AUTH = null;

    /*
     * Set valid default values for the args of each sub-command; where
     * the first entry corresponds to the sub-command to execute. Note
     * that if the key and value of a given map entry are identical, then
     * the corresponding arg is input as a single on-or-off flag, rather
     * than a name-value pair.
     */
    private static Map<String, String> changeMountPointSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> changeParamsSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> deployAdminSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> removeAdminSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> deployDCSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> deploySNSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> deployTopologySubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> executeSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> interruptCancelSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> migrateSNSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> removeSNSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> startStopServiceSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> startStopServiceWithZoneIdSubArgsMap =
            new HashMap<String, String>();
    private static Map<String, String> startStopServiceWithZoneNameSubArgsMap =
            new HashMap<String, String>();
    private static Map<String, String> planWaitSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> removeDatacenterSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> addTableSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> evolveTableSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> removeTableSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> addIndexSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> removeIndexSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> createUserSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> changeUserSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> dropUserSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> grantSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> revokeSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> networkRestoreSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> verifyDataSubArgMap =
        new HashMap<String, String>();
    private static Map<String, String> enableRequestsSubArgsMap =
        new HashMap<String, String>();
    private static Map<String, String> setTableLimitsSubArgsMap =
        new HashMap<String, String>();

    static {
        /*
         * ChangeMountPointSub: use either "-storagedir" or "-path"; and
         * either "-add" or "-remove".
         */
        changeMountPointSubArgsMap.put("-sn", "1");
        changeMountPointSubArgsMap.put(
            "-storagedir", "/var/tmp/test-storagedir");
        changeMountPointSubArgsMap.put("-path", "/var/tmp/test-path");
        changeMountPointSubArgsMap.put("-add", "-add");
        changeMountPointSubArgsMap.put("-remove", "-remove");

        /* ChangeParamsSub */
        changeParamsSubArgsMap.put("-service", "sn1");
        changeParamsSubArgsMap.put("-all-rns", "-all-rns");
        changeParamsSubArgsMap.put("-all-admins", "-all-admins");
        changeParamsSubArgsMap.put("-all-sns", "-all-sns");
        changeParamsSubArgsMap.put("-zn", "dc1");
        changeParamsSubArgsMap.put("-znname", "dc1");
        changeParamsSubArgsMap.put("-dc", "dc1");
        changeParamsSubArgsMap.put("-dcname", "dc1");
        changeParamsSubArgsMap.put("-dry-run", "-dry-run");
        changeParamsSubArgsMap.put("-params", "-params");
        changeParamsSubArgsMap.put("-security", "-security");
        changeParamsSubArgsMap.put("-global", "-global");

        /* DeployAdminSub */
        deployAdminSubArgsMap.put("-sn", "1");

        /* RemoveAdminSub */
        removeAdminSubArgsMap.put("-admin", "1");
        removeAdminSubArgsMap.put("-zn", "dc1");
        removeAdminSubArgsMap.put("-znname", "dc1");
        removeAdminSubArgsMap.put("-dc", "dc1");
        removeAdminSubArgsMap.put("-dcname", "dc1");

        /* DeployDCSub */
        deployDCSubArgsMap.put("-name", "dc1");
        deployDCSubArgsMap.put("-rf", "3");
        deployDCSubArgsMap.put("-type", "primary");

        /* DeploySNSub: use either "-zn" or "-znname" */
        deploySNSubArgsMap.put("-host", "test-host-name");
        deploySNSubArgsMap.put("-port", "13230");
        deploySNSubArgsMap.put("-zn", "dc1");
        deploySNSubArgsMap.put("-znname", "dc1");
        deploySNSubArgsMap.put("-dc", "dc1");
        deploySNSubArgsMap.put("-dcname", "dc1");

        /* DeployTopologySub */
        deployTopologySubArgsMap.put("-name", "test-topology-name");

        /* ExecuteSub */
        executeSubArgsMap.put("-id", "1");
        executeSubArgsMap.put("-last", "-last");

        /* InterruptCancelSub */
        interruptCancelSubArgsMap.put("-id", "1");
        interruptCancelSubArgsMap.put("-last", "-last");

        /* MigrateSNSub */
        migrateSNSubArgsMap.put("-from", "1");
        migrateSNSubArgsMap.put("-to", "3");

        /* RemoveSNSub */
        removeSNSubArgsMap.put("-sn", "1");

        /* StartStopServiceSub: use either "-service" or "-all-rns" */
        startStopServiceSubArgsMap.put("-service", "rg1-rn1");
        startStopServiceSubArgsMap.put("-all-rns", "-all-rns");

        /* StartStopServiceSub: use -all-rns when using -zn flag */
        startStopServiceWithZoneIdSubArgsMap.put("-all-rns", "-all-rns");
        startStopServiceWithZoneIdSubArgsMap.put("-zn", "dc1");

        /* StartStopServiceSub: use -all-rns when using -znname flag */
        startStopServiceWithZoneNameSubArgsMap.put("-all-rns", "-all-rns");
        startStopServiceWithZoneNameSubArgsMap.put("-znname", "dc1");

        /* PlanWaitSub */
        planWaitSubArgsMap.put("-id", "1");
        planWaitSubArgsMap.put("-seconds", "30");
        planWaitSubArgsMap.put("-last", "-last");

        /*
         * RemoveDatacenterSub: use either
         * PlanCommand.RemoveDatacenterSub.ID_FLAG ("-zn") or
         * PlanCommand.RemoveDatacenterSub.NAME_FLAG ("-znname")
         */
        removeDatacenterSubArgsMap.put(
            PlanCommand.RemoveDatacenterSub.ID_FLAG, "dc1");
        removeDatacenterSubArgsMap.put(
            PlanCommand.RemoveDatacenterSub.NAME_FLAG, "dc1");

        /* AddTableSub */
        addTableSubArgsMap.put(
            PlanCommand.AddTableSub.TABLE_NAME_FLAG, "table1");
        /* CreateUserSub */
        createUserSubArgsMap.put("-name", "user1");
        createUserSubArgsMap.put("-admin", "-admin");
        createUserSubArgsMap.put("-disable", "-disable");

        /* evolveTableSub */
        evolveTableSubArgsMap.put(
            PlanCommand.EvolveTableSub.TABLE_NAME_FLAG, "table1");
        /* ChangeUserSub */
        changeUserSubArgsMap.put("-name", "user1");
        changeUserSubArgsMap.put("-disable", "-disable");
        changeUserSubArgsMap.put("-enable", "-enable");
        changeUserSubArgsMap.put("-password", "-password");
        changeUserSubArgsMap.put("-retain-current-password",
                                 "-retain-current-password");
        changeUserSubArgsMap.put("-clear-retained-password",
                                 "-clear-retained-password");

        /* removeTableSub */
        removeTableSubArgsMap.put(
            PlanCommand.RemoveTableSub.TABLE_NAME_FLAG, "table1");

        /* addIndexSub */
        addIndexSubArgsMap.put(
            PlanCommand.AddIndexSub.INDEX_NAME_FLAG, "index1");
        addIndexSubArgsMap.put(
            PlanCommand.AddIndexSub.TABLE_FLAG, "table1");
        addIndexSubArgsMap.put(
            PlanCommand.AddIndexSub.FIELD_FLAG, "field1");
        addIndexSubArgsMap.put(
            PlanCommand.AddIndexSub.DESC_FLAG, "index1 of table1");

        /* removeIndexSub */
        removeIndexSubArgsMap.put(
            PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG, "index1");
        removeIndexSubArgsMap.put(
            PlanCommand.RemoveIndexSub.TABLE_FLAG, "table1");
        /* DropUserSub */
        dropUserSubArgsMap.put("-name", "user1");

        /* GrantRoleSub */
        grantSubArgsMap.put("-user", "user1");
        grantSubArgsMap.put("-role", "sysadmin");
        grantSubArgsMap.put("-role", "sysdba");

        /* RevokeRoleSub */
        revokeSubArgsMap.put("-user", "user1");
        revokeSubArgsMap.put("-role", "sysadmin");
        revokeSubArgsMap.put("-role", "sysdba");

        /* NetworkRestoreSub */
        networkRestoreSubArgsMap.put(
            PlanCommand.NetworkRestoreSub.SOURCE_ID_FLAG, "rg1-rn3");
        networkRestoreSubArgsMap.put(
            PlanCommand.NetworkRestoreSub.TARGET_ID_FLAG, "rg1-rn3");
        networkRestoreSubArgsMap.put(
            PlanCommand.NetworkRestoreSub.RETAIN_LOG_FLAG,
            PlanCommand.NetworkRestoreSub.RETAIN_LOG_FLAG);

        verifyDataSubArgMap.put("-log-read-delay", "0");
        verifyDataSubArgMap.put("-btree-batch-delay", "0");
        verifyDataSubArgMap.put("-index", "enable");
        verifyDataSubArgMap.put("-datarecord", "enable");
        verifyDataSubArgMap.put("-service", "rg1-rn1");
        verifyDataSubArgMap.put("-zn", "dc1");
        verifyDataSubArgMap.put("-znname", "dc1");
        verifyDataSubArgMap.put("-dc", "dc1");
        verifyDataSubArgMap.put("-dcname", "dc1");
        verifyDataSubArgMap.put("-all-rns", "-all-rns");
        verifyDataSubArgMap.put("-all-admins", "-all-admins");
        verifyDataSubArgMap.put("-all-services", "-all-services");


        /* EnableRequestTypeSub */
        enableRequestsSubArgsMap.put(
            PlanCommand.EnableRequestsSub.REQUEST_TYPE, "readonly");
        enableRequestsSubArgsMap.put(
            PlanCommand.EnableRequestsSub.TARGET_SHARDS_FLAG, "rg1");
        enableRequestsSubArgsMap.put(
            PlanCommand.EnableRequestsSub.STORE_FLAG,
            PlanCommand.EnableRequestsSub.STORE_FLAG);
            
        /* SetTableLimitsSub */
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG, "table1");
            setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.TABLE_NS_FLAG, "nspname");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG, "1");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.WRITE_LIMIT_FLAG, "1");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.SIZE_LIMIT_FLAG, "1");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.INDEX_LIMIT_FLAG, "1");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.CHILD_TABLE_LIMIT_FLAG, "1");
        setTableLimitsSubArgsMap.put(
            PlanCommand.SetTableLimitsSub.INDEX_KEY_SIZE_LIMIT_FLAG, "1");
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
        adminParams.setAdminId(new AdminId(ADMIN_ID));
    }

    /*
     * Convenience method shared by all the test cases that employ the
     * mocked objects.
     */
    private void doVerification(final CommandShell shell,
                                final CommandService cs,
                                final Parameters parameters) {

        if (parameters != null) {
            verify(parameters);
        }

        if (cs != null) {
            verify(cs);
        }

        if (shell != null) {
            verify(shell);
        }
    }

    private void doVerification(final CommandShell shell,
                                final CommandService cs) {

        doVerification(shell, cs, null);
    }

    @Test
    public void testPlanCommandGetCommandOverview() throws Exception {

        final PlanCommand planObj = new PlanCommand();
        final String expectedResult =
            "Encapsulates operations, or jobs that modify store state." +
            eol + "All subcommands with the exception of " +
            "interrupt and wait change" + eol + "persistent state. Plans " +
            "are asynchronous jobs so they return immediately" + eol +
            "unless -wait is used.  Plan status can be checked using " +
            "\"show plans\"." + eol + "Optional arguments for all plans " +
            "include:" +
            eolt + "-wait -- wait for the plan to complete before returning" +
            eolt + "-plan-name -- name for a plan.  These are not unique" +
            eolt + "-noexecute -- do not execute the plan.  If specified " +
            "the plan" + eolt + "              " +
            "can be run later using \"plan execute\"" +
            eolt + "-force -- used to force plan execution and plan retry";
        assertEquals(expectedResult, planObj.getCommandOverview());
    }

    /* SUB-CLASS TEST CASES */

    /* 1. Test case coverage for: PlanCommand.ChangeMountPointSub. */

    @Test
    public void testChangeMountPointSubGetCommandSyntax() throws Exception {

        final PlanCommand.ChangeStorageDirSub subObj =
            new PlanCommand.ChangeStorageDirSub();
        final String expectedResult =
            "plan change-storagedir -sn <id> " + eolt +
            "-storagedir <path to storage directory> " +
            "-add|-remove " +
            "[-storagedirsize <size of storage directory>] " +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testChangeMountPointSubGetCommandDescription()
        throws Exception {

        final PlanCommand.ChangeStorageDirSub subObj =
            new PlanCommand.ChangeStorageDirSub();
        final String expectedResult =
            "Adds or removes a storage directory on a Storage Node for use" +
            eolt +
            "by a Replication Node. When -add is specified, the optional" +
            eolt +
            "-storagedirsize flag can be specified to set the size of the" +
            eolt +
            "directory. The size format is \"number [unit]\", where unit" +
            eolt +
            "can be KB, MB, GB, or TB. The unit is case insensitive and may" +
            eolt +
            "be separated from the number by a space, \"-\", or \"_\".";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testChangeMountPointSubMatches() throws Exception {

        final TestChangeMountPointSub subObj = new TestChangeMountPointSub();

        final String goodSubCommandName = "change-mountpoint";
        /*
         * Verify that matches returns true for valid abbreviations of the
         * sub-command.
         */
        for (int i = goodSubCommandName.length();
                 i >= subObj.getPrefixMatchLength(); i--) {
            assertTrue(subObj.matches(goodSubCommandName.substring(0, i)));
        }

        /*
         * Verify that matches returns false for sub-commands with valid
         * content that has been abbreviated to an invalid length; including
         * the empty string.
         */
        for (int i = 0; i < subObj.getPrefixMatchLength(); i++) {
            assertFalse(subObj.matches(goodSubCommandName.substring(0, i)));
        }

        /*
         * Verify that matches returns false for a sub-command that has
         * length equal to the full length of the valid sub-command
         * (change-mountpoint), with valid content for the first part of
         * the sub-command within the valid abbreviation range, but some
         * form of invalid content after the valid abbreviation range.
         */
        for (int i = goodSubCommandName.length() - 1;
                 i >= subObj.getPrefixMatchLength(); i--) {

            final StringBuilder buf = new StringBuilder();
            for (int j = 0; j < goodSubCommandName.length() - i; j++) {
                buf.append("x");
            }
            assertFalse(subObj.matches(
                        goodSubCommandName.substring(0, i)  + buf.toString()));
        }

        /*
         * Verify that matches returns false for a sub-command that has
         * valid length (greater than or equal to prefixMatchLength), but
         * invalid content at each point in the sub-command.
         */
        final StringBuilder strBldr = new StringBuilder("x");
        for (int i = 1; i < goodSubCommandName.length(); i++) {
            strBldr.append(String.valueOf(i % 10));
        }
        assertFalse(subObj.matches(strBldr.toString()));
    }

    @Test
    public void testChangeMountPointSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.ChangeStorageDirSub(),
                            "change-storagedir");
    }

    @Test
    public void testChangeMountPointSubExecuteRequiredArgs() throws Exception {

        final String command = "change-storagedir";
        final Map<String, String> argsMap = changeMountPointSubArgsMap;

        /* Missing all required args */
        doExecuteRequiredArgs(new PlanCommand.ChangeStorageDirSub(),
                              command, argsMap,
                              new String[] {"-sn", "-add", "-remove"});

        /* Missing only the "-sn" required arg */
        doExecuteRequiredArgs(new PlanCommand.ChangeStorageDirSub(),
                              command, argsMap, new String[] {"-sn"});

        /*
         * Missing both the "-add" and "-remove" args.
         * Note: if "-remove" is missing, but "-sn" and "-add" are input,
         * no exception will occur. Similarly, if "-add" is missing, but
         * "-sn" and "-remove" are input, then no exception will occur.
         * If "-sn" is input, then a 'required-arg-exception' will occur
         * only if both "-add" and "-remove" are missing.
         */
        doExecuteRequiredArgs(new PlanCommand.ChangeStorageDirSub(),
                              command, argsMap,
                              new String[] {"-add", "-remove"});
    }

    /* Sub-class to gain access to protected method(s). */
    private static class TestChangeMountPointSub
        extends PlanCommand.ChangeStorageDirSub {
        public int getPrefixMatchLength() {
            return prefixMatchLength;
        }
    }

    /**
     * Test to check all storage directories size is less than the disk
     * capacity and exception is thrown when it crosses the disk capacity when
     * using "plan change-storagedir"
     */
    @Test
    public void testCheckAllStorageDirSizesExecPlanChangeStorageDir()
        throws Exception {
        /* Setting up a 4X1 store with 1 SN of capacity 4. */
        CreateStore createStore;
        createStore = new CreateStore(kvstoreName,
                5000,
                1, /* Storage Nodes */
                1, /* Replication Factor */
                100, /* Partitions */
                4  /* capacity */);

        createStore.start();
        CommandShell shell;
        ByteArrayOutputStream shellOutput;
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        shell.connectAdmin(
            "localhost", createStore.getRegistryPort(),
            createStore.getDefaultUserName(),
            createStore.getDefaultUserLoginPath());
        StorageNodeId snId = new StorageNodeId(1);
        long totalSpace;
        boolean totalSpaceExists = false;
        FileStore fileStore;
        long alteredSize = 0L;

        /* Setting up the storage directories path and its size array. */
        Map<String, String> directories = new HashMap<>();
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        String sizeString = "0";
        for (int i = 0; i < 4; i++) {
            String dirname = testdir+"Storagedir"+(i+1);
            File file  = new File(dirname);
            if (!file.exists()) {
                file.mkdir();
            }
            if (!totalSpaceExists) {
                fileStore = Files.getFileStore(Path.of(dirname));
                totalSpace = fileStore.getTotalSpace();
                if (totalSpace > 0) {
                    double totalSpaceInGB = (double) totalSpace /
                        (1024 * 1024 * 1024);
                    int sizeInGB = (int) (totalSpaceInGB / 4);
                    alteredSize = sizeInGB+5;
                    sizeString = sizeInGB + "_gb";
                    totalSpaceExists=true;
                }
            }
            directories.put(file.getAbsolutePath(), sizeString);
        }
        final long sizeChanged = alteredSize;

        createAndChangeStorageDirectorySize(snId,
                                            directories,
                                            shell,
                                            sizeChanged);

        createStore.shutdown();
        for (String filePath : directories.keySet()) {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        }
    }

    /**
     * Helper method to create and test the storage directories in the store
     */
    private void createAndChangeStorageDirectorySize(
        StorageNodeId snId,
        Map<String, String> directories,
        CommandShell shell,
        long alteredSize) throws Exception {

        /* Creating the storage directories in the store with combined size less
         * than the disk size and then increasing one storage directory size
         * so that combined size crosses the disk capacity and error is thrown.
         */
        final PlanCommand.ChangeStorageDirSub subObj =
            new PlanCommand.ChangeStorageDirSub();
        String retMsg;
        for (Map.Entry<String, String> entry : directories.entrySet()) {
            String[] args = {
                "change-storagedir",
                "-sn", snId.getFullName(),
                "-storagedir", entry.getKey(),
                "-storagedirsize", entry.getValue(),
                "-add", "-wait"
            };
            retMsg = subObj.execute(args, shell);
            assertTrue(retMsg.contains("successfully"));
        }
        String sizeAltered = alteredSize+"_gb";
        String[] argsAltered = {
            "change-storagedir",
            "-sn", snId.getFullName(),
            "-storagedir",directories.keySet().iterator().next(),
            "-storagedirsize", sizeAltered,
            "-add", "-wait"
        };
        retMsg = subObj.execute(argsAltered, shell);
        String pattern  = " ended with errors. ";
        assertTrue(retMsg.contains(pattern));

        /*
         * Creating a change-storagedir plan without execution and executing
         * another plan in the meantime, and then running the initial plan,
         * it should throw the error that the storage directory map
         * has changed since plan creation.
         */

        sizeAltered = (alteredSize-6)+"_gb";
        String[] argsAlteredWithNoExecute = {
            "change-storagedir",
            "-sn", snId.getFullName(),
            "-storagedir",directories.keySet().iterator().next(),
            "-storagedirsize", sizeAltered,
            "-add", "-wait", "-noexecute"
        };
        retMsg = subObj.execute(argsAlteredWithNoExecute, shell);
        String planId  = retMsg.substring(
            retMsg.indexOf(":")+1).trim();
        assertTrue(retMsg.contains("Created plan"));

        sizeAltered = (alteredSize-7)+"_gb";
        String[] argsAlteredInMeantime = {
            "change-storagedir",
            "-sn", snId.getFullName(),
            "-storagedir",directories.keySet().iterator().next(),
            "-storagedirsize", sizeAltered,
            "-add", "-wait"
        };
        retMsg = subObj.execute(argsAlteredInMeantime, shell);
        assertTrue(retMsg.contains("successfully"));

        final PlanCommand.ExecuteSub subObjExecute =
            new PlanCommand.ExecuteSub();
        String[] exeArg = {
            "execute",
            "-id", planId,
            "-wait"
        };
        AdminFaultException thrownException =
            assertThrows(AdminFaultException.class,
            ()-> subObjExecute.execute(exeArg, shell));
        assertTrue(thrownException.getMessage().contains(
            "has changed since plan creation."));
    }

    /* 2. Test case coverage for: PlanCommand.ChangeParamsSub. */

    @Test
    public void testChangeParamsSubGetCommandSyntax() throws Exception {

        final PlanCommand.ChangeParamsSub subObj =
            new PlanCommand.ChangeParamsSub();
        final String expectedResult = subObj.commandSyntax;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testChangeParamsSubGetCommandDescription() throws Exception {

        final PlanCommand.ChangeParamsSub subObj =
            new PlanCommand.ChangeParamsSub();
        final String expectedResult = subObj.commandDesc;
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testChangeParamsSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.ChangeParamsSub(), "change-parameters");
    }

    @Test
    public void testChangeParamsSubExecuteRequiredArgs() throws Exception {

        final String command = "change-parameters";
        final Map<String, String> argsMap = changeParamsSubArgsMap;

        /*
         * serviceName == null && (global, security, allAdmin, allRN, allSN)
         * are all FALSE, -params NOT input
         */
        doExecuteRequiredArgs(
            new PlanCommand.ChangeParamsSub(), command, argsMap,
            new String[]
            {"-security", "-service", "-all-rns", "-all-admins", "-all-sns",
             "-global", "-params", "-all-ans"});

        /*
         * serviceName == null && (global, security, allAdmin, allRN, allSN)
         * are all FALSE, -params input
         */
        doExecuteRequiredArgs(
            new PlanCommand.ChangeParamsSub(), command, argsMap,
            new String[] {"-security", "-service", "-all-rns", "-all-admins",
                          "-global", "-all-sns", "-all-ans" });

        /*
         * serviceName != null && (global, security, allAdmin, allRN, allSN)
         * are all FALSE, -params NOT input
         */
        doExecuteRequiredArgs(
            new PlanCommand.ChangeParamsSub(), command, argsMap,
            new String[] {"-security", "-all-rns", "-all-admins", "-all-sns",
                          "-zn", "-znname", "-dc", "-dcname", "-params",
                          "-global", "-all-ans"},
            "-params");

        /*
         * serviceName == null && (security, allAdmin, allRN, allSN) are NOT
         * all FALSE, -params NOT input
         */
        doExecuteRequiredArgs(
            new PlanCommand.ChangeParamsSub(), command, argsMap,
            new String[] {"-service", "-params"}, "-params");
    }

    /**
     * Verify the expected exception is thrown when the -service flag is
     * combined with one of the -all-* flags, or the -zn, -znname, -dc, or
     * -dcname flag.
     */
    @Test
    public void testChangeParamsSubExecuteInvalidFlagCombo() throws Exception {

        final PlanCommand.ChangeParamsSub subObj =
            new PlanCommand.ChangeParamsSub();

        final ShellUsageException allFlagException =
            new ShellUsageException(subObj.serviceAllError, subObj);
        final ShellUsageException dcFlagException =
            new ShellUsageException(subObj.serviceDcError, subObj);
        ShellUsageException expectedException;

        final Map<String, String> argsMap = changeParamsSubArgsMap;
        final int replaceIndx = 3;
        final String[] flagsArray =
            {"-global", "-security", "-all-rns", "-all-admins", "-all-sns",
             "-zn", "-znname", "-dc", "-dcname", "-all-ans"};
        final String[] allArgs = {"change-parameters",
                                  "-service", argsMap.get("-service"),
                                  "replace-this-flag", "-params"};
        final String[] dcArgs = {"change-parameters",
                                 "-service", argsMap.get("-service"),
                                 "replace-this-flag", "replace-this-value",
                                 "-params"};
        String[] args;

        for (final String curFlag : flagsArray) {
            if (curFlag.startsWith("-zn") || curFlag.startsWith("-dc")) {
                expectedException = dcFlagException;
                args = dcArgs;
                args[replaceIndx] = curFlag;
                args[replaceIndx + 1] = argsMap.get(curFlag);
            } else {
                expectedException = allFlagException;
                args = allArgs;
                args[replaceIndx] = curFlag;
            }

            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            replay(cs);

            expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs,
                                                                      null));
            replay(shell);

            /* Run the test and verify the results. */
            try {
                subObj.execute(args, shell);
                fail("ShellUsageException expected, but wasn't encountered");
            } catch (ShellUsageException e) {
                assertEquals(expectedException.getMessage(), e.getMessage());
                doVerification(shell, cs);
            }
        }
    }

    @Test
    public void testChangeParamsSubExecuteNoParams() throws Exception {

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);
        final PlanCommand.ChangeParamsSub subObj =
            new PlanCommand.ChangeParamsSub();

        final String expectedResult = "No parameters were specified";
        final String[] args = {"change-parameters", "-all-admins", "-params"};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("unexpected result!");
        } catch (ShellArgumentException e) {
            assertEquals(expectedResult, e.getMessage());
        }
        doVerification(shell, cs);
    }

    @Test
    public void testChangeParamsSubExecuteDcDeprecation()
        throws Exception {

        doChangeParamsSubExecute("",
                                 "plan", "change-parameters",
                                 "-all-rns", "-zn", "zn1",
                                 "-params", "requestQuiesceTime=1 s");
        doChangeParamsSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                 "plan", "change-parameters",
                                 "-all-rns", "-zn", "dc1",
                                 "-params", "requestQuiesceTime=1 s");
        doChangeParamsSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                 "plan", "change-parameters",
                                 "-all-rns", "-dc", "zn1",
                                 "-params", "requestQuiesceTime=1 s");
        doChangeParamsSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                 "plan", "change-parameters",
                                 "-all-rns", "-dc", "dc1",
                                 "-params", "requestQuiesceTime=1 s");
    }

    private void doChangeParamsSubExecute(final String deprecation,
                                          final String... cmd)
        throws Exception {

        /* Establish mocks */
        final CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.createChangeAllParamsPlan(
                   isNull(String.class), anyObject(DatacenterId.class),
                   anyObject(ParameterMap.class),
                   isNull(AuthContext.class),
                   eq(SerialVersion.CURRENT))).andStubReturn(42);
        cs.approvePlan(42, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(42, false, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        CommandShell shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(false);
        expect(shell.getHidden()).andStubReturn(true);
        shell.getVerbose();
        expectLastCall().andStubReturn(false);
        shell.verboseOutput(anyObject(String.class));
        expectLastCall().anyTimes();
        shell.requiredArg(isNull(String.class), anyObject(ShellCommand.class));
        expectLastCall().anyTimes();
        replay(shell);

        /* Execute command and check result */
        assertEquals(
            "Command result",
            deprecation +
            "Started plan 42. Use show plan -id 42 to check status." + eolt +
            "To wait for completion, use plan wait -id 42",
            new PlanCommand().execute(cmd, shell));

        shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(true);
        expect(shell.getHidden()).andStubReturn(true);
        shell.getVerbose();
        expectLastCall().andStubReturn(false);
        shell.verboseOutput(anyObject(String.class));
        expectLastCall().anyTimes();
        shell.requiredArg(isNull(String.class), anyObject(ShellCommand.class));
        expectLastCall().anyTimes();
        replay(shell);
        checkPlanJson(cmd, shell);
    }

    /* 3. Test case coverage for: PlanCommand.DeployAdminSub. */

    @Test
    public void testDeployAdminSubGetCommandSyntax() throws Exception {

        final PlanCommand.DeployAdminSub subObj =
            new PlanCommand.DeployAdminSub();
        final String expectedResult =
            PlanCommand.DeployAdminSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testDeployAdminSubGetCommandDescription() throws Exception {

        final PlanCommand.DeployAdminSub subObj =
            new PlanCommand.DeployAdminSub();
        final String expectedResult = PlanCommand.DeployAdminSub.COMMAND_DESC;
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testDeployAdminSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.DeployAdminSub(), "deploy-admin");
    }

    @Test
    public void testDeployAdminSubExecuteRequiredArgs() throws Exception {

        final String command = "deploy-admin";
        final Map<String, String> argsMap = deployAdminSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.DeployAdminSub(), command, argsMap,
            new String[]  {"-sn"});
    }

    /* 4. Test case coverage for: PlanCommand.RemoveAdminSub. */

    @Test
    public void testRemoveAdminSubGetCommandSyntax() throws Exception {

        final PlanCommand.RemoveAdminSub subObj =
            new PlanCommand.RemoveAdminSub();
        final String expectedResult = subObj.commandSyntax;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveAdminSubGetCommandDescription() throws Exception {

        final PlanCommand.RemoveAdminSub subObj =
            new PlanCommand.RemoveAdminSub();
        final String expectedResult = subObj.commandDesc;
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testRemoveAdminSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.RemoveAdminSub(), "remove-admin");
    }

    @Test
    public void testRemoveAdminSubExecuteRequiredArgs() throws Exception {

        final String command = "remove-admin";
        final Map<String, String> argsMap = removeAdminSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.RemoveAdminSub(), command, argsMap,
            new String[]  {"-admin", "-zn", "-znname", "-dc", "-dcname"});
    }

    /**
     * Verify the expected exception is thrown when the -admin flag is
     * combined with either the -zn, -znname, -dc, or -dcname flag.
     */
    @Test
    public void testRemoveAdminSubExecuteInvalidFlagCombo() throws Exception {

        final PlanCommand.RemoveAdminSub subObj =
            new PlanCommand.RemoveAdminSub();

        final ShellUsageException expectedException =
            new ShellUsageException(subObj.adminDcError, subObj);

        final Map<String, String> argsMap = removeAdminSubArgsMap;
        final int replaceIndx = 3;
        final String[] flagsArray = {"-zn", "-znname", "-dc", "-dcname"};
        final String[] args = {"remove-admin",
                               "-admin", argsMap.get("-admin"),
                               "replace-this-flag", "replace-this-value"};

        for (String element : flagsArray) {
            args[replaceIndx] = element;
            args[replaceIndx + 1] = argsMap.get(element);

            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            replay(cs);

            expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs,
                                                                      null));
            replay(shell);

            /* Run the test and verify the results. */
            try {
                subObj.execute(args, shell);
                fail("ShellUsageException expected, but wasn't encountered");
            } catch (ShellUsageException e) {
                assertEquals(expectedException.getMessage(), e.getMessage());
                doVerification(shell, cs);
            }
        }
    }

    /**
     * Verify the expected exception is thrown when the -dc or -zn flag is
     * combined with the -dcname or -znname flag.
     */
    @Test
    public void testRemoveAdminSubExecuteDcIdNameCombo() throws Exception {

        final PlanCommand.RemoveAdminSub subObj =
            new PlanCommand.RemoveAdminSub();

        final ShellUsageException expectedException =
            new ShellUsageException(subObj.dcIdNameError, subObj);

        final Map<String, String> argsMap = removeAdminSubArgsMap;
        final String[] idArgs = {"-zn", "-dc"};
        final String[] nameArgs = {"-znname", "-dcname"};

        for (int i = 0; i < 4; i++) {
            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);
            final String idArg = idArgs[i%2];
            final String nameArg = nameArgs[i/2];
            final String[] args = {"remove-admin",
                                   idArg, argsMap.get(idArg),
                                   nameArg, argsMap.get(nameArg)};

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            replay(cs);

            expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs,
                                                                      null));
            replay(shell);

            /* Run the test and verify the results. */
            try {
                subObj.execute(args, shell);
                fail("ShellUsageException expected, but wasn't encountered");
            } catch (ShellUsageException e) {
                assertEquals(expectedException.getMessage(),
                             e.getMessage());
                doVerification(shell, cs);
            }
        }
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when the -admin flag is used and there is no Admin in the store
     * with the specified AdminId.
     */
    @Test
    public void testRemoveAdminSubExecuteAdminFlagNone() throws Exception {
        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();
        final AdminId aid = AdminId.parse("3");
        final AdminParams aidParams = null;
        final Integer nAdmins = 4;
        final String expect = sub.noAdminError + "[" + aid + "]";

        removeAdminSubExecuteAdminFlag(sub, aid, aidParams, nAdmins, expect);
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when the -admin flag is used and there is only one Admin in the
     * store.
     */
    @Test
    public void testRemoveAdminSubExecuteAdminFlagOnly1() throws Exception {
        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();
        final AdminId aid = AdminId.parse("1");
        final AdminParams aidParams = adminParams;
        final Integer nAdmins = 1;
        final String expect = sub.only1AdminError;

        removeAdminSubExecuteAdminFlag(sub, aid, aidParams, nAdmins, expect);
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when the -admin flag is used and there are less than 4 Admins in
     * the store and the -force flag is not used.
     */
    @Test
    public void testRemoveAdminSubExecuteAdminFlagTooFew() throws Exception {
        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();
        final AdminId aid = AdminId.parse("2");
        final AdminParams aidParams = adminParams;
        final Integer nAdmins = 2;
        final String expect = sub.tooFewAdminError +
            "There are only " + nAdmins + " Admins in the store.";

        removeAdminSubExecuteAdminFlag(sub, aid, aidParams, nAdmins, expect);
    }

    /**
     * Convenience method shared by the testRemoveAdminSubExecuteAdminFlagXXX
     * test case methods that performs the actual steps of the test case, using
     * the case-specific values referenced by each parameter.
     */
    private void removeAdminSubExecuteAdminFlag(
                     final PlanCommand.RemoveAdminSub subObj,
                     final AdminId aid,
                     final AdminParams paramsByAid,
                     final Integer nAdmins,
                     final String expectedResult)
        throws Exception {

        final String[] args = {"remove-admin", "-admin",
                               Integer.toString(aid.getAdminInstanceId())};

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);
        final Parameters params = createMock(Parameters.class);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(params);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        expect(params.getAdminCount()).andReturn(nAdmins);
        expect(params.get(aid)).andReturn(paramsByAid);
        replay(params);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("should not reach here");
        } catch (ShellArgumentException e) {
            assertTrue(e.getMessage().contains(expectedResult));
        }
        doVerification(shell, cs, params);
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when either the -zn flag or the -znname flag is used and one of
     * three error or warning conditions related to Admins exist; for example,
     * when there is no Admin in the specified datacenter, the specified
     * datacenter contains all the Admins in the store, or if removing all the
     * Admins in the specified datacenter would cause quorum to be lost.
     */
    @Test
    public void testRemoveAdminSubExecuteDcFlag() throws Exception {

        final PlanCommand.RemoveAdminSub subObj =
            new PlanCommand.RemoveAdminSub();

        final Map<String, String> argsMap = removeAdminSubArgsMap;

        final String[] aidStr = {"3", "1", "2", "4"};
        final Integer nAdmins = aidStr.length;

        final List<Set<AdminId>> adminIdSet = new ArrayList<Set<AdminId>>();
        adminIdSet.add(new HashSet<AdminId>());
        adminIdSet.add(new HashSet<AdminId>());
        adminIdSet.add(new HashSet<AdminId>());

        /* First adminIdSet should be left empty */

        /* Second adminIdSet should contain all admins in the store */
        for (String element : aidStr) {
            adminIdSet.get(1).add(AdminId.parse(element));
        }

        /* Third adminIdSet should contain a subset of the store's admins */
        for (int i = 2; i < aidStr.length; i++) {
            adminIdSet.get(2).add(AdminId.parse(aidStr[i]));
        }

        final AdminId[] aid = new AdminId[aidStr.length];
        for (int i = 0; i < aidStr.length; i++) {
            aid[i] = AdminId.parse(aidStr[i]);
        }

        final int replaceIndx = 1;
        final String[] flagsArray = {"-zn", "-znname"};
        final String[] args = {"remove-admin",
                               "replace-this-flag", "replace-this-value"};

        final DatacenterId dcid = DatacenterId.parse(argsMap.get("-zn"));
        final String dcName = argsMap.get("-znname");
        final String[] expectedResult =
            {
                subObj.noAdminDcError + "[" + dcid.toString() + "]",
                subObj.allAdminDcError + "[" + dcid.toString() + "]",
                subObj.tooFewAdminDcError + "There are " + aidStr.length +
                    " Admins in the store and " + adminIdSet.get(2).size() +
                    " Admins in the specified zone " +
                    "[" + dcid.toString() + "]",
                subObj.noAdminDcError + "[" + dcName + "]",
                subObj.allAdminDcError + "[" + dcName + "]",
                subObj.tooFewAdminDcError + "There are " + aidStr.length +
                    " Admins in the store and " + adminIdSet.get(2).size() +
                    " Admins in the specified zone " +
                    "[" + dcName + "]"
            };

        final Topology topo = new Topology("TEST_TOPOLOGY");
        topo.add(Datacenter.newInstance(dcName, 1,
                                        DatacenterType.PRIMARY, false, false));

        for (int i = 0; i < flagsArray.length; i++) {
            args[replaceIndx] = flagsArray[i];
            args[replaceIndx + 1] = argsMap.get(flagsArray[i]);

            for (int j = 0; j < adminIdSet.size(); j++) {
                final int k = j + i * adminIdSet.size();
                final CommandShell shell = createMock(CommandShell.class);
                final CommandService cs = createMock(CommandService.class);
                final Parameters params = createMock(Parameters.class);

                /* Establish what is expected from each mock for this test */
                expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
                expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
                    .andReturn(params);
                /*
                 * If -znname is used, there is an extra call to getTopology;
                 * because CommandUtils.getDatacenterId is called.
                 */
                if (("-znname").equals(args[replaceIndx])) {
                    expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                        .andReturn(topo);
                }
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                    .andReturn(topo);
                replay(cs);

                expect(shell.getAdmin()).andReturn(
                    CommandServiceAPI.wrap(cs,null));
                replay(shell);

                expect(params.getAdminCount()).andReturn(nAdmins);
                expect(params.getAdminIds(dcid, topo))
                    .andReturn(adminIdSet.get(j));
                replay(params);

                /* Run the test and verify the results. */
                try {
                    subObj.execute(args, shell);
                    fail("should not reach");
                } catch (ShellArgumentException e) {
                    assertTrue(e.getMessage().contains(expectedResult[k]));
                }
                doVerification(shell, cs, params);
            }
        }
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when either the -zn flag or the -znname flag is used and there are
     * no Admins in the specified datacenter.
     */
    @Test
    public void testRemoveAdminSubExecuteDcFlagNone() throws Exception {

        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();

        /* Store contains N Admins, but NONE are in the specified dc */
        final int nAdminsInDc = 0;
        final String resultPrefix = sub.noAdminDcError;

        removeAdminSubExecuteDcFlag(sub, "-zn", nAdminsInDc, resultPrefix);
        removeAdminSubExecuteDcFlag(sub, "-znname", nAdminsInDc, resultPrefix);
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when either the -zn flag or the -znname flag is used and all
     * Admins are in the specified datacenter; which, if removal is actually
     * allowed, would result in the removal of all Admins from the store.
     */
    @Test
    public void testRemoveAdminSubExecuteDcFlagAll() throws Exception {

        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();

        /* Store contains N Admins, and ALL N are in the specified dc */
        final int nAdminsInDc = removeAdminSubExecuteStoreAdmins().length;
        final String resultPrefix = sub.allAdminDcError;

        removeAdminSubExecuteDcFlag(sub, "-zn", nAdminsInDc, resultPrefix);
        removeAdminSubExecuteDcFlag(sub, "-znname", nAdminsInDc, resultPrefix);
    }

    /**
     * Verify the RemoveAdminSub.execute method returns the expected String
     * value when either the -zn flag or the -znname flag is used and a subset
     * of the store's Admins are in the specified datacenter such that if all
     * Admins in the specified datacenter are allowed to be removed, the loss
     * of one or more of the remaining Admins would result in the loss of
     * quorum.
     */
    @Test
    public void testRemoveAdminSubExecuteDcFlagTooFew() throws Exception {

        final PlanCommand.RemoveAdminSub sub =
            new PlanCommand.RemoveAdminSub();

        /* Store contains N Admins, and half are in the specified dc */
        final int nAdminsInDc = removeAdminSubExecuteStoreAdmins().length / 2;
        final String resultPrefix = sub.tooFewAdminDcError +
            "There are " + removeAdminSubExecuteStoreAdmins().length +
            " Admins in the store and " + nAdminsInDc + " Admins in the " +
            "specified zone ";

        removeAdminSubExecuteDcFlag(sub, "-zn", nAdminsInDc, resultPrefix);
        removeAdminSubExecuteDcFlag(sub, "-znname", nAdminsInDc, resultPrefix);
    }

    /**
     * Convenience method shared by the testRemoveAdminSubExecuteDcFlagXXX test
     * case methods as well as the removeAdminSubExecuteDcFlag convenience
     * method. Returns an array containing the ids (in String form) of each of
     * the Admins in the store under test. A shared method such as this is
     * provided to allow the array of ids to be defined in one place; so that
     * each test case method can construct its expected result, and so that the
     * removeAdminSubExecuteDcFlag method can construct the Set of AdminIds it
     * uses to execute the given test case.
     */
    private static String[] removeAdminSubExecuteStoreAdmins() {
        return new String[] {"3", "1", "2", "4"};
    }

    /**
     * Convenience method shared by the testRemoveAdminSubExecuteDcFlagXXX test
     * case methods that performs the actual steps of the test case, using the
     * case-specific values referenced by each parameter.
     */
    private void removeAdminSubExecuteDcFlag(
                     final PlanCommand.RemoveAdminSub subObj,
                     final String dcFlag,
                     final int nAdminsInDc,
                     final String expectedBase) throws Exception {

        final String[] adminsInStore = removeAdminSubExecuteStoreAdmins();
        final Integer nAdminsInStore = adminsInStore.length;
        final Set<AdminId> adminsInDc = new HashSet<AdminId>();
        for (int i = 0; i < nAdminsInDc; i++) {
            adminsInDc.add(AdminId.parse(adminsInStore[i]));
        }

        final Map<String, String> argsMap = removeAdminSubArgsMap;
        final DatacenterId dcid = DatacenterId.parse(argsMap.get("-zn"));
        final String dcName = argsMap.get("-znname");

        String expectedResult = null;
        String dcVal = null;

        if (("-zn").equals(dcFlag)) {
            expectedResult = expectedBase + "[" + dcid.toString() + "]";
            dcVal = Integer.toString(dcid.getDatacenterId());
        } else {
            expectedResult = expectedBase + "[" + dcName + "]";
            dcVal = dcName;
        }
        final String[] args = {"remove-admin", dcFlag, dcVal};

        final Topology topo = new Topology("TEST_TOPOLOGY");
        topo.add(Datacenter.newInstance(dcName, 1,
                                        DatacenterType.PRIMARY, false, false));

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);
        final Parameters params = createMock(Parameters.class);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(params);
        /* If using -znname, there is an extra call to getTopology;
         * because CommandUtils.getDatacenterId is called.
         */
        if (("-znname").equals(dcFlag)) {
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                .andReturn(topo);
        }
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);
        expect(params.getAdminCount()).andReturn(nAdminsInStore);
        expect(params.getAdminIds(dcid, topo)).andReturn(adminsInDc);
        replay(params);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("should not reach");
        } catch (ShellArgumentException e) {
            assertTrue(e.getMessage().contains(expectedResult));
        }
        doVerification(shell, cs, params);
    }

    @Test
    public void testRemoveAdminSubExecuteDcDeprecation()
        throws Exception {

        doRemoveAdminSubExecute("",
                                "plan", "remove-admin", "-zn", "zn1");
        doRemoveAdminSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                "plan", "remove-admin", "-zn", "dc1");
        doRemoveAdminSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                "plan", "remove-admin", "-dc", "zn1");
        doRemoveAdminSubExecute(PlanSubCommand.dcFlagsDeprecation,
                                "plan", "remove-admin", "-dc", "dc1");
    }

    private void doRemoveAdminSubExecute(final String deprecation,
                                         final String... cmd)
        throws Exception {

        /* Establish mocks */
        final Parameters params = createMock(Parameters.class);
        expect(params.getAdminCount()).andStubReturn(4);
        final DatacenterId dcId = new DatacenterId(1);
        final Topology topo = new Topology("MyTopo");
        topo.add(
            Datacenter.newInstance("MyZone", 1, DatacenterType.PRIMARY, false,
                                   false));
        final AdminId adminId = new AdminId(ADMIN_ID);
        expect(params.getAdminIds(dcId, topo))
            .andStubReturn(singleton(adminId));
        replay(params);

        final CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(params);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(topo);
        expect(cs.createRemoveAdminPlan(
                   isNull(String.class), eq(dcId), isNull(AdminId.class),
                   eq(false), eq(NULL_AUTH), eq(SerialVersion.CURRENT)))
            .andStubReturn(42);
        cs.approvePlan(42, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(42, false, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        CommandShell shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(false);
        replay(shell);

        /* Execute command and check result */
        assertEquals(
            "Command result",
            deprecation +
            "Started plan 42. Use show plan -id 42 to check status." + eolt +
            "To wait for completion, use plan wait -id 42",
            new PlanCommand().execute(cmd, shell));

        shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(true);
        replay(shell);
        checkPlanJson(cmd, shell);
    }

    /* 5. Test case coverage for: PlanCommand.DeployDCSub. */

    @Test
    public void testDeployZoneSubGetCommandSyntax() throws Exception {
        testDeployZoneSubGetCommandSyntax(
            new PlanCommand.DeployZoneSub(), "zone");
    }

    @Test
    public void testDeployDCSubGetCommandSyntax() throws Exception {
        testDeployZoneSubGetCommandSyntax(
            new PlanCommand.DeployDCSub(), "datacenter");
    }

    private void testDeployZoneSubGetCommandSyntax(
        final PlanCommand.DeployZoneSub subObj, final String term) {

        final String expectedResult =
            "plan deploy-" + term + " -name <zone name>" +
            eolt + "-rf <replication factor>" +
            eolt + "[-type {primary | secondary}]" +
            eolt + "[-arbiters | -no-arbiters]" +
            eolt + "[-master-affinity | -no-master-affinity]" +
            eolt + PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testDeployDCSubGetCommandDescription() throws Exception {
        assertEquals(PlanCommand.DeployZoneSub.COMMAND_DESC + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "plan deploy-zone",
                     new PlanCommand.DeployDCSub().getCommandDescription());
        assertEquals(PlanCommand.DeployZoneSub.COMMAND_DESC,
                     new PlanCommand.DeployZoneSub().getCommandDescription());
    }

    @Test
    public void testDeployDCSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.DeployDCSub(), "deploy-zone");
        doExecuteUnknownArg(
            new PlanCommand.DeployZoneSub(), "deploy-zone");
    }

    @Test
    public void testDeployDCSubExecuteInvalidValue() throws Exception {
        doExecuteInvalidValue(
            new PlanCommand.DeployDCSub(), "deploy-zone",
            "-type", "Invalid zone type: ");
        doExecuteInvalidValue(
            new PlanCommand.DeployZoneSub(), "deploy-zone",
            "-type", "Invalid zone type: ");
    }

    @Test
    public void testDeployDCSubExecuteRequiredArgs() throws Exception {

        final String command = "deploy-zone";
        final Map<String, String> argsMap = deployDCSubArgsMap;

        final PlanCommand.DeployZoneSub[] subs = {
            new PlanCommand.DeployZoneSub(),
            new PlanCommand.DeployDCSub()
        };
        for (final PlanCommand.DeployZoneSub sub : subs) {

            /* Missing all required args. */
            doExecuteRequiredArgs(
                sub, command, argsMap, new String[]  {"-name", "-rf"}, "rf");

            /* Missing -name arg but not -rf. */
            doExecuteRequiredArgs(
                sub, command, argsMap, new String[]  {"-name"}, "name");

            /* Missing -rf arg but not -name. */
            doExecuteRequiredArgs(
                sub, command, argsMap, new String[]  {"-rf"}, "rf");
        }
    }

    @Test
    public void testDeployZoneSubExecuteDcDeprecation()
        throws Exception {

        doDeployZoneSubExecute(
            "",
            false,
            "plan", "deploy-zone", "-name", "MyZone", "-rf", "1");
        doDeployZoneSubExecute(
            DeployDCSub.dcCommandDeprecation,
            false,
            "plan", "deploy-datacenter", "-name", "MyZone", "-rf", "1");
    }

    /*
     * Test case to check the ShellException is thrown when a blank string is
     * passed as zone name.
     */

    @Test
    public void testDeployZoneSubExecute()
        throws Exception {

        /* Expect ShellException when empty or non-empty blank name is passed */
        doDeployZoneSubExecuteWithEmptyOrNonEmptyBlankZoneName(true,
            "plan", "-name", "", "-rf", "1");
        doDeployZoneSubExecuteWithEmptyOrNonEmptyBlankZoneName(true,
            "plan", "-name", "  ", "-rf", "1");

    }

    private void doDeployZoneSubExecuteWithEmptyOrNonEmptyBlankZoneName(
                                                    final boolean json,
                                                    final String... cmd)
        throws Exception {

        /* Establish mocks */
        final PlanCommand.DeployZoneSub subObj = new PlanCommand
            .DeployZoneSub();
        final CommandService cs = createMock(CommandService.class);
        String info = "the value is empty or contain only blanks";
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.createDeployDatacenterPlan(null, cmd[2], 1,
            DatacenterType.PRIMARY, false,
            false, null,
            SerialVersion.CURRENT))
            .andStubReturn(42);
        cs.approvePlan(42, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(42, false, null,
            SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        CommandShell shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(json);
        expect(shell.getCurrentCommand()).andReturn(subObj).anyTimes();

        shell.badArgUsage("name", info, subObj);
        expectLastCall().andThrow(new ShellException()).anyTimes();

        replay(shell);

        /* Executing command and checking result */
        assertThrows(ShellException.class, () -> subObj.exec(cmd, shell));
    }

    @Test
    public void testDeployZoneSubExecuteJsonMode()
        throws Exception {
        doDeployZoneSubExecute(
            "",
            true,
            "plan", "deploy-zone", "-name", "MyZone", "-rf", "1");
    }

    private void doDeployZoneSubExecute(final String deprecation,
                                        final boolean json,
                                        final String... cmd)
        throws Exception {

        /* Establish mocks */
        final CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.createDeployDatacenterPlan(null, "MyZone", 1,
                                             DatacenterType.PRIMARY, false,
                                             false, null,
                                             SerialVersion.CURRENT))
            .andStubReturn(42);
        cs.approvePlan(42, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(42, false, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        CommandShell shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(json);
        replay(shell);

        /* Execute command and check result */
        final String result = new PlanCommand().execute(cmd, shell);
        if (json) {
            JsonNode resultNode = JsonUtils.parseJsonNode(result);

            assertEquals("plan deploy-zone",
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(42, valueNode.get("plan_id").asInt());

            shell = createMock(CommandShell.class);
            expect(shell.getAdmin())
                .andStubReturn(CommandServiceAPI.wrap(cs, null));
            expect(shell.getJson()).andStubReturn(true);
            replay(shell);
            checkPlanJson(cmd, shell);
        } else {
            assertEquals(
                "Command result",
                deprecation +
                "Started plan 42. Use show plan -id 42 to check status." + eolt +
                "To wait for completion, use plan wait -id 42",
                result);
        }
    }

    /* 6. Test case coverage for: PlanCommand.DeploySNSub. */

    @Test
    public void testDeploySNSubGetCommandSyntax() throws Exception {

        final PlanCommand.DeploySNSub subObj =
            new PlanCommand.DeploySNSub();
        final String expectedResult =
            "plan deploy-sn -zn <id> | -znname <name> " +
            "-host <host> -port <port>" + eolt +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testDeploySNSubGetCommandDescription() throws Exception {

        final PlanCommand.DeploySNSub subObj =
            new PlanCommand.DeploySNSub();
        final String expectedResult =
            "Deploys the storage node at the specified host and port " +
            "into the" + eolt + "specified zone.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testDeploySNSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.DeploySNSub(), "deploy-sn");
    }

    @Test
    public void testDeploySNSubExecuteRequiredArgs() throws Exception {

        final String command = "deploy-sn";
        final Map<String, String> argsMap = deploySNSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.DeploySNSub(), command, argsMap,
            new String[] {
                "-host", "-port", "-zn", "-znname", "-dc", "-dcname"
            });

        /* Missing -host arg but not the others. */
        doExecuteRequiredArgs(
            new PlanCommand.DeploySNSub(), command, argsMap,
            new String[]  {"-host"});

        /* Missing -port arg but not the others. */
        doExecuteRequiredArgs(
            new PlanCommand.DeploySNSub(), command, argsMap,
            new String[]  {"-port"});

        /* Missing -zn and -znname args but not the others. */
        doExecuteRequiredArgs(
            new PlanCommand.DeploySNSub(), command, argsMap,
            new String[]  {"-zn", "-znname", "-dc", "-dcname"});
    }

    /* 7. Test case coverage for: PlanCommand.DeployTopologySub. */

    @Test
    public void testDeployTopologySubGetCommandSyntax() throws Exception {

        final PlanCommand.DeployTopologySub subObj =
            new PlanCommand.DeployTopologySub();
        final String expectedResult =
             "plan deploy-topology -name <topology name>" +
             PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testDeployTopologySubGetCommandDescription() throws Exception {

        final PlanCommand.DeployTopologySub subObj =
            new PlanCommand.DeployTopologySub();
        final String expectedResult =
            "Deploys the specified topology to the store.  This " +
            "operation can" + eolt + "take a while, depending on " +
            "the size and state of the store.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testDeployTopologySubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.DeployTopologySub(), "deploy-topology");
    }

    @Test
    public void testDeployTopologySubExecuteRequiredArgs() throws Exception {

        final String command = "deploy-topology";
        final Map<String, String> argsMap = deployTopologySubArgsMap;

        /* Missing the only required arg, -name. */
        doExecuteRequiredArgs(
            new PlanCommand.DeployTopologySub(), command, argsMap,
            new String[]  {"-name"}, "-name");
    }


    /* 8. Test case coverage for: PlanCommand.ExecuteSub. */

    @Test
    public void testExecuteSubGetCommandSyntax() throws Exception {

        final PlanCommand.ExecuteSub subObj = new PlanCommand.ExecuteSub();
        final String expectedResult = "plan execute -id <id> | -last" +
                                      PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testExecuteSubGetCommandDescription() throws Exception {

        final PlanCommand.ExecuteSub subObj = new PlanCommand.ExecuteSub();
        final String expectedResult =
            "Executes a created, but not yet executed plan.  The plan " +
            "must have" + eolt + "been previously created using the " +
            "-noexecute flag. Use -last to" + eolt + "reference the " +
            "most recently created plan.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testExecuteSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.ExecuteSub(), "execute");
    }

    @Test
    public void testExecuteSubExecuteRequiredArgs() throws Exception {

        final String command = "execute";
        final Map<String, String> argsMap = executeSubArgsMap;

        /* Missing both required args, -id, and -last. */
        doExecuteRequiredArgs(
            new PlanCommand.ExecuteSub(), command, argsMap,
            new String[]  {"-id", "-last"}, "-id|-last");
    }

    /* 9. Test case coverage for: PlanCommand.InterruptSub. */

    @Test
    public void testInterruptSubGetCommandSyntax() throws Exception {

        final PlanCommand.InterruptSub subObj = new PlanCommand.InterruptSub();
        final String expectedResult =
            "plan interrupt -id <plan id> | -last " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testInterruptSubGetCommandDescription() throws Exception {

        final PlanCommand.InterruptSub subObj = new PlanCommand.InterruptSub();
        final String expectedResult =
            "Interrupts a running plan. An interrupted plan can " +
            "only be re-executed" + eolt + "or canceled.  Use -last " +
            "to reference the most recently" + eolt + "created plan.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testInterruptSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.InterruptSub(), "interrupt");
    }

    @Test
    public void testInterruptSubExecuteRequiredArgs() throws Exception {

        final String command = "interrupt";
        final Map<String, String> argsMap = interruptCancelSubArgsMap;

        /* Missing both required args, -id, and -last. */
        doExecuteRequiredArgs(
            new PlanCommand.InterruptSub(), command, argsMap,
            new String[]  {"-id", "-last"}, "-id|-last");
    }

    /* 10. Test case coverage for: PlanCommand.CancelSub. */

    @Test
    public void testCancelSubGetCommandSyntax() throws Exception {

        final PlanCommand.CancelSub subObj = new PlanCommand.CancelSub();
        final String expectedResult =
            "plan cancel -id <plan id> | -last " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testCancelSubGetCommandDescription() throws Exception {

        final PlanCommand.CancelSub subObj = new PlanCommand.CancelSub();
        final String expectedResult =
            "Cancels a plan that is not running.  A running plan must " +
            "be" + eolt + "interrupted before it can be canceled. " +
            "Use -last to reference the most" + eolt + "recently " +
            "created plan.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testCancelSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.CancelSub(), "cancel");
    }

    @Test
    public void testCancelSubExecuteRequiredArgs() throws Exception {

        final String command = "cancel";
        final Map<String, String> argsMap = interruptCancelSubArgsMap;

        /* Missing both required args, -id, and -last. */
        doExecuteRequiredArgs(
            new PlanCommand.CancelSub(), command, argsMap,
            new String[]  {"-id", "-last"}, "-id|-last");
    }

    /* 11. Test case coverage for: PlanCommand.MigrateSNSub. */

    @Test
    public void testMigrateSNSubGetCommandSyntax() throws Exception {

        final PlanCommand.MigrateSNSub subObj = new PlanCommand.MigrateSNSub();
        final String expectedResult =
            "plan migrate-sn -from <id> -to <id> " +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testMigrateSNSubGetCommandDescription() throws Exception {

        final PlanCommand.MigrateSNSub subObj = new PlanCommand.MigrateSNSub();
        final String expectedResult =
            "Migrates the services from one storage node to another. " +
            "The old node" + eolt + "must not be running.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testMigrateSNSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.MigrateSNSub(), "migrate-sn");
    }

    @Test
    public void testMigrateSNSubExecuteRequiredArgs() throws Exception {

        final String command = "migrate-sn";
        final Map<String, String> argsMap = migrateSNSubArgsMap;

        /* Missing both required args, -from, and -to. */
        doExecuteRequiredArgs(
            new PlanCommand.MigrateSNSub(), command, argsMap,
            new String[]  {"-from", "-to"});

        /* Missing only the required arg -from. */
        doExecuteRequiredArgs(
            new PlanCommand.MigrateSNSub(), command, argsMap,
            new String[]  {"-from"});

        /* Missing only the required arg -from. */
        doExecuteRequiredArgs(
            new PlanCommand.MigrateSNSub(), command, argsMap,
            new String[]  {"-to"});
    }

    /* 12. Test case coverage for: PlanCommand.RemoveSNSub. */

    @Test
    public void testRemoveSNSubGetCommandSyntax() throws Exception {

        final PlanCommand.RemoveSNSub subObj = new PlanCommand.RemoveSNSub();
        final String expectedResult =
            "plan remove-sn -sn <id>" +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveSNSubGetCommandDescription() throws Exception {

        final PlanCommand.RemoveSNSub subObj = new PlanCommand.RemoveSNSub();
        final String expectedResult =
            "Removes the specified storage node from the topology.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testRemoveSNSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.RemoveSNSub(), "remove-sn");
    }

    @Test
    public void testRemoveSNSubExecuteRequiredArgs() throws Exception {

        final String command = "remove-sn";
        final Map<String, String> argsMap = removeSNSubArgsMap;

        /* Missing the only required arg, -sn. */
        doExecuteRequiredArgs(
            new PlanCommand.RemoveSNSub(), command, argsMap,
            new String[]  {"-sn"}, "-sn");
    }

    /* 13. Test case coverage for: PlanCommand.StartServiceSub. */

    @Test
    public void testStartServiceSubGetCommandSyntax() throws Exception {

        final PlanCommand.StartServiceSub subObj =
            new PlanCommand.StartServiceSub();
        final String expectedResult =
            "plan start-service {-service <id> | -all-rns " +
            "[-zn <id> | -znname <name>] | " +
            "-all-ans [-zn <id> | -znname <name>] | " +
            "-zn <id> | -znname <name>}" +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testStartServiceSubGetCommandDescription() throws Exception {

        final PlanCommand.StartServiceSub subObj =
            new PlanCommand.StartServiceSub();
        final String expectedResult = "Starts the specified service(s).";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testStartServiceSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.StartServiceSub(), "start-service");
    }

    @Test
    public void testStartSubExecuteRequiredArgs() throws Exception {

        final String command = "start-service";
        final Map<String, String> argsMap = startStopServiceSubArgsMap;

        /* Missing both required args, -service, and -all-rns. */
        doExecuteRequiredArgs(
            new PlanCommand.StartServiceSub(), command, argsMap,
            new String[]  {"-service", "-all-rns", "-all-ans", "-zn",
                           "-znname"},
                           "-service|-all-rns|-all-ans|-zn|-znname");
    }

    @Test
    public void testStartSubExecuteInvalidCombo() throws Exception {

        final String cannotMixMsg1 =
            "Cannot use -service and -all-rns flags together";
        final String connotMixMsg2 =
            "Cannot use -zn or -znname flags with the -service flag";
        final String connotMixMsg3 =
            "Cannot use both the -zn and -znname flags";

        final String[][] errorMessages =
            { {connotMixMsg2, cannotMixMsg1, connotMixMsg2},
              {connotMixMsg3},
            };

        final Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.putAll(startStopServiceSubArgsMap);
        argsMap.putAll(startStopServiceWithZoneIdSubArgsMap);
        argsMap.putAll(startStopServiceWithZoneNameSubArgsMap);

        int replaceIndex1 = 1, replaceIndex2 = 3;

        final String[] firstFlags = {"-service", "-zn"};
        final String[] secondFlags = {"-znname", "-all-rns", "-zn"};

        final String[] allArgs = {"start-service",
                                  "replace-this-flag", "replace-this-flag",
                                  "replace-this-flag", "replace-this-flag"};

        final Topology topo = new Topology("TEST_TOPOLOGY");
        final RepGroup rg = new RepGroup();
        topo.add(rg);
        rg.add(new RepNode(new StorageNodeId(1)));

        /*
         * i=0, j=0: start-service -service -znname zn1
         * i=0, j=1: start-service -service -all-rns
         * i=0, j=2: start-service -service -zn zn1
         * i=1, j=0: start-service -zn -znname
         */
        for (int i = 0; i <= 1; i++) {
            for(int j = 0; j <= 2; j++) {
                if (i==1 && j==1) {
                    break;
                }
                String curFlag = firstFlags[i];
                String nextFlag = secondFlags[j];
                allArgs[replaceIndex1] = curFlag;
                allArgs[replaceIndex1 + 1] = argsMap.get(curFlag);
                allArgs[replaceIndex2] = nextFlag;
                allArgs[replaceIndex2 + 1] = argsMap.get(nextFlag);

                final PlanCommand.StartServiceSub subObj =
                            new PlanCommand.StartServiceSub();
                final ShellUsageException expectedException =
                    new ShellUsageException(errorMessages[i][j], subObj);

                final CommandShell shell = createMock(CommandShell.class);
                final CommandService cs = createMock(CommandService.class);

                /* Establish what is expected from each mock for this test */
                expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);
                replay(cs);

                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null));
                replay(shell);

                /* Run the test and verify the results. */
                try {
                    subObj.execute(allArgs, shell);
                    fail("ShellUsageException expected,"
                          + " but required arg input");
                } catch (ShellUsageException e) {
                    assertEquals(expectedException.getMessage(),
                                 e.getMessage());
                }
            }
        }
    }

    @Test
    public void testStartSubExecuteExpectedBehavior() throws Exception {

        final String[][] args = {{"start-service", "-all-rns", "-zn", "zn1"},
                                 {"start-service", "-all-rns", "-zn", "zn2"},
                                 /* take the last zone id zn2 */
                                 {"start-service", "-all-rns", "-zn", "zn1",
                                  "-zn", "zn3", "-zn", "zn2"},
                                 {"start-service", "-all-rns", "-znname", "zn1"},
                                 {"start-service", "-all-rns", "-znname", "zn2"},
                                 /* take the last zone name zn2 */
                                 {"start-service", "-all-rns", "-znname", "zn1",
                                  "-znname", "zn3", "-znname", "zn2"},
                                 /* zone only */
                                 {"start-service", "-zn", "zn1"},
                                 {"start-service", "-znname", "zn2"},
                                 /* All RNs */
                                 {"start-service", "-all-rns"}};

        final Set<RepNodeId> zn1RNs = new HashSet<RepNodeId>();
        final Set<RepNodeId> zn2RNs = new HashSet<RepNodeId>();

        final List<Set<RepNodeId>> expectedRnSets =
            new ArrayList<Set<RepNodeId>>();
        expectedRnSets.add(zn1RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn1RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn1RNs);     /* -zn zn1 only command */
        expectedRnSets.add(zn2RNs);     /* -znname zn2 only command */

        final String expectedMessage =
            "Started plan 42. Use show plan -id 42 to check status." + eolt +
            "To wait for completion, use plan wait -id 42";

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;

        final Topology topo = new Topology("TEST_TOPOLOGY");
        Datacenter dc1 = Datacenter
            .newInstance(dcName1, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc1);
        Datacenter dc2 = Datacenter
            .newInstance(dcName2, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc2);
        StorageNode sn1 = new StorageNode(dc1, dummyHost1, dummyRegistyPort1);
        topo.add(sn1);
        StorageNode sn2 = new StorageNode(dc2, dummyHost2, dummyRegistyPort2);
        topo.add(sn2);
        RepGroup rg = new RepGroup();
        topo.add(rg);

        RepNodeId rnId1 = new RepNodeId(1, 1);
        RepNodeId rnId2 = new RepNodeId(1, 2);
        RepNodeId rnId3 = new RepNodeId(1, 3);
        RepNodeId rnId4 = new RepNodeId(1, 4);

        RepNode rn1 = new RepNode(sn1.getResourceId());
        RepNode rn2 = new RepNode(sn1.getResourceId());
        RepNode rn3 = new RepNode(sn2.getResourceId());
        RepNode rn4 = new RepNode(sn2.getResourceId());

        rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);

        zn1RNs.add(rnId1); zn1RNs.add(rnId2);
        zn2RNs.add(rnId3); zn2RNs.add(rnId4);

        for (int i = 0; i < args.length; i++) {
            CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);
            final Parameters params = createMock(Parameters.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).
                andReturn(SerialVersion.CURRENT).anyTimes();
            expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT)).
                andStubReturn(params);
            expect(params.getAdminIds(dc1.getResourceId(), topo)).
                    andReturn(Collections.<AdminId>emptySet()).anyTimes();
            expect(params.getAdminIds(dc2.getResourceId(), topo)).
                    andReturn(Collections.<AdminId>emptySet()).anyTimes();
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);

            if (i != args.length - 1) {
                expect(cs.createStartServicesPlan(isNull(String.class),
                   eq(expectedRnSets.get(i)), eq(NULL_AUTH),
                   eq(SerialVersion.CURRENT))).andStubReturn(42);
            } else {
                expect(cs.createStartAllRepNodesPlan(isNull(String.class),
                       eq(NULL_AUTH),
                       eq(SerialVersion.CURRENT))).andStubReturn(42);
            }

            cs.approvePlan(42, null, SerialVersion.CURRENT);
            expectLastCall().anyTimes();
            cs.executePlan(42, false, null, SerialVersion.CURRENT);
            expectLastCall().anyTimes();
            replay(cs);
            replay(params);

            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null)).anyTimes();
            expect(shell.getJson()).andStubReturn(false);
            replay(shell);

            final PlanCommand.StartServiceSub subObj =
                            new PlanCommand.StartServiceSub();
            final String result = subObj.execute(args[i], shell);
            assertEquals(result, expectedMessage);
            shell = createMock(CommandShell.class);
            expect(shell.getAdmin()).andReturn(
                CommandServiceAPI.wrap(cs,null)).anyTimes();
            expect(shell.getJson()).andStubReturn(true);
            replay(shell);
            checkPlanJson(args[i], shell, new PlanCommand.StartServiceSub());
        }
    }

    @Test
    public void testStartSubExecuteInvalidZone() throws Exception {
        final PlanCommand.StartServiceSub subObj =
            new PlanCommand.StartServiceSub();

        final String[][] args = {{"start-service", "-all-rns", "-zn", "zn2"},
                               {"start-service", "-all-rns", "-znname", "zn2"}};

        final Set<RepNodeId> rnSet = new HashSet<RepNodeId>();

        final String[] expectedExceptionMessages =
            {"The specified zone id does not exist",
             "The specified zone name does not exist"};

        final String dcName = "zn1";
        final String dummyHost = "dummy1.us.oracle.com";
        final int dummyRegistyPort = 5001;

        final Topology topo = new Topology("TEST_TOPOLOGY");
        Datacenter dc = Datacenter
            .newInstance(dcName, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc);
        StorageNode sn = new StorageNode(dc, dummyHost, dummyRegistyPort);
        topo.add(sn);
        RepGroup rg = new RepGroup();
        topo.add(rg);

        RepNodeId rnId1 = new RepNodeId(1, 1);
        RepNodeId rnId2 = new RepNodeId(1, 2);
        RepNodeId rnId3 = new RepNodeId(1, 3);
        RepNodeId rnId4 = new RepNodeId(1, 4);

        RepNode rn1 = new RepNode(sn.getResourceId());
        RepNode rn2 = new RepNode(sn.getResourceId());
        RepNode rn3 = new RepNode(sn.getResourceId());
        RepNode rn4 = new RepNode(sn.getResourceId());

        rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);

        rnSet.add(rnId1); rnSet.add(rnId2);
        rnSet.add(rnId3); rnSet.add(rnId4);

        for (int i = 0; i < args.length; i++) {
            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);
            cs.approvePlan(42, null, SerialVersion.CURRENT);
            cs.executePlan(42, false, null, SerialVersion.CURRENT);
            replay(cs);

            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null));
            replay(shell);

            try {
                subObj.execute(args[i], shell);
                fail("IllegalArgumentException expected," +
                     " but required arg input");
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), expectedExceptionMessages[i]);
            }
        }
    }

    /* 14. Test case coverage for: PlanCommand.StopServiceSub. */

    @Test
    public void testStopServiceSubGetCommandSyntax() throws Exception {

        final PlanCommand.StopServiceSub subObj =
            new PlanCommand.StopServiceSub();
        final String expectedResult =
            "plan stop-service {-service <id> | -all-rns " +
            "[-zn <id> | -znname <name>] | " +
            "-all-ans [-zn <id> | -znname <name>] | " +
            "-zn <id> | -znname <name>}" +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testStopServiceSubGetCommandDescription() throws Exception {

        final PlanCommand.StopServiceSub subObj =
            new PlanCommand.StopServiceSub();
        final String expectedResult = "Stops the specified service(s).";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testStopServiceSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.StopServiceSub(), "stop-service");
    }

    @Test
    public void testStopSubExecuteRequiredArgs() throws Exception {

        final String command = "stop-service";
        final Map<String, String> argsMap = startStopServiceSubArgsMap;

        /* Missing both required args, -service, and -all-rns. */
        doExecuteRequiredArgs(
            new PlanCommand.StopServiceSub(), command, argsMap,
            new String[]  {"-service", "-all-rns", "-all-ans", "-zn",
                           "-znname"},
                           "-service|-all-rns|-all-ans|-zn|-znname");
    }

    @Test
    public void testStopSubExecuteInvalidCombo() throws Exception {

        final String cannotMixMsg1 =
            "Cannot use -service and -all-rns flags together";
        final String connotMixMsg2 =
            "Cannot use -zn or -znname flags with the -service flag";
        final String connotMixMsg3 =
            "Cannot use both the -zn and -znname flags";

        final String[][] errorMessages =
            { {connotMixMsg2, cannotMixMsg1, connotMixMsg2},
              {connotMixMsg3},
            };

        final Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.putAll(startStopServiceSubArgsMap);
        argsMap.putAll(startStopServiceWithZoneIdSubArgsMap);
        argsMap.putAll(startStopServiceWithZoneNameSubArgsMap);

        int replaceIndex1 = 1, replaceIndex2 = 3;

        final String[] firstFlags = {"-service", "-zn"};
        final String[] secondFlags = {"-znname", "-all-rns", "-zn"};

        final String[] allArgs = {"start-service",
                                  "replace-this-flag", "replace-this-flag",
                                  "replace-this-flag", "replace-this-flag"};

        final Topology topo = new Topology("TEST_TOPOLOGY");
        final RepGroup rg = new RepGroup();
        topo.add(rg);
        rg.add(new RepNode(new StorageNodeId(1)));

        /*
         * i=0, j=0: start-service -service -znname zn1
         * i=0, j=1: start-service -service -all-rns
         * i=0, j=2: start-service -service -zn zn1
         * i=1, j=0: start-service -zn -znname
         */
        for (int i = 0; i <= 1; i++) {
            for(int j = 0; j <= 2; j++) {
                if (i==1 && j==1) {
                    break;
                }
                String curFlag = firstFlags[i];
                String nextFlag = secondFlags[j];
                allArgs[replaceIndex1] = curFlag;
                allArgs[replaceIndex1 + 1] = argsMap.get(curFlag);
                allArgs[replaceIndex2] = nextFlag;
                allArgs[replaceIndex2 + 1] = argsMap.get(nextFlag);

                final PlanCommand.StopServiceSub subObj =
                    new PlanCommand.StopServiceSub();
                final ShellUsageException expectedException =
                    new ShellUsageException(errorMessages[i][j], subObj);

                final CommandShell shell = createMock(CommandShell.class);
                final CommandService cs = createMock(CommandService.class);

                /* Establish what is expected from each mock for this test */
                expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);
                replay(cs);

                expect(shell.getAdmin()).
                       andReturn(CommandServiceAPI.wrap(cs, null));
                replay(shell);

                /* Run the test and verify the results. */
                try {
                    subObj.execute(allArgs, shell);
                    fail("ShellUsageException expected, "
                          + "but required arg input");
                } catch (ShellUsageException e) {
                    assertEquals(expectedException.getMessage(),
                                 e.getMessage());
                }
            }
        }
    }

    @Test
    public void testStopSubExecuteExpectedBehavior() throws Exception {

        final String[][] args = {
            {"stop-service", "-all-rns", "-zn", "zn1"},
            {"stop-service", "-all-rns", "-zn", "zn2"},
            /* take the last zone id zn2 */
            {"stop-service", "-all-rns", "-zn", "zn1",
                "-zn", "zn3", "-zn", "zn2"},
            {"stop-service", "-all-rns", "-znname", "zn1"},
            {"stop-service", "-all-rns", "-znname", "zn2"},
            /* take the last zone name zn2 */
            {"stop-service", "-all-rns", "-znname", "zn1",
                "-znname", "zn3", "-znname", "zn2"},
            /* zone only */
            {"stop-service", "-zn", "zn1"},
            {"stop-service", "-znname", "zn2"},
            /* All RNs */
            {"stop-service", "-all-rns"}};

        final Set<RepNodeId> zn1RNs = new HashSet<RepNodeId>();
        final Set<RepNodeId> zn2RNs = new HashSet<RepNodeId>();
        final List<Set<RepNodeId>> expectedRnSets =
            new ArrayList<Set<RepNodeId>>();
        expectedRnSets.add(zn1RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn1RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn2RNs);
        expectedRnSets.add(zn1RNs);     /* -zn zn1 only command */
        expectedRnSets.add(zn2RNs);     /* -znname zn2 only command */

        final String expectedMessage =
            "Started plan 42. Use show plan -id 42 to check status." + eolt +
            "To wait for completion, use plan wait -id 42";

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;

        final Topology topo = new Topology("TEST_TOPOLOGY");
        Datacenter dc1 = Datacenter
            .newInstance(dcName1, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc1);
        Datacenter dc2 = Datacenter
            .newInstance(dcName2, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc2);
        StorageNode sn1 = new StorageNode(dc1, dummyHost1, dummyRegistyPort1);
        topo.add(sn1);
        StorageNode sn2 = new StorageNode(dc2, dummyHost2, dummyRegistyPort2);
        topo.add(sn2);
        RepGroup rg = new RepGroup();
        topo.add(rg);

        RepNodeId rnId1 = new RepNodeId(1, 1);
        RepNodeId rnId2 = new RepNodeId(1, 2);
        RepNodeId rnId3 = new RepNodeId(1, 3);
        RepNodeId rnId4 = new RepNodeId(1, 4);

        RepNode rn1 = new RepNode(sn1.getResourceId());
        RepNode rn2 = new RepNode(sn1.getResourceId());
        RepNode rn3 = new RepNode(sn2.getResourceId());
        RepNode rn4 = new RepNode(sn2.getResourceId());

        rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);

        zn1RNs.add(rnId1); zn1RNs.add(rnId2);
        zn2RNs.add(rnId3); zn2RNs.add(rnId4);

        for (int i = 0; i < args.length; i++) {

            CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);
            final Parameters params = createMock(Parameters.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).
                   andReturn(SerialVersion.CURRENT).anyTimes();
            expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(params);
            expect(params.getAdminIds(dc1.getResourceId(), topo)).
                    andReturn(Collections.<AdminId>emptySet()).anyTimes();
            expect(params.getAdminIds(dc2.getResourceId(), topo)).
                    andReturn(Collections.<AdminId>emptySet()).anyTimes();
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);

            if (i != args.length - 1) {
                expect(cs.createStopServicesPlan(isNull(String.class),
                   eq(expectedRnSets.get(i)), eq(NULL_AUTH),
                   eq(SerialVersion.CURRENT))).andStubReturn(42);
            } else {
                expect(cs.createStopAllRepNodesPlan(isNull(String.class),
                       eq(NULL_AUTH),
                       eq(SerialVersion.CURRENT))).andStubReturn(42);
            }

            cs.approvePlan(42, null, SerialVersion.CURRENT);
            expectLastCall().anyTimes();
            boolean force = args[i][args[i].length - 1].equals("-force");
            cs.executePlan(42, force, null, SerialVersion.CURRENT);
            expectLastCall().anyTimes();
            replay(cs);
            replay(params);

            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null)).anyTimes();
            expect(shell.getJson()).andStubReturn(false);
            replay(shell);

            final PlanCommand.StopServiceSub subObj =
                    new PlanCommand.StopServiceSub();
            final String result = subObj.execute(args[i], shell);
            assertEquals(result, expectedMessage);

            shell = createMock(CommandShell.class);
            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null)).anyTimes();
            expect(shell.getJson()).andStubReturn(true);
            replay(shell);
            checkPlanJson(args[i], shell, new PlanCommand.StopServiceSub());
        }
    }

    @Test
    public void testStopSubExecuteInvalidZone() throws Exception {
        final String[][] args = {
            {"stop-service", "-all-rns", "-zn", "zn2"},
            {"stop-service", "-all-rns", "-znname", "zn2"}};

        final String[] expectedExceptionMessages =
            {"The specified zone id does not exist",
             "The specified zone name does not exist"};

        final String dcName = "zn1";

        final Topology topo = new Topology("TEST_TOPOLOGY");
        Datacenter dc = Datacenter
            .newInstance(dcName, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc);

        for (int i = 0; i < args.length; i++) {

            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);

            cs.approvePlan(42, null, SerialVersion.CURRENT);
            cs.executePlan(42, false, null, SerialVersion.CURRENT);
            replay(cs);

            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null));
            replay(shell);

            try {
                final PlanCommand.StopServiceSub subObj =
                    new PlanCommand.StopServiceSub();
                subObj.execute(args[i], shell);
                fail("IllegalArgumentException expected," +
                     " but required arg input");
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), expectedExceptionMessages[i]);
            }
        }
    }

    @Test
    public void testStopSubExecuteInvalidAdmin() throws Exception {
        final String[][] args = {{"stop-service", "-service", "admin2"},
                                 {"stop-service", "-service", "admin2"}};

        final String[] expectedExceptionMessages =
            {"Unknown or unsupported service: admin2",
             "Unknown or unsupported service: admin2"};

        final Topology topo = new Topology("TEST_TOPOLOGY");

        for (int i = 0; i < args.length; i++) {

            final CommandShell shell = createMock(CommandShell.class);
            final CommandService cs = createMock(CommandService.class);
            final Parameters params = createMock(Parameters.class);

            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(params);
            expect(params.get(new AdminId(2))).andStubReturn(null);
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);

            cs.approvePlan(42, null, SerialVersion.CURRENT);
            cs.executePlan(42, false, null, SerialVersion.CURRENT);
            replay(cs);
            replay(params);

            expect(shell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(cs,null));
            replay(shell);

            try {
                final PlanCommand.StopServiceSub subObj =
                    new PlanCommand.StopServiceSub();
                subObj.execute(args[i], shell);
                fail("ShellException expected");
            } catch (ShellException e) {
                assertEquals(e.getMessage(), expectedExceptionMessages[i]);
            }
        }
    }

    /* 15. Test case coverage for: PlanCommand.PlanWaitSub. */

    @Test
    public void testPlanWaitSubGetCommandSyntax() throws Exception {

        final PlanCommand.PlanWaitSub subObj = new PlanCommand.PlanWaitSub();
        final String expectedResult =
              "plan wait -id <id> | -last [-seconds <timeout in seconds>] " +
              CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testPlanWaitSubGetCommandDescription() throws Exception {

        final PlanCommand.PlanWaitSub subObj = new PlanCommand.PlanWaitSub();
        final String expectedResult =
            "Waits for the specified plan to complete.  If the " +
            "optional timeout" + eolt + "is specified, wait that " +
            "long, otherwise wait indefinitely.  Use -last" + eolt +
            "to reference the most recently created plan.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testPlanWaitSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.PlanWaitSub(), "wait");
    }

    @Test
    public void testPlanWaitSubExecuteRequiredArgs() throws Exception {

        final String command = "wait";
        final Map<String, String> argsMap = planWaitSubArgsMap;

        /* Missing both required args, -id, and -last. */
        doExecuteRequiredArgs(
            new PlanCommand.PlanWaitSub(), command, argsMap,
            new String[]  {"-id", "-last"}, "-id");
    }

    /* 16. Test case coverage for: PlanCommand.RemoveDatacenterSub. */

    @Test
    public void testRemoveZoneSubGetCommandSyntax()
        throws Exception {

        testRemoveZoneSubGetCommandSyntax(
            new PlanCommand.RemoveZoneSub(), "zone");
    }

    @Test
    public void testRemoveDatacenterSubGetCommandSyntax()
        throws Exception {

        testRemoveZoneSubGetCommandSyntax(
            new PlanCommand.RemoveDatacenterSub(), "datacenter");
    }

    private void testRemoveZoneSubGetCommandSyntax(
        final PlanCommand.RemoveZoneSub subObj,
        final String term)
        throws Exception {

        final String expectedResult =
            "plan remove-" + term + " " +
            PlanCommand.RemoveDatacenterSub.ID_FLAG + " <id> | " +
            PlanCommand.RemoveDatacenterSub.NAME_FLAG + " <name>" +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveDatacenterSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            "Removes the specified zone from the store.";
        assertEquals(
            expectedResult + eol + eolt +
            "This command is deprecated and has been replaced by:" +
            eol + eolt + "plan remove-zone",
            new PlanCommand.RemoveDatacenterSub().getCommandDescription());
        assertEquals(
            expectedResult,
            new PlanCommand.RemoveZoneSub().getCommandDescription());

    }

    @Test
    public void testRemoveDatacenterSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.RemoveDatacenterSub(), "remove-datacenter");
        doExecuteUnknownArg(new PlanCommand.RemoveZoneSub(), "remove-zone");
    }

    @Test
    public void testRemoveDatacenterSubExecuteRequiredArgs() throws Exception {

        final String command = "wait";
        final Map<String, String> argsMap = removeDatacenterSubArgsMap;

        final PlanCommand.RemoveZoneSub[] cmdSubs = {
            new PlanCommand.RemoveDatacenterSub(),
            new PlanCommand.RemoveZoneSub()
        };
        for (PlanCommand.RemoveZoneSub cmdSub : cmdSubs) {

            /* Missing both required args, -zn, and -znname. */
            doExecuteRequiredArgs(
                cmdSub, command, argsMap,
                new String[] {PlanCommand.RemoveDatacenterSub.ID_FLAG,
                              PlanCommand.RemoveDatacenterSub.NAME_FLAG},
                PlanCommand.RemoveDatacenterSub.ID_FLAG + " | " +
                PlanCommand.RemoveDatacenterSub.NAME_FLAG);
        }
    }

    /* 17. Test case coverage for: PlanCommand.AddTableSub. */
    @Test
    public void testAddTableSubGetCommandSyntax() throws Exception {

        final PlanCommand.AddTableSub subObj =
            new PlanCommand.AddTableSub();
        final String expectedResult = "plan add-table " +
            PlanCommand.AddTableSub.TABLE_NAME_FLAG + " <name> "+
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveZoneSubExecuteDcDeprecation() throws Exception {
        doRemoveZoneSubExecute("",
                               "plan", "remove-zone", "-znname", "MyZone");
        doRemoveZoneSubExecute(PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-zone", "-dcname", "MyZone");
        doRemoveZoneSubExecute("",
                               "plan", "remove-zone", "-zn", "zn1");
        doRemoveZoneSubExecute(PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-zone", "-dc", "zn1");
        doRemoveZoneSubExecute(PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-zone", "-zn", "dc1");
        doRemoveZoneSubExecute(PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-zone", "-dc", "dc1");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation,
                               "plan", "remove-datacenter",
                               "-znname", "MyZone");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation +
                               PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-datacenter",
                               "-dcname", "MyZone");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation,
                               "plan", "remove-datacenter", "-zn", "zn1");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation +
                               PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-datacenter", "-dc", "zn1");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation +
                               PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-datacenter", "-zn", "dc1");
        doRemoveZoneSubExecute(RemoveDatacenterSub.dcCommandDeprecation +
                               PlanSubCommand.dcFlagsDeprecation,
                               "plan", "remove-datacenter", "-dc", "dc1");
    }

    @Test
    public void testAddTableSubGetCommandDescription()
        throws Exception {

        final PlanCommand.AddTableSub subObj =
            new PlanCommand.AddTableSub();
        final String expectedResult =
            "Add a new table to the store.  " + "The table name is an " +
            "optionally namespace" + eolt + "qualified dot-separated " +
            "name with the format" + eolt +
            "[ns:]tableName[.childTableName]*.  Use the " +
            "table create command to" + eolt + "create the named table." +
            "  Use \"table list -create\" to see the list of " +
            eolt + "tables that can be added.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    private void doRemoveZoneSubExecute(final String deprecation,
                                        final String... cmd)
        throws Exception {

        /* Establish mocks */
        final Datacenter dc = createMock(Datacenter.class);
        expect(dc.getName()).andStubReturn("MyZone");
        final DatacenterId dcId = new DatacenterId(1);
        expect(dc.getResourceId()).andStubReturn(dcId);
        replay(dc);

        final Topology topo = createMock(Topology.class);
        final DatacenterMap dcMap = new DatacenterMap(topo);
        dcMap.put(dc);
        expect(topo.getDatacenterMap()).andStubReturn(dcMap);

        /*
         * There seems to be no way to make Java 7 happy with this and be
         * typesafe
         */
        @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
        final IExpectationSetters setter =
            expect(topo.get((ResourceId) dcId))
            .andReturn((Component) dc).anyTimes();

        replay(topo);

        final CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(null, SerialVersion.CURRENT))
            .andStubReturn(topo);
        expect(cs.createRemoveDatacenterPlan(null, dcId, null,
                                             SerialVersion.CURRENT))
            .andStubReturn(42);
        cs.approvePlan(42, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(42, false, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        CommandShell shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(false);
        replay(shell);

        /* Run the test and verify the results. */
        assertEquals(
            "Command result",
            deprecation +
            "Started plan 42. Use show plan -id 42 to check status." + eolt +
            "To wait for completion, use plan wait -id 42",
            new PlanCommand().execute(cmd, shell));

        shell = createMock(CommandShell.class);
        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(true);
        replay(shell);
        checkPlanJson(cmd, shell);
    }

    @Test
    public void testAddTableSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.AddTableSub(), "add-table");
    }

    @Test
    public void testAddTableSubExecuteRequiredArgs() throws Exception {

        final String command = "add-table";
        final Map<String, String> argsMap = addTableSubArgsMap;
        final PlanCommand.AddTableSub cmdSub =
            new PlanCommand.AddTableSub();
        /* Missing the only required arg, -id. */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[]  {PlanCommand.AddTableSub.TABLE_NAME_FLAG},
                           PlanCommand.AddTableSub.TABLE_NAME_FLAG);
    }

    /* 18. Test case coverage for: PlanCommand.EvolveTableSub. */
    @Test
    public void testEvolveTableSubGetCommandSyntax() throws Exception {

        final PlanCommand.EvolveTableSub subObj =
            new PlanCommand.EvolveTableSub();
        final String expectedResult = "plan evolve-table " +
            PlanCommand.EvolveTableSub.TABLE_NAME_FLAG + " <name> " +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testEvolveTableSubGetCommandDescription()
        throws Exception {

        final PlanCommand.EvolveTableSub subObj =
            new PlanCommand.EvolveTableSub();
        final String expectedResult =
            "Evolve a table in the store.  The table name is an " +
            "optionally namespace" + eolt + "qualified " +
            "dot-separated name with the format " + eolt +
            "[ns:]tableName[.childTableName]*.  The named table must " +
            "have been" + eolt + "evolved using the \"table evolve\" " +
            "command.  Use \"table list -evolve\" " +
            "to " + eolt + "see the list of tables that can be evolved.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testEvolveTableSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.EvolveTableSub(), "evolve-table");
    }

    @Test
    public void testEvolveTableSubExecuteRequiredArgs() throws Exception {

        final String command = "evolve-table";
        final Map<String, String> argsMap = evolveTableSubArgsMap;
        final PlanCommand.EvolveTableSub cmdSub =
            new PlanCommand.EvolveTableSub();
        /* Missing the only required arg, -table. */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[]  {PlanCommand.EvolveTableSub.TABLE_NAME_FLAG},
                           PlanCommand.EvolveTableSub.TABLE_NAME_FLAG);
    }

    /* 19. Test case coverage for: PlanCommand.RemoveTableSub. */
    @Test
    public void testRemoveTableSubGetCommandSyntax() throws Exception {

        final PlanCommand.RemoveTableSub subObj =
            new PlanCommand.RemoveTableSub();
        final String expectedResult = "plan remove-table " +
            PlanCommand.RemoveTableSub.TABLE_NAME_FLAG + " <name> " +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveTableSubGetCommandDescription()
        throws Exception {

        final PlanCommand.RemoveTableSub subObj =
            new PlanCommand.RemoveTableSub();
        final String expectedResult =
            "Remove a table from the store.  The table name is an " +
            "optionally" + eolt + "namespace qualified dot-separated " +
            "name with the format" + eolt +
            "[ns:]tableName[.childTableName]*.  The named table must exist " +
            "and must" + eolt + "not have " +
            "any child tables.  Indexes on the table are automatically " +
            eolt + "removed.  Data stored in this table is also " +
            "removed.  Depending on " + eolt + "the indexes and amount of " +
            "data stored in the table this may be a " + eolt +
            "long-running plan.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testRemoveTableSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.RemoveTableSub(), "remove-table");
    }

    @Test
    public void testRemoveTableSubExecuteRequiredArgs() throws Exception {

        final String command = "remove-table";
        final Map<String, String> argsMap = removeTableSubArgsMap;
        final PlanCommand.RemoveTableSub cmdSub =
            new PlanCommand.RemoveTableSub();
        /* Missing the only required arg, -table. */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[]  {PlanCommand.RemoveTableSub.TABLE_NAME_FLAG},
                           PlanCommand.RemoveTableSub.TABLE_NAME_FLAG);
    }

    /* 20. Test case coverage for: PlanCommand.AddIndexSub. */
    @Test
    public void testAddIndexSubGetCommandSyntax() throws Exception {

        final PlanCommand.AddIndexSub subObj =
            new PlanCommand.AddIndexSub();
        final String expectedResult = "plan add-index " +
            PlanCommand.AddIndexSub.INDEX_NAME_FLAG + " <name> " +
            PlanCommand.AddIndexSub.TABLE_FLAG + " <name> " +
            "[" + PlanCommand.AddIndexSub.FIELD_FLAG + " <name>]* " +
            eolt  +
            "[" + PlanCommand.AddIndexSub.DESC_FLAG + " <description>]" +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testAddIndexSubGetCommandDescription()
        throws Exception {

        final PlanCommand.AddIndexSub subObj =
            new PlanCommand.AddIndexSub();
        final String expectedResult =
            "Add an index to a table in the store.  " +
            "The table name is an optionally" + eolt + "namespace " +
            "qualified dot-separated name with the format" + eolt +
            "[ns:]tableName[.childTableName]*.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testAddIndexSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.AddIndexSub(), "add-index");
    }

    @Test
    public void testAddIndexSubExecuteRequiredArgs() throws Exception {

        final String command = "add-index";
        final Map<String, String> argsMap = addIndexSubArgsMap;
        final PlanCommand.AddIndexSub cmdSub =
            new PlanCommand.AddIndexSub();

        /* Missing all required args. */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.AddIndexSub.INDEX_NAME_FLAG,
                          PlanCommand.AddIndexSub.TABLE_FLAG,
                          PlanCommand.AddIndexSub.FIELD_FLAG},
                          PlanCommand.AddIndexSub.INDEX_NAME_FLAG);

        /* Missing only the "-name" required arg */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.AddIndexSub.INDEX_NAME_FLAG},
                          PlanCommand.AddIndexSub.INDEX_NAME_FLAG);

        /* Missing only the "-table" required arg */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.AddIndexSub.TABLE_FLAG},
                          PlanCommand.AddIndexSub.TABLE_FLAG);

        /* Missing only the "-field" required arg */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.AddIndexSub.FIELD_FLAG},
                          PlanCommand.AddIndexSub.FIELD_FLAG);
    }

    /* 21. Test case coverage for: PlanCommand.RemoveIndexSub. */
    @Test
    public void testRemoveIndexSubGetCommandSyntax() throws Exception {

        final PlanCommand.RemoveIndexSub subObj =
            new PlanCommand.RemoveIndexSub();
        final String expectedResult = "plan remove-index " +
            PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG + " <name> " +
            PlanCommand.RemoveIndexSub.TABLE_FLAG + " <name> " +
            PlanCommand.PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveIndexSubGetCommandDescription()
        throws Exception {

        final PlanCommand.RemoveIndexSub subObj =
            new PlanCommand.RemoveIndexSub();
        final String expectedResult =
            "Remove an index from a table.  The table name is an " +
            "optionally namespace" + eolt + "qualified " +
            "dot-separated name with the format " + eolt +
            "[ns:]tableName[.childTableName]*.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testRemoveIndexSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.RemoveIndexSub(), "remove-index");
    }

    @Test
    public void testRemoveIndexSubExecuteRequiredArgs() throws Exception {

        final String command = "remove-index";
        final Map<String, String> argsMap = removeIndexSubArgsMap;
        final PlanCommand.RemoveIndexSub cmdSub =
            new PlanCommand.RemoveIndexSub();

        /* Missing all required args. */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG,
                          PlanCommand.RemoveIndexSub.TABLE_FLAG},
                          PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG);

        /* Missing only the "-name" required arg */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG},
                          PlanCommand.RemoveIndexSub.INDEX_NAME_FLAG);

        /* Missing only the "-table" required arg */
        doExecuteRequiredArgs(cmdSub, command, argsMap,
            new String[] {PlanCommand.RemoveIndexSub.TABLE_FLAG},
                          PlanCommand.RemoveIndexSub.TABLE_FLAG);
    }

    /* 17. Test case coverage for: PlanCommand.CreateUserSub. */
    @Test
    public void testCreateUserSubGetCommandSyntax() throws Exception {
        final PlanCommand.CreateUserSub subObj =
                new PlanCommand.CreateUserSub();
        final String expectedResult = PlanCommand.CreateUserSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testCreateUserSubGetCommandDescription() throws Exception {
        final PlanCommand.CreateUserSub subObj =
                new PlanCommand.CreateUserSub();
        final String expectedResult = PlanCommand.CreateUserSub.COMMAND_DESC;
        assertEquals(expectedResult + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "execute \'CREATE USER\'",
                     subObj.getCommandDescription());
    }

    @Test
    public void testCreateUserSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.CreateUserSub(), "create-user");
    }

    @Test
    public void testCreateUserSubExecuteRequiredArgs() throws Exception {
        final String command = "create-user";
        final Map<String, String> argsMap = createUserSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.CreateUserSub(), command, argsMap,
            new String[]  {"-name"}, "-name");
    }

    /* 18. Test case coverage for: PlanCommand.CreateUserSub. */

    @Test
    public void testChangeUserSubGetCommandSyntax() throws Exception {
        final PlanCommand.ChangeUserSub subObj =
                new PlanCommand.ChangeUserSub();
        final String expectedResult = PlanCommand.ChangeUserSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testChangeUserSubGetCommandDescription() throws Exception {
        final PlanCommand.ChangeUserSub subObj =
                new PlanCommand.ChangeUserSub();
        final String expectedResult = PlanCommand.ChangeUserSub.COMMAND_DESC;
        assertEquals(expectedResult + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "execute \'ALTER USER\'",
                     subObj.getCommandDescription());
    }

    @Test
    public void testChangeUserSubExecuteRequiredArgs() throws Exception {
        final String command = "change-user";
        final Map<String, String> argsMap = changeUserSubArgsMap;

        /* Missing -name with all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.ChangeUserSub(), command, argsMap,
            new String[]  {"-name"}, "-name");
    }

    @Test
    public void testChangeUserSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.ChangeUserSub(), "change-user");
    }

    /* 19. Test case coverage for: PlanCommand.CreateUserSub. */

    @Test
    public void testDropUserSubGetCommandSyntax() throws Exception {
        final PlanCommand.DropUserSub subObj =
                new PlanCommand.DropUserSub();
        final String expectedResult = PlanCommand.DropUserSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testDropUserSubGetCommandDescription() throws Exception {
        final PlanCommand.DropUserSub subObj =
                new PlanCommand.DropUserSub();
        final String expectedResult = PlanCommand.DropUserSub.COMMAND_DESC;
        assertEquals(expectedResult + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "execute \'DROP USER\'",
                     subObj.getCommandDescription());
    }

    @Test
    public void testDropUserSubExecuteRequiredArgs() throws Exception {
        final String command = "drop-user";
        final Map<String, String> argsMap = dropUserSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.CreateUserSub(), command, argsMap,
            new String[]  {"-name"}, "-name");
    }

    @Test
    public void testDropUserSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.DropUserSub(), "drop-user");
    }

    /* 20. Test case coverage for: PlanCommand.GrantSub */
    @Test
    public void testGrantSubGetCommandSyntax() throws Exception {
        final PlanCommand.GrantSub subObj = new PlanCommand.GrantSub();
        final String expectedResult = PlanCommand.GrantSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testGrantSubGetCommandDescription() throws Exception {
        final PlanCommand.GrantSub subObj = new PlanCommand.GrantSub();
        final String expectedResult = PlanCommand.GrantSub.COMMAND_DESC;
        assertEquals(expectedResult + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "execute \'GRANT\'",
                     subObj.getCommandDescription());
    }

    @Test
    public void testGrantSubExecuteRequiredArgs() throws Exception {
        final String command = "grant";
        final Map<String, String> argsMap = grantSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(new PlanCommand.GrantSub(), command, argsMap,
                              new String[]{PlanCommand.GrantSub.USER_FLAG,
                                           PlanCommand.GrantSub.ROLE_FLAG},
                                           PlanCommand.GrantSub.USER_FLAG);

        /* Missing only the "-user" required arg */
        doExecuteRequiredArgs(new PlanCommand.GrantSub(), command, argsMap,
                              new String[]{PlanCommand.GrantSub.USER_FLAG},
                                           PlanCommand.GrantSub.USER_FLAG);

        /* Missing only the "-role" required arg */
        doExecuteRequiredArgs(new PlanCommand.GrantSub(), command, argsMap,
                              new String[]{PlanCommand.GrantSub.ROLE_FLAG},
                                           PlanCommand.GrantSub.ROLE_FLAG);
    }

    @Test
    public void testGrantSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.GrantSub(), "grant");
    }

    /* 21. Test case coverage for: PlanCommand.RevokeSub */
    @Test
    public void testRevokeSubGetCommandSyntax() throws Exception {
        final PlanCommand.RevokeSub subObj =
            new PlanCommand.RevokeSub();
        final String expectedResult = PlanCommand.RevokeSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRevokeSubGetCommandDescription() throws Exception {
        final PlanCommand.RevokeSub subObj =
            new PlanCommand.RevokeSub();
        final String expectedResult = PlanCommand.RevokeSub.COMMAND_DESC;
        assertEquals(expectedResult + eol + eolt +
                     "This command is deprecated and has been replaced by:" +
                     eol + eolt + "execute \'REVOKE\'",
                     subObj.getCommandDescription());
    }

    @Test
    public void testRevokeSubExecuteRequiredArgs() throws Exception {
        final String command = "revoke";
        final Map<String, String> argsMap = revokeSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(new PlanCommand.RevokeSub(), command, argsMap,
                              new String[]{PlanCommand.RevokeSub.USER_FLAG,
                                           PlanCommand.RevokeSub.ROLE_FLAG},
                                           PlanCommand.RevokeSub.USER_FLAG);

        /* Missing only the "-user" required arg */
        doExecuteRequiredArgs(new PlanCommand.RevokeSub(), command, argsMap,
                              new String[]{PlanCommand.RevokeSub.USER_FLAG},
                                           PlanCommand.RevokeSub.USER_FLAG);

        /* Missing only the "-role" required arg */
        doExecuteRequiredArgs(new PlanCommand.RevokeSub(), command, argsMap,
                              new String[]{PlanCommand.RevokeSub.ROLE_FLAG},
                                           PlanCommand.RevokeSub.ROLE_FLAG);
    }

    @Test
    public void testRevokeSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.RevokeSub(), "revoke");
    }

    /* 22. Test case coverage for: PlanCommand.NetworkRestoreSub */
    @Test
    public void testNetworkRestoreSubGetCommandSyntax() throws Exception {
        final PlanCommand.NetworkRestoreSub subObj =
            new PlanCommand.NetworkRestoreSub();
        final String expectedResult =
             PlanCommand.NetworkRestoreSub.COMMAND_SYNTAX;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testNetworkRestoreSubGetCommandDescription() throws Exception {
        final PlanCommand.NetworkRestoreSub subObj =
            new PlanCommand.NetworkRestoreSub();
        final String expectedResult =
            PlanCommand.NetworkRestoreSub.COMMAND_DESC;
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testNetworkRestoreSubExecuteRequiredArgs() throws Exception {
        final String command = "network-restore";
        final Map<String, String> argsMap = networkRestoreSubArgsMap;

        /* Missing all required args. */
        doExecuteRequiredArgs(
            new PlanCommand.NetworkRestoreSub(), command, argsMap,
            new String[]{PlanCommand.NetworkRestoreSub.SOURCE_ID_FLAG,
                         PlanCommand.NetworkRestoreSub.TARGET_ID_FLAG},
                         PlanCommand.NetworkRestoreSub.SOURCE_ID_FLAG);

        /* Missing only the "-from" required arg */
        doExecuteRequiredArgs(
            new PlanCommand.NetworkRestoreSub(), command, argsMap,
            new String[]{PlanCommand.NetworkRestoreSub.SOURCE_ID_FLAG},
            PlanCommand.NetworkRestoreSub.SOURCE_ID_FLAG);

        /* Missing only the "-to" required arg */
        doExecuteRequiredArgs(
            new PlanCommand.NetworkRestoreSub(), command, argsMap,
            new String[]{PlanCommand.NetworkRestoreSub.TARGET_ID_FLAG},
            PlanCommand.NetworkRestoreSub.TARGET_ID_FLAG);
    }

    @Test
    public void testNetworkRestoreSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new PlanCommand.NetworkRestoreSub(), "network-restore");
    }

    private void executeArgs(String[] args,
                             ShellUsageException expected,
                             PlanSubCommand subObj,
                             String message)
        throws Exception {
        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but wasn't encountered");
        } catch (ShellUsageException e) {
            assertEquals(message + expected.getMessage(), e.getMessage());
            doVerification(shell, cs);
        }
    }

    @Test
    public void testVerifyDataSubExecuteInvalidArgs() throws Exception {
        final PlanCommand.VerifyDataSub subObj =
            new PlanCommand.VerifyDataSub();
        final ShellUsageException noValueException =
            new ShellUsageException(" must be followed by enable or disable",
                                    subObj);

        final String[] enableFlagsArray =
            { "-verify-log", "-verify-btree", "-index", "-datarecord",
              "-show-corrupt-files" };
        int replaceIndex = 1;
        final String[] args =
            { "verify-data", "replace-this-flag", "-all-rns" };
        for (String cur : enableFlagsArray) {
            args[replaceIndex] = cur;
            executeArgs(args, noValueException, subObj, cur);

        }

        final String[] numFlagArray =
            { "-log-read-delay", "-btree-batch-delay" };
        final ShellUsageException negativeNumException =
            new ShellUsageException(subObj.negativeDelayError, subObj);
        final String[] numArgs = { "verify-data", "replace-this-flag",
                                   "replace-this-flag", "-all-rns" };
        for (String cur : numFlagArray) {
            numArgs[replaceIndex] = cur;
            numArgs[replaceIndex + 1] = "-1";

            executeArgs(numArgs, negativeNumException, subObj, "");

        }

        final ShellUsageException wrongTimeFormatException =
            new ShellUsageException("-valid-time must be followed by a valid " +
                "time with a valid unit.", subObj);
        final String[] wrongFormats = {"minutes", "10", "10minutes",
                                      "10 sec", "10*minutes"};
        final String[] timeArgs = { "verify-data", "-valid-time",
                                   "replace-this-flag", "-all-rns" };
        replaceIndex = 2;
        for (String cur : wrongFormats) {
            timeArgs[replaceIndex] = cur;
            executeArgs(timeArgs, wrongTimeFormatException, subObj, "");
        }

        /* If the -valid-time argument is valid, plan should not throw
         * wrongTimeFormatException but negativeNumException. */
        final String[] validFormats = {"10-minutes", "10 minutes", "10_minutes"};
        final String[] validTimeArgs = { "verify-data", "-valid-time",
                                    "replace-this-flag",
                                    "-btree-batch-delay", "-1", "-all-rns" };
        replaceIndex = 2;
        for (String cur : validFormats) {
            validTimeArgs[replaceIndex] = cur;
            executeArgs(validTimeArgs, negativeNumException, subObj, "");
        }
    }

    @Test
    public void testVerifyDataSubExecuteRequiredArgs() throws Exception {
        final String command = "verify-data";

        final Map<String, String> argsMap = verifyDataSubArgMap;
        doExecuteRequiredArgs(new PlanCommand.VerifyDataSub(), command, argsMap,
                              new String[] { "-service", "-all-rns",
                                             "-all-admins", "-all-services",
                                             "-zn", "-znname", "dc",
                                             "-dcname" });

    }

    @Test
    public void testVerifyDataSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(new PlanCommand.VerifyDataSub(), "verify-data");
    }

    @Test
    public void testVerifyDataSubExecuteInvalidCombo() throws Exception {
        final PlanCommand.VerifyDataSub subObj =
            new PlanCommand.VerifyDataSub();
        final ShellUsageException btreeComboException =
            new ShellUsageException(subObj.btreeComboError, subObj);

        final String[] btreeFlagsArray =
            { "-btree-batch-delay", "-index", "-datarecord" };
        int replaceIndex = 3;
        final String[] btreeArgs =
            { "verify-data", "-verify-btree", "disable", "replace-this-flag",
              "replace-this-value", "-all-rns" };

        for (String curFlag : btreeFlagsArray) {
            btreeArgs[replaceIndex] = curFlag;
            btreeArgs[replaceIndex + 1] = verifyDataSubArgMap.get(curFlag);

            executeArgs(btreeArgs, btreeComboException, subObj, "");

        }

        final ShellUsageException logComboException =
            new ShellUsageException(subObj.logComboError, subObj);

        final String[] logArgs =
            { "verify-data", "-verify-log", "disable", "-log-read-delay", "0" };
        executeArgs(logArgs, logComboException, subObj, "");

        final ShellUsageException allFlagException =
            new ShellUsageException(subObj.serviceAllError, subObj);
        final ShellUsageException dcFlagException =
            new ShellUsageException(subObj.serviceDcError, subObj);
        ShellUsageException expectedException;

        final Map<String, String> argsMap = verifyDataSubArgMap;
        final int replaceIndx = 3;
        final String[] flagsArray =
            { "-all-rns", "-all-admins", "-all-services", "-zn", "-znname",
              "-dc", "-dcname" };
        final String[] allArgs =
            { "verify-data", "-service", argsMap.get("-service"),
              "replace-this-flag" };
        final String[] dcArgs =
            { "verify-data", "-service", argsMap.get("-service"),
              "replace-this-flag", "replace-this-value" };
        String[] args;

        for (final String curFlag : flagsArray) {
            if (curFlag.startsWith("-zn") || curFlag.startsWith("-dc")) {
                expectedException = dcFlagException;
                args = dcArgs;
                args[replaceIndx] = curFlag;
                args[replaceIndx + 1] = argsMap.get(curFlag);
            } else {
                expectedException = allFlagException;
                args = allArgs;
                args[replaceIndx] = curFlag;
            }

            executeArgs(args, expectedException, subObj, "");
        }

        final ShellUsageException allComboException =
            new ShellUsageException(subObj.incompatibleAllError, subObj);

        final String[] allFlags =
            { "-all-rns", "-all-admins", "-all-services" };
        final String[] allComboArgs =
            { "verify-data", "replace-this-flag", "replace-this-flag" };
        replaceIndex = 1;
        for (int i = 0; i < allFlags.length; i++) {
            for (int j = i + 1; j < allFlags.length; j++) {
                allComboArgs[replaceIndex] = allFlags[i];
                allComboArgs[replaceIndex + 1] = allFlags[j];

                executeArgs(allComboArgs, allComboException, subObj, "");
            }
        }

        final ShellUsageException noVerifyException =
            new ShellUsageException(subObj.logOrBtreeError, subObj);

        final String[] noVerifyArgs =
            { "verify-data", "-verify-log", "disable", "-verify-btree",
              "disable", "-all-rns" };

        executeArgs(noVerifyArgs, noVerifyException, subObj, "");

    }

    /* 23. Test case coverage for: PlanCommand.EnableRequestType. */

    @Test
    public void testEnableRequestTypeSubGetCommandSyntax() throws Exception {

        final EnableRequestsSub subObj = new EnableRequestsSub();
        final String expectedResult =
            EnableRequestsSub.COMMAND_SYNTAX + PlanSubCommand.genericFlags;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testEnableRequestTypeSubGetCommandDescription()
        throws Exception {

        final EnableRequestsSub subObj = new EnableRequestsSub();
        final String expectedResult = EnableRequestsSub.COMMAND_DESC;
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testEnableRequestsSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
            new EnableRequestsSub(), EnableRequestsSub.COMMAND_NAME);
    }

    @Test
    public void testEnableRequestsSubExecuteRequiredArgs() throws Exception {

        final String command = EnableRequestsSub.COMMAND_NAME;
        final Map<String, String> argsMap = enableRequestsSubArgsMap;

        /*
         * shard == null && store is FALSE, -request-type NOT input
         */
        doExecuteRequiredArgs(
            new EnableRequestsSub(), command, argsMap,
            new String[]
            { EnableRequestsSub.REQUEST_TYPE, EnableRequestsSub.STORE_FLAG,
              EnableRequestsSub.TARGET_SHARDS_FLAG });

        /*
         * shard == null && store is FALSE, -request-type input
         */
        doExecuteRequiredArgs(
            new EnableRequestsSub(), command, argsMap,
            new String[]
            { EnableRequestsSub.STORE_FLAG,
              EnableRequestsSub.TARGET_SHARDS_FLAG });

        /*
         * shard != null && store is FALSE, -request-type NOT input
         */
        doExecuteRequiredArgs(
            new EnableRequestsSub(), command, argsMap,
            new String[]
            { EnableRequestsSub.REQUEST_TYPE, EnableRequestsSub.STORE_FLAG },
            EnableRequestsSub.REQUEST_TYPE);
    }

    /**
     * Verify the expected exception is thrown when the -shard flag is
     * combined with -store flag.
     */
    @Test
    public void testEnableRequestsSubExecuteInvalidFlagCombo()
        throws Exception {

        final EnableRequestsSub subObj = new EnableRequestsSub();
        final ShellUsageException conflictFlagsException =
            new ShellUsageException(
                EnableRequestsSub.CONFLICT_FLAGS_ERROR, subObj);
        final Map<String, String> argsMap = enableRequestsSubArgsMap;
        final String[] conflictArgs =
            { EnableRequestsSub.COMMAND_NAME,
              EnableRequestsSub.STORE_FLAG,
              EnableRequestsSub.TARGET_SHARDS_FLAG,
              argsMap.get(EnableRequestsSub.TARGET_SHARDS_FLAG),
              EnableRequestsSub.REQUEST_TYPE,
              "none"};

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs,
                                                                  null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(conflictArgs, shell);
            fail("ShellUsageException expected, but wasn't encountered");
        } catch (ShellUsageException e) {
            assertEquals(conflictFlagsException.getMessage(), e.getMessage());
            doVerification(shell, cs);
        }
    }

    @Test
    public void testEnableRequestsSubExecuteNoRequestType() throws Exception {

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);
        final EnableRequestsSub subObj = new EnableRequestsSub();
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " +
                EnableRequestsSub.REQUEST_TYPE + " requires an argument",
                subObj);
        final String[] args =
            { EnableRequestsSub.COMMAND_NAME,
              EnableRequestsSub.STORE_FLAG,
              EnableRequestsSub.REQUEST_TYPE};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but wasn't encountered");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
            doVerification(shell, cs);
        }
    }
    
    /* 24. Test case coverage for: PlanCommand.SetTableLimitsSub. */
    @Test
    public void testSetTableLimitsSubGetCommandSyntax() throws Exception {
        final PlanCommand.SetTableLimitsSub subObj = 
                new PlanCommand.SetTableLimitsSub();
        final String expectedResult = "plan set-table-limit " +
                PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG +
                " <name> " +
                "[" + PlanCommand.SetTableLimitsSub.TABLE_NS_FLAG +
                " <name>] " + eolt +
                "[" + PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG +
                " <KB/sec>|none] " + eolt +
                "[" + PlanCommand.SetTableLimitsSub.WRITE_LIMIT_FLAG +
                " <KB/sec>|none] " + eolt +
                "[" + PlanCommand.SetTableLimitsSub.SIZE_LIMIT_FLAG +
                " <GB>|none]" + eolt +
                "[" + PlanCommand.SetTableLimitsSub.INDEX_LIMIT_FLAG +
                " <# indexes>|none]" + eolt +
                "[" + PlanCommand.SetTableLimitsSub.CHILD_TABLE_LIMIT_FLAG +
                "<# tables>|none]" + eolt +
                "[" + PlanCommand.SetTableLimitsSub.INDEX_KEY_SIZE_LIMIT_FLAG +
                " <bytes>|none]" +
                PlanCommand.SetTableLimitsSub.genericFlags;

        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testSetTableLimitsSubGetCommandDescription() throws Exception {
        final PlanCommand.SetTableLimitsSub subObj = 
                new PlanCommand.SetTableLimitsSub();
        final String expectedResult = PlanCommand.SetTableLimitsSub.COMMAND_DESC;

        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testSetTableLimitsSubIsHidden() throws Exception {
        final PlanCommand.SetTableLimitsSub subObj = 
        new PlanCommand.SetTableLimitsSub();

        assertTrue(subObj.isHidden());
    }

    @Test
    public void testSetTableLimitsSubExecuteUnknownArg() throws Exception {
        doExecuteUnknownArg(
                new PlanCommand.SetTableLimitsSub(), "set-table-limits");
    }
    
    @Test
    public void testSetTableLimitsSubExecuteRequiredArgs() throws Exception {
        final String command = "set-table-limits";
        final Map<String, String> argsMap = setTableLimitsSubArgsMap;
        final PlanCommand.SetTableLimitsSub subObj =
                new PlanCommand.SetTableLimitsSub();

        /* Missing the TABLE name required arg, -name. */
        doExecuteRequiredArgs(subObj, command, argsMap,
                new String[] { PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG},
                PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG);

        /* Missing all limit args, at least one of them is required */
        doExecuteRequiredArgs(subObj, command, argsMap,
                new String[] { PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.WRITE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.SIZE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.INDEX_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.CHILD_TABLE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.INDEX_KEY_SIZE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG },
                "at least one limit must be specified");

        /* Missing the TABLE name required arg, -name. */
        doExecuteRequiredArgs(subObj, command, argsMap,
                new String[] {PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG,
                    PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.WRITE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.SIZE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.INDEX_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.CHILD_TABLE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.INDEX_KEY_SIZE_LIMIT_FLAG,
                    PlanCommand.SetTableLimitsSub.READ_LIMIT_FLAG },
                    PlanCommand.SetTableLimitsSub.TABLE_NAME_FLAG);
    }

    @Test
    public void testSetTableLimitsSubExecute() throws Exception {
        int planId = 23;
        final String[] args = {
                "set-table-limits",
                "-name", "aTable",
                "-read-limit", "2000" };
        TableLimits newLimits = new TableLimits(2000,
                TableLimits.NO_CHANGE,
                TableLimits.NO_CHANGE,
                TableLimits.NO_CHANGE,
                TableLimits.NO_CHANGE,
                TableLimits.NO_CHANGE);
        final PlanCommand.SetTableLimitsSub subObj =
                new PlanCommand.SetTableLimitsSub();
        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.createTableLimitPlan(
                null,
                null,
                "aTable",
                newLimits,
                null,
                SerialVersion.CURRENT)).andStubReturn(planId);
        cs.approvePlan(planId, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        cs.executePlan(planId, false, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);
        expect(shell.getAdmin()).
                andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(true);
        replay(shell);

        /* Run the test and verify the results. */
        ShellCommandResult result = subObj.executeJsonOutput(args, shell);
        assertEquals(result.getOperation(), "plan set-table-limits");
        assertEquals(result.getReturnCode(), 5000);
        assertEquals(result.getDescription(), "Operation ends successfully");
    }

    /* Convenience methods shared by the sub-command classes under test. */

    /**
     * Submits an argument unknown to the execute method of the given
     * sub-command class and verifies that the expected ShellUsageException
     * results.
     */
    private void doExecuteUnknownArg(final PlanCommand.PlanSubCommand cmdSub,
                                     final String command)
        throws Exception {

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        final String arg = "UNKNOWN_ARG";
        ShellUsageException expectedException =
            new ShellUsageException("Invalid argument: " + arg, cmdSub);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(false);
        if (cmdSub instanceof PlanCommand.InterruptCancelSub ||
                   cmdSub instanceof PlanCommand.PlanWaitSub) {
            expectedException =
                new ShellUsageException("Unknown argument: " + arg, cmdSub);
            /* Void method: unknownArgument */
            shell.unknownArgument(arg, cmdSub);
            expectLastCall().andThrow(expectedException);
        }
        replay(shell);

        /* Run the test and verify the results. */
        try {
            cmdSub.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
            doVerification(shell, cs);
        }
    }

    /**
     * Submits one or more valid arguments to the execute method of the given
     * sub-command class, but excludes the given required arguments, and then
     * verifies that the expected ShellUsageException with message based on the
     * given requiredArgName results.
     */
    private void doExecuteRequiredArgs(final PlanCommand.PlanSubCommand cmdSub,
                                       final String command,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs,
                                       final String requiredArgName)
        throws Exception {

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        final String[] args =
            getArgsArrayMinusRequiredArgs(command, argsMap, requiredArgs);
        /* check whether multiple incompatible options are given. */
        boolean allAdmin, allRN, allSN, security, allAN, global;
        String serviceName = null;
        boolean isIncompatibleAllError = false;
        allAdmin = allRN = allSN  = security = allAN = global = false;

        for (int i = 1; i < args.length; i++) {
            final String arg = args[i];
            if ("-service".equals(arg)) {
                serviceName = Shell.nextArg(args, i++, cmdSub);
            } else if ("-all-rns".equals(arg)) {
                allRN = true;
            } else if ("-all-admins".equals(arg)) {
                allAdmin = true;
            } else if ("-all-sns".equals(arg)) {
                allSN = true;
            } else if ("-security".equals(arg)) {
                security = true;
            } else if ("-all-ans".equals(arg)) {
                allAN = true;
            } else if ("-global".equals(arg)) {
                global = true;
            }
        }

        if (serviceName == null) {
            if (allAdmin || allRN || allSN || security || allAN || global) {
                /* check whether multiple incompatible options are given. */
                if (((allAdmin ? 1 : 0) +
                     (allRN ? 1 : 0) +
                     (allSN ? 1 : 0) +
                     (allAN ? 1 : 0) +
                     (global ? 1 : 0) +
                     (security ? 1 : 0)) > 1) {
                    isIncompatibleAllError = true;
                }
            }
        }

        String expectedMsg = null;

        if (isIncompatibleAllError) {
            /*
             * expectedMsd is assigned diffrent value when multiple
             * incompatible options are given
             */
            expectedMsg = "Invalid argument combination: Only one of the " +
            "flags -all-rns, -all-sns, -all-admins, -all-ans, -global and " +
            "-security may be used.";
        } else {
            expectedMsg = "Missing required argument" +
            ((requiredArgName != null) ? " (" + requiredArgName + ")" : "") +
            " for command: " + args[0];
        }
        final ShellUsageException expectedException =
            new ShellUsageException(expectedMsg, cmdSub);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));

        expect(shell.getJson()).andStubReturn(false);

        /* Void method: requiredArg */
        shell.requiredArg(requiredArgName, cmdSub);
        expectLastCall().andThrow(expectedException).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            cmdSub.execute(args, shell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
            /*
             * Run doVerification when multiple incompatible options are
             * not given
             */
            if (!isIncompatibleAllError) {
                doVerification(shell, cs);
            }
        }
    }

    private void doExecuteRequiredArgs(final PlanCommand.PlanSubCommand cmdSub,
                                       final String command,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs)
        throws Exception {

        doExecuteRequiredArgs(cmdSub, command, argsMap, requiredArgs, null);
    }

    /**
     * Constructs and returns a String array that can be specified as the args
     * parameter of the execute method of one of the sub-command classes under
     * test; where the array returned is intended to contain the desired
     * sub-command to execute, along with a set of valid arguments in which one
     * or more of the arguments required by the sub-command are excluded.
     *
     * Thus, the first element of the returned array is the name of the
     * sub-command to execute, and the remaining elements are the command's
     * associated arguments; where those elements include all name/value pairs
     * or flags from the given argsMap, except for those map entries with names
     * referenced in the given requiredArgs parameter.
     */
    private String[] getArgsArrayMinusRequiredArgs(
                         final String command,
                         final Map<String, String> argsMap,
                         final String[] requiredArgs) {

        final List<String> requiredArgsList = Arrays.asList(requiredArgs);
        final List<String> argsList = new ArrayList<String>();
        argsList.add(command);
        /* If "-params" is a key, it must be last in the return array. */
        final String paramsFlag = "-params";
        boolean setParamsLast = false;
        for (Map.Entry<String, String> argsPair : argsMap.entrySet()) {
            final String argName = argsPair.getKey();
            final String argVal = argsPair.getValue();
            if (requiredArgsList.contains(argName)) {
                continue;
            }
            if (argName.equals(argVal)) {
                if (argName.equals(paramsFlag)) {
                    setParamsLast = true;
                    continue;
                }
                argsList.add(argName);
                continue;
            }
            argsList.add(argName);
            argsList.add(argVal);
        }
        if (setParamsLast) {
            argsList.add(paramsFlag);
        }
        return argsList.toArray(new String[argsList.size()]);
    }

    /**
     * Submits an argument with an invalid value to the execute method of the
     * given sub-command class and verifies that the expected
     * ShellUsageException results.
     */
    private void doExecuteInvalidValue(final PlanCommand.PlanSubCommand cmdSub,
                                       final String command,
                                       final String arg,
                                       final String exceptionMessage)
        throws Exception {

        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        final String value = "INVALID_VALUE";
        final ShellUsageException expectedException =
            new ShellUsageException(exceptionMessage + value, cmdSub);
        final String[] args = {command, arg, value};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(false);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            cmdSub.execute(args, shell);
            fail("ShellUsageException expected");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
            doVerification(shell, cs);
        }
    }

    private void checkPlanJson(String[] cmd,
                               Shell shell,
                               ShellCommand command)
        throws Exception {
        final ShellCommandResult scr =
            command.executeJsonOutput(cmd, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode returnValue = scr.getReturnValue();
        assertEquals(returnValue.get("planId").asInt(), 42);
    }

    private void checkPlanJson(String[] cmd,
                               Shell shell)
        throws Exception {
        checkPlanJson(cmd, shell, new PlanCommand());
    }
}
