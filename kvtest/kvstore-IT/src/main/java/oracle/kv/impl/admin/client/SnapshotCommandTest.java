/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot.SnapResult;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/**
 * Verifies the functionality and error paths of the SnapshotCommand class.
 */
public class SnapshotCommandTest extends TestBase {

    private final ParameterMap pMap = new ParameterMap();
    private final AdminParams adminParams = new AdminParams(pMap);

    private static final int ADMIN_ID = 99;
    private static final AuthContext NULL_AUTH = null;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        adminParams.setAdminId(new AdminId(ADMIN_ID));
    }

    /* SUB-CLASS TEST CASES */

    /* 1. Test case coverage for: SnapshotCommand.CreateSnapshotSub. */

    @Test
    public void testCreateSnapshotSubGetCommandSyntax() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String expectedResult =
            "snapshot create -name <name> [-zn <id> | -znname <name>] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testCreateSnapshotSubGetCommandDescription() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String expectedResult =
                "Creates a new snapshot using the specified name as " +
                "the prefix. If a zone with the specified id or name is " +
                "specified then the command applies to all the SNs " +
                "executing in that zone. Snapshot of configurations will " +
                "backup for related SNs in zones";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testCreateSnapshotSubRequiredArgsMissing() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String[] args = {"create"};
        String requiredArgName = "-name";
        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        /* Void method: requiredArg */
        String expectedMsg = "Missing required argument" +
            (" + requiredArgName + ") +
            " for command: " + args[0];
        final ShellUsageException expectedException =
            new ShellUsageException(expectedMsg, subObj);
        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        shell.requiredArg(requiredArgName, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testCreateSnapshotRequiredArgsError() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String[][] args =
            {{"create", "-name", "test", "-zn", "zn11"},
             {"create", "-name", "test", "-znname", "error" }};
        Datacenter dc1 = Datacenter
                .newInstance("zn1", 1, DatacenterType.PRIMARY, false, false);
        final Topology topo = new Topology("TEST_TOPOLOGY");
        topo.add(dc1);
        PrintStream ps = new PrintStream(new File("test.txt"));
        String errorMsg = "id";
        for (int i = 0; i < args.length; i++) {
            CommandShell shell = createMock(CommandShell.class);
            CommandService cs = createMock(CommandService.class);
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(topo);
            replay(cs);

            expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
            expect(shell.getVerbose()).andReturn(true);
            expect(shell.getJson()).andStubReturn(false);
            expect(shell.getOutput()).andReturn(ps);
            replay(shell);
            if (i == 1) {
                errorMsg = "name";
            }
            try {
                subObj.execute(args[i], shell);
                fail("expected ShellUsageException");
            } catch (ShellUsageException sue) {
                assertThat("incorrect zone", sue.getMessage(), containsString(
                    String.format("zone %s does not exist", errorMsg)));
            }
        }
    }

    @Test
    public void testCreateSnapshotSubExpectedBehavior() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String[][] allArgs = {{"create", "-name", "test1"},
                                {"create", "-name", "test2", "-znname", "zn1"},
                                {"create", "-name", "test3", "-zn", "zn1"},
                                {"create", "-name", "test4", "-znname", "zn2"},
                                {"create", "-name", "test5", "-zn", "zn2"}};

        final AdminId adminId = new AdminId(1);
        final Set<AdminId> adminSet = new HashSet<AdminId>();
        adminSet.add(adminId);

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;
        final Topology topo = new Topology("TEST_TOPOLOGY");
        final SnapResultSummary snapResults = new SnapResultSummary(
            new ArrayList<SnapResult>(), new ArrayList<SnapResult>(),
            true, true, new ArrayList<SnapResult>(),
            new ArrayList<SnapResult>());
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

        RepNode rn1 = new RepNode(sn1.getResourceId());
        RepNode rn2 = new RepNode(sn1.getResourceId());
        RepNode rn3 = new RepNode(sn2.getResourceId());
        RepNode rn4 = new RepNode(sn2.getResourceId());

        rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);
        PrintStream ps = new PrintStream(new File("test.txt"));
        try {
            /**
             * i = 0 : Create a snapshot without zone flag.
             *         Results in 5 operations: 4 operations for repnodes rn1-4
             *         and 1 operation for Admin1.
             * i = 1 and 2 : Create a snapshot using zone flag. Zone is zn1.
             *         Results in 3 operations. 2 operations for repnodes rn1-2
             *         and 1 operation for Admin1.
             * i = 3 and 4 : Create a snapshot using zone flag. Zone is zn2.
             *         Results in 2 operations. 2 operations for repnodes rn3-4
             *         There is no admin in the storage node sn2.
             */
            for (int i = 0; i < allArgs.length; i++) {
                CommandShell shell = createMock(CommandShell.class);
                CommandService cs = createMock(CommandService.class);
                Parameters params = createMock(Parameters.class);
                AdminParams adParams = createMock(AdminParams.class);
                /* Establish what is expected from each mock for this test */
                expect(cs.getSerialVersion()).
                    andReturn(SerialVersion.CURRENT).anyTimes();
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
                expect(cs.executeSnapshotOperation(isA(SnapshotOperation.class),
                isA(String.class), i == 0 ? isNull(DatacenterId.class) :
                isA(DatacenterId.class), eq(NULL_AUTH),
                eq(SerialVersion.CURRENT))).andStubReturn(snapResults);
                replay(cs);

                expect(params.getAdminIds()).andReturn(adminSet);
                expect(params.get(adminId)).andReturn(adParams);
                replay(params);

                expect(adParams.getStorageNodeId()).
                    andReturn(sn1.getResourceId());
                replay(adParams);

                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null));
                expect(shell.getVerbose()).andReturn(true);
                expect(shell.getJson()).andStubReturn(false);
                expect(shell.getOutput()).andReturn(ps);
                replay(shell);
                String[] args = allArgs[i];
                subObj.execute(args, shell);

                shell = createMock(CommandShell.class);
                cs = createMock(CommandService.class);
                params = createMock(Parameters.class);
                adParams = createMock(AdminParams.class);

                expect(cs.getSerialVersion()).
                    andReturn(SerialVersion.CURRENT).anyTimes();
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
                expect(cs.executeSnapshotOperation(isA(SnapshotOperation.class),
                isA(String.class), i == 0 ? isNull(DatacenterId.class) :
                isA(DatacenterId.class), eq(NULL_AUTH),
                eq(SerialVersion.CURRENT))).andStubReturn(snapResults);
                replay(cs);

                expect(params.getAdminIds()).andReturn(adminSet);
                expect(params.get(adminId)).andReturn(adParams);
                replay(params);

                expect(adParams.getStorageNodeId()).
                    andReturn(sn1.getResourceId());
                replay(adParams);

                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null));
                expect(shell.getVerbose()).andReturn(true);
                expect(shell.getJson()).andStubReturn(true);
                expect(shell.getOutput()).andReturn(ps);
                replay(shell);

                final ShellCommandResult scr =
                    subObj.executeJsonOutput(allArgs[i], shell);
                final ObjectNode retValue = scr.getReturnValue();
                assertThat(retValue.get("snapshotName").asText(),
                           containsString("test"));
                assertEquals(scr.getOperation(), "snapshot operation");
                assertEquals(scr.getReturnCode(),
                             ErrorMessage.NOSQL_5000.getValue());
            }
        } catch (ShellUsageException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateSnapshotReturnValueAndCode() throws Exception {
        final SnapshotCommand.CreateSnapshotSub subObj =
            new SnapshotCommand.CreateSnapshotSub();
        final String [][] allArgs = {
             {"create", "-name", "test1"},
             {"create", "-name", "test2", "-znname", "zn1"},
             {"create", "-name", "test3", "-zn", "zn1"},
             {"create", "-name", "test4", "-znname", "zn2"},
             {"create", "-name", "test5", "-zn", "zn2"}
        };

        final AdminId adminId = new AdminId(1);
        final Set<AdminId> adminSet = new HashSet<AdminId>();
        adminSet.add(adminId);

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;
        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;
        final String dummyHost3 = "dummy3.us.oracle.com";
        final int dummyRegistyPort3 = 5003;
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
        StorageNode sn3 = new StorageNode(dc2, dummyHost3, dummyRegistyPort3);
        topo.add(sn3);

        RepGroup rg1 = new RepGroup();
        topo.add(rg1);
        RepNode rg1rn1 = new RepNode(sn1.getResourceId());
        RepNode rg2rn1 = new RepNode(sn2.getResourceId());
        RepNode rg3rn1 = new RepNode(sn3.getResourceId());
        rg1.add(rg1rn1);
        rg1.add(rg2rn1);
        rg1.add(rg3rn1);

        RepGroup rg2 = new RepGroup();
        topo.add(rg2);
        RepNode rg1rn2 = new RepNode(sn1.getResourceId());
        RepNode rg2rn2 = new RepNode(sn2.getResourceId());
        RepNode rg3rn2 = new RepNode(sn3.getResourceId());
        rg2.add(rg1rn2);
        rg2.add(rg2rn2);
        rg2.add(rg3rn2);

        RepGroup rg3 = new RepGroup();
        topo.add(rg3);
        RepNode rg1rn3 = new RepNode(sn1.getResourceId());
        RepNode rg2rn3 = new RepNode(sn2.getResourceId());
        RepNode rg3rn3 = new RepNode(sn3.getResourceId());
        rg3.add(rg1rn3);
        rg3.add(rg2rn3);
        rg3.add(rg3rn3);

        PrintStream ps = new PrintStream(new File("testsnapshot.log"));

        /**
          * i = 0 : Create a snapshot without zone flag.
          *         Results in 10 operations: 9 operations for 9 repnodes
          *         and 1 operation for Admin1.
          * i = 1 and 2 : Create a snapshot using zone flag. Zone is zn1.
          *         Results in 4 operations: 3 operations for 3 repnodes
          *         and 1 operation for Admin1.
          * i = 3 and 4 : Create a snapshot using zone flag. Zone is zn2.
          *         Results in 6 operations. 6 operations for 6 repnodes
          *         There is no admin in the storage node sn2.
        */
            
        /* run all the test cases for the following 3 scenarios */
        /* j = 1 : all snapshot creations are successful */
        /* j = 2 : quorum snapshot creations are successful */
        /* j = 3 : quorum snapshot creations are not successful */
        for (int j = 1; j <= 3; j++) {
            boolean allSucceeded;
            boolean quorumSucceeded;

            if (j == 1) {
                allSucceeded = true;
                quorumSucceeded = true;
            } else if (j == 2) {
                allSucceeded = false;
                quorumSucceeded = true;
            } else {
                allSucceeded = false;
                quorumSucceeded = false;
            }

            final SnapResultSummary snapResults =
                new SnapResultSummary(new ArrayList<SnapResult>(),
                                      new ArrayList<SnapResult>(),
                                      allSucceeded,
                                      quorumSucceeded,
                                      new ArrayList<SnapResult>(),
                                      new ArrayList<SnapResult>());

            for (int i = 0; i < allArgs.length; i++) {
                /* First test non-JSON mode */
                CommandShell shell = createMock(CommandShell.class);
                CommandService cs = createMock(CommandService.class);
                Parameters params = createMock(Parameters.class);
                AdminParams adParams = createMock(AdminParams.class);
                String expectedShellMsgRegex = "Created data snapshot named " +
                                               "[0-9-]+-test" + (i+1);

                if (i == 1) {
                    expectedShellMsgRegex += " in zone " +
                                             "zn:\\[id=1 name=zn1\\]";
                } else if (i == 2) {
                    expectedShellMsgRegex += " in zone " +
                                             "zn:\\[id=zn1 name=zn1\\]";
                } else if (i == 3) {
                    expectedShellMsgRegex += " in zone " +
                                             "zn:\\[id=2 name=zn2\\]";
                } else if (i == 4) {
                    expectedShellMsgRegex += " in zone " +
                                             "zn:\\[id=zn2 name=zn2\\]";
                }

                if (j == 1) {
                    expectedShellMsgRegex += " on all [0-9]+ components";
                } else if (j == 2) {
                    expectedShellMsgRegex += " on [0-9]+ components " +
                                             "but failed to create snapshot " +
                                             "on [0-9]+ components " +
                                             "\\(snapshot created on quorum " +
                                             "RNs\\)";
                } else if (j == 3) {
                    expectedShellMsgRegex += " on [0-9]+ components " +
                                             "but failed to create snapshot " +
                                             "on [0-9]+ components " +
                                             "\\(snapshot not created on " +
                                             "quorum RNs\\)";
                }

                /* Establish what is expected from each mock */
                /* for this test */
                expect(cs.getSerialVersion()).
                    andReturn(SerialVersion.CURRENT).anyTimes();
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
                expect(cs.executeSnapshotOperation(
                    isA(SnapshotOperation.class),
                    isA(String.class),
                    i == 0
                        ? isNull(DatacenterId.class)
                        : isA(DatacenterId.class),
                    eq(NULL_AUTH),
                    eq(SerialVersion.CURRENT))).andStubReturn(snapResults);
                replay(cs);
                expect(params.getAdminIds()).andReturn(adminSet);
                expect(params.get(adminId)).andReturn(adParams);
                replay(params);
                expect(adParams.getStorageNodeId()).
                    andReturn(sn1.getResourceId());
                replay(adParams);
                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null));
                expect(shell.getVerbose()).andReturn(true);
                expect(shell.getJson()).andStubReturn(false);
                expect(shell.getOutput()).andReturn(ps);
                replay(shell);
                String [] args = allArgs[i];
                final String shellRetVal = subObj.execute(args, shell);
                assertTrue("Failed to match output from command shell",
                           Pattern.compile(expectedShellMsgRegex)
                                  .matcher(shellRetVal)
                                  .find());

                /* Then test JSON mode */
                shell = createMock(CommandShell.class);
                cs = createMock(CommandService.class);
                params = createMock(Parameters.class);
                adParams = createMock(AdminParams.class);
                expect(cs.getSerialVersion()).
                    andReturn(SerialVersion.CURRENT).anyTimes();
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
                expect(cs.executeSnapshotOperation(
                    isA(SnapshotOperation.class),
                    isA(String.class),
                    i == 0
                        ? isNull(DatacenterId.class)
                        : isA(DatacenterId.class),
                    eq(NULL_AUTH),
                    eq(SerialVersion.CURRENT))).andStubReturn(snapResults);
                replay(cs);
                expect(params.getAdminIds()).andReturn(adminSet);
                expect(params.get(adminId)).andReturn(adParams);
                replay(params);
                expect(adParams.getStorageNodeId()).
                    andReturn(sn1.getResourceId());
                replay(adParams);
                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null));
                expect(shell.getVerbose()).andReturn(true);
                expect(shell.getJson()).andStubReturn(true);
                expect(shell.getOutput()).andReturn(ps);
                replay(shell);
                final ShellCommandResult scr =
                    subObj.executeJsonOutput(allArgs[i], shell);
                final ObjectNode retValue = scr.getReturnValue();
                assertThat(retValue.get("snapshotName").asText(),
                    containsString("test"));
                assertEquals(scr.getOperation(), "snapshot operation");
                if (j == 1 ) {
                    assertEquals(scr.getReturnCode(),
                                 ErrorMessage.NOSQL_5000.getValue());
                    assertEquals(scr.getDescription(),
                                 "Operation ends successfully");
                } else if (j == 2) {
                    assertEquals(scr.getReturnCode(),
                                 ErrorMessage.NOSQL_5301.getValue());
                    assertEquals(scr.getDescription(),
                                 "Created complete snapshot but " +
                                 "the snapshot operation failed " +
                                 "on some nodes");
                } else {
                    assertEquals(scr.getReturnCode(),
                                 ErrorMessage.NOSQL_5300.getValue());
                    assertEquals(scr.getDescription(),
                                 "Snapshot failed");
                }
            }
        }
    }

    /* 2. Test case coverage for: SnapshotCommand.RemoveSnapshotSub. */

    @Test
    public void testRemoveSnapshotSubGetCommandSyntax() throws Exception {
        final SnapshotCommand.RemoveSnapshotSub subObj =
            new SnapshotCommand.RemoveSnapshotSub();
        final String expectedResult =
            "snapshot remove {-name <name> | -all} [-zn <id> |" +
            " -znname <name>] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testRemoveSnapshotSubGetCommandDescription() throws Exception {
        final SnapshotCommand.RemoveSnapshotSub subObj =
            new SnapshotCommand.RemoveSnapshotSub();
        final String expectedResult =
                "Removes the named snapshot.  If -all is specified " +
                "remove all snapshots. If a zone with the specified id or " +
                "name is specified then the command applies to all the SNs " +
                "executing in that zone. Snapshot of configurations will be " +
                "removed for related SNs in zones";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testRemoveSnapshotSubRequiredArgsMissing() throws Exception {
        final SnapshotCommand.RemoveSnapshotSub subObj =
            new SnapshotCommand.RemoveSnapshotSub();
        final String[] args = {"remove"};
        String requiredArgName = "-name";
        final CommandShell shell = createMock(CommandShell.class);
        final CommandService cs = createMock(CommandService.class);

        /* Void method: requiredArg */
        String expectedMsg = "Missing required argument" +
            (" + requiredArgName + ") +
            " for command: " + args[0];
        final ShellUsageException expectedException =
            new ShellUsageException(expectedMsg, subObj);
        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        shell.requiredArg(requiredArgName, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testRemoveSnapshotSubExpectedBehavior() throws Exception {
        final SnapshotCommand.RemoveSnapshotSub subObj =
                new SnapshotCommand.RemoveSnapshotSub();
        final String[][] allArgs = {{"remove", "-name", "test1"},
                                  {"remove", "-name", "test2", "-znname", "zn1"},
                                  {"remove", "-name", "test3", "-zn", "zn1"},
                                  {"remove", "-name", "test4", "-znname", "zn2"},
                                  {"remove", "-name", "test5", "-zn", "zn2"},
                                  {"remove", "-all"},
                                  {"remove", "-all", "-znname", "zn1"},
                                  {"remove", "-all", "-zn", "zn1"},
                                  {"remove", "-all", "-znname", "zn2"},
                                  {"remove", "-all", "-zn", "zn2"}};

        final AdminId adminId = new AdminId(1);
        final Set<AdminId> adminSet = new HashSet<AdminId>();
        adminSet.add(adminId);

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;
        final Topology topo = new Topology("TEST_TOPOLOGY");
        final SnapResultSummary snapResults = new SnapResultSummary(
            new ArrayList<SnapResult>(), new ArrayList<SnapResult>(),
            true, true, new ArrayList<SnapResult>(),
            new ArrayList<SnapResult>());
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

        RepNode rn1 = new RepNode(sn1.getResourceId());
        RepNode rn2 = new RepNode(sn1.getResourceId());
        RepNode rn3 = new RepNode(sn2.getResourceId());
        RepNode rn4 = new RepNode(sn2.getResourceId());

        rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);
        PrintStream ps = new PrintStream(new File("test.txt"));
        try {
            /**
             * i = 0 : Remove a snapshot without zone flag.
             *         Results in 5 operations: 4 operations for repnodes rn1-4
             *         and 1 operation for Admin1.
             * i = 1 and 2 : Remove a snapshot using zone flag. Zone is zn1.
             *         Results in 3 operations. 2 operations for repnodes rn1-2
             *         and 1 operation for Admin1.
             * i = 3 and 4 : Remove a snapshot using zone flag. Zone is zn2.
             *         Results in 2 operations. 2 operations for repnodes rn3-4
             *         There is no admin in the storage node sn2.
             * i = 5 and 6 : Remove all snapshot in zone zn1.
             *         Results in 3 operations. 2 operations for repnodes rn1-2
             *         and 1 operation for Admin1.
             * i = 7 and 8 : Remove all snapshot in zone zn2.
             *         Results in 2 operations. 2 operations for repnodes rn3-4
             *         There is no admin in the storage node sn2.
             */
            for (int i = 0; i < allArgs.length; i++) {
                CommandShell shell = createMock(CommandShell.class);
                CommandService cs = createMock(CommandService.class);
                Parameters params = createMock(Parameters.class);
                AdminParams adParams = createMock(AdminParams.class);
                /* Establish what is expected from each mock for this test */
                expect(cs.getSerialVersion()).
                    andReturn(SerialVersion.CURRENT).anyTimes();
                expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
                expect(cs.executeSnapshotOperation(
                    isA(SnapshotOperation.class), i < 5 ? isA(String.class) :
                    isNull(String.class), (i == 0 || i == 5) ?
                    isNull(DatacenterId.class) : isA(DatacenterId.class),
                    eq(NULL_AUTH), eq(SerialVersion.CURRENT))).
                    andStubReturn(snapResults);
                replay(cs);

                expect(params.getAdminIds()).andReturn(adminSet).anyTimes();
                expect(params.get(adminId)).andReturn(adParams).anyTimes();
                replay(params);

                expect(adParams.getStorageNodeId()).
                    andReturn(sn1.getResourceId()).anyTimes();
                replay(adParams);

                expect(shell.getAdmin()).
                    andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
                expect(shell.getVerbose()).andReturn(true).anyTimes();
                expect(shell.getJson()).andStubReturn(false);
                expect(shell.getOutput()).andReturn(ps).anyTimes();
                replay(shell);
                String[] args = allArgs[i];
                subObj.execute(args, shell);
                final ShellCommandResult scr =
                    subObj.executeJsonOutput(args, shell);
                assertEquals(scr.getOperation(), "snapshot operation");
                assertEquals(scr.getReturnCode(),
                             ErrorMessage.NOSQL_5000.getValue());
            }
        } catch (ShellUsageException e) {
            e.printStackTrace();
        }
    }
}
