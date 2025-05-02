/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static oracle.kv.util.TestUtils.checkException;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.TopologyCommand.TopologyChangeRFSub;
import oracle.kv.impl.admin.client.TopologyCommand.TopologyRebalanceSub;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;


import org.junit.Test;

/**
 * Verifies the functionality and error paths of the TopologyCommand class.
 * Note that although the CLI test framework verifies many of the same aspects
 * of TopologyCommand as this unit test, the tests from the CLI test framework
 * do not contribute to the unit test coverage measure that is automatically
 * computed nightly. Thus, the intent of this test class is to provide
 * additional unit test coverage for the TopologyCommand class that will be
 * automatically measured nightly.
 */
public class TopologyCommandTest extends TestBase {

    private final String command = "topology";
    private final ParameterMap pMap = new ParameterMap();
    private final CommandShell shell = createMock(CommandShell.class);
    private final CommandService cs = createMock(CommandService.class);
    private final AdminParams adminParams = new AdminParams(pMap);

    private static final int ADMIN_ID = 99;
    private static final AuthContext NULL_AUTH = null;
    private static final String[] EMPTY_STRING_LIST = {};

    @Override
    public void setUp() throws Exception {

        super.setUp();
        adminParams.setAdminId(new AdminId(ADMIN_ID));
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /* Convenience method shared by all the test cases that employ the
     * mocked objects.
     */
    private void doVerification() {

        verify(cs);
        verify(shell);
    }

    @Test
    public void testTopologyCommandGetCommandOverview() throws Exception {

        final TestTopologyCommand testTopologyObj = new TestTopologyCommand();
        final String expectedResult =
            "Encapsulates commands that manipulate store topologies." +
            Shell.eol + "Examples are " +
            "redistribution/rebalancing of nodes or changing replication" +
            Shell.eol + "factor.  Topologies are created and modified " +
            "using this command.  They" + Shell.eol + "are then deployed by " +
            "using the " + "\"plan deploy-topology\" command.";
        assertEquals(expectedResult, testTopologyObj.getCommandOverview());
    }

    /* Sub-class to gain access to protected method(s). */
    private static class TestTopologyCommand extends TopologyCommand {
    }

    /* SUB-CLASS TEST CASES */

    /* 1. Test case coverage for: TopologyCommand.TopologyChangeRFSub. */

    @Test
    public void testTopologyChangeRFSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String expectedResult =
             "topology change-repfactor -name <name> -pool " +
             "<pool name>" + Shell.eolt + "-zn <id> | -znname <name> -rf " +
             "<replication factor> " +
             CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyChangeRFSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String expectedResult =
              "Modifies the topology to change the replication factor of " +
              "the specified" + Shell.eolt + "zone to a new value. " +
              "The replication factor cannot be decreased for a" + Shell.eolt +
              "primary zone, but can be decreased for a secondary zone.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyChangeRFSubExecuteUnknownArg()
        throws Exception {
        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRequiredPoolArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-pool";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRequiredDcIdArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-zn";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRequiredDcNameArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-znname";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteDcIdInvalidArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String dcIdFlag = "-zn";
        final String dcId = "INVALID_DC_ID";
        final String expectedResult = "Invalid zone ID: " + dcId;

        final String[] args = {command, dcIdFlag, dcId};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected");
        } catch (ShellUsageException e) {
            assertEquals(expectedResult, e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfInvalidArg()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String rfFlag = "-rf";
        final String rf = "INVALID_RF";
        final ShellArgumentException expectedException;
        try {
            /* create the expected exception */
            subObj.invalidArgument(rf);
            fail("ShellArgumentException should have been thrown");
            return;
        } catch (ShellArgumentException se) {
            expectedException = se;
        }

        final String[] args = {command, rfFlag, rf};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            fail("ShellArgumentException expected, returned: " + result);
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfNonPositive()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String rfFlag = "-rf";
        final String rf = "-1";
        final ShellArgumentException expectedException;
        try {
            /* create the expected exception */
            subObj.invalidArgument(rf);
            fail("ShellArgumentException should have been thrown");
            return;
        } catch (ShellArgumentException se) {
            expectedException = se;
        }
        final String[] args = {command, rfFlag, rf};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            fail("ShellArgumentException expected, returned: " + result);
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfOutOfRange30()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "31";
        final int newRf = Integer.parseInt(rf);
        final String expectedResult = "Illegal replication factor: " + rf;

        final String[] args = {
            command, nameFlag, topoName, poolFlag, poolName, dcNameFlag,
            dcName, rfFlag, rf
        };

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate).anyTimes();
        expect(cs.changeRepFactor(topoName, poolName, dcid, newRf, NULL_AUTH,
                                  SerialVersion.CURRENT))
            .andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        checkException(() -> subObj.execute(args, shell),
                       ShellArgumentException.class,
                       expectedResult);
        doVerification();
    }

    @Test
    public void testTopologyChangeRFSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-name";

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;

        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, poolFlag, poolName, rfFlag, rf, dcNameFlag, dcName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecutePoolNull() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-pool";

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;

        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, nameFlag, topoName, rfFlag, rf, dcNameFlag, dcName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteDcNameNull() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();
        final String arg = "-znname";

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName, rfFlag, rf};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteDcNameDne() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "3";

        final Topology testTopo = new Topology(topoName);

        final ShellUsageException expectedException =
            new ShellUsageException("Zone does not exist: " + dcName,
                                    subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but Zone " + dcName +
                 " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecutePoolDne() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Pool does not exist: " + poolName, subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);

        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteDcIdNotNullPoolDne()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcIdArg = String.valueOf(dcIdVal);
        final String dcIdFlag = "-zn";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Pool does not exist: " + poolName, subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcIdFlag, dcIdArg, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);

        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "3";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + topoName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();

        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfInvalidVal() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final int oldRf = 3;
        final Datacenter dc =
            Datacenter.newInstance(dcName, oldRf,
                                   DatacenterType.PRIMARY, false, false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "2";

        final String expectedExceptionMsg =
            "The replication factor of a primary zone cannot be made" +
            " smaller. The current replication factor for zone zn1 is 3, the" +
            " requested replication factor was 2.";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        checkException(() -> subObj.execute(args, shell),
                       ShellArgumentException.class, expectedExceptionMsg);
        doVerification();
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfChanged() throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final int oldRf = 3;
        final Datacenter dc =
            Datacenter.newInstance(dcName, oldRf,
                                   DatacenterType.PRIMARY, false, false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "5";
        final int newRf = Integer.parseInt(rf);

        final String expectedResult =
            "Changed replication factor in " + topoName;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList).anyTimes();
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate).anyTimes();
        expect(cs.changeRepFactor(topoName, poolName, dcid, newRf, NULL_AUTH,
                                  SerialVersion.CURRENT))
            .andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }

        final ShellCommandResult scr =
            new TopologyCommand.TopologyChangeRFSub().
            executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(), expectedResult);
        assertNull(scr.getReturnValue());
    }

    @Test
    public void testTopologyChangeRFSubExecuteRfSecondaryToZero()
        throws Exception
    {
        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final int oldRf = 3;
        final Datacenter dc =
            Datacenter.newInstance(dcName, oldRf,
                                   DatacenterType.SECONDARY, false, false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "0";
        final int newRf = Integer.parseInt(rf);

        final String expectedResult =
            "Changed replication factor in " + topoName;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList).anyTimes();
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate).anyTimes();
        expect(cs.changeRepFactor(topoName, poolName, dcid, newRf, NULL_AUTH,
                                  SerialVersion.CURRENT))
            .andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }

        final ShellCommandResult scr =
            new TopologyCommand.TopologyChangeRFSub().
            executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(), expectedResult);
        assertNull(scr.getReturnValue());
    }

    @Test
    public void testTopologyChangeRFSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final int oldRf = 3;
        final Datacenter dc =
            Datacenter.newInstance(dcName, oldRf,
                                   DatacenterType.PRIMARY, false, false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "5";
        final int newRf = Integer.parseInt(rf);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate);
        expect(cs.changeRepFactor(topoName, poolName, dcid, newRf, NULL_AUTH,
                                  SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteReturnEmpty()
        throws Exception {

        final TopologyCommand.TopologyChangeRFSub subObj =
            new TopologyCommand.TopologyChangeRFSub();

        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final int oldRf = 3;
        final Datacenter dc =
            Datacenter.newInstance(dcName, oldRf,
                                   DatacenterType.PRIMARY, false, false);

        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String rfFlag = "-rf";
        final String rf = "5";
        final int newRf = Integer.parseInt(rf);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName, rfFlag, rf};

        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate);
        expect(cs.changeRepFactor(topoName, poolName, dcid, newRf, NULL_AUTH,
                                  SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeRFSubExecuteDcDeprecation()
        throws Exception {

        testTopologyChangeRFSubExecute(
            "",
            "topology", "change-repfactor", "-name", "MyTopo",
            "-pool", "MyPool", "-rf", "2", "-zn", "zn1");
        testTopologyChangeRFSubExecute(
            TopologyChangeRFSub.dcFlagsDeprecation,
            "topology", "change-repfactor", "-name", "MyTopo",
            "-pool", "MyPool", "-rf", "2", "-zn", "dc1");
        testTopologyChangeRFSubExecute(
            TopologyChangeRFSub.dcFlagsDeprecation,
            "topology", "change-repfactor", "-name", "MyTopo",
            "-pool", "MyPool", "-rf", "2", "-dc", "zn1");
        testTopologyChangeRFSubExecute(
            "",
            "topology", "change-repfactor", "-name", "MyTopo",
            "-pool", "MyPool", "-rf", "2", "-znname", "MyZone");
        testTopologyChangeRFSubExecute(
            TopologyChangeRFSub.dcFlagsDeprecation,
            "topology", "change-repfactor", "-name", "MyTopo",
            "-pool", "MyPool", "-rf", "2", "-dcname", "MyZone");
    }

    private void testTopologyChangeRFSubExecute(final String deprecation,
                                                final String... cmd)
        throws Exception {

        /* Establish mocks */
        reset(cs, shell);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        final Topology topo = new Topology("MyTopo");
        topo.add(Datacenter.newInstance("MyZone", 1,
                                        DatacenterType.PRIMARY, false, false));
        final TopologyCandidate topoCandidate =
            new TopologyCandidate("MyTopo", topo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(topo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(singletonList("MyPool"));
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(singletonList("MyTopo"));
        expect(cs.getTopologyCandidate("MyTopo", NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andReturn(topoCandidate);
        expect(cs.changeRepFactor("MyTopo", "MyPool", new DatacenterId(1),
                                  2, NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn("");
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        assertEquals("Command result",
                     deprecation,
                     new TopologyCommand().execute(cmd, shell));
        verify(shell, cs);
    }

    /* 2. Test case coverage for: TopologyCommand.TopologyCloneSub. */

    @Test
    public void testTopologyCloneSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String expectedResult =
            "topology clone -from <from topology> -name <to topology> or " +
            Shell.eolt + "topology clone -current -name <toTopology> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyCloneSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String expectedResult =
            "Clones an existing topology so as to create a new " +
            "candidate topology " + Shell.eolt +
            "to be used for topology change operations.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyCloneSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteRequiredNameArg() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteRequiredFromArg() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String arg = "-from";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteNameArgNull() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String fromFlag = "-from";
        final String fromName = "FROM_TOPOLOGY";
        final String useCurrentFlag = "-current";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, fromFlag, fromName, useCurrentFlag};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + nameFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteChangedMaster()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String znFlag = "-zn";
        final String znName = "zn1";
        final String masterFlag = "-master-affinity";
        final String[] args = {command, nameFlag, topoName, znFlag, znName,
                               masterFlag};

        final Datacenter dc =
                Datacenter.newInstance(znName, 3, DatacenterType.PRIMARY, false,
                                       false);

        final int dcIdVal = 1;
        final DatacenterId dcId = new DatacenterId(dcIdVal);

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        final String expectedResult = "Changed zone master affinity zn1 to " +
            "true in TEST_TOPOLOGY";

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(testTopo).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.changeZoneMasterAffinity(topoName, dcId, true, NULL_AUTH,
            SerialVersion.CURRENT)).andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(result, expectedResult);
        } finally {
            doVerification();
        }

        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(), expectedResult);
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteChangedNonMaster()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String znFlag = "-zn";
        final String znName = "zn1";
        final String noMasterFlag = "-no-master-affinity";
        final String[] args = {command, nameFlag, topoName, znFlag, znName,
                               noMasterFlag};

        final Datacenter dc =
                Datacenter.newInstance(znName, 3, DatacenterType.PRIMARY, false,
                                       false);

        final int dcIdVal = 1;
        final DatacenterId dcId = new DatacenterId(dcIdVal);

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        final String expectedResult = "Changed zone master affinity zn1 to " +
            "false in TEST_TOPOLOGY";

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.changeZoneMasterAffinity(topoName, dcId, false, NULL_AUTH,
            SerialVersion.CURRENT)).andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteZnDnExist()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String znFlag = "-zn";
        final String znName = "zn2";
        final String noMasterFlag = "-no-master-affinity";
        final String[] args = {command, nameFlag, topoName, znFlag, znName,
                               noMasterFlag};
        final ShellUsageException expectedException =
                new ShellUsageException("Zone does not exist: " +
                                        znName, subObj);

        final Datacenter dc =
                Datacenter.newInstance("zn1", 3, DatacenterType.PRIMARY, false,
                                       false);

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but zn2 exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteTopoDnExist()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
                new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String znFlag = "-zn";
        final String znName = "zn1";
        final String noMasterFlag = "-master-affinity";
        final String[] args = {command, nameFlag, topoName, znFlag, znName,
                               noMasterFlag};
        final ShellUsageException expectedException =
                new ShellUsageException("Topology " + topoName +
                    " does not exist. " +
                    "Use topology list to see existing candidates.", subObj);

        final Datacenter dc =
                Datacenter.newInstance("zn1", 3, DatacenterType.PRIMARY, false,
                                       false);

        /* Objects to be used with the mock objects */
        String anotherTopoName = "ANOTHER_TEST_TOPOLOGY";
        final Topology testTopo = new Topology(anotherTopoName);
        testTopo.add(dc);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(anotherTopoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but zn2 exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
}

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteMissingMasterFlag()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String znFlag = "-zn";
        final String znName = "zn1";
        final String[] args = {command, nameFlag, topoName, znFlag, znName};
        final ShellUsageException expectedException =
                new ShellUsageException("Missing required argument for command: " +
                                        command, subObj);


        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected," +
                 " but -master-affinity/-no-master-affinity exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteMissingZnFlag()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String masterFlag = "-master-affinity";
        final String[] args = {command, nameFlag, topoName, masterFlag};
        final ShellUsageException expectedException =
                new ShellUsageException("Missing required argument for command: " +
                                        command, subObj);


        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but -zn exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyChangeZoneAffinitySubExecuteMissingNameFlag()
            throws Exception {

        final TopologyCommand.TopologyChangeZoneAffinitySub subObj =
            new TopologyCommand.TopologyChangeZoneAffinitySub();
        final String znFlag = "-zn";
        final String znName = "zn1";
        final String masterFlag = "-master-affinity";
        final String[] args = {command, znFlag, znName, masterFlag};
        final ShellUsageException expectedException =
                new ShellUsageException("Missing required argument for command: " +
                                        command, subObj);


        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but -name exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteFromArgNull() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String fromFlag = "-from";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, nameFlag, topoName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + fromFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteUseCurrent() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String useCurrentFlag = "-current";

        final String[] args =
            {command, nameFlag, topoName, useCurrentFlag};

        final String expectedResult = "Created " + topoName;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.copyCurrentTopology(topoName, NULL_AUTH,
                                      SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteFromTopoDne() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String fromFlag = "-from";
        final String fromName = "From_TOPOLOGY";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + fromName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);

        final String[] args =
            {command, nameFlag, topoName, fromFlag, fromName};

        /* Objects to be used with the mock objects */
        final List<String> fromList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(fromList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but specified " +
                 fromName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCloneSubExecuteTopoCloned() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String fromFlag = "-from";
        final String fromName = "FROM_TOPOLOGY";

        final String expectedResult = "Created " + topoName;

        final String[] args =
            {command, nameFlag, topoName, fromFlag, fromName};

        /* Objects to be used with the mock objects */
        final List<String> fromList = new ArrayList<String>();
        fromList.add(fromName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(fromList).anyTimes();
        expect(cs.copyTopology(fromName, topoName, NULL_AUTH,
                               SerialVersion.CURRENT))
            .andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }

        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(), expectedResult);
    }

    @Test
    public void testTopologyCloneSubExecuteRemoteException() throws Exception {

        final TopologyCommand.TopologyCloneSub subObj =
            new TopologyCommand.TopologyCloneSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String fromFlag = "-from";
        final String fromName = "FROM_TOPOLOGY";

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        final String[] args =
            {command, nameFlag, topoName, fromFlag, fromName};

        /* Objects to be used with the mock objects */
        final List<String> fromList = new ArrayList<String>();
        fromList.add(fromName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(fromList);
        expect(cs.copyTopology(fromName, topoName, NULL_AUTH,
                               SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testCloneCurrentWithReservedName() throws Exception {
        final String cmdline = "clone -current -name has$dollar";

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.copyCurrentTopology("has$dollar", NULL_AUTH,
                                      SerialVersion.CURRENT))
            .andReturn("");
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        try {
            final String result = new TopologyCommand.TopologyCloneSub()
                .execute(cmdline.split(" "), shell);
            assertEquals(TopologyCommand.RESERVED_CANDIDATE_NAME_WARNING,
                         result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testCloneWithReservedName() throws Exception {
        final String cmdline = "clone -from old -name has$dollar";

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
             .andReturn(singletonList("old"));
        expect(cs.copyTopology("old", "has$dollar", NULL_AUTH,
                               SerialVersion.CURRENT))
            .andReturn("");
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        try {
            final String result = new TopologyCommand.TopologyCloneSub()
                .execute(cmdline.split(" "), shell);
            assertEquals(TopologyCommand.RESERVED_CANDIDATE_NAME_WARNING,
                         result);
        } finally {
            doVerification();
        }
    }

    /* 3. Test case coverage for: TopologyCommand.TopologyCreateSub. */

    @Test
    public void testTopologyCreateSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String expectedResult =
            "topology create -name <candidate name> -pool " +
            "<pool name>" + Shell.eolt + "-partitions <num> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyCreateSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String expectedResult =
            "Creates a new topology with the specified number of " +
            "partitions" + Shell.eolt + "using the specified storage pool.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyCreateSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecuteRequiredPoolArg()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String arg = "-pool";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }


    @Test
    public void testTopologyCreateSubExecuteRequiredPartitionsArg()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String arg = "-partitions";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, poolFlag, poolName, partitionsFlag, nPartitions};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + nameFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecutePoolNull() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, nameFlag, topoName, partitionsFlag, nPartitions};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecutePartitionsNull() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + partitionsFlag +
                 " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecutePartitionsInvalid()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "INVALID_NUM_PARTITIONS";
        final ShellArgumentException expectedException;
        try {
            /* create the expected exception */
            subObj.invalidArgument(nPartitions);
            fail("ShellArgumentException should have been thrown");
            return;
        } catch (ShellArgumentException se) {
            expectedException = se;
        }

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             partitionsFlag, nPartitions};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellArgumentException expected, but value specified for " +
                 partitionsFlag + " was valid");
        } catch (ShellArgumentException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecutePoolDne() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final ShellUsageException expectedException =
            new ShellUsageException("Pool does not exist: " + poolName,
                                    subObj);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             partitionsFlag, nPartitions};
        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but specified pool " +
                 poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecuteTopoCreated() throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final int numPartitions = Integer.parseInt(nPartitions);
        final boolean json = false;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             partitionsFlag, nPartitions};

        final String expectedResult = "Created: " + topoName + Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.createTopology(topoName, poolName, numPartitions, json,
                                 SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                 NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(json).times(2);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyCreateSubExecuteTopoCreatedJsonMode()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final int numPartitions = Integer.parseInt(nPartitions);
        final boolean json = true;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             partitionsFlag, nPartitions};

        ObjectNode returnValue = JsonUtils.createObjectNode();
        returnValue.put("name", topoName);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.createTopology(topoName, poolName, numPartitions, json,
                                 SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                 NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(returnValue.toString()).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command + " create",
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
            assertEquals(topoName, valueNode.get("name").asText());
        } finally {
            doVerification();
        }

        reset(cs, shell);
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.createTopology(
            topoName, poolName, numPartitions, true,
            SerialVersion.ADMIN_CLI_JSON_V2_VERSION,
            NULL_AUTH, SerialVersion.CURRENT)).
                andReturn(returnValue.toString()).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getJson()).andReturn(json).anyTimes();
        replay(shell);
        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnValue().get("name").asText(), topoName);
    }

    @Test
    public void testTopologyCreateSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyCreateSub subObj =
            new TopologyCommand.TopologyCreateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";
        final String partitionsFlag = "-partitions";
        final String nPartitions = "300";
        final int numPartitions = Integer.parseInt(nPartitions);
        final boolean json = false;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             partitionsFlag, nPartitions};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.createTopology(topoName, poolName, numPartitions, json,
                                 SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                 NULL_AUTH, SerialVersion.CURRENT))
                .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(json);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testCreateWithReservedName() throws Exception {
        final String cmdline =
            "create -name has$dollar -pool mypool -partitions 3";
        final boolean json = false;

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(singletonList("mypool"));
        expect(cs.createTopology("has$dollar", "mypool", 3, json,
                                 SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                 NULL_AUTH,
                                 SerialVersion.CURRENT))
            .andReturn("");
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(json).times(2);
        replay(shell);

        try {
            final String result = new TopologyCommand.TopologyCreateSub()
                .execute(cmdline.split(" "), shell);
            assertEquals(TopologyCommand.RESERVED_CANDIDATE_NAME_WARNING,
                         result);
        } finally {
            doVerification();
        }
    }

    /* 4. Test case coverage for: TopologyCommand.TopologyDeleteSub. */

    @Test
    public void testTopologyDeleteSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String expectedResult =
            "topology delete -name <name> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyDeleteSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String expectedResult = "Deletes a topology.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyDeleteSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyDeleteSubExecuteRequiredArg() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyDeleteSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyDeleteSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String arg0 = "-name";
        final String arg1 = "TEST_TOPOLOGY";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + arg1 + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args = {command, arg0, arg1};
        final List<String> topoList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg1 + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyDeleteSubExecuteRemoved() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final String expectedResult = "Removed " + topoName + Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.deleteTopology(
            topoName, NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }

        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(), expectedResult);
    }

    @Test
    public void testTopologyDeleteSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.deleteTopology(topoName, NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyDeleteSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyDeleteSub subObj =
            new TopologyCommand.TopologyDeleteSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.deleteTopology(topoName, NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 5. Test case coverage for: TopologyCommand.TopologyListSub. */

    @Test
    public void testTopologyListSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();
        final String expectedResult =
            "topology list " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyListSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();
        final String expectedResult =  "Lists existing topologies.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyListSubExecuteUnknownArgument() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();
        final String arg = "EXTRA_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);

        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        replay(cs);

        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyListSubExecuteReturnList() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();
        final List<String> unorderedTopoNames = Arrays.asList(new String[]
            {"MMM_TEST_TOPOLOGY", "ZZZ_TEST_TOPOLOGY", "AAA_TEST_TOPOLOGY"});

        final String[] args = {command};

        final String expectedResult = "AAA_TEST_TOPOLOGY" + Shell.eol +
            "MMM_TEST_TOPOLOGY" + Shell.eol + "ZZZ_TEST_TOPOLOGY" + Shell.eol;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(unorderedTopoNames).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getHidden()).andReturn(false).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ObjectNode returnValue = scr.getReturnValue();
        final ArrayNode topologyArray =
            (ArrayNode)returnValue.get("topologies");
        assertEquals(topologyArray.get(0).asText(), "AAA_TEST_TOPOLOGY");
        assertEquals(topologyArray.get(1).asText(), "MMM_TEST_TOPOLOGY");
        assertEquals(topologyArray.get(2).asText(), "ZZZ_TEST_TOPOLOGY");
    }

    @Test
    public void testTopologyListSubExecuteRemoteException() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();

        final String[] args = {command};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(false);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyListSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyListSub subObj =
            new TopologyCommand.TopologyListSub();

        final String[] args = {command};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(false);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);
        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testListInternalNotHidden() throws Exception {

        final String cmdline = "list";
        final String internalCandidate =
            TopologyCandidate.INTERNAL_NAME_PREFIX + "bar";

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(asList("foo", internalCandidate, "baz"));
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(false);
        replay(shell);

        try {
            final String result = new TopologyCommand.TopologyListSub()
                .execute(cmdline.split(" "), shell);
            assertEquals("baz\nfoo\n", result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testListInternalHiddenFlag() throws Exception {
        /*
         * The -hidden flag is no longer parsed by the topology command code.
         * Instead it is parsed by Shell and would be returned by
         * shell.getHidden().
         */
        final String cmdline = "list"; // "-hidden"
        final String internalCandidate =
            TopologyCandidate.INTERNAL_NAME_PREFIX + "bar";

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(asList("foo", internalCandidate, "baz"));
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(true);  // -hidden
        replay(shell);

        try {
            final String result = new TopologyCommand.TopologyListSub()
                .execute(cmdline.split(" "), shell);
            assertEquals(internalCandidate + "\nbaz\nfoo\n", result);
        } finally {
            doVerification();
        }
    }

    /* 6. Test case coverage for: TopologyCommand.TopologyMoveRNSub. */

    @Test
    public void testTopologyMoveRNSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String expectedResult =
            "topology move-repnode -name <name> -rn <id> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyMoveRNSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String expectedResult =
            "Modifies the topology to move the specified RepNode to " +
            "an available" + Shell.eolt + "storage node chosen by the system.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyMoveRNSubIsHidden() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final boolean expectedResult = true;
        assertEquals(expectedResult, subObj.isHidden());
    }

    @Test
    public void testTopologyMoveRNSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteRequiredRnArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String arg = "-rn";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteRequiredSnArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String arg = "-sn";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteIllegalRnArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String rnFlag = "-rn";
        final String rnId = "ILLEGAL_RN_ARG";

        final String[] args = {command, nameFlag, topoName, rnFlag, rnId};

        final String expectedResult = "Invalid RepNode id: " + rnId;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("can't get here");
        } catch (ShellArgumentException e) {
            assertEquals(e.getMessage(), expectedResult);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteIllegalSnArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String rnFlag = "-rn";
        final String rnId = "rg1-rn1";
        final String snFlag = "-sn";
        final String snId = "ILLEGAL_SN_ARG";

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnId, snFlag, snId};

        final String expectedResult = "Invalid StorageNode id: " + snId;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
        } catch (ShellArgumentException e) {
            assertEquals(e.getMessage(), expectedResult);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteNullNameArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String rnFlag = "-rn";
        final String rnId = "rg1-rn1";
        final String snFlag = "-sn";
        final String snId = "1";

        final String[] args =
            {command, rnFlag, rnId, snFlag, snId};

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " +
                 nameFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteNullRnArg() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String snFlag = "-sn";
        final String snId = "1";

        final String[] args =
            {command, nameFlag, topoName, snFlag, snId};

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " +
                 nameFlag + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyMoveRNSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String rnFlag = "-rn";
        final String rnId = "rg1-rn1";
        final String snFlag = "-sn";
        final String snId = "1";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + topoName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnId, snFlag, snId};

        final List<String> topoList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + topoName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }

    }

    @Test
    public void testTopologyMoveRNSubExecuteRnDne() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String rnFlag = "-rn";
        final String rnId = "rg1-rn1";
        final String snFlag = "-sn";
        final String snId = "1";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "RepNode does not exist: " + rnId, subObj);

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnId, snFlag, snId};

        /* Objects to be used with the mock objects */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + topoName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /*
     * *** Note ***
     *
     * The remaining test cases for the execute method of the TopologyMoveRNSub
     * class provide coverage for the cases where a Topology containing various
     * Component types is used. In some cases a Topology is created and
     * populated with only a set of RepGroup(s) containing a set of
     * RepNode(s). In other cases, the Topology will contain a Datacenter, a
     * set of StorageNode(s), and a set of RepGroup(s) with RepNode(s)
     * associated with at least one of the StorageNode(s). Providing a Topology
     * with these characteristics will result in coverage for the cases where
     * the execute method invokes the ensureRepNodeExists(RepNodeId) method of
     * the CommandUtils class and the ensureStorageNodeExists(StorageNodeId)
     * under various conditions. This means that in order for either of those
     * methods to succeed, the Topology must contain a RepNode and (in some
     * cases) a StorageNode with an id that satisfies the parameters input to
     * the execute method under test.
     *
     * Thus, the Topology created for each of the test cases below will contain
     * a RepNode having a RepNodeId of the form 'rgN-rnM', where 'N' is equal
     * to the value of the 'rgId'field defined in each such test case and 'M'
     * is equal to the value of the corresponding 'nodeNum' field. This means
     * that the Topology must also contain a RepGroup with id equal to
     * 'rgN'. In order to place a RepGroup having id equal to 'rgN' in the
     * Topology, 'N' RepGroups must be created and added to the Topology. This
     * is because the id of a RepGroup should not be set directly. Rather, as
     * RepGroups are created and added to a Topology, the id of each added
     * RepGroup is automatically generated in sequence, starting at 1. That is,
     * the first RepGroup that is added to the Topology will have a generated
     * RepGroupId of 'rg1', the second RepGroup will have a RepGroupId of
     * 'rg2', and so on. Thus, in each of the test cases below, after creating
     * the Topology, 'N' RepGroup(s) are created and added to the Topology;
     * resulting in a set of RepGroups with ids 'rg1', 'rg2', ..., 'rgN', where
     * N equals the value of 'rgId'.
     *
     * With respect to the RepNodes, a RepNode is added to the Topology by
     * adding the RepNode to one of the RepGroups belonging to the
     * Topology. And in a fashion similar to that for RepGroupIds, the
     * RepNodeId of each RepNode added to a given RepGroup also should not be
     * set directly, as each id will be automatically generated in sequence and
     * assigned to each RepNode when it is added to the Topology. Thus, 'M'
     * RepNodes (where 'M' is equal to value of the 'nodeNum' field) will be
     * created in each test case below and added to the single RepGroup with id
     * equal to 'rgN' (where 'N' equals the value of 'rgId'). This will result
     * in RepNodes with ids of the form: 'rgN-rn1', 'rgN-rn2', ...,
     * 'rgN-rnM'. Note, of course, that the Topology will also contain
     * RepGroups with ids 'rg1', 'rg2', ... 'rg<N-1>' that each contain no
     * RepNodes.
     *
     * For the test cases whose Topology must contain a specific StorageNode
     * (so that ensureStorageNodeExists(StorageNodeId) succeeds) in addition to
     * a RepGroup with a specific RepNode, prior to adding the RegGroups and
     * RepNodes (as described above), a Datacenter is first created and added
     * to the Topology, and then 'K' StorageNode(s) are created and added;
     * where 'K' is equal to the value of the 'nNodes' field that is specified
     * in each such test case. Note that 'K' StorageNodes are added, in
     * succession, to the Topology for the same reason that multiple RepGroups
     * and RepNodes are added to the Topology; to add the desired Component
     * type to the Topology and to associate that Component with the desired
     * ResourceId. As was the case with the ids of the RepGroups and RepNodes
     * added to the Topology, the id of each StorageNode that is added should
     * NOT be directly set; rather, the id should be allowed to be
     * automatically generated (in sequence), when each such Compoent is added
     * to the Topology.
     *
     * Note that the various Component types that may be added to the Topology
     * are each sub-classes of Topology.Component; that is, types such as
     * Datacenter, StorageNode, RepGroup, and RepNode. As such, since there are
     * methods on the Component class that allow one to associate a given
     * Component with a given Topology (Topology.Component.setTopology) and to
     * explicitly set the id of the Component
     * (Topology.Component.setResourceId), one might ask why those methods are
     * not employed in the test cases below; where the Topology.add method is
     * used instead for each such Component.
     *
     * The Topology.add method will not only cause the Topology and the given
     * Component to be associated in the appropriate manner (setting the
     * Component in the Topology and the Topology in the Component), but will
     * also automatically generate the Component's id in a way that the
     * Topology expects. On the other hand, although calling
     * Component.setTopology will set the Topology in the given Component, the
     * Component is not set in that Topology. Additionally, calling
     * Component.setTopology followed by Topology.add (or vice-versa) will
     * result in an AssertionError. Similarly, calling Component.setResourceId
     * followed by Topology.add (or vice-versa) will also result in an
     * AssertionError. This is because the Component.setTopology method allows
     * the Topology to be set only once, and the Component.setResourceId method
     * allows the Component's id to be set only once; throwing an
     * AssertionError if either the Topology or the resource id has already
     * been set. Since a call to Topology.add will ultimately invoke both
     * Component.setTopology and Component.setResourceId, combining a call to
     * Topology.add with a direct call to either Component.setTopology or
     * Component.setResourceId will not succeed.
     */

    /**
     * Verifies that ensureRepNodeExists(RepNodeId) is exercised and works as
     * expected when the Topology contains the specified RepGroup and RepNode;
     * and that ensureStorageNodeExists(StorageNodeId) is exercised and works
     * as expected when the Topology does NOT contain the specified
     * StorageNode.
     */
    @Test
    public void testTopologyMoveRNSubExecuteSnDne() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int rgId = 2;
        final int nodeNum = 3;

        final String rnFlag = "-rn";
        final String rnId = "rg" + rgId + "-rn" + nodeNum;
        final String snFlag = "-sn";
        final String snId = String.valueOf(nodeNum);

        final ShellUsageException expectedException =
            new ShellUsageException(
                "StorageNode does not exist: sn" + snId, subObj);

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnId, snFlag, snId};

        /* Create the Topology and the objects it should contain. */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        final StorageNodeId storageNodeId = new StorageNodeId(nodeNum);
        final RepGroup[] repGroups = new RepGroup[rgId];
        final RepNode[] repNodes = new RepNode[nodeNum];

        /* Initializations and additions to the Topology. */
        topoList.add(topoName);

        /* Create 'rgId' number of RepGroups and add each RepGroup to the
         * Topology's repGroupMap.
         */
        for (int i = 0; i < rgId; i++) {
            repGroups[i] = new RepGroup();
            testTopo.add(repGroups[i]);
        }

        /* Create 'nodeNum' RepNodes and add each to the repNodeMap of the
         * RepGroup in the Topology with id equal to 'rgId'. At this point,
         * the Topology created above should contain the desired RepNode,
         * but not the StorageNode with id equal to 'snId'.
         */
        for (int i = 0; i < nodeNum; i++) {
            repNodes[i] = new RepNode(storageNodeId);
            repGroups[rgId - 1].add(repNodes[i]);
        }

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but snId " + snId + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * Verifies that ensureRepNodeExists(RepNodeId) is exercised and works as
     * expected when the Topology contains the specified RepGroup and RepNode;
     * and that ensureStorageNodeExists(StorageNodeId) is exercised and works
     * as expected when the Topology DOES contain the specified StorageNode.
     */
    @Test
    public void testTopologyMoveRNSubExecuteSnExists() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int nNodes = 5;
        final int rgId = 2;
        final int nodeNum = 3;

        final String rnFlag = "-rn";
        final String rnIdStr = "rg" + rgId + "-rn" + nodeNum;
        final String snFlag = "-sn";
        final String snIdStr = String.valueOf(nodeNum);

        final int dcIdVal = 1;
        final int repFactor = 3;
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final String[] hostnames = new String[nNodes];
        final int[] ports = new int[nNodes];
        for (int i = 0; i < nNodes; i++) {
            hostnames[i] = "TEST_HOST_" + (i + 1);
            ports[i] = 9990 + (i + 1);
        }

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnIdStr, snFlag, snIdStr};

        /* Create the Topology and the objects it should contain. */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        final StorageNodeId[] storageNodeIds = new StorageNodeId[nNodes];
        final StorageNode[] storageNodes = new StorageNode[nNodes];
        final RepGroup[] repGroups = new RepGroup[rgId];
        final RepNode[] repNodes = new RepNode[nodeNum];

        /* Initializations and additions to the Topology. */
        topoList.add(topoName);

        /* Create a Datacenter and add it to the Topology's datacenterMap. */
        final Datacenter dc =
            Datacenter.newInstance(dcName, repFactor,
                                   DatacenterType.PRIMARY, false, false);
        testTopo.add(dc);

        /* Create 'nNodes' number of StorageNodes, each belonging to the
         * single Datacenter created above, each running on its own host,
         * and add each StorageNode to the the Topology's storageNodeMap.
         */
        for (int i = 0; i < nNodes; i++) {
            storageNodes[i] = new StorageNode(dc, hostnames[i], ports[i]);
            testTopo.add(storageNodes[i]);
            storageNodeIds[i] = storageNodes[i].getStorageNodeId();
        }

        /* Create 'rgId' number of RepGroups and add each RepGroup to the
         * Topology's repGroupMap.
         */
        for (int i = 0; i < rgId; i++) {
            repGroups[i] = new RepGroup();
            testTopo.add(repGroups[i]);
        }

        /* Create 'nodeNum' RepNodes and add each to the repNodeMap of the
         * RepGroup in the Topology with id equal to 'rgId'. At this point,
         * the Topology created above should contain the desired RepNode,
         * as well as the StorageNode with id equal to 'snId'.
         */
        for (int i = 0; i < nodeNum; i++) {
            repNodes[i] = new RepNode(storageNodeIds[i]);
            repGroups[rgId - 1].add(repNodes[i]);
        }

        /* Values to input to CommandService.moveRN */
        final RepNodeId rnId = repNodes[nodeNum - 1].getResourceId();
        final StorageNodeId oldSnId = storageNodeIds[nodeNum - 1];
        final StorageNodeId newSnId = storageNodeIds[nNodes - 1];
        final String expectedResult =
            "Moved " + rnId + " from " + oldSnId + " to " + newSnId;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.moveRN(topoName, rnId, oldSnId, NULL_AUTH,
                         SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /**
     * Verifies that ensureStorageNodeExists(StorageNodeId) is by-passed when
     * no '-sn' flag is specified, and that CommandService.moveRN is exercised
     * with NULL for the 'snId' parameter.
     */
    @Test
    public void testTopologyMoveRNSubExecuteSnNull() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int nNodes = 5;
        final int rgId = 2;
        final int nodeNum = 3;

        final String rnFlag = "-rn";
        final String rnIdStr = "rg" + rgId + "-rn" + nodeNum;

        final int dcIdVal = 1;
        final int repFactor = 3;
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final String[] hostnames = new String[nNodes];
        final int[] ports = new int[nNodes];
        for (int i = 0; i < nNodes; i++) {
            hostnames[i] = "TEST_HOST_" + (i + 1);
            ports[i] = 9990 + (i + 1);
        }

        final String[] args = {command, nameFlag, topoName, rnFlag, rnIdStr};

        /* Create the Topology and the objects it should contain. */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        final StorageNodeId[] storageNodeIds = new StorageNodeId[nNodes];
        final StorageNode[] storageNodes = new StorageNode[nNodes];
        final RepGroup[] repGroups = new RepGroup[rgId];
        final RepNode[] repNodes = new RepNode[nodeNum];

        /* Initializations and additions to the Topology. */
        topoList.add(topoName);

        /* Create a Datacenter and add it to the Topology's datacenterMap. */
        final Datacenter dc =
            Datacenter.newInstance(dcName, repFactor,
                                   DatacenterType.PRIMARY, false, false);
        testTopo.add(dc);

        /* Create 'nNodes' number of StorageNodes, each belonging to the
         * single Datacenter created above, each running on its own host,
         * and add each StorageNode to the the Topology's storageNodeMap.
         */
        for (int i = 0; i < nNodes; i++) {
            storageNodes[i] = new StorageNode(dc, hostnames[i], ports[i]);
            testTopo.add(storageNodes[i]);
            storageNodeIds[i] = storageNodes[i].getStorageNodeId();
        }

        /* Create 'rgId' number of RepGroups and add each RepGroup to the
         * Topology's repGroupMap.
         */
        for (int i = 0; i < rgId; i++) {
            repGroups[i] = new RepGroup();
            testTopo.add(repGroups[i]);
        }

        /* Create 'nodeNum' RepNodes and add each to the repNodeMap of the
         * RepGroup in the Topology with id equal to 'rgId'. At this point,
         * the Topology created above should contain the desired RepNode,
         * as well as the StorageNode with id equal to 'snId'.
         */
        for (int i = 0; i < nodeNum; i++) {
            repNodes[i] = new RepNode(storageNodeIds[i]);
            repGroups[rgId - 1].add(repNodes[i]);
        }

        /* Values to input to CommandService.moveRN */
        final RepNodeId rnId = repNodes[nodeNum - 1].getResourceId();
        final StorageNodeId oldSnId = storageNodeIds[nodeNum - 1];
        final StorageNodeId newSnId = storageNodeIds[nNodes - 1];
        final String expectedResult =
            "Moved " + rnId + " from " + oldSnId + " to " + newSnId;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.moveRN(topoName, rnId, null, NULL_AUTH,
                         SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /**
     * Covers the code path where CommandService.noAdmin is called and throws a
     * ShellException after CommandService.moveRN is called and throws a
     * RemoteException.
     */
    @Test
    public void testTopologyMoveRNSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int nNodes = 5;
        final int rgId = 2;
        final int nodeNum = 3;

        final String rnFlag = "-rn";
        final String rnIdStr = "rg" + rgId + "-rn" + nodeNum;
        final String snFlag = "-sn";
        final String snIdStr = String.valueOf(nodeNum);

        final int dcIdVal = 1;
        final int repFactor = 3;
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final String[] hostnames = new String[nNodes];
        final int[] ports = new int[nNodes];
        for (int i = 0; i < nNodes; i++) {
            hostnames[i] = "TEST_HOST_" + (i + 1);
            ports[i] = 9990 + (i + 1);
        }

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnIdStr, snFlag, snIdStr};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Create the Topology and the objects it should contain. */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        final StorageNodeId[] storageNodeIds = new StorageNodeId[nNodes];
        final StorageNode[] storageNodes = new StorageNode[nNodes];
        final RepGroup[] repGroups = new RepGroup[rgId];
        final RepNode[] repNodes = new RepNode[nodeNum];

        /* Initializations and additions to the Topology. */
        topoList.add(topoName);

        /* Create a Datacenter and add it to the Topology's datacenterMap. */
        final Datacenter dc =
            Datacenter.newInstance(dcName, repFactor,
                                   DatacenterType.PRIMARY, false, false);
        testTopo.add(dc);

        /* Create 'nNodes' number of StorageNodes, each belonging to the
         * single Datacenter created above, each running on its own host,
         * and add each StorageNode to the the Topology's storageNodeMap.
         */
        for (int i = 0; i < nNodes; i++) {
            storageNodes[i] = new StorageNode(dc, hostnames[i], ports[i]);
            testTopo.add(storageNodes[i]);
            storageNodeIds[i] = storageNodes[i].getStorageNodeId();
        }

        /* Create 'rgId' number of RepGroups and add each RepGroup to the
         * Topology's repGroupMap.
         */
        for (int i = 0; i < rgId; i++) {
            repGroups[i] = new RepGroup();
            testTopo.add(repGroups[i]);
        }

        /* Create 'nodeNum' RepNodes and add each to the repNodeMap of the
         * RepGroup in the Topology with id equal to 'rgId'. At this point,
         * the Topology created above should contain the desired RepNode,
         * as well as the StorageNode with id equal to 'snId'.
         */
        for (int i = 0; i < nodeNum; i++) {
            repNodes[i] = new RepNode(storageNodeIds[i]);
            repGroups[rgId - 1].add(repNodes[i]);
        }

        /* Values to input to CommandService.moveRN */
        final RepNodeId rnId = repNodes[nodeNum - 1].getResourceId();
        final StorageNodeId oldSnId = storageNodeIds[nodeNum - 1];

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.moveRN(topoName, rnId, oldSnId, NULL_AUTH,
                         SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * Covers the code path where CommandService.noAdmin is called but simply
     * returns (does not throw a ShellException) after CommandService.moveRN is
     * called and throws a RemoteException, and the final statement of the
     * method returns the empty String.
     */
    @Test
    public void testTopologyMoveRNSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyMoveRNSub subObj =
            new TopologyCommand.TopologyMoveRNSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final int nNodes = 5;
        final int rgId = 2;
        final int nodeNum = 3;

        final String rnFlag = "-rn";
        final String rnIdStr = "rg" + rgId + "-rn" + nodeNum;
        final String snFlag = "-sn";
        final String snIdStr = String.valueOf(nodeNum);

        final int dcIdVal = 1;
        final int repFactor = 3;
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final String[] hostnames = new String[nNodes];
        final int[] ports = new int[nNodes];
        for (int i = 0; i < nNodes; i++) {
            hostnames[i] = "TEST_HOST_" + (i + 1);
            ports[i] = 9990 + (i + 1);
        }

        final String[] args =
            {command, nameFlag, topoName, rnFlag, rnIdStr, snFlag, snIdStr};

        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Create the Topology and the objects it should contain. */
        final Topology testTopo = new Topology(topoName);
        final List<String> topoList = new ArrayList<String>();
        final StorageNodeId[] storageNodeIds = new StorageNodeId[nNodes];
        final StorageNode[] storageNodes = new StorageNode[nNodes];
        final RepGroup[] repGroups = new RepGroup[rgId];
        final RepNode[] repNodes = new RepNode[nodeNum];

        /* Initializations and additions to the Topology. */
        topoList.add(topoName);

        /* Create a Datacenter and add it to the Topology's datacenterMap. */
        final Datacenter dc =
            Datacenter.newInstance(dcName, repFactor,
                                   DatacenterType.PRIMARY, false, false);
        testTopo.add(dc);

        /* Create 'nNodes' number of StorageNodes, each belonging to the
         * single Datacenter created above, each running on its own host,
         * and add each StorageNode to the the Topology's storageNodeMap.
         */
        for (int i = 0; i < nNodes; i++) {
            storageNodes[i] = new StorageNode(dc, hostnames[i], ports[i]);
            testTopo.add(storageNodes[i]);
            storageNodeIds[i] = storageNodes[i].getStorageNodeId();
        }

        /* Create 'rgId' number of RepGroups and add each RepGroup to the
         * Topology's repGroupMap.
         */
        for (int i = 0; i < rgId; i++) {
            repGroups[i] = new RepGroup();
            testTopo.add(repGroups[i]);
        }

        /* Create 'nodeNum' RepNodes and add each to the repNodeMap of the
         * RepGroup in the Topology with id equal to 'rgId'. At this point,
         * the Topology created above should contain the desired RepNode,
         * as well as the StorageNode with id equal to 'snId'.
         */
        for (int i = 0; i < nodeNum; i++) {
            repNodes[i] = new RepNode(storageNodeIds[i]);
            repGroups[rgId - 1].add(repNodes[i]);
        }

        /* Values to input to CommandService.moveRN */
        final RepNodeId rnId = repNodes[nodeNum - 1].getResourceId();
        final StorageNodeId oldSnId = storageNodeIds[nodeNum - 1];

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.moveRN(topoName, rnId, oldSnId, NULL_AUTH,
                         SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 7. Test case coverage for: TopologyCommand.TopologyPreviewSub. */

    @Test
    public void testTopologyPreviewSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String expectedResult =
            "topology preview -name <name> [-start <from topology>] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyPreviewSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String expectedResult =
            "Describes the actions that would be taken to transition " +
            "from the " + Shell.eolt + "starting topology to the named, " +
            "target topology. If -start is not " + Shell.eolt + "specified " +
            "the current topology is used. This command should be used " +
            Shell.eolt +  "before deploying a new topology.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyPreviewSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteRequiredStartArg()
        throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String arg = "-start";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String arg0 = "-name";
        final String arg1 = "TEST_TOPOLOGY";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + arg1 + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args = {command, arg0, arg1};
        final List<String> topoList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg1 + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteNoStartTopo() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final String expectedResult =
            "store=" + topoName + "  numPartitions=0 sequence=0" +
            Shell.eol + Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.preview(topoName, null, Boolean.TRUE,
                          SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                          NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getVerbose()).andReturn(Boolean.TRUE);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteStartTopoDne()
        throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String startFlag = "-start";
        final String startName = "START_TOPOLOGY";

        final String[] args =
            {command, nameFlag, topoName, startFlag, startName};

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + startName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);

        /* Exclude startName from topoList to cause the DNE exception. */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + startName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteWithStartTopo() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String startFlag = "-start";
        final String startName = "START_TOPOLOGY";

        final String[] args =
            {command, nameFlag, topoName, startFlag, startName};

        final String expectedResult = "No differences in topologies.";

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        topoList.add(startName);

        final Topology testTopo = new Topology(topoName);
        final Topology startTopo = new Topology(startName);
        final Parameters params = new Parameters(topoName);
        final TopologyCandidate testCandidate =
            new TopologyCandidate(topoName, testTopo);
        final TopologyDiff diff =
            new TopologyDiff(startTopo, startName, testCandidate, params);
        final String diffStr = diff.display(Boolean.TRUE);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.preview(topoName, startName, Boolean.TRUE,
                          SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                          NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(diffStr).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getVerbose()).
            andReturn(Boolean.TRUE).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }

        reset(cs, shell);
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.preview(topoName, startName, Boolean.TRUE,
                          SerialVersion.ADMIN_CLI_JSON_V2_VERSION, NULL_AUTH,
                          SerialVersion.CURRENT)).
            andReturn(diff.displayJson(false).toString()).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getVerbose()).
            andReturn(Boolean.TRUE).anyTimes();
        replay(shell);
        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ArrayNode changeZones =
            (ArrayNode)scr.getReturnValue().get("changeZones");
        assertEquals(changeZones.size(), 0);
    }

    @Test
    public void testTopologyPreviewSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String startFlag = "-start";
        final String startName = "START_TOPOLOGY";

        final String[] args =
            {command, nameFlag, topoName, startFlag, startName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        topoList.add(startName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.preview(topoName, startName, Boolean.TRUE,
                          SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                          NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getVerbose()).andReturn(Boolean.TRUE);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyPreviewSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyPreviewSub subObj =
            new TopologyCommand.TopologyPreviewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String startFlag = "-start";
        final String startName = "START_TOPOLOGY";

        final String[] args =
            {command, nameFlag, topoName, startFlag, startName};

        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        topoList.add(startName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.preview(topoName, startName, Boolean.TRUE,
                          SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                          NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getVerbose()).andReturn(Boolean.TRUE);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 8. Test case coverage for: TopologyCommand.TopologyRebalanceSub. */

    @Test
    public void testTopologyRebalanceSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String expectedResult =
            "topology rebalance -name <name> -pool " +
            "<pool name> [-zn <id> | -znname <name>] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyRebalanceSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String expectedResult =
            "Modifies the named topology to create a \"balanced\" " +
            "topology. If the" + Shell.eolt + "optional -zn flag is used " +
            "only storage nodes from the specified" + Shell.eolt +
            "zone will be used for the operation.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyRebalanceSubExecuteUnknownArg()
        throws Exception {
        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteRequiredPoolArg()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-pool";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteRequiredDcIdArg()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-zn";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteRequiredDcNameArg()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-znname";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteDcIdInvalidArg()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String dcIdFlag = "-zn";
        final String dcId = "INVALID_DC_ID";
        final String expectedResult = "Invalid zone ID: " + dcId;

        final String[] args = {command, dcIdFlag, dcId};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected");
        } catch (ShellUsageException e) {
            assertEquals(expectedResult, e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-name";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, poolFlag, poolName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecutePoolNull() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String arg = "-pool";
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, nameFlag, topoName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + topoName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> topoList = new ArrayList<String>();
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + topoName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecutePoolDne() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Pool does not exist: " + poolName, subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteDcIdDne() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final int dcIdVal = 1;
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName};

        final ShellUsageException expectedException =
            new ShellUsageException("Zone does not exist: " + dcName,
                                    subObj);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        final Topology testTopo = new Topology(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but zone " + dcName +
                 " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteDcDne() throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final DatacenterId dcid = new DatacenterId(1);
        final String dcNameFlag = "-znname";
        final String dcName = dcid.toString();
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName};

        final ShellUsageException expectedException =
            new ShellUsageException("Zone does not exist: " + dcid,
                                    subObj);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);
        /* To cause the dc lookup by id to fail in ensureDatacenterExists */
        final Topology dneTopo = new Topology("NON_EXISTENT_TOPO");

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(dneTopo);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but zone " + dcid +
                 " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(
            cs.rebalanceTopology(topoName, poolName, dcid,
                                 NULL_AUTH, SerialVersion.CURRENT))
                .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * Exercises rebalanceTopology with a null Datacenter.
     */
    @Test
    public void testTopologyRebalanceSubExecuteNoDcReturnEmpty()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(
            cs.rebalanceTopology(topoName, poolName, null,
                                 NULL_AUTH, SerialVersion.CURRENT))
                .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /**
     * Exercises rebalanceTopology with a non-null Datacenter.
     */
    @Test
    public void testTopologyRebalanceSubExecuteReturnEmpty()
        throws Exception {

        final TopologyCommand.TopologyRebalanceSub subObj =
            new TopologyCommand.TopologyRebalanceSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final int dcIdVal = 1;
        final DatacenterId dcid = new DatacenterId(dcIdVal);
        final String dcNameFlag = "-znname";
        final String dcName = DatacenterId.DATACENTER_PREFIX + dcIdVal;
        final Datacenter dc =
            Datacenter.newInstance(dcName, 3, DatacenterType.PRIMARY, false,
                                   false);

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName,
             dcNameFlag, dcName};

        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        final Topology testTopo = new Topology(topoName);
        testTopo.add(dc);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(testTopo);
        expect(
            cs.rebalanceTopology(topoName, poolName, dcid,
                                 NULL_AUTH, SerialVersion.CURRENT))
                .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRebalanceSubExecuteDcDeprecation()
        throws Exception {

        testTopologyRebalanceSubExecute(
            "",
            "topology", "rebalance", "-name", "MyTopo", "-pool", "MyPool",
            "-zn", "zn1");
        testTopologyRebalanceSubExecute(
            TopologyRebalanceSub.dcFlagsDeprecation,
            "topology", "rebalance", "-name", "MyTopo", "-pool", "MyPool",
            "-zn", "dc1");
        testTopologyRebalanceSubExecute(
            TopologyRebalanceSub.dcFlagsDeprecation,
            "topology", "rebalance", "-name", "MyTopo", "-pool", "MyPool",
            "-dc", "zn1");
        testTopologyRebalanceSubExecute(
            "",
            "topology", "rebalance", "-name", "MyTopo", "-pool", "MyPool",
            "-znname", "MyZone");
        testTopologyRebalanceSubExecute(
            TopologyRebalanceSub.dcFlagsDeprecation,
            "topology", "rebalance", "-name", "MyTopo", "-pool", "MyPool",
            "-dcname", "MyZone");
    }

    private void testTopologyRebalanceSubExecute(final String deprecation,
                                                 final String... cmd)
        throws Exception {

        /* Establish mocks */
        reset(cs, shell);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        final Topology topo = new Topology("MyTopo");
        topo.add(Datacenter.newInstance("MyZone", 1,
                                        DatacenterType.PRIMARY, false, false));
        expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(topo);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(singletonList("MyPool"));
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn(singletonList("MyTopo"));
        expect(cs.rebalanceTopology("MyTopo", "MyPool", new DatacenterId(1),
                                    NULL_AUTH, SerialVersion.CURRENT))
            .andStubReturn("");
        replay(cs);

        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Execute command and check result */
        assertEquals("Command result",
                     deprecation,
                     new TopologyCommand().execute(cmd, shell));

        final ShellCommandResult scr =
            new TopologyCommand().executeJsonOutput(cmd, shell);
        assertEquals(scr.getOperation(), "topology rebalance");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertNull(scr.getReturnValue());
    }

    /* 9. Test case coverage for: TopologyCommand.TopologyRedistributeSub. */

    @Test
    public void testTopologyRedistributeSubGetCommandSyntax()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String expectedResult =
            "topology redistribute -name <name> -pool <pool name> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyRedistributeSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String expectedResult =
            "Modifies the named topology to redistribute resources " +
            "to more efficiently" + Shell.eolt + "use those available.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyRedistributeSubExecuteUnknownArg()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteRequiredPoolArg()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String arg = "-pool";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String arg = "-name";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, poolFlag, poolName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecutePoolNull() throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String arg = "-pool";
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, nameFlag, topoName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + topoName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> topoList = new ArrayList<String>();
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + topoName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecutePoolDne() throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Pool does not exist: " + poolName, subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteEmptyTopo()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final String expectedResult =
            "store=" + topoName + "  numPartitions=0 sequence=0" +
            Shell.eol + Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.redistributeTopology(topoName, poolName,
                                       NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "topology redistribute");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertNull(scr.getReturnValue());
    }

    @Test
    public void testTopologyRedistributeSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.redistributeTopology(topoName, poolName,
                                       NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRedistributeSubExecuteReturnEmpty()
        throws Exception {

        final TopologyCommand.TopologyRedistributeSub subObj =
            new TopologyCommand.TopologyRedistributeSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.redistributeTopology(topoName, poolName,
                                       NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 10. Test case coverage for: TopologyCommand.TopologyValidateSub. */

    @Test
    public void testTopologyValidateSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String expectedResult =
            "topology validate [-name <name>] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyValidateSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String expectedResult =
            "Validates the specified topology. If no name is given, " +
            "the current " + Shell.eolt +
            "topology is validated. Validation will generate " +
            "\"violations\" and " + Shell.eolt + "\"notes\". Violations are " +
            "issues that can cause problems and should be " + Shell.eolt +
            "investigated. Notes are informational and highlight " +
            "configuration " + Shell.eolt +
            "oddities that could be potential issues or could be expected.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyValidateSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyValidateSubExecuteRequiredArg() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyValidateSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String arg = "-name";
        final String expectedResult = "Flag " + arg + " requires an argument";
        final String[] args = {command};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.validateTopology(null,
                                   SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                   NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(expectedResult);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyValidateSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String arg0 = "-name";
        final String arg1 = "TEST_TOPOLOGY";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + arg1 + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args = {command, arg0, arg1};
        final List<String> topoList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg1 + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyValidateSubExecuteEmptyTopo() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final String expectedResult =
            "store=" + topoName + "  numPartitions=0 sequence=0" +
            Shell.eol + Shell.eol + "  numShards=0" +
            Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.validateTopology(
            topoName, SerialVersion.ADMIN_CLI_JSON_V1_VERSION, NULL_AUTH,
            SerialVersion.CURRENT)).andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        reset(cs, shell);
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(
            cs.validateTopology(topoName,
                                SerialVersion.ADMIN_CLI_JSON_V2_VERSION,
                                NULL_AUTH,SerialVersion.CURRENT)).
            andReturn(JsonUtils.createObjectNode().toString()).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);
        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "topology validate");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
    }

    @Test
    public void testTopologyValidateSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.validateTopology(topoName,
                                   SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                   NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyValidateSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyValidateSub subObj =
            new TopologyCommand.TopologyValidateSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String[] args = {command, nameFlag, topoName};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.validateTopology(topoName,
                                   SerialVersion.ADMIN_CLI_JSON_V1_VERSION,
                                   NULL_AUTH, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 11. Test case coverage for: TopologyCommand.TopologyViewSub. */

    @Test
    public void testTopologyViewSubGetCommandSyntax() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String expectedResult =
            "topology view -name <name> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyViewSubGetCommandDescription() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String expectedResult =
            "Displays details of the specified topology.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyViewSubExecuteUnknownArg() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyViewSubExecuteRequiredArg() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyViewSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyViewSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String arg0 = "-name";
        final String arg1 = "TEST_TOPOLOGY";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + arg1 + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args = {command, arg0, arg1};
        final List<String> topoList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg1 + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyViewSubExecuteEmptyTopo() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final String expectedResult =
            "store=" + topoName + "  numPartitions=0 sequence=0" +
            Shell.eol + Shell.eol + "  numShards=0" +
            Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        final Topology testTopo = new Topology(topoName);
        final TopologyCandidate topoCandidate =
            new TopologyCandidate(topoName, testTopo);
        final Parameters params = new Parameters(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT)).
            andReturn(topoCandidate).anyTimes();
        expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(params).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getVerbose()).
            andReturn(Boolean.TRUE).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        final ShellCommandResult scr =
            subObj.executeJsonOutput(args, shell);
        final ObjectNode returnValue = scr.getReturnValue();
        assertEquals(returnValue.get("storeName").asText(), "TEST_TOPOLOGY");
    }

    @Test
    public void testTopologyViewSubExecuteRemoteException() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final String[] args = {command, nameFlag, topoName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyViewSubExecuteReturnEmpty() throws Exception {

        final TopologyCommand.TopologyViewSub subObj =
            new TopologyCommand.TopologyViewSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String[] args = {command, nameFlag, topoName};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = null;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList);
        expect(cs.getTopologyCandidate(topoName, NULL_AUTH,
                                       SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 11. Test case coverage for: TopologyCommand.TopologyContractSub. */

    @Test
    public void testTopologyContractSubGetCommandSyntax()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String expectedResult =
            "topology contract -name <name> -pool <pool name> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyContractSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String expectedResult =
            "Modifies the named topology to contract storage nodes.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyContractSubExecuteUnknownArg()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteRequiredNameArg()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteRequiredPoolArg()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String arg = "-pool";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteNameNull() throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String arg = "-name";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, poolFlag, poolName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecutePoolNull() throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String arg = "-pool";
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, nameFlag, topoName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteTopoDne() throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Topology " + topoName + " does not exist. " +
                "Use topology list to see existing candidates.", subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> topoList = new ArrayList<String>();
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + topoName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecutePoolDne() throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Pool does not exist: " + poolName, subObj);
        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final List<String> poolList = new ArrayList<String>();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + poolName + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteEmptyTopo()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final String expectedResult =
            "store=" + topoName + "  numPartitions=0 sequence=0" +
            Shell.eol + Shell.eol;

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.listSnapshots(null, NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(EMPTY_STRING_LIST).anyTimes();
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT).
            anyTimes();
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(poolList).anyTimes();
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(topoList).anyTimes();
        expect(cs.contractTopology(topoName, poolName, NULL_AUTH,
                                   SerialVersion.CURRENT)).
            andReturn(expectedResult).anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        final ShellCommandResult scr = subObj.executeJsonOutput(args, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getOperation(), "topology contract");
    }

    @Test
    public void testTopologyContractSubExecuteRemoteException()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.contractTopology(topoName, poolName, NULL_AUTH,
                                   SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyContractSubExecuteReturnEmpty()
        throws Exception {

        final TopologyCommand.TopologyContractSub subObj =
            new TopologyCommand.TopologyContractSub();
        final String nameFlag = "-name";
        final String topoName = "TEST_TOPOLOGY";
        final String poolFlag = "-pool";
        final String poolName = "TEST_POOL";

        final String[] args =
            {command, nameFlag, topoName, poolFlag, poolName};
        final RemoteException expectedRemoteException = new RemoteException();

        final String expectedResult = "";

        /* Objects to be used with the mock objects */
        final List<String> topoList = new ArrayList<String>();
        topoList.add(topoName);
        final List<String> poolList = new ArrayList<String>();
        poolList.add(poolName);

        /* Establish what is expected from each mock for this test */
        expect(cs.listSnapshots(null, NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(EMPTY_STRING_LIST);
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getStorageNodePoolNames(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(poolList);
        expect(cs.listTopologies(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(topoList);
        expect(cs.contractTopology(topoName, poolName, NULL_AUTH,
                                   SerialVersion.CURRENT))
            .andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = subObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /* 12. Test case coverage for: TopologyCommand.TopologyRemoveShardSub. */

    @Test
    public void testTopologyRemoveShardSubGetCommandSyntax()
        throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String expectedResult =
            "topology remove-shard -failed-shard <shardId> -name " +
            "<topology name> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTopologyRemoveShardSubGetCommandDescription()
        throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String expectedResult =
                "Removes a failed shard from a topology";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTopologyRemoveShardSubUnknownArg()
        throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: unknownArgument */
        shell.unknownArgument(arg, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRemoveShardSubExecuteRequiredTopoArg()
        throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String arg = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRemoveShardSubExecuteRequiredShardArg()
        throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String arg = "-failed-shard";
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = {command, arg};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRemoveShardSubExecuteShardNull() throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String arg = "-failed-shard";
        final String topoFlag = "-name";
        final String topoName = "TEST_TOPO";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, topoFlag, topoName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRemoveShardSubExecuteTopoNull() throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String arg = "-name";
        final String nameFlag = "-failed-shard";
        final String shardName = "rg2";

        final ShellUsageException expectedException =
            new ShellUsageException("Missing required argument for command: " +
                                    command, subObj);

        final String[] args = {command, nameFlag, shardName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: requiredArg */
        shell.requiredArg(null, subObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            subObj.execute(args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testTopologyRemoveShardSubInvalidShardName() throws Exception {

        final TopologyCommand.TopologyRemoveShardSub subObj =
            new TopologyCommand.TopologyRemoveShardSub();
        final String shardName = "abc2";
        final ShellUsageException expectedException =
            new ShellUsageException(shardName + " is not a valid id." +
                                    " It must follow the format rgX", subObj);
        try {
            RepGroupId.parse(shardName);
        } catch (IllegalArgumentException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }
}
