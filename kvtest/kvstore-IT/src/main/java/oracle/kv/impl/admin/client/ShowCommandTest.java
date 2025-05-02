/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static oracle.kv.util.shell.Shell.eol;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyShort;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.ShowCommand.ShowDatacenters;
import oracle.kv.impl.admin.client.ShowCommand.ShowTopology;
import oracle.kv.impl.admin.client.ShowCommand.ShowZones;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellUsageException;


import org.junit.Test;

/**
 * Verifies the ShowCommand class.
 */
public class ShowCommandTest extends TestBase {

    private static final CommandShell shell = createMock(CommandShell.class);
    private static final CommandService cs = createMock(CommandService.class);
    private static final AuthContext NULL_AUTH = null;

    @Test
    public void testShowPlansSubExecuteMultiPlanJsonMode()
        throws Exception {

        /* Establish mocks */
        reset(cs, shell);
        SortedMap<Integer, Plan> sortedPlans = new TreeMap<Integer, Plan>();
        for (int i = 1; i < 4; i++) {
            Plan plan = createMock(Plan.class);
            expect(plan.getId()).andReturn(i);
            expect(plan.getName()).andReturn("name" + i);
            if (i == 1) {
                expect(plan.getState()).andReturn(State.ERROR);
            } else {
                expect(plan.getState()).andReturn(State.SUCCEEDED);
            }
            replay(plan);
            sortedPlans.put(i, plan);
        }
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.CURRENT);
        expect(cs.getPlanIdRange(anyLong(), anyLong(), anyInt(),
                                 anyObject(AuthContext.class), anyShort())).
            andStubReturn(new int[] { 1, 3 });
        expect(cs.getPlanRange(1, 3, null, SerialVersion.CURRENT)).
            andStubReturn(sortedPlans);
        replay(cs);
        expect(shell.getAdmin()).andStubReturn(
            CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andStubReturn(true);
        replay(shell);

        String[] cmd = new String[] { "show", "plans" };
        final String result = new ShowCommand().execute(cmd, shell);
        JsonNode resultNode = JsonUtils.parseJsonNode(result);
        assertEquals("show plans",
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
        JsonNode planNodes = valueNode.get("plans");
        int i = 1;
        for (JsonNode planNode : planNodes) {
            if (i > 3) {
                fail("show more plans than expect.");
            }
            assertEquals(planNode.get("id").asInt(), i);
            assertEquals(planNode.get("name").asText(), "name" + i);
            if (i == 1) {
                assertEquals(planNode.get("state").asText(), "ERROR");
            } else {
                assertEquals(planNode.get("state").asText(), "SUCCEEDED");
            }
            ++i;
        }
    }

    @Test
    public void testShowTopologySubExecuteDcDeprecation()
        throws Exception {

        testShowTopologySubExecute("",
                                   "show", "topology", "-zn");
        testShowTopologySubExecute(ShowTopology.dcFlagsDeprecation,
                                   "show", "topology", "-dc");
    }

    private void testShowTopologySubExecute(final String deprecation,
                                            final String... cmd)
        throws Exception {

        /* Establish mocks */
        reset(cs, shell);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.MINIMUM);
        final Topology topo = new Topology("MyTopo");
        topo.add(Datacenter.newInstance("MyZone", 1,
                                        DatacenterType.PRIMARY, false, false));
        expect(cs.getTopology(NULL_AUTH, SerialVersion.MINIMUM))
            .andStubReturn(topo);
        expect(cs.getParameters(NULL_AUTH, SerialVersion.MINIMUM))
            .andStubReturn(new Parameters("MyStore"));
        replay(cs);

        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getVerbose());
        expectLastCall().andStubReturn(false);
        replay(shell);

        /* Execute command and check result */
        assertEquals("Command result",
                     deprecation +
                     "zn: id=zn1 name=MyZone repFactor=1 type=PRIMARY " +
                     "allowArbiters=false masterAffinity=false" +
                     eol + eol,
                     new ShowCommand().execute(cmd, shell));
        final ShellCommandResult scr =
            new ShowCommand().executeJsonOutput(cmd, shell);
        final ObjectNode returnValue = scr.getReturnValue();
        assertEquals(scr.getOperation(), "show topology");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        final ArrayNode znArray = (ArrayNode)returnValue.get("zns");
        final ObjectNode znNode = (ObjectNode)znArray.get(0);
        assertEquals("MyZone", znNode.get("name").asText());
        assertEquals(1, znNode.get("repFactor").asInt());
        assertEquals(false, znNode.get("allowArbiters").asBoolean());
        assertEquals(false, znNode.get("masterAffinity").asBoolean());
    }

    @Test
    public void testShowZonesSubExecuteDcDeprecation()
        throws Exception {

        testShowZonesSubExecute("",
                                "show", "zones", "-zn", "zn1");
        testShowZonesSubExecute(ShowZones.dcFlagsDeprecation,
                                "show", "zones", "-zn", "dc1");
        testShowZonesSubExecute(ShowZones.dcFlagsDeprecation,
                                "show", "zones", "-dc", "zn1");
        testShowZonesSubExecute(ShowZones.dcFlagsDeprecation,
                                "show", "zones", "-dc", "dc1");
        testShowZonesSubExecute("",
                                "show", "zones", "-znname", "MyZone");
        testShowZonesSubExecute(ShowZones.dcFlagsDeprecation,
                                "show", "zones", "-dcname", "MyZone");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation,
                                "show", "datacenters", "-zn", "zn1");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation +
                                ShowZones.dcFlagsDeprecation,
                                "show", "datacenters", "-zn", "dc1");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation +
                                ShowZones.dcFlagsDeprecation,
                                "show", "datacenters", "-dc", "zn1");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation +
                                ShowZones.dcFlagsDeprecation,
                                "show", "datacenters", "-dc", "dc1");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation,
                                "show", "datacenters", "-znname", "MyZone");
        testShowZonesSubExecute(ShowDatacenters.dcCommandDeprecation +
                                ShowZones.dcFlagsDeprecation,
                                "show", "datacenters", "-dcname", "MyZone");
    }

    private void testShowZonesSubExecute(final String deprecation,
                                         final String... cmd)
        throws Exception {

        /* Establish mocks */
        reset(cs, shell);
        expect(cs.getSerialVersion()).andStubReturn(SerialVersion.MINIMUM);
        final Topology topo = new Topology("MyTopo");
        topo.add(Datacenter.newInstance("MyZone", 1,
                                        DatacenterType.PRIMARY, false, false));
        expect(cs.getTopology(NULL_AUTH, SerialVersion.MINIMUM))
            .andStubReturn(topo);
        expect(cs.getParameters(NULL_AUTH, SerialVersion.MINIMUM))
            .andStubReturn(new Parameters("MyStore"));
        replay(cs);

        expect(shell.getAdmin())
            .andStubReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Execute command and check results */
        assertEquals("Command result",
                     deprecation +
                     "zn: id=zn1 name=MyZone repFactor=1 type=PRIMARY " +
                     "allowArbiters=false masterAffinity=false"+ eol,
                     new ShowCommand().execute(cmd, shell));

        final ShellCommandResult scr =
            new ShowCommand().executeJsonOutput(cmd, shell);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getOperation(), "show zones");
        final ArrayNode zoneArray = (ArrayNode)scr.getReturnValue().get("zns");
        final ObjectNode znNode = (ObjectNode)zoneArray.get(0);
        assertEquals(znNode.get("name").asText(), "MyZone");
        assertEquals(znNode.get("repFactor").asInt(), 1);
        assertEquals(znNode.get("allowArbiters").asBoolean(), false);
        assertEquals(znNode.get("masterAffinity").asBoolean(), false);
        final ArrayNode snArray = (ArrayNode)znNode.get("sns");
        assertNotNull(snArray);
        for (int i = 0; i < snArray.size(); i += 1) {
            final ObjectNode snNode = (ObjectNode)snArray.get(i);
            assertEquals("sn" + i, snNode.get("resourceId").asText());
            assertEquals("localhost", snNode.get("hostname").asText());
            assertEquals(1, snNode.get("capacity").asInt());
        }
    }

    @Test
    public void testShowPoolsCommandSyntax() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        final String expectedOutput =
            "show pools [-name <name>] " +
            CommandParser.getJsonUsage();
        final String result = subObj.getCommandSyntax();
        assertEquals(expectedOutput, result);
    }

    @Test
    public void testShowPoolsCommandDescription() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        final String expectedOutput = "Lists the storage node pools";
        final String result = subObj.getCommandDescription();
        assertEquals(expectedOutput, result);
    }

    @Test
    public void testShowPoolsInvalidArgument() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        final String invalidArg = "-abc";
        String[] args = {"pools", invalidArg};
        String exceptionMessage = "Invalid argument: " + invalidArg;

        final CommandShell testShell = createMock(CommandShell.class);
        try {
            subObj.execute(args, testShell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    @Test
    public void testShowPoolsMissingPoolName() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        String[] args = {"pools", "-name"};
        final CommandShell testShell = createMock(CommandShell.class);
        String exceptionMessage = "Flag -name requires an argument";

        try {
            subObj.execute(args, testShell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    @Test
    public void testShowPoolsInvalidPoolName() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        final List<String> testPools = new ArrayList<String>();
        testPools.add("testPool1");
        testPools.add("testPool2");
        testPools.add("testPool3");

        String invalidPoolName = "testPool4";
        String[] args = {"pools", "-name", invalidPoolName};

        final String exceptionMessage =
            "Not a valid pool name: " + invalidPoolName;

        final CommandShell testShell = createMock(CommandShell.class);
        final CommandService testCs = createMock(CommandService.class);
        final Topology topo = new Topology("TEST_TOPOLOGY");
        /* Establish what is expected from each mock for this test */
        expect(testCs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(testCs.getStorageNodePoolNames(NULL_AUTH,
            SerialVersion.CURRENT)).andStubReturn(testPools);
        expect(testCs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
        .andStubReturn(topo);
        replay(testCs);

        expect(testShell.getAdmin()).andReturn(
                CommandServiceAPI.wrap(testCs,null));
        replay(testShell);

        try {
            subObj.execute(args, testShell);
            fail("ShellArgumentException expected, but required arg input");
        } catch (ShellArgumentException e) {
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    @Test
    public void testShowPoolsExpectedBehavior() throws Exception {
        final ShowCommand.ShowPools subObj = new ShowCommand.ShowPools();

        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dcName3 = "zn3";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;

        final String dummyHost3 = "dummy3.us.oracle.com";
        final int dummyRegistyPort3 = 5003;

        final Topology topo = new Topology("TEST_TOPOLOGY");
        final Datacenter dc1 = Datacenter
            .newInstance(dcName1, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc1);
        final Datacenter dc2 = Datacenter
            .newInstance(dcName2, 1, DatacenterType.PRIMARY, false, false);
        topo.add(dc2);
        final Datacenter dc3 = Datacenter
                .newInstance(dcName3, 1, DatacenterType.PRIMARY, false, false);
            topo.add(dc3);
        final StorageNode sn1 = new StorageNode(dc1, dummyHost1,
            dummyRegistyPort1);
        topo.add(sn1);
        final StorageNode sn2 = new StorageNode(dc2, dummyHost2,
            dummyRegistyPort2);
        topo.add(sn2);
        final StorageNode sn3 = new StorageNode(dc3, dummyHost3,
            dummyRegistyPort3);
        topo.add(sn3);

        final List<StorageNodeId> snList1 = new ArrayList<StorageNodeId>();
        snList1.add(sn1.getResourceId());
        snList1.add(sn2.getResourceId());
        snList1.add(sn3.getResourceId());

        final List<StorageNodeId> snList2 = new ArrayList<StorageNodeId>();
        snList2.add(sn1.getResourceId());
        snList2.add(sn2.getResourceId());

        final List<StorageNodeId> snList3 = new ArrayList<StorageNodeId>();
        snList3.add(sn3.getResourceId());

        final List<List<StorageNodeId>> allSnLists =
            new ArrayList<List<StorageNodeId>>();
        allSnLists.add(snList1);
        allSnLists.add(snList2);
        allSnLists.add(snList3);

        final List<String> testPools = new ArrayList<String>();
        testPools.add("testPool1");
        testPools.add("testPool2");
        testPools.add("testPool3");

        final String[][] args = {{"pools"},
                                 {"pools", "-name", "testPool1"},
                                 {"pools", "-name", "testPool2"},
                                 {"pools", "-name", "testPool3"}};
        final String expectedMessage1 =
            "testPool1: sn1 zn:[id=zn1 name=zn1], sn2 zn:[id=zn2 name=zn2],"
            + " sn3 zn:[id=zn3 name=zn3]" + Shell.eol +
            "testPool2: sn1 zn:[id=zn1 name=zn1], sn2 zn:[id=zn2 name=zn2]"
            + Shell.eol + "testPool3: sn3 zn:[id=zn3 name=zn3]" + Shell.eol;

        final String expectedMessage2 =
            "testPool1: sn1 zn:[id=zn1 name=zn1], sn2 zn:[id=zn2 name=zn2],"
            + " sn3 zn:[id=zn3 name=zn3]" + Shell.eol;

        final String expectedMessage3 =
            "testPool2: sn1 zn:[id=zn1 name=zn1], sn2 zn:[id=zn2 name=zn2]"
            + Shell.eol;

        final String expectedMessage4 =
            "testPool3: sn3 zn:[id=zn3 name=zn3]" + Shell.eol;
        final String[] expectedMessages =
            {expectedMessage1, expectedMessage2,
             expectedMessage3, expectedMessage4};

        for (int i=0; i < args.length; i++) {
            final CommandShell testShell = createMock(CommandShell.class);
            final CommandService testCs = createMock(CommandService.class);
            /* Establish what is expected from each mock for this test */
            expect(testCs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(testCs.getStorageNodePoolNames(NULL_AUTH,
                   SerialVersion.CURRENT)).andStubReturn(testPools);

            for (int j = 0; j < testPools.size(); j++) {
                expect(testCs.getStorageNodePoolIds(testPools.get(j)
                       , NULL_AUTH, SerialVersion.CURRENT))
                       .andStubReturn(allSnLists.get(j));
            }
            expect(testCs.getTopology(NULL_AUTH, SerialVersion.CURRENT))
                   .andStubReturn(topo);
            replay(testCs);

            expect(testShell.getAdmin()).andReturn(
                   CommandServiceAPI.wrap(testCs,null)).anyTimes();
            replay(testShell);
            String result = subObj.execute(args[i], testShell);
            assertEquals(result, expectedMessages[i]);
            final ShellCommandResult scr =
                subObj.executeJsonOutput(args[i], testShell);
            assertEquals(scr.getOperation(), "show pool");
            assertEquals(scr.getReturnCode(),
                         ErrorMessage.NOSQL_5000.getValue());
            if (i == 0) {
                final ArrayNode poolArray =
                    (ArrayNode)scr.getReturnValue().get("pools");
                final ObjectNode pool1 = (ObjectNode)poolArray.get(0);
                final ObjectNode pool2 = (ObjectNode)poolArray.get(1);
                final ObjectNode pool3 = (ObjectNode)poolArray.get(2);
                assertEquals(pool1.get("poolName").asText(), "testPool1");
                final ObjectNode snNode3 =
                    (ObjectNode)((ArrayNode)pool1.get("sns")).get(2);
                assertEquals(snNode3.get("resourceId").asText(), "sn3");
                assertEquals(snNode3.get("znId").asText(), "zn3");
                assertEquals(snNode3.get("zoneName").asText(), "zn3");
                assertEquals(pool2.get("poolName").asText(), "testPool2");
                assertEquals(pool3.get("poolName").asText(), "testPool3");
            }
        }
    }
}
