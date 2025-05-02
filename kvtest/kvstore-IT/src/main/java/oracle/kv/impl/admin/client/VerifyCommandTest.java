/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/**
 * Verifies the functionality and error paths of the VerifyCommand class.
 */
public class VerifyCommandTest extends TestBase {

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

    /* 1. Test case coverage for: VerifyCommand.VerifyConfig. */

    @Test
    public void testVerifyConfigGetCommandSyntax() throws Exception {
        final VerifyCommand.VerifyConfig subObj =
            new VerifyCommand.VerifyConfig();
        final String expectedResult =
            "verify configuration [-silent] " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testVerifyConfigGetCommandDescription() throws Exception {
        final VerifyCommand.VerifyConfig subObj =
             new VerifyCommand.VerifyConfig();
        final String expectedResult =
            "Verifies the store configuration by iterating over the " +
            "components and checking" + Shell.eolt + "their state " +
            "against that expected in the Admin database.  This call " +
            "may" + Shell.eolt + "take a while on a large store.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testVerifyConfigIncorrectArgCount() throws Exception {
        final VerifyCommand.VerifyConfig subObj =
            new VerifyCommand.VerifyConfig();
        String[] args = {"configuration", "-silent", "pqr"};
        String msg = "Incorrect number of arguments for command: VerifyConfig";
        ShellUsageException expectedException =
            new ShellUsageException(msg, subObj);
        CommandShell shell = createMock(CommandShell.class);
        CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null));
        shell.badArgCount(subObj);
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
    public void testVerifyConfigUnknownArgument() throws Exception {
        final VerifyCommand.VerifyConfig subObj =
            new VerifyCommand.VerifyConfig();
        String[] args = {"configuration", "-abc"};
        String msg = "Unknown argument: " + args[1];
        ShellUsageException expectedException =
            new ShellUsageException(msg, subObj);
        CommandShell shell = createMock(CommandShell.class);
        CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null));
        shell.unknownArgument("-abc", subObj);
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
    public void testVerifyConfigExpectedBehavior() throws Exception {
        final VerifyCommand.VerifyConfig subObj =
            new VerifyCommand.VerifyConfig();

        String[][] args = {{"configuration"},
                           {"configuration", "-silent"}};
        boolean showProgress = true;
        boolean json = false;
        List<Problem> violations = new ArrayList<Problem>();
        List<Problem> warnings = new ArrayList<Problem>();
        final String dcName1 = "zn1";
        final String dcName2 = "zn2";
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;

        final String dummyHost2 = "dummy2.us.oracle.com";
        final int dummyRegistyPort2 = 5002;
        final String expectedDisplay = "test";
        /*
         * j = 0 : verify configuration
         * j = 1 : verify configuration -silent -json
         */
        for (int j = 0; j <= 1; j++) {
            warnings.clear();
            violations.clear();
            if (j == 1) {
                showProgress = false;
                json = true;
                violations.add(new Problem() {
                    @Override
                    public ResourceId getResourceId() {
                        return null;
                    }
                });
            } else {
                warnings.add(new Problem() {
                    @Override
                    public ResourceId getResourceId() {
                        return null;
                    }
                });
            }
            Topology topo = new Topology("TEST_TOPOLOGY");
            Datacenter dc1 = Datacenter
                .newInstance(dcName1, 1, DatacenterType.PRIMARY, false, false);

            Datacenter dc2 = Datacenter
                    .newInstance(dcName2, 1, DatacenterType.PRIMARY, false,
                                 false);
            topo.add(dc1);
            topo.add(dc2);
            StorageNode sn1 =
                new StorageNode(dc1, dummyHost1, dummyRegistyPort1);
            topo.add(sn1);
            /**
             * i = 0 : storage nodes sn1 and sn2 belong to zone dc1
             *         and zone dc2 is empty.
             * i = 1 : storage node sn1 belongs to zone dc1 and
             *         storage node sn2 belongs to zone dc2.
             *         There are no empty zones.
             */
            StorageNode sn2 =
                new StorageNode(dc2, dummyHost2, dummyRegistyPort2);
            topo.add(sn2);
            RepGroup rg = new RepGroup();
            topo.add(rg);

            RepNode rn1 = new RepNode(sn1.getResourceId());
            RepNode rn2 = new RepNode(sn1.getResourceId());
            RepNode rn3 = new RepNode(sn2.getResourceId());
            RepNode rn4 = new RepNode(sn2.getResourceId());

            rg.add(rn1); rg.add(rn2); rg.add(rn3); rg.add(rn4);

            CommandShell shell = createMock(CommandShell.class);
            CommandService cs = createMock(CommandService.class);
            /* Establish what is expected from each mock for this test */
            expect(cs.getSerialVersion()).
                andReturn(SerialVersion.CURRENT);
            final VerifyResults testVResults = new VerifyResults("test",
                violations, warnings);
            expect(cs.verifyConfiguration(showProgress, true, json,
                NULL_AUTH, SerialVersion.CURRENT))
                .andStubReturn(testVResults);
            replay(cs);

            expect(shell.getAdmin()).
                andReturn(CommandServiceAPI.wrap(cs, null));
            if(j==0) {
                expect(shell.getJson()).andReturn(false);
            } else {
                expect(shell.getJson()).andReturn(true);
            }
            replay(shell);
            String result = subObj.execute(args[j], shell);
            assertEquals(expectedDisplay, result);
            if(j==0) {
                assertEquals(0, subObj.getExitCode());
            } else {
                assertNotEquals(0, subObj.getExitCode());
            }
            shell = createMock(CommandShell.class);
            cs = createMock(CommandService.class);
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            final ObjectNode node = JsonUtils.createObjectNode();
            node.put(CommandJsonUtils.FIELD_OPERATION, "test");
            node.put(CommandJsonUtils.FIELD_DESCRIPTION, "test");
            node.put(CommandJsonUtils.FIELD_RETURN_CODE,
                     ErrorMessage.NOSQL_5000.getValue());
            final VerifyResults testJsonResults =
                new VerifyResults(node.toString(), violations, warnings);
            if (j == 0) {
                expect(cs.verifyConfiguration(
                       true, true, true, NULL_AUTH,
                       SerialVersion.CURRENT)).andReturn(testJsonResults);
            } else if (j == 1) {
                expect(cs.verifyConfiguration(
                    false, true, true, NULL_AUTH,
                    SerialVersion.CURRENT)).andReturn(testJsonResults);
            }
            replay(cs);

            expect(shell.getAdmin()).
                andReturn(CommandServiceAPI.wrap(cs, null));
            expect(shell.getJson()).andStubReturn(true);
            replay(shell);
            final ShellCommandResult scr =
                subObj.executeJsonOutput(args[j], shell);
            assertEquals(scr.getOperation(), "test");
            assertEquals(scr.getDescription(), "test");
            assertEquals(scr.getReturnCode(),
                         ErrorMessage.NOSQL_5000.getValue());
        }
    }

    /* 2. Test case coverage for: VerifyCommand.VerifyPrerequisite. */

    @Test
    public void testVerifyPrerequisiteGetCommandSyntax() throws Exception {
        final VerifyCommand.VerifyPrerequisite subObj = 
                new VerifyCommand.VerifyPrerequisite();
        final String expectedResult = "verify prerequisite [-silent] [-sn snX]* " +
                CommandParser.getJsonUsage();

        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testVerifyPrerequisiteGetCommandDescription() throws Exception {
        final VerifyCommand.VerifyPrerequisite subObj = 
                new VerifyCommand.VerifyPrerequisite();
        final String expectedResult = 
                "Verifies the storage nodes are at or above the " +
                "prerequisite software version needed to upgrade to " +
                "the current version." + Shell.eolt +
                "This call may take a while on a large store.";

        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testVerifyPrerequisiteUnknownArgument() throws Exception {
        final VerifyCommand.VerifyPrerequisite subObj =
                new VerifyCommand.VerifyPrerequisite();
        String[] args = { "prerequisite", "-abc" };
        String msg = "Unknown argument: " + args[1];
        ShellUsageException expectedException = 
                new ShellUsageException(msg, subObj);
        CommandShell shell = createMock(CommandShell.class);
        CommandService cs = createMock(CommandService.class);
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);
        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        shell.unknownArgument("-abc", subObj);
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
    public void testVerifyPrerequisiteExpectedBehavior() throws Exception {
        final VerifyCommand.VerifyPrerequisite subObj = 
                new VerifyCommand.VerifyPrerequisite();
        List<Problem> violations = new ArrayList<Problem>();
        List<Problem> warnings = new ArrayList<Problem>();
        /* topology setup */
        final String dummyHost1 = "dummy1.us.oracle.com";
        final int dummyRegistyPort1 = 5001;
        final String dcName1 = "zn1";
        Topology topo = new Topology("TEST_TOPOLOGY");
        Datacenter dc1 = Datacenter
                .newInstance(dcName1, 1, DatacenterType.PRIMARY,
                        false, false);
        topo.add(dc1);
        StorageNode sn1 = new StorageNode(dc1, dummyHost1, dummyRegistyPort1);
        topo.add(sn1);
        RepGroup rg = new RepGroup();
        topo.add(rg);
        RepNode rn1 = new RepNode(sn1.getResourceId());
        RepNode rn2 = new RepNode(sn1.getResourceId());
        rg.add(rn1);
        rg.add(rn2);

        boolean json = true;
        boolean showProgress = true;
        String expectedDisplay = "test";
        final String[][] args = { { "prerequisite" },
                { "prerequisite" },
                { "prerequisite", "-silent" },
                { "prerequisite", "-silent", "-sn", "sn1" } };
        /*
         * j = 0 : verify prerequisite (isSecuredAdmin() == false)
         * j = 1 : verify prerequisite -json (isSecuredAdmin() == true)
         * j = 2 : verify prerequisite -silent (isSecuredAdmin() == true)
         * j = 3 : verify prerequisite -silent -sn sn1 (isSecuredAdmin() == true)
         */
        for (int j = 0; j <= 3; j++) {
            warnings.clear();
            violations.clear();
            if (j >= 2) {
                showProgress = false;
                violations.add(new Problem() {
                    @Override
                    public ResourceId getResourceId() {
                        return null;
                    }
                });
            } else {
                warnings.add(new Problem() {
                    @Override
                    public ResourceId getResourceId() {
                        return null;
                    }
                });
            }
            /* Establish what is expected from each mock for this test */
            CommandShell shell = createMock(CommandShell.class);
            CommandService cs = createMock(CommandService.class);
            expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
            expect(cs.getTopology(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(topo);
            Parameters params = new Parameters("test");
            StorageNodeParams snp1 = new StorageNodeParams(sn1.getStorageNodeId(),
                    sn1.getHostname(), sn1.getRegistryPort(), "");
            snp1.setSearchClusterName("dtc");
            params.add(snp1);
            expect(cs.getParameters(NULL_AUTH, SerialVersion.CURRENT)).
                    andStubReturn(params);
            final VerifyResults testVResults =
                    new VerifyResults("test", violations, warnings);
            List<StorageNodeId> snIds = null;
            if (j == 0) {
                expect(shell.isSecuredAdmin()).andStubReturn(false);
            } else {
                expect(shell.isSecuredAdmin()).andStubReturn(true);
            }
            if (j == 1) {
                expect(shell.getJson()).andStubReturn(true);
                json = true;
            } else {
                expect(shell.getJson()).andStubReturn(false);
                json = false;
            }
            if (j > 1) {
                expectedDisplay =
                    "Verification complete, 1 violation,  found." + Shell.eol +
                    "Verification violation: [sn1]\tA registered ES cluster " +
                    "is not allowed for a secure store." + Shell.eol;
                if (j == 3) {
                    snIds = topo.getStorageNodeIds();
                }
            }
            expect(cs.verifyPrerequisite(KVVersion.CURRENT_VERSION,
                    KVVersion.PREREQUISITE_VERSION,
                    snIds,
                    showProgress,
                    true,
                    json,
                    NULL_AUTH,
                    SerialVersion.CURRENT)).andStubReturn(testVResults);
            replay(cs);
            expect(shell.getAdmin()).
                    andStubReturn(CommandServiceAPI.wrap(cs, null));
            replay(shell);

            /* Execute seccion */
            if (j == 1) {
                final ShellCommandResult scr =
                    subObj.executeJsonOutput(args[j], shell);
                assertEquals(scr.getReturnCode(),
                    ErrorMessage.NOSQL_5200.getValue());
                assertEquals(scr.getOperation(),
                    "verify prerequisite -json");
            } else {
                String result = subObj.execute(args[j], shell);
                assertEquals(expectedDisplay, result);
            }
        }
    }
}
