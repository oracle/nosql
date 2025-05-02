/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static oracle.kv.impl.util.TestUtils.assertMatch;
import static oracle.kv.impl.util.TestUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellHelpException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.Test;

/**
 * Test the syntax of the 'plan failover' CLI command.
 */
public class PlanFailoverCommandSyntaxTest extends TestBase {

    private ByteArrayOutputStream shellOutput;
    private CommandShell shell;
    private TestFailoverSub command;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        command = new TestFailoverSub();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.setUp();
    }

    /* Tests */

    @Test
    public void testHelp()
        throws Exception {

        try {
            execute("failover -help");
            fail("Expected ShellHelpException");
        } catch (ShellHelpException e) {
            assertMatch("(?s)Usage: plan failover.*",
                        e.getHelpMessage());
            assertMatch("(?s)Usage: plan failover.*",
                        e.getVerboseHelpMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testTooShortAbbreviation()
        throws Exception {

        assertNull("Failove subcommand",
                   new PlanCommand().findCommand("failove"));
    }

    @Test
    public void testSubcommandAvailable()
        throws Exception {

        assertNotNull("Failover subcommand",
                      new PlanCommand().findCommand("failover"));
    }

    @Test
    public void testNoFlags()
        throws Exception {

        try {
            execute("failover");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Must specify at least one offline-secondary zone",
                         e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testUnknownFlag()
        throws Exception {

        try {
            execute("failover -blorp");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Invalid argument: -blorp", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagNoValues()
        throws Exception {

        try {
            execute("failover -zn");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -zn requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagBadValue()
        throws Exception {

        try {
            execute("failover -zn blorp");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Invalid zone ID: blorp", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagNoType()
        throws Exception {

        try {
            execute("failover -zn zn1");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals(
                "Missing required argument (-type) for command: failover",
                e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagNoTypeValue()
        throws Exception {

        try {
            execute("failover -zn zn1 -type");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -type requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagBadTypeValue()
        throws Exception {

        try {
            execute("failover -zn zn1 -type blorp");
            fail("Expected ShellArgumentException");
        } catch (ShellArgumentException e) {
            assertMatch("(?s)Invalid argument: blorp.*", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneNameFlagNoValues()
        throws Exception {

        try {
            execute("failover -znname");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -znname requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneNameFlagNoType()
        throws Exception {

        try {
            execute("failover -znname Boston");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals(
                "Missing required argument (-type) for command: failover",
                e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneNameFlagNoTypeValue()
        throws Exception {

        try {
            execute("failover -znname Boston -type");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -type requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneNameFlagBadTypeValue()
        throws Exception {

        try {
            execute("failover -znname Boston -type blorp");
            fail("Expected ShellArgumentException");
        } catch (ShellArgumentException e) {
            assertMatch("(?s)Invalid argument: blorp.*", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdBothTypes()
        throws Exception {

        try {
            execute("failover -znname Boston -type primary" +
                    " -zn zn1 -type offline-secondary");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Zone zn1 was specified with multiple types",
                         e.getMessage());
        }
    }

    @Test
    public void testZoneNameBothTypes()
        throws Exception {

        try {
            execute("failover -zn zn1 -type offline-secondary" +
                    " -znname Boston -type primary");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Zone Boston was specified with multiple types",
                         e.getMessage());
        }
    }

    @Test
    public void testNoOfflineZones()
        throws Exception {

        try {
            execute("failover -znname Boston -type primary");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Must specify at least one offline-secondary zone",
                         e.getMessage());
        }
    }

    @Test
    public void testMisc()
        throws Exception {

        final String result =
            execute("failover -znname Boston -type primary" +
                    " -zn zn3 -type offline-secondary" +
                    " -znname Chicago -type offline-secondary" +
                    " -zn zn4 -type primary");
        assertMatch("Plan [0-9]* ended successfully", result);
        assertEquals("Primary zones",
                     set(new DatacenterId(1), new DatacenterId(4)),
                     command.primaryZones);
        assertEquals("Offline zones",
                     set(new DatacenterId(2), new DatacenterId(3)),
                     command.offlineZones);
        assertEquals("Output", "", shellOutput.toString());
    }

    /* Utilities */

    /**
     * Subclass of FailoverSub that provides client-side implementations of
     * remote methods, to simplify testing.
     */
    private static class TestFailoverSub extends PlanCommand.FailoverSub {
        final Map<String, Integer> dcNameMap = new HashMap<String, Integer>();
        Set<DatacenterId> primaryZones;
        Set<DatacenterId> offlineZones;
        TestFailoverSub() {
            dcNameMap.put("Boston", 1);
            dcNameMap.put("Chicago", 2);
        }
        @Override
        CommandServiceAPI getAdmin(CommandShell cmd) {
            return null;
        }
        @Override
        DatacenterId getDatacenterId(CommandShell cmd,
                                     CommandServiceAPI cs,
                                     String dcName)
            throws ShellException {

            if (dcNameMap.containsKey(dcName)) {
                return new DatacenterId(dcNameMap.get(dcName));
            }
            throw new ShellException("Zone not found: " + dcName);
        }
        @SuppressWarnings("hiding")
        @Override
        String failover(CommandShell shell,
                        CommandServiceAPI cs,
                        Set<DatacenterId> primaryZones,
                        Set<DatacenterId> offlineZones) {
            this.primaryZones = primaryZones;
            this.offlineZones = offlineZones;
            return "Plan 1 ended successfully";
        }
    }

    private String execute(String commandLine)
        throws ShellException {

        return command.exec(commandLine.split(" "), shell);
    }
}
