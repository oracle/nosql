/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static oracle.kv.impl.util.TestUtils.assertMatch;
import static oracle.kv.impl.util.TestUtils.set;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellHelpException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.Test;

/**
 * Test the syntax of the repair-admin-quorum CLI command.
 */
public class RepairAdminQuorumCommandSyntaxTest extends TestBase {

    private ByteArrayOutputStream shellOutput;
    private CommandShell shell;
    private TestRepairAdminQuorumCommand command;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        command = new TestRepairAdminQuorumCommand();
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
            execute("repair-admin-quorum -help");
            fail("Expected ShellHelpException");
        } catch (ShellHelpException e) {
            assertMatch("(?s)Usage: repair-admin-quorum.*",
                        e.getHelpMessage());
            assertMatch("(?s)Usage: repair-admin-quorum.*",
                        e.getVerboseHelpMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testTooShortAbbreviation()
        throws Exception {

        assertNull("Command", shell.findCommand("repair-"));
    }

    @Test
    public void testSubcommandAvailable()
        throws Exception {

        assertNotNull("Command", shell.findCommand("repair-a"));
        assertNotNull("Command", shell.findCommand("repair-admin-quorum"));
    }

    @Test
    public void testNoFlags()
        throws Exception {

        try {
            execute("repair-admin-quorum");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertMatch("Need to specify .* flags\n*", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testUnknownFlag()
        throws Exception {

        try {
            execute("repair-admin-quorum -blorp");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertMatch("Unknown argument: -blorp", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneIdFlagNoValue()
        throws Exception {

        try {
            execute("repair-admin-quorum -zn");
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
            execute("repair-admin-quorum -zn foo");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Invalid zone ID: foo", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testZoneNameFlagNoValue()
        throws Exception {

        try {
            execute("repair-admin-quorum -znname");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -znname requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testAdminIdFlagNoValue()
        throws Exception {

        try {
            execute("repair-admin-quorum -admin");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Flag -admin requires an argument", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testAdminIdFlagBadValue()
        throws Exception {

        try {
            execute("repair-admin-quorum -admin 3jane");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertEquals("Invalid Admin ID: 3jane", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testNoAdmins()
        throws Exception {

        try {
            execute("repair-admin-quorum");
            fail("Expected ShellUsageException");
        } catch (ShellUsageException e) {
            assertMatch("Need to specify .* flags\n*", e.getMessage());
        }
        assertEquals("Output", "", shellOutput.toString());
    }

    @Test
    public void testMiscArgs()
        throws Exception {

        execute("repair-a -znname Boston -zn 3" +
                " -znname SanFrancisco -admin admin5 -zn 4 -admin admin6");
        assertEquals("Zone IDs",
                     set(new DatacenterId(3), new DatacenterId(4)),
                     command.zoneIds);
        assertEquals("Zone names",
                     set("Boston", "SanFrancisco"),
                     command.zoneNames);
        assertEquals("Admin IDs",
                     set(new AdminId(5), new AdminId(6)),
                     command.adminIds);
        assertEquals("Output", "", shellOutput.toString());
    }

    /* Utilities */

    /** Capture arguments to repairAdminQuorum. */
    private static class TestRepairAdminQuorumCommand
            extends RepairAdminQuorumCommand {
        Set<DatacenterId> zoneIds;
        Set<String> zoneNames;
        Set<AdminId> adminIds;
        @SuppressWarnings("hiding")
        @Override
        Set<AdminId> repairAdminQuorum(CommandShell shell,
                                       Set<DatacenterId> zoneIds,
                                       Set<String> zoneNames,
                                       Set<AdminId> adminIds) {
            this.zoneIds = zoneIds;
            this.zoneNames = zoneNames;
            this.adminIds = adminIds;
            return Collections.<AdminId>emptySet();
        }
    }

    private void execute(String commandLine)
        throws ShellException {

        command.execute(commandLine.split(" "), shell);
    }
}
