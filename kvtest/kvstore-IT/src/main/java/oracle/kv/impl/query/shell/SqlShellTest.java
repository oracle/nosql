/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.query.shell;

import static oracle.kv.impl.util.CommandParser.CONSISTENCY_TIME_FLAG;
import static oracle.kv.impl.util.CommandParser.MASTER_SYNC_FLAG;
import static oracle.kv.impl.util.CommandParser.PERMISSIBLE_LAG_FLAG;
import static oracle.kv.impl.util.CommandParser.PRETTY_FLAG;
import static oracle.kv.impl.util.CommandParser.REPLICA_ACK_FLAG;
import static oracle.kv.impl.util.CommandParser.REPLICA_SYNC_FLAG;
import static oracle.kv.impl.util.CommandParser.TIMEOUT_FLAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVVersion;
import oracle.kv.impl.query.shell.OnqlShell.ConsistencyCommand;
import oracle.kv.impl.query.shell.OnqlShell.DurabilityCommand;
import oracle.kv.impl.query.shell.OnqlShell.OutputCommand;
import oracle.kv.impl.query.shell.OnqlShell.OutputModeCommand;
import oracle.kv.impl.query.shell.OnqlShell.RequestTimeoutCommand;
import oracle.kv.impl.query.shell.OnqlShell.ShowCommand;
import oracle.kv.impl.query.shell.OnqlShell.ShowCommand.ShowQuery;
import oracle.kv.impl.query.shell.OnqlShell.VersionCommand;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.util.shell.CommonShell.ConnectStoreCommand;
import oracle.kv.util.shell.CommonShell.DebugCommand;
import oracle.kv.util.shell.CommonShell.HiddenCommand;
import oracle.kv.util.shell.CommonShell.HistoryCommand;
import oracle.kv.util.shell.CommonShell.NamespaceCommand;
import oracle.kv.util.shell.CommonShell.PageCommand;
import oracle.kv.util.shell.CommonShell.TimeCommand;
import oracle.kv.util.shell.CommonShell.VerboseCommand;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.Shell.CommandComparator;
import oracle.kv.util.shell.ShellCommand;

public class SqlShellTest extends ShellTestBase {

    @Test
    public void testCommandShellGetCommands()
        throws Exception {

        final OnqlShell shell = getTestShell();
        try {
            final List<? extends ShellCommand> shellCommands =
                shell.getCommands();
            final List<? extends ShellCommand> testCommands =
                Arrays.asList(new ConnectStoreCommand(),
                              new ConsistencyCommand(),
                              new DebugCommand(),
                              new DurabilityCommand(),
                              new Shell.ExitCommand(),
                              new Shell.HelpCommand(),
                              new HiddenCommand(),
                              new HistoryCommand(),
                              new ImportCommand(),
                              new NamespaceCommand(),
                              new Shell.LoadCommand(),
                              new OutputModeCommand(),
                              new OutputCommand(),
                              new PutCommand(),
                              new PageCommand(),
                              new RequestTimeoutCommand(),
                              new ShowCommand(),
                              new TimeCommand(),
                              new VerboseCommand(),
                              new VersionCommand());

            final CommandComparator cmdComparator = new CommandComparator();
            Collections.sort(shellCommands, cmdComparator);
            Collections.sort(testCommands, cmdComparator);

            assertTrue("Unexpected number of shell commands returned by " +
                       "CommandShell.getCommands [expected=" +
                       testCommands.size() + ", received=" +
                       shellCommands.size() + "]",
                       (testCommands.size() == shellCommands.size()));

            for (int i = 0; i < shellCommands.size(); i++) {
                final ShellCommand shellCmd = shellCommands.get(i);
                final ShellCommand testCmd = testCommands.get(i);

                assertTrue("Unexpected shell command returned by " +
                           "CommandShell.getCommands [expected=" +
                           testCmd.getClass().getSimpleName() +
                           ", received=" +
                           shellCmd.getClass().getSimpleName() + "]",
                           (cmdComparator.compare(shellCmd, testCmd) == 0));
            }
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testSetGetQueryOutput()
        throws Exception {

        final OnqlShell shell = getTestShell();
        final String outputFile = getTempFileName("query", ".out");
        assertTrue(shell.getQueryOutput() == shell.getOutput());
        assertTrue(shell.getQueryOutputFile() == null);

        shell.setQueryOutputFile(outputFile);
        assertTrue(new File(outputFile).exists());
        assertTrue(shell.getQueryOutput() != shell.getOutput());
        assertTrue(outputFile.equals(shell.getQueryOutputFile()));

        shell.setQueryOutputFile(null);
        assertTrue(shell.getQueryOutput() == shell.getOutput());
        assertTrue(shell.getQueryOutputFile() == null);
    }

    @Test
    public void testSetGetQueryOutputMode() {
        final OnqlShell shell = getTestShell();
        assertEquals(shell.getQueryOutputMode(),
                     OnqlShell.QUERY_OUTPUT_MODE_DEF);
        shell.setQueryOutputMode(OutputMode.LINE);
        assertEquals(shell.getQueryOutputMode(), OutputMode.LINE);
    }

    /* ConsistencyCommand coverage. */
    @Test
    public void testConsistencyCommandGetCommandDescription()
        throws Exception {

        final ConsistencyCommand consObj = new ConsistencyCommand();
        final String expectedResult = ConsistencyCommand.DESCRIPTION;
        assertEquals(expectedResult, consObj.getCommandDescription());
    }

    @Test
    public void testConsistencyCommandGetCommandSyntax()
        throws Exception {

        final ConsistencyCommand consObj = new ConsistencyCommand();
        final String expectedResult = ConsistencyCommand.SYNTAX;
        assertEquals(expectedResult, consObj.getCommandSyntax());
    }

    @Test
    public void testConsistencyCommandExecuteInvalidArgument() {
        final OnqlShell shell = getTestShell();
        final ConsistencyCommand consObj = new ConsistencyCommand();

        String[] args = { ConsistencyCommand.NAME, "INVALID" };
        runWithInvalidArgument(shell, consObj, args);

        args = new String[] { ConsistencyCommand.NAME, CONSISTENCY_TIME_FLAG,
                              PERMISSIBLE_LAG_FLAG, "INVALID" };
        runWithInvalidArgument(shell, consObj, args);

        args = new String[] { ConsistencyCommand.NAME, CONSISTENCY_TIME_FLAG,
                              PERMISSIBLE_LAG_FLAG, "0" };
        runWithInvalidArgument(shell, consObj, args);

        args = new String[] { ConsistencyCommand.NAME, CONSISTENCY_TIME_FLAG,
                              PERMISSIBLE_LAG_FLAG, "10000",
                              TIMEOUT_FLAG, "INVALID"};
        runWithInvalidArgument(shell, consObj, args);

        args = new String[] { ConsistencyCommand.NAME, CONSISTENCY_TIME_FLAG,
                              PERMISSIBLE_LAG_FLAG, "10000",
                              TIMEOUT_FLAG, "0"};
        runWithInvalidArgument(shell, consObj, args);
        shell.shutdown();
    }

    @Test
    public void testConsistencyCommandExecuteUnknownArgument() {
        final OnqlShell shell = getTestShell();
        final ConsistencyCommand consObj = new ConsistencyCommand();
        String[] args = new String[] { ConsistencyCommand.NAME,
                                       CONSISTENCY_TIME_FLAG,
                                       "INVALID" };
        runWithUnknownArgument(shell, consObj, args);
        shell.shutdown();
    }

    @Test
    public void testConsistencyCommandExecuteMissingArgument() {
        final OnqlShell shell = getTestShell();
        final ConsistencyCommand consObj = new ConsistencyCommand();
        String[] args = new String[] { ConsistencyCommand.NAME,
                                       CONSISTENCY_TIME_FLAG};
        runWithRequiredFlag(shell, consObj, args, PERMISSIBLE_LAG_FLAG);

        args = new String[] { ConsistencyCommand.NAME, CONSISTENCY_TIME_FLAG,
                              PERMISSIBLE_LAG_FLAG, "10000"};
        runWithRequiredFlag(shell, consObj, args, CONSISTENCY_TIME_FLAG);
        shell.shutdown();
    }

    @Test
    public void testConsistencyCommandExecute() {
        final String retMsg = "Read consistency policy: ";
        final OnqlShell shell = getTestShell();
        final ConsistencyCommand consObj = new ConsistencyCommand();
        final String command = ConsistencyCommand.NAME;
        try {
            String[] args = {command};
            Consistency cons = shell.getStoreConsistency();
            runCommand(shell, consObj, args, retMsg +
                       OnqlShell.getConsistencyName(cons));

            final String consName = "ABSOLUTE";
            args = new String[]{ command, consName };
            runCommand(shell, consObj, args, retMsg + consName);
            args = new String[]{ command };
            runCommand(shell, consObj, args, retMsg + consName);
            args = new String[]{ command, consName.toLowerCase() };
            runCommand(shell, consObj, args, retMsg + consName);

            args = new String[]{ command, CONSISTENCY_TIME_FLAG,
                                 PERMISSIBLE_LAG_FLAG, "10000",
                                 TIMEOUT_FLAG, "20000",};
            runCommand(shell, consObj, args, retMsg + "Consistency.Time[");
        } /* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConsistencyCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final ConsistencyCommand consObj = new ConsistencyCommand();
        final String[] args = {ConsistencyCommand.NAME, "-help"};
        runWithHelpException(shell, consObj, args);
        shell.shutdown();
    }

    /* DurabilityCommand coverage. */
    @Test
    public void testDurabilityCommandGetCommandDescription()
        throws Exception {

        final DurabilityCommand durObj = new DurabilityCommand();
        final String expectedResult = DurabilityCommand.DESCRIPTION;
        assertEquals(expectedResult, durObj.getCommandDescription());
    }

    @Test
    public void testDurabilityCommandGetCommandSyntax()
        throws Exception {

        final DurabilityCommand durObj = new DurabilityCommand();
        final String expectedResult = DurabilityCommand.SYNTAX;
        assertEquals(expectedResult, durObj.getCommandSyntax());
    }

    @Test
    public void testDurabilityCommandExecuteInvalidArgument() {
        final OnqlShell shell = getTestShell();
        final DurabilityCommand durObj = new DurabilityCommand();
        final String command = DurabilityCommand.NAME;

        String[] args = { command, "INVALID" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  MASTER_SYNC_FLAG, "INVALID" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  REPLICA_SYNC_FLAG, "INVALID" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  REPLICA_ACK_FLAG, "INVALID" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  "ABSOLUTE",
                              MASTER_SYNC_FLAG, "WRITE_NO_SYNC" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  "ABSOLUTE",
                              REPLICA_SYNC_FLAG, "WRITE_NO_SYNC" };
        runWithInvalidArgument(shell, durObj, args);

        args = new String[] { command,  "ABSOLUTE", REPLICA_ACK_FLAG, "ALL" };
        runWithInvalidArgument(shell, durObj, args);
    }

    @Test
    public void testDurabilityCommandExecuteUnknownArgument() {
        final OnqlShell shell = getTestShell();
        final DurabilityCommand durObj = new DurabilityCommand();
        final String command = DurabilityCommand.NAME;
        String[] args = new String[] { command, REPLICA_ACK_FLAG, "ALL",
                                       "INVALID" };
        runWithUnknownArgument(shell, durObj, args);

        args = new String[] { command, REPLICA_SYNC_FLAG, "SYNC", "INVALID" };
        runWithUnknownArgument(shell, durObj, args);

        args = new String[] { command, MASTER_SYNC_FLAG, "SYNC", "INVALID" };
        runWithUnknownArgument(shell, durObj, args);

        shell.shutdown();
    }

    @Test
    public void testDurabilityCommandExecuteMissingArgument() {
        final OnqlShell shell = getTestShell();
        final DurabilityCommand durObj = new DurabilityCommand();
        final String command = DurabilityCommand.NAME;

        String[] args = new String[] { command, MASTER_SYNC_FLAG, "SYNC"};
        runWithRequiredFlag(shell, durObj, args, REPLICA_SYNC_FLAG);

        args = new String[] { command, MASTER_SYNC_FLAG, "SYNC",
                              REPLICA_SYNC_FLAG, "SYNC"};
        runWithRequiredFlag(shell, durObj, args, REPLICA_ACK_FLAG);
        shell.shutdown();
    }

    @Test
    public void testDurabilityCommandExecute() {
        final String retMsg = "Write durability policy: ";
        final OnqlShell shell = getTestShell();
        final DurabilityCommand durObj = new DurabilityCommand();
        final String command = DurabilityCommand.NAME;
        try {
            String[] args = {command};
            Durability dur = shell.getStoreDurability();
            runCommand(shell, durObj, args, retMsg +
                       OnqlShell.getDurabilityName(dur));

            final String durName = "COMMIT_SYNC";
            args = new String[]{ command, durName };
            runCommand(shell, durObj, args, retMsg + durName);
            args = new String[]{ command };
            runCommand(shell, durObj, args, retMsg + durName);
            args = new String[]{ command, durName.toLowerCase() };
            runCommand(shell, durObj, args, retMsg + durName);

            args = new String[]{ command, MASTER_SYNC_FLAG, "SYNC",
                                 REPLICA_SYNC_FLAG, "SYNC".toLowerCase(),
                                 REPLICA_ACK_FLAG, "ALL"};
            runCommand(shell, durObj, args, retMsg + "Durability[");
        } /* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testDurabilityCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final DurabilityCommand durObj = new DurabilityCommand();
        final String[] args = {DurabilityCommand.NAME, "-help"};
        runWithHelpException(shell, durObj, args);
        shell.shutdown();
    }

    /* TimeoutCommand coverage. */
    @Test
    public void testRequestTimeoutCommandGetCommandDescription()
        throws Exception {

        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String expectedResult = RequestTimeoutCommand.DESCRIPTION;
        assertEquals(expectedResult, timeObj.getCommandDescription());
    }

    @Test
    public void testRequestTimeoutCommandGetCommandSyntax()
        throws Exception {

        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String expectedResult = RequestTimeoutCommand.SYNTAX;
        assertEquals(expectedResult, timeObj.getCommandSyntax());
    }

    @Test
    public void testRequestTimeoutCommandExecuteBadArgumentCount() {
        final OnqlShell shell = getTestShell();
        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String[] args = {RequestTimeoutCommand.NAME, "1000", "1000"};
        runWithIncorrectNumberOfArguments(shell, timeObj, args);
        shell.shutdown();
    }

    @Test
    public void testRequestTimeoutCommandExecuteInvalidArgument() {
        final OnqlShell shell = getTestShell();
        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String command = RequestTimeoutCommand.NAME;
        final String[] args = {command, "INVALID"};
        runWithInvalidArgument(shell, timeObj, args);
        shell.shutdown();
    }

    @Test
    public void testRequestTimeoutCommandExecute() {

        final String retMsg = "Request timeout used: ";
        final OnqlShell shell = getTestShell();
        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String command = RequestTimeoutCommand.NAME;

        String[] args = {command};
        int requestTimeout = shell.getRequestTimeout();
        runCommand(shell, timeObj, args,
                   retMsg + String.format("%,d", requestTimeout) + "ms");

        requestTimeout = 100000;
        args = new String[]{ command, String.valueOf(requestTimeout) };
        runCommand(shell, timeObj, args,
                   retMsg + String.format("%,d", requestTimeout) + "ms");

        args = new String[]{command};
        runCommand(shell, timeObj, args,
                   retMsg + String.format("%,d", requestTimeout) + "ms");
        shell.shutdown();
    }

    @Test
    public void testRequestTimeoutCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final RequestTimeoutCommand timeObj = new RequestTimeoutCommand();
        final String[] args = {RequestTimeoutCommand.NAME, "-help"};
        runWithHelpException(shell, timeObj, args);
        shell.shutdown();
    }

    /* OutputCommand coverage. */
    @Test
    public void testOutputCommandGetCommandDescription()
        throws Exception {

        final OutputCommand outputObj = new OutputCommand();
        final String expectedResult = OutputCommand.DESCRIPTION;
        assertEquals(expectedResult, outputObj.getCommandDescription());
    }

    @Test
    public void testOutputCommandGetCommandSyntax()
        throws Exception {

        final OutputCommand outputObj = new OutputCommand();
        final String expectedResult = OutputCommand.SYNTAX;
        assertEquals(expectedResult, outputObj.getCommandSyntax());
    }

    @Test
    public void testOutputCommandExecuteBadArgumentCount() {
        final OnqlShell shell = getTestShell();
        final OutputCommand outputObj = new OutputCommand();
        final String[] args = {OutputCommand.NAME, "a.out", "a.out"};
        runWithIncorrectNumberOfArguments(shell, outputObj, args);
        shell.shutdown();
    }

    @Test
    public void testOutputCommandExecute() {

        final OnqlShell shell = getTestShell();
        final OutputCommand outputObj = new OutputCommand();
        final String command = OutputCommand.NAME;

        String[] args = {command};
        runCommand(shell, outputObj, args, "Query output is stdout");

        final String retMsg = "Query output file is ";
        String file = getTempFileName("query", ".out");
        args = new String[]{ command, file };
        runCommand(shell, outputObj, args, retMsg + file);

        args = new String[]{command};
        runCommand(shell, outputObj, args, retMsg + file);
        shell.shutdown();
    }

    @Test
    public void testOutputCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final OutputCommand outputObj = new OutputCommand();
        final String[] args = {OutputCommand.NAME, "-help"};
        runWithHelpException(shell, outputObj, args);
        shell.shutdown();
    }

    /* OutputModeCommand coverage. */
    @Test
    public void testOutputModeCommandGetCommandDescription()
        throws Exception {

        final OutputModeCommand modeObj = new OutputModeCommand();
        final String expectedResult = OutputModeCommand.DESCRIPTION;
        assertEquals(expectedResult, modeObj.getCommandDescription());
    }

    @Test
    public void testOutputModeCommandGetCommandSyntax()
        throws Exception {

        final OutputModeCommand modeObj = new OutputModeCommand();
        final String expectedResult = OutputModeCommand.SYNTAX;
        assertEquals(expectedResult, modeObj.getCommandSyntax());
    }

    @Test
    public void testOutputModeCommandExecuteInvalidArgument() {
        final OnqlShell shell = getTestShell();
        final OutputModeCommand modeObj = new OutputModeCommand();

        String[] args = {OutputModeCommand.NAME, "invalid"};
        runWithUnknownArgument(shell, modeObj, args);

        args = new String[]{OutputModeCommand.NAME, "csv", "line"};
        runWithInvalidArgument(shell, modeObj, args);

        args = new String[]{OutputModeCommand.NAME, "json", "-invalid"};
        runWithInvalidArgument(shell, modeObj, args);

        shell.shutdown();
    }

    @Test
    public void testOutputModeCommandExecute() {

        final String retMsg = "Query output mode is ";
        final OnqlShell shell = getTestShell();
        final OutputModeCommand modeObj = new OutputModeCommand();
        final String command = OutputModeCommand.NAME;

        String[] args = {command};
        String modeName = shell.getQueryOutputMode().name();
        runCommand(shell, modeObj, args, retMsg + modeName);

        OutputMode mode = OutputMode.LINE;
        args = new String[]{ command, mode.toString()};
        String expectedRet = retMsg + mode.toString();
        runCommand(shell, modeObj, args, expectedRet);
        args = new String[]{command};
        runCommand(shell, modeObj, args, expectedRet);
        args = new String[]{ command, mode.toString().toLowerCase()};
        runCommand(shell, modeObj, args, expectedRet);

        expectedRet = retMsg + "pretty JSON";
        args = new String[]{command, "json", PRETTY_FLAG};
        runCommand(shell, modeObj, args, expectedRet);
        args = new String[]{command};
        runCommand(shell, modeObj, args, expectedRet);

        expectedRet = retMsg + "JSON";
        args = new String[]{command, "json"};
        runCommand(shell, modeObj, args, expectedRet);
        args = new String[]{command};
        runCommand(shell, modeObj, args, expectedRet);

        shell.shutdown();
    }

    @Test
    public void testOutputModeCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final OutputModeCommand modeObj = new OutputModeCommand();
        final String[] args = {OutputCommand.NAME, "-help"};
        runWithHelpException(shell, modeObj, args);
        shell.shutdown();
    }

    /* VersionCommand coverage. */
    @Test
    public void testVersionCommandGetCommandDescription()
        throws Exception {

        final VersionCommand verObj = new VersionCommand();
        final String expectedResult = VersionCommand.DESCRIPTION;
        assertEquals(expectedResult, verObj.getCommandDescription());
    }

    @Test
    public void testVersionCommandGetCommandSyntax()
        throws Exception {

        final VersionCommand verObj = new VersionCommand();
        final String expectedResult = VersionCommand.SYNTAX;
        assertEquals(expectedResult, verObj.getCommandSyntax());
    }

    @Test
    public void testVersionModeCommandExecute() {

        final String retMsg = "Client version: ";
        final OnqlShell shell = getTestShell();
        final VersionCommand verObj = new VersionCommand();
        final String command = VersionCommand.NAME;

        String[] args = {command};
        runCommand(shell, verObj, args, retMsg +
                   KVVersion.CURRENT_VERSION.getNumericVersionString());
        shell.shutdown();
    }

    @Test
    public void testVersionCommandExecuteHelp() {
        final OnqlShell shell = getTestShell();
        final VersionCommand verObj = new VersionCommand();
        final String[] args = {VersionCommand.NAME, "-help"};
        runWithHelpException(shell, verObj, args);
        shell.shutdown();
    }

    /* show query sub command coverage. */
    @Test
    public void testShowQueryGetCommandDescription()
        throws Exception {

        ShowQuery showQueryObj = new ShowQuery();
        String expectedResult = ShowQuery.DESCRIPTION;
        assertEquals(expectedResult, showQueryObj.getCommandDescription());
    }

    @Test
    public void testShowQueryGetCommandSyntax()
        throws Exception {

        ShowQuery showQueryObj = new ShowQuery();
        String expectedResult = ShowQuery.SYNTAX;
        assertEquals(expectedResult, showQueryObj.getCommandSyntax());
    }

    @Test
    public void testShowQueryBadArgumentCount() {
        OnqlShell shell = getTestShell();
        ShowQuery showQueryObj = new ShowQuery();
        String[] args = { ShowQuery.NAME };
        runWithIncorrectNumberOfArguments(shell, showQueryObj, args);
        shell.shutdown();
    }

    @Test
    public void testShowQueryExecuteInvalidArgument() {
        OnqlShell shell = getTestShell();
        ShowQuery showQueryObj = new ShowQuery();
        String[] args = {ShowQuery.NAME, "show tables"};
        runWithInvalidArgument(shell, showQueryObj, args);
        shell.shutdown();
    }

    @Test
    public void testShowQueryExecute() {
        OnqlShell shell = getTestShell();
        ShowQuery showQueryObj = new ShowQuery();

        String[] args = {ShowQuery.NAME, "select * from t1"};
        runCommand(shell, showQueryObj, args, false, "Table t1 does not exist");

        String ddl ="create table t1(id integer, name string, primary key(id))";
        assertTrue(createTable(ddl, "t1") != null);
        runCommand(shell, showQueryObj, args, "\"primary index\"");

        String[] lines = new String[] {
            "show query 'select * from t1 where name = \"John\"'",
            "show query select * from t1 where name = \"John\"",
            "sh que select * from t1 where name = \"John\"",
            "show query \nselect * \nfrom t1 \nwhere name = \n\"John\""
        };

        String expInfo = "\"primary index\"";
        for (String line : lines) {
            shell.execute(line);
            String output = getShellOutput(shell);
            assertTrue("Expected to get message that contains " + expInfo +
                       ", but get " + output, output.contains(expInfo));
        }

        shell.shutdown();
    }

    @Test
    public void testShowQueryExecuteHelp() {
        OnqlShell shell = getTestShell();
        ShowQuery showQueryObj = new ShowQuery();
        String[] args = { ShowQuery.NAME, "-help" };
        runWithHelpException(shell, showQueryObj, args);
        shell.shutdown();
    }

    @Test
    public void testSyntaxErrorLocation() {
        String query = "select\n*\nfrom   t1;\n";
        InputStream in = new ByteArrayInputStream(query.getBytes());
        OnqlShell shell = getTestShell(in);
        shell.loop();
        assertTrue(getShellOutput(shell).contains
                   ("Error: at (3, 7) Table t1 does not exist"));
    }
}
