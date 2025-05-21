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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreConfig;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.client.CommandShell.ConnectCommand;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.shell.AggregateCommand;
import oracle.kv.shell.DeleteCommand;
import oracle.kv.shell.ExecuteCommand;
import oracle.kv.shell.GetCommand;
import oracle.kv.shell.PutCommand;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.CommonShell;
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
import oracle.kv.util.shell.ShellException;

import org.junit.Test;

/**
 * Verifies aspects of the CommandShell class that AdminClientTest does
 * not verify. Note that although the CLI test framework verifies many
 * of the same aspects of the CommandShell as this unit test, the tests
 * from the CLI test framework do not contribute to the unit test coverage
 * measure that is automatically computed nightly. Thus, the intent of
 * this test class is to provide additional unit test coverage for the
 * CommandShell class that will be automatically measured nightly.
 */
public class CommandShellTest extends TestBase {

    private static final String HOSTNAME = "localhost";
    private static final String PORT = "9999";
    private static final String STORENAME = "kvstore";

    private final List<String> prefixList = new ArrayList<String>();

    @Override
    public void setUp() throws Exception {

        super.setUp();
        prefixList.add("-host");
        prefixList.add(HOSTNAME);
        prefixList.add("-port");
        prefixList.add(PORT);
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    private CommandShell getTestShell() {
        return new CommandShell(System.in, System.out);
    }

    private CommandShell getTestShell(InputStream input, PrintStream output) {
        return new CommandShell(input, output);
    }

    private String[] argArray(String... args) {

        final List<String> retList = new ArrayList<String>();
        for (String arg : prefixList) {
            retList.add(arg);
        }
        for (String arg : args) {
            retList.add(arg);
        }
        return retList.toArray(new String[retList.size()]);
    }

    @Test
    public void testCommandShellGetPrompt() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            shell.parseArgs(argArray("test command"));
            assertNotNull("CommandShell.getPrompt should not return null " +
                          "prompt", shell.getPrompt());
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellGetPromptNull() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            shell.parseArgs(argArray("-noprompt"));
            assertNull("CommandShell.getPrompt should return null prompt " +
                       "when '-noprompt' flag is used", shell.getPrompt());
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellShowHiddenFlag() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            shell.parseArgs(argArray("-hidden", "test command"));
            assertTrue("CommandShell.getHidden should return true " +
                       "when '-hidden' flag is used prior to command",
                       shell.getHidden());
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellShowHiddenCommand() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            shell.parseArgs(argArray("test command", "-hidden"));
            assertTrue("CommandShell.getHidden should return true " +
                       "when '-hidden' flag is used after command",
                       shell.getHidden());
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellGetCommands() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final List<? extends ShellCommand> shellCommands =
                shell.getCommands();
            final List<? extends ShellCommand> testCommands =
                Arrays.asList(new AggregateCommand(),
                              new AwaitCommand(),
                              new ConfigureCommand(),
                              new ConnectCommand(),
                              new DebugCommand(),
                              new DeleteCommand(),
                              new ExecuteCommand(),
                              new Shell.ExitCommand(),
                              new GetCommand(),
                              new Shell.HelpCommand(),
                              new HiddenCommand(),
                              new HistoryCommand(),
                              new NamespaceCommand(),
                              new Shell.LoadCommand(),
                              new LogtailCommand(),
                              new PageCommand(),
                              new PingCommand(),
                              new PlanCommand(),
                              new PolicyCommand(),
                              new PoolCommand(),
                              new PutCommand(),
                              new RepairAdminQuorumCommand(),
                              new ShowCommand(),
                              new SnapshotCommand(),
                              new TableCommand(),
                              new TimeCommand(),
                              new TableSizeCommand(),
                              new TopologyCommand(),
                              new VerboseCommand(),
                              new VerifyCommand()
                              );

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
    public void testCommandShellInitConnect() throws Exception {
        final CommandShell shell = getTestShell();
        try {
            shell.init();
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellInitNoConnect() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final String[] args = {"-noconnect"};
            shell.parseArgs(args);
            shell.init();
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellGetUsageHeader() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            assertEquals(CommandShell.usageHeader, shell.getUsageHeader());
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellDoRetry() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final boolean retry0 = shell.doRetry();
            if (!retry0) {
                final boolean retry1 = shell.doRetry();
                assertTrue("Successive calls to CommandShell.doRetry " +
                           "returned false then true, but should have " +
                           "returned false then false.",
                           (retry0 == retry1));
            } else {
                final boolean retry1 = shell.doRetry();
                assertTrue("Successive calls to CommandShell.doRetry " +
                           "returned true then true, but should have " +
                           "returned true then false.",
                           (retry0 != retry1));
            }
        } finally {
            shell.shutdown();
        }
    }

    /**
     * When CommandShell encounters an unknown exception type, 1 of 3
     * possible actions is taken:
     * <ol>
     *   <li>If the exception is an AdminFaultException whose cause is an
     *       IllegalCommandException, then the exception message will be
     *       displayed with the version information excluded.
     *   <li>If the exception is an AdminFaultException but the cause is not
     *       an IllegalCommandException, then the exception message will be
     *       displayed with the version information included.
     *   <li>If the exception is not an AdminFaultException, then the
     *       exception will be handled by CommandShell's parent class
     *       (Shell).
     * </ol>
     * The following three test cases (testCommandShellHandleUnknownException
     * 1, 2, and 3) verify that CommandShell handles each of the cases
     * described above as expected.
     */
    @Test
    public void testCommandShellHandleUnknownException1() throws Exception {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);
        final String exceptionInput = "test exception " +
            "[AdminFaultException:IllegalCommandException]";
        final String expectedResult = IllegalCommandException.class.getName() +
                                      ":" + exceptionInput + Shell.eol;
        try {
            final AdminFaultException exc =
                new AdminFaultException(
                    new IllegalCommandException(exceptionInput));
            shell.handleUnknownException("test line", exc);
            assertEquals(expectedResult, outStream.toString());
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
            cmdOut.close();
        }
    }

    @Test
    public void testCommandShellHandleUnknownException2() throws Exception {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);
        final String exceptionInput = "test exception [AdminFaultException]";
        final String expectedResult =
            Exception.class.getName() + ":" +
            exceptionInput + CommandShell.versionString + Shell.eol;
        try {
            final AdminFaultException exc =
                new AdminFaultException(new Exception(exceptionInput));
            shell.handleUnknownException("test line", exc);
            assertEquals(expectedResult, outStream.toString());
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellHandleUnknownException3() throws Exception {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);
        final String exceptionInput = "test exception [Exception]";
        final String expectedResult =
            "test exception [Exception] (java.lang.Exception)" + Shell.eol;
        try {
            final Exception exc = new Exception(exceptionInput);
            shell.handleUnknownException("test line", exc);
            assertEquals(expectedResult, outStream.toString());
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    /**
     * When CommandShell encounters a KVSecurityException type, 1 of 2 possible
     * actions is taken:
     * <ol>
     *   <li>If the exception is an instance of AuthenticationRequiredException,
     *       CommandShell will try to connect to admin again.
     *   <li>If the exception is other types of KVSecurityExeception, the
     *       exception message will be displayed with the version information
     *       included.
     * The following two test cases (testCommandShellHandleKVSecurityException
     * 1 and 2) verify that CommandShell handles each of the cases described
     * above as expected.
     */
    @Test
    public void testCommandShellHandleKVSecurityException() throws Exception{
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);
        final String exceptionInput = "test exception " +
            "[AuthenticationRequiredException]";

        /* Reconnect to admin will cause error information in this case */
        final String expectedResult =
             "Error handling command test line: " +
             "Hostport list is not specified." + Shell.eol;
        final KVSecurityException kvse =
            new AuthenticationRequiredException(exceptionInput, false);
        try {
            shell.handleKVSecurityException("test line", kvse);
            assertEquals(expectedResult, outStream.toString());
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellHandleKVSecurityException2() throws Exception {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);

        final String exceptionInput = "test exception [KVSecurityException]";
        final String expectedResult =
            "Error handling command test line: " + exceptionInput;
        @SuppressWarnings("serial")
        final KVSecurityException kvse =
            new KVSecurityException(exceptionInput) { };
        try {
            shell.handleKVSecurityException("test line", kvse);
            assertTrue(outStream.toString().contains(expectedResult));
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellConnect() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            shell.connectAdmin(HOSTNAME, Integer.parseInt(PORT), null, null);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }

        try {
            shell.openStore(HOSTNAME, Integer.parseInt(PORT), STORENAME,
                null, null);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellConnectWithWrongLoginFile() throws Exception {
        final CommandShell shell = getTestShell();
        try {
            shell.connectAdmin(HOSTNAME, Integer.parseInt(PORT), null,
                          "no-exist file");
            fail("expected ShellException");
        } catch (ShellException e) {
            assertTrue(e.getMessage().contains("no-exist file"));
        } finally {
            shell.shutdown();
        }
    }

    @Test
    public void testCommandShellConnectOverrideDefaultConfig() {

        final CommandShell shell = getTestShell();
        try {
            shell.openStore(HOSTNAME, Integer.parseInt(PORT), STORENAME, null,
                            null, 10000, Consistency.ABSOLUTE,
                            Durability.COMMIT_NO_SYNC);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    /* CommandShell.HistoryCommand coverage. */

    @Test
    public void testHistoryCommandExecuteArgLast() throws Exception {

        /* Capture the output so the exception message is not printed to
         * the test output.
         */
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommandShell shell = getTestShell(System.in, cmdOut);

        /* Use the handleUnknownException method of CommandShell to inject
         * 3 lines of command history so that when the last 3 lines of the
         * history is retrieved by the execute method being tested here,
         * there is a known result that can be verified.
         */
        final String exceptionInput = "test exception [AdminFaultException]";
        final AdminFaultException exc =
                new AdminFaultException(new Exception(exceptionInput));
        final String[] excInputLines = {"history test line 0",
                                        "history test line 1",
                                        "history test line 2"};
        String expectedResult = null;
        final StringBuffer expectedResultBuf = new StringBuffer();
        for (int i = 0; i < excInputLines.length; i++) {
            shell.handleUnknownException(excInputLines[i], exc);
            expectedResultBuf.append((i + 1) + " " + excInputLines[i] +
                                     Shell.eol);
        }
        expectedResult = expectedResultBuf.toString();

        final CommandShell.HistoryCommand historyObj =
            new CommandShell.HistoryCommand();
        /* Retrieve the last 3 lines of command history, and verify the
         * result.
         */
        final String[] args =
            {"test command", "-last", "3"};
        final String result = historyObj.execute(args, shell);
        assertEquals(expectedResult, result);
    }

    /* CommandShell.ConnectCommand coverage. */

    @Test
    public void testConnectAdminCommandExecute() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "admin", "-host", HOSTNAME, "-port", PORT};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectAdminCommandExecuteBadArgNone() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "admin", "BAD_ARG_NONE"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectAdminCommandExecuteBadArgPort() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "admin", "-port", "BAD_ARG_PORT"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectAdminCommandExecuteBadArgCount() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "admin", "-host", HOSTNAME};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectCommandExecuteMissArgSecurity() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args = {"test command", "admin", "-host", HOSTNAME,
                                   "-port", PORT, "-security"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectCommandExecuteMissArgUser() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args = {"test command", "admin", "-host", HOSTNAME,
                                   "-port", PORT, "-username"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectAdminCommandGetCommandDescription()
        throws Exception {

        final CommandShell.ConnectCommand.ConnectAdminSubCommand connectObj =
            new TestConnectCommand.ConnectAdminSubCommand();
        final String expectedResult =
            CommandShell.ConnectCommand.ConnectAdminSubCommand.
                CONNECT_ADMIN_COMMAND_DESC;
        assertEquals(expectedResult, connectObj.getCommandDescription());
    }

    @Test
    public void testConnectAdminCommandGetCommandSyntax() throws Exception {

        final CommandShell.ConnectCommand.ConnectAdminSubCommand connectObj =
            new TestConnectCommand.ConnectAdminSubCommand();
        final String expectedResult =
            CommandShell.ConnectCommand.ConnectAdminSubCommand.
                CONNECT_ADMIN_COMMAND_SYNTAX;
        assertEquals(expectedResult, connectObj.getCommandSyntax());
    }

    @Test
    public void testConnectStoreCommandExecute() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandExecuteBadArgNone() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "BAD_ARG_NONE"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandExecuteBadArgPort() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-port", "BAD_ARG_PORT"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandExecuteBadArgCount() throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }

        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandExecuteOverrideConnectConfig()
        throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME, "-timeout", "10000", "-consistency",
                 "ABSOLUTE", "-durability", "COMMIT_NO_SYNC"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandExecuteBadConnectConfig()
        throws Exception {

        final CommandShell shell = getTestShell();
        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME, "-timeout", "-1"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }

        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME, "-timeout", "a"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }

        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME, "-consistency", "a"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }

        try {
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            final String[] args =
                {"test command", "store", "-host", HOSTNAME, "-port", PORT,
                 "-name", STORENAME, "-durability", "a"};
            connectObj.execute(args, shell);
            fail("expected ShellException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandGetCommandDescription() throws Exception {

        final CommandShell.ConnectCommand.ConnectStoreSubCommand connectObj =
            new TestConnectCommand.ConnectStoreSubCommand();
        final String expectedResult =
            CommonShell.ConnectStoreCommand.CONNECT_STORE_COMMAND_DESC;
        assertEquals(expectedResult, connectObj.getCommandDescription());
    }

    @Test
    public void testConnectStoreCommandGetCommandSyntax() throws Exception {

        final CommandShell.ConnectCommand.ConnectStoreSubCommand connectObj =
            new TestConnectCommand.ConnectStoreSubCommand();
        final String expectedResult =
            CommandShell.ConnectCommand.ConnectStoreSubCommand.
                CONNECT_STORE_COMMAND_SYNTAX;
        assertEquals(expectedResult, connectObj.getCommandSyntax());
    }

    private static class TestConnectCommand extends ConnectCommand {
    }

    /*
     * The test cases below exercise the CommandShell.noAdmin method;
     * covering the cases where the RemoteException input to noAdmin is
     * either null or non-null. The remaining code paths of noAdmin cannot
     * be covered here without starting an actual admin service and
     * connecting the CommandShell under test to that admin; which is
     * beyond the scope of this test class.
     */

    @Test
    public void testCommandShellNoAdminNotNull() throws Exception {

        final CommandShell testShell =
            new TestCommandShell(System.in, System.out);
        final RemoteException remoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin", remoteException);

        try {
            testShell.noAdmin(remoteException);
            fail("expected ShellException");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testCommandShellNoAdminNull() throws Exception {

        final CommandShell testShell =
            new TestCommandShell(System.in, System.out);
        final RemoteException remoteException = null;
        final ShellException expectedException =
            new ShellException("Cannot contact admin", remoteException);

        try {
            testShell.noAdmin(remoteException);
            fail("expected ShellException");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testCommandShellCsfParams() throws Exception {
        final CommandShell shell = getTestShell();
        try {
            shell.parseArgs(argArray("-noconnect"));
            assertEquals(shell.getCsfOpenTimeout(),
                         KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT);
            assertEquals(shell.getCsfReadTimeout(),
                         KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT);
            shell.parseArgs(argArray("-registry-open-timeout", "6000",
                                     "-registry-read-timeout", "3000"));
            assertEquals(shell.getCsfOpenTimeout(), 6000);
            assertEquals(shell.getCsfReadTimeout(), 3000);
        } finally {
            shell.shutdown();
        }
    }

    private static class TestCommandShell extends CommandShell {
        TestCommandShell(InputStream input, PrintStream output) {
            super(input, output);
        }
    }

    @Test
    public void testHelperHosts() throws Exception {
        CreateStore createStore = new CreateStore(kvstoreName, 5000, 3, 3, 10,
                                                  2, 2 * CreateStore.MB_PER_SN,
                                                  true, null);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(outStream);
        CommandShell shell = new CommandShell(System.in, output);
        try {
            /* connect to an admin using helper hosts*/
            createStore.start();
            String[] args = new String[]{"-helper-hosts",
                createStore.getHostname() + ":" +
                createStore.getRegistryPort(1),
                createStore.getHostname() + ":" +
                createStore.getRegistryPort(2)};
            shell.parseArgs(args);
            shell.connect();
            assertNotNull("Failed to connect to admin.", shell.getAdmin());
            shell.shutdown();

            /* connect to an admin when the provided host:port does not
             * have an admin*/
            CommandServiceAPI cs = createStore.getAdmin();
            Set<ResourceId> stopAdminId = new HashSet<ResourceId>();
            stopAdminId.add(createStore.getAdminId(1));
            int planId = cs.createStopServicesPlan(null, stopAdminId);
            runPlan(cs, planId);
            Thread.sleep(1000);
            shell = new CommandShell(System.in, output);
            args = new String[]{"-helper-hosts",
                createStore.getHostname() + ":" +
                createStore.getRegistryPort(1)};
            shell.parseArgs(args);
            shell.connect();
            assertNotNull("Failed to connect to admin.", shell.getAdmin());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e);

        } finally {
            shell.shutdown();
            createStore.shutdown();
        }

    }

    @Test
    public void testConnectStoreFlagUsingAdminOnlySN() throws Exception {
        CreateStore createStore = new CreateStore(kvstoreName, 5000, 3, 3, 10,
                                                  3, 3 * CreateStore.MB_PER_SN,
                                                  true, null);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(outStream);
        CommandShell shell = new CommandShell(System.in, output);
        try {
            createStore.start();
            stopRNsOnSN(createStore, 0);
            /* Now SN1 only has an admin, next we will use the host and port
             * of SN1 to connect admin and store.*/

            /*Connect admin and store using -helper-hosts*/
            String[] args = new String[]{"-helper-hosts",
                                         createStore.getHostname() + ":" +
                                         createStore.getRegistryPort(0),
                                         "-store", kvstoreName};
            shell.parseArgs(args);
            shell.connect();
            assertNotNull("Failed to connect to store.", shell.getStore());

            /*Connect admin and store using -host/-port*/
            shell = new CommandShell(System.in, output);
            args = new String[]{"-host", createStore.getHostname(),
                                "-port", "" + createStore.getRegistryPort(0),
                                "-store", kvstoreName};
            shell.parseArgs(args);
            shell.connect();
            assertNotNull("Failed to connect to store.", shell.getStore());

        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        } finally {
            shell.shutdown();
            createStore.shutdown();
        }
    }

    @Test
    public void testConnectStoreCommandUsingAdminOnlySN() throws Exception {
        CreateStore createStore = new CreateStore(kvstoreName, 5000, 3, 3, 10,
                                                  3, 3 * CreateStore.MB_PER_SN,
                                                  true, null);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(outStream);
        CommandShell shell = new CommandShell(System.in, output);
        try {
            createStore.start();
            stopRNsOnSN(createStore, 0);
            /* Now SN1 only has an admin, next we will use the host and port
             * of SN1 to connect admin and store.*/

            /*Connect admin using -helper-hosts*/
            String[] args = new String[]{"-helper-hosts",
                                         createStore.getHostname() + ":" +
                                         createStore.getRegistryPort(0)};
            shell.parseArgs(args);
            shell.connect();

            /*Connect store using connect command*/
            final CommandShell.ConnectCommand connectObj =
                new CommandShell.ConnectCommand();
            args = new String[]{"test command", "store",
                                "-name", kvstoreName};
            connectObj.execute(args, shell);
            assertNotNull("Failed to connect to store.", shell.getStore());

            /*Connect admin using -host/-port*/
            shell = new CommandShell(System.in, output);
            args = new String[]{"-host", createStore.getHostname(),
                                "-port", "" + createStore.getRegistryPort(0)};
            shell.parseArgs(args);
            shell.connect();

            /*Connect store using connect command*/
            args = new String[]{"test command", "store",
                                "-name", kvstoreName};
            connectObj.execute(args, shell);
            assertNotNull("Failed to connect to store.", shell.getStore());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e);
        } finally {
            shell.shutdown();
            createStore.shutdown();
        }
    }

    private void runPlan(CommandServiceAPI cs,
                         int planId)
        throws Exception {
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 30, TimeUnit.SECONDS);
        cs.assertSuccess(planId);
    }

    private void stopRNsOnSN(CreateStore createStore, int index)
        throws Exception {
        /*Stop RNs on a SN so that the SN will be an admin-only SN.*/
        CommandServiceAPI cs = createStore.getAdmin();
        List<RepNodeId> rns = createStore.getRNs(createStore.
                                                 getStorageNodeAgent(index).
                                                 getStorageNodeId());
        Set<RepNodeId> stopRNs = new HashSet<RepNodeId>(rns);
        int planId = cs.createStopServicesPlan(null, stopRNs);
        runPlan(cs, planId);
        Thread.sleep(1000);
    }
}
