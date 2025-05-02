/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.util.shell;

import static oracle.kv.impl.util.CommandParser.CONSISTENCY_FLAG;
import static oracle.kv.impl.util.CommandParser.DURABILITY_FLAG;
import static oracle.kv.impl.util.CommandParser.FROM_FLAG;
import static oracle.kv.impl.util.CommandParser.HELPER_HOSTS_FLAG;
import static oracle.kv.impl.util.CommandParser.HIDDEN_FLAG;
import static oracle.kv.impl.util.CommandParser.HOST_FLAG;
import static oracle.kv.impl.util.CommandParser.LAST_FLAG;
import static oracle.kv.impl.util.CommandParser.NAME_FLAG;
import static oracle.kv.impl.util.CommandParser.NOCONNECT_FLAG;
import static oracle.kv.impl.util.CommandParser.NOPROMPT_FLAG;
import static oracle.kv.impl.util.CommandParser.OFF_FLAG;
import static oracle.kv.impl.util.CommandParser.ON_FLAG;
import static oracle.kv.impl.util.CommandParser.PORT_FLAG;
import static oracle.kv.impl.util.CommandParser.SECURITY_FLAG;
import static oracle.kv.impl.util.CommandParser.STORE_FLAG;
import static oracle.kv.impl.util.CommandParser.TIMEOUT_FLAG;
import static oracle.kv.impl.util.CommandParser.TO_FLAG;
import static oracle.kv.impl.util.CommandParser.USER_FLAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.KVStore;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.CommonShell.ConnectStoreCommand;
import oracle.kv.util.shell.CommonShell.DebugCommand;
import oracle.kv.util.shell.CommonShell.HiddenCommand;
import oracle.kv.util.shell.CommonShell.HistoryCommand;
import oracle.kv.util.shell.CommonShell.NamespaceCommand;
import oracle.kv.util.shell.CommonShell.PageCommand;
import oracle.kv.util.shell.CommonShell.TimeCommand;
import oracle.kv.util.shell.CommonShell.VerboseCommand;

import org.junit.Test;

public class CommonShellTest extends TestBase {

    private CreateStore createStore = null;
    private static int PORT = 13250;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
    }

    private TestShell getTestShell() {
        return new TestShell(System.in, System.out);
    }

    private TestShell getTestShell(InputStream input, PrintStream output) {
        return new TestShell(input, output);
    }

    @Test
    public void testSetGetStoreConsistency() {
        final CommonShell shell = getTestShell();
        assertTrue(shell.getStoreConsistency() != null);
        shell.setStoreConsistency(Consistency.ABSOLUTE);
        assertTrue(shell.getStoreConsistency().equals(Consistency.ABSOLUTE));
    }

    @Test
    public void testSetGetStoreDurability() {
        final CommonShell shell = getTestShell();
        assertTrue(shell.getStoreDurability() != null);
        shell.setStoreDurability(Durability.COMMIT_SYNC);
        assertTrue(shell.getStoreDurability().equals(Durability.COMMIT_SYNC));
    }

    @Test
    public void testGetDurabilityNames() {
        Set<String> names = CommonShell.getDurabilityNames();
        for (String name: names) {
            final Durability dur = CommonShell.getDurability(name);
            assertTrue(CommonShell.getDurabilityName(dur).equals(name));
        }

        final Durability dur = new Durability(SyncPolicy.SYNC,
                                              SyncPolicy.WRITE_NO_SYNC,
                                              ReplicaAckPolicy.ALL);
        final String name = CommonShell.getDurabilityName(dur);
        assertTrue(name.contains(SyncPolicy.SYNC.toString()));
        assertTrue(name.contains(SyncPolicy.WRITE_NO_SYNC.toString()));
        assertTrue(name.contains(ReplicaAckPolicy.ALL.toString()));
    }

    @Test
    public void testGetConsistencyNames() {
        Set<String> names = CommonShell.getConsistencyNames();
        for (String name: names) {
            final Consistency cons = CommonShell.getConsistency(name);
            assertTrue(CommonShell.getConsistencyName(cons).equals(name));
        }

        final Consistency.Time tcons = new Consistency.Time(100,
                                                            TimeUnit.SECONDS,
                                                            2,
                                                            TimeUnit.MINUTES);
        final String name = CommonShell.getConsistencyName(tcons);
        assertTrue(name.contains(String.valueOf(100 * 1000)));
        assertTrue(name.contains(String.valueOf(2 * 60 * 1000)));
    }

    @Test
    public void testSetGetRequestTimeout() {
        final CommonShell shell = getTestShell();
        assertTrue(shell.getRequestTimeout() != 0);
        shell.setRequestTimeout(10000);
        assertTrue(shell.getRequestTimeout() == 10000);
    }

    @Test
    public void testSetGetPageHeight() {
        final CommonShell shell = getTestShell();
        assertEquals(-1, shell.getPageHeight());
        assertFalse(shell.isPagingEnabled());
        shell.setPageHeight(30);
        assertEquals(30, shell.getPageHeight());
        assertTrue(shell.isPagingEnabled());
    }

    @Test
    public void testOpenCloseStore()
        throws ShellException {

        startStore();
        final CommonShell shell = getTestShell();
        shell.openStore(createStore.getHostname(),
                        createStore.getRegistryPort(),
                        createStore.getStoreName(),
                        createStore.getDefaultUserName(),
                        createStore.getDefaultUserLoginPath());
        KVStore store = shell.getStore();
        if (store == null) {
            fail("Failed to open store");
        } else {
            assertEquals(CommonShell.REQUEST_TIMEOUT_DEF,
                         shell.getRequestTimeout());
            assertEquals(CommonShell.CONSISTENCY_DEF,
                         shell.getStoreConsistency());
            assertEquals(CommonShell.DURABILITY_DEF,
                         shell.getStoreDurability());
            shell.closeStore();
            try {
                store = shell.getStore();
                fail("Expected to get exception");
            } catch (ShellException se) {
                assertTrue(se.getMessage().contains("Not Connected"));
            }
        }

        final int requestTimeout = 10000;
        shell.openStore("localhost", PORT, kvstoreName,
                        createStore.getDefaultUserName(),
                        createStore.getDefaultUserLoginPath(),
                        requestTimeout, Consistency.ABSOLUTE,
                        Durability.COMMIT_SYNC);
        store = shell.getStore();
        if (store == null) {
            fail("Failed to open store");
        } else {
            assertEquals(requestTimeout, shell.getRequestTimeout());
            assertEquals(Consistency.ABSOLUTE, shell.getStoreConsistency());
            assertEquals(Durability.COMMIT_SYNC, shell.getStoreDurability());
            shell.closeStore();
            try {
                store = shell.getStore();
                fail("Expected to get exception");
            } catch (ShellException se) {
                assertTrue(se.getMessage().contains("Not Connected"));
            }
        }
    }

    @Test
    public void testShellParser() {
        final String host = "slc04asp.us.oracle.com";
        final String port = "9999";
        final String storeName = "mystore";
        final String userName = "user1";
        final String securityFile = "/tmp/login_user1";
        final String timeout = "10000";
        final String consistency = "ABSOLUTE";
        final String durability = "COMMIT_WRITE_NO_SYNC";
        String[] requiredFlags = { HOST_FLAG, PORT_FLAG };

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        TestShell shell = getTestShell(System.in, cmdOut);
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList(requiredFlags));

        /* Missing argument that defined in requiredFlags */
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        assertTrue(outStream.toString().contains("Missing required argument"));
        assertTrue(outStream.toString().contains(TestShell.USAGE));

        /* -host <host>*/
        outStream.reset();
        addArg(argList, HOST_FLAG, host);
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        assertTrue(outStream.toString().contains("Missing required argument"));
        assertTrue(outStream.toString().contains(TestShell.USAGE));

        /* -host <host> -port <port> */
        addArg(argList, PORT_FLAG, port);
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        assertTrue(shell.getHostName().equals(host));
        assertEquals(Integer.valueOf(port).intValue(), shell.getRegistryPort());
        String[] hostPorts = shell.getHostPorts(null);
        assertEquals(1, hostPorts.length);
        assertTrue(hostPorts[0].equals(host + ":" + port));

        /* Arguments in RC file */
        shell.parseArgument(new String[]{}, getArgs(argList), requiredFlags);
        assertTrue(shell.getHostName().equals(host));
        assertEquals(Integer.valueOf(port).intValue(), shell.getRegistryPort());

        /* Invalid argument from RC file will be ignored */
        shell.parseArgument(getArgs(argList),
                            new String[]{ "-invalid invalid" },
                            requiredFlags);
        assertTrue(shell.getHostName().equals(host));
        assertEquals(Integer.valueOf(port).intValue(), shell.getRegistryPort());

        /* Check if the argument from RC file can be loaded successfully */
        shell.parseArgument(getArgs(argList),
                            new String[]{ STORE_FLAG, storeName },
                            requiredFlags);
        assertTrue(shell.getHostName().equals(host));
        assertEquals(Integer.valueOf(port).intValue(), shell.getRegistryPort());
        assertTrue(shell.getStoreName().equals(storeName));

        /*
         * Check if the argument from RC file can be overwritten by
         * input arguments.
         */
        addArg(argList, STORE_FLAG, storeName);
        shell.parseArgument(getArgs(argList),
                            new String[]{ STORE_FLAG, "invalid",
                                          USER_FLAG, "user1" },
                            requiredFlags);
        assertTrue(shell.getHostName().equals(host));
        assertEquals(Integer.valueOf(port).intValue(), shell.getRegistryPort());
        assertTrue(shell.getUserName().equals(userName));
        assertTrue(shell.getStoreName().equals(storeName));

        outStream.reset();
        argList.clear();
        requiredFlags = new String[] { HELPER_HOSTS_FLAG, STORE_FLAG };
        /*
         * -heler-hosts <host:port>* -store <storeName> -username <user>
         * -security <security_file> -timeout <timeout_ms>
         * -consistency <consistency> -durability <durability>
         *
         */
        shell = getTestShell(System.in, cmdOut);
        final String hostPort = host + ":" + port;
        addArg(argList, HELPER_HOSTS_FLAG,
               hostPort + "," + "A" + hostPort);
        addArg(argList, STORE_FLAG, storeName);
        addArg(argList, USER_FLAG, userName);
        addArg(argList, SECURITY_FLAG, securityFile);
        addArg(argList, TIMEOUT_FLAG, timeout);
        addArg(argList, CONSISTENCY_FLAG, consistency);
        addArg(argList, DURABILITY_FLAG, durability);
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        hostPorts = shell.getHostPorts(null);
        assertEquals(2, hostPorts.length);
        assertTrue(hostPorts[0].equals(hostPort));
        assertTrue(hostPorts[1].equals("A" + hostPort));
        assertTrue(shell.getStoreName().equals(storeName));
        assertTrue(shell.getUserName().equals(userName));
        assertTrue(shell.getSecurityFile().equals(securityFile));
        assertEquals(Integer.valueOf(timeout).intValue(),
                     shell.getRequestTimeout());
        final Consistency cons = shell.getStoreConsistency();
        final String consName = CommonShell.getConsistencyName(cons);
        assertTrue(consName.equals(consistency));
        final Durability dur = shell.getStoreDurability();
        final String durName = CommonShell.getDurabilityName(dur);
        assertTrue(durName.equals(durability));

        /* Hidden flags: -noprompt, -noconnect, -hidden. */
        addArg(argList, NOPROMPT_FLAG, null);
        addArg(argList, NOCONNECT_FLAG, null);
        addArg(argList, HIDDEN_FLAG, null);
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        assertTrue(shell.isNoPrompt());
        assertTrue(shell.isNoConnect());
        assertTrue(shell.getHidden());

        /* CommandToRun */
        final String[] commandToRun = {
            "commandToRun", "arg1", "arg2"
        };
        for (String arg: commandToRun) {
            addArg(argList, arg, null);
        }
        shell.parseArgument(getArgs(argList), null, requiredFlags);
        shell.start();
        final String output = outStream.toString();
        for (String arg: commandToRun) {
            assertTrue(output.contains(arg));
        }

        /* Test help flag */
        final String[] flags = {"-help", "help", "-?", "?"};
        for (String flag : flags) {
            outStream.reset();
            final TestShell ts = getTestShell(System.in, cmdOut);
            ts.parseArgument(new String[] {flag}, null, null);
            assertTrue(outStream.toString().equals(TestShell.USAGE));
        }
    }

    private void addArg(final List<String> argList,
                        final String flag,
                        final String value) {
        argList.add(flag);
        if (value != null) {
            argList.add(value);
        }
    }

    private String[] getArgs(final List<String> argList) {
        return argList.toArray(new String[argList.size()]);
    }

    /* ConnectStoreCommand coverage. */
    @Test
    public void testConnnectStoreCommandGetCommandDescription()
        throws Exception {

        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        final String expectedResult =
            ConnectStoreCommand.CONNECT_STORE_COMMAND_DESC;
        assertEquals(expectedResult, connObj.getCommandDescription());
    }

    @Test
    public void testConnectStoreCommandGetCommandSyntax()
        throws Exception {

        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        final String expectedResult =
            ConnectStoreCommand.CONNECT_STORE_COMMAND_SYNTAX;
        assertEquals(expectedResult, connObj.getCommandSyntax());
    }

    @Test
    public void testConnectStoreCommandExecuteInvalidArgument()
        throws Exception {
        final CommonShell shell = getTestShell();

        /* <port> must be in [1, 65535]*/
        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        String[] args = { ConnectStoreCommand.NAME, PORT_FLAG, "0" };
        runWithInvalidArgument(shell, connObj, args);
        args = new String[] { ConnectStoreCommand.NAME, PORT_FLAG, "65536" };
        runWithInvalidArgument(shell, connObj, args);

        /* <timeout> must be greater than 0 */
        args = new String[] { "conn", TIMEOUT_FLAG, "0" };
        runWithInvalidArgument(shell, connObj, args);

        /* Consistency */
        args = new String[] { "conn", CONSISTENCY_FLAG, "invalid" };
        runWithInvalidArgument(shell, connObj, args);

        /* Durability */
        args = new String[] {"conn", DURABILITY_FLAG, "invalid"};
        runWithInvalidArgument(shell, connObj, args);

        /* Missing required argument: -name <storeName> */
        args = new String[]{ "conn", HOST_FLAG, "localhost",
                            PORT_FLAG, "5000" };
        runWithRequiredFlag(shell, connObj, args, NAME_FLAG);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testConnectStoreCommandExecute() {
        startStore();
        assertTrue(createStore != null);

        final CommonShell shell = getTestShell();
        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        final int requestTimeout = 100000;
        String[] args = { "conn", HOST_FLAG, createStore.getHostname(),
                          PORT_FLAG,
                          String.valueOf(createStore.getRegistryPort()),
                          NAME_FLAG, createStore.getStoreName(),
                          TIMEOUT_FLAG, String.valueOf(requestTimeout),
                          CONSISTENCY_FLAG, "NONE_REQUIRED_NO_MASTER",
                          DURABILITY_FLAG, "COMMIT_WRITE_NO_SYNC"};

        args = createStore.maybeAddSecurityFlag(args);

        try {
            connObj.execute(args, shell);
            assertTrue(shell.getStore() != null);
            assertEquals(requestTimeout, shell.getRequestTimeout());
            assertEquals(Consistency.NONE_REQUIRED_NO_MASTER,
                         shell.getStoreConsistency());
            assertEquals(Durability.COMMIT_WRITE_NO_SYNC,
                         shell.getStoreDurability());
        } catch (ShellException se) {
            fail("Expected to be successfully, but get exception: " + se);
        }
        shell.shutdown();
    }

    @Test
    public void testConnectStoreCommandExecuteHelp() {
        final CommonShell shell = getTestShell();
        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        final String[] args = {"conn", "-?"};
        runWithHelpException(shell, connObj, args);
        shell.shutdown();
    }

    @Test
    public void testConnectStoreCommandExecuteUnknownArgCommand() {
        final CommonShell shell = getTestShell();
        final ConnectStoreCommand connObj = new ConnectStoreCommand();
        final String[] args = {"conn", "invalid-arg"};
        runWithUnknownArgument(shell, connObj, args);
        shell.shutdown();
    }

    /* HistoryCommand coverage. */
    @Test
    public void testHistoryCommandExecuteArgFromTo() throws Exception {

        /* Capture the output so the exception message is not printed to
         * the test output.
         */
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommonShell shell = getTestShell(System.in, cmdOut);

        /* Use the handleUnknownException method of CommonShell to inject
         * 4 lines of command history so that when the first and second
         * lines ("-from", "1", "-to", "2") of history are retrieved by
         * the execute method being tested here, there is a known result
         * that can be verified.
         */
        final String exceptionInput = "test exception [AdminFaultException]";
        final AdminFaultException exc =
                new AdminFaultException(new Exception(exceptionInput));
        final String[] excInputLines = {"history test line 0",
                                        "history test line 1",
                                        "history test line 2",
                                        "history test line 3"};
        String expectedResult = null;
        final StringBuffer expectedResultBuf = new StringBuffer();
        for (int i = 0; i < excInputLines.length; i++) {
            shell.handleUnknownException(excInputLines[i], exc);
            if (i == 0 || i == 1) {
                expectedResultBuf.append(
                    (i + 1) + " " + excInputLines[i] + Shell.eol);
            }
        }
        expectedResult = expectedResultBuf.toString();

        final HistoryCommand historyObj = new HistoryCommand();
        /* Retrieve the command history from line 1 to line 2, inclusive,
         * and verify the result.
         */
        final String[] args = {"test command", FROM_FLAG, "1", TO_FLAG, "2"};
        final String result = historyObj.execute(args, shell);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testHistoryCommandExecuteUnknownArgument() throws Exception {

        final CommonShell shell = getTestShell();
        final HistoryCommand historyObj = new HistoryCommand();
        final String badArg = "BAD ARG";
        final String[] args = {"test command", badArg};
        runWithUnknownArgument(shell, historyObj, args);
    }

    @Test
    public void testHistoryCommandExecuteBadLastIAE() throws Exception {

        final CommonShell shell = getTestShell();
        final HistoryCommand historyObj = new HistoryCommand();
        final String[] args = {"test command", LAST_FLAG, "BAD LAST ARG"};
        runWithInvalidArgument(shell, historyObj, args);
    }

    @Test
    public void testHistoryCommandExecuteBadFromIAE() throws Exception {

        final CommonShell shell = getTestShell();
        final HistoryCommand historyObj = new HistoryCommand();
        final String[] args = {"test command", FROM_FLAG, "BAD FROM ARG"};
        runWithInvalidArgument(shell, historyObj, args);
    }

    @Test
    public void testHistoryCommandExecuteBadToIAE() throws Exception {

        final CommonShell shell = getTestShell();
        final HistoryCommand historyObj = new HistoryCommand();
        final String[] args = {"test command", TO_FLAG, "BAD TO ARG"};
        runWithInvalidArgument(shell, historyObj, args);
    }

    @Test
    public void testHistoryCommandExecuteArgNone() throws Exception {

        final CommonShell shell = getTestShell();
        final String expectedResult = "";
        final HistoryCommand historyObj = new HistoryCommand();
        final String[] args = new String[0];
        final String result = historyObj.execute(args, shell);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testHistoryCommandExecuteHelp() {
        final CommonShell shell = getTestShell();
        final HistoryCommand historyObj = new HistoryCommand();
        final String[] args = {"history", "-help"};
        runWithHelpException(shell, historyObj, args);
        shell.shutdown();
    }

    @Test
    public void testHistoryCommandGetCommandDescription() throws Exception {

        final HistoryCommand historyObj = new TestHistoryCommand();
        final String expectedResult = HistoryCommand.DESCRIPTION;
        assertEquals(expectedResult, historyObj.getCommandDescription());
    }

    @Test
    public void testHistoryCommandGetCommandSyntax() throws Exception {

        final HistoryCommand historyObj = new TestHistoryCommand();
        final String expectedResult =
            "history [-last <n>] [-from <n>] [-to <n>]";
        assertEquals(expectedResult, historyObj.getCommandSyntax());
    }

    /* Sub-class to gain access to protected method(s). */
    private static class TestHistoryCommand extends HistoryCommand {
    }

    /* TimeCommand coverage. */
    @Test
    public void testTimeCommandGetCommandDescription() throws Exception {

        final TimeCommand timeObj = new TimeCommand();
        final String expectedResult = TimeCommand.DESCRIPTION;
        assertEquals(expectedResult, timeObj.getCommandDescription());
    }

    @Test
    public void testTimeCommandGetCommandSyntax() throws Exception {

        final TimeCommand timeObj = new TimeCommand();
        final String expectedResult = TimeCommand.SYNTAX;
        assertEquals(expectedResult, timeObj.getCommandSyntax());
    }

    @Test
    public void testTimeCommandExecuteBadArgCount() {
        final CommonShell shell = getTestShell();
        final TimeCommand timeObj = new TimeCommand();
        final String[] args = {"timer", ON_FLAG, OFF_FLAG};
        runWithIncorrectNumberOfArguments(shell, timeObj, args);
        shell.shutdown();
    }

    @Test
    public void testTimeCommandExecuteUnknownArgument() {
        final CommonShell shell = getTestShell();
        final TimeCommand timeObj = new TimeCommand();
        final String[] args = {"timer", "true"};
        runWithUnknownArgument(shell, timeObj, args);
        shell.shutdown();
    }

    @Test
    public void testTimeCommandExecute() {
        final String expectedOn = "Timer is now on";
        final String expectedOff = "Timer is now off";
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommonShell shell = getTestShell(System.in, cmdOut);
        final TimeCommand timeObj = new TimeCommand();
        try {
            String[] args = {"timer"};
            runCommand(shell, timeObj, args, expectedOff);

            args = new String[]{"timer", ON_FLAG};
            runCommand(shell, timeObj, args, expectedOn);

            args = new String[]{"timer"};
            runCommand(shell, timeObj, args, expectedOn);

            shell.runLine("timer");
            final String expectedMsg = "Time:";
            assertTrue("Expected to get output message that contain " +
                expectedMsg + ", but not",
                outStream.toString().indexOf(expectedMsg) >= 0);

            args = new String[]{"timer", OFF_FLAG};
            runCommand(shell, timeObj, args, expectedOff);

            args = new String[]{"timer"};
            runCommand(shell, timeObj, args, expectedOff);
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            fail("Not expected to caught exception");
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testTimeCommandExecuteHelp() {
        final CommonShell shell = getTestShell();
        final TimeCommand timeObj = new TimeCommand();
        final String[] args = {"timer", "-help"};
        runWithHelpException(shell, timeObj, args);
        shell.shutdown();
    }

    /* PageCommand coverage. */
    @Test
    public void testPageCommandGetCommandDescription() throws Exception {

        final PageCommand pageObj = new PageCommand();
        final String expectedResult = PageCommand.DESCRIPTION;
        assertEquals(expectedResult, pageObj.getCommandDescription());
    }

    @Test
    public void testPageCommandGetCommandSyntax() throws Exception {

        final PageCommand pageObj = new PageCommand();
        final String expectedResult = PageCommand.SYNTAX;
        assertEquals(expectedResult, pageObj.getCommandSyntax());
    }

    @Test
    public void testPageCommandExecuteBadArgCount() {
        final CommonShell shell = getTestShell();
        final PageCommand pageObj = new PageCommand();
        final String[] args = {"page", ON_FLAG, OFF_FLAG};
        runWithIncorrectNumberOfArguments(shell, pageObj, args);
        shell.shutdown();
    }

    @Test
    public void testPageCommandExecuteInvalidCommand() {
        final CommonShell shell = getTestShell();
        final PageCommand pageObj = new PageCommand();
        String[] args = {"page", "abc"};
        runWithInvalidArgument(shell, pageObj, args);

        args = new String[] { "page", "-1" };
        runWithInvalidArgument(shell, pageObj, args);
        shell.shutdown();
    }

    @Test
    public void testPageCommandExecute() {
        final String expectedOn = "Paging mode is now on, height: ";
        final String expectedOff = "Paging mode is now off";
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommonShell shell = getTestShell(System.in, cmdOut);
        final PageCommand pageObj = new PageCommand();
        try {
            final int height = 100;
            String[] args = {"page", String.valueOf(height)};
            runCommand(shell, pageObj, args, expectedOn + height);

            args = new String[]{"page", "off"};
            runCommand(shell, pageObj, args, expectedOff);

            args = new String[]{"page"};
            runCommand(shell, pageObj, args, expectedOff);

            args = new String[]{"page", String.valueOf(height)};
            runCommand(shell, pageObj, args, expectedOn + height);

            args = new String[]{"page"};
            runCommand(shell, pageObj, args, expectedOn + height);

        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            fail("Not expected to caught exception");
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testPageCommandExecuteHelp() {
        final CommonShell shell = getTestShell();
        final PageCommand pageObj = new PageCommand();
        final String[] args = {"page", "-help"};
        runWithHelpException(shell, pageObj, args);
    }

    /* CommonShell.HiddenCommand coverage. */

    @Test
    public void testHiddenCommandGetCommandDescription() throws Exception {

        final HiddenCommand hiddenObj = new HiddenCommand();
        final String expectedResult = HiddenCommand.DESCRIPTION;
        assertEquals(expectedResult, hiddenObj.getCommandDescription());
    }

    @Test
    public void testHiddenCommandGetCommandSyntax() throws Exception {

        final HiddenCommand hiddenObj = new HiddenCommand();
        final String expectedResult = HiddenCommand.SYNTAX;
        assertEquals(expectedResult, hiddenObj.getCommandSyntax());
    }

    @Test
    public void testHiddenCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final CommonShell.HiddenCommand hiddenObj =
            new CommonShell.HiddenCommand();
        final String hiddenEnabled = "enabled";
        final String hiddenDisabled = "disabled";
        final String[] args0 = { "test command", "off" };
        final String result0 = hiddenObj.execute(args0, shell);
        assertTrue("CommonShell.HiddenCommand.execute did not" +
                   "return expected result: expected '" + hiddenDisabled +
                   "' but returned '" + result0 + "'",
                   result0.contains(hiddenDisabled));

        final String[] args1 = { "test command", "on" };
        final String result1 = hiddenObj.execute(args1, shell);
        assertTrue("CommonShell.HiddenCommand.execute did not" +
                   "return expected result: expected '" + hiddenEnabled +
                   "' but returned '" + result1 + "'",
                   result1.contains(hiddenEnabled));
    }

    @Test
    public void testToggleHiddenCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final CommonShell.HiddenCommand hiddenObj =
            new CommonShell.HiddenCommand();
        final String hiddenEnabled = "enabled";
        final String hiddenDisabled = "disabled";
        final String args[] = { "test command" };
        final String result0 = hiddenObj.execute(args, shell);
        if (result0.contains(hiddenEnabled)) {
            final String result1 = hiddenObj.execute(args, shell);
            assertTrue("CommonShell.HiddenCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + hiddenDisabled +
                       "'", result1.contains(hiddenDisabled));
        } else if (result0.contains(hiddenDisabled)) {
            final String result1 = hiddenObj.execute(args, shell);
            assertTrue("CommonShell.HiddenCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + hiddenEnabled +
                       "'", result1.contains(hiddenEnabled));
        } else {
            fail("CommonShell.HiddenCommand.execute did not return " +
                 "one of two expected results: [either '" + hiddenEnabled +
                 " or '" + hiddenDisabled + "', but returned '" + result0 +
                 "'");
        }
    }

    @Test
    public void testHiddenCommandIsHidden() throws Exception {

        final CommonShell.HiddenCommand hiddenObj = new TestHiddenCommand();
        assertTrue("CommonShell.HiddenCommand.isHidden should always " +
                   "return true",  hiddenObj.isHidden());
    }

    /* Sub-class to gain access to protected method(s). */
    private static class TestHiddenCommand extends HiddenCommand {
    }

    /* VerboseCommand coverage. */

    @Test
    public void testVerboseCommandGetCommandDescription() throws Exception {

        final VerboseCommand verboseObj = new VerboseCommand();
        final String expectedResult = VerboseCommand.DESCRIPTION;
        assertEquals(expectedResult, verboseObj.getCommandDescription());
    }

    @Test
    public void testVerboseCommandGetCommandSyntax() throws Exception {

        final VerboseCommand verboseObj = new VerboseCommand();
        final String expectedResult = VerboseCommand.SYNTAX;
        assertEquals(expectedResult, verboseObj.getCommandSyntax());
    }

    @Test
    public void testVerboseCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final VerboseCommand verboseObj =
            new VerboseCommand();
        final String verboseOn = "Verbose mode is now on";
        final String verboseOff = "Verbose mode is now off";
        final String[] args0 = {"test command", "off" };
        final String result0 = verboseObj.execute(args0, shell);
        assertTrue("VerboseCommand.execute did not" +
                   "return expected result: expected '" + verboseOff + "' " +
                   "but returned '" + result0 + "'",
                   verboseOff.equals(result0));
        assertFalse(shell.getVerbose());

        final String[] args1 = {"test command", "on" };
        final String result1 = verboseObj.execute(args1, shell);
        assertTrue("VerboseCommand.execute did not" +
                   "return expected result: expected '" + verboseOn + "' " +
                   "but returned '" + result1 + "'",
                   verboseOn.equals(result1));
        assertTrue(shell.getVerbose());
    }

    @Test
    public void testToggleVerboseCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final VerboseCommand verboseObj = new VerboseCommand();
        final String verboseOn = "Verbose mode is now on";
        final String verboseOff = "Verbose mode is now off";
        final String args[] = { "test command" };
        final String result0 = verboseObj.execute(args, shell);
        if (verboseOn.equals(result0)) {
            final String result1 = verboseObj.execute(args, shell);
            assertTrue("VerboseCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + verboseOff + "'",
                       verboseOff.equals(result1));
        } else if (verboseOff.equals(result0)) {
            final String result1 = verboseObj.execute(args, shell);
            assertTrue("VerboseCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + verboseOn + "'",
                       verboseOn.equals(result1));
        } else {
            fail("VerboseCommand.execute did not return " +
                 "one of two expected results: [either '" + verboseOn +
                 " or '" + verboseOff + "', but returned '" + result0 + "'");
        }
    }

    /* DebugCommand coverage. */

    @Test
    public void testDebugCommandGetCommandDescription() throws Exception {

        final DebugCommand debugObj = new DebugCommand();
        final String expectedResult = DebugCommand.DESCRIPTION;
        assertEquals(expectedResult, debugObj.getCommandDescription());
    }

    @Test
    public void testDebugCommandGetCommandSyntax() throws Exception {

        final DebugCommand debugObj = new DebugCommand();
        final String expectedResult = DebugCommand.SYNTAX;
        assertEquals(expectedResult, debugObj.getCommandSyntax());
    }

    @Test
    public void testDebugCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final DebugCommand debugObj = new DebugCommand();
        final String debugOn = "Debug mode is now on";
        final String debugOff = "Debug mode is now off";
        final String[] args0 = {"test command", "off" };
        final String result0 = debugObj.execute(args0, shell);
        assertTrue("DebugCommand.execute did not" +
                   "return expected result: expected '" + debugOff + "' " +
                   "but returned '" + result0 + "'",
                   debugOff.equals(result0));
        assertFalse(shell.getDebug());

        final String[] args1 = {"test command", "on" };
        final String result1 = debugObj.execute(args1, shell);
        assertTrue("DebugCommand.execute did not" +
                   "return expected result: expected '" + debugOn + "' " +
                   "but returned '" + result1 + "'",
                   debugOn.equals(result1));
        assertTrue(shell.getDebug());
    }

    @Test
    public void testToggleDebugCommandExecute() throws Exception {

        final CommonShell shell = getTestShell();
        final DebugCommand debugObj = new DebugCommand();
        final String debugOn = "Debug mode is now on";
        final String debugOff = "Debug mode is now off";
        final String args[] = { "test command" };
        final String result0 = debugObj.execute(args, shell);
        if (debugOn.equals(result0)) {
            final String result1 = debugObj.execute(args, shell);
            assertTrue("DebugCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + debugOff + "'",
                       debugOff.equals(result1));
        } else if (debugOff.equals(result0)) {
            final String result1 = debugObj.execute(args, shell);
            assertTrue("DebugCommand.execute did not " +
                       "return expected result: previous call returned '" +
                       result0 + "',  current call returned '" +
                       result1 + "', but expected '" + debugOn + "'",
                       debugOn.equals(result1));
        } else {
            fail("DebugCommand.execute did not return " +
                 "one of two expected results: [either '" + debugOff +
                 " or '" + debugOn + "', but returned '" + result0 + "'");
        }
    }

    @Test
    public void testDebugCommandIsHidden() throws Exception {

        final DebugCommand debugObj = new DebugCommand();
        assertTrue("DebugCommand.isHidden should always return true",
                   debugObj.isHidden());
    }

    /*
     * NamespaceCommand coverage.
     */
    @Test
    public void testNamespaceCommandGetCommandDescription() throws Exception {
        final NamespaceCommand nscmd = new NamespaceCommand();
        final String expectedResult = NamespaceCommand.DESCRIPTION;
        assertEquals(expectedResult, nscmd.getCommandDescription());
    }

    @Test
    public void testNamespaceCommandGetCommandSyntax() throws Exception {
        final NamespaceCommand nscmd = new NamespaceCommand();
        final String expectedResult = NamespaceCommand.SYNTAX;
        assertEquals(expectedResult, nscmd.getCommandSyntax());
    }

    @Test
    public void testNamespaceCommandExecuteBadArgCount() {
        final CommonShell shell = getTestShell();
        final NamespaceCommand nscmd = new NamespaceCommand();
        final String[] args = {"namespace", "test", "test"};
        runWithIncorrectNumberOfArguments(shell, nscmd, args);
        shell.shutdown();
    }

    @Test
    public void testNamespaceCommandExecute() {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        final CommonShell shell = getTestShell(System.in, cmdOut);
        final NamespaceCommand nscmd = new NamespaceCommand();
        try {
            String[] args = new String[]{"Namespace", "NoSQL"};
            String expRet = "Namespace is NoSQL";
            runCommand(shell, nscmd, args, expRet);

            args = new String[]{"Namespace"};
            expRet = "Namespace is " + TableAPI.SYSDEFAULT_NAMESPACE_NAME;
            runCommand(shell, nscmd, args, expRet);
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            fail("Not expected to caught exception");
        }/* CHECKSTYLE:ON */ finally {
            shell.shutdown();
        }
    }

    @Test
    public void testNamespaceCommandExecuteHelp() {
        final CommonShell shell = getTestShell();
        final NamespaceCommand nscmd = new NamespaceCommand();
        final String[] args = {"namespace", "-help"};
        runWithHelpException(shell, nscmd, args);
        shell.shutdown();
    }

    private void runCommand(final Shell shell,
                            final ShellCommand cmdObj,
                            final String[] args,
                            final String expectedMsg)
        throws ShellException {

        final String result = cmdObj.execute(args, shell);
        assertTrue("Expected to get message that start with " +
            expectedMsg + ", but get " + result,
            result.startsWith(expectedMsg));
    }

    private void startStore() {
        try {
            createStore = new CreateStore(kvstoreName,
                                          PORT,
                                          1, /* Storage nodes */
                                          1, /* Replication factor */
                                          10, /* Partitions */
                                          1,  /* Capacity per SN */
                                          CreateStore.MB_PER_SN,
                                          false, /* Use threads is false */
                                          null);
            createStore.start();
        } catch (Exception e) {
            fail("unexpected exception in createStore: " + e);
        }
    }

    private void runWithInvalidArgument(final Shell shell,
                                        final ShellCommand cmdObj,
                                        final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellArgumentException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue("Expected to catch ShellArgumentException but not",
                (e instanceof ShellArgumentException));
        }
    }

    private void runWithRequiredFlag(final Shell shell,
                                     final ShellCommand cmdObj,
                                     final String[] args,
                                     final String requiredFlag) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains(requiredFlag));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    private void runWithIncorrectNumberOfArguments(final Shell shell,
                                                   final ShellCommand cmdObj,
                                                   final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains("Incorrect number of arguments" +
            		" for command"));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    private void runWithUnknownArgument(final Shell shell,
                                        final ShellCommand cmdObj,
                                        final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains("Unknown argument"));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    private void runWithHelpException(final Shell shell,
                                      final ShellCommand cmdObj,
                                      final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellHelpException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue("Expected to catch ShellHelpException but not",
                (e instanceof ShellHelpException));
        }
    }

    private static class TestShell extends CommonShell {
        static String USAGE = "TestShell usage message";
        public static final
            List<? extends ShellCommand> commands =
                           Arrays.asList(new ConnectStoreCommand(),
                                         new HistoryCommand(),
                                         new PageCommand(),
                                         new TimeCommand());

        private TestParser parser = null;

        public TestShell(InputStream input, PrintStream output) {
            super(input, output);
        }

        @Override
        public List<? extends ShellCommand> getCommands() {
            return commands;
        }

        @Override
        public String getPrompt() {
            return "test-> ";
        }

        @Override
        public String getUsageHeader() {
            return null;
        }

        @Override
        public void init() {
        }

        @Override
        public void shutdown() {
        }

        public void parseArgument(String[] args,
                                  String[] rcArgs,
                                  String[] requiredFlags) {

            parser = new TestParser(args, rcArgs, requiredFlags);
            parser.parseArgs();
        }

        @Override
        public String run(String commandName, String[] args) {
            return "Run " + commandName + " " + Arrays.toString(args);
        }

        public String getHostName() {
            if (parser == null) {
                return null;
            }
            return (parser == null) ? null : parser.getHostname();
        }

        public int getRegistryPort() {
            return (parser == null) ? 0 : parser.getRegistryPort();
        }

        public String getStoreName() {
            return (parser == null) ? null : parser.getStoreName();
        }

        public String getUserName() {
            return (parser == null) ? null : parser.getUserName();
        }

        public String getSecurityFile() {
            return (parser == null) ? null : parser.getSecurityFile();
        }

        public class TestParser extends ShellParser {
            public TestParser(String[] args,
                              String[] rcArgs,
                              String[] requiredFlags) {
                super(args, rcArgs, requiredFlags);
            }

            @Override
            public String getShellUsage() {
                return USAGE;
            }

            @Override
            public void usage(String errorMsg) {
                if (errorMsg != null) {
                    output.println(errorMsg);
                }
                final String usage = getShellUsage();
                output.print(usage);
            }
        }
    }
}
