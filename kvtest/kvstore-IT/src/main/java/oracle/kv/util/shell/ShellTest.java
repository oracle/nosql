/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util.shell;

import static oracle.kv.util.shell.Shell.eol;
import static oracle.kv.util.shell.Shell.tab;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.shell.Shell.CommandHistory;
import oracle.kv.util.shell.Shell.CommandHistoryElement;
import oracle.kv.util.shell.Shell.ExitCommand;
import oracle.kv.util.shell.Shell.HelpCommand;
import oracle.kv.util.shell.Shell.LoadCommand;

import org.junit.Test;

/**
 * Test the abstract Shell class.
 */
public class ShellTest extends TestBase {

    /* Default input provides nothing */
    private final InputStream nullInput = new ByteArrayInputStream(new byte[0]);

    /* Default output captures in byte array */
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final PrintStream captureOutput = new PrintStream(baos);

    private static final String INVALID_ARGUMENT_STR = "Invalid argument:";
    private static final String UNKNOWN_ARGUMENT_STR = "Unknown argument:";

    @Test
    public void testDeprecated() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        final boolean showDeprecated = shell.showDeprecated();
        /* By default showDeprecated is false. Validate this. */
        assertFalse(showDeprecated);

        /* Set showDeprecated to true. Validate the value is set to true. */
        shell.setShowDeprecated(true);
        assertTrue(shell.showDeprecated());

        /* Set showDeprecated to false. Validate the value is set to false. */
        shell.setShowDeprecated(false);
        assertFalse(shell.showDeprecated());
    }

    @Test
    public void testHidden() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        final boolean showHidden = shell.getHidden();
        shell.toggleHidden();
        assertTrue(showHidden != shell.getHidden());

        shell.toggleHidden();
        assertEquals(showHidden, shell.getHidden());
    }

    @Test
    public void testGetUsage() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        final TestCommand cmd = shell.testCommand;
        final String usageNoCmd = shell.getUsageHeader();
        final String usageShowCmd =
            shell.getUsageHeader() + tab + cmd.getCommandName() + eol;

        /* Make sure we start off with showHidden == false */
        if (shell.getHidden()) {
            shell.toggleHidden();
        }
        assertFalse(shell.getHidden());

        String usage = null;

        /* cmd is hidden and we do not show hidden */
        cmd.setHidden(true);
        usage = shell.getUsage();
        assertEquals(usage, usageNoCmd);

        /* cmd is not hidden and we do not show hidden */
        cmd.setHidden(false);
        usage = shell.getUsage();
        assertEquals(usage, usageShowCmd);

        /* Change the shell to show hidden commands */
        shell.toggleHidden();
        assertTrue(shell.getHidden());

        /* cmd is hidden but we show hidden */
        cmd.setHidden(true);
        usage = shell.getUsage();
        assertEquals(usage, usageShowCmd);

        /* cmd is not hidden and we also show hidden */
        cmd.setHidden(false);
        usage = shell.getUsage();
        assertEquals(usage, usageShowCmd);

        /* cmd is not hidden but has a null command name */
        cmd.setHidden(false);
        cmd.setCommandName(null);
        usage = shell.getUsage();
        assertEquals(usage, usageNoCmd);
    }

    @Test
    public void testPrompt() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        shell.setPrompt(null);
        shell.prompt();
        assertEquals("", baos.toString());

        shell.setPrompt(TestShell.DEF_PROMPT);
        shell.prompt();
        assertEquals(TestShell.DEF_PROMPT, baos.toString());
    }

    @Test
    public void testDoRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        shell.setRetryFromSuper();
        final boolean retry = shell.doRetry();

        shell.setRetry(!retry);
        assertFalse(retry == shell.doRetry());
    }

    /*
     * Tests handleShellException, and secondarily, getExitCode()
     */
    @Test
    public void testHandleShellException() {
        subTestHandleShellExceptionRetry();
        subTestHandleShellExceptionSE();
        subTestHandleShellExceptionSHE();
        subTestHandleShellExceptionSUE();
        subTestHandleShellExceptionSAE();
    }

    private void subTestHandleShellExceptionRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String msg = "A message";
        final ShellException se = new ShellException(msg);
        final String line = "Error line";

        shell.setRetry(true);
        assertTrue(shell.handleShellException(line, se));
        assertEquals(0, shell.getExitCode());
        assertEquals(0, shell.getHistory().getSize());
    }

    private void subTestHandleShellExceptionSE() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String msg = "A message";
        final ShellException se = new ShellException(msg);
        final String line = "Error line";

        shell.setRetry(false);
        assertFalse(shell.handleShellException(line, se));
        assertEquals(1, shell.getExitCode());
        assertTrue(baos.toString().contains(
                       "Error handling command " + line));
        assertTrue(baos.toString().contains(se.getMessage()));
        assertEquals(lastCommandEntry(shell).getCommand(), line);
    }

    private void subTestHandleShellExceptionSHE() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final ShellHelpException she =
            new ShellHelpException(shell.testCommand);
        final String line = "Error line";

        shell.setRetry(false);
        assertFalse(shell.handleShellException(line, she));
        assertEquals(Shell.EXIT_USAGE, shell.getExitCode());
        assertTrue(baos.toString().contains(
                       she.getVerboseHelpMessage()));
        assertEquals(lastCommandEntry(shell).getCommand(), line);
    }

    private void subTestHandleShellExceptionSUE() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String msg = "A message";
        final ShellUsageException sue =
            new ShellUsageException(msg, shell.testCommand);
        final String line = "Error line";

        shell.setRetry(false);
        assertFalse(shell.handleShellException(line, sue));
        assertEquals(Shell.EXIT_USAGE, shell.getExitCode());
        assertTrue(baos.toString().contains(sue.getMessage()));
        assertTrue(baos.toString().contains(sue.getVerboseHelpMessage()));
        assertEquals(lastCommandEntry(shell).getCommand(), line);
    }

    private void subTestHandleShellExceptionSAE() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String msg = "A message";
        final ShellArgumentException sae = new ShellArgumentException(msg);
        final String line = "Error line";

        shell.setRetry(false);
        assertFalse(shell.handleShellException(line, sae));
        assertEquals(Shell.EXIT_INPUTERR, shell.getExitCode());
        assertTrue(baos.toString().contains(sae.getMessage()));
        assertEquals(lastCommandEntry(shell).getCommand(), line);
    }

    @Test
    public void testHandleUnknownException() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String msg = "A message";
        final IllegalArgumentException iae = new IllegalArgumentException(msg);
        final String line = "Error line";

        shell.handleUnknownException(line, iae);
        assertEquals(Shell.EXIT_UNKNOWN, shell.getExitCode());
        assertTrue(baos.toString().contains(iae.getClass().getName()));
        assertEquals(lastCommandEntry(shell).getCommand(), line);
    }

    /*
     * Primarily tests verboseOutput(), but secondarily tests getVerbose(),
     * toggleVerbose(), and indirectly, checkVerbose().
     */
    @Test
    public void testVerboseOutput() {

        try {
            final TestShell shell = new TestShell(nullInput, captureOutput);
            final String msg1 = "message 1";
            final String msg2 = "message 2";
            final String msg3 = "message 3";
            final String msg4 = "message 4";

            /* initialize the state of global_verbose to true */
            if (!shell.toggleVerbose()) {
                assertTrue(shell.toggleVerbose());
            }

            /* verbose = true, global_verbose = true */

            /* set cmd-level verbose to true */
            shell.runLine(TestCommand.LINE_VERBOSE);
            assertTrue(shell.getVerbose());
            shell.verboseOutput(msg1);
            assertTrue(baos.toString().contains(msg1));

            /* verbose = true, global_verbose = false */
            assertFalse(shell.toggleVerbose());
            assertTrue(shell.getVerbose());
            shell.verboseOutput(msg2);
            assertTrue(baos.toString().contains(msg2));

            /* verbose = false, global_verbose = false */

            /* set cmd-level verbose to false */
            shell.runLine(TestCommand.LINE_NORMAL);
            assertFalse(shell.getVerbose());
            shell.verboseOutput(msg3);
            assertFalse(baos.toString().contains(msg3));

            /* verbose = false, global_verbose = true */
            assertTrue(shell.toggleVerbose());
            shell.verboseOutput(msg4);
            assertTrue(baos.toString().contains(msg4));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    @Test
    public void testTerminate() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        assertFalse(shell.getTerminate());
        shell.setTerminate();
        assertTrue(shell.getTerminate());
    }

    /*
     * Test the loop() method. Also tests getInput() and getOutput.
     */

    @Test
    public void testLoop() {
        final String[] lines = new String[] {
            TestCommand.LINE_NORMAL,
            TestCommand.LINE_SHELL_EXCEPTION,
            TestCommand.LINE_NORMAL
        };

        final InputStream input = makeInputStream(lines);
        final TestShell shell = new TestShell(input, captureOutput);

        /* input reader is initialized by loop() */
        assertTrue(shell.getInput() == null);
        assertTrue(shell.getOutput() == captureOutput);

        shell.loop();

        // TBD: init() doesn't get called???
        //assertTrue(shell.getInitOccurred());
        assertEquals(3, shell.testCommand.getExecuteCount());
        assertTrue(shell.getShutdownOccurred());
        assertTrue(baos.toString().contains("Exception reading input:"));
        assertTrue(shell.getInput() != null);
    }

    @Test
    public void testLoopMultiLineCommand() {
        subTestLoopMultiCommandsInSingleLine();
        subTestLoopCommandInMultiLine();
        subTestLoopOutOfMultiLinesInput();
    }

    private void subTestLoopMultiCommandsInSingleLine() {
        final String cmdSemicolonSingleQuoted =
            TestCommand.CMD_NAME + " -arg '\";Bejing; China;\"'";
        final String cmdSemicolonDoubleQuoted =
            TestCommand.CMD_NAME + " -arg \";Bejing; China;\"";
        final String[] lines = new String[] {
            TestCommand.CMD_NAME + ";" + TestCommand.LINE_VERBOSE + ";",
            TestCommand.CMD_NAME + "; " + TestCommand.LINE_VERBOSE + "; ",
            "; " + TestCommand.CMD_NAME + ";" + TestCommand.LINE_VERBOSE + "; ",
            cmdSemicolonSingleQuoted + "; " + cmdSemicolonDoubleQuoted + "; "
        };

        final InputStream input = makeInputStream(lines);
        final TestShell shell = new TestShell(input, captureOutput);

        shell.loop();
        assertEquals(8, shell.testCommand.getExecuteCount());
        assertEquals(cmdSemicolonDoubleQuoted,
                     lastCommandEntry(shell).getCommand());
    }

    private void subTestLoopCommandInMultiLine() {

        final String[] lines = new String[] {
            TestCommand.CMD_NAME + ";",
            TestCommand.CMD_NAME, ";",
            TestCommand.CMD_NAME, TestCommand.ARG_OK + ";",
            TestCommand.CMD_NAME, TestCommand.ARG_OK, ";",
            TestCommand.CMD_NAME, "", TestCommand.ARG_OK, "", ";",
            TestCommand.CMD_NAME + " \\", TestCommand.ARG_OK + " \\","-verbose",
            TestCommand.CMD_NAME + " \\", TestCommand.ARG_OK + ";",
            TestCommand.CMD_NAME,
                TestCommand.ARG_OK + "; " + TestCommand.CMD_NAME, "", ";",
            TestCommand.LINE_NORMAL + "; " + TestCommand.LINE_VERBOSE, ";",
            TestCommand.CMD_NAME + " \"", "test\"", ";",
            TestCommand.CMD_NAME + " \"test", "\"", ";",
        };

        final String[] expCmdsExecuted = new String[] {
            TestCommand.CMD_NAME,
            TestCommand.CMD_NAME,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + " " + TestCommand.ARG_OK + " -verbose" ,
            TestCommand.CMD_NAME + " " + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME,
            TestCommand.LINE_NORMAL,
            TestCommand.LINE_VERBOSE,
            TestCommand.CMD_NAME + " \"\ntest\"",
            TestCommand.CMD_NAME + " \"test\n\"",
        };

        final InputStream input = makeInputStream(lines);
        final TestShell shell = new TestShell(input, captureOutput);
        final CommandHistory history = shell.getHistory();

        shell.loop();
        assertEquals(18, shell.testCommand.getExecuteCount());
        assertTrue(history.getSize() >= expCmdsExecuted.length);
        for (int i = 0; i < expCmdsExecuted.length; i++) {
            assertEquals(expCmdsExecuted[(expCmdsExecuted.length - i - 1)],
                         history.get(history.getSize() - i - 1).getCommand());
        }
    }

    private void subTestLoopOutOfMultiLinesInput() {
        final String[] lines = new String[] {
            TestCommand.CMD_NAME, "", "?"
        };

        final InputStream input = makeInputStream(lines);
        baos.reset();
        final TestShell shell = new TestShell(input, captureOutput);
        shell.loop();
        assertEquals(2, shell.testCommand.getExecuteCount());
        assertEquals(TestCommand.CMD_NAME + " ?",
            lastCommandEntry(shell).getCommand());
        assertTrue(baos.toString().contains("Usage:"));
    }

    @Test
    public void testPrintln() {
        final String itHappened = "IT HAPPENED";
        final TestShell shell = new TestShell(nullInput, captureOutput);

        shell.println(itHappened);
        assertTrue(baos.toString().contains(itHappened));
    }

    @Test
    public void testExecute() {
        subTestExecuteEmpty();
        subTestExecuteNormal();
        subTestExecuteSERetry();
        subTestExecuteSENoRetry();
        subTestExecuteUENoRetry();
    }

    private void subTestExecuteEmpty() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        shell.execute("");
        assertEquals(0, shell.getHistory().getSize());

        shell.execute("      ");
        assertEquals(0, shell.getHistory().getSize());
    }

    private void subTestExecuteNormal() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        shell.execute(TestCommand.LINE_NORMAL);
        assertEquals(1, shell.getHistory().getSize());
    }

    private void subTestExecuteSERetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        /* Cause an exception to happen, allow retry, and succeed */
        shell.setRetryCount(1);
        shell.execute(TestCommand.LINE_SHELL_EXCEPTION);
        assertEquals(1, shell.getHistory().getSize());
    }

    private void subTestExecuteSENoRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        /* Cause an exception to happen, disallow retry */
        shell.setRetry(false);
        shell.execute(TestCommand.LINE_SHELL_EXCEPTION);
        assertEquals(1, shell.getHistory().getSize());
    }

    private void subTestExecuteUENoRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        /* Cause an exception to happen, disallow retry */
        shell.setRetry(false);
        shell.execute(TestCommand.LINE_UNKNOWN_EXCEPTION);
        assertEquals(1, shell.getHistory().getSize());
    }

    @Test
    public void testFindCommand() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        assertEquals(shell.testCommand,
                     shell.findCommand(TestCommand.CMD_NAME));
        assertTrue(shell.findCommand("bazooka") == null);
    }

    /*
     * The checks of resultA, resultB, resultC generate erroneous nullpointer
     * warnings
     */
    @Test
    public void testExtractArg() {
        final String[] testArgs = new String[] { "-a", "-b", "-b", "-c" };

        /*
         * The extractArg method is named oddly.  It really removes the
         * argument rather than extracting it.
         */

        /* Remove the first arg */
        final String[] resultA = Shell.extractArg(testArgs, "-a");
        assertTrue(resultA != null);
        assertTrue(countOf("-a", resultA) == 0);
        assertTrue(resultA.length - countOf(null, resultA) == 3);

        /* Remove the multiple occurrences of an arg */
        final String[] resultB = Shell.extractArg(testArgs, "-b");
        assertTrue(resultB != null);
        assertTrue(countOf("-b", resultB) == 0);
        assertTrue(resultB.length - countOf(null, resultB) == 2);

        /* Remove the final arg */
        final String[] resultC = Shell.extractArg(testArgs, "-c");
        assertTrue(resultC != null);
        assertTrue(countOf("-c", resultC) == 0);
        assertTrue(resultC.length - countOf(null, resultC) == 3);

        /*
         * If testArgs does not contain the specified argument, an array
         * bounds exception occurrs, so that case is not tested.
         */
    }

    @Test
    public void testParseLine() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String[] inputTokens = new String[] {
            "ABC", "DEF", "HIJ KLM", "123"};
        final String jsonString =
            "{\"ABC\":1,\"DEF\":{\"HIJ\":{\"KLM OPQ\":1}}}";
        final String jsonStringMultiLines =
            "{\"ABC\":1,\n\"DEF\":\n{\"HIJ\":\n{\"KLM OPQ\":1}\n}\n}";

        final StringBuilder sb = new StringBuilder();
        for (String t : inputTokens) {
            sb.append(" ");
            if (t.indexOf(" ") >= 0) {
                /* quote the token */
                sb.append('"');
                sb.append(t);
                sb.append('"');
            } else {
                sb.append(t);
            }
        }
        sb.append(" ");

        int len = inputTokens.length;
        /* Append jsonString without escape character.*/
        sb.append('\'');
        sb.append(jsonString);
        sb.append("' ");
        len++;

        /* Append jsonString with escape character. */
        sb.append('"');
        sb.append(jsonString.replace("\"", "\\\""));
        sb.append('"');
        len++;

        /* Append jsonStringMultiLines. */
        sb.append('\'');
        sb.append(jsonStringMultiLines);
        sb.append('\'');
        len++;

        final String[] tokens = shell.parseLine(sb.toString());
        assertEquals(tokens.length, len);
        for (int i = 0; i < inputTokens.length; i++) {
            assertEquals(inputTokens[i], tokens[i]);
        }
        assertEquals(tokens[tokens.length - 3], jsonString);
        assertEquals(tokens[tokens.length - 2], jsonString);
        assertEquals(tokens[tokens.length - 1], jsonStringMultiLines);
    }

    @Test
    public void testRunLine() {
        try {
            final TestShell shell = new TestShell(nullInput, captureOutput);

            shell.runLine("");
            assertEquals(0, shell.getHistory().getSize());

            shell.runLine("# This is it");
            assertEquals(0, shell.getHistory().getSize());

            shell.runLine(TestCommand.LINE_NORMAL);
            assertEquals(1, shell.getHistory().getSize());
            assertEquals(TestCommand.LINE_NORMAL,
                         lastCommandEntry(shell).getCommand());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    @Test
    public void testRun()  {
        subTestRunOK();
        subTestRunExcept();
        subTestRunNotFound();
    }

    private void subTestRunOK() {
        try {
            final TestShell shell = new TestShell(nullInput, captureOutput);

            final String result =
                shell.run(TestCommand.CMD_NAME,
                          new String[] { TestCommand.CMD_NAME,
                                         TestCommand.ARG_OK });
            assertEquals(result, TestCommand.RETURN_SUCCESS);
            assertEquals(1, shell.testCommand.getExecuteCount());
            /* run() doesn't set history */
            assertEquals(0, shell.getHistory().getSize());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestRunExcept() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.run(TestCommand.CMD_NAME,
                      new String[] { TestCommand.CMD_NAME,
                                     TestCommand.ARG_SE });
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellException.class, se.getClass());
            assertEquals(1, shell.testCommand.getExecuteCount());
        }
    }

    private void subTestRunNotFound() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.run("bazooka", new String[0]);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellArgumentException.class, se.getClass());
        }
    }

    @Test
    public void testNextArg() {
        try {
            final TestShell shell = new TestShell(nullInput, captureOutput);
            final String expectedResult = "bar";

            try {
                Shell.nextArg(new String[] { "-foo" }, 0, shell.testCommand);
                fail("expected exception");
            } catch (ShellException se) {
                assertEquals(ShellUsageException.class, se.getClass());
                assertTrue(se.getMessage().contains("requires an argument"));
            }

            final String result =
                Shell.nextArg(
                    new String[] {
                        "-foo", expectedResult }, 0, shell.testCommand);
            assertEquals(expectedResult, result);
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    @Test
    public void testInvalidArgument() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.testCommand.invalidArgument("foo");
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellArgumentException.class, se.getClass());
            assertTrue(se.getMessage().contains(INVALID_ARGUMENT_STR));
        }
    }

    @Test
    public void testUnknownArgument() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.unknownArgument("foo", shell.testCommand);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(UNKNOWN_ARGUMENT_STR));
        }
    }

    @Test
    public void testBadArgCount() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.badArgCount(shell.testCommand);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(
                           "Incorrect number of arguments"));
        }
    }

    @Test
    public void testRequiredArg() {
        final TestShell shell = new TestShell(nullInput, captureOutput);

        try {
            shell.requiredArg("foo", shell.testCommand);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(
                           "Missing required argument (foo) for command:"));
        }

        try {
            shell.requiredArg(null, shell.testCommand);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(
                           "Missing required argument for command:"));
        }
    }

    @Test
    public void testMakeWhiteSpace() {
        assertEquals("", Shell.makeWhiteSpace(0));
        assertEquals("   ", Shell.makeWhiteSpace(3));
        assertEquals("    ", Shell.makeWhiteSpace(4));
    }

    @Test
    public void testCheckHelp() {

        final ShellCommand testCommand = new TestCommand();
        final String[] helpArgs = new String[] {
            "-help", "help", "HeLp", "?", "-?" };
        for (String helpArg : helpArgs) {
            final String[] testArgs = new String[] { helpArg };
            try {
                Shell.checkHelp(testArgs, testCommand);
                fail("expected exception for " + helpArg);
            } catch (ShellException se) {
                assertEquals(ShellHelpException.class, se.getClass());
            }
        }

        final String[] nonHelpArgs = new String[] { "foo", "" };
        for (String nonHelpArg : nonHelpArgs) {
            final String[] testArgs = new String[] { nonHelpArg };
            try {
                Shell.checkHelp(testArgs, testCommand);
            } catch (ShellException se) {
                fail("unexpected exception " + se + " for " + nonHelpArg);
            }
        }
    }

    @Test
    public void testCheckArg() {

        final String[] someArgs = new String[] { "A", "b", "3" };
        assertTrue(Shell.checkArg(someArgs, "a"));
        assertTrue(Shell.checkArg(someArgs, "b"));
        assertTrue(Shell.checkArg(someArgs, "3"));
        assertFalse(Shell.checkArg(someArgs, "c"));
        assertFalse(Shell.checkArg(someArgs, null));
    }

    @Test
    public void testGetArg() {

        final String[] someArgs = new String[] { "A", "b", "3" };
        assertEquals("b", Shell.getArg(someArgs, "a"));
        assertEquals("3", Shell.getArg(someArgs, "b"));
        assertTrue(null == Shell.getArg(someArgs, "3"));
        assertTrue(null == Shell.getArg(someArgs, "c"));
    }

    @Test
    public void testMatches() {

        /* 2-arg matches (no prefix) */
        assertTrue(Shell.matches("abc", "abc"));
        assertTrue(Shell.matches("ABC", "abc"));
        assertTrue(Shell.matches("abc", "ABC"));

        assertFalse(Shell.matches("abd", "abc"));
        assertFalse(Shell.matches("dbc", "abc"));
        assertFalse(Shell.matches("ab",  "abc"));
        assertFalse(Shell.matches("abcd", "abc"));

        /* 3-arg matches (with prefix) */
        assertTrue(Shell.matches("abc", "abc", 3));
        assertTrue(Shell.matches("ABC", "abc", 3));
        assertTrue(Shell.matches("abc", "abc", 2));
        assertTrue(Shell.matches("AB",  "abc", 2));

        assertFalse(Shell.matches("abd",  "abc", 3));
        assertFalse(Shell.matches("abcd", "abc", 3));
        assertFalse(Shell.matches("ab",   "abc", 3));
    }

    /* Test HelpCommand */
    @Test
    public void testHelpCommand() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final HelpCommand helpCommand = new HelpCommand();
        final String usageString = "Usage Header";
        final String notFindString = "Could not find command:";

        shell.commands.add(helpCommand);

        /* test matches() */
        assertTrue(helpCommand.matches("?"));
        assertTrue(helpCommand.matches("help"));
        assertFalse(helpCommand.matches("sleep"));

        /* test checkForDeprecatedFlag */

        /* Pass -include-deprecated flag to the help command */
        String[] argsPassed1 = {"help", "-include-deprecated", "plan"};
        final String[] argsReturned1 =
                helpCommand.checkForDeprecatedFlag(argsPassed1, shell);
        final String[] expectedReturn1 = {"help", "plan"};

        assertTrue(Arrays.equals(argsReturned1, expectedReturn1));


        /* showDeprecated flag should be set to true */
        assertTrue(shell.showDeprecated());


        /* Do not pass -include-deprecated flag to the help command */
        String[] argsPassed2 = {"help", "plan"};
        final String[] argsReturned2 =
                helpCommand.checkForDeprecatedFlag(argsPassed2, shell);
        final String[] expectedReturn2 = {"help", "plan"};

        assertTrue(Arrays.equals(argsReturned2, expectedReturn2));


        /* showDeprecated flag should be set to false */
        assertFalse(shell.showDeprecated());

        /* test execute */

        final CommandShell commandShell =
                new CommandShell(nullInput, captureOutput);

        try {
            /* Pass -include-deprecated flag to the CLI help command */
           String[] passArguments1 = {"help", "-include-deprecated", "plan"};
           /*Execute the help command */
           final String execRes1 =
                   helpCommand.execute(passArguments1, commandShell);
           /*
            * -include-deprecated flag used in CLI help. Hence it should show
            * all the deprecated sub commands for the help command.
            * Assert for the presence of deploy-datacenter and remove-datacenter
            * (deprecated commands) in the result.
            */
           assertTrue(execRes1.contains("deploy-datacenter"));
           assertTrue(execRes1.contains("remove-datacenter"));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }

        try {
            /* Do not pass -include-deprecated flag to the CLI help command */
            String[] passArguments2 = {"help", "plan"};
            /*Execute the help command */
            final String execRes2 =
                    helpCommand.execute(passArguments2, commandShell);
            /*
             * -include-deprecated flag not used in CLI help. Hence it should
             * not show any deprecated sub commands for the help command.
             * Assert for the absence of deploy-datacenter and remove-datacenter
             * (deprecated commands) in the result.
             */
            assertFalse(execRes2.contains("deploy-datacenter"));
            assertFalse(execRes2.contains("remove-datacenter"));
         } catch (ShellException se) {
             fail("unexpected exception: " + se);
         }

        try {
            /* missing command to get help for */
            final String execRes1 = helpCommand.execute(
                new String[] { "?" }, shell);
            assertTrue(execRes1.contains(usageString));
            assertFalse(execRes1.contains(notFindString));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }

        try {
            /* command to get help for is not found*/
            final String execRes2 = helpCommand.execute(
                new String[] { "?", "xyz" }, shell);
            assertTrue(execRes2.contains(notFindString));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }

        try {
            /* command to get help for (testing) is found*/
            final String execRes3 = helpCommand.execute(
                new String[] { "?", TestCommand.CMD_NAME }, shell);
            assertTrue(execRes3.contains("Usage:"));
            assertFalse(execRes3.contains(notFindString));
            assertTrue(execRes3.contains(TestCommand.CMD_NAME));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }

        /* test getCommandSyntax */
        final String expGetCmdRes =
                "help [command [sub-command]] [-include-deprecated] " +
                CommandParser.optional(CommandParser.JSON_FLAG);
        final String getCmdRes = helpCommand.getCommandSyntax();
        assertEquals(expGetCmdRes, getCmdRes);

        /* test getCommandDescription */
        final String expGetCmdDescString =
            "Print help messages.  With no arguments";
        final String getCmdDescRes = helpCommand.getCommandDescription();
        assertTrue(getCmdDescRes.contains(expGetCmdDescString));
    }

    /* Test ExitCommand */
    @Test
    public void testExitCommand() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final ExitCommand exitCommand = new ExitCommand();

        shell.commands.add(exitCommand);

        /* test matches() */
        assertTrue(exitCommand.matches("exit"));
        assertTrue(exitCommand.matches("quit"));
        assertFalse(exitCommand.matches("sleep"));

        /* test execute */

        /* missing command to get help for */
        assertFalse(shell.getTerminate());
        try {
            final String execRes = exitCommand.execute(
                new String[] { "exit" }, shell);
            assertTrue(execRes != null);
            assertTrue(shell.getTerminate());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }

        /* test getCommandSyntax */
        final String expGetCmdRes =
             "exit | quit " +
             CommandParser.optional(CommandParser.JSON_FLAG);
        final String getCmdRes = exitCommand.getCommandSyntax();
        assertEquals(expGetCmdRes, getCmdRes);

        /* test getCommandDescription */
        final String expGetCmdDescRes = "Exit the interactive command shell.";
        final String getCmdDescRes = exitCommand.getCommandDescription();
        assertEquals(expGetCmdDescRes, getCmdDescRes);
    }

    /* Test LoadCommand */
    @Test
    public void testLoadCommand() {
        subTestLoadCommandExecuteMissingFile();
        subTestLoadCommandExecuteUnknownArg();
        subTestLoadCommandExecuteInvalidFile();
        subTestLoadCommandExecuteNormal();
        subTestLoadCommandExecuteNormalExit();
        subTestLoadCommandExecuteExceptionRetry();
        subTestLoadCommandExecuteExceptionNoRetry();
        subTestLoadCommandExecuteUnknownException();
        subTestLoadCommandMisc();
        subTestLoadCommandExecuteMultiLinesCommand();
    }

    private void subTestLoadCommandExecuteMissingFile() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        try {
            /* missing -file */
            loadCommand.execute(
                new String[] { "load" }, shell);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(
                           "Missing required argument (-file) for command:"));
        }
    }

    private void subTestLoadCommandExecuteUnknownArg() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile = makeTestFile(new String[0]);

        try {
            /* unknown argument */
            loadCommand.execute(
                new String[] { "load", "-file", testFile, "-foo" }, shell);
            fail("expected exception");
        } catch (ShellException se) {
            assertEquals(ShellUsageException.class, se.getClass());
            assertTrue(se.getMessage().contains(UNKNOWN_ARGUMENT_STR));
        }
    }

    private void subTestLoadCommandExecuteInvalidFile() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        try {
            /* invalid file */
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", "no file for you" }, shell);
            assertTrue(execRes.contains("Failed to load file"));
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestLoadCommandExecuteNormal() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile =
            makeTestFile(new String[] { TestCommand.LINE_NORMAL });

        /* normal execution flow */
        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertEquals("", execRes);
            assertEquals(1, shell.testCommand.getExecuteCount());
            assertFalse(shell.getTerminate());
            assertEquals(1, shell.getHistory().getSize());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestLoadCommandExecuteNormalExit() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);
        shell.commands.add(new ExitCommand());

        final String testFile =
            makeTestFile(new String[] { TestCommand.LINE_NORMAL, "exit" });

        /* normal execution flow, with exit request */
        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertEquals("", execRes);
            assertEquals(1, shell.testCommand.getExecuteCount());
            assertTrue(shell.getTerminate());
            assertEquals(2, shell.getHistory().getSize());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    /*
     * Why does a retry indication in response to a shell exception not
     * cause the command to be retried?
     */
    private void subTestLoadCommandExecuteExceptionRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile =
            makeTestFile(new String[] { TestCommand.LINE_NORMAL,
                                        TestCommand.LINE_SHELL_EXCEPTION,
                                        TestCommand.LINE_NORMAL });

        /* Exception with retry */
        shell.setRetryCount(1);
        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertEquals("", execRes);
            assertEquals(3, shell.testCommand.getExecuteCount());
            /*
             * The load command doesn't actually retry when retry is
             * signaled
             */
            assertEquals(2, shell.getHistory().getSize());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestLoadCommandExecuteExceptionNoRetry() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile =
            makeTestFile(new String[] { TestCommand.LINE_NORMAL,
                                        TestCommand.LINE_SHELL_EXCEPTION,
                                        TestCommand.LINE_NORMAL });

        /* Exception with no retry */
        shell.setRetryCount(0);
        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertTrue(execRes.contains("Script error"));
            assertEquals(2, shell.testCommand.getExecuteCount());
            assertEquals(2, shell.getHistory().getSize());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestLoadCommandExecuteUnknownException() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile =
            makeTestFile(new String[] { TestCommand.LINE_NORMAL,
                                        TestCommand.LINE_UNKNOWN_EXCEPTION,
                                        TestCommand.LINE_NORMAL });

        /* Unknown Exception unconditionally terminates */
        shell.setRetryCount(1);
        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertEquals("", execRes);
            assertEquals(2, shell.testCommand.getExecuteCount());
            assertEquals(2, shell.getHistory().getSize());
            assertEquals(Shell.EXIT_UNKNOWN, shell.getExitCode());
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    private void subTestLoadCommandMisc() {
        final LoadCommand loadCommand = new LoadCommand();

        /* test getCommandSyntax */
        final String expGetCmdRes = "load -file <path to file>";
        final String getCmdRes = loadCommand.getCommandSyntax();
        assertEquals(expGetCmdRes, getCmdRes);

        /* test getCommandDescription */
        final String expGetCmdDescString =
            "Load the named file and interpret its contents as a script ";
        final String getCmdDescRes = loadCommand.getCommandDescription();
        assertTrue(getCmdDescRes.contains(expGetCmdDescString));
    }

    private void subTestLoadCommandExecuteMultiLinesCommand() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final LoadCommand loadCommand = new LoadCommand();
        shell.commands.add(loadCommand);

        final String testFile = makeTestFile(
            new String[] {
                TestCommand.LINE_NORMAL + ";",
                TestCommand.LINE_NORMAL, ";",
                TestCommand.CMD_NAME, TestCommand.ARG_OK + ";",
                TestCommand.CMD_NAME, TestCommand.ARG_OK, ";",
                TestCommand.CMD_NAME, TestCommand.ARG_OK, "",
                TestCommand.CMD_NAME + " \\",
                    TestCommand.ARG_OK + " \\","-verbose",
                TestCommand.CMD_NAME + " \\", TestCommand.ARG_OK + ";",
                TestCommand.CMD_NAME,
                    TestCommand.ARG_OK + "; " + TestCommand.LINE_NORMAL, "", ";",
                TestCommand.LINE_NORMAL + "; " + TestCommand.LINE_VERBOSE, ";"
            });

        final String[] expCmdsExecuted = new String[] {
            TestCommand.LINE_NORMAL,
            TestCommand.LINE_NORMAL,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + " " + TestCommand.ARG_OK + " -verbose" ,
            TestCommand.CMD_NAME + " " + TestCommand.ARG_OK,
            TestCommand.CMD_NAME + "\n" + TestCommand.ARG_OK,
            TestCommand.LINE_NORMAL,
            TestCommand.LINE_NORMAL,
            TestCommand.LINE_VERBOSE
        };

        try {
            final String execRes = loadCommand.execute(
                new String[] { "load", "-file", testFile }, shell);
            assertEquals("", execRes);
            assertEquals(15, shell.testCommand.getExecuteCount());
            final CommandHistory history = shell.getHistory();
            assertEquals(expCmdsExecuted.length, history.getSize());
            for (int i = 0; i < expCmdsExecuted.length; i++) {
                assertEquals(expCmdsExecuted[(expCmdsExecuted.length - i - 1)],
                    history.get(history.getSize() - i - 1).getCommand());
            }
        } catch (ShellException se) {
            fail("unexpected exception: " + se);
        }
    }

    /* Test CommandHistory */
    @Test
    public void testCommandHistory() {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final CommandHistory history = shell.getHistory();
        final String addCmd1 = "testing 123";
        final String addCmd2 = "testing 345";
        final String addCmd3 = "testing 678";
        final String thisMethod = "testCommandHistory";

        /* initial checks on empty history */
        assertTrue(history.getLastException() == null);
        assertEquals("", history.dumpLastFault());

        /* Add a command with no exception */
        history.add(addCmd1, null);

        /* checks on empty with no exceptions present */
        assertTrue(history.getLastException() == null);
        assertEquals("", history.dumpLastFault());

        /* Add a command with an exception */
        history.add(addCmd2, new ShellException("oops"));

        /* test getSize() */
        assertEquals(2, history.getSize());

        /* test get() */
        final CommandHistoryElement cmd1 = history.get(0);
        assertEquals(addCmd1, cmd1.getCommand());
        assertTrue(cmd1.getException() == null);

        final CommandHistoryElement cmd2 = history.get(1);
        assertEquals(addCmd2, cmd2.getCommand());
        assertTrue(cmd2.getException() != null);

        final CommandHistoryElement cmd3 = history.get(2);
        assertTrue(cmd3 == null);
        assertTrue(baos.toString().contains("No such command in history"));

        /* test dump() */
        final String dumpRes1 = history.dump(0, 0);
        assertTrue(dumpRes1.contains(addCmd1));
        assertFalse(dumpRes1.contains(addCmd2));

        final String dumpRes2 = history.dump(0, 1);
        assertTrue(dumpRes2.contains(addCmd1));
        assertTrue(dumpRes2.contains(addCmd2));

        /* test commandFaulted */
        assertFalse(history.commandFaulted(0));
        assertTrue(history.commandFaulted(1));

        /* test dumpCommand */
        final String dumpCmdRes1 = history.dumpCommand(0, false);
        assertTrue(dumpCmdRes1.contains(addCmd1));
        assertFalse(dumpCmdRes1.contains(thisMethod));

        final String dumpCmdRes2 = history.dumpCommand(0, true);
        assertTrue(dumpCmdRes2.contains(addCmd1));
        assertFalse(dumpCmdRes2.contains(thisMethod));

        final String dumpCmdRes3 = history.dumpCommand(1, false);
        assertTrue(dumpCmdRes3.contains(addCmd2));
        assertFalse(dumpCmdRes3.contains(thisMethod));

        final String dumpCmdRes4 = history.dumpCommand(1, true);
        assertTrue(dumpCmdRes4.contains(addCmd2));
        assertTrue(dumpCmdRes4.contains(thisMethod));

        /* test dumpFaultingCommands */
        final String dumpFltRes = history.dumpFaultingCommands(0, 1);
        assertFalse(dumpFltRes.contains(addCmd1));
        assertTrue(dumpFltRes.contains(addCmd2));

        /* Add a command with no exception to improve code coverage */
        history.add(addCmd3, null);

        /* test getLastException */
        final Exception lfe = history.getLastException();
        assertEquals(lfe, cmd2.getException());

        /* test dumpLastFault */
        final String lastFlt = history.dumpLastFault();
        assertTrue(lastFlt.contains(addCmd2));
    }

    /* Test CommandComparator */

    /* TBD: what is the purpose of this? */
    @Test
    public void testCommandComparator() {
        final TestCommand cmd1 = new TestCommand();
        final TestCommand cmd2 = new TestCommand();
        final TestCommand cmd3 = new TestCommand();
        cmd3.setCommandName("aaa");
        final Shell.CommandComparator comparator =
            new Shell.CommandComparator();

        assertEquals(0, comparator.compare(cmd1, cmd2));
        assertEquals(0, comparator.compare(cmd2, cmd1));
        assertTrue(comparator.compare(cmd1, cmd3) > 0);
        assertTrue(comparator.compare(cmd3, cmd1) < 0);
    }

    /* Tests if the -json flag can be parsed correctly */
    @Test
    public void testJsonFlagParsing() throws Exception {
        final TestShell shell = new TestShell(nullInput, captureOutput);
        final String cmdLine =
            TestJsonFlagCommand.DESC + " " + CommandParser.JSON_FLAG;

        final TestJsonFlagCommand jsonCommand =
            new TestJsonFlagCommand(true);

        shell.commands.add(jsonCommand);
        shell.execute(cmdLine);

        jsonCommand.setOverrideJsonFlag(false);
        shell.execute(cmdLine);
    }

    @Test
    public void testCommandHistoryMaskInformation() {
        final String[] maskFlags = new String[] {
            "-secret", "-password", "identified by"
        };

        final String[] cmds = new String[] {
            "pwdfile secret -set -alias user2 -secret a",
            "pwdfile secret -set -alias user2 -secret \' \';",
            "pwdfile secret -set -alias abcde -secret abcde",
            "pwdfile secret -set -alias user2 -secret ' abcde abcde '",
            "pwdfile secret -set -alias user2 -secret \" abcde abcde \";",
            "pwdfile secret -set -alias user2 -secret \"abcde;",
            "pwdfile secret -set -alias user2 -secret \'abcde",
            "pwdfile secret -set -alias user2 -secret \"\"",
            "pwdfile secret -set -alias user2 -secret",
            "pwdfile secret -set -alias user2 -secret ABCDE -password abcde;",
            "pwdfile secret -set -alias user2 -SECRET abcde",

            "plan change-user -name test -set-password -password test",

            "exec \"create user user1 identified by abcde \";",
            "exec \"create user user1 IDENTIFIEd by 'abcde ABCDE' ADMIN\"",
            "exec \"alter user user1 identified by \"\"",
            "exec \"alter user user1 IdentifieD by \'abc\';",

            "create user user1 identified by abcde;",
            "create user user1 IDENTIFIED by \"abc abcde\";"
        };

        final String[] cmdsInHistory = new String[] {
            "pwdfile secret -set -alias user2 -secret *",
            "pwdfile secret -set -alias user2 -secret \'*\';",
            "pwdfile secret -set -alias abcde -secret *****",
            "pwdfile secret -set -alias user2 -secret '*************'",
            "pwdfile secret -set -alias user2 -secret \"*************\";",
            "pwdfile secret -set -alias user2 -secret \"*****;",
            "pwdfile secret -set -alias user2 -secret \'*****",
            "pwdfile secret -set -alias user2 -secret \"\"",
            "pwdfile secret -set -alias user2 -secret",
            "pwdfile secret -set -alias user2 -secret ***** -password *****;",
            "pwdfile secret -set -alias user2 -SECRET abcde",

            "plan change-user -name test -set-password -password ****",

            "exec \"create user user1 identified by ***** \";",
            "exec \"create user user1 IDENTIFIEd by '***********' ADMIN\"",
            "exec \"alter user user1 identified by \"\"",
            "exec \"alter user user1 IdentifieD by \'***\';",

            "create user user1 identified by *****;",
            "create user user1 IDENTIFIED by \"*********\";"
        };

        final TestShell shell =
                new TestShell(nullInput, captureOutput, maskFlags);

        for (String cmd : cmds) {
            shell.execute(cmd);
        }

        final CommandHistory history = shell.getHistory();
        assertTrue(history.getSize() == cmdsInHistory.length);
        for (int i = 0; i < history.getSize(); i++) {
            CommandHistoryElement elem = history.get(i);
            assertTrue(elem.getCommand().equals(cmdsInHistory[i]));
        }
    }

    /*
     * Utility methods and classes
     */

    private int countOf(String arg, String[] list) {
        int count = 0;
        for (String s : list) {
            if (arg == s || (arg != null && arg.equals(s))) {
                count++;
            }
        }
        return count;
    }

    private CommandHistoryElement lastCommandEntry(Shell shell) {
        final CommandHistory history = shell.getHistory();
        if (0 == history.getSize()) {
            return null;
        }
        return history.get(history.getSize() - 1);
    }

    private InputStream makeInputStream(String[] lines) {
        final ByteArrayOutputStream myBaos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(myBaos);
        for (String line : lines) {
            ps.println(line);
        }
        ps.flush();
        return new TestInputStream(
            new ByteArrayInputStream(myBaos.toByteArray()));
    }

    /*
     * Create a temporary file containing the input lines.
     * @return the name of the file.
     */
    private String makeTestFile(String[] lines) {
        final File testDir = TestUtils.getTestDir();
        final File testFile = new File(testDir.getPath(), "ShellTest.input");
        try {
            final PrintWriter writer = new PrintWriter(testFile);
            for (String line : lines) {
                writer.println(line);
            }
            writer.close();
        } catch (IOException ioe) {
            fail("error writing test file " + testFile.getPath());
        }
        return testFile.getPath();
    }

    class TestInputStream extends InputStream {
        private final InputStream subStream;
        private boolean ioeThrown = false;

        TestInputStream(InputStream input) {
            this.subStream = input;
        }

        @Override
        public int read()
            throws IOException {
            if (!ioeThrown) {
                ioeThrown = true;
                throw new IOException("Injected IOE");
            }
            return subStream.read();
        }
    }

    class TestCommand extends ShellCommand {

        private static final String CMD_DESCR = "A Test Command";
        private static final String CMD_NAME = "testing";

        private static final String ARG_OK = "OK";
        private static final String LINE_NORMAL = CMD_NAME + " " + ARG_OK;
        private static final String LINE_VERBOSE =
            CMD_NAME + " " + ARG_OK + " -verbose";

        private static final String ARG_SE = "ShellException";
        private static final String LINE_SHELL_EXCEPTION =
            CMD_NAME + " " + ARG_SE;

        private static final String ARG_UE = "UnknownException";
        private static final String LINE_UNKNOWN_EXCEPTION =
            CMD_NAME + " " + ARG_UE;

        private static final String RETURN_SUCCESS = "Success";

        private boolean hidden = false;
        private boolean overrideHidden = false;

        private String commandName = null;
        private boolean overrideCommandName = false;

        private int executeCount = 0;

        private TestCommand() {
            super(CMD_NAME, CMD_NAME.length());
        }

        /*
         * These must be implemented by specific shell command classes
         */

        @Override
        protected String getCommandDescription() {
            return CMD_DESCR;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            executeCount++;
            Shell.checkHelp(args, this);

            if (args.length < 2) {
                shell.badArgCount(this);
            }

            final String arg = args[1];
            if (ARG_SE.equals(arg)) {
                throw new ShellException(
                    "Arg: " + ARG_SE + " forces shell exception");
            }
            if (ARG_UE.equals(arg)) {
                throw new IllegalArgumentException(
                    "Arg: " + ARG_UE + " forces unknown exception");
            }
            if (ARG_OK.equals(arg)) {
                return RETURN_SUCCESS;
            }
            shell.unknownArgument(arg, this);

            /* Not reached */
            return "";
        }

        /* Overrides for control */

        @Override
        protected boolean isHidden() {
            return overrideHidden ? hidden : super.isHidden();
        }

        @Override
        public String getCommandName() {
            return overrideCommandName ? commandName : super.getCommandName();
        }

        /* internal accessors */

        private void setHidden(boolean hidden) {
            this.hidden = hidden;
            this.overrideHidden = true;
        }

        private void setCommandName(String name) {
            this.commandName = name;
            this.overrideCommandName = true;
        }

        private int getExecuteCount() {
            return executeCount;
        }
    }

    class TestShell extends Shell {
        private static final String DEF_PROMPT = "Test Prompt";
        private static final String DEF_USAGE_HEADER = "Usage Header";

        private String prompt = DEF_PROMPT;
        private final String usageHeader = DEF_USAGE_HEADER;

        private final List<ShellCommand> commands =
                               new ArrayList<ShellCommand>();
        private final TestCommand testCommand = new TestCommand();
        private boolean initOccurred = false;
        private boolean shutdownOccurred = false;

        /* If true, doRetry() delegate to super.  Otherwise, use retryCount */
        private boolean retryFromSuper = true;

        private static final int RETRY_UNLIMITED = -1;
        private static final int RETRY_NONE = 0;
        private int retryCount = RETRY_NONE;

        private TestShell(InputStream input, PrintStream output) {
            super(input, output);
            commands.add(testCommand);
        }

        private TestShell(InputStream input,
                          PrintStream output,
                          String[] maskFlags) {
            super(input, output, true, maskFlags);
            commands.add(testCommand);
        }

        /*
         * These must be implemented by specific shell classes
         */
        @Override
        public List<? extends ShellCommand> getCommands() {
            return commands;
        }

        @Override
        public String getPrompt() {
            return prompt;
        }

        @Override
        public String getUsageHeader() {
            return usageHeader;
        }

        @Override
        public void init() {
            initOccurred = true;
        }

        @Override
        public void shutdown() {
            shutdownOccurred = true;
        }

        /* Overrides for control */

        @Override
        public boolean doRetry() {
            if (retryFromSuper) {
                return super.doRetry();
            }
            if (retryCount > 0) {
                retryCount -= 1;
                return true;
            }

            return retryCount < 0;
        }

        /* internal accessors */

        private void setRetryFromSuper() {
            retryFromSuper = true;
        }

        private void setRetry(boolean retry) {
            retryFromSuper = false;
            retryCount = retry ? RETRY_UNLIMITED : RETRY_NONE;
        }

        private void setRetryCount(int retryCount) {
            retryFromSuper = false;
            this.retryCount = retryCount;
        }

        private void setPrompt(String prompt) {
            this.prompt = prompt;
        }

        @SuppressWarnings("unused")
        private boolean getInitOccurred() {
            return initOccurred;
        }

        private boolean getShutdownOccurred() {
            return shutdownOccurred;
        }
    }

    class TestJsonFlagCommand extends ShellCommand {
        public static final String DESC = "TestOverrideJson";

        TestJsonFlagCommand(boolean overrideJsonFlag) {
            super(DESC, DESC.length());
            this.overrideJsonFlag = overrideJsonFlag;
        }

        @Override
        protected String getCommandDescription() {
            return DESC;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            /*
             * If the overrideJsonFlag is true but no -json flag in argument,
             * or the overrideJsonFlag is false but the -json flag appears in
             * argument (not extracted by Shell), test fails.
             */
            if (overrideJsonFlag !=
                    Shell.checkArg(args, CommandParser.JSON_FLAG)) {
                fail((overrideJsonFlag ? "Expected" : "Unexpected") +
                     " -json flag in arguments");
            }
            return "";
        }

        public void setOverrideJsonFlag(boolean json) {
            overrideJsonFlag = json;
        }
    }
}
