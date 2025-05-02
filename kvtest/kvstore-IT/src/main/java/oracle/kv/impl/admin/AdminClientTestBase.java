/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.logging.Level.INFO;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.test.TestStatus;

/**
 * Simple base class for Admin client test classes.
 */
public class AdminClientTestBase extends AdminTestBase {

    String adminPort;

    protected AdminClientTestBase(boolean secured) {
        super(secured);
    }

    @Override
    public void setUp()
        throws Exception {
        super.setUp();

        TestStatus.setManyRNs(true);
        setAdminPort(atc.getParams().getStorageNodeParams().getRegistryPort());
    }

    private void setAdminPort(int port) {
        adminPort = Integer.toString(port);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    String[] cliPrefixArgs() {
        return new String[] { "-host", "localhost", "-port", adminPort };
    }

    /**
     * Call CommandShell.main, and return any output it prints as a String.
     * @param args The command line arguments
     * @return The command's output
     * @throws Exception
     */
    String runCLICommand(String... args)
        throws Exception {

        logger.log(INFO, "runCLICommand {0}", Arrays.toString(args));
        try {
            String prefixArgs[] = cliPrefixArgs();

            /* Concatenate the prefix and given arg arrays. */
            String fullArgs[] = new String[prefixArgs.length + args.length];
            System.arraycopy(prefixArgs, 0, fullArgs, 0,
                             prefixArgs.length);
            System.arraycopy(args, 0, fullArgs, prefixArgs.length,
                             args.length);

            /* Arrange to capture stdout. */
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            PrintStream cmdOut = new PrintStream(outStream);
            /* Run the command. */
            CommandShell shell = new CommandShell(System.in, cmdOut);
            shell.parseArgs(fullArgs);
            shell.start();
            /*
             * During the execution of the plan, the Admin master may change
             * and the command shell will track it. So update the port, just
             * in case.
             */
            setAdminPort(shell.getAdminPort());

            logger.log(INFO, "runCLICommand result: {0}", outStream);

            return outStream.toString();

        } catch (Exception | Error e) {
            logger.log(INFO, "runCLICommand throws", e);
            throw e;
        }
    }

    /**
     * Run a CLI command and assert that the results include the expected
     * substring.  The command is divided into space-separated tokens, so don't
     * use this method if command arguments contain spaces.
     */
    void assertCLICommandResult(final String command,
                                final String expectedSubstring)
       throws Exception {

        assertCLICommandResult(command, command, expectedSubstring);
    }

    /**
     * Run a CLI command and use a named assertion to check that the results
     * include the expected substring.  The command is divided into
     * space-separated tokens, so don't use this method if command arguments
     * contain spaces.
     */
    void assertCLICommandResult(final String testName1,
                                final String command,
                                final String expectedSubstring)
        throws Exception {

        assertCLICommandResult(testName1,
                command.split(" "),
                expectedSubstring);
    }

    /**
     * Run a CLI command and use a named assertion to check that the results
     * include the expected substring, specifying the command as an array for
     * use with arguments that contain spaces.
     */
    void assertCLICommandResult(final String testName1,
                                final String[] command,
                                final String expectedSubstring)
        throws Exception {

        /*
         * Include the port used to contact the admin in the error message in
         * order to help debug AdminClientTest failures. We could remove this
         * information after that debugging is done, or perhaps we could leave
         * it if it turns out to be helpful. [KVSTORE-1415]
         */
        final String usingAdminPort = adminPort;
        final String result = runCLICommand(command);
        assertThat(testName1 + " (using admin port " +
                   usingAdminPort + ")",
                   result, containsString(expectedSubstring));
        logger.info(testName1 + ":\n" + result);
    }

    void executeCLICommandResult(final String testName1,
                                 final String command,
                                 final String expectedSubstring)
        throws Exception {

        assertCLICommandResult(
                testName1,
                new String[] { "-store", kvstoreName, "execute", command },
                expectedSubstring);
    }
}