/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Collections.emptyList;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.logging.LogRecord;

import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellInputReader;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.Test;

/**
 * Verifies the functionality and error paths of the LogtailCommand class.
 * Note that although the CLI test framework verifies many of the same aspects
 * of LogtailCommand as this unit test, the tests from the CLI test framework
 * do not contribute to the unit test coverage measure that is automatically
 * computed nightly. Thus, the intent of this test class is to provide
 * additional unit test coverage for the LogtailCommand class that will be
 * automatically measured nightly.
 */
public class LogtailCommandTest extends TestBase {

    private final String command = "logtail";
    private final ParameterMap pMap = new ParameterMap();
    private final CommandShell shell = createMock(CommandShell.class);
    private final CommandService cs = createMock(CommandService.class);

    private final AdminParams adminParams = new AdminParams(pMap);

    private static final int ADMIN_ID = 99;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        adminParams.setAdminId(new AdminId(ADMIN_ID));
    }

    /* Convenience method shared by all the test cases that employ the
     * mocked objects.
     */
    private void doVerification() {

        verify(cs);
        verify(shell);
    }

    @Test
    public void testLogtailCommandGetDescription() throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();
        final String expectedResult =
            "Monitors the store-wide log file until interrupted by an " +
            "\"enter\"" + Shell.eolt + "keypress.";
        assertEquals(expectedResult, logtailObj.getCommandDescription());
    }

    @Test
    public void testLogtailCommandExecuteBadArgCountTooFew() throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + command,
                logtailObj);
        final String[] args = { };

        /* Don't need a mocked shell for this test case. */
        final CommandShell tmpShell = new CommandShell(System.in, System.out);

        try {
            logtailObj.execute(args, tmpShell);
            fail("ShellUsageException expected, but correct number of args " +
                 "specified [expected less than " + args.length + " args]");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testLogtailCommandExecuteBadArgCountTooMany()
        throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();
        final String arg = "ARG_ONE";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + command,
                logtailObj);
        final String[] args = {command, arg};

        /* Don't need a mocked shell for this test case. */
        final CommandShell tmpShell = new CommandShell(System.in, System.out);

        try {
            logtailObj.execute(args, tmpShell);
            fail("ShellUsageException expected, but correct number of args " +
                 "specified [expected less than " + args.length + " args]");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testLogtailCommandExecuteIOException()
        throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();

        final String[] args = {command};

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        /**
         * Inject an IOException which is expected to be caught when
         * shell.getInput().readLine() is invoked in LogtailCommand.execute()
         */
        final InputStream in = new InputStream() {

            @Override
            public int read() throws IOException
            {
                throw new IOException("Injected I/O exception!");
            }
        };
        final ShellInputReader shellRdr = ShellInputReader.getReader(in, cmdOut);

        final String expectedResult = "Exception reading input during logtail";

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getLogSince(0L, null, SerialVersion.CURRENT))
            .andReturn(new RetrievedEvents<LogRecord>(
                           0, emptyList()))
            .times(0, 1);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getOutput()).andReturn(cmdOut);
        expect(shell.getInput()).andReturn(shellRdr);
        replay(shell);

        try {
            final String result = logtailObj.execute(args, shell);
            /* No exception if getLogSince was not called */
            if (!result.isEmpty()) {
                assertEquals(expectedResult, result);
            }
        } finally {
            doVerification();
        }
    }

    @Test
    public void testLogtailCommandExecuteReturnEmpty()
        throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();

        final String[] args = {command};

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);

        final byte[] cr = new byte[] {'\r'};
        final InputStream inStream = new ByteArrayInputStream(cr);
        final ShellInputReader shellRdr =
            ShellInputReader.getReader(inStream, cmdOut);

        final String expectedResult = "";

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getLogSince(0L, null, SerialVersion.CURRENT))
            .andReturn(new RetrievedEvents<LogRecord>(
                           0, emptyList()))
            .times(0, 1);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getOutput()).andReturn(cmdOut);
        expect(shell.getInput()).andReturn(shellRdr);
        replay(shell);

        try {
            final String result = logtailObj.execute(args, shell);
            /* No exception if getLogSince was not called */
            if (!result.isEmpty()) {
                assertEquals(expectedResult, result);
            }
        } finally {
           doVerification();
        }
    }

    @Test
    public void testLogtailCommandExecuteRemoteException() throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();
        final String[] args = {command};

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);

        final byte[] cr = new byte[] {'\r'};
        final InputStream inStream = new ByteArrayInputStream(cr);
        final ShellInputReader shellRdr =
            ShellInputReader.getReader(inStream, cmdOut);

        final RemoteException expectedRemoteException =
            new RemoteException("RemoteException on call to getLogSince");
        final String expectedResult = "Exception from logtail: " +
            expectedRemoteException.getMessage();

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getLogSince(0L, null, SerialVersion.CURRENT))
            .andThrow(expectedRemoteException)
            .times(0, 1);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getOutput()).andReturn(cmdOut);
        expect(shell.getInput()).andReturn(shellRdr);
        replay(shell);

        try {
            final String result = logtailObj.execute(args, shell);
            /* No exception if getLogSince was not called */
            if (!result.isEmpty()) {
                assertEquals(expectedResult, result);
            }
        } finally {
            doVerification();
        }
    }

    @Test
    public void testLogtailCommandExecuteUnauthorizedException()
        throws Exception {

        final LogtailCommand logtailObj = new LogtailCommand();
        final String[] args = {command};

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);

        final byte[] cr = new byte[] {'\r'};
        final InputStream inStream = new ByteArrayInputStream(cr);
        final ShellInputReader shellRdr =
            ShellInputReader.getReader(inStream, cmdOut);

        final UnauthorizedException expectedUnauthorizedException =
            new UnauthorizedException("UnauthorizedException on call to " +
                                      "getLogSince");

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getLogSince(0L, null, SerialVersion.CURRENT))
            .andThrow(expectedUnauthorizedException)
            .times(0, 1);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getOutput()).andReturn(cmdOut);
        expect(shell.getInput()).andReturn(shellRdr);
        replay(shell);

        try {
            logtailObj.execute(args, shell);
        } finally {
            doVerification();
        }
    }
}
