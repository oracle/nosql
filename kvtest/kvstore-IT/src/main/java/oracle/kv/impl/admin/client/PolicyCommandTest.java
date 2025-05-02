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
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.Test;

/**
 * Verifies the functionality and error paths of the PolicyCommand class. Note
 * that although the CLI test framework verifies many of the same aspects of
 * PolicyCommand as this unit test, the tests from the CLI test framework do
 * not contribute to the unit test coverage measure that is automatically
 * computed nightly. Thus, the intent of this test class is to provide
 * additional unit test coverage for the PolicyCommand class that will be
 * automatically measured nightly.
 */
public class PolicyCommandTest extends TestBase {

    private final PolicyCommand policyObj = new PolicyCommand();
    private final String command = "change-policy";
    private final String paramsFlag = "-params";

    private final ParameterMap pMap = new ParameterMap();
    private final CommandShell shell = createMock(CommandShell.class);
    private final CommandService cs = createMock(CommandService.class);
    private final AdminParams adminParams = new AdminParams(pMap);

    private static final int ADMIN_ID = 99;
    private static final AuthContext NULL_AUTH = null;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        adminParams.setAdminId(new AdminId(ADMIN_ID));
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /* Convenience method shared by a number of the test cases (not all)
     * that employ the mocked objects. This method executes a number of
     * methods from the mock framework that establish what is expected
     * from each mock by the test case that invokes this method.
     */
    private void setMockExpectations() throws Exception {

        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getPolicyParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(pMap);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(Boolean.FALSE);
        replay(shell);
    }

    /* Convenience method shared by all the test cases  that employ the
     * mocked objects.
     */
    private void doVerification() {

        verify(cs);
        verify(shell);
    }

    @Test
    public void testPolicyCommandGetCommandSyntax() throws Exception {

        final TestPolicyCommand testPolicyObj = new TestPolicyCommand();
        final String expectedResult =
            testPolicyObj.getName() + " " +
            CommandParser.getJsonUsage() + " " +
            "[-dry-run] -params [name=value]*";
        assertEquals(expectedResult, testPolicyObj.getCommandSyntax());
    }

    @Test
    public void testPolicyCommandGetCommandDescription() throws Exception {

        final String expectedResult =
            "Modifies store-wide policy parameters that apply to not yet " +
            "deployed" + Shell.eolt + "services. The parameters to change " +
            "follow the -params flag and are" + Shell.eolt + "separated by " +
            "spaces. Parameter values with embedded spaces must be" +
            Shell.eolt + "quoted.  For example name=\"value with spaces\". " +
            "If -dry-run is" + Shell.eolt + "specified the new parameters " +
            "are returned without changing them.";
        assertEquals(expectedResult, policyObj.getCommandDescription());
    }

    @Test
    public void testPolicyCommandExecuteBadArgLength() throws Exception {

        final String arg0 = "UNKNOWN_ARG_0";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + command,
                policyObj);
        final String[] args = {command, arg0};
        final CommandShell tmpShell = new CommandShell(System.in, System.out);

        try {
            policyObj.execute(args, tmpShell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    /**
     * The following test cases provide test overage for the various code paths
     * in the execute method of the PolicyCommand class.
     *
     * Because the execute method takes a Shell parameter, the object input for
     * that parameter in each of these test cases is a mock of the CommandShell
     * class. Additionally, because the execute method retrieves the admin
     * service from that CommandShell, resulting in a call to
     * getSerialVersion, a mock of the CommandService class is also employed
     * in each of these test cases.
     *
     * Once the necessary mocked objects are created, the methods from those
     * mocks that will be invoked when exercising the code path under test are
     * declared to the mock framework, along with the expected result of each
     * call. After the expectations declared to the mock framework are
     * replayed, the execute method is then invoked with the given
     * test-specific arguments and the results of that invocation are verified.
     */
    @Test
    public void testPolicyCommandExecuteUnknownArg() throws Exception {

        final String arg0 = "UNKNOWN_ARG_0";
        final String arg1 = "UNKNOWN_ARG_1";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg0, policyObj);
        final String[] args = {command, arg0, arg1};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));

        /* Note that because expect() cannot take a void method, the
         * 'expectLastCall idiom' (call the method, followed by a call to
         * expectLastCall and the expected result) is used below to
         * establish the expectations from the call to unknownArgument.
         */
        shell.unknownArgument(arg0, policyObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testPolicyCommandExecuteParamsMissingArg() throws Exception {

        final String arg0 = "-dry-run";
        final ShellUsageException expectedException =
            new ShellUsageException("Incorrect number of arguments for command: " +
                                    command, policyObj);
        final String[] args = {command, arg0};

        /* Establish what is expected from each mock for this test */
        replay(cs);

        /* Note that because expect() cannot take a void method, the
         * 'expectLastCall idiom' (call the method, followed by a call to
         * expectLastCall and the expected result) is used below to
         * establish the expectations from the call to requiredArg.
         */
        shell.badArgCount(policyObj);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testPolicyCommandExecuteParamsNotSpecified() throws Exception {

        final String arg = "-dry-run";
        final String expectedResult = "No parameters were specified";
        /* Place the -params flag last */
        final String[] args = {command, arg, paramsFlag};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellArgumentException e) {
            assertEquals(expectedResult, e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testPolicyCommandExecuteParamsDryRun() throws Exception {

        final String arg = "-dry-run";

        final String name = ParameterState.AP_LOG_FILE_COUNT;
        final String value = "30";
        final String[] args = {command, arg, paramsFlag, name + "=" + value};
        final String expectedResult =
            "adminId=" + ADMIN_ID + Shell.eol +
            "adminLogFileCount=" + value + Shell.eol;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getPolicyParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(pMap);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(Boolean.FALSE);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = policyObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
    }

    /**
     * Verifies that when a call to the setPolicies method on the admin
     * encounters a RemoteException, a call to CommandShell.noAdmin is
     * made, producing the expected ShellException.
     */
    @Test
    public void testPolicyCommandExecuteRemoteException() throws Exception {

        final String name = ParameterState.AP_LOG_FILE_COUNT;
        final String value = "30";
        final String[] args = {command, paramsFlag, name + "=" + value};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        expect(cs.getPolicyParameters(NULL_AUTH, SerialVersion.CURRENT))
            .andReturn(pMap);
        /* Void method: setPolicies */
        cs.setPolicies(pMap, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getHidden()).andReturn(Boolean.TRUE);
        expect(shell.getVerbose()).andReturn(Boolean.FALSE);
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * Verifies that when a call to the setPolicies method on the admin
     * returns successfully, the execute method successfully completes
     * its processing and returns an empty string.
     */
    @Test
    public void testPolicyCommandExecuteReturnEmpty() throws Exception {

        final String name = ParameterState.AP_LOG_FILE_COUNT;
        final String value = "30";
        final String[] args = {command, paramsFlag, name + "=" + value};
        final String verboseStr = "New policy parameters:" +
            Shell.eol +  "adminId=" + ADMIN_ID + Shell.eol +
            "adminLogFileCount=" + value + Shell.eol;

        final String expectedResult = "";

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        expect(cs.getPolicyParameters(NULL_AUTH, SerialVersion.CURRENT)).
            andReturn(pMap).anyTimes();
        /* Void method: setPolicies */
        cs.setPolicies(pMap, NULL_AUTH, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).
            andReturn(CommandServiceAPI.wrap(cs, null)).anyTimes();
        expect(shell.getHidden()).
            andReturn(Boolean.TRUE).anyTimes();
        expect(shell.getVerbose()).
            andReturn(Boolean.TRUE).anyTimes();
        /* Void method: verboseOutput */
        shell.verboseOutput(verboseStr);
        expectLastCall().anyTimes();
        replay(shell);

        /* Run the test and verify the results. */
        try {
            final String result = policyObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        final ShellCommandResult scr =
            policyObj.executeJsonOutput(args, shell);
        assertEquals(scr.getOperation(), "change policy");
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5000.getValue());
        assertEquals(scr.getDescription(),
                     "Operation ends successfully");
    }

    /* The following test cases cover the possible invalid name=value
     * combinations that can be associated with the -params arg, and the
     * corresponding error paths that result. These test cases not only
     * provide coverage for the execute method of PolicyCommand, but also
     * many of the code paths in the CommandUtils.assignParam method.
     */

    /**
     * For path '-params UNKNOWN_NAME=VALUE'.
     */
    @Test
    public void testPolicyCommandExecuteParamUnknownName()
        throws Exception {

        final String name = "UNKNOWN_NAME";
        final String value = "VALUE";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Unknown parameter field: " + name, policyObj);
        final String[] args = {command, paramsFlag, name + "=" + value};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * For path '-params adminLogFileCount=INVALID_VALUE'.
     */
    @Test
    public void testPolicyCommandExecuteParamInvalidValue() throws Exception {

        final String name = ParameterState.AP_LOG_FILE_COUNT;
        final String value = "INVALID_VALUE";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Illegal parameter value:" + Shell.eolt +
                "For input string: " + "\"" + value + "\"", policyObj);
        final String[] args = {command, paramsFlag, name + "=" + value};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * For path '-params adminId=value'.
     */
    @Test
    public void testPolicyCommandExecuteParamInvalidName() throws Exception {

        final String name = ParameterState.AP_ID;
        final String value = "VALUE";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Parameter is not valid for the service: " + name, policyObj);
        final String[] args = {command, paramsFlag, name + "=" + value};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /* The following test cases cover the possible invalid arg values that
     * can be associated with the -params arg, and the corresponding
     * error paths that result. These test cases not only provide
     * coverage for the execute method of PolicyCommand, but also many
     * of the code paths in the CommandUtils.parseParams method.
     */

    /**
     * For path '-params -FLAG_ARG'.
     */
    @Test
    public void testPolicyCommandExecuteParamsNoFlags() throws Exception {

        final String arg = "-FLAG_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "No flags are permitted after the -params flag", policyObj);
        final String[] args = {command, paramsFlag, arg};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * For path '-params UNKNOWN_ARG'.
     */
    @Test
    public void testPolicyCommandExecuteParamsUnknown() throws Exception {

        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Unable to parse parameter assignment: " + arg, policyObj);
        final String[] args = {command, paramsFlag, arg};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /**
     * For path '-params name='.
     */
    @Test
    public void testPolicyCommandExecuteParamsNameEquals() throws Exception {

        final String arg = "name=";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Parameters require a value after =", policyObj);
        final String[] args = {command, paramsFlag, arg};

        setMockExpectations();

        /* Run the test and verify the results. */
        try {
            policyObj.execute(args, shell);
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    /* Sub-class to gain access to protected method(s). */
    private static class TestPolicyCommand extends PolicyCommand {
        String getName() {
            return name;
        }
    }
}
