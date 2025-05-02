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
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;


import org.junit.Test;

/**
 * Verifies the functionality and error paths of the ConfigureCommand class.
 * Note that although the CLI test framework verifies many of the same aspects
 * of ConfigureCommand as this unit test, the tests from the CLI test framework
 * do not contribute to the unit test coverage measure that is automatically
 * computed nightly. Thus, the intent of this test class is to provide
 * additional unit test coverage for the ConfigureCommand class that will be
 * automatically measured nightly.
 */
public class ConfigureCommandTest extends TestBase {

    private final String command = "configure";
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

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /* Convenience method shared by all the test cases that employ the
     * mocked objects.
     */
    private void doVerification() {

        verify(cs);
        verify(shell);
    }

    @Test
    public void testConfigureCommandGetSyntax() throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String expectedResult =
            "configure -name <storename> " +
            CommandParser.getJsonUsage();
        assertEquals(expectedResult, configureObj.getCommandSyntax());
    }

    @Test
    public void testConfigureCommandGetDescription() throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String expectedResult =
            "Configures a new store.  This call must be made before " +
            "any other" + Shell.eolt + "administration can be performed.";
        assertEquals(expectedResult, configureObj.getCommandDescription());
    }

    @Test
    public void testConfigureCommandExecuteBadArgLength() throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + command,
                configureObj);
        final String[] args = {command, arg};

        /* Don't need a mocked shell for this test case. */
        final CommandShell tmpShell = new CommandShell(System.in, System.out);

        try {
            configureObj.execute(args, tmpShell);
            fail("ShellUsageException expected, but correct number of args " +
                 "specified [expected less than " + args.length + " args]");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testConfigureCommandExecuteRequiredNameFlag()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String argFlag = "UNKNOWN_FLAG";
        final String argVal = "UNKNOWN_NAME";
        final String expectedArgFlag = "-name";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Missing required argument (" + expectedArgFlag + ") for " +
                "command: " + command, configureObj);

        final String[] args = {command, argFlag, argVal};

        /* Don't need a mocked shell for this test case. */
        final CommandShell tmpShell = new CommandShell(System.in, System.out);

        try {
            configureObj.execute(args, tmpShell);
            fail("ShellUsageException expected, but correct flag " +
                 "specified [flag expected=" + expectedArgFlag + ", " +
                 "flag specified=" + argFlag + "]");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    @Test
    public void testConfigureCommandExecuteStoreNameTooLong()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String nameFlag = "-name";

        final int maxStoreNameLen = 255;
        final byte[] nameBytes = new byte[maxStoreNameLen + 1];
        for (int i = 0; i < nameBytes.length; i++) {
            nameBytes[i] = 'a';
        }
        final String storeName = new String(nameBytes);

        final ShellArgumentException expectedException =
            new ShellArgumentException(
                "Invalid store name.  It exceeds the maximum length of " +
                maxStoreNameLen);

        final String[] args = {command, nameFlag, storeName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        try {
            configureObj.execute(args, shell);
            fail("ShellArgumentException expected, but valid store name " +
                 "specified [" + storeName + "]");
        } catch (ShellArgumentException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testConfigureCommandExecuteStoreNameInvalid()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String nameFlag = "-name";
        final String storeName = "TEST;STORE@INVALID";

        final ShellArgumentException expectedException =
            new ShellArgumentException(
                "Invalid store name: " + storeName + ".  It must consist of " +
                "letters, digits, hyphen, underscore, period.");

        final String[] args = {command, nameFlag, storeName};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        replay(shell);

        try {
            configureObj.execute(args, shell);
            fail("ShellArgumentException expected, but valid store name " +
                 "specified [" + storeName + "]");
        } catch (ShellArgumentException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testConfigureCommandExecuteStoreConfigured()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String nameFlag = "-name";
        final String storeName = "TEST_STORE";

        final String[] args = {command, nameFlag, storeName};

        final String expectedResult = "Store configured: " + storeName;

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).
            andReturn(SerialVersion.CURRENT).anyTimes();
        /* Void method: configure */
        cs.configure(storeName, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(false).anyTimes();
        replay(shell);

        try {
            final String result = configureObj.execute(args, shell);
            assertEquals(expectedResult, result);
        } finally {
            doVerification();
        }
        reset(cs, shell);
        expect(cs.getSerialVersion()).
        andReturn(SerialVersion.CURRENT).anyTimes();

        cs.configure(storeName, null, SerialVersion.CURRENT);
        expectLastCall().anyTimes();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(true);
        replay(shell);
        final ShellCommandResult scr =
            configureObj.executeJsonOutput(args, shell);
        final ObjectNode returnValue = scr.getReturnValue();
        assertEquals(returnValue.get("storeName").asText(), "TEST_STORE");
    }

    @Test
    public void testConfigureCommandExecuteStoreConfiguredJsonMode()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String nameFlag = "-name";
        final String storeName = "TEST_STORE";
        final String jsonFlag = "-json-v1";

        final String[] args = {command, nameFlag, storeName, jsonFlag};

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        /* Void method: configure */
        cs.configure(storeName, null, SerialVersion.CURRENT);
        expectLastCall();
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        expect(shell.getJson()).andReturn(true);
        replay(shell);

        try {
            final String result = configureObj.execute(args, shell);
            JsonNode resultNode = JsonUtils.parseJsonNode(result);
            assertEquals(command,
                resultNode.get(CommandJsonUtils.FIELD_OPERATION).
                    asText());
            assertEquals(5000,
                resultNode.get(CommandJsonUtils.FIELD_RETURN_CODE).
                    asInt());
            assertEquals("Operation ends successfully",
                resultNode.get(CommandJsonUtils.FIELD_DESCRIPTION).
                    asText());
            assertNull(resultNode.get(CommandJsonUtils.FIELD_CLEANUP_JOB));
            JsonNode valueNode = resultNode.get(
                CommandJsonUtils.FIELD_RETURN_VALUE);
            assertEquals(storeName, valueNode.get("store_name").asText());
        } finally {
            doVerification();
        }
    }

    @Test
    public void testConfigureCommandExecuteRemoteException()
        throws Exception {

        final ConfigureCommand configureObj = new ConfigureCommand();
        final String nameFlag = "-name";
        final String storeName = "TEST_STORE";

        final String[] args = {command, nameFlag, storeName};

        final RemoteException expectedRemoteException = new RemoteException();
        final ShellException expectedException =
            new ShellException("Cannot contact admin",
                               expectedRemoteException);

        /* Establish what is expected from each mock for this test */
        expect(cs.getSerialVersion()).andReturn(SerialVersion.CURRENT);
        /* Void method: configure */
        cs.configure(storeName, null, SerialVersion.CURRENT);
        expectLastCall().andThrow(expectedRemoteException);
        replay(cs);

        expect(shell.getAdmin()).andReturn(CommandServiceAPI.wrap(cs, null));
        /* Void method: noAdmin */
        shell.noAdmin(expectedRemoteException);
        expectLastCall().andThrow(expectedException);
        replay(shell);

        try {
            configureObj.execute(args, shell);
            fail("ShellException expected, but admin contacted");
        } catch (ShellException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        } finally {
            doVerification();
        }
    }
}
