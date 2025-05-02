/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;

import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.RecordBuilder;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.shell.PutCommand.PutKVCommand;
import oracle.kv.shell.PutCommand.PutTableCommand;
import oracle.kv.shell.PutCommand.PutTableCommand.TableAddValueSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableAddArrayValueSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableAddMapValueSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableAddRecordValueSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableExitSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableCancelSub;
import oracle.kv.shell.PutCommand.PutTableCommand.TableShowSub;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;

import org.junit.Test;

/* Test case coverage for PutCommand. */
public class PutCommandTest extends DataCLITestBase {

    @Test
    public void testPutCommandGetCommandOverview()
        throws Exception {

        final String expectedResult = PutCommand.COMMAND_OVERVIEW;
        assertEquals(expectedResult, new PutCommand().getCommandOverview());
    }

    @Test
    public void testPutKVCommandGetCommandSyntax()
        throws Exception {

        final String expectedResult = PutKVCommand.PUT_KV_SYNTAX;
        assertEquals(expectedResult,
            new PutKVCommand().getCommandSyntax());
    }

    @Test
    public void testPutKVCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            PutKVCommand.PUT_KV_DESCRIPTION;
        assertEquals(expectedResult,
            new PutKVCommand().getCommandDescription());
    }

    @Test
    public void testPutKVCommandExecuteBadArgs()
        throws Exception {

        final PutKVCommand subObj = new PutKVCommand();
        final String[] cmds =
            new String[]{PutKVCommand.KV_COMMAND};

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires an argument. */
        final String[] flagRequiredArgs = {
            PutKVCommand.KEY_FLAG,
            PutCommand.VALUE_FLAG,
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(PutKVCommand.KEY_FLAG, "/users");
        argsMap.put(PutCommand.VALUE_FLAG, "bob.smith");

        String[] requiredArgs = new String[] {
            PutKVCommand.KEY_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);

        requiredArgs = new String[] {
            PutCommand.VALUE_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);
    }

    @Test
    public void testPutKVCommandExecuteHelp()
        throws Exception {

        final PutKVCommand subObj = new PutKVCommand();
        final String[] cmds =
            new String[]{PutKVCommand.KV_COMMAND};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutKVCommandExecute()
        throws Exception {

        final PutKVCommand putKVCmd = new PutKVCommand();
        CommandShell shell = connectToStore();

        /* Case0: put kv -key INVALID_KEY -value test*/
        String[] cmds = getPutKVArgs("INVALID_KEY", "test");
        doExecuteShellArgumentException(putKVCmd, cmds, shell);

        /* Case1: put kv -key /user/01 -value Bob.Smith*/
        cmds = getPutKVArgs("/user/01", "Bob.Smith");
        String expectedMsg = "Operation successful, record inserted";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /* Case2: redo case1 to update the existing record. */
        expectedMsg = "Operation successful, record updated";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case3: redo case1 with -if-absent
         * put kv -key /user/01 -value Bob.Smith -if-absent
         */
        cmds = getPutKVArgs("/user/01", "Bob.Smith",
            new String[]{ PutCommand.IFABSENT_FLAG });
        expectedMsg = "Operation failed";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case4: redo case1 with -if-present
         * put kv -key /user/01 -value Bob.Smith -if-present
         */
        cmds = getPutKVArgs("/user/01", "Bob.Smith",
            new String[]{ PutCommand.IFPRESENT_FLAG });
        expectedMsg = "Operation successful, record updated";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case5: put a new record with -if-absent
         * put kv -key /user/02 -value Jack.Jones -if-absent
         */
        cmds = getPutKVArgs("/user/02", "Jack.Jones",
            new String[]{ PutCommand.IFABSENT_FLAG });
        expectedMsg = "Operation successful, record inserted";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case6: put a new record with -if-present
         * put kv -key /user/03 -value Hack.White -if-present
         */
        cmds = getPutKVArgs("/user/03", "Hack.White",
            new String[]{ PutCommand.IFPRESENT_FLAG });
        expectedMsg = "Operation failed";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case12: put a new record with -value -hex
         * put kv -key /text/hex/01 -value ABAgMEBQYHCAkA== -hex
         */
        cmds = new String[] {
            PutKVCommand.KV_COMMAND,
            PutKVCommand.KEY_FLAG, "/text/hex/01",
            PutCommand.VALUE_FLAG, "ABAgMEBQYHCAkA==",
            PutKVCommand.HEX_FLAG
        };
        expectedMsg = "Operation successful, record inserted";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);

        /**
         * Case13: put a new record with -value -hex -file
         * put kv -key /text/hex/01 -value ABAgMEBQYHCAkA== -hex
         */
        cmds = new String[] {
            PutKVCommand.KV_COMMAND,
            PutKVCommand.KEY_FLAG, "/text/hex/01",
            PutCommand.VALUE_FLAG, saveAsTempFile("ABAgMEBQYHCAkA=="),
            PutKVCommand.HEX_FLAG,
            PutCommand.FILE_FLAG,
        };
        expectedMsg = "Operation successful, record updated";
        doExecuteCheckRet(putKVCmd, cmds, expectedMsg, shell);
    }

    private String[] getPutKVArgs(String key, String value) {
        return getPutKVArgs(key, value, null);
    }

    private String[] getPutKVArgs(String key, String value, String[] args) {
        final String[] cmdArgs = new String[] {
            PutKVCommand.KV_COMMAND,
            PutKVCommand.KEY_FLAG, "",
            PutCommand.VALUE_FLAG, ""
        };

        String[] retArgs;
        if (args != null) {
            retArgs = new String[cmdArgs.length + args.length];
            System.arraycopy(cmdArgs, 0, retArgs, 0, cmdArgs.length);
            System.arraycopy(args, 0, retArgs, cmdArgs.length, args.length);
        } else {
            retArgs = cmdArgs;
        }
        retArgs[2] = key;
        retArgs[4] = value;
        return retArgs;
    }

    @Test
    public void testPutTableCommandGetCommandSyntax()
        throws Exception {

        final String expectedResult =
            PutTableCommand.PUT_TABLE_SYNTAX;
        assertEquals(expectedResult,
            new PutTableCommand().getCommandSyntax());
    }

    @Test
    public void testPutTableAddValueSubCommandGetCommandSyntax()
        throws Exception {
        final String syntax_FieldRequired_ValueNullable =
            TableAddValueSub.COMMAND + " " +
                PutTableCommand.FIELD_FLAG_DESC + " [" +
                PutTableCommand.VALUE_FLAG_DESC + " | " +
                PutTableCommand.NULL_VALUE_FLAG + " | " +
                Shell.eolt + TableAddValueSub.FILE_BINARY_DESC + "]";

        final String syntax_FieldRequired_ValueNotNullable =
            TableAddValueSub.COMMAND + " " +
                PutTableCommand.FIELD_FLAG_DESC + " [" +
                PutTableCommand.VALUE_FLAG_DESC + " | " +
                TableAddValueSub.FILE_BINARY_DESC + "]";

        final String syntax_FieldNotRequired_ValueNotNullable =
            TableAddValueSub.COMMAND + " [" +
                PutTableCommand.VALUE_FLAG_DESC + " | " +
                TableAddValueSub.FILE_BINARY_DESC + "]";

        assertEquals(syntax_FieldRequired_ValueNullable,
            new PutTableCommand
                .TableAddValueSub().getCommandSyntax());

        assertEquals(syntax_FieldRequired_ValueNotNullable,
            new PutTableCommand
                .TableAddValueSub(true, false).getCommandSyntax());

        assertEquals(syntax_FieldNotRequired_ValueNotNullable,
            new PutTableCommand
                .TableAddValueSub(false, false).getCommandSyntax());
    }

    @Test
    public void testPutTableAddArrayValueSubCommandGetCommandSyntax()
        throws Exception {

        final String syntax_FieldRequired =
            TableAddArrayValueSub.COMMAND + " " +
            PutTableCommand.FIELD_FLAG_DESC;

        final String syntax_FieldNotRequired =
            TableAddArrayValueSub.COMMAND + " " +
            CommandParser.optional(PutTableCommand.FIELD_FLAG_DESC);

        assertEquals(syntax_FieldRequired,
            new PutTableCommand
                .TableAddArrayValueSub().getCommandSyntax());

        assertEquals(syntax_FieldNotRequired,
            new PutTableCommand
                .TableAddArrayValueSub(false).getCommandSyntax());
    }

    @Test
    public void testPutTableAddMapValueSubCommandGetCommandSyntax()
        throws Exception {

        final String syntax_FieldRequired =
            TableAddMapValueSub.COMMAND + " " +
            PutTableCommand.FIELD_FLAG_DESC;

        final String syntax_FieldNotRequired =
            TableAddMapValueSub.COMMAND + " " +
            CommandParser.optional(PutTableCommand.FIELD_FLAG_DESC);

        assertEquals(syntax_FieldRequired,
            new PutTableCommand
                .TableAddMapValueSub().getCommandSyntax());

        assertEquals(syntax_FieldNotRequired,
            new PutTableCommand
                .TableAddMapValueSub(false).getCommandSyntax());
    }

    @Test
    public void testPutTableAddRecordValueSubCommandGetCommandSyntax()
        throws Exception {

        final String syntax_FieldRequired =
            TableAddRecordValueSub.COMMAND + " " +
            PutTableCommand.FIELD_FLAG_DESC;

        final String syntax_FieldNotRequired =
            TableAddRecordValueSub.COMMAND + " " +
            CommandParser.optional(PutTableCommand.FIELD_FLAG_DESC);

        assertEquals(syntax_FieldRequired,
            new PutTableCommand
                .TableAddRecordValueSub().getCommandSyntax());

        assertEquals(syntax_FieldNotRequired,
            new PutTableCommand
                .TableAddRecordValueSub(false).getCommandSyntax());
    }

    @Test
    public void testPutTableCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            PutTableCommand.PUT_TABLE_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand().getCommandDescription());
    }

    @Test
    public void testPutTableAddValueSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            TableAddValueSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableAddValueSub().getCommandDescription());
    }

    @Test
    public void testPutTableAddArrayValueSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            PutTableCommand
                .TableAddArrayValueSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableAddArrayValueSub().getCommandDescription());
    }

    @Test
    public void testPutTableAddMapValueSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            TableAddMapValueSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableAddMapValueSub().getCommandDescription());
    }

    @Test
    public void testPutTableAddRecordValueSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            PutTableCommand
                .TableAddRecordValueSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableAddRecordValueSub().getCommandDescription());
    }

    @Test
    public void testPutTableShowSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            TableShowSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableShowSub().getCommandDescription());
    }

    @Test
    public void testPutTableCancelSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            TableCancelSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableCancelSub().getCommandDescription());
    }

    @Test
    public void testPutTableExitSubCommandGetCommandDescription()
        throws Exception {

        final String expectedResult =
            TableExitSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new PutTableCommand
                .TableExitSub().getCommandDescription());
    }

    @Test
    public void testPutTableCommandExecuteBadArgs()
        throws Exception {

        final PutTableCommand subObj =
            new PutTableCommand();
        final String[] cmds = new String[]{
            PutTableCommand.TABLE_COMMAND
        };

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires an argument. */
        final String[] flagRequiredArgs = {
            PutTableCommand.TABLE_FLAG,
            PutCommand.JSON_FLAG,
            PutCommand.FILE_FLAG
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(PutTableCommand.TABLE_FLAG, "users");
        argsMap.put(PutCommand.IFABSENT_FLAG, PutCommand.IFABSENT_FLAG);
        argsMap.put(PutCommand.JSON_FLAG, "{\"a\":\"1\"}");

        String[] requiredArgs = new String[] {
            PutTableCommand.TABLE_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);
    }

    @Test
    public void testPutTableAddValueSubExecuteBadArgs()
        throws Exception {

        CommandShell shell = connectToStore();
        final String tableName = "users";
        final PutTableCommand putTableCmd =
            new PutTableCommand();

        final TableAddValueSub subObj =
            new TableAddValueSub();
        final String[] cmdsAddValue = new String[]{
            TableAddValueSub.COMMAND
        };
        final String[] cmdsPutTable = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, tableName
        };
        final String[] cmdsPutTableAddValue = new String[]{
            PutTableCommand.TABLE_COMMAND,
            TableAddValueSub.COMMAND
        };
        addUserTable(tableName);

        /* Check unknown argument. */
        doExecuteUnknownArg(subObj, cmdsAddValue, shell);

        /* Check flag requires an argument. */
        final String[] flagRequiredArgs = {
            PutTableCommand.FIELD_FLAG,
            PutCommand.VALUE_FLAG,
            PutCommand.FILE_FLAG
        };
        doExecuteFlagRequiredArg(subObj, cmdsAddValue, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(PutTableCommand.FIELD_FLAG, "f1");
        argsMap.put(PutCommand.VALUE_FLAG, "v1");

        String[] requiredArgs = new String[] {
            PutTableCommand.FIELD_FLAG
        };

        execute(putTableCmd, cmdsPutTable, shell);
        doExecuteRequiredArgs(shell.getCurrentCommand(), cmdsPutTableAddValue,
            argsMap, requiredArgs, requiredArgs[0], shell);

        requiredArgs = new String[] {
            PutCommand.VALUE_FLAG
        };
        doExecuteRequiredArgs(shell.getCurrentCommand(), cmdsPutTableAddValue,
            argsMap, requiredArgs, requiredArgs[0], shell);
    }

    @Test
    public void testPutTableAddArrayValueSubExecuteBadArgs()
        throws Exception {

        CommandShell shell = connectToStore();
        final String tableName = "users";
        final PutTableCommand putTableCmd =
            new PutTableCommand();

        final String[] cmdsPutTable = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, tableName
        };
        final String[] cmdsPutTableAddArraysValue = new String[]{
            PutTableCommand.TABLE_COMMAND,
            TableAddArrayValueSub.COMMAND
        };
        addUserTable(tableName);

        /* Check unknown argument. */
        execute(putTableCmd, cmdsPutTable, shell);
        doExecuteUnknownArg(shell.getCurrentCommand(),
            cmdsPutTableAddArraysValue, shell);

        /* Check flag requires an argument. */
        final String[] flagRequiredArgs = {
            PutTableCommand.FIELD_FLAG
        };
        doExecuteFlagRequiredArg(shell.getCurrentCommand(),
            cmdsPutTableAddArraysValue, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(PutTableCommand.FIELD_FLAG, "f1");

        String[] requiredArgs = new String[] {
            PutTableCommand.FIELD_FLAG
        };
        doExecuteRequiredArgs(shell.getCurrentCommand(),
            cmdsPutTableAddArraysValue, argsMap, requiredArgs,
            requiredArgs[0], shell);
    }

    @Test
    public void testPutTableExitCancelShowBadArgsCount()
        throws Exception {

        CommandShell shell = connectToStore();
        final String tableName = "users";
        final PutTableCommand putTableCmd =
            new PutTableCommand();
        final String[] cmdsPutTable = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, tableName
        };

        addUserTable(tableName);
        execute(putTableCmd, cmdsPutTable, shell);

        doExecuteBadArgCount(shell.getCurrentCommand(), TableExitSub.COMMAND,
            new String[]{PutTableCommand.TABLE_COMMAND, TableExitSub.COMMAND},
            shell);
        doExecuteBadArgCount(shell.getCurrentCommand(), TableCancelSub.COMMAND,
            new String[]{PutTableCommand.TABLE_COMMAND, TableCancelSub.COMMAND},
            shell);
        doExecuteBadArgCount(shell.getCurrentCommand(), TableShowSub.COMMAND,
            new String[]{PutTableCommand.TABLE_COMMAND, TableShowSub.COMMAND},
            shell);
    }

    @Test
    public void testPutTableCommandExecuteHelp()
        throws Exception {

        final PutTableCommand subObj =
                new PutTableCommand();
        final String[] cmds =
            new String[]{PutTableCommand.TABLE_COMMAND};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableAddValueCommandExecuteHelp()
        throws Exception {

        final TableAddValueSub subObj = new TableAddValueSub();
        final String[] cmds = new String[]{TableAddValueSub.COMMAND};
        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableAddArrayValueCommandExecuteHelp()
        throws Exception {

        final TableAddArrayValueSub subObj =
            new TableAddArrayValueSub();
        final String[] cmds = new String[]{
            TableAddArrayValueSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableAddMapValueCommandExecuteHelp()
        throws Exception {

        final TableAddMapValueSub subObj =
            new TableAddMapValueSub();
        final String[] cmds = new String[]{
            TableAddMapValueSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableAddRecordValueCommandExecuteHelp()
        throws Exception {

        final TableAddRecordValueSub subObj =
            new TableAddRecordValueSub();
        final String[] cmds = new String[]{
            TableAddRecordValueSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableShowCommandExecuteHelp()
        throws Exception {

        final TableShowSub subObj =
            new TableShowSub();
        final String[] cmds = new String[]{
            TableShowSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableExitCommandExecuteHelp()
        throws Exception {

        final TableExitSub subObj =
            new TableExitSub();
        final String[] cmds = new String[]{
            TableExitSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testPutTableCancelCommandExecuteHelp()
        throws Exception {

        final TableCancelSub subObj =
            new TableCancelSub();
        final String[] cmds = new String[]{
            TableCancelSub.COMMAND
        };

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    private void putTableAddValueCommandExecuteTest(String namespace)
        throws Exception {

        CommandShell shell = connectToStore();
        final TableImpl user = addUserTable(namespace, "users");
        final PutTableCommand putTableCmd = new PutTableCommand();
        final String[] cmdsPutTable = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, user.getFullNamespaceName()
        };
        String[] cmds;
        String expectedMsg;

        loadUsersTable(30, user);

        /**
         * Case1:
         *  put table -name [ns:]users
         *  add-value -field group -value group100
         *  add-value -field user -file user.out [failed]
         *  add-value -field user -value user100
         *  add-value -field name -null-value [failed]
         *  add-value -field name -value name100
         *  show
         *  exit
         */
        execute(putTableCmd, cmdsPutTable, shell);
        ShellCommand currentCmd = shell.getCurrentCommand();
        /*add-value -field user -field group -value group100 */
        cmds = getAddValueArgs("group", "group100");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /*add-value -field user -file user.out */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            TableAddValueSub.COMMAND,
            PutTableCommand.FIELD_FLAG, "user",
            PutCommand.FILE_FLAG, "user.out"
        };
        doExecuteShellArgumentException(currentCmd, cmds, shell);
        /*add-value -field user -field user -file user0 */
        cmds[cmds.length - 2] = PutCommand.VALUE_FLAG;
        cmds[cmds.length - 1] = "user100";
        doExecuteReturnNull(currentCmd, cmds, shell);

        /*add-value -field name -null-value */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            TableAddValueSub.COMMAND,
            PutTableCommand.FIELD_FLAG, "name",
            PutTableCommand.NULL_VALUE_FLAG
        };
        expectedMsg = "is not nullable";
        doExecuteShellException(currentCmd, cmds, expectedMsg, shell);

        /*add-value -field name -value name100 */
        cmds = getAddValueArgs("name", "name100");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /*show */
        cmds = getSingleCommandArgs("show");
        String[] expectedMsgs = new String[] {
            "\"group\" : \"group100\"",
            "\"user\" : \"user100\"",
            "\"name\" : \"name100\""
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);

        /* exit */
        cmds = getSingleCommandArgs("exit");
        expectedMsg = "Operation successful, row inserted";
        doExecuteCheckRet(currentCmd, cmds, expectedMsg, shell);

        /**
         * Case2: test -update flag
         *  put table -name [ns:]users -update
         *  add-value -field group -value group2
         *  add-value -field user -value user0
         *  show
         *  add-value -field age -value aaa [failed]
         *  add-value -field age -value 30
         *  exit
         */
        /* put table -name users -update */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
            PutTableCommand.UPDATE_FLAG
        };
        execute(putTableCmd, cmds, shell);
        currentCmd = shell.getCurrentCommand();

        /*add-value -field user -field group -value group100 */
        cmds = getAddValueArgs("group", "group2");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /*add-value -field user -value user0 */
        cmds = getAddValueArgs("user", "user0");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /*show */
        cmds = getSingleCommandArgs("show");
        expectedMsgs = new String[] {
            "\"group\" : \"group2\"",
            "\"user\" : \"user0\"",
            "\"name\" : \"name20\"",
            "\"age\" : 20",
            "\"address\" : \"address20\"",
            "\"phone\" : \"phone20\""
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);

        /*add-value -field age -value aaa */
        cmds = getAddValueArgs("age", "aaa");
        expectedMsg = "Invalid integer value: aaa";
        this.doExecuteShellException(currentCmd, cmds, expectedMsg, shell);

        /*add-value -field age -value 30 */
        cmds = getAddValueArgs("age", "30");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /* show */
        cmds = getSingleCommandArgs("show");
        expectedMsgs = new String[] {
            "\"group\" : \"group2\"",
            "\"user\" : \"user0\"",
            "\"name\" : \"name20\"",
            "\"age\" : 30",
            "\"address\" : \"address20\"",
            "\"phone\" : \"phone20\""
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);

        /* exit */
        cmds = getSingleCommandArgs("exit");
        expectedMsg = "Operation successful, row updated";
        doExecuteCheckRet(currentCmd, cmds, expectedMsg, shell);

        /* clean up */
        removeTable(namespace, user.getFullName(), true, commandService);
    }

    @Test
    public void testPutTableAddValueCommandExecute() throws Exception {
        /* tables without namespace */
        putTableAddValueCommandExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        putTableAddValueCommandExecuteTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        putTableAddValueCommandExecuteTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    private void putTableAddComplexValueCommandExecuteTest(String namespace)
        throws Exception {

        CommandShell shell = connectToStore();
        final TableImpl table = addTestTable(namespace, "mytable");
        final PutTableCommand putTableCmd = new PutTableCommand();
        final String[] cmdsPutTable = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, table.getFullNamespaceName()
        };

        /**
         *  put table -name [ns:]mytable
         *  add-value -field id -value 1
         *  add-array-value -field arrayF0
         *  add-value -value true
         *  add-value -value false
         *  show
         *  #[ true, false ]
         *  exit
         *  add-map-value -field mapF0
         *  #[failed: Invalid long value: bbb]
         *  add-value -field aaa -value bbb         *
         *  add-value -field aaa -value 1
         *  add-value -field aab -value 2
         *  show
         *  #{ "aaa" : 1, "aab" : 2 }
         *  exit
         *  show
         *  #"id" : 1,
         *  #"arrayF0" : [ false, true, false ],
         *  #"mapF0" : { "aaa" : 1, "aab" : 2 }
         *  add-value -field binaryF -value b3JhY2xlbm9zcWw=
         *  add-value -field fixedF -value c2ltcGxldGVzdA==
         *  #[failed: Invalid enumeration value 'type1', \
         *  must be in values: [TYPE1, TYPE2, TYPE3]]
         *  add-value -field enumF -value type1
         *  add-value -field enumF -value TYPE3
         *  show
         *  #"id" : 1,
         *  #"arrayF0" : [ true, false ],
         *  #"mapF0" : {
         *  #   "aaa" : 1,
         *  #   "aab" : 2
         *  #},
         *  #"binaryF" : "b3JhY2xlbm9zcWw=",
         *  #"fixedF" : "c2ltcGxldGVzdA==",
         *  #"enumF" : "TYPE3"
         *  add-record-value -field recordF0
         *  add-value -field rid -value 1
         *  add-array-value -field arrayF1
         *  add-value -value 1.11
         *  add-value -value 2.11
         *  exit
         *  add-map-value -field mapF1
         *  add-value -field info1 -value 98.7654321
         *  add-value -field info2 -value 1.23456789
         *  exit
         *  show
         *  #"rid" : 1,
         *  #"arrayF1" : [ 1.11, 2.11 ],
         *  #"mapF1" : {
         *  #   "info1" : 98.7654321,
         *  #   "info2" : 1.23456789
         *  #}
         *  exit
         *  exit
         *  #Operation successful, row inserted.
         */

        /* put table -name mytable */
        execute(putTableCmd, cmdsPutTable, shell);
        ShellCommand currentCmd = shell.getCurrentCommand();

        /* add-value -field id -value 1 */
        String[] cmds = getAddValueArgs("id", "1");
        doExecuteReturnNull(currentCmd, cmds, shell);

        /* add-array-value -field arrayF0 */
        cmds = getAddComplexValueArgs("array", "arrayF0");
        doExecuteReturnNull(currentCmd, cmds, shell);
        currentCmd = shell.getCurrentCommand();

        /* add-value -value true */
        cmds = getAddValueArgs(null, "true");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-value -value false */
        cmds = getAddValueArgs(null, "false");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* show */
        cmds = getSingleCommandArgs("show");
        String expectedMsg = "[true, false]";
        doExecuteCheckRet(currentCmd, cmds, expectedMsg, shell);
        /* exit */
        cmds = getSingleCommandArgs("exit");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* add-map-value -field mapF0 */
        cmds = getAddComplexValueArgs("map", "mapF0");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();
        /**
         * add-value -field aaa -value bbb
         * #[failed: Invalid long value: bbb]
         */
        cmds = getAddValueArgs("aaa", "bbb");
        expectedMsg = "Invalid long value: bbb";
        doExecuteShellException(currentCmd, cmds, expectedMsg, shell);
        /* add-value -field aaa -value 1 */
        cmds = getAddValueArgs("aaa", "1");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-value -field aab -value 2 */
        cmds = getAddValueArgs("aab", "2");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /**
         * show
         * { "aaa" : 1, "aab" : 2 }
         */
        cmds = getSingleCommandArgs("show");
        String[] expectedMsgs = new String[] {
            "\"aaa\" : 1",
            "\"aab\" : 2"
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);
        /* exit */
        cmds = getSingleCommandArgs("exit");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /**
         *  show
         * #"id" : 1,
         * #"arrayF0" : [ true, false ],
         * #"mapF0" : { "aaa" : 1, "aab" : 2 } }
         */
        cmds = getSingleCommandArgs("show");
        expectedMsgs = new String[] {
            "\"id\" : 1",
            "\"arrayF0\" : [true, false]",
            "\"mapF0\"",
            "\"aaa\" : 1",
            "\"aab\" : 2"
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);
        /* add-value -field binaryF -value b3JhY2xlbm9zcWw= */
        cmds = getAddValueArgs("binaryF", "b3JhY2xlbm9zcWw=");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-value -field fixedF -value c2ltcGxldGVzdA== */
        cmds = getAddValueArgs("fixedF", "c2ltcGxldGVzdA==");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /**
         * add-value -field enumF -value type1
         * failed: Invalid enumeration value 'type1',
         *         must be in values: [TYPE1, TYPE2, TYPE3]]
         */
        cmds = getAddValueArgs("enumF", "type1");
        expectedMsg = "Invalid enumeration value 'type1', " +
            "must be in values: [TYPE1, TYPE2, TYPE3]";
        doExecuteShellException(currentCmd, cmds, expectedMsg, shell);
        /* add-value -field enumF -value TYPE3 */
        cmds = getAddValueArgs("enumF", "TYPE3");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /**
         * show
         * "id" : 1,
         * "arrayF0" : [true, false],
         * "mapF0" : {
         *    "aaa" : 1,
         *    "aab" : 2
         * },
         * "binaryF" : "b3JhY2xlbm9zcWw=",
         * "fixedF" : "c2ltcGxldGVzdA==",
         * "enumF" : "TYPE3"
         */
        cmds = getSingleCommandArgs("show");
        expectedMsgs = new String[] {
            "\"id\" : 1",
            "\"arrayF0\" : [true, false]",
            "\"mapF0\"",
            "\"aaa\" : 1",
            "\"aab\" : 2",
            "\"binaryF\" : \"b3JhY2xlbm9zcWw=\"",
            "\"fixedF\" : \"c2ltcGxldGVzdA==\"",
            "\"enumF\" : \"TYPE3\""
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);

        /* add-record-value -field recordF0 */
        cmds = getAddComplexValueArgs("record", "recordF0");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* add-value -field rid -value 1 */
        cmds = getAddValueArgs("rid", "1");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-array-value -field arrayF1 */
        cmds = getAddComplexValueArgs("array", "arrayF1");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* add-value -value 1.11 */
        cmds = getAddValueArgs(null, "1.11");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-value -value 2.11 */
        cmds = getAddValueArgs(null, "2.11");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* exit  */
        cmds = getSingleCommandArgs("exit");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* add-map-value -field mapF1 */
        cmds = getAddComplexValueArgs("map", "mapF1");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* add-value -field info1 -value 98.7654321 */
        cmds = getAddValueArgs("info1", "98.7654321");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* add-value -field info2 -value 1.23456789 */
        cmds = getAddValueArgs("info2", "1.23456789");
        doExecuteReturnNull(currentCmd, cmds, shell);
        /* exit */
        cmds = getSingleCommandArgs("exit");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* show */
        /* #"rid" : 1, */
        /* #"arrayF1" : [1.11, 2.11], */
        /* #"mapF1" : { */
        /* #   "info1" : 98.7654321, */
        /* #   "info2" : 1.23456789 */
        /* #} */
        cmds = getSingleCommandArgs("show");
        expectedMsgs = new String[] {
            "\"rid\" : 1",
            "\"arrayF1\" : [1.11, 2.11]",
            "\"mapF1\"",
            "\"info1\" : 98.7654321",
            "\"info2\" : 1.23456789"
        };
        doExecuteCheckRet(currentCmd, cmds, expectedMsgs, shell);
        /* exit */
        cmds = getSingleCommandArgs("exit");
        doExecuteReturnNull(currentCmd, cmds, shell);

        currentCmd = shell.getCurrentCommand();

        /* exit */
        cmds = getSingleCommandArgs("exit");
        expectedMsg = "Operation successful, row inserted.";
        doExecuteCheckRet(currentCmd, cmds, expectedMsg, shell);

        /* clean up */
        removeTable(namespace, table.getFullName(), true, commandService);
    }

    @Test
    public void testPutTableAddComplexValueCommandExecute()
        throws Exception {
        /* tables without namespace */
        putTableAddComplexValueCommandExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        putTableAddComplexValueCommandExecuteTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        putTableAddComplexValueCommandExecuteTest(
            TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    private void putTableExecuteJsonTest(String namespace)
        throws Exception {

        CommandShell shell = connectToStore();
        final TableImpl user = addUserTable(namespace, "users");
        final String nsTableName = user.getFullNamespaceName();
        final PutTableCommand putTableCmd = new PutTableCommand();
        Row updRow, newRow;
        String[] cmds;
        String expectedMsg;

        loadUsersTable(30, user);

        PrimaryKey key = user.createPrimaryKey();
        key.put("group", "group0");
        key.put("user", "user0");
        Row row = tableImpl.get(key, null);
        if (row == null) {
            fail("Expected to get row but not by key: " +
            key.toJsonString(false));
        }

        /* Case1: put table -name [ns:]users -json <jsonString>. */
        updRow = user.createRow(row);
        updRow.put("age", updRow.get("age").asInteger().get() + 10);
        newRow =  user.createRow(row);
        newRow.put("group", "group1000").toJsonString(false);

        /* Put an new row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, updRow.toJsonString(false)
        };
        expectedMsg = "Operation successful, row updated";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Put an updated row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, newRow.toJsonString(false)
        };
        expectedMsg = "Operation successful, row inserted";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Case2: put table -name [ns:]users -json <jsonString> -if-absent. */
        updRow = user.createRow(row);
        updRow.put("age", updRow.get("age").asInteger().get() + 10);
        newRow =  user.createRow(row);
        newRow.put("group", "group2000").toJsonString(false);

        /* Put an new row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, newRow.toJsonString(false)
        };
        expectedMsg = "Operation successful, row inserted";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Put an updated row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, updRow.toJsonString(false),
            PutCommand.IFABSENT_FLAG
        };
        expectedMsg = "Operation failed, A record was already present";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Case3: put table -name users -json <jsonString> -if-present. */
        updRow = user.createRow(row);
        updRow.put("age", updRow.get("age").asInteger().get() + 10);
        newRow =  user.createRow(row);
        newRow.put("group", "group3000").toJsonString(false);

        /* Put an new row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, newRow.toJsonString(false),
            PutCommand.IFPRESENT_FLAG
        };
        expectedMsg = "No existing record was present";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Put an updated row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, updRow.toJsonString(false),
            PutCommand.IFPRESENT_FLAG
        };
        expectedMsg = "Operation successful, row updated";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Case4: put table -name users -json <jsonString> -update. */
        updRow = user.createRow(row);
        updRow.put("age", updRow.get("age").asInteger().get() + 10);
        newRow =  user.createRow(row);
        newRow.put("group", "group4000").toJsonString(false);

        /* Put an new row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, newRow.toJsonString(false),
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "No existing record was present with the given " +
                "primary key: {\"group\":\"group4000\",\"user\":\"user0\"}";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* Put an updated row */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, updRow.toJsonString(false),
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "Operation successful, row updated";
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Update partial row */
        String json = "{\"group\":\"group2\",\"user\":\"user0\"," +
                      "\"address\":\"address20_upd\"}";
        key = user.createPrimaryKeyFromJson(json, false);
        row = tableImpl.get(key, null);
        if (row == null) {
            fail("Expected to get row but not by key: " +
                    key.toJsonString(false));
        } else {
            cmds = new String[] {
                PutTableCommand.TABLE_COMMAND,
                PutTableCommand.TABLE_FLAG, nsTableName,
                PutCommand.JSON_FLAG, json,
                PutTableCommand.UPDATE_FLAG
            };
            expectedMsg = "Operation successful, row updated";
            doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

            row.put("address", "address20_upd");
            newRow = tableImpl.get(key, null);
            if (newRow.compareTo(row) != 0) {
                fail("The expected update row is " + row.toJsonString(false) +
                     ", but get" + newRow.toJsonString(false));
            }
        }

        /* Update row with an incomplete primary key from JSON input */
        json = "{\"group\":\"group2\", \"address\":\"address20_upd_upd\"}";
        key = user.createPrimaryKeyFromJson(json, false);
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, json,
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "Missing primary key field: user";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* Update row with empty primary key from JSON input */
        json = "{\"address\":\"address20_upd_upd\"}";
        key = user.createPrimaryKeyFromJson(json, false);
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, json,
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "Primary key is empty";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* Case5: put table -name [ns:]users -exact */
        updRow = user.createRow(row);
        updRow.put("age", updRow.get("age").asInteger().get() + 10);
        updRow.remove("age");
        newRow =  user.createRow(row);
        newRow.put("group", "group5000").toJsonString(false);
        newRow.remove("age");
        String newRowJson = newRow.toJsonString(false);

        /* Use -json <jsonString> flag. */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, newRowJson,
            PutTableCommand.EXACT_FLAG
        };
        expectedMsg = "Not enough fields for value in JSON input." +
                      "Found 5, expected 6";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* Use -file <file> flag. */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.FILE_FLAG, saveAsTempFile(newRowJson),
            PutTableCommand.EXACT_FLAG
        };
        expectedMsg = "Not enough fields for value in JSON input."
            + "Found 5, expected 6";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* Use -json <jsonString> -update flag. */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.JSON_FLAG, updRow.toJsonString(false),
            PutTableCommand.EXACT_FLAG,
            PutTableCommand.UPDATE_FLAG,
        };
        expectedMsg = "Not enough fields for value in JSON input." +
                      "Found 5, expected 6";
        doExecuteShellException(putTableCmd, cmds, expectedMsg, shell);

        /* clean up */
        removeTable(namespace, user.getFullName(), true, commandService);
    }

    @Test
    public void testPutTableExecuteJson()
        throws Exception {

        /* tables without namespace */
        putTableExecuteJsonTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        putTableExecuteJsonTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        putTableExecuteJsonTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    private void putTableExecuteFileTest(String namespace)
        throws Exception {

        CommandShell shell = connectToStore();
        final TableImpl user = addUserTable(namespace, "users");
        final String nsTableName = user.getFullNamespaceName();
        final PutTableCommand putTableCmd = new PutTableCommand();
        String[] cmds;
        String expectedMsg;

        loadUsersTable(10, user);

        /* Case1: put table -name [ns:]users -file <file> -if-absent. */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.FILE_FLAG, dumpRowsToFile(user, false),
            PutCommand.IFABSENT_FLAG
        };
        expectedMsg = "Inserted " + 10 + " rows to " + user.getFullName();
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Case2: put table -name [ns:]users -file <file> -update. */
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.FILE_FLAG, dumpRowsToFile(user, true),
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "Updated " + 10 + " rows to " + user.getFullName();
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Case3: test strings that contains curly braces */
        String[] addresses = {
            "{address",
            "address}",
            "a{ddress",
            "a}ddress",
            "{",
            "}",
            "{address \"{abc\"",
            "{address \"abc{\"",
        };

        final StringBuilder sb = new StringBuilder();
        final String group = "group1000";

        /* Put table with file that contains single line JSON strings */
        int i = 0;
        for (String address : addresses) {
            Row row = user.createRow();
            row.put("group", group);
            row.put("user", "user-" + (i++));
            row.put("address", address);
            sb.append(row.toJsonString(false));
            sb.append("\n");
        }
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.FILE_FLAG, saveAsTempFile(sb.toString()),
            PutCommand.IFABSENT_FLAG
        };
        expectedMsg = "Inserted " + addresses.length + " rows to " +
                      user.getFullName();
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* Put table with file that contains pretty JSON strings */
        sb.setLength(0);
        i = 0;
        for (String address : addresses) {
            Row row = user.createRow();
            row.put("group", group);
            row.put("user", "user-" + (i++));
            row.put("address", address + "_upd");
            sb.append(row.toJsonString(true));
            sb.append("\n");
        }
        cmds = new String[] {
            PutTableCommand.TABLE_COMMAND,
            PutTableCommand.TABLE_FLAG, nsTableName,
            PutCommand.FILE_FLAG, saveAsTempFile(sb.toString()),
            PutTableCommand.UPDATE_FLAG
        };
        expectedMsg = "Updated " + addresses.length + " rows to " +
            user.getFullName();
        doExecuteCheckRet(putTableCmd, cmds, expectedMsg, shell);

        /* clean up */
        removeTable(namespace, user.getFullName(), true, commandService);
    }

    @Test
    public void testPutTableExecuteFile()
        throws Exception {

        /* tables without namespace */
        putTableExecuteFileTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        putTableExecuteFileTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        putTableExecuteFileTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    private String[] getAddValueArgs(String field, String value) {
        final String[] cmdArgs = new String[] {
            PutTableCommand.TABLE_COMMAND,
            TableAddValueSub.COMMAND,
            PutTableCommand.FIELD_FLAG, "",
            PutCommand.VALUE_FLAG, ""
        };
        cmdArgs[3] = (field == null)? "" : field;
        cmdArgs[5] = value;
        return cmdArgs;
    }

    private String[] getAddComplexValueArgs(String type, String field) {
        final String[] cmdArgs = new String[] {
            PutTableCommand.TABLE_COMMAND,
            TableAddArrayValueSub.COMMAND,
            PutTableCommand.FIELD_FLAG, ""
        };
        if (type.equalsIgnoreCase("map")) {
            cmdArgs[1] = TableAddMapValueSub.COMMAND;
        } else if (type.equalsIgnoreCase("record")) {
            cmdArgs[1] = TableAddRecordValueSub.COMMAND;
        }
        cmdArgs[3] = field;
        return cmdArgs;
    }

    private String[] getSingleCommandArgs(String type) {
        final String[] cmdArgs = new String[] {
            PutTableCommand.TABLE_COMMAND,
            TableShowSub.COMMAND
        };
        if (type.equalsIgnoreCase("exit")) {
            cmdArgs[1] = TableExitSub.COMMAND;
        } else if (type.equalsIgnoreCase("cancel")){
            cmdArgs[1] = TableCancelSub.COMMAND;
        }
        return cmdArgs;
    }

    private String dumpRowsToFile(TableImpl user, boolean pretty) {

        StringBuilder sb = new StringBuilder();
        PrimaryKey key = user.createPrimaryKey();
        key.put("group", "group0");

        TableIterator<Row> iter = tableImpl.tableIterator(key, null, null);
        String comments = "#Users of group100";
        sb.append(comments);
        sb.append("\n");
        try {
            while (iter.hasNext()) {
                Row row = iter.next();
                row.put("group", "group100");
                comments = "#User: " + row.get("user").asString().get();
                sb.append(comments);
                sb.append("\n");
                sb.append(row.toJsonString(pretty));
                sb.append("\n");
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
        return saveAsTempFile(sb.toString());
    }

    private TableImpl addTestTable(String namespace, String tableName)
        throws Exception {

        RecordBuilder rb = TableBuilder.createRecordBuilder("recordF0");
        rb.addInteger("rid");
        rb.addField("arrayF1",
            TableBuilder.createArrayBuilder().addFloat().build());
        rb.addField("mapF1",
            TableBuilder.createMapBuilder().addDouble().build());

        TableBuilder tb = namespace == null ?
            TableBuilder.createTableBuilder(tableName) :
            TableBuilder.createTableBuilder(namespace, tableName);
        tb.addInteger("id")
            .addField("arrayF0",
                TableBuilder.createArrayBuilder().addBoolean().build())
            .addField("mapF0",
                TableBuilder.createMapBuilder().addLong().build())
            .addBinary("binaryF")
            .addFixedBinary("fixedF", 10)
            .addEnum("enumF", new String[]{"TYPE1", "TYPE2", "TYPE3"}, null)
            .addField("recordF0", rb.build())
            .primaryKey(new String[]{"id"});
       addTable(tb, true);
       return namespace == null ?
           getTable(tableName) : getTable(namespace, tableName);
    }
}
