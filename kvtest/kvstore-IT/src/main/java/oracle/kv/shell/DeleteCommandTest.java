/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.shell;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.TableAPI;

import org.junit.Test;

/* Test case coverage for DeleteCommand. */
public class DeleteCommandTest extends DataCLITestBase {

    @Test
    public void testDeleteCommandGetCommandOverview()
        throws Exception {

        final String expectedResult = DeleteCommand.COMMAND_OVERVIEW;
        assertEquals(expectedResult, new DeleteCommand().getCommandOverview());
    }

    @Test
    public void testDeleteKVCommandGetCommandSyntax() throws Exception {

        final String expectedResult =
            DeleteCommand.DeleteKVCommand.COMMAND_SYNTAX;
        assertEquals(expectedResult,
            new DeleteCommand.DeleteKVCommand().getCommandSyntax());
    }

    @Test
    public void testDeleteKVCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            DeleteCommand.DeleteKVCommand.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new DeleteCommand.DeleteKVCommand().getCommandDescription());
    }

    @Test
    public void testDeleteKVCommandExecuteBadArgs()
        throws Exception {

        final DeleteCommand.DeleteKVCommand subObj =
            new DeleteCommand.DeleteKVCommand();
        final String[] cmds =
            new String[]{DeleteCommand.DeleteKVCommand.COMMAND_NAME};

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires argument. */
        final String[] flagRequiredArgs = {
            DeleteCommand.DeleteKVCommand.KEY_FLAG,
            DeleteCommand.START_FLAG,
            DeleteCommand.END_FLAG
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(DeleteCommand.DeleteKVCommand.KEY_FLAG, "/users");
        argsMap.put(DeleteCommand.START_FLAG, "group0");
        argsMap.put(DeleteCommand.END_FLAG, "group1");
        argsMap.put(DeleteCommand.DeleteKVCommand.MULTI_FLAG,
                    DeleteCommand.DeleteKVCommand.MULTI_FLAG);

        final String[] requiredArgs = new String[] {
            DeleteCommand.DeleteKVCommand.KEY_FLAG,
            DeleteCommand.DeleteKVCommand.MULTI_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);
    }

    @Test
    public void testDeleteKVCommandExecuteHelp()
        throws Exception {

        final DeleteCommand.DeleteKVCommand subObj =
            new DeleteCommand.DeleteKVCommand();
        final String[] cmds =
            new String[]{DeleteCommand.DeleteKVCommand.COMMAND_NAME};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testDeleteKVCommandExecute()
        throws Exception {

        final DeleteCommand.DeleteKVCommand subObj =
            new DeleteCommand.DeleteKVCommand();
        final CommandShell shell = connectToStore();

        /* Case1: delete kv -key /KEY_NOT_EXISTS */
        String keyString = "/KEY_NOT_EXISTS";
        String[] cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.KEY_FLAG,
            keyString
        };
        String expectedMsg = "Key deletion failed: " + keyString;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case2: delete kv -key /employee/id/-/01 */
        keyString = "/employee/id/-/01";
        String valueString = "Bob.Smith";
        store.put(Key.fromString(keyString),
                  Value.createValue(valueString.getBytes()));
        cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.KEY_FLAG,
            keyString
        };
        expectedMsg = "Key deleted: " + keyString;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /* Load user records to store. */
        loadUsers(30, null);

        /*Case3: delete kv -key /group/0/-/user -start 0 -end 1 -all*/
        keyString = "/group/0/-/user";
        cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.KEY_FLAG, keyString,
            DeleteCommand.START_FLAG, "0",
            DeleteCommand.END_FLAG, "1",
            DeleteCommand.DeleteKVCommand.MULTI_FLAG
        };
        expectedMsg = "2 Keys deleted starting at " + keyString;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case4: delete kv -key /group/0 -all*/
        keyString = "/group/0";
        cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.KEY_FLAG, keyString,
            DeleteCommand.DeleteKVCommand.MULTI_FLAG
        };
        expectedMsg = "8 Keys deleted starting at " + keyString;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case5: delete kv -key /group -start 2 -all*/
        keyString = "/group";
        cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.KEY_FLAG,  keyString,
            DeleteCommand.START_FLAG, "2",
            DeleteCommand.DeleteKVCommand.MULTI_FLAG
        };
        expectedMsg = "10 Keys deleted starting at " + keyString;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case6: delete kv -all*/
        keyString = "/group";
        cmds = new String[] {
            DeleteCommand.DeleteKVCommand.COMMAND_NAME,
            DeleteCommand.DeleteKVCommand.MULTI_FLAG
        };
        expectedMsg = "10 Keys deleted starting at root";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);
    }

    @Test
    public void testDeleteTableCommandGetCommandSyntax() throws Exception {

        final String expectedResult =
            DeleteCommand.DeleteTableCommand.COMMAND_SYNTAX;
        assertEquals(expectedResult,
            new DeleteCommand.DeleteTableCommand().getCommandSyntax());
    }

    @Test
    public void testDeleteTableCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            DeleteCommand.DeleteTableCommand.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new DeleteCommand.DeleteTableCommand().getCommandDescription());
    }

    @Test
    public void testDeleteTableCommandExecuteBadArgs()
        throws Exception {

        final DeleteCommand.DeleteTableCommand subObj =
            new DeleteCommand.DeleteTableCommand();
        String[] cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME
        };

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires argument. */
        String[] flagRequiredArgs = {
            DeleteCommand.DeleteTableCommand.TABLE_FLAG,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG,
            DeleteCommand.DeleteTableCommand.ANCESTOR_FLAG,
            DeleteCommand.DeleteTableCommand.CHILD_FLAG,
            DeleteCommand.DeleteTableCommand.JSON_FLAG,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG,
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "f1"
        };
        flagRequiredArgs = new String[] {
            DeleteCommand.DeleteTableCommand.VALUE_FLAG,
            DeleteCommand.START_FLAG,
            DeleteCommand.END_FLAG,
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME
        };
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(DeleteCommand.DeleteTableCommand.TABLE_FLAG, "users");
        String[] requiredArgs = new String[] {
            DeleteCommand.DeleteTableCommand.TABLE_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);

        argsMap.put(DeleteCommand.DeleteTableCommand.FIELD_FLAG, "f1");
        argsMap.put(DeleteCommand.DeleteTableCommand.VALUE_FLAG, "v1");
        requiredArgs = new String[] {
            DeleteCommand.DeleteTableCommand.VALUE_FLAG,
            DeleteCommand.START_FLAG,
            DeleteCommand.END_FLAG,
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0] + " or " + requiredArgs[1] +
            " | " + requiredArgs[2], shell);
    }

    @Test
    public void testDeleteTableCommandExecuteHelp()
        throws Exception {

        final DeleteCommand.DeleteTableCommand subObj =
            new DeleteCommand.DeleteTableCommand();
        final String[] cmds =
            new String[]{DeleteCommand.DeleteTableCommand.COMMAND_NAME};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    private void deleteTableCommandExecuteTest(String namespace)
        throws Exception {

        final DeleteCommand.DeleteTableCommand subObj =
            new DeleteCommand.DeleteTableCommand();
        final CommandShell shell = connectToStore();

        final String tableName = "users";
        final String nsTableName =
            NameUtils.makeQualifiedName(namespace, "users");
        //TODO: allow MRTable mode after MRTable supports child tables.
        final TableImpl user = addUserTable(namespace, tableName,
                                            true/*noMRTableMode*/);
        final TableImpl email = addEmailTable(namespace, "email", user);
        loadUsersTable(30, user);
        loadUserEmail(user, email);

        /**
         * Case0: delete table -name [ns:]TableNotExists
         */
        String[] cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG,
            NameUtils.makeQualifiedName(namespace, "TableNotExists"),
            DeleteCommand.DeleteTableCommand.DELETE_ALL_FLAG
        };
        String expectedMsg = "TableNotExists";
        doExecuteShellException(subObj, cmds, expectedMsg, shell);

        /**
         * Case1: Delete a single row.
         * delete table -name [ns:]users -field group -value group0
         * -field user -value user1
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group0",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "user0"
        };
        expectedMsg = "1 row deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case2: Delete with a primary key which contains full major key path.
         * delete table -name [ns:]users -field group -value group0
         * -field user -start user5 -end user9
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group0",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.START_FLAG, "user5",
            DeleteCommand.END_FLAG, "user9",
        };
        expectedMsg = "5 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case3: Delete with a primary key with a field range.
         * delete table -name [ns:]users -field group -start group2
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.START_FLAG, "group2"
        };
        expectedMsg = "10 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case4: Delete with a primary key using -json
         * delete table -name [ns:]users
         * -json '{"group":"group1", "user":"user3"}'
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.JSON_FLAG,
            "{\"group\":\"group1\", \"user\":\"user3\"}"
        };
        expectedMsg = "1 row deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case5: Delete all.
         * delete table -name [ns:]users -delete-all'
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.DELETE_ALL_FLAG
        };
        expectedMsg = "13 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        loadUsersTable(30, user);
        /**
         * Case6-A: Delete rows using a primary key together with rows
         * in its child table.
         * delete table -name [ns:]users -field group -value group0
         * -field user -value user0 -child users.email
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group0",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "user0",
            DeleteCommand.DeleteTableCommand.CHILD_FLAG, email.getFullName()
        };
        expectedMsg = "3 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case6-B: Delete rows using a primary key together with rows
         * in its child table.
         * delete table -name [ns:]users -field group -value group0
         * -field user -value user0 -child [ns:]users.email
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG, nsTableName,
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group0",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "user1",
            DeleteCommand.DeleteTableCommand.CHILD_FLAG,
            email.getFullNamespaceName()
        };
        expectedMsg = "3 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case7-A: Delete rows using a primary key together with rows
         * in its parent table.
         * delete table -name [ns:]users.email -field group -value group0
         * -field user -start user0 -end user4 -ancestor users.
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG,
            email.getFullNamespaceName(),
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group1",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.START_FLAG, "user0",
            DeleteCommand.END_FLAG, "user4",
            DeleteCommand.DeleteTableCommand.ANCESTOR_FLAG, user.getFullName()
        };
        expectedMsg = "15 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case7-B: Delete rows using a primary key together with rows
         * in its parent table.
         * delete table -name [ns:]users.email -field group -value group0
         * -field user -start user0 -end user4 -ancestor [ns:]users.
         */
        cmds = new String[] {
            DeleteCommand.DeleteTableCommand.COMMAND_NAME,
            DeleteCommand.DeleteTableCommand.TABLE_FLAG,
            email.getFullNamespaceName(),
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "group",
            DeleteCommand.DeleteTableCommand.VALUE_FLAG, "group2",
            DeleteCommand.DeleteTableCommand.FIELD_FLAG, "user",
            DeleteCommand.START_FLAG, "user0",
            DeleteCommand.END_FLAG, "user4",
            DeleteCommand.DeleteTableCommand.ANCESTOR_FLAG,
            user.getFullNamespaceName()
        };
        expectedMsg = "15 rows deleted";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /* clean up */
        removeTable(namespace, email.getFullName(), true, commandService);
        removeTable(namespace, user.getFullName(), true, commandService);
    }

    @Test
    public void testDeleteTableCommandExecute() throws Exception {
        /* tables without namespace */
        deleteTableCommandExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        deleteTableCommandExecuteTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        deleteTableCommandExecuteTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

}
