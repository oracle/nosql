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

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.Shell;

import com.sleepycat.util.UtfOps;

import org.junit.Test;

/* Test case coverage for GetCommand. */
public class GetCommandTest extends DataCLITestBase {

    @Test
    public void testGetCommandGetCommandOverview()
        throws Exception {

        final String expectedResult = GetCommand.COMMAND_OVERVIEW;
        assertEquals(expectedResult, new GetCommand().getCommandOverview());
    }

    @Test
    public void testGetKVCommandGetCommandSyntax()
        throws Exception {

        final String expectedResult = GetCommand.GetKVCommand.COMMAND_SYNTAX;
        assertEquals(expectedResult,
            new GetCommand.GetKVCommand().getCommandSyntax());
    }

    @Test
    public void testGetKVCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            GetCommand.GetKVCommand.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new GetCommand.GetKVCommand().getCommandDescription());
    }

    @Test
    public void testGetKVCommandExecuteBadArgs()
        throws Exception {

        final GetCommand.GetKVCommand subObj = new GetCommand.GetKVCommand();
        final String[] cmds =
            new String[]{GetCommand.GetKVCommand.COMMAND_NAME};

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires an argument. */
        final String[] flagRequiredArgs = {
            GetCommand.GetKVCommand.KEY_FLAG,
            GetCommand.FILE_FLAG,
            GetCommand.START_FLAG,
            GetCommand.END_FLAG
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(GetCommand.GetKVCommand.KEY_FLAG, "/users");
        argsMap.put(GetCommand.FILE_FLAG, "./a.out");
        argsMap.put(GetCommand.GetKVCommand.MULTI_FLAG,
                    GetCommand.GetKVCommand.MULTI_FLAG);

        final String[] requiredArgs = new String[] {
            GetCommand.GetKVCommand.KEY_FLAG,
            GetCommand.GetKVCommand.MULTI_FLAG
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);
    }

    @Test
    public void testGetKVCommandExecuteHelp()
        throws Exception {

        final GetCommand.GetKVCommand subObj = new GetCommand.GetKVCommand();
        final String[] cmds =
            new String[]{GetCommand.GetKVCommand.COMMAND_NAME};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    @Test
    public void testGetKVCommandExecute()
        throws Exception {

        CommandShell shell = connectToStore();
        final GetCommand.GetKVCommand subObj = new GetCommand.GetKVCommand();
        /* Case1: get kv -key /KEY_NOT_EXISTS */
        String[] cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG,
            "/KEY_NOT_EXISTS"
        };
        String expectedMsg = "Key not found in store";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case2: get kv -key keyWithNoPrefixSlash */
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, "keyWithNoPrefixSlash"
        };
        doExecuteShellArgumentException(subObj, cmds, shell);

        /*Case3: get kv -key /employee/id/-/01 */
        String keyString = "/employee/id/-/01";
        String valueString = "Bob.Smith";
        store.put(Key.fromString(keyString),
                  Value.createValue(valueString.getBytes()));
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG,
            keyString
        };
        doExecuteCheckRet(subObj, cmds, valueString, shell);

        /*Case4: get kv -key /employee/id/-/01 */
        keyString = "/employee/id/-/01";
        store.put(Key.fromString(keyString),
                  Value.createValue(valueString.getBytes()));
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG
        };
        expectedMsg = "1 Record returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case6: get kv -key /test/binary */
        keyString = "/test/binary";
        byte[] bytes = new byte[] {
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a
        };
        store.put(Key.fromString(keyString), Value.createValue(bytes));
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString
        };
        valueString = "AQIDBAUGBwgJCg==";
        doExecuteCheckRet(subObj, cmds, valueString, shell);

        loadUsers(30, null);

        /*Case7: get kv -key /group/2/-/user/1 */
        keyString = "/group/2/-/user/1";
        valueString = "name21";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG,
            keyString
        };
        doExecuteCheckRet(subObj, cmds, valueString, shell);

        /*Case8: get kv -key /group/2/-/user/1 -file <output>*/
        String output = getTempFileName("getkv", ".out");
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.FILE_FLAG, output
        };
        expectedMsg = "Wrote value to file " + output;
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case9: get kv -key /group/1/-/user -start 0 -end 9 -keyonly -all*/
        keyString = "/group/1/-/user";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.START_FLAG, "0",
            GetCommand.END_FLAG, "9",
            GetCommand.KEY_ONLY_FLAG,
            GetCommand.GetKVCommand.MULTI_FLAG
        };
        expectedMsg = "10 Keys returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case10: get kv -key /group/2/-/user -all -file <output>*/
        keyString = "/group/2/-/user";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.FILE_FLAG, output
        };
        expectedMsg = "10 Records returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case11: get kv -key /group/0 -all -keyonly*/
        keyString = "/group/0";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.KEY_ONLY_FLAG
        };
        expectedMsg = "10 Keys returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case12: get kv -key /group/0 -all*/
        keyString = "/group/0";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG
        };
        expectedMsg = "10 Records returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case13: get kv -key /group/0 -start user0 -all*/
        keyString = "/group/0";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.START_FLAG, "user0"
        };
        expectedMsg = "0 Record returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case14: get kv -key /group -start 1 -end 2 -keyonly -all*/
        keyString = "/group";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.START_FLAG, "1",
            GetCommand.END_FLAG, "2",
            GetCommand.KEY_ONLY_FLAG
        };
        expectedMsg = "20 Keys returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case15: get kv -key /group -start 2 -end 1 -keyonly -all*/
        keyString = "/group";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.START_FLAG, "2",
            GetCommand.END_FLAG, "1",
            GetCommand.KEY_ONLY_FLAG
        };
        doExecuteShellArgumentException(subObj, cmds, shell);

        /*Case16: get kv -key /group -all*/
        keyString = "/group";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG
        };
        expectedMsg = "30 Records returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /*Case17: get kv -key /group -all -valueonly*/
        keyString = "/group";
        cmds = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.GetKVCommand.VALUE_ONLY_FLAG
        };
        expectedMsg = "30 Records returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);
    }

    @Test
    public void testGetKVToBinaryFile()
        throws Exception {

        final CommandShell shell = connectToStore();
        final String keyString = "/binary";
        final String outputFile = getTempFileName("getKVFile", ".out");

        final String[] getKVArgs = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString
        };

        final String[] getKVToFileArgs = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.FILE_FLAG, outputFile
        };

        final String[] getKVToFileWithALLArgs = new String[] {
            GetCommand.GetKVCommand.COMMAND_NAME,
            GetCommand.GetKVCommand.KEY_FLAG, keyString,
            GetCommand.FILE_FLAG, outputFile,
            GetCommand.GetKVCommand.MULTI_FLAG,
            GetCommand.GetKVCommand.VALUE_ONLY_FLAG
        };

        final GetCommand.GetKVCommand subObj = new GetCommand.GetKVCommand();
        for (int i = 0; i < 10; i++) {
            final int len = 5 * i;
            final byte[] bytes = getBinaryBytes(len);
            String base64Str = TableJsonUtils.encodeBase64(bytes);
            if (bytes.length > 0) {
                base64Str += " [Base64]";
            }
            store.put(Key.fromString(keyString), Value.createValue(bytes));
            doExecuteCheckRet(subObj, getKVArgs, base64Str, shell);
            doExecuteCheckFile(subObj, getKVToFileArgs, outputFile,
                               bytes, shell);
            doExecuteCheckFile(subObj, getKVToFileWithALLArgs, outputFile,
                               (base64Str + Shell.eol).getBytes(), shell);
        }
    }


    private byte[] getBinaryBytes(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)(i % 0x100);
        }
        return bytes;
    }

    @Test
    public void testGetTableCommandGetCommandSyntax() throws Exception {

        final String expectedResult = GetCommand.GetTableCommand.COMMAND_SYNTAX;
        assertEquals(expectedResult,
            new GetCommand.GetTableCommand().getCommandSyntax());
    }

    @Test
    public void testGetTableCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult =
            GetCommand.GetTableCommand.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new GetCommand.GetTableCommand().getCommandDescription());
    }

    @Test
    public void testGetTableCommandExecuteBadArgs()
        throws Exception {

        CommandShell shell = connectToStore();
        final GetCommand.GetTableCommand subObj =
            new GetCommand.GetTableCommand();

        /* Check unknown argument. */
        String[] cmds = new String[]{GetCommand.GetTableCommand.COMMAND_NAME};
        doExecuteUnknownArg(subObj, cmds, shell);

        /* Check flag requires an argument. */
        String[] flagRequiredArgs = new String[] {
            GetCommand.GetTableCommand.TABLE_FLAG,
            GetCommand.GetTableCommand.INDEX_FLAG,
            GetCommand.GetTableCommand.FIELD_FLAG,
            GetCommand.GetTableCommand.ANCESTOR_FLAG,
            GetCommand.GetTableCommand.CHILD_FLAG,
            GetCommand.JSON_FLAG,
            GetCommand.FILE_FLAG
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.FIELD_FLAG, "field1"
        };
        flagRequiredArgs = new String[] {
            GetCommand.GetTableCommand.VALUE_FLAG,
            GetCommand.START_FLAG,
            GetCommand.END_FLAG,
        };
        doExecuteFlagRequiredArg(subObj, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        HashMap<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(GetCommand.GetTableCommand.TABLE_FLAG, "users");
        cmds = new String[]{GetCommand.GetTableCommand.COMMAND_NAME};
        String[] requiredArgs = new String[] {
            GetCommand.GetTableCommand.TABLE_FLAG
        };
        /* Missing -table */
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);

        /* -field f1 but missing -value */
        argsMap.put(GetCommand.GetTableCommand.FIELD_FLAG, "f1");
        argsMap.put(GetCommand.GetTableCommand.VALUE_FLAG, "v1");
        argsMap.put(GetCommand.START_FLAG, "s2");
        argsMap.put(GetCommand.END_FLAG, "e2");
        requiredArgs = new String[] {
            GetCommand.GetTableCommand.VALUE_FLAG,
            GetCommand.START_FLAG,
            GetCommand.END_FLAG,
        };
        doExecuteRequiredArgs(subObj, cmds, argsMap, requiredArgs,
            requiredArgs[0] + " or " + requiredArgs[1] +
            " | " + requiredArgs[2], shell);
    }

    @Test
    public void testGetTableCommandExecuteHelp()
        throws Exception {

        final GetCommand.GetTableCommand subObj =
            new GetCommand.GetTableCommand();
        final String[] cmds =
            new String[]{GetCommand.GetTableCommand.COMMAND_NAME};

        CommandShell shell = connectToStore();
        doExecuteHelp(subObj, cmds, shell);
    }

    private void getTableCommandExecuteTest(String namespace)
        throws Exception {

        final String tableName = "users";
        final String nsTableName =
            NameUtils.makeQualifiedName(namespace, "users");
        //TODO: allow MRTable mode after MRTable supports child tables.
        final TableImpl user = addUserTable(namespace, tableName,
                                            true/*noMRTableMode*/);
        final String indexName = "idxName";
        addIndex(user, indexName, new String[]{"name"}, true);
        final TableImpl email = addEmailTable(namespace, "email", user);

        loadUsersTable(30, user);
        loadUserEmail(user, email);

        CommandShell shell = connectToStore();
        /*
         * Disable the table cache the shell is using so that it can keep
         * up with the test's metadata changes.
         */
        ((TableAPIImpl)shell.getStore().getTableAPI()).setCacheEnabled(false);
        final GetCommand.GetTableCommand subObj =
            new GetCommand.GetTableCommand();

        /**
         * Case0: get table -name TableNotExists
         */
        String[] cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG,
            NameUtils.makeQualifiedName(namespace, "TableNotExists"),
        };
        String expectedMsg = "TableNotExists";
        doExecuteShellException(subObj, cmds, expectedMsg, shell);

        /**
         * Case1: get by a not-existed primary key.
         *  get table -name [ns:]users -field group -value groupNotExists
         *  -field user -value userNotExists
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "groupNotExists",
            GetCommand.GetTableCommand.FIELD_FLAG, "user",
            GetCommand.GetTableCommand.VALUE_FLAG, "userNotExists",
        };
        expectedMsg = "Key not found in store";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case2: get by a primary key
         *  get table -name [ns:]users -field group -value group0
         *  -field user -value user0
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "group0",
            GetCommand.GetTableCommand.FIELD_FLAG, "user",
            GetCommand.GetTableCommand.VALUE_FLAG, "user0",
        };
        doExecuteReturnNull(subObj, cmds, shell);

        /**
         * Case3: get by a primary key with -keyonly.
         *  get table -name [ns:]users -field group -value group0
         *  -field user -value user0 -keyonly
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "group0",
            GetCommand.GetTableCommand.FIELD_FLAG, "user",
            GetCommand.GetTableCommand.VALUE_FLAG, "user0",
            GetCommand.KEY_ONLY_FLAG,
        };
        doExecuteReturnNull(subObj, cmds, shell);

        /**
         * Case4: get by a primary key with full major path.
         *  get table -name [ns:]users -field group -value group0
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "group0"
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case5: get by a primary key with full major path.
         *  get table -name [ns:]users -field group -value group0
         *  -field user -start user0 -end user4
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "group0",
            GetCommand.GetTableCommand.FIELD_FLAG, "user",
            GetCommand.START_FLAG, "user0",
            GetCommand.END_FLAG, "user4",
        };
        expectedMsg = "5 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case6: get by a primary key with full major path, return keys only.
         *  get table -name [ns:]users -field group -value group0
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.GetTableCommand.VALUE_FLAG, "group0",
            GetCommand.KEY_ONLY_FLAG
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case7: get by a primary key with a field range
         *  get table -name [ns:]users -field group -start group0 -end group1
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
            GetCommand.GetTableCommand.FIELD_FLAG, "group",
            GetCommand.START_FLAG, "group0",
            GetCommand.END_FLAG, "group1"
        };
        expectedMsg = "20 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case8: get by a index key
        *  get table -name [ns:]users -index idxName -field name -value name10
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.GetTableCommand.INDEX_FLAG, "idxName",
           GetCommand.GetTableCommand.FIELD_FLAG, "name",
           GetCommand.GetTableCommand.VALUE_FLAG, "name10"
       };
       expectedMsg = "1 row returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case9: get by a index key, return keys only.
        *  get table -name [ns:]users -index idxName
        *  -field name -value name10 -keyonly
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.GetTableCommand.INDEX_FLAG, "idxName",
           GetCommand.GetTableCommand.FIELD_FLAG, "name",
           GetCommand.GetTableCommand.VALUE_FLAG, "name10",
           GetCommand.KEY_ONLY_FLAG
       };
       expectedMsg = "1 row returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case10: get by a index key with a range.
        *  get table -name [ns:]users -index idxName
        *  -field name -start name10 -end name19
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.GetTableCommand.INDEX_FLAG, "idxName",
           GetCommand.GetTableCommand.FIELD_FLAG, "name",
           GetCommand.START_FLAG, "name10",
           GetCommand.END_FLAG, "name19"
       };
       expectedMsg = "10 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case11: full scan table, write the resultset to a output file.
        *  get table -name [ns:]users -file <output>
        */
       String output = getTempFileName("getTable", ".out");
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.FILE_FLAG, output
       };
       expectedMsg = "30 rows returned from " + tableName + " table" +
                     Shell.eol + "Wrote result to file " + output;
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case12: table scan with a child table
        *  get table -name [ns:]users -child users.email -field group
        *  -value group0 -field user -value user9
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.GetTableCommand.CHILD_FLAG, email.getFullName(),
           GetCommand.GetTableCommand.FIELD_FLAG, "group",
           GetCommand.GetTableCommand.VALUE_FLAG, "group0",
           GetCommand.GetTableCommand.FIELD_FLAG, "user",
           GetCommand.GetTableCommand.VALUE_FLAG, "user9",
       };
       expectedMsg = "3 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case12-B: table scan with a child table
        *  get table -name [ns:]users -child [ns:]users.email -field group
        *  -value group0 -field user -value user9
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, nsTableName,
           GetCommand.GetTableCommand.CHILD_FLAG, email.getFullNamespaceName(),
           GetCommand.GetTableCommand.FIELD_FLAG, "group",
           GetCommand.GetTableCommand.VALUE_FLAG, "group0",
           GetCommand.GetTableCommand.FIELD_FLAG, "user",
           GetCommand.GetTableCommand.VALUE_FLAG, "user9",
       };
       expectedMsg = "3 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case13: table scan with a parent table
        *  get table -name [ns:]users.email -ancestor users
        *  -field group -value group1 -field users -value user0
        *  -field eid -end 1
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, email.getFullNamespaceName(),
           GetCommand.GetTableCommand.ANCESTOR_FLAG, user.getFullName(),
           GetCommand.GetTableCommand.FIELD_FLAG, "group",
           GetCommand.GetTableCommand.VALUE_FLAG, "group1",
           GetCommand.GetTableCommand.FIELD_FLAG, "user",
           GetCommand.GetTableCommand.VALUE_FLAG, "user0",
           GetCommand.GetTableCommand.FIELD_FLAG, "eid",
           GetCommand.END_FLAG, "1",
       };
       expectedMsg = "2 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case13-B: table scan with a parent table
        *  get table -name [ns:]users.email -ancestor [ns:]users
        *  -field group -value group1 -field users -value user0
        *  -field eid -end 1
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, email.getFullNamespaceName(),
           GetCommand.GetTableCommand.ANCESTOR_FLAG,
           user.getFullNamespaceName(),
           GetCommand.GetTableCommand.FIELD_FLAG, "group",
           GetCommand.GetTableCommand.VALUE_FLAG, "group1",
           GetCommand.GetTableCommand.FIELD_FLAG, "user",
           GetCommand.GetTableCommand.VALUE_FLAG, "user0",
           GetCommand.GetTableCommand.FIELD_FLAG, "eid",
           GetCommand.END_FLAG, "1",
       };
       expectedMsg = "2 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case14: get by primary key using -json
        *  get table -name [ns:]users -json '{"group":"group1"}'
        *  -field user -start user4 -pretty
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
           GetCommand.JSON_FLAG, "{\"group\":\"group1\"}",
           GetCommand.GetTableCommand.FIELD_FLAG, "user",
           GetCommand.START_FLAG, "user5",
           GetCommand.GetTableCommand.PRETTY_FLAG
       };
       expectedMsg = "5 rows returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /**
        * Case15: table scan with -report-size
        *  get table -name [ns:]users -report-size
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
           GetCommand.GetTableCommand.REPORT_SIZE_FLAG
       };
       String[] expectedMsgs = new String[] {
           "Number of records: 30",
           "Primary Key sizes:",
           "Data sizes:",
           "Index Key sizes of idxName:"
       };
       doExecuteCheckRet(subObj, cmds, expectedMsgs, true, shell);

       /**
        * Case16: table scan with -report-size -keyonly
        *  get table -name [ns:]users -report-size -keyonly
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
           GetCommand.GetTableCommand.REPORT_SIZE_FLAG,
           GetCommand.KEY_ONLY_FLAG
       };
       expectedMsgs = new String[] {
           "Number of records: 30",
           "Primary Key sizes:",
           "Data sizes: Not available"
       };
       doExecuteCheckRet(subObj, cmds, expectedMsgs, true, shell);

       /**
        * Case17: index scan with -report-size
        *  get table -name [ns:]users -index idxName -report-size
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
           GetCommand.GetTableCommand.INDEX_FLAG, "idxName",
           GetCommand.GetTableCommand.REPORT_SIZE_FLAG,
       };
       expectedMsgs = new String[] {
           "Number of records: 30",
           "Primary Key sizes:",
           "Data sizes:",
           "Index Key sizes of idxName:"
       };
       doExecuteCheckRet(subObj, cmds, expectedMsgs, true, shell);

       /**
        * Case18: index scan with -report-size
        *  get table -name [ns:]users -index idxName -report-size -keyonly
        */
       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, user.getFullNamespaceName(),
           GetCommand.GetTableCommand.INDEX_FLAG, "idxName",
           GetCommand.GetTableCommand.REPORT_SIZE_FLAG,
           GetCommand.KEY_ONLY_FLAG
       };
       expectedMsgs = new String[] {
           "Number of records: 30",
           "Primary Key sizes:",
           "Index Key sizes:"
       };
       doExecuteCheckRet(subObj, cmds, expectedMsgs, true, shell);

       /**
        * Case19:
        * get table -name [ns:]users -index idxName -field name -null-value
        */
       addIndex(email, "idxEmail", new String[]{"email"}, true);

       Row row = email.createRow();
       row.put("group", "group100");
       row.put("user", "user100");
       row.put("eid", 1);
       tableImpl.put(row, null, null);

       cmds = new String[] {
           GetCommand.GetTableCommand.COMMAND_NAME,
           GetCommand.GetTableCommand.TABLE_FLAG, email.getFullNamespaceName(),
           GetCommand.GetTableCommand.INDEX_FLAG, "idxEmail",
           GetCommand.GetTableCommand.FIELD_FLAG, "email",
           GetCommand.GetTableCommand.NULL_VALUE_FLAG
       };
       expectedMsg = "1 row returned";
       doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

       /* clean up */
       removeTable(namespace, email.getFullName(), true, commandService);
       removeTable(namespace, user.getFullName(), true, commandService);
    }

    @Test
    public void testGetTableCommandExecute()
        throws Exception {

        /* tables without namespace */
        getTableCommandExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        getTableCommandExecuteTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        getTableCommandExecuteTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    @Test
    public void testGetTableWithIndexOnComplexFieldExecute()
        throws Exception {

        final TableImpl contact = addContactTable();

        /* idx_email idx_phone idx_address_name*/
        loadContacts(contact, 100);

        CommandShell shell = connectToStore();
        final GetCommand.GetTableCommand subObj =
            new GetCommand.GetTableCommand();

        /**
         * Case0: get table -name contact -index idx_email
         *            -field email[] -null-value
         */
        String[] cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_email",
            GetCommand.GetTableCommand.FIELD_FLAG, "email[]",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
        };
        String expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case1: get table -name contact -index idx_email
         *            -field email[] -value email0@0
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_email",
            GetCommand.GetTableCommand.FIELD_FLAG, "email[]",
            GetCommand.GetTableCommand.VALUE_FLAG, "email0@0"
        };
        expectedMsg = "1 row returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case2: get table -name contact -index idx_email
         *            -field email[] -start email0 -end email2
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_email",
            GetCommand.GetTableCommand.FIELD_FLAG, "email[]",
            GetCommand.START_FLAG, "email0",
            GetCommand.END_FLAG, "email2",
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case3: get table -name contact -index idx_phone
         *            -field phone.keys() -value home
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_phone",
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.keys()",
            GetCommand.GetTableCommand.VALUE_FLAG, "home"
        };
        expectedMsg = "90 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case4: get table -name contact -index idx_phone
         *            -field phone.keys() -value home
         *            -field phone.values() -start phone90
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_phone",
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.keys()",
            GetCommand.GetTableCommand.VALUE_FLAG, "home",
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.values()",
            GetCommand.START_FLAG, "phone90"
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case5: get table -name contact -index idx_phone
         *            -field phone.keys() -null-value
         *            -field phone.values() -null-value
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_phone",
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.keys()",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.values()",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case6: get table -name contact -index idx_phone
         *            -field phone.keys() -null-value
         *            -field phone.values() -null-value
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_phone",
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.keys()",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
            GetCommand.GetTableCommand.FIELD_FLAG, "phone.values()",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case7: get table -name contact -index idx_address_city_name
         *            -field address.city -null-value
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_address_city_name",
            GetCommand.GetTableCommand.FIELD_FLAG, "address.city",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG
        };
        expectedMsg = "10 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case8: get table -name contact -index idx_address_city_name
         *            -field address.city -null-value
         *            -field name -start name5
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_address_city_name",
            GetCommand.GetTableCommand.FIELD_FLAG, "address.city",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
            GetCommand.GetTableCommand.FIELD_FLAG, "name",
            GetCommand.START_FLAG, "name5",
        };
        expectedMsg = "5 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case9: get table -name contact -index idx_address_city_name
         *            -field address.city -null-value
         *            -field name -start name5
         *            -field name -start name8
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_address_city_name",
            GetCommand.GetTableCommand.FIELD_FLAG, "address.city",
            GetCommand.GetTableCommand.NULL_VALUE_FLAG,
            GetCommand.GetTableCommand.FIELD_FLAG, "name",
            GetCommand.START_FLAG, "name5",
            GetCommand.END_FLAG, "name8",
        };
        expectedMsg = "3 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);

        /**
         * Case10: get table -name contact -index idx_address_city_name
         *            -field address.city -value city8
         *            -field name -start name5
         */
        cmds = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, "contacts",
            GetCommand.GetTableCommand.INDEX_FLAG, "idx_address_city_name",
            GetCommand.GetTableCommand.FIELD_FLAG, "address.city",
            GetCommand.GetTableCommand.VALUE_FLAG, "city8",
            GetCommand.GetTableCommand.FIELD_FLAG, "name",
            GetCommand.START_FLAG, "name5"
        };
        expectedMsg = "6 rows returned";
        doExecuteCheckRet(subObj, cmds, expectedMsg, shell);
    }

    @Test
    public void testGetTableReportSizeExecute()
        throws Exception {

        final CommandShell shell = connectToStore();

        String[] statements = new String[] {
            "create table trec(i integer, r record(rs string, ri integer), " +
                               "primary key(i))",
            "create index idx1 on trec(r.rs)",
            "create index idx2 on trec(r.rs, r.ri)",
            "create index idx3 on trec(r.rs, r.ri, i)",
        };
        String jsonRow = "{\"i\":1,\"r\":{\"rs\":\"test\",\"ri\":1}}";
        doExecuteReportSizeCheckRet(shell, "trec", statements, jsonRow,
                                    1, 11f, new Float[]{6f, 8f, 9f});

        statements = new String[] {
            "create table tarray(i integer, a array(string), primary key(i))",
            "create index idx1 on tarray(a[])"
        };
        jsonRow = "{\"i\":1,\"a\":[\"string1\",\"string2\",\"string3\"]}";
        doExecuteReportSizeCheckRet(shell, "tarray", statements, jsonRow,
                                    1, 29f, new Float[]{27f});

        statements = new String[] {
            "create table tarray_rec(i integer, " +
                "a array(record(rs string, ri integer)), primary key(i))",
            "create index idx1 on tarray_rec(a[].rs)",
            "create index idx2 on tarray_rec(a[].rs, a[].ri, i)"
        };
        jsonRow = "{\"i\":1,\"a\":[{\"rs\":\"test\",\"ri\":1}," +
            "{\"rs\":\"test1\",\"ri\":2}]}";
        doExecuteReportSizeCheckRet(shell, "tarray_rec", statements, jsonRow,
                                    1, 22f, new Float[]{13f, 19f});

        statements = new String[] {
            "create table if not exists tarray_rec_rec(" +
                "i integer, " +
                "a array(record(rs string, " +
                "               ri integer, " +
                "               rec record(rs string, ri integer))), " +
                "primary key(i))",
            "create index if not exists idx1 on " +
                "tarray_rec_rec(a[].rec.rs, a[].rec.ri, i)",
            "create index if not exists idx2 on " +
                "tarray_rec_rec(a[].rs, a[].ri, a[].rec.rs, a[].rec.ri, i)"
        };
        jsonRow = "{\"i\":1,\"a\":[{\"rs\":\"test\",\"ri\":1,\"rec\":" +
                      "{\"rs\":\"rec-test-2\",\"ri\":1}}," +
                      "{\"rs\":\"test1\",\"ri\":2,\"rec\":" +
                      "{\"rs\":\"rec-test-2\",\"ri\":1}}]}";
        doExecuteReportSizeCheckRet(shell, "tarray_rec_rec", statements, jsonRow,
                                    1, 52f, new Float[]{30f, 47f});

        jsonRow = "{\"i\":2,\"a\":[{\"rs\":\"test\",\"ri\":1,\"rec\":null}," +
                    "{\"rs\":\"test1\",\"ri\":2,\"rec\":null}]}";
        doExecuteReportSizeCheckRet(shell, "tarray_rec_rec", statements, jsonRow,
                                    2, 38f, new Float[]{18f, 35f});
        statements = new String[] {
            "create table if not exists tmap(i integer, m map(string), " +
                                             "primary key(i))",
            "create index if not exists idx1 on tmap(m.keys())",
            "create index if not exists idx2 on tmap(m.values())",
            "create index if not exists idx3 on tmap(m.key1)",
            "create index if not exists idx4 on tmap(m.keys(), m.values())"
        };
        jsonRow =
            "{\"i\":1,\"m\":{\"key1\":\"1\",\"key2\":\"2\",\"key3\":\"3\"}}";
        doExecuteReportSizeCheckRet(shell, "tmap", statements, jsonRow,
                                    1, 26f, new Float[]{18f, 9f, 3f, 27f});

        statements = new String[] {
            "create table if not exists tmap(i integer, m map(string), " +
                                             "primary key(i))",
            "create index if not exists idx1 on tmap(m.keys())",
            "create index if not exists idx2 on tmap(m.values())",
            "create index if not exists idx3 on tmap(m.key1)",
            "create index if not exists idx4 on tmap(m.keys(), m.values())"
        };
        jsonRow = "{\"i\":2,\"m\":null}";
        doExecuteReportSizeCheckRet(shell, "tmap", statements, jsonRow, 2,
                                    14.5f, new Float[]{9.5f, 5f, 2f, 14.5f});

        statements = new String[]{
            "create table tmap_array(i integer, m map(array(integer)), " +
                                     "primary key(i))",
            "create index idx1 on tmap_array(m.keys())"
        };
        jsonRow =
            "{\"i\":1,\"m\":{\"key1\":[1,2],\"key2\":[3,4],\"key3\":[5,6]}}";
        doExecuteReportSizeCheckRet(shell, "tmap_array", statements, jsonRow,
                                    1, 32f, new Float[]{18f});

        statements = new String[] {
            "create table tmap_rec(i integer, " +
                                  "m map(record(ri integer, rs string)), " +
                                  "primary key(i))",
            "create index idx1 on tmap_rec(m.values().ri, m.values().rs)",
            "create index idx2 on tmap_rec(m.keys(), m.values().ri, " +
                                          "m.values().rs)",
            "create index idx3 on tmap_rec(m.keys(), m.values().ri, " +
                                          "m.values().rs, i)"
        };
        jsonRow = "{\"i\":1,\"m\":{" +
                        "\"key1\":{\"rs\":\"rec-test-1\",\"ri\":1}," +
                        "\"key2\":{\"rs\":\"rec-test-2\",\"ri\":2}," +
                        "\"key3\":{\"rs\":\"rec-test-3\",\"ri\":3}}}}";
        doExecuteReportSizeCheckRet(shell, "tmap_rec", statements, jsonRow,
                                    1, 62f, new Float[]{42f, 60f, 63f});

        statements = new String[] {
            "create table tmap_rec_rec(" +
                "i integer, " +
                "m map(record(ri integer, rs string, " +
                              "rec record(ri integer, rs string))), " +
                "primary key(i))",
            "create index idx1 on tmap_rec_rec(m.values().rec.ri, " +
                                              "m.values().rec.rs)",
            "create index idx2 on tmap_rec_rec(m.keys(), " +
                                               "m.values().rec.ri, " +
                                               "m.values().rec.rs)",
            "create index idx3 on tmap_rec_rec(m.keys(), " +
                                              "m.values().ri, " +
                                              "m.values().rs, " +
                                              "m.values().rec.ri, " +
                                              "m.values().rec.rs, " +
                                              "i)"
        };
        jsonRow = "{\"i\":1,\"m\":{" +
                "\"key1\":{\"rs\":\"rec-test-1\",\"ri\":1," +
                          "\"rec\":{\"rs\":\"rec-rec-test-1\",\"ri\":1}}," +
                "\"key2\":{\"rs\":\"rec-test-2\",\"ri\":2," +
                          "\"rec\":{\"rs\":\"rec-rec-test-2\",\"ri\":2}}," +
                "\"key3\":{\"rs\":\"rec-test-3\",\"ri\":3," +
                          "\"rec\":{\"rs\":\"rec-rec-test-3\",\"ri\":3}}}}";
        doExecuteReportSizeCheckRet(shell, "tmap_rec_rec", statements, jsonRow,
                                    1, 119f, new Float[]{54f, 72f, 117f});
    }

    @SuppressWarnings("null")
    private void doExecuteReportSizeCheckRet(CommandShell shell,
                                             String tableName,
                                             String[] statements,
                                             String jsonRow,
                                             int pkColValue,
                                             float dataSize,
                                             Float[] indexSizes)
        throws Exception {

        assert(statements.length > 0);

        final String[] args = new String[] {
            GetCommand.GetTableCommand.COMMAND_NAME,
            GetCommand.GetTableCommand.TABLE_FLAG, tableName,
            GetCommand.GetTableCommand.REPORT_SIZE_FLAG
        };

        final GetCommand.GetTableCommand subObj =
            new GetCommand.GetTableCommand();

        for (String statement : statements) {
            store.executeSync(statement);
        }

        final TableAPI tableAPI = store.getTableAPI();
        final Table table = tableAPI.getTable(tableName);
        if (table == null) {
            fail("Table not found: " + tableName);
        }

        final Row row = table.createRowFromJson(jsonRow, true);
        if (row == null) {
            fail("Failed to create row from json: " + jsonRow);
        }

        final Version ver = tableAPI.put(row, null, null);
        if (ver == null) {
            fail("Failed to insert row to table" + row.toJsonString(false));
        }

        final String tableIdStr = ((TableImpl) table).getIdString();
        final int tableIdByteLen =
            UtfOps.getByteLength(tableIdStr.toCharArray());
        final int pkColByteLen = UtfOps.getByteLength(
            SortableString.toSortable(pkColValue).toCharArray());
        /**
         * Table rows are mapped to key/value pairs in a way that the major
         * components of the key consist of the table id and the primary key
         * value. The primary key size is calculated based on the serialized
         * byte array of the table id and primary key value.
         */
        final float primaryKeySize =
            tableIdByteLen +
            1 /* Key.BINARY_COMP_DELIM, which is a NUL char */ +
            pkColByteLen;

        final String[] expectedMsgs = new String[indexSizes.length + 2];
        final String fmt = "Average size: %.01f";
        int i = 0;
        expectedMsgs[i++] = String.format(fmt, primaryKeySize);
        expectedMsgs[i++] = String.format(fmt, dataSize);
        if (indexSizes.length > 0) {
            for (Float size: indexSizes) {
                expectedMsgs[i++] = String.format(fmt, size);
            }
        }
        doExecuteCheckRet(subObj, args, expectedMsgs, true, shell);
    }
}
