/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static oracle.kv.impl.admin.client.TableSizeCommand.COMMAND_NAME;
import static oracle.kv.impl.admin.client.TableSizeCommand.INDEX_FLAG;
import static oracle.kv.impl.admin.client.TableSizeCommand.JSON_FLAG;
import static oracle.kv.impl.admin.client.TableSizeCommand.KEY_PREFIX_FLAG;
import static oracle.kv.impl.admin.client.TableSizeCommand.NROWS_FLAG;
import static oracle.kv.impl.admin.client.TableSizeCommand.TABLE_NAME_FLAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.DDLTestUtils;
import oracle.kv.util.TableTestUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Verifies the functionality and error paths of the TableSizeCommand class.
 */
public final class TableSizeCommandTest extends TestBase {

    private static CreateStore createStore;
    private static KVStore store;
    private static CommandServiceAPI cs;
    private static TableAPI tableImpl;
    private static final int startPort = 8000;

    private static final int ERR_TABLE_NOT_EXISTS = 0;
    private static final int ERR_PARSE_JSON_FAILED = 1;

    @BeforeClass
    public static void staticSetup()
        throws Exception {

        Assume.assumeFalse("Test needs to create region", mrTableMode);

        TestUtils.clearTestDirectory();
        createStore = new CreateStore(
            "kvtest-" + TableSizeCommandTest.class.getName(),
            startPort, 1, 1, 10, 1);
        createStore.start();
        store = KVStoreFactory.getStore(createStore.createKVConfig());
        mrTableSetUp(store);
        cs = createStore.getAdmin();
        tableImpl = store.getTableAPI();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (store != null) {
            store.close();
        }
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Override
    public void setUp()
        throws Exception {

        removeAllTables();
    }

    @Override
    public void tearDown()
        throws Exception {

        removeAllTables();
    }

    @Test
    public void testTableSizeGetCommandSyntax()
        throws Exception {

        final TableSizeCommand cmd = new TableSizeCommand();
        final String expectedResult = TableSizeCommand.COMMAND_SYNTAX;
        assertEquals(expectedResult, cmd.getCommandSyntax());
    }

    @Test
    public void testTableSizeGetCommandDescription()
        throws Exception {

        final TableSizeCommand cmd = new TableSizeCommand();
        final String expectedResult = TableSizeCommand.COMMAND_DESCRIPTION;
        assertEquals(expectedResult, cmd.getCommandDescription());
    }

    @Test
    public void testTableSizeExecuteBadArgs()
        throws Exception {

        final TableSizeCommand cmd = new TableSizeCommand();

        /* unknown arg */
        doExecuteUnknownArg(cmd, COMMAND_NAME);

        /* flag required argument */
        final String[] flagRequiredArgs = {
            TABLE_NAME_FLAG,
            JSON_FLAG,
            NROWS_FLAG,
            INDEX_FLAG
        };
        doExecuteFlagRequiredArg(cmd, COMMAND_NAME, flagRequiredArgs);

        /* required argument */
        String jsonString = "{\"username\":\"jack\",\"age\":50,\"id\":\"1\"}";
        String[] requiredArgs = {
            TABLE_NAME_FLAG,
            JSON_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TABLE_NAME_FLAG, "user.email");
        argsMap.put(JSON_FLAG, jsonString);
        argsMap.put(NROWS_FLAG, "1000");
        doExecuteRequiredArgs(cmd, COMMAND_NAME, argsMap, requiredArgs,
                              requiredArgs[0]);

        requiredArgs = new String[] {JSON_FLAG};
        doExecuteRequiredArgs(cmd, COMMAND_NAME, argsMap, requiredArgs,
                              requiredArgs[0]);
    }

    private void tableSizeExecuteTest(String namespace)
        throws Exception {

        final CommandShell shell = connectToStore();
        ShellCommand cmd = new TableSizeCommand();
        String jsonString = "{\"name\":\"jack\",\"age\":50,\"id\":1}";

        /* kv-> table-size -name [ns:]TABLE_NOT_EXISTS -json ...*/
        String[] cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG,
            NameUtils.makeQualifiedName(namespace, "TABLE_NOT_EXISTS"),
            JSON_FLAG, jsonString,
        };
        execShellCommand(shell, cmd, cmdArgs, ERR_TABLE_NOT_EXISTS);

        /* create table "user" */
        final String tableName = "user";
        final String nsTableName =
            NameUtils.makeQualifiedName(namespace, tableName);
        final String index1Name = "idx1";
        final String index2Name = "idx2";
        /* Exercise the createTableBuilder() variants w/ and w/o namespace */
        TableBuilder tb = namespace == null ?
            TableBuilder.createTableBuilder(tableName) :
            TableBuilder.createTableBuilder(namespace, tableName);
        tb.addInteger("id")
          .addString("name")
          .addInteger("age")
          .primaryKey("id");
        TableImpl table = addTable(tb, true);

        /* kv-> table-size -name [ns:]user -json INVALID_JSON */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, "INVALID_JSON",
        };
        execShellCommand(shell, cmd, cmdArgs, ERR_PARSE_JSON_FAILED);

        /* kv-> table-size -name [ns:]user -json <jsonString> */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
        };
        execShellCommand(shell, cmd, cmdArgs);

        /*
         * kv-> table-size -name [ns:]user -json <jsonString>
         * -cachesize -rows 1000
         */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
            NROWS_FLAG, "1000"
        };
        execShellCommand(shell, cmd, cmdArgs);

        /* Add a index index1*/
        addIndex(table, index1Name, new String[]{"name"}, true);

        /*
         * kv-> table-size -name [ns:]user -json <jsonString>
         * -cachesize -rows 1000
         */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
            NROWS_FLAG, "1000"
        };
        execShellCommand(shell, cmd, cmdArgs);

        /* Add a index index2 */
        addIndex(table, index2Name, new String[]{"name", "age"}, true);

        /*
         * kv-> table-size -name [ns:]user -json <jsonString> -cachesize
         * -rows 1000
         */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
            NROWS_FLAG, "1000"
        };
        execShellCommand(shell, cmd, cmdArgs);

        /**
         * kv-> table-size -name [ns:]user -json <jsonString> -cachesize
         *      -rows 1000 -index index1Name -keyprefix 2
         */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
            NROWS_FLAG, "1000",
            INDEX_FLAG, index1Name,
            KEY_PREFIX_FLAG, "2",
        };
        execShellCommand(shell, cmd, cmdArgs);

        /**
         * kv-> table-size -name [ns:]user -json <jsonString> -cachesize
         *      -rows 1000 -index index1Name -keyprefix 2 -index index2Name
         *      -keyprefix 3
         */
        cmdArgs = new String[]{
            COMMAND_NAME,
            TABLE_NAME_FLAG, nsTableName,
            JSON_FLAG, jsonString,
            NROWS_FLAG, "1000",
            INDEX_FLAG, index1Name,
            KEY_PREFIX_FLAG, "2",
            INDEX_FLAG, index2Name,
            KEY_PREFIX_FLAG, "3",
        };
        execShellCommand(shell, cmd, cmdArgs);

        /* clean up */
        removeTable(namespace, table.getFullName(), true);
    }

    @Test
    public void testTableSizeExecute() throws Exception {
        /* tables without namespace */
        tableSizeExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        DDLTestUtils.execStatement(store, "CREATE NAMESPACE " + ns);
        tableSizeExecuteTest(ns);
        DDLTestUtils.execStatement(store, "DROP NAMESPACE " + ns + " CASCADE");

        /* tables in system default namespace */
        tableSizeExecuteTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    @Test
    public void testTableSizeOnMultiKeyIndexExecute()
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
        doExecuteCheckRet(shell, "trec", statements, jsonRow,
                          8, 11, new Integer[]{6, 8, 9});

        statements = new String[] {
            "create table tarray(i integer, a array(string), primary key(i))",
            "create index idx1 on tarray(a[])"
        };
        jsonRow = "{\"i\":1,\"a\":[\"string1\",\"string2\",\"string3\"]}";
        doExecuteCheckRet(shell, "tarray", statements, jsonRow,
                          8, 29, new Integer[]{27});

        statements = new String[] {
            "create table tarray_rec(i integer, " +
                "a array(record(rs string, ri integer)), primary key(i))",
            "create index idx1 on tarray_rec(a[].rs)",
            "create index idx2 on tarray_rec(a[].rs, a[].ri, i)"
        };
        jsonRow = "{\"i\":1,\"a\":[{\"rs\":\"test0\",\"ri\":1}," +
            "{\"rs\":\"test1\",\"ri\":2}]}";
        doExecuteCheckRet(shell, "tarray_rec", statements, jsonRow,
                          8, 23, new Integer[]{14, 20});

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
        jsonRow = "{ \"i\":1," +
                  "  \"a\":[{\"rs\":\"test0\"," +
                  "          \"ri\":1," +
                  "          \"rec\": {\"rs\":\"rec-test-2\",\"ri\":1}" +
                  "         }," +
                  "         {\"rs\":\"test1\"," +
                  "          \"ri\":2," +
                  "          \"rec\": {\"rs\":\"rec-test-2\",\"ri\":1}" +
                  "         }]}";
        doExecuteCheckRet(shell, "tarray_rec_rec", statements, jsonRow,
                          8, 53, new Integer[]{30, 48});

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
        doExecuteCheckRet(shell, "tmap", statements, jsonRow,
                          8, 26, new Integer[]{18, 9, 3, 27});

        statements = new String[]{
            "create table tmap_array(i integer, m map(array(integer)), " +
                                     "primary key(i))",
            "create index idx1 on tmap_array(m.keys())"
        };
        jsonRow =
            "{\"i\":1,\"m\":{\"key1\":[1,2],\"key2\":[3,4],\"key3\":[5,6]}}";
        doExecuteCheckRet(shell, "tmap_array", statements, jsonRow,
                          8, 32, new Integer[]{18});

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
        doExecuteCheckRet(shell, "tmap_rec", statements, jsonRow,
                          9, 62, new Integer[]{42, 60, 63});


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
        doExecuteCheckRet(shell, "tmap_rec_rec", statements, jsonRow,
                          9, 119, new Integer[]{54, 72, 117});
    }

    private void doExecuteCheckRet(CommandShell shell,
                                   String tableName,
                                   String[] statements,
                                   String jsonRow,
                                   int primaryKeySize,
                                   int dateSize,
                                   Integer[] indexSizes)
        throws Exception {

        assert(statements.length > 0);

        final String[] args = new String[] {
            TableSizeCommand.COMMAND_NAME,
            TableSizeCommand.TABLE_NAME_FLAG, tableName,
            TableSizeCommand.JSON_FLAG, jsonRow
        };

        final ShellCommand cmd = new TableSizeCommand();

        for (String statement : statements) {
            statement = addRegionsForMRTable(statement);
            store.executeSync(statement);
        }

        final String[] expectedMsgs = new String[indexSizes.length + 2];
        final String fmt = "%d";
        int i = 0;

        /*
         * Note: The primary key contains the table ID, so is subject to change
         * size if the ID changes. The IDs can change independent of the test
         * when system tables are added (for example).
         */
        expectedMsgs[i++] = String.format(fmt, primaryKeySize);
        expectedMsgs[i++] = String.format(fmt, dateSize);
        if (indexSizes.length > 0) {
            for (Integer size: indexSizes) {
                expectedMsgs[i++] = String.format(fmt, size);
            }
        }

        /* Run the test and verify the results. */
        try {
            String message = execShellCommand(shell, cmd, args, null);
            if (message != null) {
                for (String msg: expectedMsgs) {
                    assertTrue("The return message is expected to contain " +
                               "message: " + msg + ", but get :" + message,
                               message.indexOf(msg) >= 0);
                }
                return;
            }
            fail("The return message expected but null: " +
                Arrays.toString(expectedMsgs));
        } catch (ShellException e) {
            fail("Expected to OK but get exception: " + e);
        }
    }

    private CommandShell connectToStore()
        throws Exception {

        final CommandShell shell = new CommandShell(System.in, System.out);
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        return shell;
    }

    private void removeAllTables()
        throws Exception {

        removeTables(store.getTableAPI().getTables());
        removeAllData();
    }

    private void removeAllData() {
        Iterator<Key> iter = store.storeKeysIterator
            (Direction.UNORDERED, 10);
        try {
            while (iter.hasNext()) {
                store.delete(iter.next());
            }
        } catch (Exception e) {
            System.err.println("Exception cleaning store: " + e);
        }
    }

    private void removeTables(Map<String, Table> tables)
        throws Exception {

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getValue().getChildTables().size() > 0) {
                removeTables(entry.getValue().getChildTables());
            }
            TableImpl table = (TableImpl)entry.getValue();
            if (table.isSystemTable()) {
                continue;
            }

            for (Map.Entry<String, Index> index : table.getIndexes().entrySet()) {
                removeIndex(table, index.getKey(), true);
            }
            removeTable(table.getInternalNamespace(), table.getFullName(),
                true);
        }
    }

    private void removeTable(String namespace,
                             String tableName,
                             boolean shouldSucceed)
        throws Exception {

        TableTestUtils.removeTable(namespace, tableName, shouldSucceed, cs,
                                   store);
    }

    private TableImpl addTable(TableBuilder builder,
                               boolean shouldSucceed)
        throws Exception {

        TableTestUtils.addTable(builder, shouldSucceed,
                                false/*noMRTableMode*/, store);
        return getTable(builder.getNamespace(), builder.getName());
    }

    private TableImpl getTable(String namespace, String tableName) {
        return (TableImpl)tableImpl.getTable(namespace, tableName);
    }

    private void addIndex(TableImpl table, String indexName,
                          String[] indexFields, boolean shouldSucceed)
        throws Exception {

        TableTestUtils.addIndex(table, indexName, indexFields,
                                shouldSucceed, cs, store);
    }

    private void removeIndex(TableImpl table, String indexName,
                            boolean shouldSucceed)
        throws Exception {

        TableTestUtils.removeIndex(table.getInternalNamespace(),
                                   table.getFullName(),
                                   indexName,
                                   shouldSucceed, cs, store);
    }

    private void execShellCommand(final Shell shell,
                                  final ShellCommand shellCommand,
                                  final String[] args)
        throws Exception {

        execShellCommand(shell, shellCommand, args, null);
    }

    private String execShellCommand(final Shell shell,
                                    final ShellCommand shellCommand,
                                    final String[] args,
                                    final Integer iErrMsg)
        throws Exception {

        boolean expectedOK = true;
        if (iErrMsg != null) {
            expectedOK = false;
        }
        try {
            String message = shellCommand.execute(args, shell);
            if (!expectedOK) {
                fail("Expected to get ShellException but OK.");
            }
            return message;
        } catch (ShellException se) {
            if (expectedOK) {
                fail("Expected to be OK but get exception: " +
                     se.getMessage());
            }
        }
        return null;
    }

    private void doExecuteUnknownArg(final ShellCommand cmd,
                                     final String arg)
        throws Exception {

        doExecuteUnknownArg(cmd, new String[]{arg}, null);
    }

    private void doExecuteUnknownArg(final ShellCommand subObj,
                                     final String[] cmds,
                                     final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);

        String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        if (cmdShell == null) {
            cmdShell  = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String cmd,
                                          final String[] args)
        throws Exception {

        doExecuteFlagRequiredArg(subObj, new String[]{cmd}, args, null);
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String[] cmds,
                                          final String[] args,
                                          final Shell shell)
        throws Exception {

        for (String arg: args) {
            doExecuteFlagRequiredArg(subObj, cmds, arg, shell);
        }
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String[] cmds,
                                          final String arg,
                                          final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        if (cmdShell == null) {
            cmdShell = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private void doExecuteRequiredArgs(final ShellCommand subObj,
                                       final String cmd,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs,
                                       final String requiredArgName)
        throws Exception {

        doExecuteRequiredArgs(subObj, new String[]{cmd}, argsMap,
                              requiredArgs, requiredArgName, null);
    }

    private void doExecuteRequiredArgs(final ShellCommand subObj,
                                       final String[] cmds,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs,
                                       final String requiredArgName,
                                       final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Missing required argument (" + requiredArgName +
                ") for command: " + cmds[cmds.length - 1], subObj);

        final String[] args = getArgsArrayMinusRequiredArgs(cmds, argsMap,
                                                            requiredArgs);
        if (shell == null) {
            cmdShell = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private String[] getArgsArrayMinusRequiredArgs(
                         final String[] cmds,
                         final Map<String, String> argsMap,
                         final String[] requiredArgs) {

        final List<String> requiredArgsList =
            (requiredArgs == null)? null:Arrays.asList(requiredArgs);
        final List<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(cmds));
        for (Map.Entry<String, String> argsPair : argsMap.entrySet()) {
            final String argName = argsPair.getKey();
            final String argVal = argsPair.getValue();
            if (requiredArgsList != null &&
                requiredArgsList.contains(argName)) {
                continue;
            }
            if (argName.equals(argVal)) {
                argsList.add(argName);
                continue;
            }
            argsList.add(argName);
            argsList.add(argVal);
        }
        return argsList.toArray(new String[argsList.size()]);
    }
}
