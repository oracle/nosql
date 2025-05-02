/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.CreateStore;
import oracle.kv.util.DDLTestUtils;
import oracle.kv.util.TableTestUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellHelpException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for data CLI unit test. The @BeforeClass and @AfterClass methods
 * create and destroy a store respectively. @Before and @After methods removes
 * the existing records in the store.
 *
 *
 */

public class DataCLITestBase extends TestBase {
    private final static int PORT = 12000;

    private static CreateStore createStore = null;
    private static int createStoreCount = 0;
    private ByteArrayOutputStream buffer;
    private PrintStream out;

    static KVStore store = null;
    static TableAPI tableImpl = null;
    static CommandServiceAPI commandService = null;

    @BeforeClass
    public static void staticSetup()
        throws Exception {

        TestUtils.clearTestDirectory();
        startStore();

        KVStoreConfig config = createStore.createKVConfig();
        store = KVStoreFactory.getStore(config);
        mrTableSetUp(store);
        tableImpl = store.getTableAPI();
        commandService = createStore.getAdmin();
    }

    private static void startStore() {
        try {
            createStoreCount++;
            createStore = new CreateStore("kvtest-" +
                                          testClassName +
                                          "-" + createStoreCount,
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

        /*
         * Do not call superclass setUp methods, since it has been done in
         * staticSetUp method at class level.
         */
        removeExistingData();
        buffer = new ByteArrayOutputStream();
        out = new PrintStream(buffer);
    }

    @Override
    public void tearDown()
        throws Exception {

        /*
         * Do not call superclass tearDown methods, since it has been done in
         * staticTearDown method at class level.
         */
        removeExistingData();
    }

    void removeExistingData()
        throws Exception {

        TableTestUtils.removeTables(tableImpl.getTables(), commandService,
                                    store);
        removeKVs();
    }

    private void removeKVs() {
        final Iterator<Key> itr = store.storeKeysIterator(
            Direction.UNORDERED, 0);
        while (itr.hasNext()) {
            final Key key = itr.next();
            store.delete(key);
        }
    }

    CommandShell connectToStore()
        throws Exception {

        final CommandShell shell = new CommandShell(System.in, out);
        shell.openStore(createStore.getHostname(),
                        createStore.getRegistryPort(),
                        createStore.getStoreName(),
                        createStore.getDefaultUserName(),
                        createStore.getDefaultUserLoginPath());
        return shell;
    }

    void removeTable(String namespace,
                     String tableName,
                     boolean shouldSucceed,
                     CommandServiceAPI cs)
        throws Exception {

        TableTestUtils.removeTable(namespace, tableName, shouldSucceed, cs,
                                   store);
    }

    void addTable(TableBuilder builder,
                  boolean shouldSucceed,
                  boolean noMRTableMode)
        throws Exception {

        TableTestUtils.addTable(builder, shouldSucceed, noMRTableMode,
                                store);
    }

    void addTable(TableBuilder builder, boolean shouldSucceed)
        throws Exception {
        addTable(builder, shouldSucceed, false/*noMRTableMode*/);
    }

    TableImpl getTable(String tableName) {
        return (TableImpl) tableImpl.getTable(tableName);
    }

    TableImpl getTable(String namespace, String tableName) {
        return (TableImpl) tableImpl.getTable(namespace, tableName);
    }

    void addIndex(TableImpl table, String indexName,
                  String[] indexFields, boolean shouldSucceed)
        throws Exception {

        TableTestUtils.addIndex(table, indexName, indexFields,
                                shouldSucceed, commandService, store);
    }

    void createNamespace(String ns) throws Exception {
        DDLTestUtils.execStatement(store,
            "CREATE NAMESPACE IF NOT EXISTS " + ns);
    }

    void dropNamespace(String ns) throws Exception {
        DDLTestUtils.execStatement(store,
            "DROP NAMESPACE IF EXISTS " + ns + " CASCADE");
    }

    String getTempFileName(String prefix, String suffix) {
        File file = null;
        try {
            file = File.createTempFile(prefix, suffix);
            file.deleteOnExit();
            if (file.exists()) {
                file.delete();
            } else {
                file.createNewFile();
            }
            return file.getAbsolutePath();
        } catch (IOException e) {
            fail("Failed to create a temp file.");
        }
        return null;
    }

    String saveAsTempFile(String info) {
        File file = new File(getTempFileName("cliValue", ".txt"));
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write(info.getBytes());
        } catch (IOException e) {
            fail("Failed to write to file " + file.getAbsolutePath());
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ignored) {
                }
            }
        }
        return file.getAbsolutePath();
    }

    /**
     * Execute a command with a argument "UNKNOW_ARG", it is expected
     * to throw a ShellUsageException.
     */
    void doExecuteUnknownArg(final ShellCommand command,
                             final String[] cmds,
                             final Shell shell)
        throws Exception {

        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, command);

        final String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        /* Run the test and verify the results. */
        try {
            execute(command, args, shell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    /**
     * Execute a command with wrong argument count, it is expected
     * to throw a ShellUsageException.
     */
    void doExecuteBadArgCount(final ShellCommand command,
                              final String cmdName,
                              final String[] cmds,
                              final Shell shell)
        throws Exception {

        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + cmdName,
                command);

        String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;
        /* Run the test and verify the results. */
        try {
            execute(command, args, shell);
            fail("ShellUsageException expected, but correct number of args.");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    /**
     * Execute a command with missing required argument, it is expected
     * to throw a ShellUsageException.
     */
    void doExecuteRequiredArgs(final ShellCommand command,
                               final String[] cmds,
                               final Map<String, String> argsMap,
                               final String[] requiredArgs,
                               final String requiredArgName,
                               final Shell shell)
        throws Exception {

        final ShellUsageException expectedException =
            new ShellUsageException(
                "Missing required argument (" + requiredArgName +
                ") for command: " + cmds[cmds.length - 1], command);
        final String[] args =
            getArgsArrayMinusRequiredArgs(cmds, argsMap, requiredArgs);
        /* Run the test and verify the results. */
        try {
            execute(command, args, shell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private String[] getArgsArrayMinusRequiredArgs(final String[] cmds,
        final Map<String, String> argsMap, final String[] requiredArgs) {

        final List<String> requiredArgsList =
            (requiredArgs == null)? null : Arrays.asList(requiredArgs);
        final List<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(cmds));
        if (argsMap != null) {
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
        }
        return argsList.toArray(new String[argsList.size()]);
    }

    /**
     * Execute a command with missing required argument for a flag,
     * it is expected to throw a ShellUsageException.
     */
    void doExecuteFlagRequiredArg(final ShellCommand command,
                                  final String[] cmds,
                                  final String[] args,
                                  final Shell shell)
        throws Exception {

        for (String arg: args) {
            doExecuteFlagRequiredArg(command, cmds, arg, shell);
        }
    }

    private void doExecuteFlagRequiredArg(final ShellCommand command,
                                          final String[] cmds,
                                          final String arg,
                                          final Shell shell)
        throws Exception {

        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg +
                " requires an argument", command);
        final String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;
        /* Run the test and verify the results. */
        try {
            execute(command, args, shell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    /**
     * Execute a command with "help" argument, ShellHelpException is
     * expected to be thrown.
     */
    void doExecuteHelp(final ShellCommand command,
                       final String[] cmds,
                       final Shell shell)
        throws Exception {

        final ShellHelpException expectedException =
            new ShellHelpException(command);
        final String arg = "help";

        String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        /* Run the test and verify the results. */
        try {
            execute(command, args, shell);
            fail("ShellHelpException expected but OK.");
        } catch (ShellHelpException e) {
            assertEquals(expectedException.getHelpMessage(),
                e.getHelpMessage());
        }
    }

    /**
     * Execute a command with given arguments, ShellException is
     * expected to be thrown.
     */
    void doExecuteShellException(final ShellCommand command,
                                 final String[] cmds,
                                 final String message,
                                 final Shell shell)
        throws Exception {

        /* Run the test and verify the results. */
        try {
            execute(command, cmds, shell);
            fail("ShellException expected but OK");
        } catch (ShellException e) {
            assertTrue(("Expected message: " + e.getMessage() + " to " +
                        "contain string: " + message),
                       e.getMessage().contains(message));
        }
    }

    /**
     * Execute a command with given arguments, ShellArgumentException is
     * expected to be thrown.
     */
    void doExecuteShellArgumentException(final ShellCommand command,
                                         final String[] cmds,
                                         final Shell shell)
        throws Exception {

        try {
            execute(command, cmds, shell);
            fail("ShellException expected but OK");
        } catch (ShellArgumentException e) {
        }
    }

    /**
     * Execute a command with given arguments, it is expected to OK and
     * return null.
     */
    void doExecuteReturnNull(final ShellCommand command,
                             final String[] cmds,
                             final Shell shell)
        throws Exception {

        String message = execute(command, cmds, shell);
        assertTrue("The return message is expected to be null: " +
            ", but get :" + message, message == null);
    }

    /**
     * Execute a command with given arguments, it is expected to OK and
     * return message is expected to contain the given expectedMsg.
     */
    void doExecuteCheckRet(final ShellCommand command,
                           final String[] cmds,
                           final String expectedMsg,
                           final Shell shell)
        throws Exception {

        doExecuteCheckRet(command, cmds, new String[]{expectedMsg},
            false, shell);
    }

    /**
     * Execute a command with given arguments, it is expected to OK and
     * return message is expected to contain all the strings in expectedMsgs.
     */
    void doExecuteCheckRet(final ShellCommand command,
                           final String[] cmds,
                           final String[] expectedMsgs,
                           final Shell shell)
        throws Exception {

        doExecuteCheckRet(command, cmds, expectedMsgs, false, shell);
    }

    /**
     * Execute a command with given arguments, it is expected to OK and
     * return message or output is expected to contain all the strings
     * in expectedMsgs. checkOutput should be set to true if checking
     * output of CommandShell.
     */
    void doExecuteCheckRet(final ShellCommand command,
                           final String[] cmds,
                           final String[] expectedMsgs,
                           final boolean checkOutput,
                           final Shell shell)
        throws Exception {

        /* Run the test and verify the results. */
        try {
            String message = execute(command, cmds, shell);
            if (checkOutput) {
                message = buffer.toString();
            }
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

    /**
     * Execute a command with given arguments, it is expected to OK and the
     * command result is dumped to output file.
     */
    void doExecuteCheckFile(final ShellCommand command,
                            final String[] cmds,
                            final String fileName,
                            final byte[] expBytes,
                            final Shell shell)
        throws Exception {

        /* Run the test and verify the results. */
        try {
            String message = execute(command, cmds, shell);
            assertTrue("The return message is expected to contain " + fileName +
                       ", but not", message.contains(fileName));
            if (expBytes != null) {
                final byte[] bytes = readFile(fileName);
                assertEquals("The number of bytes are not expected, expect " +
                             expBytes.length + ", but actual " + bytes.length,
                             expBytes.length, bytes.length);
                assertTrue(Arrays.equals(expBytes, bytes));
                return;
            }
        } catch (ShellException e) {
            fail("Expected to OK but get exception: " + e);
        }
    }

    private byte[] readFile(String fileName){
        final File file = new File(fileName);
        final byte[] result = new byte[(int)file.length()];
        InputStream input = null;
        try {
            int totalBytesRead = 0;
            input = new BufferedInputStream(new FileInputStream(file));
            while (totalBytesRead < result.length){
                int bytesRemaining = result.length - totalBytesRead;
                int bytesRead = input.read(result, totalBytesRead,
                                           bytesRemaining);
                if (bytesRead > 0){
                    totalBytesRead = totalBytesRead + bytesRead;
                }
            }
        }
        catch (FileNotFoundException ex) {
            fail("File " + fileName + " not found");
        }
        catch (IOException ex) {
            fail("Read file " + fileName + " failed: " + ex.getMessage());
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ignored) {
                }
            }
        }
        return result;
    }

    String execute(ShellCommand command, String[] args, Shell shell)
        throws ShellException {

        buffer.reset();
        return command.execute(args, shell);
    }

    /**
     * Table users and email related classes and methods, these 2 tables
     * are used in PutCommandTest, GetCommandTest and DeleteCommandTest.
     */
    static class UserInfo {
        public final String name;
        public final int age;
        public final String address;
        public final String phone;

        UserInfo(int index) {
            this.name = "name" + index;
            this.age = index;
            this.address = "address" + index;
            this.phone = "phone" + index;
        }

        String genUserInfoJsonString() {
            return "{\"name\":\"" + name + "\"," + "\"age\":" + age + ","
                + "\"address\":\"" + address + "\"," + "\"phone\":\"" + phone
                + "\"}";
        }
    }

    TableImpl addUserTable(String tableName)
        throws Exception {
        return addUserTable(null, tableName);
    }

    TableImpl addUserTable(String namespace, String tableName)
        throws Exception {
        return addUserTable(namespace, tableName, false);
    }

    TableImpl addUserTable(String namespace, String tableName, boolean noMRTableMode)
        throws Exception {
        /* Exercise the createTableBuilder() variants w/ and w/o namespace */
        TableBuilder tb = namespace == null ?
            TableBuilder.createTableBuilder(tableName) :
            TableBuilder.createTableBuilder(namespace, tableName);

        tb.addString("group", null, false, "")
            .addString("user", null, false, "")
            .addString("name", null, false, "")
            .addInteger("age", null, false, 0)
            .addString("address", null, false, "")
            .addString("phone", null, false, "")
            .primaryKey(new String[]{"group", "user"})
            .shardKey(new String[]{"group"});
        addTable(tb, true, noMRTableMode);
        /* Exercise the getTable() variants w/ and w/o namespace */

        return namespace == null ?
            getTable(tableName) : getTable(namespace, tableName);
    }

    TableImpl addEmailTable(String ns, String tableName, TableImpl parent)
        throws Exception {

        /* Exercise the createTableBuilder() variants w/ and w/o namespace */
        TableBuilder tb = ns == null ?
            TableBuilder.createTableBuilder(tableName, null, parent) :
            TableBuilder.createTableBuilder(ns, tableName, null, parent, null);

        tb.addInteger("eid")
            .addString("email")
            .primaryKey(new String[]{"eid"});
       //TODO: allow MRTable mode after MRTable supports child tables.
       addTable(tb, true, true/*noMRTableMode*/);
       return ns == null ?
           getTable(parent.getFullName() + "." + tableName) :
           getTable(ns, parent.getFullName() + "." + tableName);
    }

    /**
     * A table with indexes on complex fields.
     */
    TableImpl addContactTable()
        throws Exception {

        final String ddl =
            "create table contacts (" +
                 "id integer, " +
                 "name string, " +
                 "email array(string), " +
                 "phone map(string), " +
                 "address record(city string, street string), " +
                 "primary key(id)" +
             ")";

        StatementResult result = store.executeSync(ddl);
        assertTrue(result.isDone() && result.isSuccessful());

        result = store.executeSync("create index idx_email on " +
                    "contacts(email[])");
        assertTrue(result.isDone() && result.isSuccessful());

        result = store.executeSync("create index idx_phone on " +
                    "contacts (phone.keys(), phone.values())");
        assertTrue(result.isDone() && result.isSuccessful());

        result = store.executeSync("create index idx_address_city_name on " +
                    "contacts(address.city, name)");
        assertTrue(result.isDone() && result.isSuccessful());

        return getTable("contacts");
    }

    void loadUsers(int num, String prefix) {
        for (int i = 0; i < num; i++) {
            UserInfo user = new UserInfo(i);
            String record = user.genUserInfoJsonString();
            List<String> majorPaths;
            List<String> minorPaths;
            if (prefix != null) {
                majorPaths =
                    Arrays.asList(prefix, "group" + String.valueOf(i/10));
                minorPaths =
                    Arrays.asList("user" + String.valueOf(i%10));
            } else {
                majorPaths =
                    Arrays.asList("group", String.valueOf(i/10));
                minorPaths =
                    Arrays.asList("user", String.valueOf(i % 10));
            }
            Key key = Key.createKey(majorPaths, minorPaths);
            store.put(key, Value.createValue(record.getBytes()));
        }
    }

    void loadUsersTable(int num, TableImpl table) {
        for (int i = 0; i < num; i++) {
            UserInfo user = new UserInfo(i);
            String groupkey = "group" + String.valueOf(i/10);
            String userkey = "user" + String.valueOf(i%10);
            try {
                final Row row = table.createRow();
                row.put("group", groupkey);
                row.put("user", userkey);
                row.put("name", user.name);
                row.put("age", user.age);
                row.put("address", user.address);
                row.put("phone", user.phone);
                tableImpl.put(row, null, null);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {

            }
        }
    }

    void loadUserEmail(TableImpl parent, TableImpl table) {

        final TableIterator<Row> iter =
            tableImpl.tableIterator(parent.createPrimaryKey(), null, null);
        try {
            while(iter.hasNext()) {
                final Row userRow = iter.next();
                final Row emailRow = table.createRow(userRow);
                emailRow.put("eid", 1);
                emailRow.put("email",
                    userRow.get("name").asString().get() + "@abc.com");
                tableImpl.put(emailRow, null, null);

                emailRow.put("eid", 2);
                emailRow.put("email",
                    userRow.get("name").asString().get() + "@efg.com");
                tableImpl.put(emailRow, null, null);
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    void loadContacts(TableImpl table, int num) {
        for (int id = 0; id < num; id++) {
            Row row = addContactRow(table, id);
            tableImpl.put(row, null, null);
        }
    }

    private Row addContactRow(TableImpl table, int id) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("name", "name" + id);
        if (id % 10 == 1) {
            return row;
        }
        row.putArray("email")
            .add("email" + id + "@0")
            .add("email" + id + "@1");
        row.putMap("phone")
            .put("home", "phone" + id + "@home")
            .put("office", "phone" + id + "@office");
        row.putRecord("address")
            .put("city", "city" + (id % 10))
            .put("street", "street" + (id % 10));
        return row;
    }
}
