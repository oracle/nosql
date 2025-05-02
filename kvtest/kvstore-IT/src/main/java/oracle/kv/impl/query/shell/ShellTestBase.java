/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.query.shell;

import static oracle.kv.impl.util.CommandParser.JSON_FLAG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellHelpException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
 * Base class for sql shell test classes. The @BeforeClass and @AfterClass
 * methods create and destroy a store respectively.  The store and utility
 * methods in this class can be used for test cases.
 */
public class ShellTestBase extends TestBase {

    private static ByteArrayOutputStream outStream =
        new ByteArrayOutputStream();
    private static int start_port = 13250;

    static CreateStore createStore = null;
    static KVStore store = null;
    static TableAPI tableAPI = null;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        startStore();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableAPI = store.getTableAPI();
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

    }

    @Override
    public void tearDown()
        throws Exception {

        removeAllTables();
    }

    static void startStore() {
        try {
            final String storeName =
                "kvtest-" + testClassName + "-tablestore";
            createStore = new CreateStore(storeName,
                                          start_port,
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

    static KVStore getKVStore() {
        return store;
    }

    /**
     * Creates a KVStoreConfig for use in tests.
     */
    static KVStoreConfig createKVConfig(CreateStore cs) {
        KVStoreConfig config = cs.createKVConfig();
        config.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return config;
    }

    static void executeDDL(final String ddl) {
        try {
            store.executeSync(ddl);
        } catch (IllegalArgumentException iae) {
            fail("Failed to execute statement \'" + ddl + "\': " +
                 iae.getMessage());
        } catch (FaultException fe) {
            fail("Failed to execute statement \'" + ddl + "\': " +
                 fe.getMessage());
        }
    }

    static void runCommand(final Shell shell,
                           final ShellCommand cmdObj,
                           final String[] args,
                           final String... expectedStrings) {
        runCommand(shell, cmdObj, args, true, expectedStrings);
    }

    static void runCommand(final Shell shell,
                           final ShellCommand cmdObj,
                           final String[] args,
                           final boolean shouldSucceed,
                           final String... expectedStrings) {

        String result = null;
        try {
            result = cmdObj.execute(args, shell);
            if (!shouldSucceed) {
                fail("Run command is expected to be failed but succeed.");
            }
        } catch (ShellException se) {
            if (shouldSucceed) {
                fail("Run command failed: " + se.getMessage());
            }
        }
        if (result != null) {
            for (String string : expectedStrings) {
                assertTrue("Expected to get message that contains " + string +
                           ", but get " + result, result.contains(string));
            }
        }
    }

    static void runWithInvalidArgument(final Shell shell,
                                       final ShellCommand cmdObj,
                                       final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellArgumentException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue("Expected to catch ShellArgumentException but not",
                (e instanceof ShellArgumentException));
        }
    }

    static void runWithRequiredFlag(final Shell shell,
                                    final ShellCommand cmdObj,
                                    final String[] args,
                                    final String requiredFlag) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains(requiredFlag));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    static void runWithIncorrectNumberOfArguments(final Shell shell,
                                                  final ShellCommand cmdObj,
                                                  final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains("Incorrect number of arguments" +
                    " for command"));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    static void runWithUnknownArgument(final Shell shell,
                                       final ShellCommand cmdObj,
                                       final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellUsageException ");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue(e.getMessage().contains("Unknown argument"));
            assertTrue("Expected to catch ShellUsageException but not",
                (e instanceof ShellUsageException));
        }
    }

    static void runWithHelpException(final Shell shell,
                                     final ShellCommand cmdObj,
                                     final String[] args) {
        try {
            cmdObj.execute(args, shell);
            fail("Expected to catch ShellHelpException");
        } catch (ShellException e) /* CHECKSTYLE:OFF */ {
            assertTrue("Expected to catch ShellHelpException but not",
                (e instanceof ShellHelpException));
        }
    }

    static String getCommandLine(final String[] args, boolean isJsonOut) {
        final StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(arg);
        }
        if (isJsonOut) {
            sb.append(" ");
            sb.append(JSON_FLAG);
        }
        return sb.toString();
    }

    static String getShellOutput(Shell shell) {
        final String ret = ((TestShell)shell).getOutputResult();
        return ret == null ? "" : ret;
    }

    static void resetShellOutput(Shell shell) {
        ((TestShell)shell).resetOutput();
    }

    static String getTempFileName(String prefix, String suffix) {
        File file = null;
        try {
            file = File.createTempFile(prefix, suffix);
            file.deleteOnExit();
            if (file.exists()) {
                file.delete();
            }
            return file.getAbsolutePath();
        } catch (IOException e) {
            fail("Failed to create a temp file.");
        }
        return null;
    }

    static OnqlShell getTestShell() {
        return getTestShell(System.in);
    }

    static OnqlShell getTestShell(InputStream input) {
        return new TestShell(input, outStream);
    }

    static Table createTable(String ddl, String tblName) {
        executeDDL(ddl);
        return getTable(tblName);
    }

    static Table getTable(final String name) {
        assert(tableAPI != null);
        final Table table = tableAPI.getTable(name);
        if (table == null) {
            fail("Table not found: " + name);
        }
        return table;
    }

    static void removeAllTables() {
        Map<String, Table> tables = store.getTableAPI().getTables();
        for (Entry<String, Table> entry : tables.entrySet()) {
            TableImpl table = (TableImpl)entry.getValue();
            if (!table.isSystemTable()) {
                removeTable(table);
            }
        }
    }

    static void removeTable(TableImpl table) {
        if (table.hasChildren()) {
            for (Table tbl : table.getChildTables().values()) {
                removeTable((TableImpl)tbl);
            }
        }
        removeTable(table.getFullNamespaceName());
    }

    static void removeTable(String tableName) {
        String dropTable = "DROP TABLE " + tableName;
        executeDDL(dropTable);
    }

    private static class TestShell extends OnqlShell {
        private ByteArrayOutputStream buffer;

        TestShell(InputStream input, PrintStream output) {
            super(input, output);
            this.buffer = null;
        }

        public TestShell(InputStream input, ByteArrayOutputStream outStream) {
            this(input, new PrintStream(outStream));
            this.buffer = outStream;
        }

        @Override
        public void execute(String line) {
            resetOutput();
            super.execute(line);
        }

        String getOutputResult() {
            if (buffer != null) {
                return buffer.toString();
            }
            return null;
        }

        @Override
        public KVStore getStore() {
            /*
             * Use getKVStore method to get the store handle in ShellTestBase
             * as the parent class CommonShell also have a store handle with
             * the same name.
             */
            return getKVStore();
        }

        public void resetOutput() {
            if (buffer != null) {
                buffer.reset();
            }
        }
    }
}
