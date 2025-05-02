/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.util.DbVerify;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * To verify the fix to KVSTORE-1881
 */
public class LoadTableRemoveTest extends TableTestBase {

    private static boolean verbose = false;

    private static final int ERASURE_PERIOD_S = 1;
    private static final long CHKPT_BYTES_INTERVAL = 1000000;
    private static final long CLEANER_BYTES_INTERVAL = 100000;
    private static final long LOG_MAX_SIZE = 1000000;

    private static final String S1K = makeString(1024);

    private static void initPolicyMap() {
        final String erasureParams = String.format(
                "je.env.runEraser=true;" +
                "je.erase.period=%d s;" +
                "je.checkpointer.bytesInterval=%d;" +
                "je.log.fileMax=%d;" +
                "je.env.runCleaner=true;" +
                "je.cleaner.bytesInterval=%d;" +
                "je.env.runINCompressor=false;" +
                "je.cleaner.threads=3;" +
                "je.cleaner.minUtilization=65;" +
                "je.cleaner.extinctScanBatchSize=1000;" +
                "je.cleaner.extinctScanBatchDelay=10 s;",
                ERASURE_PERIOD_S,
                CHKPT_BYTES_INTERVAL,
                LOG_MAX_SIZE,
                CLEANER_BYTES_INTERVAL
        );
        output("JE properties: " + erasureParams);
        policyMap = new ParameterMap();
        policyMap.setParameter(ParameterState.JE_MISC, erasureParams);
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        initPolicyMap();
        TableTestBase.staticSetUp();
        output("Started store with erasure parameters");
    }

    @Test
    public void test() throws Exception {
        final String t0 = "users0";
        final String t1 = "users1";
        final String t2 = "users2";
        final int batchSize = 300;

        /*
         * Create 3 tables: users0, users1, users2
         */
        createTable(t0);
        createTable(t1);
        createTable(t2);

        /*
         * Load rows to 3 tables
         *
         * Generate obsoletions in as many files as possible, by updating data
         * in non-dropped tables.
         *
         * Loop is required because obsoletions need to go along with extinction
         * in as many files as possible. Updating twice to make cleaner
         * extremely busy.
         */
        for (int i = 0; i < 100; i++) {
            loadRows(t0, batchSize * i, batchSize);
            loadRows(t1, batchSize);
            loadRows(t1, batchSize);
            loadRows(t2, batchSize);
            loadRows(t2, batchSize);
        }

        /* Drop table users0 */
        dropTable(t0);
        output("Dropped table " + t0);

        /*
         * Let Cleaner clean as many files as it can, but at the same time
         * Extinction thread should not be able to process all of the data.
         * This time is a problem - how much to set.
         *
         * Basically batch delay is 10 seconds, so taking 5s.
         */
        synchronized(this) {
            wait(5 * 1000);
        }

        /*
         * Force a checkpoint before taking a snapshot, by adding more data.
         */
        loadRows(t2, batchSize * 10);

        /*
         * Create snapshot
         */
        String snapshotName = createSnapshot("users");
        output("Created snapshot: " + snapshotName);

        /*
         * DbVerify snapshot
         */
        output("Run dbverify on snapshot " + snapshotName);
        List<File> snapshotDirs = getRepNodeSnapshotDirs(snapshotName);
        runDbVerify(snapshotDirs);

        /*
         * Load snapshot to restoreStore
         */
        if (restoreStore == null) {
            startRestoreStore();
        }
        loadToRestoreStore(snapshotName);
        output("Loaded snapshot " + snapshotName);

        assertTrue(compareStoreTableMetadata(createStore, restoreStore));
        assertEquals(countRows(tableImpl, t1), countRows(restoreTableImpl, t1));
        assertEquals(countRows(tableImpl, t2), countRows(restoreTableImpl, t2));
    }

    private void createTable(String tableName) {
        String ddl = "create table " + tableName + "(" +
                     "id integer, name string, primary key(id))";
        executeDdl(ddl, true);
        output("Created table " + tableName);
    }

    private void dropTable(String tableName) {
        String ddl = "drop table " + tableName;
        executeDdl(ddl, true);
        output("Dropped table: " + tableName);
    }

    private boolean runDbVerify(List<File> envPaths) {
        for(File path : envPaths) {
            VerifySummary summary = doVerfiy(path);
            if (summary.hasErrors()) {
                output("DbVerify found errors: " + summary);
                return false;
            }
        }
        output("DbVerify found no error");
        return true;
    }

    private static VerifySummary doVerfiy(File envPath) {
        final String[] args = {"-vdr", "-h", envPath.getAbsolutePath(), "-q"};
        return new DbVerify().runVerify(args, true);
    }

    private void loadRows(String tableName, int numRows) {
        loadRows(tableName, 0, numRows);
    }

    private void loadRows(String tableName, int idOffset, int numRows) {
        TableImpl table = (TableImpl)tableImpl.getTable(tableName);
        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i + idOffset);
            row.put("name", S1K);
            tableImpl.put(row, null, null);
        }
        output(tableName + ": loaded " + numRows + " rows");
    }

    private static int countRows(TableAPI tableAPI, String tableName) {
        Table table = tableAPI.getTable(tableName);
        assertNotNull(table);

        final TableIterator<Row> iter =
            tableAPI.tableIterator(table.createPrimaryKey(), null, countOpts);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        return count;
    }

    private static String makeString(int length) {
        return String.format("%0" + length + "d", 0);
    }

    private static void output(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }
}
