/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.Load.LoadStream;

import org.junit.Test;

/**
 * Very simple test to exercise the KVS Load command.
 * Test created for SR#23681, Load operation with wrong backup source
 * directory, should fail and have an exit value different from 0.
 */
public class LoadTest extends TestBase {

    private static final File testdir = TestUtils.getTestDir();
    private static final File backupDir = new File(testdir, "backup");
    private static final File restoreDir = new File(testdir, "restore");
    private static final File restoreSecDir = new File(testdir, "restoreSec");
    private static final File checkpointDir = new File(testdir, "checkpoint");
    private static final File statsDir = new File(testdir, "stats");

    private static final String USER =
        "create table user(id integer, name string, age integer, " +
                          "comments string, primary key(id))";
    private static final String USER_NAME_INDEX =
        "create index idxName on user(name)";
    private static final String USER_AGE_NAME_INDEX =
        "create index idxAgeName on user(age, name)";

    private static final String EMAIL = "create table user.email(" +
        "eid integer, email string, primary key(eid))";

    private static final String IDENTITY = "CREATE TABLE identity (id " +
            "LONG GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1 " +
            "MAXVALUE 100),name STRING, PRIMARY KEY (id))";

    /* Debugging flags */
    private static final boolean verboseOutput = false;
    private static final boolean clearTestDirectory = true;

    private final int haRange = 5;
    private final String testhost = "localhost";

    private final String backupName = "backupStore";
    private final String backupSecName = "backupSecStore";
    private final int backupStartPort = 8000;

    private final String restoreName = "restoreStore";
    private final String restoreSecName = "restoreSecStore";
    private final int restoreStartPort = 9000;

    private final String user = "user";
    private final String userPassword = "UserA__12345";

    private CreateStore backupStore;
    private KVStore backupStoreImpl;

    private CreateStore restoreStore;
    private KVStore restoreStoreImpl;

    private int restorePort;
    private int backupPort;
    private String backupStoreName = backupName;
    private String restoreStoreName;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        TestStatus.setManyRNs(true);

        backupPort = new PortFinder(backupStartPort, haRange).getRegistryPort();
        restorePort = new PortFinder(restoreStartPort, haRange)
                        .getRegistryPort();

        backupDir.mkdirs();
        restoreDir.mkdirs();
        restoreSecDir.mkdirs();
        checkpointDir.mkdirs();
        statsDir.mkdirs();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();

        LoggerUtils.closeAllHandlers();
        shutdownStores();
        if (clearTestDirectory) {
            clearTestDirectory();
        }
    }

    @Test
    public void testLoadInvalidArguments()
        throws IOException {

        /* Invalid backup source directory */
        List<String> command = getLoadArgs(new File("no file for you"),
                                           testhost, restoreStartPort,
                                           restoreName);

        StringBuilder stringBuilder = new StringBuilder();
        int exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        String result = stringBuilder.toString();
        assert(result.contains("Could not access backup source directory")) :
        "Expected \"Could not access backup source directory\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with an invalid backup source directory";

        /* Invalid status file directory */
        command = getLoadArgs(backupDir, testhost, restoreStartPort,
                              restoreName, "-checkpoint", "no file for you");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("Could not access checkpoint files directory")) :
        "Expected \"Could not access checkpoint files directory\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with an invalid checkpoint files directory";

        /* Miss -host flag */
        command = getLoadArgs(backupDir, null, restoreStartPort, restoreName);
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("Flag -host is required")) :
        "Expected \"Flag -host is required\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with missing -host flag";

        /* Miss -port flag */
        command = getLoadArgs(backupDir, testhost, null, restoreName);
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("Flag -port is required")) :
        "Expected \"Flag -port is required\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with missing -port flag";

        /* Miss -store flag */
        command = getLoadArgs(backupDir, testhost, restoreStartPort, null);
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("Flag -store is required")) :
        "Expected \"Flag -store is required\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with missing -store flag";

        /* Load admin with multiple source directories */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-load-admin", "-force");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("There must be exactly one source dir if " +
                               "loading Admin metadata")) :
        "There must be exactly one source dir if loading Admin metadata";
        assert(exitCode > 0) :
        "Operation Load Admin succeeded with multiple source directories";

        /* Load with -bulkput-stream-parallelism 0 */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-bulkput-stream-parallelism", "0");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result.contains("-bulkput-stream-parallelism requires a " +
                "positive integer"));
        assertTrue(exitCode > 0);

        /* Load with -bulkput-heap-percent 0 */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-bulkput-heap-percent", "0");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result.contains("-bulkput-heap-percent is a percentage " +
                "and must be in the range 1 ~ 99"));
        assertTrue(exitCode > 0);

        /* Load with -bulkput-per-shard-parallelism 0 */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-bulkput-per-shard-parallelism", "0");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result.contains("-bulkput-per-shard-parallelism requires" +
                " a positive integer"));
        assertTrue(exitCode > 0);

        /* Load with -bulkput-max-request-size 0 */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-bulkput-max-request-size", "0");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result.contains("-bulkput-max-request-size requires" +
                " a positive integer"));
        assertTrue(exitCode > 0);

        /* Load with -request-timeout-ms 0 */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-request-timeout-ms", "0");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result.contains("-request-timeout-ms requires a positive " +
                "integer"));
        assertTrue(exitCode > 0);

        /* Invalid stats files directory */
        command = getLoadArgs(backupDir, testhost, restoreStartPort,
                              restoreName, "-collect-stats", "no file for you");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assert(result.contains("Could not access stats files directory")) :
        "Expected \"Could not access stats files directory\" Exception";
        assert(exitCode > 0) :
        "Operation Load succeeded with an invalid stats files directory";

        /* Load with invalid value for -bulkput-heap-percent. */
        String[] percentArgs = new String[] {
            "-bulkput-heap-percent",
        };
        int[] invalidPercents = new int[] {0, 100};
        for (String arg : percentArgs) {
            for (int percent: invalidPercents) {
                command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                                      testhost, restoreStartPort, restoreName,
                                      arg, String.valueOf(percent));
                stringBuilder.setLength(0);
                exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
                result = stringBuilder.toString();
                assertTrue(result.contains(arg + " is a percentage and must " +
                        "be in the range 1 ~ 99"));
                assertTrue(exitCode > 0);
            }
        }

        /*
         * Load with -bulkput-heap-percent where the sum of it and 60% (the
         * default JE cache percent) is greater than 85.
         */
        command = getLoadArgs(Arrays.asList(backupDir, backupDir),
                              testhost, restoreStartPort, restoreName,
                              "-bulkput-heap-percent", "30");
        stringBuilder.setLength(0);
        exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        result = stringBuilder.toString();
        assertTrue(result,
            result.contains("The sum of bulkput-heap-percent and the " +
            "stream cache percent (60) must typically be less than 85"));
        assertTrue(exitCode > 0);
    }

    @Test
    public void testLoadToStore()
        throws Exception {

        restoreStoreName = restoreName;

        startBackupStore(false);
        startRestoreStore(false);

        final int nUsers = 1000;
        final int nEmails = 500;
        final int nIdentities = 100;
        final Table userTable = addUserTable();
        final Table emailTable = addUserEmailTable();
        final Table identityTable = addIdentityTable();
        loadUsers(backupStoreImpl, userTable, nUsers);
        loadEmails(backupStoreImpl, emailTable, nEmails);
        loadIdentities(backupStoreImpl, identityTable, nIdentities);

        final String snapshot = createSnapshot("mybackup");

        runLoadDataTest(snapshot, null, null, (nUsers + nEmails));

        testLockOnCheckpointDir(snapshot, null);

        testCollectStats(snapshot, null);

        testEnvConfigProperties(snapshot, null);

        shutdownStores();
    }

    private void testLockOnCheckpointDir(final String snapshot,
                                         final String[] loadArgs)
        throws Exception {

        final File checkPointDir = new File(testdir, "checkpointTest");
        checkPointDir.mkdirs();

        /* Case1: The checkPontDir is not writable */
        checkPointDir.setWritable(false);
        loadData(snapshot, checkPointDir.getAbsolutePath(), null,
                 "Failed to open checkpoint files directory: " +
                 checkPointDir.getAbsolutePath(), false);

        /*
         * Case2: Start 2 load data process using the same checkpoint file
         *        directory.
         */
        checkPointDir.setWritable(true);
        Thread t1 = new Thread(() -> {
            /* Load data */
            try {
                loadData(snapshot, checkPointDir.getAbsolutePath(),
                         loadArgs, "Load succeeded", true);
            } catch (Exception e) {
                fail("Failed to run loadData: " + e.getMessage());
            }
        });

        Thread t2 = new Thread(() -> {
            /* Load data */
            try {
                loadData(snapshot, checkPointDir.getAbsolutePath(),
                         loadArgs,
                         "Failed to acquire a lock on checkpoint files " +
                         "directory: " + checkPointDir.getAbsolutePath(),
                         false);
            } catch (Exception e) {
                fail("Failed to run loadData: " + e.getMessage());
            }
        });

        t1.start();
        Thread.sleep(100);
        t2.start();

        t1.join();
        t2.join();
    }

    private void testCollectStats(final String snapshot,
                                   final String[] loadArgs) {
        String[] testArgs = new String[]{"-collect-stats",
                                         statsDir.getAbsolutePath()};
        try {
            loadData(snapshot, null, mergeArgs(loadArgs, testArgs),
                     "Load succeeded", true);
            checkStatsFiles(statsDir, snapshot);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to run loadData: " + e.getMessage());
        }
    }

    /**
     * Check the statistic files for backup snapshot environments.
     */
    private void checkStatsFiles(File baseDir, String snapshot) {
        List<File> snapDirs = getBackupRepNodeSnapshotDirs(backupStore,
                                                           snapshot);
        File[] files = baseDir.listFiles();
        assertTrue(files.length == snapDirs.size());

        String[] sections = new String[] {
            "I/O", "Cache", "Cleaning", "Node Compression",
            "Checkpoints", "Environment", "Locks",
        };
        for (File file : files) {
            assertTrue(file.getName().contains(".stat"));
            verifyStatsFile(file, sections);
        }
    }

    private void testEnvConfigProperties(final String snapshot,
                                         final String[] loadArgs) {

        String[] testArgs = new String[] {"-je.log.fileCacheSize", "1000",
                                          "-je.log.faultReadSize", "8192",
                                          "-je.evictor.evictBytes", "1048576"};
        try {
            loadData(snapshot, null, mergeArgs(loadArgs, testArgs),
                     "Load succeeded", true);
        } catch (Exception e) {
            fail("Failed to run loadData: " + e.getMessage());
        }
    }

    private String[] mergeArgs(String[] loadArgs, String[] testArgs) {
        if (loadArgs == null) {
            return testArgs;
        }

        if (testArgs == null) {
            return loadArgs;
        }

        List<String> args = new ArrayList<String>();
        args.addAll(Arrays.asList(loadArgs));
        for (String arg : testArgs) {
            args.add(arg);
        }
        return args.toArray(new String[args.size()]);
    }

    /**
     * Checks the contents of the statistic file.
     */
    private void verifyStatsFile(File file, String[] sections) {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(file));
            int index = 0;
            String nextLine;
            while ((nextLine = in.readLine()) != null){
                if (nextLine.contains(sections[index])) {
                    index++;
                }
                if (index == sections.length) {
                    break;
                }
            }
            assertTrue(index == sections.length);
        } catch (FileNotFoundException ex) {
            fail("Stats file does not exist: " + file);
        } catch (IOException ex) {
            fail("Failed to read the stats file: " + file);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    public void testLoadToSecuredStore()
        throws Exception {

        backupStoreName = backupSecName;
        restoreStoreName = restoreSecName;

        startBackupStore(true);
        startRestoreStore(true);

        final int nUsers = 1000;
        final int nEmails = 500;
        final Table userTable = addUserTable();
        final Table emailTable = addUserEmailTable();
        loadUsers(backupStoreImpl, userTable, nUsers);
        loadEmails(backupStoreImpl, emailTable, nEmails);
        String snapshot = createSnapshot("mybackup");

        String[] userArgs = getSecureLoadArgs(restoreStore, user, userPassword);
        runLoadDataTest(snapshot, userArgs, userArgs, (nUsers + nEmails));

        shutdownStores();
    }

    /**
     * Test the resume loading.
     *
     * Steps to test the resumption of loading from previous failure:
     *  1. Start backup and restore stores, load records to backup store,
     *     then create snapshot for backup store.
     *  2. Start loading from backup directories with a checkpoint directory,
     *     a fake exception will be thrown out from LoadStream.getNext() of
     *     last partition of the backup directory of 3rd shard, then the
     *     loading failed.
     *  3. Rerun the loading with the existed checkpoint directory, the loaded
     *     partitions in previous run should be skipped in this run.
     */
    @Test
    public void testResumeLoading()
        throws Exception {

        restoreStoreName = restoreName;

        startBackupStore(false);
        startRestoreStore(false);

        final int nUsers = 10000;
        final Table userTable = addUserTable();
        loadUsers(backupStoreImpl, userTable, nUsers);


        String ss = createSnapshot("mybackup");
        List<File> snapDirs = getBackupRepNodeSnapshotDirs(backupStore, ss);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final String failedDB = getLastDatabase(backupStoreImpl);

        /* Load records, a dummy exception will be thrown */
        Load load = getLoadInstance(snapDirs, null, null,
                                    checkpointDir.getAbsolutePath(),
                                    0 /* maxStreamsPerShard */,
                                    1 /* bulkputHeapPercent */,
                                    0 /* bulkputPerShardParallelism */,
                                    0 /* bulkputMaxRequestSize */,
                                    0 /* requestTimeoutMs */,
                                    verboseOutput,
                                    new PrintStream(out));

        load.setLoadStreamGetNext(new TestHook<LoadStream>() {
            /**
             * Throw a dummy exception, wait at most 1 minute to make sure
             * some partitions are completed.
             */
            @Override
            public void doHook(LoadStream stream) {
                if (stream.getDatabase().equalsIgnoreCase(failedDB)) {
                    if (stream.getReadCount() > 90) {
                        /*there should be more than 90 records in the last partition.*/
                        int maxRetry = 60;
                        while(maxRetry > 0) {
                            if (!getLoadedInfo(checkpointDir).isEmpty()){
                                break;
                            }
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                break;
                            }
                            maxRetry--;
                        }
                        throw new RuntimeException("dummy exception from " +
                            stream.name());
                    }
                }
            }
        });

        try {
            load.run();
            fail("Expect to catch exception but succeed.");
        } catch (RuntimeException re) {
            String msg = re.getMessage();
            assertTrue(msg.contains("dummy exception") &&
                       msg.contains(failedDB));
        }

        List<String> loadedInfo = getLoadedInfo(checkpointDir);
        if (!loadedInfo.isEmpty()) {
            assertTrue(!loadedInfo.contains(failedDB));
        }

        /* Resume the loading with checkpoint files.*/
        out.reset();
        load = getLoadInstance(snapDirs, null, null,
                               checkpointDir.getAbsolutePath(),
                               0 /* maxStreamsPerShard */,
                               0 /* bulkputHeapPercent */,
                               0 /* bulkputPerShardParallelism */,
                               0 /* bulkputMaxRequestSize */,
                               0 /* requestTimeoutMs */,
                               true, new PrintStream(out));
        try {
            load.run();
        } catch (RuntimeException re) {
            fail("Expect to catch exception but succeed.");
        }

        if (!loadedInfo.isEmpty()) {
            String output = out.toString();
            for (String name : loadedInfo) {
                String str = "Skipping already loaded database: " + name;
                assertTrue(output.contains(str));
            }
        }

        checkDBCheckpointFiles(checkpointDir, snapDirs.size(), restoreStoreName,
                               getAllDatabases(backupStoreImpl));

        compareStoreKVData(backupStoreImpl, restoreStoreImpl);
        shutdownStores();
    }

    /**
     * Returns the last partition of the last RepGroup
     */
    private String getLastDatabase(KVStore store) {
        Map<RepGroupId, List<PartitionId>> rgPartitions =
            TopologyUtil.getRGIdPartMap(((KVStoreImpl)store).getTopology());
        List<PartitionId> pids =
            rgPartitions.get(new RepGroupId(rgPartitions.size()));
        return pids.get(pids.size() - 1).getPartitionName();
    }

    /**
     * Returns the map of RegGroup name and its partition Names of the given
     * store.
     */
    private Map<String, List<String>> getAllDatabases(KVStore store) {
        Map<RepGroupId, List<PartitionId>> rgPartitions =
            TopologyUtil.getRGIdPartMap(((KVStoreImpl)store).getTopology());

        Map<String, List<String>> allDatabases = new HashMap<>();
        for (Entry<RepGroupId, List<PartitionId>> entry :
             rgPartitions.entrySet()) {

            RepGroupId rgId = entry.getKey();
            List<String> shardDatabases = new ArrayList<>();
            for (PartitionId pid : entry.getValue()) {
                shardDatabases.add(pid.getPartitionName());
            }
            allDatabases.put(rgId.getGroupName(), shardDatabases);
        }
        return allDatabases;
    }

    /**
     * Collected the name of loaded databases or snapshots from checkpoint files.
     */
    private List<String> getLoadedInfo(File checkpointHome){
        List<String> names = new ArrayList<>();
        File[] files = checkpointHome.listFiles();
        for (File file : files) {
            if (file.getName().contains("cp.lck")) {
                continue;
            }
            assert(file.isDirectory());
            File[] cpFiles = file.listFiles();
            for (File cpFile : cpFiles) {
                names.add(cpFile.getName());
            }
        }
        return names;
    }

    /**
     * Check the checkpoint files.
     */
    private void checkDBCheckpointFiles(File baseDir, int nBackupDirs,
                                        String targetStore,
                                        Map<String, List<String>> shardDBs) {

        File[] files = baseDir.listFiles();
        assertTrue(files.length == nBackupDirs + 1);

        for (File file : files) {
            if (file.getName().contains("cp.lck")) {
                continue;
            }
            String shardCheckpointDir = file.getName();
            if (shardDBs.containsKey(shardCheckpointDir)) {
                List<String> databases = shardDBs.get(shardCheckpointDir);
                assertTrue(databases.size() > 0);
                checkDBCheckpointFiles(file, targetStore, databases);
            }
        }
    }

    /**
     * Check the checkpoint files for a shard.
     */
    private void checkDBCheckpointFiles(File shardCheckpointDir,
                                        String targetStoreName,
                                        List<String> databases) {
        assertTrue(shardCheckpointDir.isDirectory());
        assertTrue(shardCheckpointDir.listFiles().length == databases.size());

        for (String dbName : databases) {
            File file = new File(shardCheckpointDir, dbName);
            assertTrue(file.exists() && file.isFile());
            verifyCheckpointFile(file, targetStoreName);
        }
    }

    /**
     * Verify the content of checkpoint file.
     */
    private void verifyCheckpointFile(File checkpointFile, String storeName) {

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(checkpointFile);
            Properties props = new Properties();
            props.load(fis);
            assertTrue(((String)props.get("store"))
                           .equalsIgnoreCase(storeName));
            assertTrue(props.get("loadTime") != null);
        } catch (FileNotFoundException ex) {
            fail("Status file does not exist: " + checkpointFile);
        } catch (IOException ex) {
            fail("Failed to read the status file: " + checkpointFile);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    private void runLoadDataTest(String snapshot,
                                 String[] loadAdminArgs,
                                 String[] loadDataArgs,
                                 int numRecords)
        throws Exception {

        /* Load metadata */
        loadAdmin(snapshot, loadAdminArgs);

        /* Load data */
        loadData(snapshot, null, loadDataArgs,
                 "Load succeeded, wrote " + numRecords + " records", true);

        /* Load data with status files */
        loadData(snapshot, checkpointDir.getAbsolutePath(),
                 loadDataArgs, "Load succeeded, wrote 0 records", true);

        /* Load data with status files again */
        String expMsg = null;
        if (loadDataArgs != null) {
            for (String arg : loadDataArgs) {
                if (arg.contains("-single-database-cursor")) {
                    expMsg = "No more database to load";
                }
            }
        }
        if (expMsg == null) {
            expMsg = "Load succeeded, wrote 0 records";
        }
        loadData(snapshot, checkpointDir.getAbsolutePath(), loadDataArgs,
                 expMsg, true);

    }

    /**
     * Load metadata
     */
    private void loadAdmin(String snapshot, String[] loadArgs)
        throws Exception {

        File snapDir = getAdminSnapshotDir(backupStore, snapshot);
        assertTrue(snapDir != null);

        String[] args;
        final String[] loadAdminArgs = new String[] {"-load-admin", "-force"};
        if (loadArgs != null && loadArgs.length > 0) {
            args = new String[loadArgs.length + loadAdminArgs.length];
            System.arraycopy(loadAdminArgs, 0, args, 0, loadAdminArgs.length);
            System.arraycopy(loadArgs, 0, args, loadAdminArgs.length,
                             loadArgs.length);
        } else {
            args = loadAdminArgs;
        }

        List<String> command = getLoadArgs(Arrays.asList(snapDir), args);

        StringBuilder stringBuilder = new StringBuilder();
        int exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        assertTrue(exitCode == 0);
        assertEqualTableMetadata(backupStore, restoreStore);
    }


    /**
     * Load data
     */
    private void loadData(String snapshot, String cpDir,
                          String[] loadArgs, String expResult,
                          boolean shouldSuceed)
        throws Exception {

        List<File> snapDirs = getBackupRepNodeSnapshotDirs(backupStore,
                                                           snapshot);
        int nShards = ((KVStoreImpl)backupStoreImpl).getTopology()
                        .getRepGroupIds().size();
        assertTrue(snapDirs != null && snapDirs.size() == nShards);

        List<String> args = new ArrayList<>();
        if (loadArgs != null && loadArgs.length > 0) {
            args.addAll(Arrays.asList(loadArgs));
        }
        if (cpDir != null) {
            args.add("-checkpoint");
            args.add(cpDir);
        }

        List<String> command;
        if (args.size() > 0) {
            command = getLoadArgs(snapDirs,
                                  args.toArray(new String[args.size()]));
        } else {
            command = getLoadArgs(snapDirs);
        }

        StringBuilder stringBuilder = new StringBuilder();
        int exitCode = ProcessSetupUtils.runProcess(command, stringBuilder);
        assertTrue("Expected exit code " + (shouldSuceed ? "0" : "non-0") +
                   " got " + exitCode + ". Process output: " +
                   stringBuilder,
                   (shouldSuceed ? (exitCode == 0) : (exitCode != 0)));
        if (expResult != null) {
            int start = stringBuilder.indexOf("Load succeeded,");
            if (start != -1) {
                final String output = stringBuilder.toString();
                final Pattern pattern =
                        Pattern.compile("Load succeeded, wrote (\\d+) records");
                Matcher matcher = pattern.matcher(expResult);
                if (matcher.find()) {
                    final int expRecords = Integer.parseInt(matcher.group(1));
                    matcher = pattern.matcher(output);
                    final int actualRecords = matcher.find() ?
                            Integer.parseInt(matcher.group(1)) :
                            0;
                    assertTrue(actualRecords >= expRecords);
                } else {
                    assertTrue(output.contains(expResult));
                }
            }
        }
        if (shouldSuceed) {
            compareStoreKVData(backupStoreImpl, restoreStoreImpl);
        }
    }

    private Load getLoadInstance(List<File> snapshotDirs,
                                 String username, String securityFile,
                                 String checkpointDirectory,
                                 int maxStreamsPerShard,
                                 int bulkputHeapPercent,
                                 int bulkputPerShardParallelism,
                                 int bulkputMaxRequestSize,
                                 long requestTimeoutMs,
                                 boolean verbose,
                                 PrintStream out)
        throws Exception {

        return new Load(snapshotDirs.toArray(new File[snapshotDirs.size()]),
                        restoreStoreName,
                        testhost,
                        restorePort,
                        username,
                        securityFile,
                        checkpointDirectory,
                        null /*statsDir*/,
                        null,
                        maxStreamsPerShard,
                        bulkputHeapPercent,
                        bulkputPerShardParallelism,
                        bulkputMaxRequestSize,
                        requestTimeoutMs,
                        verbose,
                        out);
    }

    /**
     * Generates the arguments for Load process.
     */
    private List<String> getLoadArgs(List<File> snapDirs, String... args) {
        return getLoadArgs(snapDirs, testhost, restorePort,
                           restoreStoreName, args);
    }

    private List<String> getLoadArgs(File snapDir, String hostname,
                                     Integer port, String storeName,
                                     String... args) {
        return getLoadArgs(Arrays.asList(snapDir), hostname, port,
                           storeName, args);
    }

    private List<String> getLoadArgs(List<File> snapDirs, String hostname,
                                     Integer port, String storeName,
                                     String... args) {

        List<String> command = ProcessSetupUtils.setupJavaJarKVStore();
        command.add("load");
        if (snapDirs != null && !snapDirs.isEmpty()) {
            command.add("-source");
            command.add(joinString(snapDirs));
        }
        if (hostname != null) {
            command.add("-host");
            command.add(hostname);
        }
        if (port != null) {
            command.add("-port");
            command.add(String.valueOf(port));
        }
        if (storeName != null) {
            command.add("-store");
            command.add(storeName);
        }
        if (args != null) {
            for (String arg : args) {
                command.add(arg);
            }
        }
        return command;
    }

    private String joinString(List<File> snapDirs) {
        if (snapDirs.size() == 1) {
            return snapDirs.get(0).toString();
        }

        StringBuilder sb = new StringBuilder();
        for (File dir : snapDirs) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(dir.getAbsolutePath());
        }
        return sb.toString();
    }

    /**
     * Start and shutdown stores related functions.
     */

    /**
     * Start backup store and open store.
     */
    private void startBackupStore(boolean secured)
        throws Exception {

        backupStore = createBackupStore(backupDir, backupPort,
                                        backupStoreName, secured);
        assertTrue(backupStore != null);

        if (secured) {
            backupStoreImpl = openStore(backupStore, true, user, userPassword);
        } else {
            backupStoreImpl = openStore(backupStore);
        }
        assertTrue(backupStoreImpl != null);
    }

    /**
     * Start restore store and open store.
     */
    private void startRestoreStore(boolean secured)
        throws Exception {

        restoreStore = createRestoreStore(restoreDir, restorePort,
                                          restoreStoreName, secured);
        assertTrue(restoreStore != null);

        if (secured) {
            restoreStoreImpl = openStore(restoreStore, true,
                                         user, userPassword);
        } else {
            restoreStoreImpl = openStore(restoreStore);
        }
        assertTrue(restoreStoreImpl != null);
    }

    /**
     * Shutdowns stores.
     */
    private void shutdownStores() {

        shutdownBackupStore();
        shutdownRestoreStore();
    }

   /**
    * Make a local 3x1 store.
    */
   private CreateStore createBackupStore(File root, int port,
                                         String storename,
                                         boolean secured)
       throws Exception {

       CreateStore createStore =
           new CreateStore(storename,
                           port,
                           1, /* n SNs */
                           1, /* rf */
                           100, /* n partitions */
                           3, /* capacity per SN */
                           3 * CreateStore.MB_PER_SN,
                           false, /* use threads is false */
                           null, /* mgmtImpl */
                           true, /* mgmtPortsShared */
                           secured); /* secure store */
       createStore.setRootDir(root.toString());
       if (secured) {
           /* first user must have admin enabled */
           createStore.addUser("admin", "ADmin@#12345", true /* admin */);

           /* add 1 users and give one of the readwrite access */
           createStore.addUser(user, userPassword, true /* admin */);

           createStore.start(false);

           /* access */
           createStore.grantRoles(user, "readwrite", "dbadmin");
       } else {
           createStore.start(false);
       }
       return createStore;
   }

   /**
    * Make a local 1x1 store.
    */
   private CreateStore createRestoreStore(File root, int port,
                                          String storename,
                                          boolean secured)
       throws Exception {

       CreateStore createStore =
           new CreateStore(storename,
                           port,
                           1, /* n SNs */
                           1, /* rf */
                           30, /* n partitions */
                           1, /* capacity per SN */
                           CreateStore.MB_PER_SN,
                           false, /* use threads is false */
                           null, /* mgmtImpl */
                           true, /* mgmtPortsShared */
                           secured); /* secure store */
       createStore.setRootDir(root.toString());
       if (secured) {
           /* first user must have admin enabled */
           createStore.addUser("admin", "ADmin_@12345", true /* admin */);

           /* add 1 users and give one of the readwrite access */
           createStore.addUser(user, userPassword, true /* admin */);

           createStore.start(false);

           /* access */
           createStore.grantRoles(user, "readwrite", "dbadmin", "writesystable");
       } else {
           createStore.start(false);
       }
       return createStore;
    }

    /**
     * Open store
     */
    private KVStore openStore(CreateStore createStore) {
        return openStore(createStore, false, null, null);
    }

    private KVStore openStore(CreateStore createStore, boolean secured,
                              String username, String password) {
        if (!secured) {
            return KVStoreFactory.getStore(createKVConfig(createStore));
        }

        LoginCredentials creds =
            new PasswordCredentials(username, password.toCharArray());
        KVStoreConfig kvConfig = createKVConfig(createStore);
        Properties props = createSecurityProperties(createStore);
        kvConfig.setSecurityProperties(props);
        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    /**
     * Shutdown backup store.
     */
    private void shutdownBackupStore() {
        if (backupStoreImpl != null) {
            backupStoreImpl.close();
            backupStoreImpl = null;
        }

        if (backupStore != null) {
            backupStore.shutdown();
            backupStore = null;
        }
    }

    /**
     * Shutdown restore store.
     */
    private void shutdownRestoreStore() {
        if (restoreStoreImpl != null) {
            restoreStoreImpl.close();
            restoreStoreImpl = null;
        }

        if (restoreStore != null) {
            restoreStore.shutdown();
            restoreStore = null;
        }
    }

    /**
     * Creates a KVStoreConfig for use in tests.
     */
    private KVStoreConfig createKVConfig(CreateStore cs) {
        KVStoreConfig config =
            new KVStoreConfig(cs.getStoreName(),
                              cs.getHostname() + ":" +
                              cs.getRegistryPort());
        config.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return config;
    }

    /**
     * Creates a snapshot
     */
    private String createSnapshot(String name)
        throws Exception {

        Snapshot snapshot = new Snapshot(backupStore.getAdmin(),
                                         verboseOutput,
                                         System.err);
        return snapshot.createSnapshot(name);
    }

    /**
     * Returns a list of snapshot directories, one per RepGroup (shard).
     */
    private static List<File>
        getBackupRepNodeSnapshotDirs(CreateStore createStore,
                                     String snapshotName) {

        ArrayList<File> list = new ArrayList<>();
        Set<String> groups = new HashSet<>();

        for (StorageNodeId snid : createStore.getStorageNodeIds()) {
            List<RepNodeId> rns = createStore.getRNs(snid);
            for (RepNodeId rnid : rns) {
                String group = rnid.getGroupName();
                if (groups.contains(group)) {
                    continue;
                }
                groups.add(group);
                list.add(new File
                         (FileNames.getSnapshotDir(createStore.getRootDir(),
                                                   createStore.getStoreName(),
                                                   null,
                                                   snid,
                                                   rnid), snapshotName));
            }
        }
        return list;
    }

    /**
     * Returns the directory that contains admin snapshot for the first admin
     * found.  If that admin's id is not "Admin1" this will fail.  There is no
     * current mechanism to get the AdminId.  If this test starts failing
     * something should be added to CreateStore.
     */
    private static File getAdminSnapshotDir(CreateStore createStore,
                                            String snapshotName) {
        StorageNodeId adminSnid = null;
        for (StorageNodeId snid : createStore.getStorageNodeIds()) {
            if (createStore.hasAdmin(snid)) {
                adminSnid = snid;
                break;
            }
        }
        assertNotNull(adminSnid);
        return new File
            (FileNames.getSnapshotDir(createStore.getRootDir(),
                                      createStore.getStoreName(),
                                      null,
                                      adminSnid,
                                      new AdminId(1)), snapshotName);
    }

    /**
     * Compares table metadata of 2 stores.
     */
    private static void assertEqualTableMetadata(CreateStore store1,
                                                 CreateStore store2)
        throws Exception {

        TableMetadata md1 = getTableMetadata(store1);
        TableMetadata md2 = getTableMetadata(store2);
        if (md1.compareMetadata(md2)) {
//            System.out.println("TableMetadata=" + md1);
            return;
        }
        fail("TableMetadata unequal. md1=" + md1 + " md2=" + md2);
    }

    private static TableMetadata getTableMetadata(CreateStore st)
        throws Exception{

        return getTableMetadata(st.getAdmin());
    }

    private static TableMetadata getTableMetadata(CommandServiceAPI cs)
        throws Exception {

        return cs.getMetadata(TableMetadata.class,
                              MetadataType.TABLE);
    }


    /*
     * Security arguments related functions.
     */

    /**
     * Creates security arguments "-username <name> -security <file>"
     */
    private String[] getSecureLoadArgs(CreateStore createStore,
                                       String username, String password)
        throws IOException {

        String securityFile = createSecurityFile(createStore,
                                                 username, password);
        assertTrue(new File(securityFile).exists());
        return new String[]{"-username", username, "-security", securityFile};
    }

    /**
     * Create security properties includes:
     *  oracle.kv.transport=ssl
     *  oracle.kv.ssl.trustStore=<path-to-trust-store>
     */
    private static Properties
        createSecurityProperties(CreateStore createStore) {

        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath());
        return props;
    }

    /**
     * Create security properties includes:
     *  oracle.kv.transport=ssl
     *  oracle.kv.ssl.trustStore=<path-to-trust-store>
     *  oracle.kv.auth.pwdfile.file=<path-to-pwd-file>
     */
    private static Properties createSecurityProperties(CreateStore createStore,
                                                       String username,
                                                       String password){

        Properties props = createSecurityProperties(createStore);
        File pwdFile = createPasswordStore(username, password);

        props.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY, pwdFile.getPath());
        return props;
    }

    /**
     * Creates a password store for auto login of client tests. The filestore
     * is used currently. The path of password store is <test-dir>/<user>.pwd.
     */
    private static File createPasswordStore(String username, String password) {

        try {
            final File pwdFile = new File(TestUtils.getTestDir(),
                                          username + ".pwd");
            if (pwdFile.exists()) {
                return pwdFile;
            }
            final PasswordManager pwdMgr = PasswordManager.load(
                "oracle.kv.impl.security.filestore.FileStoreManager");

            final PasswordStore pwdfile = pwdMgr.getStoreHandle(pwdFile);
            pwdfile.create(null /* auto login */);
            pwdfile.setSecret(username, password.toCharArray());
            pwdfile.save();

            assertTrue(pwdFile.exists());
            return pwdFile;
        } catch (Exception e) {
            fail("Can't find password or access manager class: " + e);
        }
        return null;
    }


    /**
     * Creates a security file for login to admin, the path of password store
     * is <test-dir>/<user>.sec.
     */
    private static String createSecurityFile(CreateStore createStore,
                                             String username, String password)
        throws IOException {

        File secFile = new File(TestUtils.getTestDir(), username + ".sec");
        FileOutputStream fos = new FileOutputStream(secFile);

        Properties props = createSecurityProperties(createStore,
                                                    username, password);
        props.store(fos, null);
        fos.flush();
        fos.close();

        assertTrue(secFile.exists());
        return secFile.getPath();
    }

    private boolean compareStoreKVData(KVStore store1, KVStore store2) {
        int c1 = countStoreRecords(store1);
        int c2 = countStoreRecords(store2);
        if (c1 == c2) {
            final Iterator<KeyValueVersion> iter =
                store1.storeIterator(Direction.UNORDERED, 500);
            while (iter.hasNext()) {
                KeyValueVersion kvv = iter.next();
                ValueVersion vv = store2.get(kvv.getKey());

                /* Note: you can't compare Version across stores */
                if (vv == null || !vv.getValue().equals(kvv.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private int countStoreRecords(KVStore store) {
        final Iterator<Key> iter =
            store.storeKeysIterator(Direction.UNORDERED, 500,
                                 null, null, null,
                                 Consistency.ABSOLUTE,
                                 0, null); /* default timeout */
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        return count;
    }

    private int countTable(KVStore store, Table table) {
        TableIterator<PrimaryKey> iter =
            store.getTableAPI().tableKeysIterator(table.createPrimaryKey(),
                                                  null, null);
        int count = 0;
        while(iter.hasNext()) {
            iter.next();
            count++;
        }
        return count;
    }

    /*
     * Table related utility functions.
     */

    /**
     * Add user table
     */
    private Table addUserTable() {
        return addTable(backupStoreImpl,
                        new String[]{USER,
                                     USER_NAME_INDEX,
                                     USER_AGE_NAME_INDEX},
                        "user", new String[]{"idxName", "idxAgeName"});
    }

    /**
     * Add user.email table
     */
    private Table addUserEmailTable() {
        return addTable(backupStoreImpl, new String[]{EMAIL},
                        "user.email", null);
    }

    /**
     * Add identity table
     */
    private Table addIdentityTable() {
        return addTable(backupStoreImpl, new String[]{IDENTITY},
                "identity", null);
    }

    private Table addTable(KVStore store,
                           String[] ddls,
                           String tableName,
                           String[] indexNames) {

        for (String ddl : ddls) {
            StatementResult sr = store.executeSync(ddl);
            assertTrue(sr.isSuccessful());
        }

        Table table = store.getTableAPI().getTable(tableName);
        assertTrue(table != null);

        int nIndexes = (indexNames != null) ? indexNames.length : 0;
        assertTrue(table.getIndexes().size() == nIndexes);

        if (indexNames != null) {
            for (String indexName : indexNames) {
                assertTrue(table.getIndex(indexName) != null);
            }
            nIndexes = indexNames.length;
        }

        return table;
    }

    private void loadUsers(KVStore store, Table userTable, int num) {
        final TableAPI tableAPI = store.getTableAPI();
        for (int id = 0; id < num; id++) {
            Row row = createUserRow(userTable, id);
            assertTrue(tableAPI.put(row, null, null) != null);
        }
        assertTrue(countTable(store, userTable) == num);
    }

    private void loadEmails(KVStore store, Table emailTable, int num) {
        final TableAPI tableAPI = store.getTableAPI();
        for (int id = 0; id < num; id++) {
            Row row = createEmailRow(emailTable, id);
            assertTrue(tableAPI.put(row, null, null) != null);
        }
        assertTrue(countTable(store, emailTable) == num);
    }

    private void loadIdentities(KVStore store, Table identityTable, int num) {
        final TableAPI tableAPI = store.getTableAPI();
        for (int id = 0; id < num; id++) {
            Row row = createIdentityRow(identityTable, id);
            assertTrue(tableAPI.put(row, null, null) != null);
        }
        assertTrue(countTable(store, identityTable) == num);
    }

    private Row createUserRow(Table userTable, int id) {
        final Row row = userTable.createRow();
        row.put("id", id);
        row.put("name", "name" + id);
        row.put("age", id % 30 + 20);
        row.put("comments", genComments(1000));
        return row;
    }

    private Row createEmailRow(Table emailTable, int id) {
        final Row row = emailTable.createRow();
        row.put("id", id);
        row.put("eid", 0);
        row.put("email", "email" + id + "@abc.com");
        return row;
    }

    private Row createIdentityRow(Table identityTable, int id) {
        final Row row = identityTable.createRow();
        row.put("name", "name" + id);
        return row;
    }

    private String genComments(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append('A' + (i % 26));
        }
        return sb.toString();
    }
}
