/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.recovery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.TestUtils;
import oracle.kv.util.recovery.ARTRequiredFiles;
import oracle.kv.util.recovery.AdminRecover;
import oracle.kv.util.recovery.RecoverConfig;
import oracle.kv.util.recovery.SNRecover;

import com.sleepycat.je.BackupFSArchiveCopy;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.utilint.CronScheduleParser;

import org.junit.Test;

/**
 * SNRecover Utility Test
 */
public class SNRecoverTest extends TestBase {

    private final static String STORE_NAME = "SNRecoverTest";
    private CreateStore createStore = null;
    private File inputConfigFile = null;
    private File backupConfigFile = null;
    private File backupDirectory= null;
    private File outputAdminDir = null;

    private final static String jeRecoverCopyClass =
        "com.sleepycat.je.RecoverArchiveFSCopy";

    private final static String failingRecoverCopyClass =
        "oracle.kv.impl.util.recovery.FailingObjectStorageCopy";

    private static final long HOUR_MILLIS = 60 * 60 * 1000;

    /**
     * Date formatter to print dates in human readable format.  Synchronize on
     * this object when using it.
     */
    private static final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public void setUp()
        throws Exception {
        super.setUp();

        /*
         * Delete temporary created files from previous runs.
         */
        final File ansZipFile = new File("/tmp/ans.zip");
        if (ansZipFile.exists()) {
            Files.delete(ansZipFile.toPath());
        }
        final File recoverConfigZipFile = new File("/tmp/recoverconfig.zip");
        if (recoverConfigZipFile.exists()) {
            Files.delete(recoverConfigZipFile.toPath());
        }
        final File topologyOutputJSON = new File("/tmp/topologyoutput.json");
        if (topologyOutputJSON.exists()) {
            Files.delete(topologyOutputJSON.toPath());
        }
        final File artrequiredfiles = new File("/tmp/artrequiredfiles");
        if (artrequiredfiles.exists()) {
            FileUtils.deleteDirectory(artrequiredfiles);
        }
        final File recoverConfig = new File("/tmp/recoverconfig");
        if (recoverConfig.exists()) {
            FileUtils.deleteDirectory(recoverConfig);
        }
        final File copyBackupDir = new File("/tmp/copybackup");
        if (copyBackupDir.exists()) {
            FileUtils.deleteDirectory(copyBackupDir);
        }

        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        if (inputConfigFile != null) {
            Files.delete(inputConfigFile.toPath());
        }
        if (backupConfigFile != null) {
            Files.delete(backupConfigFile.toPath());
        }
        if (backupDirectory != null) {
            FileUtils.deleteDirectory(
                new File(backupDirectory.getParent()));
        }
        if (outputAdminDir != null) {
            FileUtils.deleteDirectory(outputAdminDir);
        }
        LoggerUtils.closeAllHandlers();
    }

    /*
     * All of the tests are divided into following categories :
     *
     * [] Bad -config, -requiredfile, topologyfile and -hostname arguments,
     *    input config file, requiredfile, topology file existence check.
     * [] Incorrect/Unsupported value passed in input config file. We are
     *    supporting baseArchivePath and recoveryCopyClass in config file.
     * [] Generate requiredfiles.json and topologyoutput.json file after
     *    running ARTRequiredFiles and RecoverConfig on a 1x3 store having
     *    admin shard with scheduled backup enabled. With requiredfiles.json,
     *    topologyoutput.json and hostname as input argument, copy winner
     *    replication and admin node jdb files in storage directory location.
     */

    /**
     * Bad Arguments SNRecover tests
     * @throws Exception
     */
    @Test
    public void testBadArguments() throws Exception {
        testSNRecoverEmptyArgument();
        testSNRecoverNoConfigFlag();
        testSNRecoverNoRequiredFileFlag();
        testSNRecoverNoTopologyFileFlag();
        testSNRecoverNoHostNameFlag();
        testSNRecoverNoConfigArgument();
        testSNRecoverNoRequiredFileArgument();
        testSNRecoverNoTopologyFileArgument();
        testSNRecoverNoHostNameArgument();
        testSNRecoverEmptyConfigArgument();
        testSNRecoverEmptyRequiredFileArgument();
        testSNRecoverEmptyTopologyFileArgument();
        testSNRecoverEmptyHostNameArgument();
        testSNRecoverInvalidConfigPathArgument();
        testSNRecoverInvalidRequiredFileArgument();
        testSNRecoverInvalidTopologyFileArgument();
        testSNRecoverWrongArgument();
        testSNRecoverConfigFileNotExist();
        testSNRecoverRequiredFileNotExist();
        testSNRecoverTopologyFileNotExist();
    }

    private void testSNRecoverEmptyArgument() throws Exception {

        final String[] argv = {};
        final String message = "Empty argument list";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoConfigFlag() throws Exception {

        final String[] argv = {"-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname", "localhost"};
        final String message = "-config flag argument not specified";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoRequiredFileFlag() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname", "localhost"};
        final String message = "-requiredfile flag argument not specified";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoTopologyFileFlag() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-hostname", "localhost"};
        final String message = "-topologyfile flag argument not specified";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoHostNameFlag() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json"};
        final String message = "-hostname argument not specified";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoConfigArgument() throws Exception {

        final String[] argv = {"-config"};
        final String message = "-config requires an argument";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoRequiredFileArgument() throws Exception {

        final String[] argv = {"-requiredfile"};
        final String message = "-requiredfile requires an argument";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoTopologyFileArgument() throws Exception {

        final String[] argv = {"-topologyfile"};
        final String message = "-topologyfile requires an argument";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverNoHostNameArgument() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname"};
        final String message = "-hostname requires an argument";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverEmptyConfigArgument() throws Exception {

        final String[] argv = {"-config", ""};
        final String message =
            "Config file path name must not be empty";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverEmptyRequiredFileArgument() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", ""};
        final String message =
            "RequiredFile path must not be empty";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverEmptyTopologyFileArgument()
        throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-topologyfile", ""};
        final String message =
            "topology output json file path must not be empty";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverEmptyHostNameArgument() throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties",
             "-hostname", ""};
        final String message = "hostname of the storage node not be empty";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverInvalidConfigPathArgument()
        throws Exception {

        final String[] argv = {"-config", "tmp",
                               "-requiredfile", "/tmp/requiredfile.json"};
        final String message =
            "Config file path name must be an absolute path";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverInvalidRequiredFileArgument()
        throws Exception {

        final String[] argv = {"-requiredfile", "tmp"};
        final String message =
            "RequiredFile path must be an absolute path";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverInvalidTopologyFileArgument()
        throws Exception {

        final String[] argv = {"-topologyfile", "tmp"};
        final String message =
            "topology output json file path must be an absolute path";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverWrongArgument() throws Exception {

        final String[] argv = {"-abc"};
        final String message = "-abc is not a supported option.";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverConfigFileNotExist() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname", "localhost"};
        final File inputDir = new File("/tmp/recoverproperties");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }

        final String message = "Specified input config file " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateSNRecoverOutput(argv, message);
    }

    private void testSNRecoverRequiredFileNotExist() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname", "localhost"};
        final File inputConfigDir = new File("/tmp/recoverproperties");
        if (!inputConfigDir.exists()) {
            RecoverConfig.makeDir(inputConfigDir);
        }
        final File inputDir = new File("/tmp/requiredfile.json");
        if (inputDir.exists()) {
            assertTrue(FileUtils.deleteDirectory(inputDir));
        }

        final String message = "Specified requiredfile json file " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateSNRecoverOutput(argv, message);
        Files.delete(inputConfigDir.toPath());
    }

    private void testSNRecoverTopologyFileNotExist() throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfile", "/tmp/requiredfile.json",
                               "-topologyfile", "/tmp/topologyoutput.json",
                               "-hostname", "localhost"};
        final File inputConfigDir = new File("/tmp/recoverproperties");
        if (!inputConfigDir.exists()) {
            Files.createFile(inputConfigDir.toPath());
        }
        final File inputRequiredDir = new File("/tmp/requiredfile.json");
        if (!inputRequiredDir.exists()) {
            Files.createFile(inputRequiredDir.toPath());
        }
        final File inputDir = new File("/tmp/topologyoutput.json");
        if (inputDir.exists()) {
            assertTrue(FileUtils.deleteDirectory(inputDir));
        }

        final String message = "Specified topologyoutput json file " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateSNRecoverOutput(argv, message);
        Files.delete(inputConfigDir.toPath());
        Files.delete(inputRequiredDir.toPath());
    }

    private void validateSNRecoverOutput(String[] argument,
                                            String message) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        PrintStream originalSysErr = System.err;
        System.setErr(ps);
        try {
            assertFalse(SNRecover.mainInternal(argument));
            assertTrue(output.toString().contains(message));
        } finally {
            System.setErr(originalSysErr);
        }
    }

    /**
     * Invalid and Unsupported arguments in input config file tests
     * @throws Exception
     */
    @Test
    public void testInvalidInputConfigFileTests() throws Exception {

        /*
         * Currently in kv internal unit testing we will test recoverCopyClass
         * parameter in input config file.
         *
         * Object Storage specific parameters will be tested as part of
         * RecoverObjectStorageCopyTest in spartakv branch.
         *
         * TODO : Test needs to be updated as we support more parameters
         * in input config file.
         */
        testEmptyConfigFileCheck();
    }

    private void testEmptyConfigFileCheck() throws IOException {

        Properties props = new Properties();
        checkInvalidConfigFileProperties(props);
    }

    private void checkInvalidConfigFileProperties(Properties props)
        throws IOException {

        inputConfigFile = File.createTempFile("config", ".properties");
        final File requiredfile = new File("/tmp/requiredfile.json");
        requiredfile.createNewFile();
        final File topologyfile = new File("/tmp/topologyoutput.json");
        topologyfile.createNewFile();
        writeProps(props);
        final String [] argv = {"-config", inputConfigFile.toString(),
                                "-requiredfile", "/tmp/requiredfile.json",
                                "-topologyfile", "/tmp/topologyoutput.json",
                                "-hostname", "localhost"};

        SNRecover snrecover = new SNRecover();
        TestUtils.checkException(
            () -> snrecover.parseArgs(argv),
            IllegalArgumentException.class);
        Files.delete(requiredfile.toPath());
    }

    /**
     * Running SNRecover test. Check for running of SNRecover utility
     * and copy of winner admin and replication node jdb files at storage
     * directory. 
     * Also running SNRecover failure scenario. 
     * @throws Exception
     */
    @Test
    public void testSNRecoverTest() throws Exception {

        /*
         * --Create a 1x3 kvstore with admin shard
         * --Enable scheduled backups with time multiplier set
         * --Run ARTRequiredFiles Utility with given target
         *   recovery time and base directory
         * --Check for generation of art.json and
         *   <kvstore>_requiredfiles.json under target
         *   zip file.
         * --Run AdminRecover to copy the winner node admin
         *   jdb files identified in requiredfile.json.
         * --Run RecoverConfig on copied admin jdb file to
         *   generate the topologyoutput.json
         * --With requirefiles.json and topologyoutput.json, copy
         *   the jdb files for replication and admin node at
         *   respective storage directory.
         */

        /* Start a 1x3 store with admin shard */
        createStore = new CreateStore(STORE_NAME,
                                      5240, // random port to start at
                                      3, // numSNs
                                      3, // RF
                                      3, // numPartitions
                                      1, // capacity
                                      CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null,
                                      true,
                                      SECURITY_ENABLE);
        createStore.start();

        final CommandServiceAPI cs = createStore.getAdmin();
        final Parameters params = cs.getParameters();

        /*
         * Create je.backup.copyConfig properties file
         */
        backupConfigFile =
            File.createTempFile("backupconfig", ".properties");
        Properties copyProps = new Properties();
        copyProps.setProperty(BackupFSArchiveCopy.CHECKSUM_KEY, "SHA1");
        try (OutputStream out = new FileOutputStream(backupConfigFile)) {
            copyProps.store(out, "Copy config file");
        }

        /*
         * Create je.backup.locationConfig base directory
         */
        backupDirectory =
            new File("/tmp/backup/" + cs.getStoreName());
        RecoverConfig.makeDir(backupDirectory);

        /*
         * Setting up the schedule for backups. We are following
         * same method as done in BackupManagerTest.testBasics()
         */
        final String schedule = "0 * * * *";
        BackupManager.timeMultiplier = HOUR_MILLIS / 2000;
        CronScheduleParser parser =
            BackupManager.createSnapshotScheduleParser(schedule);

        /*
         * Re-starts all RNs and Admins with configProperties
         */
        final String jeParameterValue =
            "je.env.runBackup true; je.backup.schedule " + schedule + "; "
            + "je.backup.copyClass com.sleepycat.je.BackupFSArchiveCopy; "
            + "je.backup.copyConfig " + backupConfigFile.toString() + "; "
            + "je.backup.locationClass "
            + "com.sleepycat.je.BackupFileLocation; "
            + "je.backup.locationConfig "
            + backupDirectory.getAbsolutePath();
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.JE_MISC, jeParameterValue);
        final Set<AdminId> adminList = params.getAdminIds();

        /*
         * Change params for admin sequentially rather than using
         * createChangeAllAdminsPlan i.e --all-admins
         */
        int p;
        for (AdminId admin : adminList) {
            if (admin.toString().equals("admin1")) {
                /*
                 * We are setting backup specific parameters on
                 * two admins out of three in admin shard.
                 *
                 * Setting and restarting Master admin leading to
                 * some issues. In TODO List.
                 */
                continue;
            }
            p = cs.createChangeParamsPlan("changeAdminParams", admin, map);
            cs.approvePlan(p);
            cs.executePlan(p, false);
            cs.awaitPlan(p, 0, null);
            cs.assertSuccess(p);
        }

        /*
         * change params for RNs in one go using createChangeAllParamsPlan
         * i.e. --all-rns
         */
        p = cs.createChangeAllParamsPlan("changeAllParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /*
         * Wait until 10 ms (real time) after the start of the next
         * backup
         */
        BackupManager.sleepMs(
            parser.getDelayTime() + 10*BackupManager.timeMultiplier);

        /*
         * At this point with restart of the nodes i.e admins and RNs
         * scheduled backups have been started in backup Directory.
         * We need to arrive at a target recovery time so that zip
         * file can be generated.
         *
         * Wait until 90% of the way into the tenth backup period.
         * Tenth backup period has been selected on conservative
         * basis so that we can get positive sleep duration.
         */
        long firstBackup = parser.getTime() + parser.getDelayTime();
        long sleep = (long) (10.90 * parser.getInterval()) -
            /* Subtract time since start of first backup */
            (BackupManager.currentTimeMs() - firstBackup);
        BackupManager.sleepMs(sleep);

        /*
         * We can have current time as target recovery time because there
         * will be at least 9-10 backups taken by that time. So we should
         * be able to derive at an ART from current time.
         */
        final long currentTime = BackupManager.currentTimeMs();
        final String timeString = formatTime(currentTime);
        int idx = timeString.indexOf('-');
        final String targetRecoveryTime =
            timeString.substring(idx - 2, idx) +  /* year (last two digits) */
            timeString.substring(idx + 1, idx + 3)  + /* month */
            timeString.substring(idx + 4, idx + 6)  + /* date */
            timeString.substring(idx + 7, idx + 9); /* hour */

        /*
         * Create Recovery input config file with right parameters
         */
        Properties props = new Properties();
        props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "/tmp/copybackup");
        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                          jeRecoverCopyClass);

        inputConfigFile = File.createTempFile("recoverconfig", ".properties");
        writeProps(props);

        /*
         * Before running adminrecover, we need to change the directory
         * format of the backups because JE backups directory format is
         * different from cloud backups directory. Recovery enhancements
         * are for the cloud backups directory format.
         *
         *  JE : for admin1 : /tmp/backup/SNRecoverTest/1/...
         *       for rg1-rn1 : /tmp/backup/SNRecoverTest/rg1-rn1/....
         *
         *  Cloud : for admin1 : /tmp/backup/SNRecoverTest/admin1
         *          for rg1-rn1 : /tmp/backup/SNRecoverTest/rg1/rn1/...
         */
        File currentAdmin2 = new File("/tmp/backup/SNRecoverTest/2");
        File admin2 = new File("/tmp/copybackup/SNRecoverTest/admin2");
        RecoverConfig.makeDir(admin2);
        File currentAdmin3 = new File("/tmp/backup/SNRecoverTest/3");
        File admin3 = new File("/tmp/copybackup/SNRecoverTest/admin3");
        RecoverConfig.makeDir(admin3);
        File currentrg1rn1 = new File("/tmp/backup/SNRecoverTest/rg1-rn1");
        File rg1_rn1 = new File("/tmp/copybackup/SNRecoverTest/rg1/rn1");
        RecoverConfig.makeDir(rg1_rn1);
        File currentrg1rn2 = new File("/tmp/backup/SNRecoverTest/rg1-rn2");
        File rg1_rn2 = new File("/tmp/copybackup/SNRecoverTest/rg1/rn2");
        RecoverConfig.makeDir(rg1_rn2);
        File currentrg1rn3 = new File("/tmp/backup/SNRecoverTest/rg1-rn3");
        File rg1_rn3 = new File("/tmp/copybackup/SNRecoverTest/rg1/rn3");
        RecoverConfig.makeDir(rg1_rn3);

        FileUtils.copyDir(currentAdmin2, admin2);
        FileUtils.copyDir(currentAdmin3, admin3);
        FileUtils.copyDir(currentrg1rn1, rg1_rn1);
        FileUtils.copyDir(currentrg1rn2, rg1_rn2);
        FileUtils.copyDir(currentrg1rn3, rg1_rn3);

        final String [] argv = {"-targetRecoveryTime", targetRecoveryTime,
                                "-config", inputConfigFile.toString(),
                                "-target", "/tmp/ans.zip"};
        assertTrue(ARTRequiredFiles.mainInternal(argv));

        /*
         * ans.zip having artrequiredfiles directory has been generated.
         * Now unzip the ans.zip file.
         */
        final File outputDir = new File("/tmp");
        checkOutputDirectory(outputDir, "artrequiredfiles");

        /*
         * ans.zip has been generated. Now we will run adminrecover with
         * following parameters so that winner node admin jdb file has
         * been copied in the target path.
         *
         * -config <configuration file path> : inputConfigFile
         * -requiredfiles <artrequiredfiles directory path> :
         *   /tmp/artrequiredfiles
         * -target <output directory path> : /tmp/adminjdbfiles
         */
        outputAdminDir = new File("/tmp/adminjdbfiles");
        RecoverConfig.makeDir(outputAdminDir);

        Properties adminRecoverProps = new Properties();
        adminRecoverProps.setProperty(ARTRequiredFiles.BASEDIR_KEY,
                                      "/tmp/copybackup");
        adminRecoverProps.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                                      jeRecoverCopyClass);

        Files.delete(inputConfigFile.toPath());
        inputConfigFile =
            File.createTempFile("adminrecoverconfig", ".properties");
        writeProps(adminRecoverProps);

        final String [] adminrecover =
            {"-config", inputConfigFile.toString(),
             "-requiredfiles", "/tmp/artrequiredfiles",
             "-target", outputAdminDir.toString()};
        assertTrue(AdminRecover.mainInternal(adminrecover));

        /*
         * Output directory format will be /tmp/adminjdbfiles/<adminX>/.
         * In a 1x3 store, there will be single admin. adminDirCompletePath
         * will be the input parameter for RecoverConfig enhancement
         */
        String adminDirCompletePath = checkAdminJDBDirectory();

        /*
         * At this point we have admin jdb files copied from archive. Now
         * we will run RecoverConfig on admin jdb files and generate
         * topology output json file.
         *
         * -target path will be /tmp/recoverconfig.zip
         */
        String outputRecoverConfigZip = "/tmp/recoverconfig.zip";

        /*
         * Run recoverconfig utility on adminDirCompletePath and
         * check creation of topologyJSON output in config.zip file
         */
        String[] recoverConfigArgv = {"-input", adminDirCompletePath,
                                      "-target", outputRecoverConfigZip};
        String message = "Configuration information recovered "
                         + "successfully at " + outputRecoverConfigZip;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        System.setOut(ps);
        assertTrue(RecoverConfig.main1(recoverConfigArgv));
        assertTrue(output.toString().contains(message));

        /*
         * recoverconfig.zip having top level directory as recoverconfig
         * has been generated. Now unzip the recoverconfig.zip
         */
        final File recoverConfigOutputDir = new File("/tmp");
        checkOutputDirectory(recoverConfigOutputDir, "recoverconfig");

        /*
         * Before running SNRecover, delete current files in storage
         * directory of admin and replication nodes. Then files will be
         * recreated by SNRecover enhancement. We will check for existence
         * of the files under newly created storage directory after SNRecover
         * run.
         */
        List<StorageNodeId> snIds =
            cs.getTopology().getSortedStorageNodeIds();
        createStore.shutdown();
        for (StorageNodeId snId : snIds) {
            StorageNodeParams snp = params.get(snId);
            final File adminEnvPath = new File(snp.getRootDirPath() +
                                               "/" + STORE_NAME +
                                               "/" + snId.getFullName() + "/" +
                                               "admin" +
                                               String.valueOf(
                                                   snId.getStorageNodeId()) +
                                               "/" + "env" + "/");
            FileUtils.deleteDirectory(adminEnvPath);

            final File replicationNodeEnvPath =
                new File(snp.getRootDirPath() +
                         "/" + STORE_NAME +
                         "/" + snId.getFullName() +
                         "/" +  "rg1-rn" +
                         String.valueOf(snId.getStorageNodeId()) +
                         "/" + "env" + "/");
            FileUtils.deleteDirectory(replicationNodeEnvPath);
        }

        /*
         * Now we will run SNRecover enhancement. We have generated
         * requiredfiles.json and topologyoutput.json file.
         */
        final String [] snrecover =
            {"-config", inputConfigFile.toString(),
             "-requiredfile",
             "/tmp/artrequiredfiles/" + STORE_NAME + "_requiredfiles.json",
             "-topologyfile", "/tmp/recoverconfig/topologyoutput.json",
             "-hostname", "localhost", "-debug"};
        assertTrue(SNRecover.mainInternal(snrecover));
        /*
         * After running SNRecover, check files in storage directory of admin
         * and replication nodes. There should be one replication node and
         * one admin node (which are winner nodes in respective shard) with
         * jdb files copied from backup archive.
         */
        int numNodeFilesCopied = 0 ;
        for (StorageNodeId snId : snIds) {
            StorageNodeParams snp = params.get(snId);
            final File adminEnvPath = new File(snp.getRootDirPath() +
                                               "/" + STORE_NAME +
                                               "/" + snId.getFullName() + "/" +
                                               "admin" +
                                               String.valueOf(
                                                   snId.getStorageNodeId()) +
                                               "/" + "env" + "/");
            if (adminEnvPath.listFiles().length > 0) {
                numNodeFilesCopied++;
            }

            final File replicationNodeEnvPath =
                new File(snp.getRootDirPath() +
                         "/" + STORE_NAME +
                         "/" + snId.getFullName() +
                         "/" +  "rg1-rn" +
                         String.valueOf(
                             snId.getStorageNodeId()) +
                         "/" + "env" + "/");
            if (replicationNodeEnvPath.listFiles().length > 0) {
                numNodeFilesCopied++;
            }
        }
        assertTrue(numNodeFilesCopied >= 2);

        /* 
         * Now we are setting up environment to inject IllegalArgumentException
         * failure using MockRecoverObjectStorageCopy and validate the failure
         * output in SNRecover.
         */
        PrintStream originalSysErr = System.err;
        System.setErr(ps);
        adminRecoverProps.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                                      failingRecoverCopyClass);

        Files.delete(inputConfigFile.toPath());
        inputConfigFile = File.createTempFile("adminrecoverconfig", ".properties");
        writeProps(adminRecoverProps);

        /*
         * SNRecover should fail because of the error that will
         * happen when the environment files are copied in.
         *  - the call to mainInternal should return false.
         *  - the utility should print an error message showing
         * the problem.
         */
        final String [] snrecoverFailure =
            {"-config", inputConfigFile.toString(),
             "-requiredfile",
             "/tmp/artrequiredfiles/" + STORE_NAME + "_requiredfiles.json",
             "-topologyfile", "/tmp/recoverconfig/topologyoutput.json",
             "-hostname", "localhost", "-debug"};
        
        try {
            assertFalse(SNRecover.mainInternal(snrecoverFailure));
            assertTrue(output.toString().contains(
                FailingObjectStorageCopy.TEST_ERROR_MSG));
        } finally {
            System.setErr(originalSysErr);
        }
    }

    private String checkAdminJDBDirectory() {

        boolean isJDBFilesExist = false;
        String adminDirCompletePath = null;
        File[] outputDirFiles = outputAdminDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (outputFile.isDirectory() &&
                    outputFile.toString().contains("admin")) {
                File [] adminjdbFiles = outputFile.listFiles();
                for (File adminjdbFile : adminjdbFiles) {
                     if (adminjdbFile.toString().contains("jdb")) {
                         isJDBFilesExist = true;
                         adminDirCompletePath = outputFile.toString();
                         break;
                     }
                }
            }
        }
        assertTrue(isJDBFilesExist);
        return adminDirCompletePath;
    }

    private void checkOutputDirectory(File outputDir, String method)
        throws Exception {

        File[] outputDirFiles = outputDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (method.equals("artrequiredfiles") &&
                    outputFile.getName().equals("ans.zip") ||
                method.equals("recoverconfig") &&
                    outputFile.getName().equals("recoverconfig.zip")) {
                AdminRecoverTest.unzipDirectory(outputFile);
            }
        }
    }

    private void writeProps(Properties props)
        throws IOException, FileNotFoundException {

        try (OutputStream configStream =
                 new FileOutputStream(inputConfigFile)) {
            props.store(configStream, "Copy config file");
        }
    }

    /** Format the date in human readable format. */
    static String formatTime(final long millis) {
        synchronized (dateFormat) {
            return dateFormat.format(new Date(millis));
        }
    }
}
