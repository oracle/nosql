/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.recovery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.TestUtils;
import oracle.kv.util.recovery.ARTRequiredFiles;
import oracle.kv.util.recovery.AdminRecover;
import oracle.kv.util.recovery.RecoverConfig;

import com.sleepycat.je.BackupFSArchiveCopy;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.utilint.CronScheduleParser;

import org.junit.Test;

/**
 * AdminRecover Utility test
 */
public class AdminRecoverTest extends TestBase {

    private final static String STORE_NAME = "AdminRecoverTest";
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
         * Delete temporary created files from previous runs
         */
        final File ansZipFile = new File("/tmp/ans.zip");
        if (ansZipFile.exists()) {
            Files.delete(ansZipFile.toPath());
        }
        final File artFileDir = new File("/tmp/artrequiredfiles");
        if (artFileDir.exists()) {
            FileUtils.deleteDirectory(artFileDir);
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
     * [] Bad -config, -requiredfiles and -target arguments, input config
     *    file and requiredfiles directory existence check.
     * [] Incorrect/Unsupported value passed in input config file. We are
     *    supporting baseArchivePath and recoveryCopyClass in config file.
     * [] Generate requiredfiles.json after running ARTRequiredFiles on a
     *    1x3 store having admin shard with scheduled backup enabled. With
     *    requiredfiles.json as input argument, copy winner admin node jdb
     *    files in target location.
     */

    /**
     * Bad Arguments AdminRecover tests
     * @throws Exception
     */
    @Test
    public void testBadArguments() throws Exception {
        testAdminRecoverEmptyArgument();
        testAdminRecoverNoConfigFlag();
        testAdminRecoverNoRequiredFilesFlag();
        testAdminRecoverNoTargetFlag();
        testAdminRecoverNoConfigArgument();
        testAdminRecoverNoRequiredFilesArgument();
        testAdminRecoverNoTargetArgument();
        testAdminRecoverEmptyConfigArgument();
        testAdminRecoverEmptyRequiredFilesArgument();
        testAdminRecoverEmptyTargetArgument();
        testAdminRecoverInvalidConfigPathArgument();
        testAdminRecoverInvalidRequiredFilesArgument();
        testAdminRecoverInvalidTargetPathArgument();
        testAdminRecoverWrongArgument();
        testAdminRecoverConfigFileNotExist();
        testAdminRecoverRequiredFilesDirNotExist();
    }

    private void testAdminRecoverEmptyArgument()
            throws Exception {

        final String[] argv = {};
        final String message = "Empty argument list";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoConfigFlag()
            throws Exception {

        final String[] argv = {"-target", "/tmp/output",
                               "-requiredfiles", "/tmp/requiredfiles"};
        final String message = "-config flag argument not specified";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoRequiredFilesFlag()
            throws Exception {

        final String[] argv = {"-target", "/tmp/output",
                               "-config", "/tmp/recoverproperties"};
        final String message = "-requiredfiles flag argument not specified";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoTargetFlag()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfiles", "/tmp/requiredfiles"};
        final String message = "-target flag argument not specified";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoConfigArgument()
            throws Exception {

        final String[] argv = {"-config"};
        final String message = "-config requires an argument";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoRequiredFilesArgument()
            throws Exception {

        final String[] argv = {"-requiredfiles"};
        final String message = "-requiredfiles requires an argument";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverNoTargetArgument()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfiles", "/tmp/requiredfiles",
                               "-target"};
        final String message = "-target requires an argument";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverEmptyConfigArgument()
            throws Exception {

        final String[] argv = {"-config", "", "-target"};
        final String message =
            "Config file path name must not be empty";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverEmptyRequiredFilesArgument()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfiles", "",
                               "-target"};
        final String message =
            "RequiredFiles directory path must not be empty";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverEmptyTargetArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties",
             "-requiredfiles", "/tmp/requiredfiles",
             "-target", ""};
        final String message = "Target path must not be empty";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverInvalidConfigPathArgument()
            throws Exception {

        final String[] argv = {"-config", "tmp",
                               "-requiredfiles", "/tmp/requiredfiles",
                               "-target"};
        final String message =
            "Config file path name must be an absolute path";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverInvalidRequiredFilesArgument()
            throws Exception {

        final String[] argv = {"-requiredfiles", "tmp", "-target"};
        final String message =
            "RequiredFiles directory path must be an absolute path";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverInvalidTargetPathArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties",
             "-requiredfiles", "/tmp/requiredfiles",
             "-target", "tmp"};
        final String message ="Target path must be an absolute path";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverWrongArgument()
            throws Exception {

        final String[] argv = {"-abc"};
        final String message = "-abc is not a supported option.";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverConfigFileNotExist()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfiles", "/tmp/requiredfiles",
                               "-target", "/tmp/output/ans.zip"};
        final File inputDir = new File("/tmp/recoverproperties");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }

        final String message = "Specified input config file " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateAdminRecoverOutput(argv, message);
    }

    private void testAdminRecoverRequiredFilesDirNotExist()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-requiredfiles", "/tmp/requiredfiles",
                               "-target", "/tmp/output/ans.zip"};
        final File inputConfigDir = new File("/tmp/recoverproperties");
        if (!inputConfigDir.exists()) {
            RecoverConfig.makeDir(inputConfigDir);
        }
        final File inputDir = new File("/tmp/requiredfiles");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }

        final String message = "Specified required json files directory " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateAdminRecoverOutput(argv, message);
        Files.delete(inputConfigDir.toPath());
    }

    private void validateAdminRecoverOutput(String[] argument,
                                            String message) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        PrintStream originalSysErr = System.err;
        System.setErr(ps);
        try {
            assertFalse(AdminRecover.mainInternal(argument));
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
        final File requiredfiles = new File("/tmp/requiredfiles");
        RecoverConfig.makeDir(requiredfiles);
        writeProps(props);
        final String [] argv = {"-config", inputConfigFile.toString(),
                                "-requiredfiles", requiredfiles.toString(),
                                "-target", "/tmp/adminrecover"};

        AdminRecover adminRecover= new AdminRecover();
        TestUtils.checkException(
            () -> adminRecover.parseArgs(argv),
            IllegalArgumentException.class);
        Files.delete(requiredfiles.toPath());
    }

    /**
     * Running AdminRecover test. Check for running of AdminRecover utility
     * and copy of winner admin node jdb files at target path. Check for
     * presence of admin jdb files under target directory.
     * Also running AdminRecover failure scenario.
     * @throws Exception
     */
    @Test
    public void testAdminRecoverTest() throws Exception {

        /*
         * --Create a 1x3 kvstore with admin shard
         * --Enable scheduled backups with time multiplier set
         * --Run ARTRequiredFiles Utility with given target
         *   recovery time and base directory
         * --Get the artrequirefiles directory path.
         * --Check into requiredfiles.json and copy admin
         *   winner node jdb files in target directory
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
         * Before running adminrecover, we need to change the directory
         * format of the backups because JE backups directory format is
         * different from cloud backups directory. Recovery enhancements
         * are for the cloud backups directory format.
         *
         *  JE : for admin1 : /tmp/backup/AdminRecoverTest/1/...
         *       for rg1-rn1 : /tmp/copybackup/AdminRecoverTest/rg1-rn1/....
         *
         *  Cloud : for admin1 : /tmp/backup/AdminRecoverTest/admin1
         *          for rg1-rn1 : /tmp/copybackup/AdminRecoverTest/rg1/rn1/...
         *
         */
        File currentAdmin2 = new File("/tmp/backup/AdminRecoverTest/2");
        File admin2 = new File("/tmp/copybackup/AdminRecoverTest/admin2");
        RecoverConfig.makeDir(admin2);
        File currentAdmin3 = new File("/tmp/backup/AdminRecoverTest/3");
        File admin3 = new File("/tmp/copybackup/AdminRecoverTest/admin3");
        RecoverConfig.makeDir(admin3);
        File currentrg1rn1 = new File("/tmp/backup/AdminRecoverTest/rg1-rn1");
        File rg1_rn1 = new File("/tmp/copybackup/AdminRecoverTest/rg1/rn1");
        RecoverConfig.makeDir(rg1_rn1);
        File currentrg1rn2 = new File("/tmp/backup/AdminRecoverTest/rg1-rn2");
        File rg1_rn2 = new File("/tmp/copybackup/AdminRecoverTest/rg1/rn2");
        RecoverConfig.makeDir(rg1_rn2);
        File currentrg1rn3 = new File("/tmp/backup/AdminRecoverTest/rg1-rn3");
        File rg1_rn3 = new File("/tmp/copybackup/AdminRecoverTest/rg1/rn3");
        RecoverConfig.makeDir(rg1_rn3);

        FileUtils.copyDir(currentAdmin2, admin2);
        FileUtils.copyDir(currentAdmin3, admin3);
        FileUtils.copyDir(currentrg1rn1, rg1_rn1);
        FileUtils.copyDir(currentrg1rn2, rg1_rn2);
        FileUtils.copyDir(currentrg1rn3, rg1_rn3);

        /*
         * Create Recovery input config file with right parameters
         */
        Properties props = new Properties();
        props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "/tmp/copybackup");
        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                          jeRecoverCopyClass);

        inputConfigFile = File.createTempFile("recoverconfig", ".properties");
        writeProps(props);

        final String [] argv = {"-targetRecoveryTime", targetRecoveryTime,
                                "-config", inputConfigFile.toString(),
                                "-target", "/tmp/ans.zip", "-debug"};
        assertTrue(ARTRequiredFiles.mainInternal(argv));

        /*
         * ans.zip having top level directory as artrequiredfiles directory
         * has been generated. Now unzip the ans.zip file under /tmp.
         */
        final File outputDir = new File("/tmp");
        checkOutputDirectory(outputDir);

        /*
         * ans.zip has been generated. Now we will run adminrecover with
         * following parameters so that winner node admin jdb file has
         * been copied in the target path.
         *
         * -config <configuration file path> : inputConfigFile
         * -requiredfiles <artrequiredfiles directory path> :
         *      /tmp/artrequiredfiles/
         * -target <output directory path> : /tmp/adminjdbfiles
         */
        outputAdminDir = new File("/tmp/adminjdbfiles");
        RecoverConfig.makeDir(outputAdminDir);

        final String [] adminrecover =
            {"-config", inputConfigFile.toString(),
             "-requiredfiles", "/tmp/artrequiredfiles",
             "-target", outputAdminDir.toString()};
        assertTrue(AdminRecover.mainInternal(adminrecover));

        /*
         * Output directory format will be /tmp/adminjdbfiles/<adminX>/.
         * In a 1x3 store, there will be single winner node admin whose
         * jdb file would have been copied.
         */
        checkAdminJDBDirectory();

        /* 
         * Now we are setting up environment to inject IllegalArgumentException
         * failure using MockRecoverObjectStorageCopy and validate the failure
         * output in AdminRecover.
         */
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        PrintStream originalSysErr = System.err;
        System.setErr(ps);

        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                          failingRecoverCopyClass);
        inputConfigFile = File.createTempFile("recoverconfig", ".properties");
        writeProps(props);

        /*
         * AdminRecover should fail because of the error that will
         * happen when the environment files are copied in.
         *  - the call to mainInternal should return false.
         *  - the utility should print an error message showing
         * the problem.
         */
        final String [] adminrecoverFailure =
            {"-config", inputConfigFile.toString(),
             "-requiredfiles", "/tmp/artrequiredfiles",
             "-target", outputAdminDir.toString()};
             
        try {
            assertFalse(AdminRecover.mainInternal(adminrecoverFailure));
            assertTrue(output.toString().contains(
                FailingObjectStorageCopy.TEST_ERROR_MSG));
        } finally {
            System.setErr(originalSysErr);
        }
    }

    private void checkAdminJDBDirectory() {

        boolean isJDBFilesExist = false;
        File[] outputDirFiles = outputAdminDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (outputFile.isDirectory() &&
                    outputFile.toString().contains("admin")) {
                File [] adminjdbFiles = outputFile.listFiles();
                for (File adminjdbFile : adminjdbFiles) {
                    if (adminjdbFile.toString().contains("jdb")) {
                        isJDBFilesExist = true;
                        break;
                    }
                }
            }
        }
        assertTrue(isJDBFilesExist);
    }

    private void checkOutputDirectory(File outputDir) throws Exception {

        File[] outputDirFiles = outputDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (outputFile.getName().equals("ans.zip")) {
                unzipDirectory(outputFile);
            }
        }
    }

    public static void unzipDirectory(File outputFile) throws Exception {

        final FileInputStream finput =
            new FileInputStream(outputFile.toString());
        final ZipInputStream zipInput = new ZipInputStream(finput);
        ZipEntry zipFile = zipInput.getNextEntry();
        while (zipFile != null) {
            String zipFilePath = "/tmp" + File.separator + zipFile.getName();
            if (!zipFile.isDirectory()) {
                final File newFile = new File(zipFilePath);
                RecoverConfig.makeDir(newFile.getParentFile());
                Files.createFile(newFile.toPath());
                unzipFile(zipInput, zipFilePath);
            } else {
                final File zipFileDir = new File(zipFilePath);
                RecoverConfig.makeDir(zipFileDir);
            }
            zipInput.closeEntry();
            zipFile = zipInput.getNextEntry();
        }
        zipInput.close();
    }

    private static void unzipFile(ZipInputStream zipIn, String filePath)
        throws IOException {

        final int BUFFER_SIZE = 4096;
        final FileOutputStream fileInput = new FileOutputStream(filePath);
        BufferedOutputStream outputStream =
            new BufferedOutputStream(fileInput);
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            outputStream.write(bytesIn, 0, read);
        }
        outputStream.close();
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
