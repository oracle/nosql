/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.recovery;

import static org.junit.Assert.assertEquals;
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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
import oracle.kv.util.recovery.RecoverConfig;

import com.sleepycat.je.BackupFSArchiveCopy;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.utilint.CronScheduleParser;

import org.junit.Test;

/**
 * ARTRequiredFiles Utility test
 */
public class ARTRequiredFilesTest extends TestBase {

    private CreateStore createStore = null;

    private final static String jeRecoverCopyClass =
        "com.sleepycat.je.RecoverArchiveFSCopy";

    private static final long HOUR_MILLIS = 60 * 60 * 1000;

    /**
     * Date formatter to print dates in human readable format.  Synchronize on
     * this object when using it.
     */
    private static final SimpleDateFormat dateFormat =
        new SimpleDateFormat("dd:MM:yy:HH:mm:ss");
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private File inputConfigFile;

    @Override
    public void setUp()
        throws Exception {
        super.setUp();
        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /*
     * All of the tests are divided into following categories :
     *
     * [] Bad -input and -target arguments, input config file existence check
     * [] Incorrect/Unsupported value passed in input config file
     * [] Generate ans.zip file from a 1x3 store having admin shard with
     *    scheduled backup enabled. Check for art.json and
     *    <kvstore>_requiredfiles.json in ans.zip
     */

    /**
     * Bad Arguments ARTRequiredFiles tests
     * @throws Exception
     */
    @Test
    public void testBadArguments() throws Exception {
        testARTRequiredFilesEmptyArgument();
        testARTRequiredFilesNoInputFlag();
        testARTRequiredFilesNoOutputFlag();
        testARTRequiredFilesNoTargetRecoveryTimeFlag();
        testARTRequiredFilesNoInputArgument();
        testARTRequiredFilesNoOutputArgument();
        testARTRequiredFilesNoTargetRecoveryTimeArgument();
        testARTRequiredFilesEmptyInputArgument();
        testARTRequiredFilesEmptyOutputArgument();
        testARTRequiredFilesEmptyTargetRecoveryTimeArgument();
        testARTRequiredFilesInvalidInputPathArgument();
        testARTRequiredFilesInvalidOutputPathArgument();
        testARTRequiredFilesInvalidOutputArgument();
        testARTRequiredFilesInvalidTargetRecoveryTimeArgument();
        testARTRequiredFilesWrongArgument();
        testARTRequiredFilesInputDirNotExist();
    }

    private void testARTRequiredFilesEmptyArgument()
            throws Exception {

        final String[] argv = {};
        final String message = "Empty argument list";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoInputFlag()
            throws Exception {

        final String[] argv = {"-target", "/tmp/ans.zip",
                               "-targetRecoveryTime", "18092106"};
        final String message = "-config flag argument not specified";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoOutputFlag()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-targetRecoveryTime", "18092106"};
        final String message = "-target flag argument not specified";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoTargetRecoveryTimeFlag()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-target", "/tmp/ans.zip"};

        final String message = "-targetRecoveryTime missing from input "
                               + "parameters";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoInputArgument()
            throws Exception {

        final String[] argv = {"-config"};
        final String message = "-config requires an argument";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoOutputArgument()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties", "-target"};
        final String message = "-target requires an argument";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesNoTargetRecoveryTimeArgument()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-target", "/tmp/ans.zip",
                               "-targetRecoveryTime"};
        final String message = "-targetRecoveryTime requires an argument";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesEmptyInputArgument()
            throws Exception {

        final String[] argv = {"-config", "", "-target"};
        final String message =
            "Config file path name must not be empty";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesEmptyOutputArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties", "-target", ""};
        final String message = "Target path must not be empty";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesEmptyTargetRecoveryTimeArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties",
             "-target", "/tmp/ans.zip",
             "-targetRecoveryTime", ""};
        final String message = "Target Recovery Time must not be empty";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesInvalidInputPathArgument()
            throws Exception {

        final String[] argv = {"-config", "tmp", "-target"};
        final String message =
            "Config file path name must be an absolute path";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesInvalidOutputPathArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverproperties", "-target", "tmp"};
        final String message ="Target path must be an absolute path";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesInvalidOutputArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverProperties", "-target", "/tmp"};
        final String message ="Target path must end with '.zip'";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesInvalidTargetRecoveryTimeArgument()
            throws Exception {

        final String[] argv =
            {"-config", "/tmp/recoverProperties",
             "-target", "/tmp/ans.zip",
             "-targetRecoveryTime", "1809210809"};
        final String message = "targetRecoveryTime should be in YYMMDDHH "
                               + "format";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesWrongArgument()
            throws Exception {

        final String[] argv = {"-abc"};
        final String message = "-abc is not a supported option.";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void testARTRequiredFilesInputDirNotExist()
            throws Exception {

        final String[] argv = {"-config", "/tmp/recoverproperties",
                               "-target", "/tmp/output/ans.zip",
                               "-targetRecoveryTime", "18092108"};
        final File inputDir = new File("/tmp/recoverproperties");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }

        final String message = "Specified input config file " +
                               inputDir.getAbsolutePath() +
                               " does not exist";
        validateARTRequiredFilesOutput(argv, message);
    }

    private void validateARTRequiredFilesOutput(String[] argument,
                                             String message) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        PrintStream originalSysErr = System.err;
        System.setErr(ps);
        try {
            assertFalse(ARTRequiredFiles.mainInternal(argument));
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
         * Currently in kv internal unit testing we will test four
         * parameters in input config file i.e base Directory,
         * target recovery time, recovercopyclass and check sum algorithm.
         *
         * Object Storage specific parameters will be tested as part of
         * RecoverObjectStorageCopyTest in spartakv branch.
         *
         * TODO : Test needs to be updated as we support more parameters
         * in input config file.
         */
        testEmptyConfigFileCheck();
        testNoBaseDirectoryCheck();
        testInvalidBaseDirectoryCheck();
        testNoRecoverCopyClassCheck();
        testInvalidRecoverCopyClassCheck();
    }

    private void testInvalidRecoverCopyClassCheck() throws IOException {

        Properties props = new Properties();
        /*
         * recover copy class should be in jeRecoverCopyClass format
         */
        props.setProperty(BackupFSArchiveCopy.CHECKSUM_KEY, "SHA1");
        props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "/backup");
        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
            "com.sleepycat.je.RecoverFSArchiveCopy");
        validateConfigFileProperties(props);
    }

    private void testNoRecoverCopyClassCheck() throws IOException {

        Properties props = new Properties();
        props.setProperty(BackupFSArchiveCopy.CHECKSUM_KEY, "SHA1");
        props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "/backup");
        validateConfigFileProperties(props);
    }

    private void testInvalidBaseDirectoryCheck() throws IOException {

        Properties props = new Properties();
        /*
         * base Directory should be an absolute path
         */
        props.setProperty(BackupFSArchiveCopy.CHECKSUM_KEY, "SHA1");
        props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "backup");
        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                          jeRecoverCopyClass);
        validateConfigFileProperties(props);
    }

    private void testNoBaseDirectoryCheck() throws IOException {

        Properties props = new Properties();
        props.setProperty(BackupFSArchiveCopy.CHECKSUM_KEY, "SHA1");
        props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                          jeRecoverCopyClass);
        validateConfigFileProperties(props);
    }

    private void testEmptyConfigFileCheck() throws IOException {

        Properties props = new Properties();
        validateConfigFileProperties(props);
    }

    private void validateConfigFileProperties(Properties props)
        throws IOException {

        inputConfigFile = File.createTempFile("config", ".properties");
        writeProps(props);
        final String [] argv = {"-targetRecoveryTime", "18082105",
                                "-config", inputConfigFile.toString(),
                                "-target", "/tmp/ans.zip"};

        ARTRequiredFiles artRequiredFiles = new ARTRequiredFiles();
        TestUtils.checkException(
            () -> artRequiredFiles.parseArgs(argv),
            IllegalArgumentException.class);
        Files.delete(inputConfigFile.toPath());
    }

    /**
     * Running ARTRequiredFiles test. Check for running of
     * ARTRequiredFiles utility and generation of target
     * zip file.
     * @throws Exception
     */
    @Test
    public void testARTRequiredFilesTest() throws Exception {

        /*
         * --Create a 1x3 kvstore with admin shard
         * --Enable scheduled backups with time multiplier set
         * --Run ARTRequiredFiles Utility with given target
         *   recovery time and base directory
         * --Check for generation of art.json and
         *   <kvstore>_requiredfiles.json under target
         *   zip file.
         */

        /* Start a 1x3 store with admin shard */
        File backupConfigFile = null;
        File backupDirectory= null;
        try {
            createStore = new CreateStore(kvstoreName,
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
            final String [] timeParts = formatTime(currentTime).split(":");
            final String targetRecoveryTime =
                timeParts[2] + timeParts[1] + timeParts[0] + timeParts[3];

            /*
             * Create Input config file with right parameters
             */
            Properties props = new Properties();
            props.setProperty(ARTRequiredFiles.BASEDIR_KEY, "/tmp/backup");
            props.setProperty(ARTRequiredFiles.RECOVERY_COPY_CLASS_KEY,
                              jeRecoverCopyClass);

            inputConfigFile = File.createTempFile("recoverconfig",
                                                  ".properties");
            writeProps(props);
            final String [] argv = {"-targetRecoveryTime", targetRecoveryTime,
                                    "-config", inputConfigFile.toString(),
                                    "-target", "/tmp/ans.zip"};
            assertTrue(ARTRequiredFiles.mainInternal(argv));

            /*
             * Check for content of /tmp/ans.zip file. It will have
             * art.json and <kvstore>_requiredfiles.json.
             *
             * Since value inside these files can be dynamic hence
             * current test only checks for availability for these
             * files.
             *
             * Verifying calculated art to be less than target recovery
             * time and checking if all shards for a kvstore are covered
             * in requiredfiles.json from getting topology information
             * from params is a part of TODO list.
             */
            final File outputDir = new File("/tmp");
            checkOutputDirectory(outputDir);
        } finally {
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
        }
    }

    private void checkOutputDirectory(File outputDir) throws Exception {

        File[] outputDirFiles = outputDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (outputFile.getName().equals("ans.zip")) {
                checkZipDirectory(outputFile);
            }
        }
    }

    private void checkZipDirectory(File outputFile) throws Exception {
        int numARTJSON = 0;
        int numRequiredFiles = 0;
        Set <String> numFiles = new HashSet<>();

        try {
            ZipFile configZipFile = new ZipFile(outputFile.getAbsolutePath());
            Enumeration <? extends ZipEntry> configEntries =
                configZipFile.entries();
            while (configEntries.hasMoreElements()) {
                ZipEntry configEntry = configEntries.nextElement();
                String entryName = configEntry.getName();
                if (entryName.contains("art.json")) {
                    numARTJSON++;
                } else if (entryName.contains("requiredfiles.json")) {
                    numRequiredFiles++;
                }
                numFiles.add(entryName);
            }
            configZipFile.close();
        } catch (IOException e) {
           throw new Exception ("Error reading ans.zip file" + e);
        }

        /*
         * art.json will be generated
         */
        assertEquals(numARTJSON, 1);

        /*
         * one entry for requiredfiles.json
         */
        assertEquals(numRequiredFiles, 1);

        /*
         * total number of files generated under ans.zip will be two
         */
        assertEquals(numFiles.size(), 2);
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
