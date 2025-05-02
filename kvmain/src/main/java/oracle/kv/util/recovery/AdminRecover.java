/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.util.recovery;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;

import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.nosql.common.json.JsonUtils;

import com.google.gson.reflect.TypeToken;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * AdminRecover is designed to copy admin jdb files of winner node in an admin
 * shard for all kvstores in nosql cloud service.
 *
 * <p>Winner node in an admin shard is the node having successful backup and
 * highest vlsn. If there are multiple nodes having successful backup and same
 * vlsn then master node is considered as winner node. If there is no master
 * node in above conditions, then node having earlier name will be considered
 * as winner node. For more details around winner node calculation, refer
 * ARTRequiredFiles documentation.
 *
 * <p>Admin winner node information in an admin shard and archive paths of
 * jdb files to be copied are derived by running ARTRequiredFiles utility.
 * For each kvstore, admin winner node information is in
 * <i>kvstore</i>_requiredfiles.json in following format. jdb files mentioned
 * in admin section are copied from backup archive in local file system using
 * AdminRecover.
 *
 * {@literal
 *     <kvstore>_requirefiles.json
 *     {
 *         "admin" : {
 *             "winnerNode" : "admin1",
 *             "jdbFilesList" : [
 *                 {
 *                     "fileName" : "00000002.jdb",
 *                     "filePath" :
 *                         "/backup1/kvstore/admin1/19011014/00000002.jdb",
 *                     "checkSum" :
 *                         "71eea00d90575cf4f256d932bae36e0de67ef887",
 *                     "checkSumAlg" : "SHA1",
 *                     "encryptionAlg" : "NONE",
 *                     "compressionAlg" : "NONE"
 *                 },
 *                 {
 *                     "fileName" : "000003.jdb",
 *                     "filePath" :
 *                         "/backup1/kvstore/admin1/19011015/000003.jdb",
 *                     "checkSum" :
 *                         "5eed2da5fb5f2ad8b4a155dc0848b8c518774c47",
 *                     "checkSumAlg" : "SHA1",
 *                     "encryptionAlg" : "NONE",
 *                     "compressionAlg" : "NONE"
 *                 }
 *             ]
 *         }
 *     }
 * }
 *
 * <p>Input parameters for AdminRecover are :
 *
 * <p>Path to config file which will contain information about base archiveURL
 * and RecoveryCopyClass. It will also contain the credential needed to
 * access the archive. The format of the config file will be simple key-value
 * format. Example for Input config file is as follows :
 *
 * {@literal
 *   ### This is configuration of AdminRecover tool ###
 *
 *   ### Required properties #####
 *
 *   # Recovery copy class which is used in object storage end.
 *   recoveryCopyClass = com.sleepycat.je.RecoverArchiveFSCopy
 *
 *   # In actual AdminRecover read me/doc, this will be a constant
 *   # value i.e oracle.nosql.objectstorage.backup.RecoverObjectStorageCopy
 *
 *   ### Optional properties ###
 *
 *   ### Not support properties ###
 *
 *   # At this point in kv, we are only supporting recoveryCopyClass in
 *   # config file.
 * }
 *
 * <p>Directory path of unzipped artrequiredfiles directory. ARTRequiredFiles
 * generates a zip file having top directory as artrequiredfiles. If the
 * generated zip file is unzipped under /tmp then input directory path for
 * AdminRecover will be /tmp/artrequiredfiles
 *
 * {@literal
 *   ans.zip : {
 *     artrequiredfiles /
 *       <kvstore1>_requiredfiles.json
 *       <kvstore2>_requiredfiles.json
 *       <kvstore3>_requiredfiles.json
 *       art.json
 *   }
 * }
 *
 * <p>Output directory path is passed as one of the argument under which
 * admin jdb files for each kvstore are copied from backup archive. If
 * output directory path is /tmp/outputadminrecover then output directory
 * structure after running AdminRecover will be as follow :
 *
 * {@literal
 *  tmp /
 *    outputadminrecover /
 *      <kvstore1>admin[X] /
 *          0001.jdb
 *          0002.jdb
 *      <kvstore2>admin[Y] /
 *          0001.jdb
 *          0002.jdb
 *      <kvstore3>admin[Z] /
 *          0001.jdb
 *          0002.jdb
 * }
 *
 * <p>For running this utility, user needs to have recovery.jar in
 * classpath. -debug flag can be used to print the stack trace
 * in exception scenario. Usage details are as follows :
 *
 * {@literal
 * java -jar <recovery.jar> adminrecover
 *      -config <configuration file path>
 *      -requiredfiles <artrequiredfiles directory path>
 *      -target <output directory path>
 *      [ -debug ]
 * }
 */

public class AdminRecover extends AdminSNRecover {

    public static final String COMMAND_NAME = "adminrecover";
    public static final String COMMAND_DESC =
        "Copies winner node admin jdb files from backup archive for each"
        + " kvstore in local filesystem under specified target directory.";
    public static final String COMMAND_ARGS =
        "-config <configuration file path>" +
        " -requiredfiles <required files directory path>" +
        " -target <output directory path>" +
        " [ -debug ]";

    private static volatile Throwable threadError = null;
    private File requiredFilesPath = null;
    private File targetPath = null;

    private final UncaughtExceptionHandler exceptionHandler =
        new AdminRecoverExceptionHandler();

    private void copyAdminsJDBFiles() throws InterruptedException {
        /*
         * Get list of files under requiredFilesPath i.e.
         * <kvstore[X]>_requiredfiles.json under requiredFilesPath and
         * initiate dedicated adminCopyThread for each
         * <kvstore[X]_requiredfiles.json
         */
        final File[] requiredFiles = requiredFilesPath.listFiles();
        for (File requiredFile : requiredFiles) {
            if (requiredFile.getName().endsWith("requiredfiles.json")) {
                /*
                 * We have kvstore[X]_requiredfiles.json file for a kvstore.
                 * Initiate de-serializing of json file, prepare output admin
                 * directory and copy required admin jdb files from backup
                 * archive to local file system.
                 */
                AdminCopyThread adminCopyThread = null;
                try {
                    adminCopyThread = new AdminCopyThread(requiredFile);
                    adminCopyThread.start();
                } finally {
                    if (adminCopyThread != null) {
                        adminCopyThread.join();
                    }
                }
            }
        }
    }

    /*
     * Uncaught exception handler for AdminRecover adminCopyThread.
     */
    private class AdminRecoverExceptionHandler
            implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.log(Level.SEVERE, 
                "AdminRecover failed unexpectedly due to a fault " +
                "in thread " + t.getName(), e);
            threadError = e;
        }
     }

    /**
     * The thread that copies admin jdb files under requiredFile.json in
     * created output admin directory. This thread is only created when
     * we identify requirefiles.json under requiredFiles input Path.
     *
     * TODO : Need to check on below comment.
     * Does there need to be a way to shutdown this thread and have the
     * thread notice that it is being shutdown?
     *
     * @see #run
     */
    public class AdminCopyThread extends StoppableThread {
        private final File requiredJSONFile;
        AdminCopyThread(final File requiredJSONFile) {
            super(null, exceptionHandler, "AdminCopy");
            this.requiredJSONFile = requiredJSONFile;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        /**
         * Copies admin jdb files from backup archive to local file system
         * under target path admin directory. Winner node admin jdb files
         * are copied sequentially.
         *
         * @see #copyAdminJDBFiles
         */
        @Override
        public void run() {
            long retryWait = INITIAL_RETRY_WAIT_MS;
            /*
             * TODO : Check for shutdown?
             */
            while (true) {
                try {
                    copyAdminJDBFiles(requiredJSONFile);
                    break;
                } catch (IOException|InterruptedException|RuntimeException e) {
                    String errorMessage = "Exception in copying winner node" +
                        " admin jdb files in " + requiredJSONFile + " " +
                        CommonLoggerUtils.getStackTrace(e);
                    try {
                        BackupManager.sleepMs(retryWait);
                    } catch (InterruptedException e1) {
                        errorMessage += " " +
                            CommonLoggerUtils.getStackTrace(e1);
                    }
                    if (adminSNRecoverException != null) {
                        errorMessage += " " +
                            CommonLoggerUtils.getStackTrace(
                                adminSNRecoverException);
                        threadError = adminSNRecoverException;
                    } else {
                        threadError = e;
                    }
                    logger.info(errorMessage);
                    if (e instanceof RuntimeException) {
                       break;
                    }
                    retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
                }
            }
        }
    }

    /**
     * Copy winner node admin jdb files under requiredFiles json. First
     * de-serialize the json file. Identify admin node. Create
     * targetpath/adminId directory. Start copying admin jdb files sequentially
     * using instantiated RecoverFileCopy object.
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException
     */
    private void copyAdminJDBFiles(File requiredFileJSON)
        throws IOException, InterruptedException {

        /*
         * De-serialize the requiredfiles.json.
         */
        Map<String, NodeJDBFiles> shardNodeJDBFilesMap;
        try (FileReader reader = new FileReader(requiredFileJSON)) {
            java.lang.reflect.Type type =
                new TypeToken<HashMap<String, NodeJDBFiles>>() {}.getType();
            shardNodeJDBFilesMap =
                JsonUtils.fromJson(reader, type);
        }

        final String fileName = requiredFileJSON.getName();
        final String kvstoreName =
            fileName.substring(0, fileName.indexOf('_'));
        for (Map.Entry<String, NodeJDBFiles> entry :
            shardNodeJDBFilesMap.entrySet()) {
            final String shardName = entry.getKey();
            final NodeJDBFiles nodeJDBFiles = entry.getValue();

            if (shardName.contains("admin")) {
                /*
                 * We have identified admin shard. Get list of
                 * jdb files to be copied from NodeJDBFiles
                 * object.
                 */
                getNodeJDBFiles(nodeJDBFiles, kvstoreName);
            }
        }
    }

    private void getNodeJDBFiles(NodeJDBFiles nodeJDBFiles,
                                 String kvstoreName)
        throws IOException, InterruptedException {
        final List<JDBFile> jdbFiles = nodeJDBFiles.getJDBFilesList();
        final String adminNodeName = nodeJDBFiles.getNodeName();

        /*
         * Before copying jdb files and getting required algorithms,
         * create targetpath/adminNodeName directory where admin jdb
         * files will be copied locally.
         */
        final File adminNodePath =
            new File(targetPath, kvstoreName + adminNodeName);
        RecoverConfig.makeDir(adminNodePath);

        /*
         * Start copying of admin jdb files
         */
        copyFiles(jdbFiles, adminNodePath, adminNodeName);
    }

    /**
     * Performs the validation of input config file and requiredfiles
     * directory path passed as an input argument. Throws an exception if
     * validation fails.
     *
     * @throws IllegalArgumentException if an input parameter is invalid
     * @throws IOException if there is an I/O error
     */
    private void validateInputParameters()
        throws IOException, IllegalArgumentException {

        /*
         * Input config file existence check
         */
        if (!inputConfigPath.exists()) {
            printUsage("Specified input config file " +
                       inputConfigPath.getAbsolutePath() +
                       " does not exist");
        }

        /*
         * Required json files directory path existence check
         */
        if (!requiredFilesPath.exists()) {
            printUsage("Specified required json files directory " +
                       requiredFilesPath.getAbsolutePath() +
                       " does not exist");
        }

        /*
         * Parse and validate required, optional and non-used parameters
         * in input config file.
         */
        if (initProperties != null) {
            throw new IllegalStateException("initProperties already "
                                            + "initialized");
        }

        Objects.requireNonNull(inputConfigPath,
                               "configFile arg must not be null");

        final Properties props = new Properties();
        try (InputStream configStream = new FileInputStream(inputConfigPath)) {
            props.load(configStream);
        }

        /* Successful initialization. */
        initProperties = props;

        try {
            validateRecoveryArchiveFSClassParameters();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "InterruptedException while parsing "
                + "recoveryCopyClass parameter: " + e, e);
        }
    }

    private void validateRecoveryArchiveFSClassParameters()
        throws IllegalArgumentException {

        if (recoveryCopyClass != null) {
            throw new IllegalStateException("recoveryCopyClass object already "
                                            + "initialized");
        }

        recoveryCopyClass =
            (String) initProperties.remove(RECOVERY_COPY_CLASS_KEY);

        /*
         * recoveryCopyClass can be
         * oracle.nosql.objectstorage.backup.BackupObjectStorageCopy or
         * com.sleepycat.je.RecoverArchiveFSCopy
         *
         * com.sleepycat.je.RecoverArchiveFSCopy is for internal testing
         * purpose.
         */
        if (recoveryCopyClass == null) {
            throw new IllegalArgumentException(
                "recoveryCopyClass missing from: " + initProperties);
        }

        /*
         * Check for whether right RecoveryCopyClass argument is specified
         * is done during RecoverFileCopy object instantiation specific to
         * the winner node jdb file.
         */
    }

    /**
     * Performs the parsing of the AdminRecover command arguments.
     *
     * @param argv AdminRecover command arguments
     * @throws Exception
     */
    public void parseArgs(String[] argv) throws Exception {

        int argc = 0;
        int nArgs = argv.length;

        if (argv.length == 0) {
            printUsage("Empty argument list");
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-config")) {
                if (argc < nArgs) {
                    final String inputConfig = argv[argc++];
                    if ("".equals(inputConfig)) {
                        printUsage("Config file path name must not"
                                   + " be empty");
                    }
                    if (!inputConfig.startsWith("/")) {
                        printUsage("Config file path name must"
                                   + " be an absolute path");
                    }
                    inputConfigPath = new File(inputConfig);
                } else {
                    printUsage("-config requires an argument");
                }
            } else if (thisArg.equals("-requiredfiles")) {
                if (argc < nArgs) {
                    final String requiredFilesPathValue = argv[argc++];
                    if ("".equals(requiredFilesPathValue)) {
                        printUsage("RequiredFiles directory path must"
                                   + " not be empty");
                    }
                    if (!requiredFilesPathValue.startsWith("/")) {
                        printUsage("RequiredFiles directory path must"
                                   + " be an absolute path");
                    }
                    requiredFilesPath = new File(requiredFilesPathValue);
                } else {
                    printUsage("-requiredfiles requires an argument");
                }
            } else if (thisArg.equals("-target")) {
                if (argc < nArgs) {
                    final String targetPathValue = argv[argc++];
                    if ("".equals(targetPathValue)) {
                        printUsage("Target path must not be empty");
                    }
                    if (!targetPathValue.startsWith("/")) {
                        printUsage("Target path must be an absolute path");
                    }
                    targetPath = new File(targetPathValue);
                } else {
                    printUsage("-target requires an argument");
                }
            } else if (thisArg.equals("-debug")) {
                isPrintStackTrace = true;
            } else {
                printUsage(thisArg + " is not a supported option.");
            }
        }

        /*
         * Check if input parameter is specified
         */
        if (inputConfigPath == null) {
            printUsage("-config flag argument not specified");
        }

        /*
         * Check if requiredfiles path parameter is specified
         */
        if (requiredFilesPath == null) {
            printUsage("-requiredfiles flag argument not specified");
        }

        /*
         * Check if target parameter is specified
         */
        if (targetPath == null) {
            printUsage("-target flag argument not specified");
        }

        /*
         * Parsing completed successfully. Validate if input config file
         * and required json files directory exists on specified path.
         * If not, exit before proceeding. If yes, then parse and validate
         * the input config file parameters. recoveryCopyClass is parsed
         * and validated separately since it is not needed/common to
         * RecoverArchiveFSCopy object.
         *
         * Information required for RecoverArchiveFSCopy object instantiation
         * is present in requiredfiles.json for each admin winner node jdb file
         * with checksum, encryption and compression Algorithm.
         * RecoverArchiveFSCopy object is initiated individually for each admin
         * winner node jdb file. Values of checksum, encryption and compression
         * (when used) are validated and initialized as part of
         * RecoverFSArchiveCopy object initializing.
         */
        validateInputParameters();
    }

    private void printUsage(String msg) throws IllegalArgumentException {
        throw new IllegalArgumentException((msg != null) ?
                                                msg + "\n" + usage() :
                                                usage());
    }

    private static String usage() {
        return "Usage : "
                + "java -jar <recovery.jar> "
                + "adminrecover "
                + "-config <configuration file path> "
                + "-requiredfiles <requiredfiles directory path> "
                + "-target <output directory path> "
                + " [ -debug ]";
    }

    /*
     * Convenience method for AdminRecover tests
     * @param argv the command line arguments
     * @return whether the operation succeeded
     */
    public static boolean mainInternal(final String[] argv) {

        AdminRecover adminRecover = new AdminRecover();

        /* Parse the AdminRecover command line arguments */
        try {
            adminRecover.parseArgs(argv);
        } catch (Exception e) {
            if (adminRecover.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in parsing arguments for" +
                               " AdminRecover: " + e);
            return false;
        }

        try {
            adminRecover.copyAdminsJDBFiles();
            if (threadError instanceof RuntimeException) {
                throw (RuntimeException) threadError;
            } else if (threadError instanceof Error) {
                throw (Error) threadError;
            } else if (threadError != null) {
                throw new RuntimeException(
                    "Exception during recovery: " + threadError, threadError);
            }
            System.out.println("jdb files for admin winner nodes has been "
                               + " successfully copied at " +
                               adminRecover.targetPath.getAbsolutePath());
        } catch (Exception e) {
            if (adminRecover.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in running AdminRecover "
                               + "Utility: " + e);
            return false;
        }
        return true;
    }

    /**
     * The main used by the AdminRecover Utility
     *
     * @param argv An array of command line arguments to the
     * AdminRecover utility.
     */
    public static void main(String[] argv) {

        final boolean succeeded = mainInternal(argv);
        if (!succeeded) {
            System.exit(1);
        }
    }
}
