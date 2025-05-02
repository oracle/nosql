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
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.kv.util.GenerateConfig;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * RecoverConfig utility is designed to recover config.xml
 * for Admins and SNs from admin database. It also generates
 * a JSON document having topology information. This utility will
 * use the latest topology, parameter, table and security metadata
 * information available in the admin .jdb files.
 *
 * This utility should only be used if there is no running admin,
 * and that, if there is an admin running, it is better to use
 * GenerateConfig.
 *
 * This program uses the information available in the Topology
 * and Admin Parameters to generate the required configuration files.
 * It also uses table and security metadata information for getting
 * sequence numbers that will be used for verification after recovering
 * store.
 *
 * Input parameter for the utility will be directory path
 * having Admin jdb files. Target parameter for the utility will
 * be path where zip file containing the config and topology
 * JSON output files will be created. Optional debug parameter,
 * if enabled then print stack trace during errors.
 *
 * For running this utility, user needs to have recovery.jar in
 * classpath. Usage details are as follows:
 *
 * {@literal
 * java -jar <recovery.jar> recoverconfig
 *        -input <admin Database DirectoryPath>
 *        -target <zipfile>
 *        [ -debug ]
 * }
 *
 * The resulting directory structure for the target zip file is as follows.
 * Example shows target zip file directory structure for a NX3 topology with
 * each SN having capacity = N.
 *
 * {@literal
 *   recoverconfig/
 *     topologyoutput.json
 *     kvroot_sn1/
 *       config.xml
 *       security.policy
 *       <storeName>
 *         security.policy
 *         sn1/
 *           config.xml
 *     kvroot_sn2/
 *       config.xml
 *       security.policy
 *       <storeName>
 *         security.policy
 *         sn2/
 *           config.xml
 *     kvroot_sn3/
 *       config.xml
 *       security.policy
 *       <storeName>
 *         security.policy
 *         sn3/
 *           config.xml
 * }
 */

public class RecoverConfig {

    public static final String COMMAND_NAME = "recoverconfig";
    public static final String COMMAND_DESC =
        "generates configuration files and topology output"
        + " from admin database";
    public static final String COMMAND_ARGS =
        "-input <admin Database Directory Path>" +
        " -target <zipfile>" +
        " [ -debug ]";

    private File adminEnv = null;
    private File targetPath = null;
    private Parameters params = null;
    private Topology topology = null;
    private File tempRecoverConfigDir = null;
    private SecurityMetadata securityMetadataDB = null;
    private TableMetadata tableMetadataDB = null;
    private boolean isPrintStackTrace = false;

    private static String SECPOLICY =
        "grant {\n  permission java.security.AllPermission;\n};\n";

    private void generateConfig() throws Exception {

        /*
         * Open Admin Parameters, TableMetadata and SecurityMetadata
         * database and initialize paramDB, tableMetadataDB
         * and SecurityMetadataDB
         */
        initializeDB();

        /*
         * Open Admin Topology History database and initialize
         * topologyDB.
         */
        initializeAdminTopologyDB();

        try {
            /*
             * Generate following files for all SNs under config zipfile
             *
             * 1.) Config.xml having bootStrap params.
             * 2.) Config.xml for all SNs.
             */
            createConfigFiles();

            /*
             * Generate topology JSON output under config zipfile
             */
            createTopologyJSONFile();

            /*
             * Generate zip file and delete files generated
             * under temporary recoverconfig directory.
             */
            createPrepareZip();
        } finally {
            /*
             * Delete temporary recoverconfig directory after generating
             * zip file irrespective of success and failure of creating
             * required files under recoverconfig and creation of zip file
             */
            deleteRecoverConfigDir();
        }
    }

    /**
     * Open and read Admin Parameters, SecurityMetadata and
     * TableMetadata DB
     * @throws Exception
     */
    private void initializeDB() throws Exception {

        /*
         * Open Admin Parameter database from jdb files and
         * initialize paramDB.
         */
        initializeAdminDB(DB_TYPE.PARAMETERS);

        /* Open Admin Security Metadata database and initialize
         * securityMetadataDB.
         */
        initializeAdminDB(DB_TYPE.SECURITY);

        /*
         * Open Admin Table Metadata database from jdb files and
         * initialize tableMetadataBB.
         */
        initializeAdminDB(DB_TYPE.TABLE);
    }

    /**
     * Open and read Admin dbType DB
     * @throws Exception
     */
    private void initializeAdminDB(final DB_TYPE dbType)
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        Database dbTypeDB = null;
        Environment env = new Environment(adminEnv, envConfig);
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);
            try {
                dbTypeDB =
                    env.openDatabase(null, dbType.getDBName(),
                                     dbConfig);
            } catch (DatabaseException e) {
                System.err.println("Error opening admin " +
                                   dbType.getDBName() + " database");
                throw e;
            }

            /*
             * Read through Admin database and initialize dbType
             * object.
             */
            try {
                readAdminDB(dbTypeDB, dbType);
            } catch (Exception e) {
                System.err.println("Error reading admin " +
                                   dbType.getDBName() + " database");
                throw e;
            }
        } finally {
            if (dbTypeDB != null) {
                dbTypeDB.close();
            }
            env.close();
        }
    }

    /**
     * Read through Admin DB and prepare dbType object
     * @throws Exception
     */
    private void readAdminDB(final Database dbTypeDB,
                             final DB_TYPE dbType)
        throws Exception {

        DatabaseEntry foundData = new DatabaseEntry();
        Cursor cursor = dbTypeDB.openCursor(null, null);
        try {
            DatabaseEntry foundKey = new DatabaseEntry();

            /*
             * There will single entry associated with dbType database
             */
            if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
                /*
                 * De-serializing foundData.getData() byte stream into
                 * the dbType class object similar to KVS.
                 */
                if (DB_TYPE.PARAMETERS.equals(dbType)) {
                    params =
                        SerializationUtil.getObject(foundData.getData(),
                                                    Parameters.class);
                } else if (DB_TYPE.TABLE.equals(dbType)) {
                    tableMetadataDB =
                        SerializationUtil.getObject(foundData.getData(),
                                                    TableMetadata.class);
                } else if (DB_TYPE.SECURITY.equals(dbType)) {
                    securityMetadataDB =
                        SerializationUtil.getObject(foundData.getData(),
                                                    SecurityMetadata.class);
                } else {
                    throw new IllegalArgumentException(
                        "Unsupported dbType: " + dbType);
                }
            } else {
                /*
                 * SecurityMetadataDB and TableMetaDB could be
                 * empty in case of non-secured and empty store.
                 *
                 * Recoverconfig utility should still support
                 * generation of topology json out and required
                 * config.xml files.
                 */
                if (DB_TYPE.PARAMETERS.equals(dbType)) {
                    throw new Exception (dbType.getDBName() +
                                         " entry not found in " +
                                         "admin database");
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Open and read Admin Topology DB
     * @throws Exception
     */
    private void initializeAdminTopologyDB() throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        Database topologyDB = null;
        Environment env = new Environment(adminEnv, envConfig);
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);

            try {
                topologyDB =
                    env.openDatabase(null,
                                     DB_TYPE.TOPOLOGY_HISTORY.getDBName(),
                                     dbConfig);
            } catch (DatabaseException e) {
                System.err.println("Error opening admin topology history " +
                                   "database");
                throw e;
            }

            /*
             * Read through Admin Topology History database and
             * initialize Topology object.
             */
            try {
                readAdminTopologyDB(topologyDB);
            } catch (Exception e) {
                System.err.println("Error reading admin topology database");
                throw e;
            }
        } finally {
            if (topologyDB != null) {
                topologyDB.close();
            }
            env.close();
        }
    }

    /**
     * Read through Admin Topology DB and initialize Topology
     * object
     * @throws Exception
     */
    private void readAdminTopologyDB(final Database topologyDB)
        throws Exception {

        DatabaseEntry foundData = new DatabaseEntry();
        Cursor cursor = topologyDB.openCursor(null, null);
        try {
            DatabaseEntry foundKey = new DatabaseEntry();

            /*
             * There are multiple entries associated with admin topology
             * history database, we fetch the last entry
             */
            if (cursor.getLast(foundKey, foundData, LockMode.DEFAULT) ==
                OperationStatus.SUCCESS) {
                /*
                 * De-serializing foundData.getData() byte stream into
                 * the Topology class object similar to KVS.
                 */
                RealizedTopology rt =
                   SerializationUtil.getObject(foundData.getData(),
                                               RealizedTopology.class);
                topology = rt.getTopology();
            } else {
                throw new Exception ("TopologyDB entry not found in admin " +
                                     " database");
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Generate config.xml for all SNs.
     * @throws Exception
     */
    private void createConfigFiles() throws Exception {

        List<StorageNodeId> snIds = topology.getSortedStorageNodeIds();
        GlobalParams gp = params.getGlobalParams();

        for (StorageNodeId snId : snIds) {

            /*
             * Get the single parameter entries -- StorageNode, Admin
             */
            StorageNodeParams snp = params.get(snId);
            AdminParams ap = GenerateConfig.getAdminParams(snId, params);

            /*
             * LoadParameters is a class that knows how to collect and write
             * ParameterMaps.
             *
             * Construct the config information
             */
            LoadParameters lp =
                GenerateConfig.generateConfig(topology, params, gp,
                                              snp, ap, snId);

            /*
             * Construct the bootstrap config information for this SN
             */
            LoadParameters bootLp = null;
            try {
                bootLp =
                    GenerateConfig.generateBootConfig(gp, snp, ap,
                                                      null /*security dir*/,
                                                      snp.getSoftwareVersion());
            } catch (Exception e) {
                System.err.println("Exception while generating bootstrap"
                                   + " params in RecoverConfig");
                throw e;
            }

            /*
             * Create temporary recoverconfig directory structure and required
             * files under it.
             */
            try {
                createFiles(snId, lp, bootLp);
            } catch (Exception e) {
                System.err.println("Exception while creating config files "
                                   + "in RecoverConfig");
                throw e;
            }
        }
    }

    /**
     * Generate the expected file hierarchy under the temporary
     * recoverconfig directory.
     *
     * Directory structure is as follows :
     * kvroot_sn[X] having config.xml, security.policy as files and
     * <storename>/sn[X] as further directory. sn[X] will have
     * config.xml for SN. <storeName> directory will also have
     * security.policy file
     */
    private void createFiles(StorageNodeId snId,
                             LoadParameters lp,
                             LoadParameters bootLp)
        throws Exception {

        /*
         * We will create an intermediate recoverconfig directory if it
         * does not exist, so that it will be source while creating zip file.
         */
        if (tempRecoverConfigDir == null) {
            tempRecoverConfigDir =
                Files.createDirectory(
                    Files.createTempDirectory("tempRecoverConfigDir")
                    .resolve("recoverconfig")).toFile();
        }

        final File kvrootSN =
            new File(tempRecoverConfigDir, "kvroot_" + snId.getFullName());
        makeDir(kvrootSN);

        final File kvstoreDir = new File(kvrootSN, topology.getKVStoreName());
        makeDir(kvstoreDir);

        final File snDir = new File(kvstoreDir, snId.getFullName());
        makeDir(snDir);

        bootLp.saveParameters(new File(kvrootSN, "config.xml"));

        /*
         * Generate security.policy under kvroot
         */
        GenerateConfig.writeFile(new File(kvrootSN, "security.policy"),
                                 SECPOLICY);
        /*
         * Generate security.policy file under kvstore
         */
        GenerateConfig.writeFile(new File(kvstoreDir, "security.policy"),
                                 SECPOLICY);

        lp.saveParameters(new File(snDir, "config.xml"));
    }

    public static void makeDir(final File baseDir) throws IOException {

        boolean fileCreated = false;
        if (!baseDir.exists()) {
            fileCreated = baseDir.mkdirs();
            if (!fileCreated) {
                throw new IOException("Could not create directory " +
                                      baseDir.getAbsolutePath());
            }
        }
    }

    /**
     * Generate topology JSON output file under base directory
     * i.e outputDirEnv, else adminEnv if not specified
     * @throws IOException
     */
    private void createTopologyJSONFile() throws IOException {

        /*
         * Creating a JSON object for having topolgy and admin
         * information
         */
        final ObjectNode jsonTopology = JsonUtils.createObjectNode();

        /*
         * verbose mode is set true because we will need
         * storage dir information during recovery process
         */
        final ObjectNode topologyJSONOutput =
            TopologyPrinter.printTopologyJson(topology, params,
                TopologyPrinter.all, true);
        jsonTopology.set("topology", topologyJSONOutput);

        /*
         * Get Admin JSON information
         */
        final ObjectNode adminJSONOutput =
            TopologyPrinter.printAdminJSON(topology, params);
        jsonTopology.set("admin", adminJSONOutput);

        /*
         * Get Metadata sequence numbers
         */
        final ObjectNode sequenceNumbers = getSequenceNumbers();
        jsonTopology.set("sequenceNumbers", sequenceNumbers);

        /*
         * tempRecoverConfigDir already created in createFiles
         */
        try {
            JsonUtils.writeFile(
                new File(tempRecoverConfigDir, "topologyoutput.json"),
                jsonTopology, true);
        } catch (IOException e) {
            System.err.println("Exception while creating topologyoutput file "
                               + "in RecoverConfig");
            throw e;
        }
    }

    private ObjectNode getSequenceNumbers() {

        /*
         * To note : SNRecover.SequenceJSON class is dependent on below output
         * for de-serializing sequence related JSON output. In case of any
         * changes in output, make sure to do similar required changes in
         * SNRecover.SequenceJSON.
         */

        final ObjectNode jsonTop = JsonUtils.createObjectNode();
        /*
         * Put SecurityMetadata sequence number
         */
        jsonTop.put("securityMetadata",
                    (securityMetadataDB != null) ?
                    securityMetadataDB.getSequenceNumber() :
                    Metadata.EMPTY_SEQUENCE_NUMBER);

        /*
         * Put TableMetadata sequence number
         */
        jsonTop.put("tableMetadata",
                    (tableMetadataDB != null) ?
                    tableMetadataDB.getSequenceNumber() :
                    Metadata.EMPTY_SEQUENCE_NUMBER);
        return jsonTop;
    }

    /*
     * After generating zip file, delete temporary recoverconfig directory
     * if it exists.
     */
    private void deleteRecoverConfigDir() throws IOException {
        try {
            if (tempRecoverConfigDir != null) {
                GenerateConfig.delete(tempRecoverConfigDir);
            }
        } catch (IOException e) {
            System.err.println("Exception while deleting recoverconfig "
                               + " directory in RecoverConfig");
            throw e;
        }
    }

    /*
     * Create zip file from temporary recoverconfig directory
     */
    private void createPrepareZip() throws IOException {

        try {
            GenerateConfig.createZip(targetPath.getAbsolutePath(),
                                     tempRecoverConfigDir.getAbsolutePath(),
                                     tempRecoverConfigDir.getName());
        } catch (IOException e) {
            System.err.println("Exception while creating zip file "
                               + "in RecoverConfig");
            throw e;
        }
    }

    /**
     * Performs the validation of Admin JE environment copied
     * from backup archive directory.
     *
     * We are doing basic validation checking that file path
     * exists and directory is not empty.
     * @throws Exception
     */
    private void validateAdminEnv() throws Exception {

        if (!adminEnv.exists()) {
            printUsage("Specified admin directory " +
                       adminEnv.getAbsolutePath() +
                       " does not exist");
        }

        if (adminEnv.list().length == 0) {
            printUsage("Specified admin directory " +
                       adminEnv.getAbsolutePath() +
                       " does not contain any jdb files");
        }
    }

    /**
     * Performs the parsing of the RecoverConfig command arguments.
     *
     * @param argv RecoverConfig command arguments
     * @throws Exception
     */
    private void parseArgs(String[] argv) throws Exception {

        int argc = 0;
        int nArgs = argv.length;

        if (argv.length == 0) {
            printUsage("Empty argument list");
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-input")) {
                if (argc < nArgs) {
                    final String inputDir = argv[argc++];
                    if ("".equals(inputDir)) {
                        printUsage("Input directory name must not be empty");
                    }
                    if (!inputDir.startsWith("/")) {
                        printUsage("Input directory must be an absolute path");
                    }
                    adminEnv = new File(inputDir);
                } else {
                    printUsage("-input requires an argument");
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
                    if (!targetPathValue.endsWith(".zip")) {
                        printUsage("Target path must end with '.zip'");
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
        if (adminEnv == null) {
            printUsage("-input flag argument not specified");
        }

        /*
         * Check if target parameter is specified
         */
        if (targetPath == null) {
            printUsage("-target flag argument not specified");
        }

        /*
         * Parsing completed successfully. validate if admin jdb files
         * exists on input directory path. If not, exit before proceeding.
         */
        validateAdminEnv();
    }

    private void printUsage(String msg) throws Exception {
        throw new Exception((msg != null) ?
                            msg + "\n" + usage() :
                            usage());
    }

    private static String usage() {
        return "Usage : "
                + "java -jar <recovery.jar> "
                + "recoverconfig "
                + "-input <admin Database Directory Path> "
                + "-target <zipfile>"
                + " [ -debug ]";
    }

    /*
     * Convenience method for RecoverConfig tests
     * @param argv the command line arguments
     * @return whether the operation succeeded
     */
    public static boolean main1(final String[] argv) {

        RecoverConfig recoverConfig = new RecoverConfig();

        /* Parse the recover config command line arguments */
        try {
            recoverConfig.parseArgs(argv);
        } catch (Exception e) {
            if (recoverConfig.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in parsing arguments for" +
                               " RecoverConfig: " + e);
            return false;
        }

        try {
            recoverConfig.generateConfig();
            System.out.println("Configuration information recovered "
                               + "successfully at " +
                               recoverConfig.targetPath.getAbsolutePath());
        } catch (Exception e) {
            if (recoverConfig.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in RecoverConfig: " + e);
            return false;
        }
        return true;
    }

    /**
     * The main used by the RecoverConfig utility.
     *
     * @param argv An array of command line arguments to the
     * RecoverConfig utility.
     */
    public static void main(String[] argv) {

        final boolean succeeded = main1(argv);
        if (!succeeded) {
            System.exit(1);
        }
    }
}
