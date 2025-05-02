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

package oracle.kv.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.shell.Shell;

/**
 * Generate configuration files and directory structure for an existing Storage
 * Node.  The resulting zip archive can be unpacked and used as a replacement
 * root directory.
 * Usage:
 *
 * {@literal
 *   java -cp <kvstore.jar> oracle.kv.util.GenerateConfig -host <host>
 *       -port <registryPort> -sn <storageNodeId> -target <zipfile>
 *       [-username <user>] [-security <security-file-path>]
 *       [-secdir <overriden security directory>]
 * }
 *
 * The resulting zipfile for kvroot "myroot" store "mystore" and -snid 1 will
 * have this structure which can be unzipped to the proper root location and
 * used.
 *  myroot/
 *    config.xml
 *    security.policy
 *    mystore/
 *      security.policy
 *      sn1/
 *        config.xml
 *
 * To start the newly-created storage node after unzip:
 *  {@literal java -jar <kvstore.jar> start -root <kvroot>}
 *
 * If the actual RepNode and/or Admin directories and data are available and
 * placed in the file hierarchy they will be used as well.  If not, they will
 * be recovered via normal JE network restore.
 *
 * This program uses the information available in the Topology and Admin
 * Parameters to generate the required configuration files.  It constructs
 * the expected hierarchy in a temporary directory, zips the files, and removes
 * the temporary files leaving the result in <zipfile>.zip.
 */

public class GenerateConfig {

    public static final String COMMAND_NAME = "generateconfig";
    public static final String COMMAND_DESC =
        "generates configuration files for the specified storage node";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " + CommandParser.getPortUsage() +
        " -sn <StorageNodeId> " + "-target <zipfile>" + Shell.eolt +
        "[" + CommandParser.getUserUsage() + "] [" +
        CommandParser.getSecurityUsage() +
        "] [-secdir <overriden security directory>]";

    private StorageNodeId snid;
    private Parser parser;
    private CommandServiceAPI cs;
    private Topology t;
    private Parameters p;
    private String target;
    private String userName;
    private String securityFilePath;
    private String secDir;
    private boolean isSecured;
    private LoginCredentials creds;

    /*
     * The top-level interface that drives the process
     */
    private void generate(Logger logger) throws Exception {

        /*
         * Initialize the store login routines if secured communication settings
         * are detected
         */
        initLogin();

        /*
         * Find the Admin, get the Topology and Parameters.  StorageNodeId
         * is already set by the command line parser.
         */
        final String host = parser.getHostname();
        final int port = parser.getRegistryPort();
        final LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(host, port, creds, logger);
        cs = RegistryUtils.getAdmin(host, port, loginMgr, logger);
        t = cs.getTopology();
        p = cs.getParameters();

        /*
         * Get the single parameter entries -- Global, StorageNode, Admin
         */
        GlobalParams gp = p.getGlobalParams();
        StorageNodeParams snp = p.get(snid);
        AdminParams ap = getAdminParams(snid, p);

        /**
         * LoadParameters is a class that knows how to collect and write
         * ParameterMaps.
         * 
         * Construct the config information
         */
        LoadParameters lp = generateConfig(t, p, gp, snp, ap, snid);

        /*
         * Authentication implies a secured store, so we need to set the
         * security directory in the BootStrapParams.
         */
        if (isSecured) {
            if (secDir == null || secDir.isEmpty()) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }
        } else {
            secDir = null;
        }

        /**
         * Construct the bootstrap config information
         */
        LoadParameters bootLp = generateBootConfig(gp, snp, ap, secDir, null);

        File rootDir = new File(snp.getRootDirPath());

        /**
         * Create the expected directory structure and files in rootDir
         */
        createFiles(rootDir.getName(), gp.getKVStoreName(), lp, bootLp);

        /**
         * Create the zip archive
         */
        createZip(target, rootDir.getName(), null);

        /**
         * Cleanup
         */
        delete(new File(rootDir.getName()));
    }

    /**
     * Find AdminParams if this SN is hosting an admin
     */
    public static AdminParams getAdminParams(StorageNodeId storageNodeId,
                                             Parameters params) {
        Collection<AdminParams> aps = params.getAdminParams();
        for (AdminParams ap : aps) {
            if (ap.getStorageNodeId().equals(storageNodeId)) {
                return ap;
            }
        }
        return null;
    }

    /**
     * Generate the config parameters
     */
    public static LoadParameters generateConfig(Topology topo,
                                                Parameters params,
                                                GlobalParams globalParams,
                                                StorageNodeParams
                                                    storageNodeParams,
                                                AdminParams adminParams,
                                                StorageNodeId storageNodeId) {

        LoadParameters lp = new LoadParameters();
        lp.addMap(globalParams.getMap());
        lp.addMap(storageNodeParams.getMap());
        if (storageNodeParams.getStorageDirMap() != null) {
            lp.addMap(storageNodeParams.getStorageDirMap());
        }
        if (storageNodeParams.getRNLogDirMap() != null) {
            lp.addMap(storageNodeParams.getRNLogDirMap());
        }
        if (storageNodeParams.getAdminDirMap() != null) {
            lp.addMap(storageNodeParams.getAdminDirMap());
        }

        if (adminParams != null) {
            lp.addMap(adminParams.getMap());
        }

        /*
         * Add any RepNodes associated with the StorageNode
         */
        List<RepNode> rns = topo.getSortedRepNodes();
        for (RepNode rn : rns) {
            if (rn.getStorageNodeId().equals(storageNodeId)) {
                RepNodeParams rnp = params.get(rn.getResourceId());
                lp.addMap(rnp.getMap());
            }
        }

        /*
         * Add any ArbNodes associated with the StorageNode
         */
        List<ArbNode> ans = topo.getSortedArbNodes();
        for (ArbNode an : ans) {
            if (an.getStorageNodeId().equals(storageNodeId)) {
                ArbNodeParams anp = params.get(an.getResourceId());
                lp.addMap(anp.getMap());
            }
        }

        return lp;
    }

    /**
     * Generate the bootstrap config parameters
     */
    public static LoadParameters generateBootConfig(GlobalParams gp,
                                                    StorageNodeParams snp,
                                                    AdminParams ap,
                                                    String securityDir,
                                                    String softwareVersion)
        throws Exception {

        LoadParameters lp = new LoadParameters();
        boolean hostingAdmin = false;
        if (ap != null) {
            hostingAdmin = true;
        }

        BootstrapParams bp =
            new BootstrapParams(snp.getRootDirPath(),
                                snp.getHostname(),
                                snp.getHAHostname(),
                                snp.getHAPortRange(),
                                snp.getServicePortRange(),
                                gp.getKVStoreName(),
                                snp.getRegistryPort(),
                                snp.getAdminWebPort(),
                                snp.getCapacity(),
                                snp.getStorageType(),
                                securityDir,
                                hostingAdmin,
                                snp.getMgmtClass(),
                                softwareVersion);
        bp.setStorageDirMap(snp.getStorageDirMap());
        bp.setRNLogDirMap(snp.getRNLogDirMap());
        bp.setAdminDirMap(snp.getAdminDirMap());
        ParameterMap map = bp.getMap();
        map.setParameter(ParameterState.COMMON_SN_ID,
                         Integer.toString(snp.getStorageNodeId().
                                          getStorageNodeId()));
        lp.addMap(map);
        lp.addMap(bp.getStorageDirMap());
        lp.addMap(bp.getRNLogDirMap());
        lp.addMap(bp.getAdminDirMap());
        lp.setVersion(ParameterState.BOOTSTRAP_PARAMETER_VERSION);
        return lp;
    }

    /**
     * Generate the expected file hierarchy under the root
     */
    private void createFiles(String rootName,
                             String storeName,
                             LoadParameters lp,
                             LoadParameters bootLp)
        throws Exception {

        String secPolicy = "grant {\n  permission java.security.AllPermission;\n};\n";
        String tmpName = rootName;
        File rootDir = new File(tmpName);
        rootDir.mkdir();
        bootLp.saveParameters(new File(rootDir, "config.xml"));
        File storeDir = new File(rootDir, storeName);
        storeDir.mkdir();
        File snDir = FileNames.getStorageNodeDir(storeDir, snid);
        snDir.mkdir();
        writeFile(new File(rootDir, "security.policy"), secPolicy);
        writeFile(new File(storeDir, "security.policy"), secPolicy);
        lp.saveParameters(FileNames.getSNAConfigFile(rootDir.getAbsolutePath(),
                                                     storeName, snid));
    }

    public static void writeFile(File file, String content) {
        PrintWriter writer = null;
        try {
            FileOutputStream fos = new FileOutputStream(file);
            writer = new PrintWriter(fos);
            writer.printf("%s", content);
        } catch (Exception e) {
            throw new IllegalStateException("Problem saving file: " +
                                            file + ": " + e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * Create the archive, recursively adding the contents of the sourceName
     * directory to the targetConfigName zip file. Adds the ".zip" suffix to
     * the targetConfigName parameter if not present. If targetName is not
     * null, uses it as the top level pathname of entries stored in the zip
     * file, otherwise uses sourceName.
     *
     * @param targetConfigName path of the output zip file
     * @param sourceName path of the source directory
     * @param targetName path for the associated directory to record in the zip
     * file, or null to use sourceName
     * @throws IOException
     */
    public static void createZip(String targetConfigName, String sourceName,
                                 String targetName) throws IOException {
        try {
            if (!targetConfigName.contains(".zip")) {
                targetConfigName += ".zip";
            }
            FileOutputStream fout =
                new FileOutputStream(new File(targetConfigName));
            ZipOutputStream zout = new ZipOutputStream(fout);
            System.out.println("Creating zipfile " + targetConfigName);
            File source = new File(sourceName);

            File target =
                new File(targetName == null ? sourceName : targetName);
            addDirectory(zout, source, target);
            zout.close();
        } catch (IOException ioe) {
            throw new IOException("IOException :" + ioe);
        }
    }

    /**
     * Recursively add the contents of a directory to the zip stream by reading
     * files from source directory and using target for the name of the
     * directory in the zip stream.
     *
     * @param zout the zip output stream
     * @param source path of the source directory
     * @param target path of the associated directory under zip file
     */
    private static void addDirectory(ZipOutputStream zout, File source,
                                     File target) {

        File[] files = source.listFiles();
        System.out.println("Adding directory " + target.getPath());
        for (int i = 0; i < files.length; ++i) {
            if (files[i].isDirectory()) {
                addDirectory(zout,
                             new File(source.getPath() +
                                      File.separator + files[i].getName()),
                             new File(target.getPath() +
                                      File.separator + files[i].getName()));
                continue;
            }
            try {
                /*
                 * Path of the file from source directory
                 */
                String sourcePath =
                    source.getPath() + File.separator + files[i].getName();

                /*
                 * Path of the file under target directory added to zip file
                 */
                String targetPath =
                    target.getPath() + File.separator + files[i].getName();

                System.out.println("Adding file " + targetPath);
                byte[] buffer = new byte[1024];
                FileInputStream fin = new FileInputStream(sourcePath);
                zout.putNextEntry(new ZipEntry(targetPath));
                int length;
                while((length = fin.read(buffer)) > 0) {
                    zout.write(buffer, 0, length);
                }
                zout.closeEntry();
                fin.close();
            } catch (IOException ioe) {
                System.out.println("IOException :" + ioe);
            }
        }
    }

    public static void delete(File f)
        throws IOException {

        if (f.isDirectory()) {
            for (File file : f.listFiles())
                delete(file);
        }
        f.delete();
    }

    private void initLogin()
        throws IOException {

        final KVStoreLogin storeLogin =
            new KVStoreLogin(userName, securityFilePath);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        isSecured = storeLogin.foundTransportSettings();
        if (isSecured) {
            creds = storeLogin.makeShellLoginCredentials();
        }
    }

    /**
     * Command line parsing
     */
    class Parser extends CommandParser {
        public static final String snidFlag = "-sn";
        public static final String targetFlag = "-target";
        public static final String secDirFlag = "-secdir";
        public Parser(String[] args) {
            super(args);
        }

        @Override
        protected void verifyArgs() {
            if (getHostname() == null ||
                getRegistryPort() == 0 ||
                snid == null ||
                target == null) {
                usage("Missing required argument");
            }
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                               "\n\t" + COMMAND_ARGS);
            System.exit(-1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(snidFlag)) {
                String sn = nextArg(arg);
                try {
                    snid = StorageNodeId.parse(sn);
                } catch (IllegalArgumentException iae) {
                    usage("Invalid Storage Node Id: " + sn);
                }
                return true;
            }
            if (arg.equals(targetFlag)) {
                target = nextArg(arg);
                return true;
            }
            if (arg.equals(secDirFlag)) {
                secDir = nextArg(arg);
                return true;
            }
            return false;
        }
    }

    public void parseArgs(String args[]) {
        parser = new Parser(args);
        parser.parseArgs();
        userName = parser.getUserName();
        securityFilePath = parser.getSecurityFile();
    }

    public static void main(String[] args) {
        GenerateConfig gen = new GenerateConfig();
        gen.parseArgs(args);
        try {
            gen.generate(LoggerUtils.getLogger(GenerateConfig.class, "main"));
        } catch (Exception e) {
            System.err.println("Exception in GenerateConfig: " + e);
        }
    }
}
