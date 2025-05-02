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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;

import com.google.gson.reflect.TypeToken;

import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.recovery.SNRecover.StorageNodeJSON.ReplicationNodeJSON;

import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * SNRecover is designed to copy jdb files of Admin and Replication winner
 * node deployed on a Storage Node. SNRecover copies jdb files of winner
 * nodes across all shards deployed on a storage node. This utility needs
 * to be run on all storage nodes in NoSQL cloud service.
 *
 * <p>Winner node in a shard is the node having successful backup and highest
 * vlsn. If there are multiple nodes having successful backup and same vlsn
 * then node which is master is considered as winner node. If there is no node
 * which is master in above conditions, then node having earlier name will be
 * considered as winner node. For more details around winner node calculation
 * in a shard, refer ARTRequiredFiles documentation.
 *
 * <p>Replication and Admin winner node information in a shard and archive
 * paths of jdb files to be copied are derived from running ARTRequiredFiles
 * utility. For each kvstore, Replication and Admin winner nodes information
 * for shards are in <i>kvstore</i>_requiredfiles.json in following format.
 * Details of computed checksum, checkSumAlg, encryptionAlg and compressionAlg
 * used during copy of jdb file to archive are available in requiredfiles.json.
 * This information is used in copy operation of jdb file from backup archive
 * to storage directory of nodes.
 *
 * {@literal
 *    <kvstore>_requirefiles.json
 *    {
 *       "rg1" : {
 *          "winnerNode" : "rg1-rn1",
 *          "jdbFilesList" : [
 *             {
 *                "fileName" : "000000.jdb",
 *                "filePath" : "/backup1/kvstore/rg1/rn1/19011012/000000.jdb",
 *                "checkSum" : "3f5bfa4aef4b35253389d675c051d2e7aa2a9640",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             },
 *             {
 *                "fileName" : "000001.jdb",
 *                "filePath" : "/backup1/kvstore/rg1/rn1/19011013/000001.jdb",
 *                "checkSum" : "9b474d6c48042cdd2bbd3994f5a682d64419531c",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             }
 *          ]
 *       }
 *       "rg2" : {
 *          "winnerNode" : "rg2-rn2",
 *          "jdbFilesList" : [
 *             {
 *                "fileName" : "000000.jdb",
 *                "filePath" : "/backup1/kvstore/rg2/rn2/19011012/000000.jdb",
 *                "checkSum" : "4487fc30f0902ba17ccaa8566ebff138b751f50f",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             },
 *             {
 *                "fileName" : "000001.jdb",
 *                "filePath" : "/backup1/kvstore/rg2/rn2/19011013/000001.jdb",
 *                "checkSum" : "10a9be71f3fc785d27385a27621ea998f301e7b1",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             }
 *          ]
 *       }
 *       "rg3" : {
 *          "winnerNode" : "rg3-rn3",
 *          "jdbFilesList" : [
 *             {
 *                "fileName" : "000000.jdb",
 *                "filePath" : "/backup1/kvstore/rg3/rn3/19011012/000000.jdb",
 *                "checkSum" : "88732661789ac414bd6178596262f05c3c5db7ac",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             },
 *             {
 *                "fileName" : "000001.jdb",
 *                "filePath" : "/backup1/kvstore/rg3/rn3/19011013/000001.jdb",
 *                "checkSum" : "6f442edce9cdfeade0ef67ba74d8d6db1d094ed6",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             }
 *          ]
 *       },
 *       "admin" : {
 *          "winnerNode" : "admin1",
 *          "jdbFilesList" : [
 *             {
 *                "fileName" : "00000002.jdb",
 *                "filePath" : "/backup1/kvstore/admin1/19011014/00000002.jdb",
 *                "checkSum" : "71eea00d90575cf4f256d932bae36e0de67ef887",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             },
 *             {
 *                "fileName" : "000003.jdb",
 *                "filePath" : "/backup1/kvstore/admin1/19011015/000003.jdb",
 *                "checkSum" : "5eed2da5fb5f2ad8b4a155dc0848b8c518774c47",
 *                "checkSumAlg" : "SHA1",
 *                "encryptionAlg" : "NONE",
 *                "compressionAlg" : "NONE"
 *             }
 *          ]
 *       }
 *    }
 * }
 *
 * <p> topologyout.json is generated for each kvstore after running
 * RecoverConfig utility on admin jdb files copied as part of AdminRecover.
 * Using topologyoutput.json, we retrieve information about Storage Nodes
 * which are part of this kvstore, Replication and Admin nodes hosted on a
 * storage node and storage directory path of nodes. Storage directories for
 * replication and admin nodes hosted on a storage node are created by
 * SNRecover. Then we copy jdb files of winner replication and admin node
 * (retrieved from requirefiles.json) in storage directory for each shard
 * which are hosted on a storage node. This process is repeated for all storage
 * nodes. In a shard, we are copying jdb files for only a single replication
 * and admin  node. jdb files for other non-winner nodes in a shard are not
 * copied during SNRecover run on storage nodes and they are created through
 * network restore during start of storage node.
 *
 * <p>Input parameters for SNRecover are :
 *
 * <p>Path to config file which will contain information about base archiveURL
 * and RecoveryCopyClass. It will also contain the credential needed to
 * access the archive. The format of the config file will be simple key-value
 * format. Example for Input config file is as follows :
 *
 * {@literal
 *   ### This is configuration of SNRecover tool ###
 *
 *   ### Required properties #####
 *
 *   # Recovery copy class which is used in object storage end.
 *   recoveryCopyClass = com.sleepycat.je.RecoverArchiveFSCopy
 *
 *   # In actual SNRecover read me/doc, this will be a constant
 *   # value i.e oracle.nosql.objectstorage.backup.RecoverObjectStorageCopy
 *
 *   ### Optional properties ###
 *
 *   ### Not support properties ###
 *
 *   # At this point in kv, we are only supporting recoveryCopyClass in config
 *   # file.
 * }
 *
 * <p>Directory path of <kvstore>_requiredfiles.json which storage node is
 * part of in the topology. <kvstore>_requiredfiles.json will have information
 * of winner replication nodes and admin node for each shard in kvstore.
 *
 * <p> Directory path of topologyoutput.json for kvstore which storage node
 * is part of in the topology. topologyoutput.json will give information
 * of which replication and admin nodes are hosted on storage node.
 * requiredfile.json and topologyoutput.json combined will identify winner
 * replication and admin nodes that are hosted on a storage node. jdb files
 * for those winner replication and admin nodes will be copied in respective
 * storage directory.
 *
 * <p> Hostname of the storage node on which SNRecover is being
 * run as identified in topologyoutput.json.
 *
 * {@literal
 *    resourceId : sn4
 *    hostname : 10.0.13.135
 *    registryPort : 5000
 * }
 *
 * <p>For running this utility, user needs to have recovery.jar in classpath.
 * -debug flag can be used to print the stack trace in exception scenario.
 * Usage details are as follows :
 *
 * {@literal
 * java -jar <recovery.jar> snrecover
 *      -config <configuration file path>
 *      -requiredfile <requiredfiles.json file path>
 *      -topologyfile <topologyoutput.json file path>
 *      -hostname <hostname of storage node for which SNRecover is to be run>
 *      [ -debug ]
 * }
 */

public class SNRecover extends AdminSNRecover {

    public static final String COMMAND_NAME = "snrecover";
    public static final String COMMAND_DESC =
        "Copies winner replication and admin nodes jdb files from backup"
        + " archive hosted on the storage node for each shard in storage"
        + " directory of replication and admin nodes identifed from"
        + " topologyoutput json file.";
    public static final String COMMAND_ARGS =
        "-config <configuration file path>" +
        " -requiredfile <requiredfile json file path>" +
        " -topologyfile <topology output json file path>" +
        " -hostname <address of storage node on which SNRecover" +
        " is to be run>" + " [ -debug ]";

    private static volatile Throwable threadError = null;
    private File requiredFilePath = null;
    private File topologyOutputFilePath = null;
    private String hostname = null;
    private List<String> storageNodeId = new ArrayList<>();

    private Map<String, NodeJDBFiles> winnerNodesJDBFilesMap =
        new HashMap<>();

    private final UncaughtExceptionHandler exceptionHandler =
        new SNRecoverExceptionHandler();

    private void copyWinnerNodeJDBFiles()
        throws IOException, InterruptedException {

        /*
         * Deserializing requiredfile.json and topologyout.json to
         * identify replication and admin nodes hosted on storage node.
         */
        deserializeRequiredFileJSON(requiredFilePath);
        deserializeTopologyOutputJSON(topologyOutputFilePath);
    }

    /**
     * De-serialize the requiredfile json file. Identify winner admin
     * and replication nodes for which jdb files need to be copied. Winner
     * Admin and Replication nodes may/may not be hosted on storage node. If
     * hosted then jdb files for nodes will be copied in parallel with each
     * node files being copied sequentially.
     *
     * @throws IOException if an I/O error occurs
     */
    private void deserializeRequiredFileJSON(File requiredFileJSON)
        throws IOException {

        Map<String,NodeJDBFiles> shardNodeJDBFilesMap;

        try (FileReader reader = new FileReader(requiredFileJSON)) {
            java.lang.reflect.Type type =
                new TypeToken<HashMap<String, NodeJDBFiles>>() {}.getType();
            shardNodeJDBFilesMap =
                JsonUtils.fromJson(reader, type);
        }

        for (Map.Entry<String, NodeJDBFiles> entry :
            shardNodeJDBFilesMap.entrySet()) {
            final String shardName = entry.getKey();
            final NodeJDBFiles nodeJDBFiles = entry.getValue();

            /*
             * We have identified a winner node for each replication and
             * admin shard. Add this shardName and nodeJDBFiles to
             * winnerNodesJDBFilesMap.
             */
             winnerNodesJDBFilesMap.put(shardName, nodeJDBFiles);
        }
    }

    /**
     * Deserialize the topology output json file. Identify replication
     * and admin nodes which are hosted on this storage node. Check if
     * hosted node is the winner node. If yes, then node jdb files will
     * be copied. Winner Replication/Admin nodes jdb files will be copied
     * in parallel with each node files being copied sequentially.
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException
     */
    private void deserializeTopologyOutputJSON(File topologyFileJSON)
        throws IOException, InterruptedException {

        TopologyOutputJSON topoOutputJSON;
        try {
            topoOutputJSON =
                JsonUtils.fromFile(topologyFileJSON, TopologyOutputJSON.class);
        } catch (IOException e) {
            throw new IOException("Exception in deserializing file " +
                                  topologyFileJSON + ": " + e, e);
        }

        /*
         * Get TopologyJSON object and check which StorageNodeJSON object's
         * hostname matches with hostname specified by input parameter
         */
        List<StorageNodeJSON> snListJSON =
            topoOutputJSON.getTopoJSON().getStorageNodeList();

        for (StorageNodeJSON snJSON : snListJSON) {
            String localHostname = snJSON.getHostName();
            if (localHostname.equals(this.hostname)) {
                /*
                 * Identified storage node for which replication and admin
                 * nodes jdb files needs to be copied.
                 *
                 * Save storage node id which will be used in copying right
                 * winner admin node jdb files hosted on this storage node.
                 *
                 * storageNodeId should be an array because it is possible
                 * that multiple sns are hosted on a storage node.
                 */
                storageNodeId.add(snJSON.getStortageNodeId());

                /*
                 * Identify winner replication node hosted on this storage
                 * node
                 */
                copyWinnerReplicationNodes(snJSON);
            }
        }

        /*
         * Storage node can also hosts admin node. We will need
         * to use AdminJSON object for identifying admin node hosted on
         * this storage and check with requiredfiles.json to see if
         * it is a winner admin node. If yes, then we will need to
         * copy admin jdb files to admin storage directory as provided
         * in topologyoutput json file.
         *
         * topologyoutput.json :
         * "admin" : {
         *      "admins" : [ {
         *          "adminId" : "admin1",
         *          "storageNodeId" : "sn1",
         *          "storageDirPath" : "/var/lib/andc/ondb/kvroot",
         *          "storageDirEnvPath" :
         *              "/var/lib/andc/ondb/kvroot/DStore1/sn1/admin1/env",
         *          "storageDirSize" : 0
         *       },
         *       ..... ]
         *  }
         *
         * requiredfiles.json :
         *
         * "admin" : {
         *      "admin1" : [ {
         *      "00000003.jdb" :
         *          "/backup1/kvstore/admin1/18083011/00000003.jdb"
         *       checkSum : "71eea00d90575cf4f256d932bae36e0de67ef887",
         *       checkSumAlg" : "SHA1",
         *       encryptionAlg : "",
         *       compressionAlg : ""
         *       },  .....]
         *  }
         *
         * NOTE: These files were already copied over local file system
         * during adminrecover. So admins/devops can copy specific admin
         * files from local system to storage node. We are providing a
         * more convenient method where by running single snrecover
         * command, all nodes hosted on storage node jdb files will be
         * copied.
         */

        /*
         * Get list of admins from TopoOutputJSON which are hosted
         * on hostname for which SNRecover is being run.
         */
        copyWinnerAdminNodes(topoOutputJSON);
    }

    private void copyWinnerAdminNodes(TopologyOutputJSON topoOutputJSON)
        throws IOException, InterruptedException {

        AdminJSON adminsJSON = topoOutputJSON.getAdminJSON();
        List<AdminJSON.AdminJSONInfo> adminsInfo =
            adminsJSON.getAdmins();
        for (AdminJSON.AdminJSONInfo adminJSONInfo : adminsInfo) {
            if (storageNodeId.contains(adminJSONInfo.getStorageNodeId())) {
                /*
                 * This is an admin which is hosted on storage node for
                 * which SNRecover is being run. Identify if this admin is
                 * a winner node admin from winnerNodesJDBFilesMap
                 */

                /*
                 * At this point, admin node is hosted on storage node for
                 * which SNRecover is being. It will be good to create
                 * storage directory for this admin node. We are not having
                 * any concept of admin log directory at this point.
                 */
                final File nodePath =
                    new File(adminJSONInfo.getStorageDirEnvPath());
                RecoverConfig.makeDir(nodePath);

                /*
                 * Identify and copy admin winner node jdb files
                 */
                copyWinnerAdminNode(adminJSONInfo);
            }
        }
    }

    private void copyWinnerAdminNode(AdminJSON.AdminJSONInfo
                                         adminJSONInfo)
        throws InterruptedException {

        for (Map.Entry<String, NodeJDBFiles> entry :
                 winnerNodesJDBFilesMap.entrySet()) {
            final NodeJDBFiles nodeJDBFiles = entry.getValue();
            final String nodeName = nodeJDBFiles.getNodeName();
            if (adminJSONInfo.getAdminId().equals(nodeName)) {
                /*
                 * This is winner node hosted on this storage node.
                 * Initiate NodeCopyThread
                 */
                NodeCopyThread nodeCopyThread = null;
                try {
                    nodeCopyThread =
                        new NodeCopyThread(nodeJDBFiles,
                                           null,
                                           adminJSONInfo);
                    nodeCopyThread.start();
                } finally {
                    if (nodeCopyThread != null) {
                        nodeCopyThread.join();
                    }
                }
            }
        }
    }

    private void copyWinnerReplicationNodes(StorageNodeJSON snJSON)
        throws IOException, InterruptedException {

        List<StorageNodeJSON.ReplicationNodeJSON>
            hostedReplicationNodes = snJSON.getHostedReplicationNodes();
        for (StorageNodeJSON.ReplicationNodeJSON replicationNodeJSON :
                 hostedReplicationNodes ) {
            String resourceId =
                replicationNodeJSON.getReplicationResourceId();

            /*
             * Create the storage node directories for replication nodes
             * hosted on storage node.
             */
            prepareStorageLogDirectories(replicationNodeJSON);

            /*
             * Check if this replication resource id is a winner node
             * replication node in winnerNodesJDBFilesMap, if yes then
             * initiate copy thread that will start copying jdb files
             * sequentially.
             */
            initiateWinnerNodeCopyThread(resourceId, replicationNodeJSON);
        }
    }

    private void initiateWinnerNodeCopyThread(String resourceId,
        StorageNodeJSON.ReplicationNodeJSON replicationNodeJSON)
        throws InterruptedException {

        for (Map.Entry<String, NodeJDBFiles> entry :
                 winnerNodesJDBFilesMap.entrySet()) {
            final NodeJDBFiles nodeJDBFiles = entry.getValue();
            final String nodeName = nodeJDBFiles.getNodeName();
            if (resourceId.equals(nodeName)) {
                /*
                 * We have find out a winner node that is hosted on
                 * this storage node. Initiate NodeCopyThread for
                 * copying jdb files.
                 */
                NodeCopyThread nodeCopyThread = null;
                try {
                    nodeCopyThread = new NodeCopyThread(nodeJDBFiles,
                                                        replicationNodeJSON,
                                                        null);
                    nodeCopyThread.start();
                } finally {
                    if (nodeCopyThread != null) {
                        nodeCopyThread.join();
                    }
                }
            }
        }
    }

    private void prepareStorageLogDirectories(
        ReplicationNodeJSON replicationNodeJSON) throws IOException {

        /*
         * At this point, replication node is hosted on storage
         * node for which SNRecover is being. It will be good to
         * create storage directory and log directory for all
         * replication nodes on this storage node.
         */
        final File nodePath =
            new File(replicationNodeJSON.getStorageDirEnvPath());
        RecoverConfig.makeDir(nodePath);

        /*
         * TODO : Handle storage directory size parameter. I think
         * we do not need to anything here. When replication node
         * comes up then storage directory size will be set
         * accordingly from config.xml.
         */

        /*
         * Preparing log directory. Need to check behavior if
         * explicit log directory for replication node is not
         * mentioned. In cloud we explicitly mention replication
         * node log directory.
         */
        if (replicationNodeJSON.getLogDirPath() != null) {
            final File nodeLogPath =
                new File(replicationNodeJSON.getLogDirPath());
            RecoverConfig.makeDir(nodeLogPath);
        }
    }

    /*
     * Uncaught exception handler for SNRecover NodeCopyThread.
     */
    private class SNRecoverExceptionHandler
            implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.log(Level.SEVERE, 
                "SNRecover failed unexpectedly due to a fault " +
                "in thread " + t.getName(), e);
            threadError = e;
        }
     }

    /**
     * The NodeCopyThread copies winner replication node and admin node
     * jdb files under requiredFile.json that are hosted on storage node.
     *
     * TODO : Need to check on below comment.
     * Does there need to be a way to shutdown this thread and have the
     * thread notice that it is being shutdown?
     *
     * @see #run
     */
    private class NodeCopyThread extends StoppableThread {

        private final NodeJDBFiles nodeJDBFiles;
        private final StorageNodeJSON.ReplicationNodeJSON replicationNodeJSON;
        private final AdminJSON.AdminJSONInfo adminNodeJSON;
        NodeCopyThread(final NodeJDBFiles nodeJDBFiles,
                       final StorageNodeJSON.ReplicationNodeJSON
                           replicationNodeJSON,
                       final AdminJSON.AdminJSONInfo adminNodeJSON) {
            super(null, exceptionHandler, "NodeCopy");
            this.nodeJDBFiles = nodeJDBFiles;
            this.replicationNodeJSON = replicationNodeJSON;
            this.adminNodeJSON = adminNodeJSON;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        /**
         * Copies identified winner node jdb files hosted on this storage node
         * from backup archive to local file system under storage directory.
         *
         * @see #copyNodeJDBFiles
         */
        @Override
        public void run() {
            long retryWait = INITIAL_RETRY_WAIT_MS;
            while (true) {
                /*
                 * TODO : Check for shutdown?
                 */
                try {
                    copyNodeJDBFiles(nodeJDBFiles,
                                     replicationNodeJSON,
                                     adminNodeJSON);
                    break;
                } catch (IOException|InterruptedException|RuntimeException e) {
                    String errorMessage = "Problem copying winner node jdb"
                                          + " files in : ";
                    if (adminNodeJSON == null) {
                        errorMessage +=
                            replicationNodeJSON.getReplicationResourceId();
                    } else {
                        errorMessage += adminNodeJSON.getAdminId();
                    }
                    errorMessage += " " + CommonLoggerUtils.getStackTrace(e);
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

    private void copyNodeJDBFiles(final NodeJDBFiles nodeJDBFiles,
                                  final StorageNodeJSON.ReplicationNodeJSON
                                      replicationNodeJSON,
                                  final AdminJSON.AdminJSONInfo
                                      adminNodeJSON)
        throws IOException, InterruptedException {

        /*
         * First create the storage directory in which node jdb files
         * will be copied. Storage directory will be created of a specified
         * size.
         *
         * Storage directory for winner replication node hosted on this storage
         * node is already created. To avoid any issues before starting copying
         * jdb file, creating directory again for safer side.
         */
        String nodeName;
        String storageEnvDirectory;
        if (adminNodeJSON == null) {
            nodeName = replicationNodeJSON.getReplicationResourceId();
            storageEnvDirectory = replicationNodeJSON.getStorageDirEnvPath();
        } else {
            nodeName = adminNodeJSON.getAdminId();
            storageEnvDirectory = adminNodeJSON.getStorageDirEnvPath();
        }

        final File nodePath = new File(storageEnvDirectory);
        RecoverConfig.makeDir(nodePath);

        /*
         * TODO : Need to check on usage of storageDirectorySize for
         * replication and admin node. I think we do not need to
         * anything here. When replication node comes up then storage
         * directory size will be set accordingly from config.xml. Need
         * to check if handling of admin directory size is different.
         */

        final List<JDBFile> jdbFiles = nodeJDBFiles.getJDBFilesList();

        /*
         * Copy nodeName jdb files
         */
        copyFiles(jdbFiles, nodePath, nodeName);
    }

    static class TopologyOutputJSON {

        TopologyJSON topology;
        AdminJSON admin;
        SequenceJSON sequenceNumbers;

        public TopologyOutputJSON() {

        }

        public TopologyOutputJSON(TopologyJSON topology,
                                  AdminJSON admin,
                                  SequenceJSON sequenceNumbers) {
            this.topology = topology;
            this.admin = admin;
            this.sequenceNumbers = sequenceNumbers;
        }

        TopologyJSON getTopoJSON() {
            return topology;
        }

        AdminJSON getAdminJSON() {
            return admin;
        }

        SequenceJSON getSequenceJSON() {
            return sequenceNumbers;
        }

        void setTopology(TopologyJSON topology) {
            this.topology = topology;
        }

        void setAdmin(AdminJSON admin) {
            this.admin = admin;
        }

        void setSequenceNumbers(SequenceJSON sequenceNumbers) {
            this.sequenceNumbers = sequenceNumbers;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof TopologyOutputJSON)) {
                return false;
            }
            final TopologyOutputJSON other = (TopologyOutputJSON) object;
            return Objects.equals(topology, other.topology) &&
                   Objects.equals(admin, other.admin) &&
                   Objects.equals(sequenceNumbers, other.sequenceNumbers);
        }

        @Override
        public int hashCode() {
            int hash = 51;
            hash = (73 * hash) + Objects.hashCode(topology);
            hash = (73 * hash) + Objects.hashCode(admin);
            hash = (73 * hash) + Objects.hashCode(sequenceNumbers);
            return hash;
        }
    }

    /*
     * TODO [#27493] : TopologyPrinter generates the JSON format of the
     * topology. It will be good to keep TopologyJSON classes with
     * TopologyPrinter so that it is convenient to maintain the relationship
     * between the input and output of JSON.
     */
    static class TopologyJSON {

        String storeName;
        int numPartitions;
        int sequenceNumber;
        List<DatacenterJSON> zns;
        List<StorageNodeJSON> sns;
        List<ShardJSON> shards;

        public TopologyJSON() {

        }

        public TopologyJSON(String storeName,
                            int numPartitions,
                            int sequenceNumber,
                            List<DatacenterJSON> zns,
                            List<StorageNodeJSON> sns,
                            List<ShardJSON> shards) {
            this.storeName = storeName;
            this.numPartitions = numPartitions;
            this.sequenceNumber = sequenceNumber;
            this.zns = zns;
            this.sns = sns;
            this.shards = shards;
        }

        String getStoreName() {
            return storeName;
        }

        int getNumPartitions() {
            return numPartitions;
        }

        int getSequenceNumber() {
            return sequenceNumber;
        }

        List<DatacenterJSON> getDataCenterList() {
            return zns;
        }

        List<StorageNodeJSON> getStorageNodeList() {
            return sns;
        }

        List<ShardJSON> getShardList() {
            return shards;
        }

        void setStoreName(String storeName) {
            this.storeName = storeName;
        }

        void setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        void setSequenceNumber(int sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        void setZns(List<DatacenterJSON> zns) {
           this.zns = zns;
        }

        void setSns(List<StorageNodeJSON> sns) {
            this.sns = sns;
        }

        void setShards(List<ShardJSON> shards) {
            this.shards = shards;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof TopologyJSON)) {
                return false;
            }
            final TopologyJSON other = (TopologyJSON) object;
            return Objects.equals(storeName, other.storeName) &&
                   Objects.equals(numPartitions, other.numPartitions) &&
                   Objects.equals(sequenceNumber, other.sequenceNumber) &&
                   Objects.equals(zns, other.zns) &&
                   Objects.equals(sns, other.sns) &&
                   Objects.equals(shards, other.shards);
        }

        @Override
        public int hashCode() {
            int hash = 61;
            hash = (73 * hash) + Objects.hashCode(storeName);
            hash = (73 * hash) + Objects.hashCode(numPartitions);
            hash = (73 * hash) + Objects.hashCode(sequenceNumber);
            hash = (73 * hash) + Objects.hashCode(zns);
            hash = (73 * hash) + Objects.hashCode(sns);
            hash = (73 * hash) + Objects.hashCode(shards);
            return hash;
        }
    }

    static class DatacenterJSON {

        String resourceId;
        String name;
        int repFactor;
        String type;
        boolean allowArbiters;
        boolean masterAffinity;

        public DatacenterJSON() {

        }

        public DatacenterJSON(String resourceId,
                              String name,
                              int repFactor,
                              String type,
                              boolean allowArbiters,
                              boolean masterAffinity) {
            this.resourceId = resourceId;
            this.name = name;
            this.repFactor = repFactor;
            this.type = type;
            this.allowArbiters = allowArbiters;
            this.masterAffinity = masterAffinity;
        }

        String getResourceId() {
            return resourceId;
        }

        String getZoneName() {
            return name;
        }

        int getZoneRepFactor() {
            return repFactor;
        }

        String getZoneType() {
            return type;
        }

        boolean getZoneAllowArbiters() {
            return allowArbiters;
        }

        boolean getZoneMasterAffinity() {
            return masterAffinity;
        }

        void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        void setName(String name) {
            this.name = name;
        }

        void setRepFactor(int repFactor) {
            this.repFactor = repFactor;
        }

        void setType(String type) {
            this.type = type;
        }

        void setAllowArbiters(boolean allowArbiters) {
            this.allowArbiters = allowArbiters;
        }

        void setMasterAffinity(boolean masterAffinity) {
            this.masterAffinity = masterAffinity;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof DatacenterJSON)) {
                return false;
            }
            final DatacenterJSON other = (DatacenterJSON) object;
            return Objects.equals(resourceId, other.resourceId) &&
                    Objects.equals(name, other.name) &&
                    Objects.equals(type, other.type) &&
                    Objects.equals(repFactor, other.repFactor) &&
                    Objects.equals(allowArbiters, other.allowArbiters) &&
                    Objects.equals(masterAffinity, other.masterAffinity);
        }

        @Override
        public int hashCode() {
            int hash = 31;
            hash = (73 * hash) + Objects.hashCode(resourceId);
            hash = (73 * hash) + Objects.hashCode(name);
            hash = (73 * hash) + Objects.hashCode(type);
            hash = (73 * hash) + Objects.hashCode(repFactor);
            hash = (73 * hash) + Objects.hashCode(allowArbiters);
            hash = (73 * hash) + Objects.hashCode(masterAffinity);
            return hash;
        }
    }

    static class StorageNodeJSON {

        String resourceId;
        String hostname;
        String registryPort;
        DatacenterJSON zone;
        String capacity;
        String rootDirPath;
        List<ReplicationNodeJSON> rns;
        List<ArbiterNodeJSON> ans;

        public StorageNodeJSON() {

        }

        public StorageNodeJSON(String resourceId,
                               String hostname,
                               String registryPort,
                               DatacenterJSON zone,
                               String capacity,
                               String rootDirPath,
                               List<ReplicationNodeJSON> rns,
                               List<ArbiterNodeJSON> ans) {
            this.resourceId = resourceId;
            this.hostname = hostname;
            this.registryPort = registryPort;
            this.zone = zone;
            this.capacity = capacity;
            this.rootDirPath = rootDirPath;
            this.rns = rns;
            this.ans = ans;
        }

        static class ReplicationNodeJSON {

            String resourceId;
            String storageDirPath;
            String storageDirEnvPath;
            long storageDirSize;
            String logDirPath;

            public ReplicationNodeJSON() {

            }

            public ReplicationNodeJSON(String resourceId,
                                       String storageDirPath,
                                       String storageDirEnvPath,
                                       long storageDirSize,
                                       String logDirPath) {
                this.resourceId = resourceId;
                this.storageDirPath = storageDirPath;
                this.storageDirEnvPath = storageDirEnvPath;
                this.storageDirSize = storageDirSize;
                this.logDirPath = logDirPath;
            }

            String getReplicationResourceId() {
                return resourceId;
            }

            String getStorageDirPath() {
                return storageDirPath;
            }

            String getStorageDirEnvPath() {
                return storageDirEnvPath;
            }

            long getStorageDirSize() {
                return storageDirSize;
            }

            String getLogDirPath() {
                return logDirPath;
            }

            void setResourceId(String resourceId) {
                this.resourceId = resourceId;
            }

            void setStorageDirPath(String storageDirPath) {
                this.storageDirPath = storageDirPath;
            }

            void setStorageDirEnvPath(String storageDirEnvPath) {
                this.storageDirEnvPath = storageDirEnvPath;
            }

            void setStorageDirSize(long storageDirSize) {
                this.storageDirSize = storageDirSize;
            }

            void setLogDirPath(String logDirPath) {
                this.logDirPath = logDirPath;
            }

            @Override
            public boolean equals(final Object object) {
                if (this == object) {
                    return true;
                }
                if (!(object instanceof ReplicationNodeJSON)) {
                    return false;
                }
                final ReplicationNodeJSON other = (ReplicationNodeJSON) object;
                return Objects.equals(resourceId, other.resourceId) &&
                        Objects.equals(storageDirPath, other.storageDirPath) &&
                        Objects.equals(storageDirEnvPath,
                                       other.storageDirEnvPath) &&
                        Objects.equals(storageDirSize, other.storageDirSize) &&
                        Objects.equals(logDirPath, other.logDirPath);
            }

            @Override
            public int hashCode() {
                int hash = 11;
                hash = (73 * hash) + Objects.hashCode(resourceId);
                hash = (73 * hash) + Objects.hashCode(storageDirPath);
                hash = (73 * hash) + Objects.hashCode(storageDirEnvPath);
                hash = (73 * hash) + Objects.hashCode(storageDirSize);
                hash = (73 * hash) + Objects.hashCode(logDirPath);
                return hash;
            }
        }

        static class ArbiterNodeJSON {
            /*
             * TODO : We do not have any arbiter setting in cloud. We will
             * add support for arbiter here if needed in future.
             */
        }

        String getStortageNodeId() {
            return resourceId;
        }

        String getHostName() {
            return hostname;
        }

        String getRegistryPort() {
            return registryPort;
        }

        DatacenterJSON getDatacenter() {
            return zone;
        }

        String getCapacity() {
            return capacity;
        }

        List<ReplicationNodeJSON> getHostedReplicationNodes() {
            return rns;
        }

        List<ArbiterNodeJSON> getHostedArbiterNodes() {
            return ans;
        }

        void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        void setHostname(String hostname) {
            this.hostname = hostname;
        }

        void setRegistryPort(String registryPort) {
            this.registryPort = registryPort;
        }

        void setZone(DatacenterJSON zone) {
            this.zone = zone;
        }

        void setCapacity(String capacity) {
            this.capacity = capacity;
        }

        void setRootDirPath(String rootDirPath) {
            this.rootDirPath = rootDirPath;
        }

        void setRns(List<ReplicationNodeJSON> rns) {
            this.rns = rns;
        }

        void setAns(List<ArbiterNodeJSON> ans) {
            this.ans = ans;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof StorageNodeJSON)) {
                return false;
            }
            final StorageNodeJSON other = (StorageNodeJSON) object;
            return Objects.equals(resourceId, other.resourceId) &&
                    Objects.equals(hostname, other.hostname) &&
                    Objects.equals(registryPort, other.registryPort) &&
                    Objects.equals(zone, other.zone) &&
                    Objects.equals(capacity, other.capacity) &&
                    Objects.equals(rootDirPath, other.rootDirPath) &&
                    Objects.equals(rns, other.rns) &&
                    Objects.equals(ans, other.ans);
        }

        @Override
        public int hashCode() {
            int hash = 13;
            hash = (73 * hash) + Objects.hashCode(resourceId);
            hash = (73 * hash) + Objects.hashCode(hostname);
            hash = (73 * hash) + Objects.hashCode(registryPort);
            hash = (73 * hash) + Objects.hashCode(zone);
            hash = (73 * hash) + Objects.hashCode(capacity);
            hash = (73 * hash) + Objects.hashCode(rootDirPath);
            hash = (73 * hash) + Objects.hashCode(rns);
            hash = (73 * hash) + Objects.hashCode(ans);
            return hash;
        }
    }

    static class ShardJSON {

        String resourceId;
        int numPartitions;
        List<ShardRNInfo> rns;
        List<StorageNodeJSON.ArbiterNodeJSON> ans;
        String partition;

        public ShardJSON() {

        }

        public ShardJSON(String resourceId,
                         int numPartitions,
                         List<ShardRNInfo> rns,
                         List<StorageNodeJSON.ArbiterNodeJSON> ans,
                         String partition) {
            this.resourceId = resourceId;
            this.numPartitions = numPartitions;
            this.rns = rns;
            this.ans = ans;
            this.partition = partition;
        }

        static class ShardRNInfo {
            String resourceId;
            String snId;
            String haPort;

            public ShardRNInfo() {

            }

            public ShardRNInfo(String resourceId,
                               String snId,
                               String haPort) {
                this.resourceId = resourceId;
                this.snId = snId;
                this.haPort = haPort;
            }

            String getResourceId() {
                return resourceId;
            }

            String getStorageNodeId() {
                return snId;
            }

            String getHaPort() {
                return haPort;
            }

            void setResourceId(String resourceId) {
                this.resourceId = resourceId;
            }

            void setSnId(String snId) {
                this.snId = snId;
            }

            void setHaPort(String haPort) {
                this.haPort = haPort;
            }

            @Override
            public boolean equals(final Object object) {
                if (this == object) {
                    return true;
                }
                if (!(object instanceof ShardRNInfo)) {
                    return false;
                }
                final ShardRNInfo other = (ShardRNInfo) object;
                return Objects.equals(resourceId, other.resourceId) &&
                        Objects.equals(snId, other.snId) &&
                        Objects.equals(haPort, other.haPort);
            }

            @Override
            public int hashCode() {
                int hash = 91;
                hash = (73 * hash) + Objects.hashCode(resourceId);
                hash = (73 * hash) + Objects.hashCode(snId);
                hash = (73 * hash) + Objects.hashCode(haPort);
                return hash;
            }
        }

        String getShardId() {
            return resourceId;
        }

        int getNumPartititions() {
            return numPartitions;
        }

        List<ShardRNInfo> getShardRNsInfo() {
            return rns;
        }

        void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        void setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        void setRns(List<ShardRNInfo> rns) {
            this.rns = rns;
        }

        void setAns(List<StorageNodeJSON.ArbiterNodeJSON> ans) {
            this.ans = ans;
        }

        void setPartition(String partition) {
            this.partition = partition;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ShardJSON)) {
                return false;
            }
            final ShardJSON other = (ShardJSON) object;
            return Objects.equals(resourceId, other.resourceId) &&
                    Objects.equals(numPartitions, other.numPartitions) &&
                    Objects.equals(rns, other.rns) &&
                    Objects.equals(ans, other.ans) &&
                    Objects.equals(partition, other.partition);
        }

        @Override
        public int hashCode() {
            int hash = 21;
            hash = (73 * hash) + Objects.hashCode(resourceId);
            hash = (73 * hash) + Objects.hashCode(numPartitions);
            hash = (73 * hash) + Objects.hashCode(rns);
            hash = (73 * hash) + Objects.hashCode(ans);
            hash = (73 * hash) + Objects.hashCode(partition);
            return hash;
        }
    }

    static class AdminJSON {

        List<AdminJSONInfo> admins;

        public AdminJSON() {

        }

        public AdminJSON( List<AdminJSONInfo> admins) {
            this.admins = admins;
        }

        static class AdminJSONInfo {

            String adminId;
            String storageNodeId;
            String storageDirPath;
            String storageDirEnvPath;
            long storageDirSize;

            public AdminJSONInfo() {

            }

            public AdminJSONInfo(String adminId,
                                 String storageNodeId,
                                 String storageDirPath,
                                 String storageDirEnvPath,
                                 long storageDirSize) {
                this.adminId = adminId;
                this.storageNodeId = storageNodeId;
                this.storageDirPath = storageDirPath;
                this.storageDirEnvPath = storageDirEnvPath;
                this.storageDirSize = storageDirSize;
            }

            String getAdminId() {
                return adminId;
            }

            String getStorageNodeId() {
                return storageNodeId;
            }

            String getStorageDirPath() {
                return storageDirPath;
            }

            String getStorageDirEnvPath() {
                return storageDirEnvPath;
            }

            long getStorageDirSize() {
                return storageDirSize;
            }

            void setAdminId(String adminId) {
                this.adminId = adminId;
            }

            void setStorageNodeId(String storageNodeId) {
                this.storageNodeId = storageNodeId;
            }

            void setStorageDirPath(String storageDirPath) {
                this.storageDirPath = storageDirPath;
            }

            void setStorageDirEnvPath(String storageDirEnvPath) {
               this.storageDirEnvPath = storageDirEnvPath;
            }

            void setStorageDirSize(long storageDirSize) {
                this.storageDirSize = storageDirSize;
            }

            @Override
            public boolean equals(final Object object) {
                if (this == object) {
                    return true;
                }
                if (!(object instanceof AdminJSONInfo)) {
                    return false;
                }
                final AdminJSONInfo other = (AdminJSONInfo) object;
                return Objects.equals(adminId, other.adminId) &&
                       Objects.equals(storageNodeId, other.storageNodeId) &&
                       Objects.equals(storageDirPath, other.storageDirPath) &&
                       Objects.equals(storageDirEnvPath,
                                      other.storageDirEnvPath) &&
                       Objects.equals(storageDirSize, other.storageDirSize);
            }

            @Override
            public int hashCode() {
                int hash = 81;
                hash = (73 * hash) + Objects.hashCode(adminId);
                hash = (73 * hash) + Objects.hashCode(storageNodeId);
                hash = (73 * hash) + Objects.hashCode(storageDirPath);
                hash = (73 * hash) + Objects.hashCode(storageDirEnvPath);
                hash = (73 * hash) + Objects.hashCode(storageDirSize);
                return hash;
            }
        }

        List<AdminJSONInfo> getAdmins() {
            return admins;
        }

        void setAdmins(List<AdminJSONInfo> admins) {
            this.admins = admins;
        }

        @Override
        public boolean equals(final Object object) {
           if (this == object) {
                return true;
           }
           if (!(object instanceof AdminJSON)) {
                return false;
           }
           final AdminJSON other = (AdminJSON) object;
           return Objects.equals(admins, other.admins);
        }

        @Override
        public int hashCode() {
            int hash = 72;
            hash = (73 * hash) + Objects.hashCode(admins);
            return hash;
        }
    }

    static class SequenceJSON {

        int securityMetadata;
        int tableMetadata;

        public SequenceJSON() {

        }

        public SequenceJSON(int securityMetadata,
                            int tableMetadata) {
            this.securityMetadata = securityMetadata;
            this.tableMetadata = tableMetadata;
        }

        int getSecurityMetaData() {
            return securityMetadata;
        }

        int getTableMetadata() {
            return tableMetadata;
        }

        void setSecurityMetadata(int securityMetadata) {
            this.securityMetadata = securityMetadata;
        }

        void setTableMetadata(int tableMetadata) {
            this.tableMetadata = tableMetadata;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof SequenceJSON)) {
                return false;
            }
            final SequenceJSON other = (SequenceJSON) object;
            return Objects.equals(tableMetadata, other.tableMetadata) &&
                   Objects.equals(securityMetadata, other.securityMetadata);
        }

        @Override
        public int hashCode() {
            int hash = 18;
            hash = (73 * hash) + Objects.hashCode(tableMetadata);
            hash = (73 * hash) + Objects.hashCode(securityMetadata);
            return hash;
        }
    }

    /**
     * Performs the validation of input config file, requiredfile.json
     * and topologyoutput.json passed as an input argument.
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
         * RequiredFile json file existence check
         */
        if (!requiredFilePath.exists()) {
            printUsage("Specified requiredfile json file " +
                       requiredFilePath.getAbsolutePath() +
                       " does not exist");
        }

        /*
         * topologyoutout json file existence check
         */
        if (!topologyOutputFilePath.exists()) {
            printUsage("Specified topologyoutput json file " +
                       topologyOutputFilePath.getAbsolutePath() +
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
        try (InputStream configStream =
                new FileInputStream(inputConfigPath)) {
            props.load(configStream);
        }

        /* Successful initialization. */
        initProperties = props;

        try {
            validateRecoveryArchiveFSClassParameters();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "InterruptedException while parsing "
                + "recoveryCopyClass parameter: " + e,
                e);
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
         * Check for whether right recoveryCopyClass argument is specified
         * is done during RecoverFileCopy object instantiation specific to
         * the winner replication node.
         */
    }

    /**
     * Performs the parsing of the SNRecover command arguments.
     *
     * @param argv SNRecover command arguments
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
            } else if (thisArg.equals("-requiredfile")) {
                if (argc < nArgs) {
                    final String requiredFilePathValue = argv[argc++];
                    if ("".equals(requiredFilePathValue)) {
                        printUsage("RequiredFile path must not be empty");
                    }
                    if (!requiredFilePathValue.startsWith("/")) {
                        printUsage("RequiredFile path must be an "
                                   + "absolute path");
                    }
                    requiredFilePath = new File(requiredFilePathValue);
                } else {
                    printUsage("-requiredfile requires an argument");
                }
            } else if (thisArg.equals("-topologyfile")) {
                if (argc < nArgs) {
                    final String targetPathValue = argv[argc++];
                    if ("".equals(targetPathValue)) {
                        printUsage("topology output json file path must"
                                   + " not be empty");
                    }
                    if (!targetPathValue.startsWith("/")) {
                        printUsage("topology output json file path must"
                                   + " be an absolute path");
                    }
                    topologyOutputFilePath = new File(targetPathValue);
                } else {
                    printUsage("-topologyfile requires an argument");
                }
            } else if (thisArg.equals("-hostname")) {
                if (argc < nArgs) {
                    hostname = argv[argc++];
                    if ("".equals(hostname)) {
                        printUsage("hostname of the storage node"
                                   + " not be empty");
                    }
                } else {
                    printUsage("-hostname requires an argument");
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
         * Check if requiredfile parameter is specified
         */
        if (requiredFilePath == null) {
            printUsage("-requiredfile flag argument not specified");
        }

        /*
         * Check if topologyfile parameter is specified
         */
        if (topologyOutputFilePath == null) {
            printUsage("-topologyfile flag argument not specified");
        }

        /*
         * Check if hostname parameter is specified
         */
        if (hostname == null) {
            printUsage("-hostname argument not specified");
        }

        /*
         * Parsing completed successfully. Validate if input config file,
         * requiredfile and topology output json exists on specified path.
         * If not, exit before proceeding. If yes, then parse and validate
         * the input config file parameters. recoveryCopyClass is parsed
         * and validated separately since it is not needed/common to
         * RecoverArchiveFSCopy object.
         *
         * Information required for RecoverArchiveFSCopy object instantiation
         * is present in requiredfiles.json for each replication winner node
         * jdb file with checksum, encryption and compression algorithm.
         * RecoverArchiveFSCopy object is initiated individually for each
         * replication winner node jdb file hosted on storage node and copy is
         * done in parallel for winner replication nodes. Values of checksum,
         * encryption and compression (when used) are validated and initialized
         * as part of RecoverFSArchiveCopy object initializing.
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
                + "java -jar <recovery.jar>"
                + " snrecover"
                + " -config <configuration file path>"
                + " -requiredfile <requiredfile json file path>"
                + " -topologyfile <topology output json file path>"
                + " -hostname <adrress of storage node on which"
                + " snrecover is to be run> [ -debug ]";
    }

    /*
     * Convenience method for SNRecover tests
     * @param argv the command line arguments
     * @return whether the operation succeeded
     */
    public static boolean mainInternal(final String[] argv) {

        SNRecover snRecover = new SNRecover();

        /* Parse the SNRecover command line arguments */
        try {
            snRecover.parseArgs(argv);
        } catch (Exception e) {
            if (snRecover.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in parsing arguments for" +
                               " SNRecover: " + e);
            return false;
        }

        try {
            snRecover.copyWinnerNodeJDBFiles();
            if (threadError instanceof RuntimeException) {
                throw (RuntimeException) threadError;
            } else if (threadError instanceof Error) {
                throw (Error) threadError;
            } else if (threadError != null) {
                throw new RuntimeException(
                    "Exception during recovery: " + threadError, threadError);
            }
            System.out.println("jdb files for winner nodes hosted"
                               + " on this storage has been successfully"
                               + " copied");
        } catch (Exception e) {
            if (snRecover.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in running SNRecover Utility: " + e);
            return false;
        }
        return true;
    }

    /**
     * The main used by the SNRecover Utility
     *
     * @param argv An array of command line arguments to the
     * SNRecover utility.
     */
    public static void main(String[] argv) {

        final boolean succeeded = mainInternal(argv);
        if (!succeeded) {
            System.exit(1);
        }
    }
}
