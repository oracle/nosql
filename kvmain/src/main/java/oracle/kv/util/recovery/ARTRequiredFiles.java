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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.util.GenerateConfig;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import com.sleepycat.je.RecoverFileCopy;
import com.sleepycat.je.dbi.BackupManager;
import com.sleepycat.je.dbi.SnapshotManifest;
import com.sleepycat.je.dbi.SnapshotManifest.LogFileInfo;

/**
 * ARTRequiredFiles is designed for reaching to an actual recovery time
 * (ART) across all kvstores in the Oracle NoSQL cloud service. This
 * utility is used for the incident of a software corruption impacting
 * all the stores. Devops/Administrators will be interested in rolling
 * back all kvstores to a particular target recovery time (TRT) in past.
 * It may or may not be possible to roll back all kvstores to exact target
 * recovery time. It depends upon multiple factors and one of factor is
 * that we should have a successful backup copied for atleast one of the
 * member of shard in oracle object storage for a particular time stamp. If
 * we get a successful backup for all shards in all kvstores at a particular
 * time stamp then it will become actual recovery time. Actual recovery time
 * may be earlier than or equal to target recovery time but will not be later.
 * We would be able to restore nosql cloud service to that calculated ART.
 *
 * <p>In addition to calculating actual recovery time, ARTRequiredFiles also
 * generates a target zip file which contains
 * <i>kvstore</i>_requiredfiles.json files for each kvstore and art.json.
 * art.json will contain actual recovery time stamp.
 *
 * <p>A winner node is defined as a replication node having most up-to-date
 * successful backup at calculated actual recovery time. If there are two
 * nodes in the shard with successful backup at ART then replication node
 * having highest vlsn or was master will be elected as winner node. In case
 * there are successful backups at replicas and matching vlsns, then winner
 * node will be one having earlier name (rg1 is earlier than rg2). Information
 * about backups being successful is taken from manifest file of nodes.
 *
 * <p>requiredfiles.json contains the list of winner nodes in each shard,
 * path of the jdb files which need to be copied, checksum of jdb file during
 * copy to backup archive, checksumAlg, encryptionAlg and compressionAlg used
 * in BackupFileCopy. This information will be used as part of follow up
 * SNRecover and AdminRecover utility. This information is extracted from
 * manifest file of the winner node.
 *
 * <p>Directory structure of the <i>nosqlservice</i>.zip will be as follows :
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
 * <p><kvstore>_requiredfiles.json structure will be as follows :
 * rg1-rn1, rg2-rn2 and rg3-rn3 are considered as winner nodes of rg1, rg2
 * and rg3 shards respectively. admin1 is winner node of admin shard.
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
 * <p>Input parameter for ARTRequiredFiles will be path to config file which
 * will contain information about base archiveURL and RecoveryCopyClass. It
 * will also contain the credentials needed to access the archive etc.
 * The format of the config file will be simple key-value format. Example for
 * input config file is as follows :
 *
 * {@literal
 *   ### This is configuration of ARTRequiredFiles tool ###
 *
 *   ### Required properties #####
 *
 *   # The base archiveURL path in oracle object storage
 *   backupDirPath =/andc/backups
 *
 *   # Recovery copy class which is used in object storage end.
 *   recoveryCopyClass = com.sleepycat.je.RecoverArchiveFSCopy
 *
 *   # In actual artrequiredfiles read me/doc, this will be a constant
 *   # value i.e oracle.nosql.objectstorage.backup.RecoverObjectStorageCopy
 *
 *   # At this point in kv, we are only supporting recoveryCopyClass and
 *   # backupDirPath in config file
 * }
 *
 * The target recovery time specified by devops/admin according to which
 * actual recovery time will be calculated.
 * -targetRecoveryTime yymmddhh
 *
 * <p>For running this utility, user needs to have recovery.jar in
 * classpath. -debug flag can be used to print the stack trace
 * in exception scenario. Usage details are as follows :
 *
 * {@literal
 * java -jar <recovery.jar> artrequiredfiles
 *        -targetRecoveryTime <yyddmmhh>
 *        -config <configuration file path>
 *        -target <zipfile>
 *        [ -debug ]
 * }
 *
 * <p>TODO : Since we will have spartakv changes for RecoverCopy
 * interface as well hence usage command may change and needs
 * to be updated. But for local kv runs, above command will work.
 *
 * <p>TODO : In the current changes, we are reaching to a consistent
 * single actual recovery time across all shards and kvstores in nosql
 * cloud service. Idea is that with rf factor being three, there is more
 * probability of getting backup for at least one node in shard. But in
 * cases, when there is no backup for any node in a shard, this approach
 * could lead to missing latest backups for other shards and going to
 * previous time stamp. It is possible that we can have individual actual
 * recovery time for each shard which is the latest time stamp at which
 * we have got successful backup in that shard. Goal will be to have actual
 * recovery times for shards as close as possible and have latest shard
 * backup considered for recovery.
 */

public class ARTRequiredFiles extends AdminSNRecover {

    public static final String COMMAND_NAME = "artrequiredfiles";
    public static final String COMMAND_DESC =
        "Calculates actual recovery time across NoSQL stores based on "
        + "target recovery time and generates a zip file contain JSON file "
        + "entries for each store having information about winner nodes "
        + "and jdb files to be copied.";
    public static final String COMMAND_ARGS =
        "-targetRecoveryTime <yyddmmhh>" +
        "-config <configuration file path>" +
        " -target <zipfile>" +
        " [ -debug ]";

    public static final String BASEDIR_KEY = "backupDirPath";

    /** checksumAlg and corresponding lengths **/
    private static final String SHA_256_CHECKSUMALG = "SHA-256";
    private static final int SHA_256_CHECKSUMALG_LENGTH = 64;
    private static final String SHA_1_CHECKSUMALG = "SHA-1";
    private static final int SHA_1_CHECKSUMALG_LENGTH = 40;

    private String targetRecoveryTime = null;
    private String baseDirectory = null;

    /** The recover copy implementation or null if not initialized. */
    private RecoverFileCopy recoverCopy;
    private File targetPath = null;
    private File tempFileList = null;

    /**
     * ActualRecoveryTimeMap has list of manifest files belonging to the
     * particular time stamp in reverse order of time stamp. It will be
     * used for generating required kvstore:shard structures for calculating
     * actual recovery time and identifying winner nodes in shard across
     * kvstores.
     */
    private SortedMap<String, ArrayList<String>> actualRecoveryTimeMap =
        new TreeMap<>(Collections.reverseOrder());

    /**
     * List of the kvstores across nosql cloud service.
     */
    private List<ARTKVStore> kvstorelist = new ArrayList<>();

    private String artTimestamp = null;

    public ARTRequiredFiles() { }

    /**
     * Generates target zip with art.json and <kvstore>_requiredfiles.json
     * @throws Exception
     */
    private void generateARTRequiredFiles() throws Exception {

        try {
            /*
             * Get the list of files in object store under base archiveURL.
             * This file list will contain path of all the jdb and manifest
             * files across kvstores and nodes in nosql cloud service that
             * have been copied over to the object storage as part of
             * scheduled backups.
             *
             * Example of a generated file list is as follow. Current example
             * shows path of files only from admin1 and rg1-rn1 nodes in
             * DStore1 from test pod :
             *
             * /andc/backups/DStore1/admin1/18071007/00000002.jdb
             * /andc/backups/DStore1/admin1/18071007/00000003.jdb
             * /andc/backups/DStore1/admin1/18071007/manifest.json
             * /andc/backups/DStore1/rg1/rn1/18071013/00000009.jdb
             * /andc/backups/DStore1/rg1/rn1/18071013/manifest.json
             *
             * File List will be store in a temporary file under
             * /tmp/tempARTDirXXXX.tmp
             */
            generateTempFileList();

            /*
             * File list has been generated, now filter out manifest file
             * path names from the list of files which have time stamp
             * less than or equal to the target recovery time. This will
             * help in pruning the search scope while calculating art
             * and generating required files json files for winner nodes.
             *
             * Also, we will prepare bucket of manifest files based on
             * time stamp of scheduled snapshot. This data structure will
             * be used for reaching to actual recovery time and winner nodes.
             */
            getManifestFileList();

            /*
             * We will process the manifest files list for time stamps to
             * generate kvstore:shard structure. If we can satisfy all
             * required actual recovery time conditions for that time stamp,
             * that will be our actual recovery time. Generate required json
             * and zip file and break out. Else move to the next time stamp
             * and process manifest files list.
             *
             * Since time stamps are in reverse order, we are guaranteed
             * to get actual recovery time closest to target recovery time.
             */
            processTimeStampManifestFileList();
        } finally {
            /*
             * Irrespective of success and failure of above operation,
             * tempFileList can be deleted now.
             */
            try {
                if (tempFileList != null) {
                    GenerateConfig.delete(tempFileList);
                }
            } catch (IOException e) {
                System.err.println("Exception while deleting temporary file "
                                   + "list " + tempFileList +
                                   " in ARTRequiredFiles utility");
                throw e;
            }
        }
    }

    private void processTimeStampManifestFileList() throws Exception {

        boolean canCalculateART = false;
        for (Map.Entry<String, ArrayList<String>> entry :
                 actualRecoveryTimeMap.entrySet()) {

            final String timestamp = entry.getKey();
            final ArrayList<String> manifestFilePathList = entry.getValue();

            /*
             * TODO : There is a possible optimization in this part of
             * operation. Since most likely list of kvstores in nosql cloud
             * service are not going to change too frequently with different
             * time stamps hence we do not need to generate kvstorelist for
             * different time stamps again. We can generate a structure with
             * first time stamp and then add/remove kvstore list for next time
             * stamp.
             *
             * Similar optimization will apply at shard level as well under
             * kvstore in case there are some elasticity operations performed
             * with in different time stamps. If there is difference in shard
             * structure or number of shards, accordingly add/remove operation
             * will be performed in existing structure.
             *
             * For, nodes in existing shard structure which are still present
             * in new time stamp, only leaf value i.e manifest file path will
             * be updated.
             *
             * Currently for first version, we are keeping it simple with
             * generating structure of kvstores:shards in nosql cloud service
             * again for new time stamp in case we did not reach to consensus
             * on actual recovery time for previous time stamp.
             */
            kvstorelist = new ArrayList<ARTKVStore>();
            generateAllKVStoreStructure(manifestFilePathList);

            /*
             * TODO : Current scheme includes the most recent available
             * backups, even though there might be other choices using
             * older backups for some shards that would result in a complete
             * backup with a smaller skew between shards.
             *
             * We have generated shard level structure for each kvstore at
             * this time stamp, now check if we can calculate ART across
             * kvstores at this time stamp, else move to the previous
             * time stamp.
             *
             * For optimizing process, we will return false at same moment
             * in case we do not have successful backup in any one of the
             * shard across kvstores. No need to copy manifest files for
             * other nodes which are not yet examined.
             */
            if (canCalculateART()) {
                /*
                 * Generate target zip which contains below structure in user
                 * specified targetPath.
                 *
                 * <nosqlservice>.zip :
                 *    artrequiredconfig /
                 *       <kvstore1>_requiredfiles.json
                 *       <kvstore2>_requiredfiles.json
                 *       <kvstore3>_requiredfiles.json
                 *       art.json
                 *
                 * Check for winner node and jdb files information for
                 * each shard in kvstore and generate requiredfiles.json
                 * from that information.
                 */
                artTimestamp = timestamp;
                canCalculateART = true;
                generateANSZip();
                break;
            }
        }

        if (!canCalculateART) {
            throw new Exception(
                "Could not reach to the actual"
                + " recovery time across kvstores in nosql"
                + " cloud service based on backups available"
                + " under " + baseDirectory + " in object"
                + " storage and specified target recovery time : "
                + targetRecoveryTime);
        }
    }

    /**
     * Generate the target zip which has art.json and
     * <kvstore>_requiredfiles json for every kvstore.
     * @throws IOException
     */
    private void generateANSZip() throws IOException {

        /*
         * Create the temporary artrequiredfiles directory which will be
         * added in the target zip file.
         *
         *  <target>.zip :
         *    artrequiredfiles \
         *      <kvstore1>_requiredfiles.json
         *      <kvstore2>_requiredfiles.json
         *      <kvstore3>_requiredfiles.json
         *      art.json
         */
        File tempARTRequiredDir = null;
        try {
            tempARTRequiredDir = Files.createDirectory(
                Files.createTempDirectory("tempARTRequiredDir")
                .resolve("artrequiredfiles")).toFile();

            /*
             * Create requiredfiles json output file for each kvstore and
             * write it to tempARTRequiredDir.
             */
            for (ARTKVStore kvstore : kvstorelist) {

                /*
                 * Creating a JSON object having winner node and jdb files
                 * information for each shard in this kvstore
                 */
                final ObjectNode jsonKvstoreRequiredDir =
                    prepareRequiredFilesObject(kvstore);

                try {
                    JsonUtils.writeFile(
                        new File(tempARTRequiredDir,
                                 kvstore.getKVStoreName() +
                                 "_requiredfiles.json"),
                        jsonKvstoreRequiredDir, true);
                } catch (IOException e) {
                    System.err.println("Exception while creating file:" +
                                       kvstore.getKVStoreName() +
                                       "_requiredfiles.json");
                    throw e;
                }
            }

            /*
             * Creating a JSON object for art.json file having ART time stamp
             * value
             */
            final ObjectNode jsonARTFile = JsonUtils.createObjectNode();
            jsonARTFile.put("ARTValue", artTimestamp);
            try {
                JsonUtils.writeFile(
                    new File(tempARTRequiredDir, "art.json"), jsonARTFile,
                    true);
            } catch (IOException e) {
                System.err.println("Exception while creating " +
                                   "art.json");
                throw e;
            }

            /*
             * Create target zip file from temporary artrequiredfiles
             * directory
             */
            try {
                GenerateConfig.createZip(targetPath.getAbsolutePath(),
                                         tempARTRequiredDir.getAbsolutePath(),
                                         tempARTRequiredDir.getName());
            } catch (IOException e) {
                System.err.println("Exception while creating " + targetPath
                                   + "in ARTRequiredFiles utility");
                throw e;
            }
        } finally {
            /*
             * Irrespective of success and failure of above operation,
             * tempARTRequiredDir can be deleted now.
             */
            try {
                if (tempARTRequiredDir != null) {
                    GenerateConfig.delete(tempARTRequiredDir);
                }
            } catch (IOException e) {
                System.err.println("Exception while deleting "
                                   + tempARTRequiredDir +
                                   " in ARTRequiredFiles utility");
                throw e;
            }
        }
    }

    private ObjectNode prepareRequiredFilesObject(ARTKVStore kvstore) {

       /*
        * Structure for requiredfiles.json is as follow. Each
        * entry is corresponding to the winner node in the shard.
        *
        * <kvstore>_requirefiles.json
        *    {
        *       "rg1" : {
        *          "winnerNode" : "rg1-rn1",
        *          "jdbFilesList" : [
        *             {
        *                "fileName" : "000.jdb",
        *                "filePath" :
        *                   "/backup1/kvstore/rg1/rn1/19011012/000.jdb",
        *                "checksum" : "3f5bfa4aef4b",
        *                "checksumAlg" : "SHA1",
        *                "encryptionAlg" : "NONE",
        *                "compressionAlg" : "NONE"
        *             },
        *             {
        *                "fileName" : "001.jdb",
        *                "filePath" : "<path>",
        *                "checksum" : "9b474d6c480",
        *                "checksumAlg" : "SHA1",
        *                "encryptionAlg" : "NONE",
        *                "compressionAlg" : "NONE"
        *             }
        *          ]
        *       },
        *       "rg2" : {
        *          "winnerNode" : "rg2-rn2",
        *          "jdbFilesList" : [
        *             {
        *                "fileName" : "000.jdb",
        *                "filePath" : "<path>",
        *                ....
        *                ....
        *             },
        *             {
        *                "fileName" : "001.jdb",
        *                "filePath" : "<path>",
        *                ....
        *                ....
        *             }
        *          ]
        *       },
        *       "rg3" : {
        *          "winnerNode" : "rg3-rn3",
        *          "jdbFilesList" : [
        *             {
        *                "fileName" : "000.jdb",
        *                "filePath" : "<path>",
        *                ....
        *                ....
        *             },
        *             {
        *                "fileName" : "001.jdb",
        *                "filePath" : "<path>",
        *                ....
        *                ....
        *             }
        *          ]
        *       }
        *       "admin" : {
        *          "winnerNode" : "admin1",
        *          "jdbFilesList" : [
        *             {
        *                "fileName" : "000.jdb",
        *                "filePath" : "<path>",
        *                "checksum" : "71eea00d90575cf4f256d932bae36e0de67ef",
        *                "checksumAlg" : "SHA1",
        *                "encryptionAlg" : "NONE",
        *                "compressionAlg" : "NONE"
        *             },
        *             {
        *                "fileName" : "001.jdb",
        *                "filePath" : "<path>",
        *                ....
        *                ....
        *             }
        *          ]
        *       }
        *    }
        * }
        */

        /*
         * Creating a JSON object for having required file information
         * for this kvstore
         */
        final ObjectNode jsonKVstoreRequiredFiles =
            JsonUtils.createObjectNode();

        /*
         * Compute and add winner node information for every shard in kvstore
         */
        final List<ARTShard> shardList = kvstore.getShardList();
        for (ARTShard shard : shardList) {
            final ObjectNode jsonShard = prepareJSONShardObject(shard);
            jsonKVstoreRequiredFiles.set(shard.getShardName(), jsonShard);
        }
        return jsonKVstoreRequiredFiles;
    }

    private ObjectNode prepareJSONShardObject(ARTShard shard) {

        final Map<String, SnapshotManifest> prospectiveWinnerNodes =
            shard.getProspectiveWinnerNodeList();

        /*
         * Calculate winner node and update information
         * of winner node with required files in Json object.
         *
         * Winner node will have successful backup and highest
         * vlsn. If there are more than one node having successful
         * backup and same vlsn, then winner node will be the one
         * with Master status. In case Master node does not have
         * successful backup and other replica nodes have successful
         * backup then winner node will be the one having highest
         * vlsn among them. In case there are more than one such
         * node having same vlsn, then winner node will the one having
         * lower ip address.
         *
         * All the prospective winner nodes have successful backup so
         * that condition is already checked before adding prospective
         * winner nodes to shard object.
         */
        SnapshotManifest winnerNode = null;
        for (SnapshotManifest prospectiveWinnerNode :
                 prospectiveWinnerNodes.values()) {

            if (winnerNode == null) {
                winnerNode = prospectiveWinnerNode;
                continue;
            }

            if (prospectiveWinnerNode.getSequence() >
                winnerNode.getSequence()) {
                /*
                 * a higher vlsn candidate found, update the winner node
                 * and continue
                 */
                winnerNode = prospectiveWinnerNode;
                continue;
            }

            /*
             * If both nodes vlsn are same, then check
             *
             * If prospective winner node is Master, update the
             * winner node and continue.
             *
             * If none of the nodes are Master then check for node
             * having earlier name (e.g. rg1 is earlier than rg2). Node
             * having earlier name will be selected as the winner node.
             */
            if (prospectiveWinnerNode.getSequence() ==
                    winnerNode.getSequence()) {
                if (prospectiveWinnerNode.getIsMaster()) {
                    winnerNode = prospectiveWinnerNode;
                    continue;
                }

                if (!prospectiveWinnerNode.getIsMaster() &&
                    !winnerNode.getIsMaster()) {
                    /*
                     * Master does not have successful backup or
                     * not yet considered. Winner node will be
                     * one having earlier name.
                     */
                    if (winnerNode.getNodeName().compareTo(
                            prospectiveWinnerNode.getNodeName()) > 0) {
                        winnerNode = prospectiveWinnerNode;
                    }
                }
            }
        }

        return prepareJSONNodeObject(winnerNode, shard);
    }

    private ObjectNode prepareJSONNodeObject(SnapshotManifest winnerNode,
                                             ARTShard shard) {

        /*
         * Creating a JSON object having files information
         * for derived winner node of this shard.
         */
        final ObjectNode jsonRequiredFiles =
            JsonUtils.createObjectNode();

        /*
         * winnerNode can not be null for a derived ART.
         */
        String fullNodeName = null;
        if (!winnerNode.getNodeName().startsWith("rg")) {
            fullNodeName = "admin" + winnerNode.getNodeName();
        } else {
            fullNodeName = winnerNode.getNodeName();
        }
        jsonRequiredFiles.put("winnerNode", fullNodeName);
        final ArrayNode fileArray =
            jsonRequiredFiles.putArray("jdbFilesList");

        /*
         * Get files information for this winner node
         */
        final SortedMap<String, LogFileInfo> snapshotFiles =
            winnerNode.getSnapshotFiles();

        for (Map.Entry<String, LogFileInfo> entry :
                 snapshotFiles.entrySet()) {

            final String logFileName = entry.getKey();
            final LogFileInfo logFileInfo = entry.getValue();

            String logFilePath = baseDirectory + "/" +
                shard.getKVStoreName() + "/";

            /*
             * We can get nodeName through winnerNode or logFileInfo,
             * both are same. For the case of admin, we have nodeName
             * as only identifier i.e 1,2,3. For rgX-rnY, we have
             * node name as rg1-rn2.
             */
            if (logFileInfo.getNodeName().startsWith("rg")) {
                final String nodeName = logFileInfo.getNodeName();
                final String [] subNodeNames = nodeName.split("-");
                logFilePath += subNodeNames[0] + "/" + subNodeNames[1] + "/";
            } else {
                /*
                 * admin node
                 */
                logFilePath += "admin" + logFileInfo.getNodeName() + "/";
            }
            logFilePath += logFileInfo.getSnapshot() + "/" + logFileName;
            final ObjectNode filePath = JsonUtils.createObjectNode();
            filePath.put("fileName", logFileName);
            filePath.put("filePath", logFilePath);

            /*
             * Apart from jdb file path, we will need checksum, checksumAlg,
             * compressionAlg and encryptionAlg in requiredfiles.json which
             * will be used for verification and copy from backup archive
             * copy in AdminRecover and SNRecover enhancements run.
             *
             * During Disaster Recovery testing, it has been noticed that
             * scheduled backups were running with SHA-1. Since, we do not
             * have facility of dumping specified checksumAlg in manifest
             * file in 18.4 hence it leads to the mis match between default
             * checksumAlg i.e. SHA-256 and specified checksumAlg i.e SHA-1.
             * To solve this issue, we will derive specified checksumAlg
             * from length of computed checksum which is part of manifest
             * file in manifest version 1. For manifest version 2, we will
             * have specified checksumAlg available in manifest file.
             *
             * TODO: This approach will not work for all the valid checksumAlg
             * because checking just on length could have one to many
             * relationship with checksumAlg. Since cloud is the only user for
             * Disaster Recovery at this point and did not specify any other
             * checksumAlg apart from SHA-1 hence this approach will work.
             */
            final String logFileBackupChecksum = logFileInfo.getChecksum();
            String logFileChecksumAlg = null;
            if (winnerNode.getVersion() < 2) {
                if (logFileBackupChecksum.length() ==
                        SHA_256_CHECKSUMALG_LENGTH) {
                    logFileChecksumAlg = SHA_256_CHECKSUMALG;
                } else if (logFileBackupChecksum.length() ==
                               SHA_1_CHECKSUMALG_LENGTH) {
                    logFileChecksumAlg = SHA_1_CHECKSUMALG;
                } else {
                    /*
                     * If checksum length does not match for SHA-256 and SHA-1
                     * then it is invalid checksumAlg specified during
                     * scheduled backups. At this point, we allow only these
                     * two checksumAlg in manifest version 1.
                     */
                    throw new IllegalArgumentException(
                        "Checksum with length " +
                        logFileBackupChecksum.length() +
                        " is not valid for the either of the SHA-1 and" +
                        " SHA-256 algorithms supported for scheduled backups");
                }
            } else {
                logFileChecksumAlg = logFileInfo.getChecksumAlg();
            }
            filePath.put("checksum", logFileInfo.getChecksum());
            filePath.put("checksumAlg", logFileChecksumAlg);
            filePath.put("encryptionAlg", logFileInfo.getEncryptionAlg());
            filePath.put("compressionAlg", logFileInfo.getCompressionAlg());
            fileArray.add(filePath);
        }
        return jsonRequiredFiles;
    }

    /**
     * Scan through the created structure to identify the
     * availability of successful backup from one of the node
     * in the shard across all shards in all kvstores in object
     * storage. If we can get successful backup on at least one of
     * the node in all shards for all kvstores then we will return
     * true with current time stamp being actual recovery time.
     *
     * @return true, if successful backup exists for all shards in all
     * kvstores, else false.
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean canCalculateART()
        throws IOException, InterruptedException {

        /*
         * check for ART calculation possibility at each kvstore
         */
        for (ARTKVStore kvstore : kvstorelist) {
            if (!canCalculateKVStoreART(kvstore)) {
                return false;
            }
        }
        return true;
    }

    private boolean canCalculateKVStoreART(ARTKVStore kvstore)
        throws IOException, InterruptedException {

        /*
         * check for ART calculation possibility at each shard
         * for this kvstore
         */
        final List<ARTShard> shardList = kvstore.getShardList();
        for (ARTShard shard : shardList) {
            if (!canCalculateShardART(shard)) {
                return false;
            }
        }
        return true;
    }

    private boolean canCalculateShardART(ARTShard shard)
        throws IOException, InterruptedException {

        /*
         * Check for ART calculation possibility at shard. shardNodeList
         * represents nodes in this shard. We will iteratively copy manifest
         * file for each node, deserialize the json object and check if
         * successful backup exists in the nodes. If yes, then return true
         * for this shard, else return false.
         *
         * If successful backup exists for a node in shard then add that node
         * as prospective winner nodes to the shard object. More details in
         * generating zip related code.
         */
        boolean isShardART = false;
        for (Entry<String, String> entry : shard.getNodeList().entrySet()) {
            final String nodeName = entry.getKey();
            final String manifestPathName = entry.getValue();

            /*
             * Copy manifest file for this node into a temporary file and
             * then de-serialize temporary file into SnapshotManifest object.
             * Check for successful backup. If yes, mark isShardArt to true,
             * add SnapshotManifest object to prospectiveWinnerNodes map for
             * finding winner node and required files for winner node in this
             * shard during generating zip file once ART is established.
             *
             * Temporary manifest file will be created under /tmp with name
             * tempManifestXXX.json
             */
            File tempManifestFile = null;
            try {
                try {
                    tempManifestFile =
                        Files.createTempFile("tempManifest", ".json").toFile();
                } catch (IOException e) {
                    System.err.println("Exception while creating local"
                                       + " temporary manifest file.");
                    throw e;
                }

                copyManifestFile(tempManifestFile, manifestPathName);

                /*
                 * Manifest file has been successfully copied in
                 * tempManifestFile. De-serialize into SnapshotManifest
                 * object. Check if it represent a complete backup.
                 */
                SnapshotManifest manifestObject = null;
                try {
                    /*
                     * De-serialize temporary JSON file into SnapshotManifest
                     * object
                     */
                    byte[] tempManifestFileContent =
                        Files.readAllBytes(tempManifestFile.toPath());
                    manifestObject =
                        SnapshotManifest.deserialize(tempManifestFileContent);
                } catch (IOException e) {
                    System.err.println(
                        "Exception while deserializing copied temporary "
                        + "manifest file: " + manifestPathName);
                }

                if (manifestObject != null &&
                    manifestObject.getIsComplete()) {
                    isShardART = true;
                    shard.addProspectiveWinnerNodeInfo(
                        nodeName, manifestObject);
                }
            } finally {
                /*
                 * Irrespective of success or failure of above operation,
                 * tempFileList can be deleted now.
                 */
                try {
                    if (tempManifestFile != null) {
                        GenerateConfig.delete(tempManifestFile);
                    }
                } catch (IOException e) {
                    System.err.println("Exception while deleting temporary"
                                       + " file list " + tempManifestFile
                                       + " in ARTRequiredFiles utility");
                    throw e;
                }
            }
        }

        /*
         * At this point, we have checked if ART calculation is possible
         * for this shard. If yes, we have added prospective winner nodes
         * to shard object.
         *
         * We will do winner nodes calculation during creation of
         * requiredfiles.json files and zip file. If we do it currently
         * and in case some other follow up shard does not give successful
         * backup then computation of winner nodes for this shard and
         * previous shards will be of no use. So we will do winner node
         * computation only after establishing ART during target zip file
         * generation.
         */
        return isShardART;
    }

    private void copyManifestFile(File tempManifestFile,
                                  String manifestPathName)
        throws InterruptedException {

        URL baseDirectoryURL = null;
        try {
            baseDirectoryURL =
                new File(manifestPathName).toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(
                "Unexpected malformed file URL for: " +
                baseDirectoryURL, e);
        }

        /*
         * Following similar re-try mechanism as used during
         * copying files to object storage during scheduled
         * snapshot. Interrupted exceptions are being handled
         * in same way as IOException.
         */
        long retryWait = INITIAL_RETRY_WAIT_MS;
        while (true) {
            try {
                /*
                 * In the case of manifest file copy from archive to local
                 * storage, checksum is not computed as no checksumAlg is in
                 * use. Hence returned value of computed checksum is null
                 * in manifest file copy operation.
                 */
                final byte[] checksumBytes =
                    recoverCopy.copy(baseDirectoryURL, tempManifestFile);
                assert checksumBytes == null;
                break;
            } catch (IOException|InterruptedException e) {
                System.err.println(
                    "Problem copying manifest file: " + manifestPathName +
                    ", retrying in: " + retryWait + " ms" +
                    ", exception: " + e);
                BackupManager.sleepMs(retryWait);
                retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
            }
        }
    }

    private void generateAllKVStoreStructure(
        ArrayList<String> manifestFilePathList) throws Exception {

        for (final String manifestFilePath : manifestFilePathList) {
            /*
             * Remove base archiveURL from manifestFilePath and split
             * manifestFilePath in sub-parts around "/"
             */
            final String[] manifestFileParts =
                generateManifestFileParts(manifestFilePath);

            /*
             * Check if object for this kvstore already exists in
             * kvstorelist, if yes then add further members
             * of this kvstore e.g. shards and nodes, if not then first
             * add this kvstore object to the list and then add members of
             * this kvstore.
             */
            final String kvstoreName = manifestFileParts[0];
            ARTKVStore kvstore = null;
            for (ARTKVStore kvs : kvstorelist) {
                if (kvstoreName.equals(kvs.getKVStoreName())) {
                    kvstore = kvs;
                    break;
                }
            }
            if (kvstore == null) {
                /*
                 * This KVStore is not yet part of the kvstore list.
                 * Add this kvstore to the kvstore list and then add
                 * further members of this kvstore.
                 */
                kvstore = new ARTKVStore(kvstoreName);
                kvstorelist.add(kvstore);
            }
            kvstore.addKVStoreMembers(manifestFilePath, manifestFileParts);
        }
    }

    /**
     * Parses the manifest file path, returning an array with:
     * - Store name
     * - Node name, either single element adminX or two elements rgX and rnY
     * - Backup name
     * - File name: XXXXXXXX.jdb
     */
    private String[] generateManifestFileParts(String manifestFilePath) {
        /*
         * Example :
         *
         * /andc/backups/DStore1/admin1/18071007/00000002.jdb
         * will be divided as :
         * DStore1 admin1 18071007 00000002.jdb
         *
         * /andc/backups/DStore1/rg1/rn1/18071007/00000002.jdb
         * will be divided as :
         * DStore1 rg1 rn1 18071007 00000002.jdb
         *
         * In unit tests on JE, backup files format is as follow :
         *
         * /andc/backups/DStore1/1/18071007/00000002.jdb
         * will be divided as :
         * DStore1 1 18071007 00000002.jdb
         *
         * /andc/backups/DStore1/rg1-rn1/18071007/00000002.jdb
         * will be divided as :
         * DStore1 rg1-rn1 18071007 0000002.jdb
         */
        final String subManifestFilePath =
            manifestFilePath.substring(baseDirectory.length() + 1);
        return subManifestFilePath.split("/");
    }

    /**
     * Parses the manifest file path, returns time stamp in the path
     * of the manifest file.
     */
    private String getManifestTimeStamp (String manifestFilePath) {
        final String [] subList = manifestFilePath.split("/");
        return subList[subList.length-2];
    }

    /**
     * Performs the pruning on the list of manifest files path in
     * tempFileList to get final list of to be copied manifest
     * files for calculating ART and required winner nodes file.
     *
     * @throws Exception
     */
    private void getManifestFileList() throws IOException {
        BufferedReader manifestFileListBuffer = null;
        try {
            try {
                manifestFileListBuffer =
                    new BufferedReader(new FileReader(tempFileList));
                String manifestFilePath = null;
                while ((manifestFilePath =
                        manifestFileListBuffer.readLine())
                           != null) {
                    final String prospectiveART =
                        getManifestTimeStamp(manifestFilePath);

                    /*
                     * check if this manifest file should be considered if
                     * time stamp in the path of the manifest file is less
                     * than or equal to the target recovery time.
                     *
                     * Actual recovery time will always be less than or equal
                     * to target recovery time.
                     */
                    if (prospectiveART.compareTo(targetRecoveryTime) <= 0) {
                        addActualRecoveryTimeMap(prospectiveART,
                                                 manifestFilePath);
                    }
                }
            } catch (IOException e) {
                System.err.println(
                    "Exception while filtering path of manifest files "
                    + "to be considered for copying from list of manifest "
                    + "files stored under backup archive on "
                    + "object storage");
            }
        } finally {
            if (manifestFileListBuffer != null) {
                manifestFileListBuffer.close();
            }
        }
    }

    private void addActualRecoveryTimeMap(final String prospectiveART,
                                          final String manifestFilePath) {

        /*
         * we have got a valid manifest file path for a
         * particular time stamp which is lesser or equal
         * to the target recovery time. Put this manifest
         * file into the actualRecoveryTimeMap which is
         * reverse sorted on time stamp.
         *
         * ActualRecoveryTimeMap will be used for generating
         * required kvstore:shard structures for identifying
         * winner nodes in shards across kvstores and calculating
         * actual recovery time.
         */
        List<String> artManifestFiles =
            actualRecoveryTimeMap.computeIfAbsent(
                prospectiveART, k -> new ArrayList<String>());
        artManifestFiles.add(manifestFilePath);
    }

    /**
     * Performs the validation of input config file passed
     * as an input argument.
     * @throws IOException, InterruptedException
     */
    private void generateTempFileList()
        throws IOException, InterruptedException {

        tempFileList = File.createTempFile("tempARTDir", ".tmp");

        URL baseDirectoryURL = null;
        try {
             baseDirectoryURL = new File(baseDirectory).toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(
                "Unexpected malformed file URL for: " +
                baseDirectoryURL, e);
        }

        try {
            recoverCopy.getFileList(baseDirectoryURL, tempFileList,
                                    true /* only manifest files in list */);
        } catch (IOException|InterruptedException e) {
            System.err.println("Exception while getting list of file under "
                               + baseDirectory +" in object storage");
            throw e;
        }
    }

    /**
     * This class represents the shards structure of a kvstore. This
     * will represent the kvstore name, admin shard and RepGroups in
     * kvstore.
     */
    private class ARTKVStore {

        /*
         * kvstore name
         */
        private final String kvstoreName;

        /*
         * List of the shards that this kvstore is hosting
         */
        private final List<ARTShard> shardList = new ArrayList<>();

        public ARTKVStore(String kvstoreName) {
            this.kvstoreName = kvstoreName;
        }

        private List<ARTShard> getShardList() {
            return shardList;
        }

        private String getKVStoreName() {
            return kvstoreName;
        }

        private void addKVStoreMembers(String manifestFilePath,
                                       String[] manifestFileParts)
            throws Exception {

            /*
             * Check if ARTShard object for this shard already exists
             * in shard list. If yes then add further members of this
             * shard i.e nodes. If not then first add this shard object
             * to the list and then add members of this shard.
             *
             * There can be two types of shard i.e admin and repgroup
             * shard. For admin shard, name of the shard will be admin.
             * There can be only single admin shard in kvstore, hence
             * keeping name of admin shard to be "admin" will work.
             *
             * For repgroup shard, name of the shard will be rgX.
             */
            String shardName = manifestFileParts[1];
            if (shardName.startsWith("admin") ||
                shardName.matches("[0-9]+")) {
                /*
                 * [0-9]+ will not be scenario on the object storage because
                 * we are putting full path as *'/adminX'/* or *'/rgX/rnY'/*
                 * in BackupObjectStorageLocation.
                 *
                 * This is primarily to support unit tests with nodeName format
                 * in BackupLocation in JE where path can be '/X'/* for admin
                 * and *'/rgX-rnY'/* for RNs case.
                 *
                 * JE format path does not support kvstore name in path. We
                 * will explicitly keep kvstore name in path for unit tests
                 * to match object storage path format.
                 */
                shardName = "admin";
            } else if (shardName.contains("-")) {
                /*
                 * Support rgx-rny format for case mentioned above
                 */
                shardName = shardName.substring(0, shardName.indexOf("-"));
            }
            ARTShard kvshard = null;
            for (ARTShard shard : shardList) {
                if (shardName.equals(shard.getShardName())) {
                    kvshard = shard;
                    break;
                }
            }
            if (kvshard == null) {
                /*
                 * This shard is not yet part of the shard list in this
                 * kvstore. Add this shard to the list of shards and
                 * then add further nodes in this shard for this kvstore.
                 */
                kvshard = new ARTShard(shardName, kvstoreName);
                shardList.add(kvshard);
            }
            kvshard.addShardMembers(manifestFilePath, manifestFileParts);
        }
    }

    /**
     * This class represents a specific shard structure for a kvstore.
     * This will represent both the Admin and RepGroup shard. This
     * ARTShard object will also be used to identify winner nodes
     * (if present) and required files for nodes in this shard object.
     */
    private class ARTShard {

        /*
         * shard name. For the case of admin shard, it will be "admin"
         * For repGroup case, it will be rgX.
         */
        private final String shardName;

        /*
         * kvstore name to which this shard belongs to. This is used while
         * forming complete path for winner node jdb files during zip/json
         * file creation.
         */
        private final String kvstoreName;

        /*
         * List of nodes and manifest file path name for individual
         * node in this shard. For admins, node name will be adminX.
         * For repNodes, nodes node will be rgX-rnY.
         */
        private Map<String, String> shardNodeList = new HashMap<>();

        /*
         * List of nodes which have successful backup for this shard
         */
        private Map<String, SnapshotManifest> prospectiveWinnerNodes =
            new HashMap<String, SnapshotManifest>();

        private ARTShard(String shardname, String kvstoreName) {
            this.shardName = shardname;
            this.kvstoreName = kvstoreName;
        }

        public String getKVStoreName() {
            return kvstoreName;
        }

        private void addProspectiveWinnerNodeInfo(String nodeName,
                SnapshotManifest manifestObject) {
            prospectiveWinnerNodes.put(nodeName, manifestObject);
        }

        private Map<String, String> getNodeList() {
            return shardNodeList;
        }

        private String getShardName() {
            return shardName;
        }

        private Map<String, SnapshotManifest> getProspectiveWinnerNodeList() {
            return prospectiveWinnerNodes;
        }

        private void addShardMembers(String manifestFilePath,
                                     String[] manifestFileParts)
            throws Exception {

            /*
             * Check if current node of this shard exists or not,
             * if it does not exist then add this node to the
             * shard node list else raise error.
             *
             * Error case will not happen because we will adding
             * only new nodes in shard since it is single pass over
             * list of manifest files for time stamp from object
             * storage. And there can not be two manifest files for a
             * node on particular time stamp of backup.
             *
             * When we will optimize this process as mentioned above
             * in processTimeStampManifestFileList then we will update
             * the same structure with new manifest file path from
             * previous time stamp.
             */
            String nodeName = null;
            if (manifestFileParts[1].startsWith("admin") ||
                manifestFileParts[1].matches("[0-9]+") ||
                manifestFileParts[1].matches("rg*-")) {
                /*
                 * [0-9]+ and rg*- checks are for unit tests having path
                 * format according to JE, supporting *'/X'/ for admin
                 * and *'/rgX-rnY'/* format for RNs.
                 */
                nodeName = manifestFileParts[1];
            } else if (manifestFileParts[1].startsWith("rg")) {
                nodeName = manifestFileParts[1] + "-" + manifestFileParts[2];
            } else {
                throw new Exception("Invalid node name " + manifestFileParts[1]
                                    + " in manifest file path");
            }

            /*
             * Check nodeName should not be there in shardNodeList. There
             * can not be two manifest files for a node on particular time
             * stamp. If it exists then raise errors.
             *
             * We should not hit this case, just a preventive check.
             */
            if (shardNodeList.containsKey(nodeName)) {
                throw new IllegalStateException(
                    "Multiple manifest files exists for a single node i.e. "
                    + nodeName + " in backup archive on object storage. Please"
                    + " manually check manifest files for node " +
                    nodeName + " at time stamp " +
                    manifestFileParts[manifestFileParts.length - 2]);
            }
            shardNodeList.put(nodeName, manifestFilePath);
        }
    }

    /**
     * Performs the validation of input config file passed
     * as an input argument.
     *
     * @throws IOException if there is an I/O error
     * @throws InterruptedException if RecoveryFileCopy object initialization
     *         is interrupted
     */
    private void validateConfigFile()
        throws IOException, InterruptedException {

        /*
         * Input config file existence check
         */
        if (!inputConfigPath.exists()) {
            printUsage("Specified input config file " +
                       inputConfigPath.getAbsolutePath() +
                       " does not exist");
        }

        /*
         * Parse and validate required, optional and non-used parameters
         * in input config file.
         */
        try {
            validateARTRequiredParameters();
        } catch (IOException e) {
            throw new IOException("Exception while parsing and validating "
                                  + "ARTRequiredFiles specific parameters: "
                                  + e, e);
        }

        try {
            validateRecoveryArchiveFSCopyParameters();
        } catch (InterruptedException e) {
            throw new InterruptedException("InterruptedException while parsing"
                                           + " and validating object storage"
                                           + " copy related parameters");
        } catch (IOException e) {
            throw new IOException("IOException while parsing and validating"
                                  + "object storage copy related parameters: "
                                  + e, e);
        }
    }

    private void validateRecoveryArchiveFSCopyParameters()
        throws InterruptedException, IOException {

        if (recoverCopy != null) {
            throw new IllegalStateException("recovercopy object already "
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
                "recoverCopyClass missing from: " + initProperties);
        }

        /*
         * Instantiate RecoverFileCopy object before doing copy
         * operation.
         */
        try {
            recoverCopy =
                BackupManager.getImplementationInstance(
                    RecoverFileCopy.class, recoveryCopyClass);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Illegal recoveryCopyClass argument: "
                + recoveryCopyClass + ": " + e,
                e);
        }

        /*
         * New temporary file need to be created that will have items
         * from input config file and additionally [checksum,encryption
         * and compression] algorithm information i.e NONE will be added.
         */
        final File tempInputConfigFile =
            createTempInputConfigFile("manifest");

        /*
         * Add NONE as checksum, encryption and compression algorithm to
         * the temporary input config file
         */
        addAlgorithmsInConfigFile(tempInputConfigFile, "NONE", "NONE",
                                  "NONE");

        /*
         * Initialize RecoverFileCopy object
         */
        try {
            recoverCopy.initialize(tempInputConfigFile);
        } catch (InterruptedException e) {
            throw new InterruptedException("Exception while parsing and"
                                           + " validating RecoveryCopyClass"
                                           + " parameters: " + e);
        } catch (IOException e) {
            throw new IOException("Exception while parsing and validating "
                                  + "RecoveryCopyClass parameters: "
                                  + e, e);
        }
    }

    private void validateARTRequiredParameters()
        throws IOException {

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

        baseDirectory = (String) props.remove(BASEDIR_KEY);
        if (baseDirectory == null) {
            throw new IllegalArgumentException(
                "baseArchiveURL missing from: " + inputConfigPath);
        }

        if (!Paths.get(baseDirectory).isAbsolute()) {
            throw new IllegalArgumentException(
                "The baseArchiveURL: " + baseDirectory +
                " path must be an absolute pathname");
        }

        /* Successful initialization. */
        initProperties = props;
    }

    /**
     * Performs the parsing of the ARTRequiredFiles command arguments.
     *
     * @param argv ARTRequiredFiles command arguments
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
            } else if (thisArg.equals("-targetRecoveryTime")) {
                if (argc < nArgs) {
                    targetRecoveryTime = argv[argc++];
                    if ("".equals(targetRecoveryTime)) {
                        printUsage("Target Recovery Time must not be empty");
                    }
                } else {
                    printUsage("-targetRecoveryTime requires an argument");
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
         * Check if target parameter is specified
         */
        if (targetPath == null) {
            printUsage("-target flag argument not specified");
        }

        /*
         * Check if targetRecoveryTime parameter is specified
         */
        if (targetRecoveryTime == null) {
            printUsage("-targetRecoveryTime missing from input parameters");
        }

        /*
         * We are assuming that target recovery time is mentioned in
         * yymmddhh format. In production, backup is enabled to run
         * every day but this frequency might be reduced in future hence
         * ARTRequiredFiles utility is designed to support hour granularity
         * as well in future.
         */
        if ((targetRecoveryTime.length() != "YYMMDDHH".length()) ||
            !targetRecoveryTime.matches("[0-9]+")) {
            printUsage("targetRecoveryTime should be in YYMMDDHH format"
                       + " with all characters as digits");
        }

        /*
         * Parsing completed successfully. Validate if input config file
         * exists on specified path. If not, exit before proceeding. If yes,
         * then parse and validate the input config file parameters.
         * targetRecoveryTime, backupDirPath and recoverycopyclass
         * are parsed and validated separately since they are not
         * needed/common to RecoverArchiveFSCopy object.
         *
         * Common information required for RecoverArchiveFSCopy object
         * instantiation is sub-set of input config file parameters list.
         *
         * This RecoverArchiveFSCopy object will be further used in getting
         * list of file under backupDir and copying manifest file to identify
         * winner nodes & calculating ART.
         */
        validateConfigFile();
    }

    private void printUsage(String msg) throws IllegalArgumentException {
        throw new IllegalArgumentException((msg != null) ?
                            msg + "\n" + usage() :
                            usage());
    }

    private static String usage() {
        return "Usage : "
                + "java -jar <recovery.jar> "
                + "artrequiredfiles "
                + "-targetRecoveryTime <yymmddhh>"
                + "-config <configuration file path> "
                + "-target <zipfile>"
                + " [ -debug ]";
    }

    /*
     * Convenience method for ARTRequiredFiles tests
     * @param argv the command line arguments
     * @return whether the operation succeeded
     */
    public static boolean mainInternal(final String[] argv) {

        ARTRequiredFiles artRequiredFiles = new ARTRequiredFiles();

        /* Parse the ARTRequiredFiles command line arguments */
        try {
            artRequiredFiles.parseArgs(argv);
        } catch (Exception e) {
            if (artRequiredFiles.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in parsing arguments for" +
                               " ARTRequiredFiles: " + e);
            return false;
        }

        try {
            artRequiredFiles.generateARTRequiredFiles();
            System.out.println("RequiredFiles json files and ART information"
                               + " successfully generated at " +
                               artRequiredFiles.targetPath.getAbsolutePath());
        } catch (Exception e) {
            if (artRequiredFiles.isPrintStackTrace) {
                e.printStackTrace();
            }
            System.err.println("Exception in running ARTRequiredFiles "
                               + "Utility: " + e);
            return false;
        }
        return true;
    }

    /**
     * The main used by the ARTRequiredFiles Utility
     *
     * @param argv An array of command line arguments to the
     * ARTRequiredFiles utility.
     */
    public static void main(String[] argv) {

        final boolean succeeded = mainInternal(argv);
        if (!succeeded) {
            System.exit(1);
        }
    }
}
