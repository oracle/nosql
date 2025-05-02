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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.GenerateConfig;

import com.sleepycat.je.RecoverFileCopy;
import com.sleepycat.je.dbi.BackupManager;

/** Provides common facilities for covering admins and SNs. */
public class AdminSNRecover {

    public static final String RECOVERY_COPY_CLASS_KEY = "recoveryCopyClass";

    /** The initial wait before retrying after an I/O error. */
    static final long INITIAL_RETRY_WAIT_MS = 1000;

    /** The maximum wait before retrying after an I/O error. */
    static final long MAX_RETRY_WAIT_MS = 60 * 60 * 1000;

    File inputConfigPath = null;
    String recoveryCopyClass = null;
    Properties initProperties = null;
    boolean isPrintStackTrace = false;
    Throwable adminSNRecoverException = null;

    final Logger logger = LoggerUtils.getLogger(getClass(), "AdminSNRecover");

    static class NodeJDBFiles {

        String winnerNode;
        List<JDBFile> jdbFilesList;

        public NodeJDBFiles() {

        }

        public NodeJDBFiles(String nodeName,
                            List<JDBFile> winnerNodeJDBFiles) {
            this.winnerNode = nodeName;
            this.jdbFilesList = winnerNodeJDBFiles;
        }

        String getNodeName() {
            return winnerNode;
        }

        List<JDBFile> getJDBFilesList() {
            return jdbFilesList;
        }

        void setWinnerNode(String winnerNode) {
            this.winnerNode = winnerNode;
        }

        void setjdbFilesList(List<JDBFile> jdbFilesList) {
            this.jdbFilesList = jdbFilesList;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof NodeJDBFiles)) {
                return false;
            }
            final NodeJDBFiles other = (NodeJDBFiles) object;
            return Objects.equals(winnerNode, other.winnerNode) &&
                   Objects.equals(jdbFilesList, other.jdbFilesList);
        }

        @Override
        public int hashCode() {
            int hash = 61;
            hash = (73 * hash) + Objects.hashCode(winnerNode);
            hash = (73 * hash) + Objects.hashCode(jdbFilesList);
            return hash;
        }
    }

    static class JDBFile {

        String fileName;
        String filePath;
        String checksum;
        String checksumAlg;
        String encryptionAlg;
        String compressionAlg;

        public JDBFile() {

        }

        public JDBFile(String fileName,
                       String filePath,
                       String checkSum,
                       String checkSumAlg,
                       String encryptionAlg,
                       String compressionAlg) {
            this.fileName = fileName;
            this.filePath = filePath;
            this.checksum = checkSum;
            this.checksumAlg = checkSumAlg;
            this.encryptionAlg = encryptionAlg;
            this.compressionAlg = compressionAlg;
        }

        String getFileName() {
            return fileName;
        }

        String getFilePath() {
            return filePath;
        }

        String getChecksum() {
            return checksum;
        }

        String getChecksumAlg() {
            return checksumAlg;
        }

        String getEncryptionAlg() {
            return encryptionAlg;
        }

        String getCompressionAlg() {
            return compressionAlg;
        }

        void setFileName(String fileName) {
            this.fileName = fileName;
        }

        void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        void setChecksum(String checksum) {
            this.checksum = checksum;
        }

        void setChecksumAlg(String checksumAlg) {
            this.checksumAlg = checksumAlg;
        }

        void setEncryptionAlg(String encryptionAlg) {
            this.encryptionAlg = encryptionAlg;
        }

        void setCompressionAlg(String compressionAlg) {
            this.compressionAlg = compressionAlg;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof JDBFile)) {
                return false;
            }
            final JDBFile other = (JDBFile) object;
            return Objects.equals(fileName, other.fileName) &&
                   Objects.equals(filePath, other.filePath) &&
                   Objects.equals(checksum, other.checksum) &&
                   Objects.equals(checksumAlg, other.checksumAlg) &&
                   Objects.equals(encryptionAlg, other.encryptionAlg) &&
                   Objects.equals(compressionAlg, other.compressionAlg);
        }

        @Override
        public int hashCode() {
            int hash = 51;
            hash = (73 * hash) + Objects.hashCode(fileName);
            hash = (73 * hash) + Objects.hashCode(filePath);
            hash = (73 * hash) + Objects.hashCode(checksum);
            hash = (73 * hash) + Objects.hashCode(checksumAlg);
            hash = (73 * hash) + Objects.hashCode(encryptionAlg);
            hash = (73 * hash) + Objects.hashCode(compressionAlg);
            return hash;
        }
    }

    /*
     * Copy jdbfiles of the identified node.
     */
    void copyFiles(List<JDBFile> jdbFiles,
                   File nodePath,
                   String nodeName)
        throws InterruptedException, IOException {

        /*
         * Start copying jdb files sequentially under nodePath
         */
        for (JDBFile jdbFile : jdbFiles) {

            final String jdbFileName = jdbFile.getFileName();
            final String jdbFilePath = jdbFile.getFilePath();
            final String backupChecksum = jdbFile.getChecksum();
            final String checkSumAlg = jdbFile.getChecksumAlg();
            final String encryptionAlg = jdbFile.getEncryptionAlg();
            final String compressionAlg = jdbFile.getCompressionAlg();

            /*
             * We have node name and jdb files to be copied. Instantiate
             * RecoverFileCopy object specific to the each node jdb file
             * and iterate over each jdb file to copy sequentially.
             *
             * Before instantiating RecoverFileCopy object, we will need to
             * create a new temporary input properties file that will be
             * passed as argument for RecoverFileCopy initialize.
             *
             * New temporary file will have items from input config file and
             * additionally [checksum,encryption and compression] algorithm
             * information will be added that we have retrieved from
             * requiredfiles.json under JDBFiles object.
             */
            File tempInputConfigFile = createTempInputConfigFile(nodeName);

            /*
             * Add jdb file specific checkSum, encryption and compression
             * algorithm to the temporary input config file
             */
            addAlgorithmsInConfigFile(tempInputConfigFile, checkSumAlg,
                                      encryptionAlg, compressionAlg);

            /*
             * Instantiate RecoverFileCopy object before doing copy
             * operation.
             */
            RecoverFileCopy recoverCopy =
                instantiateRecoverFileCopy(tempInputConfigFile);

            /*
             * Start jdb file copy operation
             */
            copyFile(recoverCopy, jdbFilePath, nodePath, jdbFileName,
                     backupChecksum);
        }
    }

    private void copyFile(final RecoverFileCopy recoverCopy,
                          final String jdbFilePath,
                          final File nodePath,
                          final String jdbFileName,
                          final String backupChecksum)
        throws IOException, InterruptedException {

        /*
         * Get URL from jdbFilePath
         */
        URL jdbFileURL = null;
        try {
            jdbFileURL =
                new File(jdbFilePath).toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(
                "Unexpected malformed file URL for: " + jdbFileURL, e);
        }

        /*
         * Create jdb file under nodePath to which jdb file from backup
         * archive will be copied.
         */
        File localJDBFile = null;
        try {
             localJDBFile =
                 new File(nodePath + "/" + jdbFileName);
             if (localJDBFile.exists()) {
                 localJDBFile.delete();
             }
             localJDBFile.createNewFile();
        } catch (IOException e) {
             throw new IOException("Exception while creating local "
                                   + jdbFileName + " file: " + e,
                                   e);
        }

        doCopyOperation(recoverCopy, jdbFileURL, jdbFileName,
                        localJDBFile, backupChecksum);
    }

    private void doCopyOperation(final RecoverFileCopy recoverCopy,
                                 final URL jdbFileURL,
                                 final String jdbFileName,
                                 final File localAdminJDBFile,
                                 final String backupChecksum)
        throws InterruptedException {

        /*
         * Following similar re-try mechanism as used during copying files
         * to object storage during scheduled snapshot. Interrupted
         * exceptions are being handled in same way as IOException.
         * Retries if I/O errors occur during the process on the
         * assumption that they might be transient, but logs them because
         * those failures are not expected at this level. Retries for I/O
         * errors wait a period of time starting at
         * {@value #INITIAL_RETRY_WAIT_MS} and doubling for each retry up
         * to a maximum value of {@value #MAX_RETRY_WAIT_MS}. Copy
         * operation is shutdown in case some other errors happens.
         */
        long retryWait = INITIAL_RETRY_WAIT_MS;
        while (true) {
            /*
             * TODO : Check for thread shutdown?
             */
            adminSNRecoverException = null;
            try {
                final byte[] checksumArchiveBytes =
                    recoverCopy.copy(jdbFileURL, localAdminJDBFile);
                final String checksumArchive =
                    BackupManager.checksumToHex(checksumArchiveBytes);
                final byte[] localFileChecksumBytes =
                    recoverCopy.checksum(localAdminJDBFile);
                final String localFileChecksum =
                    BackupManager.checksumToHex(localFileChecksumBytes);

                /*
                 * Required verification checks related to checksum
                 */
                if (checksumArchive == null) {
                    throw new IllegalArgumentException(
                         "checksum must not be null");
                }
                if ("0".equals(checksumArchive)) {
                    throw new IllegalArgumentException(
                        "checksum for copied entry must not be \"0\"");
                }

                /*
                 * Verification of the jdb file before and after copy.
                 * Also verify if jdb file has been modified on archive.
                 */
                if (!checksumArchive.equals(localFileChecksum)) {
                    throw new IllegalArgumentException(
                        "jdb file checksum before and after copy"
                        + " from archive are different");
                }
                if (!backupChecksum.equals(checksumArchive)) {
                    throw new IllegalArgumentException(
                        "jdb file modified in backup archive. checksum"
                        + " during scheduled backup and during recovery"
                        + " copy are different");
                }
                break;
            } catch (IOException|InterruptedException e) {
                adminSNRecoverException = e;
                String msg =
                    "Problem copying jdb file " + jdbFileName +
                    ", retrying in: " + retryWait + " ms" +
                    ", exception: " + e;
                logger.fine(msg);
                BackupManager.sleepMs(retryWait);
                retryWait = Math.min(retryWait * 2, MAX_RETRY_WAIT_MS);
            }
        }
    }

    private RecoverFileCopy instantiateRecoverFileCopy(final File configFile)
        throws InterruptedException, IOException {

        RecoverFileCopy recoveryCopy = null;
        try {
            recoveryCopy =
                BackupManager.getImplementationInstance(
                    RecoverFileCopy.class, recoveryCopyClass);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Illegal recoverycopyclass argument: "
                + recoveryCopyClass + ": ", e);
        }

        try {
            recoveryCopy.initialize(configFile);
        } catch (InterruptedException e) {
            throw new InterruptedException("Exception while parsing and"
                                           + " validating RecoveryCopyClass"
                                           + " parameters: " + e);
        } catch (IOException e) {
            throw new IOException("Exception while parsing and validating "
                                  + "RecoveryCopyClass parameters: "
                                  + e, e);
        } finally {
            /*
             * Irrespective of success and failure of above operation,
             * tempInputConfigFile can be deleted now.
             */
            try {
                if (configFile != null) {
                    GenerateConfig.delete(configFile);
                }
            } catch (IOException e) {
                throw new IOException("Exception while deleting "
                                      + configFile + " : " + e,
                                      e);
            }
        }
        return recoveryCopy;
    }

    void addAlgorithmsInConfigFile(final File tempInputConfigFile,
                                   final String checksumAlg,
                                   final String encryptionAlg,
                                   final String compressionAlg)
        throws IOException {

        try (/* Open temporary input config file in append mode */
             final BufferedWriter tempInputFileWriter =
                 new BufferedWriter(new FileWriter(tempInputConfigFile,
                                    true))) {
            if (checksumAlg != null) {
                tempInputFileWriter.write("checksumAlg=" + checksumAlg);
                tempInputFileWriter.newLine();
            }
            if (encryptionAlg != null) {
                tempInputFileWriter.write("encryptionAlg=" +encryptionAlg);
                tempInputFileWriter.newLine();
            }
            if (compressionAlg != null) {
                tempInputFileWriter.write("compressionAlg=" +compressionAlg);
            }
        } catch (IOException e) {
            throw new IOException("Exception occurred while adding"
                                  + " file specific algorithms in"
                                  + " temporary input config file: " + e,
                                  e);
        }
    }

    File createTempInputConfigFile(final String nodeName)
        throws IOException {

        File tempInputConfigFile = null;
        try {
            tempInputConfigFile =
                File.createTempFile("recover" + nodeName, ".config");

            /*
             * Copy input config file passed as input command argument
             * to local temporary jdb file specific config file.
             */
            try (final FileInputStream inputConfig =
                     new FileInputStream(inputConfigPath);
                 final FileOutputStream tempConfig =
                     new FileOutputStream(tempInputConfigFile)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputConfig.read(buffer)) > 0) {
                    tempConfig.write(buffer, 0, length);
                }
            }
        } catch(IOException e) {
            throw new IOException("Exception while copying "
                                  + inputConfigPath + " to "
                                  + tempInputConfigFile
                                  + " : " + e,
                                  e);
        }
        return tempInputConfigFile;
    }
}
