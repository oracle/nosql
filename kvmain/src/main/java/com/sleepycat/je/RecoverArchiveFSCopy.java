/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Properties;

import com.sleepycat.je.dbi.BackupManager;

/**
 * @hidden For internal use: disaster recovery from copied scheduled backups
 *
 * An implementation for copying files from regular file system archive into
 * the local fileSystem. The archive would typically be hosted on a different
 * disk that will be used to reinitialize production environment with
 * empty directory structure layout derived from topologoutput.json generated
 * in RecoverConfig utility run. This copy implementation will also be used
 * to copy manifest files in temporary file for deriving actual recovery time.
 * This will also help in getting list of files backed-up in baseArchiveURL for
 * ARTRequiredFiles utility run.
 *
 * <p>The current implementation is primarily intended for testing and to help
 * proof the interface. It does not support compression or encryption and is
 * not tuned for performance.
 */
public class RecoverArchiveFSCopy implements RecoverFileCopy {

    public static final String CHECKSUM_KEY = "checksumAlg";

    /*
     * Do page 4K aligned reads for good performance, 16 pages at a time
     */
    private static final int TRANSFER_BYTES = 0x1000 * 16;

    private volatile Properties initProperties;

    /* Configured properties, parsed from initProperties. */

    /* Archive storage properties, not used currently */
    private volatile String archiveEncryptionAlg;
    private volatile String archiveCompressionAlg;

    /*
     * Local FileSystem storage properties, not used currently.
     * These will need some more discussion, may not be needed in the end.
     */
    private volatile String localEncryptionAlg;
    private volatile String localCompressionAlg;

    private volatile String checksumAlg;

    /* No-args constructor required by implementation. */
    public RecoverArchiveFSCopy() {
        super();
    }

    @Override
    public synchronized void initialize(File configFile)
        throws InterruptedException, IOException {

        if (initProperties != null) {
            throw new IllegalStateException("initProperties already "
                                            + "initialized");
        }

        Objects.requireNonNull(configFile, "configFile arg must not be null");

        final Properties props = new Properties();
        try (InputStream configStream = new FileInputStream(configFile)) {
            props.load(configStream);
        }

        /*
         * checksumAlg is the required property. It will catch scenarios
         * where either checksumAlg value is not specified or checksumAlg
         * key word is misspelled.
         */
        checksumAlg = (String) props.remove(CHECKSUM_KEY);
        if (checksumAlg == null) {
            throw new IllegalArgumentException("checksum alg missing from:" +
                                                props);
        }

        /*
         * In the case of manifest file copy, it is possible that specified
         * checksumAlg is NONE. If specified checksumAlg is NONE, then we do
         * not compute the checksum and return null.
         */
        if (!checksumAlg.equals("NONE")) {
            try {
                /* Validate the algorithm. */
        	MessageDigest.getInstance(checksumAlg);
            } catch (NoSuchAlgorithmException e) {
        	throw new IllegalArgumentException(
                    "Bad checksum algorithm: " + checksumAlg, e);
            }
        }

        /* Successful initialization. */
        initProperties = props;
    }

    @Override
    public String getArchiveEncryptionAlg() {
        requireInitialized();
        return archiveEncryptionAlg;
    }

    @Override
    public String getLocalEncryptionAlg() {
        requireInitialized();
        return localEncryptionAlg;
    }

    @Override
    public String getArchiveCompressionAlg() {
        requireInitialized();
        return archiveCompressionAlg;
    }

    @Override
    public String getLocalCompressionAlg() {
        requireInitialized();
        return localCompressionAlg;
    }

    @Override
    public String getChecksumAlg() {
        requireInitialized();
        return checksumAlg;
    }

    @Override
    public byte[] copy(URL archiveURL, File localFile)
        throws IOException {

        requireInitialized();
        Objects.requireNonNull(localFile, "localFile arg must not be null");
        Objects.requireNonNull(archiveURL, "archiveFile arg must not be null");

        MessageDigest messageDigest = null;
        if (!checksumAlg.equals("NONE")) {
            try {
        	messageDigest = MessageDigest.getInstance(checksumAlg);
            } catch (NoSuchAlgorithmException e) {
                /* Should not happen, already checked during initialization. */
        	throw new IllegalArgumentException(
                    "Bad checksum algorithm: " + checksumAlg, e);
            }
        }

        final File archiveFile = getFileFromURL(archiveURL);
        final long length = archiveFile.length();

        try (final FileInputStream inputStream =
                 new FileInputStream(archiveFile);
             final FileOutputStream outputStream =
                 new FileOutputStream(localFile, false)) {
            final ByteBuffer buffer = ByteBuffer.allocate(TRANSFER_BYTES);
            for (long bytes = length; bytes > 0;) {
                int readSize = (int)Math.min(TRANSFER_BYTES, bytes);
                int readBytes = inputStream.read(buffer.array(), 0, readSize);
                if (readBytes == -1) {
                    throw new IOException("Premature EOF. Was expecting: " +
                                          readSize);
                }
                outputStream.write(buffer.array(), 0, readSize);
                if (messageDigest != null) {
                    messageDigest.update(buffer.array(), 0, readBytes);
                }
                bytes -= readBytes;
            }
        }

        /* Ensure that the copy is persistent in the file system. */
        /* Persist the file itself. */
        BackupManager.forceFile(localFile.toPath());
        /* Persist its directory entry. */
        BackupManager.forceFile(localFile.getParentFile().toPath());

        if (messageDigest != null) {
            return messageDigest.digest();
        } else {
            return null;
        }
    }

    @Override
    public void getFileList(URL baseArchiveURL, File localFile,
                            boolean isOnlyManifest)
        throws IOException {

        requireInitialized();
        Objects.requireNonNull(localFile, "localFile arg must not be null");
        Objects.requireNonNull(baseArchiveURL,
            "baseArchiveFile arg must not be null");

        final File baseArchiveFile = getFileFromURL(baseArchiveURL);
        final FileOutputStream outputStream =
            new FileOutputStream(localFile, false);
        writeToFile(baseArchiveFile, localFile, outputStream,
                    isOnlyManifest);
        outputStream.close();
    }

    private void writeToFile(File archiveFile, File localFile,
                             FileOutputStream outputStream,
                             boolean isOnlyManifest)
        throws IOException {

        File[] filesList = archiveFile.listFiles();

        if (filesList == null) {
            return;
        }

        for (File file : filesList) {
            if (file.isDirectory()) {
                writeToFile(file, localFile, outputStream, isOnlyManifest);
            } else if (!isOnlyManifest  ||
                       file.toString().contains("manifest.json")) {
                try {
                    outputStream.write(file.toString().getBytes());
                    outputStream.write('\n');
                } catch (IOException e) {
                    final String addInfo =
                        isOnlyManifest ? " manifest " : "";
                    throw new IOException(
                        "Exception during writing list of "
                        + addInfo + "files under "
                        + archiveFile.toString() +
                        "to " + localFile.toString());
                }
            }
        }
    }

    File getFileFromURL(URL archiveURL) throws IOException {
        if (!"file".equalsIgnoreCase(archiveURL.getProtocol())) {
            throw new IllegalArgumentException(
                "URL scheme must be file; not " + archiveURL.getProtocol());
        }

        final File archiveFile = new File(archiveURL.getFile());
        if (!archiveFile.exists()) {
            throw new IOException("File does not exist" +
                                  " for " + archiveFile.getAbsolutePath());
        }
        return archiveFile;
    }

    @Override
    public byte[] checksum(File localFile)
        throws InterruptedException, IOException {

        requireInitialized();
        Objects.requireNonNull(localFile, "localFile arg must not be null");

        MessageDigest messageDigest = null;
        if (!checksumAlg.equals("NONE")) {
            try {
                messageDigest = MessageDigest.getInstance(checksumAlg);
            } catch (NoSuchAlgorithmException e) {
                /* Should not happen, already checked during initialization. */
        	throw new IllegalArgumentException(
                    "Bad checksum algorithm: " + checksumAlg, e);
            }
        }

        final long length = localFile.length();
        try (final FileInputStream inputStream =
                 new FileInputStream(localFile)) {

            final ByteBuffer buffer = ByteBuffer.allocate(TRANSFER_BYTES);
            for (long bytes = length; bytes > 0;) {
                int readSize = (int)Math.min(TRANSFER_BYTES, bytes);
                int readBytes = inputStream.read(buffer.array(), 0, readSize);
                if (readBytes == -1) {
                    throw new IOException("Premature EOF. Was expecting: " +
                                          readSize);
                }
                if (messageDigest != null) {
                    messageDigest.update(buffer.array(), 0, readBytes);
                }
                bytes -= readBytes;
            }
        }
        if (messageDigest != null) {
            return messageDigest.digest();
        } else {
            return null;
        }
    }

    private void requireInitialized() {
        if (initProperties == null) {
            throw new IllegalStateException(this.getClass().getName() +
                                            " is not initialized);");
        }
    }
}
