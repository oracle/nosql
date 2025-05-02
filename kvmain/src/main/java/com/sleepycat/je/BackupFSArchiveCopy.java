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
 * @hidden For internal use: automatic backups
 *
 * An implementation for copying files into a regular file system archive. The
 * archive would typically be hosted on a different disk from that used to host
 * the environment log files, or an nfs mounted file system.
 *
 * <p>The current implementation is primarily intended for testing and to help
 * proof the interface. It does not support compression or encryption and is
 * not tuned for performance.
 */
public class BackupFSArchiveCopy implements BackupFileCopy {

    public static final String CHECKSUM_KEY = "checksumAlg";

    /*
     * Do page 4K aligned reads for good performance, 16 pages at a time
     */
    private static final int TRANSFER_BYTES = 0x1000 * 16;

    private volatile Properties initProperties;

    /* Configured properties, parsed from initProperties.
     * 
     * TODO : encryptionAlg and compressionAlg are not currently
     * set in input config file. We have not yet implemented
     * encryption and compression in file copy.    
     */
    private volatile String encryptionAlg;

    private volatile String compressionAlg;

    private volatile String checksumAlg;

    /* No-args constructor required by implementation. */
    public BackupFSArchiveCopy() {
        super();
    }

    @Override
    public synchronized void initialize(File configFile)
        throws InterruptedException, IOException {

        if (initProperties != null) {
            throw new IllegalStateException("Already initialized");
        }

        Objects.requireNonNull(configFile, "configFile arg must not be null");

        final Properties props = new Properties();
        try (InputStream configStream = new FileInputStream(configFile)) {
           props.load(configStream);
        }
        checksumAlg = (String) props.remove(CHECKSUM_KEY);
        if (checksumAlg == null) {
            throw new IllegalArgumentException("checksum alg missing from:" +
                                                props);
        }

        try {
            /* Validate the algorithm. */
            MessageDigest.getInstance(checksumAlg);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(
                "Bad checksum algorithm: " + checksumAlg, e);
        }

        if (props.size() > 0) {
            throw new IllegalArgumentException("Unknown properties:" +
                                               props);
        }
        /* Successful initialization. */
        initProperties = props;
    }

    @Override
    public String getEncryptionAlg() {
        requireInitialized();
        return encryptionAlg;
    }

    @Override
    public String getCompressionAlg() {
        requireInitialized();
        return compressionAlg;
    }

    @Override
    public String getChecksumAlg() {
        requireInitialized();
        return checksumAlg;
    }

    /**
     * Note that the copy implementation can afford to be simple and not clean
     * up on failure since a partially copied archiveURL file can never be part
     * of the manifest. Such a partially copied file will typically be
     * overwritten as the backup manager retries the copy and if it succeeds, it
     * becomes part of the manifest. If the copy is abandoned, the archive
     * pruning utility will eventually delete it and reclaim its storage.
     */
    @Override
    public byte[] copy(InputStream localFileStream, URL archiveURL)
        throws InterruptedException, IOException {

        requireInitialized();
        Objects.requireNonNull(localFileStream,
                               "localFileStream arg must not be null");
        Objects.requireNonNull(archiveURL, "archiveFile arg must not be null");

        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(checksumAlg);
        } catch (NoSuchAlgorithmException e) {
            /* Should not happen, already checked during initialization. */
            throw new IllegalArgumentException(
                "Bad checksum algorithm: " + checksumAlg, e);
        }

        final File archiveFile = getFileFromURL(archiveURL);
        try (final FileOutputStream outputStream =
                new FileOutputStream(archiveFile, false)) {
            final byte[] buffer = new byte[TRANSFER_BYTES];
            while (true) {
                int readBytes = localFileStream.read(buffer);
                if (readBytes == -1) {
                    break;
                }
                outputStream.write(buffer, 0, readBytes);
                messageDigest.update(buffer, 0, readBytes);
            }
        }

        /* Ensure that the copy is persistent in the file system. */
        /* Persist the file itself. */
        BackupManager.forceFile(archiveFile.toPath());
        /* Persist its directory entry. */
        BackupManager.forceFile(archiveFile.getParentFile().toPath());

        return messageDigest.digest();
    }

    private File getFileFromURL(URL archiveURL) throws IOException {
        if (!"file".equalsIgnoreCase(archiveURL.getProtocol())) {
            throw new IllegalArgumentException("URL scheme must be file; not" +
                archiveURL.getProtocol());
        }
        final File archiveFile = new File(archiveURL.getFile());
        final File archiveParent = archiveFile.getParentFile();
        if (!archiveParent.exists()) {
            final boolean created = archiveParent.mkdirs();
            if (!created) {
                throw new IOException("Could not create parent directories " +
                                      " for " + archiveFile.getAbsolutePath());
            }
        }
        return archiveFile;
    }

    @Override
    public byte[] checksum(URL archiveURL)
        throws InterruptedException, IOException {

        requireInitialized();
        Objects.requireNonNull(archiveURL, "archiveFile arg must not be null");
        if (!"file".equalsIgnoreCase(archiveURL.getProtocol())) {
            throw new IllegalArgumentException("URL scheme must be file; not" +
                                               archiveURL.getProtocol());
        }

        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(checksumAlg);
        } catch (NoSuchAlgorithmException e) {
            /* Should not happen, already checked during initialization. */
            throw new IllegalArgumentException(
                "Bad checksum algorithm: " + checksumAlg, e);
        }
        final File archiveFile = new File(archiveURL.getFile());
        long length = archiveFile.length();

        try (final FileInputStream inputStream =
            new FileInputStream(archiveFile)) {

            final ByteBuffer buffer = ByteBuffer.allocate(TRANSFER_BYTES);
            for (long bytes = length; bytes > 0;) {
                int readSize = (int)Math.min(TRANSFER_BYTES, bytes);
                int readBytes = inputStream.read(buffer.array(), 0, readSize);
                if (readBytes == -1) {
                    throw new IOException("Premature EOF. Was expecting: " +
                                          readSize);
                }
                messageDigest.update(buffer.array(), 0, readBytes);
                bytes -= readBytes;
            }
        }
        return messageDigest.digest();
    }

    private void requireInitialized() {
        if (initProperties == null) {
            throw new IllegalStateException(this.getClass().getName() +
                                            " is not initialized);");
        }
    }
}
