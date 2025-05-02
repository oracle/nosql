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
import java.io.IOException;
import java.net.URL;

/**
 * @hidden For internal use: disaster recovery from scheduled backups
 *
 * Interface used by Recovery Procedure to copy and verify files from
 * an archive. The archive storage may be on a different physical disk
 * on the same machine, or it could be implemented by some remote object
 * storage service accessed over the network. The interface is intended to
 * abstract out these storage details.
 *
 * <p>Local storage usually has the notion of a directory and
 * initialization step in recovery procedure makes sure that directory
 * structure is already in place before starting copy out process in
 * recovery procedure.
 *
 * <p>The class implementing this interface is also expected to have a public
 * noargs constructor to facilitate dynamic allocation and initialization of
 * the implementing object.
 *
 * <p>All implementation methods must be re-entrant to permit multiple
 * concurrent copy operations from archive files.
 *
 * <p>The methods must handle interrupts by doing minimal non-blocking cleanup,
 * before propagating the interrupt, to ensure that it is prompt and is not
 * itself prone to hanging. The impact of an interrupt should be localized to
 * the method, that is, other concurrent copy operations from archive should
 * not be impacted and it should be possible to use the object to perform other
 * operations. Methods may be interrupted if the copy from archive needs to be
 * abandoned for some reason.
 *
 * <p>The implementation methods are responsible for resolving any recoverable
 * failures internally by retrying the copy operation from archive in a manner
 * that is appropriate to the underlying storage used by the archive. The
 * methods can retry indefinitely, using appropriate backoff strategies. If a
 * copy from archive operation is taking too long, it will be interrupted after
 * a certain time-limit threshold. This will lead to partial recoverable set of
 * jdb files which will not suffice recovery operation. Manual interruption
 * could be needed in such cases from devops/administrators.
 *
 * <p>The implementation requires read access to the archive and write access
 * into the local fileSystem. The authn/z mechanism is specific to the archive
 * and local fileSystem storage. It's the implementation's responsibility to
 * ensure that it has the appropriate credentials to permit reading from the
 * archive and writing into the local fileSystem.
 *
 * <p>Unexpected exceptions thrown when recovery process calls methods on an
 * implementation of this interface will cause the recovery facility to be
 * shutdown. {@link InterruptedException} and {@link IOException} are expected
 * exceptions, while {@link IllegalArgumentException}, other runtime exceptions,
 * and errors are treated as unexpected.
 */
public interface RecoverFileCopy {

    /**
     * Initialize is invoked exactly once after the creation of the object via
     * its noargs constructor.
     *
     * @param configFile the config file used to initialize the object. It
     * could, for example, contain the credentials needed to access the
     * archive, the compression and encryption algorithms used to read and
     * write archive files, etc. The format of the config file
     * (properties, json, etc.) is private to the implementation.
     *
     * @throws InterruptedException if the initialize operation is interrupted
     * @throws IllegalArgumentException if the configuration was invalid
     * @throws IOException if some irrecoverable I/O issue was encountered
     * during initialization.
     */
    void initialize(File configFile) throws InterruptedException, IOException;

    /**
     * Returns the encryption algorithm used to decrypt the archive
     * file being accessed for copy into local file system. The encryption
     * algorithm is set via the config file during the call to
     * {@link #initialize}.
     * 
     * <p>The algorithm name is a valid argument to
     * {@link javax.crypto.Cipher#getInstance(String)} or the
     * provider overloading used by the implementation.
     *
     * @return null (if no encryption is in use), or the name of the encryption
     * algorithm at archive storage.
     */
    String getArchiveEncryptionAlg();

    /**
     * Returns the encryption algorithm used to encrypt the archive file
     * while copying into local file system. The encryption algorithm is set
     * via the config file during the call to {@link #initialize}.
     * 
     * <p>The algorithm name is a valid argument to
     * {@link javax.crypto.Cipher#getInstance(String)} or the
     * provider overloading used by the implementation.
     *
     * @return null (if no encryption is in use), or the name of the encryption
     * algorithm at local fileSystem.
     */
    String getLocalEncryptionAlg();

    /**
     * Returns the compression algorithm used to decompress the archive file
     * being accessed for copy into local fileSystem. This will be used for
     * decompressing the archive file during copy out. The compression
     * algorithm is set via the config file during the call to
     * {@link #initialize}.
     *
     * @return null (if no compression is in use), or the name of the
     * compression algorithm at archive storage.
     * The value (for now) could be one of zip or gzip.
     */
    String getArchiveCompressionAlg();

    /**
     * Returns the compression algorithm used to compress the archive file
     * while copying into local fileSystem. The compression algorithm is set
     * via the config file during the call to {@link #initialize}.
     *
     * @return null (if no compression is in use), or the name of the
     * local file system compression algorithm.
     * The value (for now) could be one of zip or gzip.
     */
    String getLocalCompressionAlg();

    /**
     * Returns the checksum algorithm used to checksum the archive file before
     * copy into the local fileSystem and after copy into the local fileSystem,
     * or "NONE" if no checksum should be computed. The checksum algorithm is
     * set via the config file during the call to {@link #initialize}. If not
     * "NONE", the algorithm name is a valid argument to {@link
     * java.security.MessageDigest#getInstance(String)} or the provider
     * overloading used by the implementation. "NONE" is used for copying
     * manifest files, where the checksum is not used and so should not be
     * computed.
     *
     * @return the name of the checksum algorithm or "NONE" if no checksum
     * should be computed
     */
    String getChecksumAlg();

    /**
     * Copies the file from the archive into the local fileSystem. The file is
     * encrypted and compressed in the archive according to the configuration
     * specified by {@link #initialize}. These configurations will be used
     * for decrypting and decompressing archive file into local fileSystem.
     *
     * <p>The recovery process assumes that the data associated with {@code
     * archiveURL} will not be modified during recovery. The checksum returned
     * for archive file should match the checksum of the {@code localfile}
     * after copying, unless no checksum is being computed.
     *
     * <p>If localFile already exists at {@code localFile}, say because of an
     * earlier aborted attempt, the file is overwritten by this operation.
     *
     * <p>An IOException may leave the localFile in an inconsistent state. For
     * example, the archive storage may become unavailable, fail, etc. The
     * caller is expected to be handle such failures.
     *
     * @param archiveURL the source path of the file to be copied from archive;
     * it must be an absolute URL.
     * @param localFile the target path for the copy in local file system,
     * it must be an absolute path.
     * @return the checksum of the contents of the archive file as they were
     * before copy, or null if no checksum was computed.
     *
     * @throws InterruptedException if the copy is interrupted
     * @throws IOException if some irrecoverable I/O issue was encountered
     * either while reading or writing the file.
     */
    byte[] copy(URL archiveURL,
                File localFile) throws InterruptedException, IOException;

    /**
     * Gets the list of archive files at the specified URL and writes them to
     * the local file. During scheduled backup, we pass configuration
     * parameter to BackupFileLocation.baseDirectory. For recovery procedure,
     * archiveURL is same as base directory parameter passed during scheduled
     * backups. If we want only list of manifest files in archives, then
     * isOnlyManifest is set to true else false.
     *
     * @param archiveURL the URL of the archive where backup files are stored,
     * e.g. /andc/backups; it must be an absolute URL.
     * @param localFile the pathname of the file where the list archive files
     * should be written; must be an absolute file path.
     * @param isOnlyManifest if true, get only list of manifest files in
     * archive, else list of all files in archive.
     *
     * @throws InterruptedException if the getFileList operation is
     * interrupted
     * @throws IOException if some irrecoverable I/O issue was encountered
     * while getting the file list
     */
    void getFileList(URL archiveURL,
                     File localFile,
                     boolean isOnlyManifest)
        throws InterruptedException, IOException;

    /**
     * Returns the checksum computed by reading local file after copy, or null
     * if no checksum was computed. The file is assumed to be encrypted and
     * compressed in the local fileSystems according to the configuration
     * specified by {@link #initialize}.
     *
     * @param localFile the file being read to compute the checksum
     * value after copy; it must be an absolute file path.
     * @return the checksum value of the contents of the local file after copy,
     * or null if no checksum was computed.
     *
     * @throws InterruptedException if the checksum operation is interrupted
     * @throws IOException if some I/O issue was encountered while reading the
     * localFile
     */
    byte[] checksum(File localFile) throws InterruptedException, IOException;
}
