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
import java.io.InputStream;
import java.net.URL;

import com.sleepycat.je.util.LogVerificationException;
import com.sleepycat.je.util.LogVerificationInputStream;

/**
 * @hidden For internal use: automatic backups
 *
 * Interface used by Backup to copy and verify files in an archive. The archive
 * storage may be on a different physical disk on the same machine, or it could
 * be implemented by some remote object storage service accessed over the
 * network. The interface is intended to abstract out these storage details.
 *
 * <p>The class implementing this interface is also expected to have a public
 * noargs constructor to facilitate dynamic allocation and initialization of
 * the implementing object.
 *
 * <p>All implementation methods must be re-entrant to permit multiple
 * concurrent operations on archive files.
 *
 * <p>The methods must handle interrupts by doing minimal non-blocking cleanup,
 * before propagating the interrupt, to ensure that it is prompt and is not
 * itself prone to hanging. The impact of an interrupt should be localized to
 * the method, that is, other concurrent operations should not be impacted and
 * it should be possible to use the object to perform other operations. Methods
 * may be interrupted if the environment needs to be shut down in a timely
 * manner, or if the copy needs to be abandoned for some reason.
 *
 * <p>The implementation methods are responsible for resolving any recoverable
 * failures internally by retrying the operation in a manner that is
 * appropriate to the underlying storage used by the archive. The methods can
 * retry indefinitely, using appropriate backoff strategies. If an operation is
 * taking too long, it will be interrupted by the backup process.
 *
 * <p>The implementation requires write access to the archive. The authn/z
 * mechanism is specific to the archive storage. It's the implementation's
 * responsibility to ensure that it has the appropriate credentials to permit
 * writing to and reading from the archive.
 *
 * <p>Unexpected exceptions thrown when backup calls methods on an
 * implementation of this interface will cause the backup facility to be
 * shutdown, but will leave the environment open. {@link InterruptedException}
 * and {@link IOException} are expected exceptions, while {@link
 * IllegalArgumentException}, other runtime exceptions, and errors are treated
 * as unexpected.
 */
public interface BackupFileCopy {

    /**
     * initialize is invoked exactly once after the creation of the object via
     * its noargs constructor.
     *
     * @param configFile the config file used to initialize the object. It
     * could, for example, contain the credentials needed to access the
     * archive,the compression and encryption algorithms used to store archive
     * files, etc. The format of the config file (properties, json, etc.) is
     * private to the implementation.
     *
     * @throws InterruptedException if the initialize operation is interrupted
     * @throws IllegalArgumentException if the configuration was invalid
     * @throws IOException if some irrecoverable I/O issue was encountered
     * during initialization.
     */
    void initialize(File configFile) throws InterruptedException, IOException;

    /**
     * Returns the encryption algorithm used to encrypt/decrypt the archive
     * file being accessed. The encryption algorithm is set via the config file
     * during the call to {@link #initialize}. The algorithm name is a valid
     * argument to {@link javax.crypto.Cipher#getInstance(String)} or the
     * provider overloading used by the implementation.
     *
     * @return null (if no encryption is in use), or the name of the encryption
     * algorithm.
     */
    String getEncryptionAlg();

    /**
     * Returns the compression algorithm used to compress the archive file
     * being accessed. The compression algorithm is set via the config file
     * during the call to {@link #initialize}.
     *
     * @return null (if no compression is in use), or the name of the
     * compression algorithm. The value (for now) could be one of zip or gzip.
     */
    String getCompressionAlg();

    /**
     * Returns the checksum algorithm used to checksum the archive file
     * being accessed. The checksum algorithm is set via the config file
     * during the call to {@link #initialize}. The algorithm name is a valid
     * argument to {@link java.security.MessageDigest#getInstance(String)} or
     * the provider overloading used by the implementation.
     *
     * @return The name of the checksum algorithm
     */
    String getChecksumAlg();

    /**
     * Copies input from a file on the local machine into the archive. The
     * archive file is encrypted and compressed in the archive according to the
     * configuration specified by {@link #initialize}.
     *
     * <p>Note that the file associated with {@code localFileStream} could be
     * modified concurrently (for table row erasure) while the copy operation
     * is in progress, so that the version of file that is copied is partially
     * erased. Erasure modifications do not change the length of the file, just
     * its existing contents. The restored environment is resilient to such a
     * partially erased state. The checksum returned for such a concurrently
     * erased file thus reflects the contents of the file in the archive and
     * will not match the state of the local file.
     *
     * <p>If archiveFile already exists at {@code archiveURL}, say because of
     * an earlier aborted attempt, the file is overwritten by the copy
     * operation. The overwrite need not be atomic. The implementation can
     * delete the partial archiveFile before copying it anew.
     *
     * <p>An IOException may leave the archiveFile in an inconsistent state.
     * For example, the archive storage may become unavailable, fail, etc. The
     * caller is expected to be handle such failures and not trust the archive
     * file contents (if the file exists at all) following an IOException.
     *
     * <p>If archive storage has the notion of a container, like a directory,
     * or a storage bucket, the implementation may create the container before
     * creating the file, or arrange for the container to be in place before
     * backup is enabled.
     *
     * <p>The {@code localFileStream} argument may be a {@link
     * LogVerificationInputStream} in order to verify log file input. In that
     * case, I/O operations on {@code localFileStream} may generate {@link
     * LogVerificationException} or {@link EnvironmentFailureException}.
     * Implementations should arrange for those exceptions to be thrown to the
     * caller.
     *
     * @param localFileStream a stream of the file contents to be copied
     * @param archiveURL the target path for the copy; it must be an absolute
     * URL
     * @return the checksum of the contents of localFileStream. The checksum is
     * calculated using the checksum algorithm returned by {@link
     * #getChecksumAlg}.
     *
     * @throws InterruptedException if the copy is interrupted
     * @throws IOException if some irrecoverable I/O issue was encountered
     * either while reading the stream or writing the file
     * @throws LogVerificationException if the input stream performs log
     * verification of {@code localFileStream} and determines that the log file
     * contents are invalid
     * @throws EnvironmentFailureException if log verification encounters an
     * unexpected failure
     */
    byte[] copy(InputStream localFileStream, URL archiveURL)
        throws InterruptedException, IOException;

    /**
     * Returns the checksum computed by reading archiveFile. The file is
     * assumed to be encrypted and compressed in the archive according to the
     * configuration specified by {@link #initialize}
     *
     * @param archiveURL the archive file being read to compute the checksum
     * value; it must be an absolute URL
     * @return the checksum value. The checksum is calculated using the
     * checksum algorithm returned by {@link #getChecksumAlg}.
     *
     * @throws InterruptedException if the checksum operation is interrupted
     * @throws IOException if some irrecoverable I/O issue was encountered
     * while reading the archiveFile
     */
    byte[] checksum(URL archiveURL) throws InterruptedException, IOException;
}
