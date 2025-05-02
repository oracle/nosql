/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.recovery;

import com.sleepycat.je.RecoverFileCopy;
import java.io.File;
import java.io.IOException;
import java.net.URL;

/*
 *  A mock implementation of RecoverObjectStorageCopy used to test 
 *  error handling in SN Recover.
 */
public class FailingObjectStorageCopy implements RecoverFileCopy {

    public static String TEST_ERROR_MSG = "Test error from mock object storage";

    @Override
    public synchronized void initialize(File configFile)
        throws InterruptedException, IOException {

        throw new IllegalArgumentException("Throwing IllegalArgumentException:"
                                           + TEST_ERROR_MSG);
    }

    @Override
    public String getArchiveEncryptionAlg() {
        return null;
    }

    @Override
    public String getLocalEncryptionAlg() {
        return null;
    }

    @Override
    public String getArchiveCompressionAlg() {
        return null;
    }

    @Override
    public String getLocalCompressionAlg() {
        return null;
    }

    @Override
    public String getChecksumAlg() {
        return null;
    }

    @Override
    public byte[] copy(URL archiveURL, File localFile)
        throws IOException {
        return null ;
    }

    @Override
    public void getFileList(URL baseArchiveURL, File localFile,
                            boolean isOnlyManifest) throws IOException 
    {
        return;   
    }

    @Override
    public byte[] checksum(File localFile)
        throws InterruptedException, IOException {
        return null;
    }
}
