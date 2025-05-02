/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import oracle.kv.TestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests RemoteFile.
 */
public class RemoteFileTest extends TestBase {

    @Test
    public void testBasic() {
        File file1 = new File("test.file");
        File file2 = new File("test.file");
        SNAInfo si1 = new SNAInfo("mystore", "sn1", "localhost", "~/kvroot");
        SNAInfo si2 = new SNAInfo("mystore", "sn1", "localhost", "~/kvroot");
        SNAInfo si3 = new SNAInfo("mystore", "sn1", "localhost", "~/kvroot2");
        RemoteFile rFile = new RemoteFile(file1, si1);

        /* Test getter */
        assertEquals(file2, rFile.getLocalFile());
        assertEquals(si2, rFile.getSNAInfo());

        /* Test equal */
        RemoteFile rFile2 = new RemoteFile(file2, si2);
        assertEquals(rFile, rFile2);

        /* Test unequal */
        RemoteFile rFile3 = new RemoteFile(file2, si3);
        assertFalse(rFile == rFile3);

        /* Compared with another type object */
        Object obj = new Object();
        assertFalse(rFile == obj);

        /* Test hashCode */
        assertEquals(rFile.hashCode(), rFile2.hashCode());

        /* Test toString */
        String localMachinename = "";
        try {
            localMachinename = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
        }
        assertEquals(rFile.toString(), localMachinename + "::test.file");
    }
}
