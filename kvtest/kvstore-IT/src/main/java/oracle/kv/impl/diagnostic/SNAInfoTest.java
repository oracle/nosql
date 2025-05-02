/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import oracle.kv.TestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests SNAInfo.
 */
public class SNAInfoTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testBasic() throws Exception {
        /* Test constructor SNAInfo(String, String, String, String) */
        SNAInfo snaInfo = new SNAInfo("mystore", "sn1", "localhost",
                                      "~/kvroot");
        assertEquals(snaInfo.getStoreName(), "mystore");
        assertEquals(snaInfo.getStorageNodeName(), "sn1");
        assertEquals(snaInfo.getHost(), "localhost");
        assertNull(snaInfo.getSSHUser());
        assertEquals(snaInfo.getRootdir(), "~/kvroot");
        snaInfo.setSSHUser("myuser");
        assertEquals(snaInfo.getSSHUser(), "myuser");

        /* Test constructor SNAInfo(String, String, String, String, String) */
        snaInfo = new SNAInfo("mystore", "sn1", "localhost", "username",
                              "~/kvroot");
        assertEquals(snaInfo.getStoreName(), "mystore");
        assertEquals(snaInfo.getStorageNodeName(), "sn1");
        assertEquals(snaInfo.getHost(), "localhost");
        assertEquals(snaInfo.getSSHUser(), "username");
        assertEquals(snaInfo.getRootdir(), "~/kvroot");

        /* Test constructor SNAInfo(String) */
        snaInfo = new SNAInfo("mystore|sn1|username@localhost|~/kvroot");
        assertEquals(snaInfo.getStoreName(), "mystore");
        assertEquals(snaInfo.getStorageNodeName(), "sn1");
        assertEquals(snaInfo.getHost(), "localhost");
        assertEquals(snaInfo.getSSHUser(), "username");
        assertEquals(snaInfo.getRootdir(), "~/kvroot");

        /* Test constructor SNAInfo(String) without specified user name */
        snaInfo = new SNAInfo("mystore|sn1|localhost|~/kvroot");
        assertEquals(snaInfo.getStoreName(), "mystore");
        assertEquals(snaInfo.getStorageNodeName(), "sn1");
        assertEquals(snaInfo.getHost(), "localhost");
        assertNull(snaInfo.getSSHUser());
        assertEquals(snaInfo.getRootdir(), "~/kvroot");

        /* Test equals and hashCode methods */
        SNAInfo snaInfo1 = new SNAInfo("mystore", "sn1", "localhost",
                                       "~/kvroot");
        SNAInfo snaInfo2 = new SNAInfo("mystore", "sn1", "127.0.0.1", "user",
                "~/kvroot");

        assertTrue(snaInfo1.equals(snaInfo2));
        assertEquals(snaInfo1.hashCode(), snaInfo2.hashCode());
        assertEquals(snaInfo1.getIP(), snaInfo2.getIP());

        String localIP = snaInfo1.getIP().getHostAddress();
        SNAInfo snaInfo3 = new SNAInfo("mystore", "sn1", localIP, "user",
                "~/kvroot");

        assertTrue(snaInfo1.equals(snaInfo3));
        assertEquals(snaInfo1.hashCode(), snaInfo3.hashCode());
        assertEquals(snaInfo1.getIP(), snaInfo3.getIP());

        /* Test toString and getSNAInfo methods */
        assertEquals(snaInfo1.getSNAInfo(), "Store: mystore, SN: sn1, Host: " +
                        "localhost");
        assertEquals(snaInfo1.toString(), "mystore|sn1|localhost|~/kvroot");
        assertEquals(snaInfo2.toString(), "mystore|sn1|user@127.0.0.1|" +
                        "~/kvroot");
    }
}
