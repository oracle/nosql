/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.ssh;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.diagnostic.SNAInfo;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests SSHClientManager.
 */
public class SSHClientManagerTest extends ClientTestBase {

    @Test
    public void testBasic() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        String store = "mystore";
        String sn = "sn1";
        String user = System.getProperty("user.name");
        String host = "localhost";
        SNAInfo snaInfo1 = new SNAInfo(store, sn, host, user, "~/kvroot1");

        SSHClient client1 = SSHClientManager.getClient(snaInfo1);

        SNAInfo snaInfo2 = new SNAInfo(store, sn, host, user, "~/kvroot2");
        SSHClient client2 = SSHClientManager.getClient(snaInfo2);

        /*
         * The target machines are same, so the references of client are
         * identical
         */
        assertTrue(client1 == client2);

        /* Clear all existing clients in cache */
        SSHClientManager.clearClients();

        SNAInfo snaInfo3 = new SNAInfo(store, sn, host, user, "~/kvroot3");
        SSHClient client3 = SSHClientManager.getClient(snaInfo3);

        /* After cache is cleared, a new client client3 is created */
        assertFalse(client1 == client3);
    }

    @Test
    public void testGetClientList() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        String store = "mystore";
        String user = System.getProperty("user.name");
        String host = "localhost";

        SNAInfo snaInfo1 = new SNAInfo(store, "sn1", host, user, "~/kvroot1");
        SNAInfo snaInfo2 = new SNAInfo(store, "sn2", host, user, "~/kvroot2");
        SNAInfo snaInfo3 = new SNAInfo(store, "sn3", host, user, "~/kvroot3");

        List<SNAInfo> snaInfoList = new ArrayList<SNAInfo>();
        snaInfoList.add(snaInfo1);
        snaInfoList.add(snaInfo2);
        snaInfoList.add(snaInfo3);
        Map<SNAInfo, SSHClient> map = SSHClientManager.getClient(snaInfoList);

        assertEquals(map.size(), 3);
    }
}
