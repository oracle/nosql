/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.Assume;
import org.junit.Test;

/**
 * Test AdminLocator.
 */
public class AdminLocatorTest extends TestBase {
    private CreateStore createStore = null;
    private final File loginFile = new File("login_file");
    private final File pwdFile = new File("pwd_file");
    private boolean isWindows;

    @Override
    public void setUp()
        throws Exception {
        super.setUp();
        isWindows = false;
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("win") >= 0) {
            isWindows = true;
        }

        if (loginFile.exists()) {
            loginFile.delete();
        }

        if (pwdFile.exists()) {
            pwdFile.delete();
        }

        FileWriter fw = new FileWriter(pwdFile);
        fw.write("Password Store:\n");
        fw.write("secret.root=NoSql00__123456");
        fw.close();

        fw = new FileWriter(loginFile);
        fw.write("oracle.kv.auth.username=root\n");
        fw.write("oracle.kv.auth.pwdfile.file=" + pwdFile.getAbsolutePath() +
                 "\n");
        fw.write("oracle.kv.transport=ssl\n");
        fw.close();
    }

    @Override
    public void tearDown()
        throws Exception {
        if (createStore != null) {
            createStore.shutdown(true);
        }

        if (loginFile.exists()) {
            loginFile.delete();
        }

        if (pwdFile.exists()) {
            pwdFile.delete();
        }
        super.tearDown();
    }

    @Test
    public void testNonSecured() throws Exception {

        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testNonSecured",
                                      5000,
                                      3, /* Storage nodes */
                                      3, /* Replication factor */
                                      10, /* Partitions */
                                      1); /* capacity */
        createStore.initStorageNodes();
        createStore.start();

        Map<StorageNode, CommandServiceAPI> adminMap =
            AdminLocator.get("localhost", 5000, logger);
        assertResult(adminMap);
    }


    @Test
    public void testSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testSecured",
                                       5000,
                                       3,  /* Storage nodes */
                                       3,  /* Replication factor */
                                       30, /* Partitions */
                                       1,  /* Capacity */
                                       CreateStore.MB_PER_SN,
                                       false, /* useThreads */
                                       null, /* mgmtImpl */
                                       true, /* mgmtPortsShared */
                                       true); /* secure */
        SecureUser user = new SecureUser("root", "NoSql00__123456", true);
        createStore.addUser(user);
        createStore.initStorageNodes();
        createStore.start();

        LoginManager loginMgr = createStore.getRepNodeLoginManager();

        Map<StorageNode, CommandServiceAPI> adminMap =
            AdminLocator.get(new String[] {"localhost:5000"}, loginMgr, null,
                             logger);

        assertResult(adminMap);
    }

    private void assertResult(Map<StorageNode, CommandServiceAPI> adminMap) {
        /* Check the number of admins */
        assertEquals(adminMap.size(), 3);

        boolean hasAdminMaster = false;
        List<String> list = new ArrayList<>();
        list.add("sn1");
        list.add("sn2");
        list.add("sn3");

        for (Map.Entry<StorageNode, CommandServiceAPI> entry :
                                                        adminMap.entrySet()) {

            list.remove(entry.getKey().getStorageNodeId().toString());
            try {
                if (entry.getValue().getAdminStatus().
                                            getIsAuthoritativeMaster()) {
                    hasAdminMaster = true;
                }
            } catch (RemoteException ex) {
            }
        }

        /* Check the storage node admins hosting admins */
        assertTrue(list.isEmpty());

        /* Check admin master */
        assertTrue(hasAdminMaster);


    }
}
