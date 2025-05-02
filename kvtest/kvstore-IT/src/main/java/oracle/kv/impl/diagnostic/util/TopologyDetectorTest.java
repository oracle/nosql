/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.util;


import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileWriter;

import oracle.kv.TestBase;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests SNAInfo.
 */
public class TopologyDetectorTest extends TestBase {

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
    public void testAdminLogonNonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);
        Assume.assumeFalse(
            "testAdminLogonSecured already covered the secure admin login " +
            "cases", SECURITY_ENABLE);

        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testAdminLogonNonSecured",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1); /* secure */
        createStore.initStorageNodes();
        createStore.start();

        TopologyDetector td = new TopologyDetector(
            "localhost", createStore.getRegistryPort(), null, null);
        Topology topo = td.getTopology(logger);
        assertNotNull(topo);
        StorageNode storageNode = topo.getSortedStorageNodes().get(0);
        assertNotNull(td.getRootDirPath(storageNode));
    }

    @Test
    public void testRepNodeLogonNonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);
        Assume.assumeFalse(
            "testRepNodeLogonNonSecured already covered the secure " +
            "repnode login cases", SECURITY_ENABLE);
        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testRepNodeLogonNonSecured",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1); /* secure */
        createStore.initStorageNodes();
        createStore.start();

        TopologyDetector td = new TopologyDetector(
            "localhost", createStore.getRegistryPort(), null, null, true);
        Topology topo = td.getTopology(logger);
        assertNotNull(topo);
        StorageNode storageNode = topo.getSortedStorageNodes().get(0);
        assertNotNull(td.getRootDirPath(storageNode));
    }

    @Test
    public void testAdminLogonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testAdminLogonSecured",
                                       5000,
                                       1, /* Storage nodes */
                                       1, /* Replication factor */
                                       3, /* Partitions */
                                       1,
                                       CreateStore.MB_PER_SN,
                                       false, /* useThreads */
                                       null, /* mgmtImpl */
                                       true, /* mgmtPortsShared */
                                       true); /* secure */
        SecureUser user = new SecureUser("root", "NoSql00__123456", true);
        createStore.addUser(user);
        createStore.initStorageNodes();
        createStore.start();

        FileWriter fw = new FileWriter(loginFile, true/*append*/);
        fw.write("oracle.kv.ssl.trustStore=" +
                        createStore.getTrustStore().getAbsolutePath());
        fw.close();

        TopologyDetector td = new TopologyDetector(
            "localhost", createStore.getRegistryPort(), "root",
            loginFile.getAbsolutePath());
        Topology topo = td.getTopology(logger);
        assertNotNull(topo);
        StorageNode storageNode = topo.getSortedStorageNodes().get(0);
        assertNotNull(td.getRootDirPath(storageNode));
    }

    @Test
    public void testRepNodeLogonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testRepNodeLogonSecured",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1,
                                      CreateStore.MB_PER_SN,
                                      false, /* useThreads */
                                      null, /* mgmtImpl */
                                      true, /* mgmtPortsShared */
                                      true); /* secure */
       SecureUser user = new SecureUser("root", "NoSql00__123456", true);
       createStore.addUser(user);
       createStore.initStorageNodes();
       createStore.start();

       FileWriter fw = new FileWriter(loginFile, true/*append*/);
       fw.write("oracle.kv.ssl.trustStore=" +
                       createStore.getTrustStore().getAbsolutePath());
       fw.close();

       TopologyDetector td = new TopologyDetector(
           "localhost", createStore.getRegistryPort(), "root",
           loginFile.getAbsolutePath(), true);
       Topology topo = td.getTopology(logger);
       assertNotNull(topo);
       StorageNode storageNode = topo.getSortedStorageNodes().get(0);
       assertNotNull(td.getRootDirPath(storageNode));
    }
}
