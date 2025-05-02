/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.param.IntParameter;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Tests setting and changing the dns cache ttl parameter.
 */
public class DnsCacheTTLTest extends TestBase {

    private static final String HOST = "localhost";
    private static final int START_PORT = 6000;
    private static final int HA_RANGE = 10;
    private static final int CACHE_TTL_VALUE = 42;
    private static final int ANOTHER_CACHE_TTL_VALUE = 4242;
    private static final Set<String> EMPTY_SET = new HashSet<>();
    private static final Set<String> SN_SET = new HashSet<>();
    private static final Set<String> SNS_AND_BOOTSTRAPADMIN = new HashSet<>();
    private static final Set<Parameter> CACHE_PARAMETER_SET = new HashSet<>();
    static {
        SN_SET.add("sn1"); SN_SET.add("sn2");
        SN_SET.add("sn3"); SN_SET.add("sn4");
    }
    static {
        SNS_AND_BOOTSTRAPADMIN.add("sn1"); SNS_AND_BOOTSTRAPADMIN.add("sn2");
        SNS_AND_BOOTSTRAPADMIN.add("sn3"); SNS_AND_BOOTSTRAPADMIN.add("sn4");
        SNS_AND_BOOTSTRAPADMIN.add("admin on sn1");
    }
    static {
        CACHE_PARAMETER_SET.add(
            new IntParameter(ParameterState.COMMON_DNS_CACHE_TTL,
                             CACHE_TTL_VALUE));
        CACHE_PARAMETER_SET.add(
            new IntParameter(ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL,
                             CACHE_TTL_VALUE));
    }

    /**
     * Tests setting dns cache ttl with makebootconfig.
     */
    @Test
    public void testMakeBootConfigCache() throws Exception {
        testMakeBootConfig(Integer.toString(CACHE_TTL_VALUE));
    }

    /**
     * Tests setting dns cache and cache negative ttl with makebootconfig.
     */
    @Test
    public void testMakeBootConfigBoth() throws Exception {
        testMakeBootConfig(
            String.format("%s,%s", CACHE_TTL_VALUE, ANOTHER_CACHE_TTL_VALUE));
    }

    private void testMakeBootConfig(String dnsCacheTTLs) throws Exception {
        final File testDir = TestUtils.getTestDir();
        final PortFinder portFinder =
            new PortFinder(START_PORT, HA_RANGE);
        final StorageNodeAgentImpl snai = new StorageNodeAgentImpl(true);
        try {
            final File rootDir =
                new File(testDir.getAbsolutePath() +
                         File.separator + "kvroot");
            rootDir.mkdirs();
            makeBootConfig(rootDir, portFinder, 1, dnsCacheTTLs);
            snai.parseArgs(
                new String[] { "-root",  rootDir.getAbsolutePath() });
            snai.start();
            final int port = snai.getRegistryPort();
            deployCluster(new int[] { port }, 1);
            String[] ttls = dnsCacheTTLs.split(",");
            assertDnsCacheTTL(port, ttls[0], EMPTY_SET);
            if (ttls.length > 1) {
                assertDnsCacheNegativeTTL(port, ttls[1], EMPTY_SET);
            } else {
                assertDnsCacheNegativeTTL(
                    port,
                    ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT,
                    EMPTY_SET);
            }
        } finally {
            try {
                snai.getStorageNodeAgent().shutdown(true, true);
            } catch (Throwable t) {
                /* ignore */
            }
        }
    }

    private void makeBootConfig(File rootDir,
                                PortFinder portFinder,
                                int capacity,
                                String dnsCacheTTLs) throws Exception {
        final String[] args ={"makebootconfig",
            "-root", rootDir.getAbsolutePath(),
            "-host", HOST,
            "-port", Integer.toString(portFinder.getRegistryPort()),
            "-harange", portFinder.getHaRange(),
            "-capacity", Integer.toString(capacity),
            "-dns-cachettl", dnsCacheTTLs,
            "-store-security", "none"};
        KVStoreMain.main(args);
    }

    private void deployCluster(int[] ports, int rf) throws Exception {
        final int adminPort = ports[0];
        runCommand(adminPort,
                   String.format("configure -name %s", kvstoreName),
                   "Store configured");
        runCommand(adminPort,
                   String.format(
                       "plan deploy-zone -name zone -arbiters -rf %s -wait",
                       rf));
        for (int port : ports) {
            runCommand(adminPort,
                       String.format("plan deploy-sn -znname zone " +
                                     "-port %s -wait -host localhost",
                                     port));
        }
        runCommand(adminPort, "plan deploy-admin -sn sn1 -wait");
        runCommand(adminPort, "pool create -name pool", "Added pool");
        for (int i = 0; i < ports.length; ++i) {
            runCommand(adminPort,
                       String.format("pool join -name pool -sn sn%s", i + 1),
                       "Added Storage Node");
        }
        runCommand(adminPort,
                   "topology create -name cluster -pool pool -partitions 10",
                   "Created");
        runCommand(adminPort, "plan deploy-topology -name cluster -wait");
    }

    private void runCommand(int port, String cmd)
        throws Exception {
        runCommand(port, cmd, "successfully");
    }

    private void runCommand(int port, String cmd, String expected)
        throws Exception {
        final String command =
            String.format("-host %s -port %s %s", HOST, port, cmd);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream cmdOut = new PrintStream(outStream);
        /* Run the command. */
        CommandShell shell = new CommandShell(System.in, cmdOut);
        shell.parseArgs(command.split(" "));
        shell.start();
        assertTrue(String.format("Command failed: %s", outStream.toString()),
                   outStream.toString().contains(expected));
    }

    private void assertDnsCacheTTL(int port, int ttl, Set<String> skipSet)
        throws Exception {
        assertDnsCacheTTL(port, Integer.toString(ttl), skipSet);
    }

    private void assertDnsCacheTTL(int port, String ttl, Set<String> skipSet)
        throws Exception {
        assertSecurityProperty(
            port, ParameterState.INETADDR_CACHE_POLICY_PROP, ttl, skipSet);
    }

    private void assertDnsCacheNegativeTTL(int port,
                                           int ttl,
                                           Set<String> skipSet)
        throws Exception {
        assertDnsCacheNegativeTTL(port, Integer.toString(ttl), skipSet);
    }

    private void assertDnsCacheNegativeTTL(int port,
                                           String ttl,
                                           Set<String> skipSet)
        throws Exception {
        assertSecurityProperty(
            port, ParameterState.INETADDR_CACHE_NEGATIVE_POLICY_PROP,
            ttl, skipSet);
    }

    private void assertSecurityProperty(int port,
                                        String name,
                                        String expected,
                                        Set<String> skipSet)
        throws Exception {
        final CommandServiceAPI adminAPI =
            RegistryUtils.getAdmin(HOST, port, null, logger);
        final Topology topo = adminAPI.getTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, null, logger);
        final Map<String, RemoteTestAPI> testAPIs = new HashMap<>();
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            testAPIs.put(snId.toString(),
                         registryUtils.getStorageNodeAgentTest(snId));
        }
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            testAPIs.put(rnId.toString(),
                         registryUtils.getRepNodeTest(rnId));
        }
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            testAPIs.put(anId.toString(),
                         registryUtils.getArbNodeTest(anId));
        }
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            try {
                testAPIs.put(String.format("admin on %s", snId),
                             registryUtils.getAdminTest(snId));
            } catch (NotBoundException e) {
                /* ignore */
            }
        }
        for (Map.Entry<String, RemoteTestAPI> entry :
             testAPIs.entrySet()) {
            final String key = entry.getKey();
            if (skipSet.contains(key)) {
                continue;
            }
            final String actual = entry.getValue().getSecurityProperty(name);
            assertEquals(String.format(
                             "Wrong security property value for %s",
                             entry.getKey()),
                         expected, actual);
        }
    }

    @Test
    public void testSNStartWithSpecifiedTTL() throws Exception {
        final CreateStore store =
            new CreateStore(kvstoreName, START_PORT,
                            3, /* storage nodes */
                            CreateStore.ZoneInfo.primaries(1), /* rf */
                            100, /* partitions */
                            1, /* capacity */
                            CreateStore.MB_PER_SN, /* memoryMB */
                            false, /* useThreads */
                            null, /* mgmtImpl */
                            false, /* mgmtPortsShared */
                            false, /* secure */
                            null, /* userExternalAuth */
                            CACHE_PARAMETER_SET /* extraParams */);
        try {
            store.start(false);
            final int port = store.getRegistryPort(new StorageNodeId(1));
            runCommand(port, "verify configuration", "0 violations");
            assertDnsCacheTTL(port, CACHE_TTL_VALUE, SNS_AND_BOOTSTRAPADMIN);
            assertDnsCacheNegativeTTL(
                port, CACHE_TTL_VALUE, SNS_AND_BOOTSTRAPADMIN);
        } finally {
            store.shutdown();
        }
    }

    @Test
    public void testPolicy() throws Exception {
        final CreateStore store =
            new CreateStore(kvstoreName, START_PORT,
                            3, /* storage nodes */
                            2, /* rf */
                            100, /* partitions */
                            1 /* capacity */);
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.COMMON_DNS_CACHE_TTL,
                         Integer.toString(CACHE_TTL_VALUE));
        map.setParameter(ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL,
                         Integer.toString(ANOTHER_CACHE_TTL_VALUE));
        store.setAllowArbiters(true);
        store.setPolicyMap(map);
        try {
            store.start(false);
            final int port = store.getRegistryPort(new StorageNodeId(1));
            assertDnsCacheTTL(port, CACHE_TTL_VALUE, SNS_AND_BOOTSTRAPADMIN);
            assertDnsCacheNegativeTTL(
                port, ANOTHER_CACHE_TTL_VALUE, SNS_AND_BOOTSTRAPADMIN);
            store.restart();
            assertDnsCacheTTL(port, CACHE_TTL_VALUE, EMPTY_SET);
            assertDnsCacheNegativeTTL(
                port, ANOTHER_CACHE_TTL_VALUE, EMPTY_SET);
        } finally {
            store.shutdown();
        }
    }

    @Test
    public void testChangeParams() throws Exception {
        final CreateStore store =
            new CreateStore(kvstoreName, START_PORT,
                            3, /* storage nodes */
                            2, /* rf */
                            100, /* partitions */
                            1 /* capacity */);
        store.setAllowArbiters(true);
        try {
            store.start();
            final int port = store.getRegistryPort(new StorageNodeId(1));
            for (int i = 0; i < 3; ++i) {
                runCommand(port,
                           String.format(
                               "plan change-parameters -wait -service sn%s " +
                               "-params dnsCacheTTL=%s",
                               i + 1, CACHE_TTL_VALUE));
            }
            assertDnsCacheTTL(port, CACHE_TTL_VALUE, SN_SET);
            assertDnsCacheNegativeTTL(
                port, ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT,
                SN_SET);
        } finally {
            store.shutdown();
        }
    }
}
