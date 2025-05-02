/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.KVStoreMain;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.Ping.ExitCode;

import org.junit.Test;

/**
 * This test focuses on Ping's role as a health check, and if it can detect
 * problems.
 */
public class PingProblemTest extends TestBase {
    private CreateStore createStore = null;

    @Override
    public void setUp()
        throws Exception {
        super.setUp();
        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    // TODO: test of an unregistered SN

    /**
     * Bad arguments, make sure JSON and exit code are valid
     */
    @Test
    public void testArguments()
        throws Exception {

        /* Expect to see a usage message */
        processPing(null /* no topology */, null /* no admin */,
                    ExitCode.EXIT_USAGE, ErrorMessage.NOSQL_5100, "-json-v1");

        /*
         * Provide a bad port value, make sure we don't get a number format
         * exception.
         */
        processPing(null /* no topology */, null /* no admin */,
                    ExitCode.EXIT_USAGE, ErrorMessage.NOSQL_5100, "-json-v1",
                    "-host", "localhost", "-port", "InvalidPortValue");
    }

    /**
     * Check that Ping has appropriate errors in situations when an store
     * has downed components.
     */
    @Test
    public void testPing()
        throws Exception {

        /* Start a 1x3 store */
        try {
            createStore = new CreateStore(kvstoreName,
                                          5240, // random port to start at
                                          3, // numSNs
                                          3, // RF
                                          3, // numPartitions
                                          1, // capacity
                                          CreateStore.MB_PER_SN,
                                          true, /* useThreads */
                                          null,
                                          true,
                                          SECURITY_ENABLE);
            createStore.start();
            CommandServiceAPI admin3 = createStore.getAdmin(2);
            StorageNodeId[] snIds  = createStore.getStorageNodeIds();
            List<HostPort> hostPorts = findHostPorts(snIds);
            String helperString = getHelpers(hostPorts);

            /*
             * Everything in the store is ok. Use both threadKVStoreMain and
             * processPing, mainly to keep threadKVStoreMain used.
             * threadKVStoreMain is handy when debugging ping, but processPing
             * does a more thorough job of checking the exit code.
             */
            String jsonOutput = threadKVStoreMain(
                createStore.maybeAddSecurityFlag("ping", "-no-exit",
                    "-helper-hosts", helperString, "-json-v1"));
            PingUtils.checkResult(jsonOutput, Ping.ExitCode.EXIT_OK,
                                  ErrorMessage.NOSQL_5000);
            processPing(admin3.getTopology(), admin3.getParameters(),
                        Ping.ExitCode.EXIT_OK, ErrorMessage.NOSQL_5000,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts",  helperString));

            /* sn1 - admin down */
            createStore.getStorageNodeAgent(0).stopAdmin(new AdminId(1), true);
            processPing(admin3.getTopology(), admin3.getParameters(),
                        Ping.ExitCode.EXIT_OPERATIONAL, ErrorMessage.NOSQL_5301,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

            /*
             * sn1 - admin down
             * sn2 - admin down
             */
            createStore.getStorageNodeAgent(1).stopAdmin(new AdminId(2), true);
            processPing(admin3.getTopology(), admin3.getParameters(),
                        Ping.ExitCode.EXIT_NO_ADMIN_QUORUM,
                        ErrorMessage.NOSQL_5302,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

            /*
             * sn1 - admin down
             * sn2 - admin down
             * sn3 - rn down
             */
            createStore.getStorageNodeAgent(2).stopRepNode(new RepNodeId(1,3),
                                                           true);
            processPing(admin3.getTopology(), admin3.getParameters(),
                        Ping.ExitCode.EXIT_NO_ADMIN_QUORUM,
                        ErrorMessage.NOSQL_5302,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

            /*
             * sn1 - admin down
             * sn2 - admin down, rn down
             * sn3 - rn down
             */
            createStore.getStorageNodeAgent(1).stopRepNode(new RepNodeId(1,2),
                                                           true);
            processPing(admin3.getTopology(), admin3.getParameters(),
                        Ping.ExitCode.EXIT_NO_SHARD_QUORUM,
                        ErrorMessage.NOSQL_5303,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

            /*
             * Use helper hosts addresses for SN1 and SN3 and shutdown SN3, to
             * make sure we use the first address.
             * sn1 - admin down
             * sn2 - admin down, rn down
             * sn3 - rn down, sn is shut down
             */
            createStore.shutdownSNA(2, true);
            List<HostPort> twoSNs = new ArrayList<HostPort>();
            twoSNs.add(hostPorts.get(0));
            twoSNs.add(hostPorts.get(2));
            helperString = getHelpers(twoSNs);

            processPing(createStore.getRepNodeAdmin(0).getTopology(),
                        null /* no admin */,
                        Ping.ExitCode.EXIT_NO_SHARD_QUORUM,
                        ErrorMessage.NOSQL_5303,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

            createStore.shutdownSNA(0, true);
            processPing(null /* no topology */, null /* no admin */,
                        Ping.ExitCode.EXIT_TOPOLOGY_FAILURE,
                        ErrorMessage.NOSQL_5304,
                        createStore.maybeAddSecurityFlag(
                            "-json-v1", "-helper-hosts", helperString));

        } finally {
            if (createStore != null) {
                createStore.shutdown();
            }
        }
    }

    /**
     * Check that Ping can find a topology from the Admins, if all RNs are
     * down.
     */
    @Test
    public void testTopoFromAdminWithPingCommand()
        throws Exception {

        testTopoFromAdmin(false /* useAdminCLI */);
    }

    /**
     * Check that admin CLI's Ping returns correct exit code.
     */
    @Test
    public void testTopoFromAdminWithRunadminCommand()
        throws Exception {

        testTopoFromAdmin(true /* useAdminCLI */);
    }

    private void testTopoFromAdmin(boolean useAdminCLI)
        throws Exception {

        /* Start a 1x3 store */
        try {
            createStore = new CreateStore(kvstoreName,
                                          5240, // random port to start at
                                          2, // numSNs
                                          2, // RF
                                          2, // numPartitions
                                          1, // capacity
                                          CreateStore.MB_PER_SN,
                                          true, /* useThreads */
                                          null,
                                          true,
                                          SECURITY_ENABLE);
            createStore.start();
            StorageNodeId[] snIds  = createStore.getStorageNodeIds();
            List<HostPort> hostPorts = findHostPorts(snIds);
            String helperString = getHelpers(hostPorts);
            String jsonOutput;

            /* Everything ok */
            if (useAdminCLI) {
                jsonOutput = threadKVStoreMain(
                    createStore.maybeAddSecurityFlag("runadmin", "-no-exit",
                        "-helper-hosts", helperString, "ping", "-json-v1"));
            } else {
                jsonOutput = threadKVStoreMain(
                    createStore.maybeAddSecurityFlag("ping", "-no-exit",
                        "-helper-hosts", helperString, "-json-v1"));
            }
            PingUtils.checkResult(jsonOutput, Ping.ExitCode.EXIT_OK,
                                  ErrorMessage.NOSQL_5000);

            /* Shut down all RNS */
            createStore.getStorageNodeAgent(0).stopRepNode(new RepNodeId(1,1),
                                                           true);
            createStore.getStorageNodeAgent(1).stopRepNode(new RepNodeId(1,2),
                                                           true);
            if (useAdminCLI) {
                jsonOutput = threadKVStoreMain(createStore.maybeAddSecurityFlag(
                    "runadmin", "-no-exit", "-helper-hosts", helperString,
                    "ping", "-json-v1"));
            } else {
                jsonOutput = threadKVStoreMain(createStore.maybeAddSecurityFlag(
                    "ping", "-no-exit", "-helper-hosts", helperString,
                    "-json-v1"));
            }
            PingUtils.checkResult(jsonOutput,
                                  Ping.ExitCode.EXIT_NO_SHARD_QUORUM,
                                  ErrorMessage.NOSQL_5303);
            PingUtils.checkJsonOutput(jsonOutput,
                                      createStore.getAdmin().getTopology(),
                                      createStore.getAdmin().getParameters(),
                                      false /* topoOverviewOnly */);
        } finally {
            if (createStore != null) {
                createStore.shutdown();
            }
        }
    }

    /* Find a list of helper hosts for the nodes in this cluster */
    private List<HostPort> findHostPorts(StorageNodeId[] snIds) {
        List<HostPort> hpList = new ArrayList<HostPort>();
        String hostname = createStore.getHostname();
        for (StorageNodeId snId : snIds) {
            hpList.add(new HostPort(hostname,
                                    createStore.getRegistryPort(snId)));
        }
        return hpList;
    }

    /**
     * Create a string that's host1:port,host2:port ... etc
     */
    private String getHelpers(List<HostPort> hpList) {
        StringBuilder sb = new StringBuilder();
        for (HostPort hp: hpList) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(hp.toString());
        }

        return sb.toString();
    }

    /**
     * Run ping as a separate process, in order to be able to check for both
     * the actual process exit code, and also the json output that displays
     * the exit code.
     * @throws IOException
     */
    private void processPing(Topology topology,
                             Parameters params,
                             ExitCode pingExitCode,
                             ErrorMessage expectedErrorMessage,
                             String... args) throws IOException {

        /* Expect to see a usage message */
        List<String> command = ProcessSetupUtils.setupJavaJarKVStore();
        command.add("ping");
        for (String a : args) {
            command.add(a);
        }

        StringBuilder sb = new StringBuilder();
        int exitValue = ProcessSetupUtils.runProcess(command, sb);

        /* check that the exit code is as expected. */
        assertEquals(sb.toString(), pingExitCode.value(), exitValue);

        /*
         * check that the json output is valid, and contains the expected
         * exit code and error message code.
         */
        PingUtils.checkResult(sb.toString(), pingExitCode,
            expectedErrorMessage);
        if (topology != null) {
            PingUtils.checkJsonOutput(sb.toString(),
                                      topology,
                                      params,
                                      false /* topoOverviewOnly */);
        }
    }

    /**
     * Run kvstoremain as a thread. Keeping this method around because it's
     * easier to debug a call to ping in a debugger this way.
     */
    private String threadKVStoreMain(String... args) throws Exception {

        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        final PrintStream originalErr = System.err;
        try {
            System.setOut(new PrintStream(outBytes));
            System.setErr(new PrintStream(errBytes));
            KVStoreMain.main(args);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
        String output =
            outBytes.size() != 0 ? outBytes.toString() : errBytes.toString();
        logger.info("Command: " + Arrays.toString(args) +
                    "\nOutput\n: " + output);
        return output;
    }

}
