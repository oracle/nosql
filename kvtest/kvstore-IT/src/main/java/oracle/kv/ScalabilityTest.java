/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.dialog.nio.NioChannelExecutor;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;



/**
 * Tests some kvstore scalability behavior.
 */
public class ScalabilityTest extends TestBase {

    /**
     * Tests that many tiny client processes should not lingering objects
     * causing memory pressure.
     *
     * [KVSTORE-288]
     */
    @Test
    public void testTinyClientOOM() throws Exception {
        /*
         * Disable task removal and use a very long connect timeout time to
         * test that lingering tasks will not pin the endpoint handler objects.
         */
        NioChannelExecutor.MAX_VALUE_TASK_REMOVAL_INTERVAL = true;
        EndpointConfigBuilder.optionDefault(
            AsyncOption.DLG_CONNECT_TIMEOUT, Integer.MAX_VALUE);

        try {
            final CreateStore createStore = new CreateStore(
                "kvstore",
                5000, /* startPort */
                1, /* numStorageNodes */
                1, /* replicationFactor */
                10, /* numPartitions */
                1, /* capacity */
                1, /* memoryMB */
                false, /* useThreads */
                null, /* mgmtImpl */
                false, /* mgmtPortsShared */
                true); /* secure */
            createStore.start();
            Runtime.getRuntime().gc();
            final MemoryMXBean mxbean = ManagementFactory.getMemoryMXBean();
            final long usedMemoryStart = mxbean.getHeapMemoryUsage().getUsed();
            for (int i = 0; i < 30; ++i) {
                final Process p = executeTinyClient();
                final int ret = p.waitFor();
                if (ret != 0) {
                    fail(readOutput(p));
                }
                Runtime.getRuntime().gc();
                logger.fine(
                    () ->
                    String.format("memory usage: %s",
                                  mxbean.getHeapMemoryUsage().getUsed()));
            }
            final long usedMemoryEnd = mxbean.getHeapMemoryUsage().getUsed();
            assertTrue(String.format("Memory usage increased too much " +
                                     "startSize=%s, endSize=%s",
                                     usedMemoryStart, usedMemoryEnd),
                       usedMemoryEnd < usedMemoryStart + 500000);
        } finally {
            NioChannelExecutor.MAX_VALUE_TASK_REMOVAL_INTERVAL = false;
            EndpointConfigBuilder.optionDefault(
                AsyncOption.DLG_CONNECT_TIMEOUT, 10000);
        }
    }

    private Process executeTinyClient() throws Exception {
        final List<String> cmdlist = new ArrayList<String>();
        cmdlist.add(String.format("%s/bin/java", System.getProperty("java.home")));
        cmdlist.add("-cp");
        cmdlist.add(System.getProperty("java.class.path"));
        cmdlist.add(String.format(
            "-D%s=%s",
            KVSecurityConstants.SECURITY_FILE_PROPERTY,
            (new File(TestUtils.getTestDir(),
                      "security/test.security")).getPath()));
        final String jvmExtraArgs = System.getProperty("oracle.kv.jvm.extraargs");
        if (jvmExtraArgs != null) {
            for (String arg : TestUtils.splitExtraArgs(jvmExtraArgs)) {
                cmdlist.add(arg);
            }
            cmdlist.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }
        cmdlist.add(TinyClient.class.getName());
        cmdlist.add(Integer.toString(5000));
        final ProcessBuilder pb = new ProcessBuilder(cmdlist);
        pb.redirectErrorStream();
        return pb.start();
    }

    private String readOutput(Process p) throws Exception {
        return (new BufferedReader(new InputStreamReader(p.getInputStream()))).
            lines().collect(Collectors.joining("\n"));
    }
}
