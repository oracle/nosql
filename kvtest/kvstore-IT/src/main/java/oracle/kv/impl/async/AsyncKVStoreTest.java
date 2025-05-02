/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.util.TestUtils.getTestDir;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.async.registry.ServiceRegistryClient;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.util.CreateStore;
import oracle.kv.util.TestUtils;

import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.utilint.TestHook;
import com.sun.management.HotSpotDiagnosticMXBean;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncKVStoreTest extends TestBase {

    @Nullable CreateStore createStore;

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
    }

    /**
     * Only runs the test cases of this suite when the async is enabled.
     */
    @BeforeClass
    public static void ensureAsyncEnabled() {
        assumeTrue("Requires async", AsyncControl.serverUseAsync);
    }

    @Test
    public void testNonSecureLookupSecureStore() throws Exception {
        createStore(true);
    }

    private void createStore(boolean secure)
        throws Exception {

        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      10, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN, /* memoryMB */
                                      true, /* useThreads */
                                      null, /* mgmtImpl */
                                      false, /* mgmtPortsShared */
                                      secure); /* secure */
        createStore.start();
        final CreateStore store = createStore;
        if (store == null) {
            return;
        }
        final long timeout = 5000;
        final TestUtils.Operation op =
            () ->
            ServiceRegistryClient.getRegistry(
                store.getHostname(), store.getRegistryPort(),
                timeout, null /* security */)
            .thenAccept(registry -> registry.list(timeout))
            .get(timeout, TimeUnit.MILLISECONDS);
        TestUtils.checkException(
            op, ExecutionException.class, "possible security mismatch");
    }

    /**
     * Tests that repeated creating new store handle does not leak file
     * descriptor.
     *
     * [KVSTORE-1515]
     */
    @Test
    public void testFDLeakUnderManyNewKVStoreHandle() throws Exception {
        System.gc();
        Thread.sleep(1000);
        final String badHost = "unresolvablehost:5000";
        final KVStoreConfig config = new KVStoreConfig("kvstore", badHost);
        final int numHandles = 1024;
        final long numOpenFDBefore = TestUtils.getNumOpenFileDescriptors();
        for (int i = 0; i < numHandles; ++i) {
            try {
                KVStoreFactory.getStore(config);
            } catch (FaultException e) {
                final Throwable rc = findRootCause(e);
                if (rc instanceof UnknownHostException) {
                    /*
                     * Expect UnknownHostException. In [KVSTORE-1515], it is
                     * the UnresolvedAddressException which is the equivalent
                     * of UnknownHostException for SocketChannel that causes
                     * the issue. However, in this code path, it is the
                     * UnknownHostException that got thrown.
                     */
                } else {
                    throw e;
                }
            }
        }
        System.gc();
        Thread.sleep(1000);
        final long numOpenFDAfter = TestUtils.getNumOpenFileDescriptors();
        final long numOpenFDDiff = numOpenFDAfter - numOpenFDBefore;
        try {
            assertTrue(String.format("New opened FD: %s", numOpenFDDiff),
                       numOpenFDDiff < 100);
        } catch (AssertionError e) {
            dumpHeap();
            dumpProcessLsof();
            throw e;
        }
    }

    private Throwable findRootCause(Exception ex) {
        Throwable th = ex;
        while (th.getCause() != null) {
            th = th.getCause();
        }
        return th;
    }

    private void dumpHeap() throws IOException {
        final File filePath = new File(
            getTestDir(),
            "AsyncKVStoreTest.testFDLeakUnderManyNewKVStoreHandle.hprof");
        if (filePath.exists()) {
            filePath.delete();
        }
        System.out.println(String.format("Dumping heap into %s", filePath));
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
            server, "com.sun.management:type=HotSpotDiagnostic",
            HotSpotDiagnosticMXBean.class);
        mxBean.dumpHeap(filePath.getAbsolutePath(), true);
    }

    private void dumpProcessLsof() throws IOException, InterruptedException {
        final File filePath = new File(
            getTestDir(),
            "AsyncKVStoreTest.testFDLeakUnderManyNewKVStoreHandle.lsof");
        if (filePath.exists()) {
            filePath.delete();
        }
        final String lsof = findLsof();
        if (lsof == null) {
            return;
        }
        final int pid = getPid();
        System.out.println(String.format("Dumping lsof into %s", filePath));
        final ProcessBuilder pb =
            new ProcessBuilder(lsof, "-K", "i", "-p", Integer.toString(pid));
        pb.redirectOutput(filePath);
        final Process p = pb.start();
        p.waitFor(1000, TimeUnit.MILLISECONDS);
    }

    private @Nullable String findLsof() {
        final String[] possibleLsofPath = new String[] {
            "/usr/bin/lsof",
            "/usr/sbin/lsof"
        };
        for (String p : possibleLsofPath) {
            if ((new File(p)).exists()) {
                return p;
            }
        }
        return null;
    }

    private int getPid() {
        /* TODO: use ProcessHandle after using Java 11+. */
        final String processName = ManagementFactory.getRuntimeMXBean().getName();
        final int pid = Integer.parseInt(processName.split("@")[0]);
        System.out.println(String.format("By ManagementFactory, pid=%s", pid));
        return pid;
    }

    /**
     * Tests the exception thrown when there is an SSL task exception issue.
     */
    @Test
    public void testSSLTaskExceptionClient() throws Exception {
        createStore(true);
        tearDowns.add(() -> { SSLDataChannel.taskHook = null; });
        final String injectedMessage = "injected";
        SSLDataChannel.taskHook = new TestHook<SSLDataChannel>() {
            @Override
            public void doHook(@Nullable SSLDataChannel ch) {
                if (ch == null) {
                    return;
                }
                if (ch.getSSLEngine().getUseClientMode()) {
                    throw new RuntimeException(injectedMessage);
                }
            }
        };
        assert createStore != null;
        final KVStoreConfig kvConfig = createStore.createKVConfig(true);
        kvConfig.setUseAsync(true);
        final KVStore kvstore = KVStoreFactory.getStore(kvConfig);
        final Key key = Key.createKey("/a");
        try {
            kvstore.get(key);
            fail("Task should fail due to "
                 + "client-side SSL task execution exception.");
        } catch (Throwable t) {
            if (isRunningAlone("testSSLTaskExceptionClient")) {
                t.printStackTrace();
            }
            assertTrue(
                t.toString(), t.toString().contains(injectedMessage));
        }
    }
}
