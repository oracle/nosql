/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.ThreadUtils.threadId;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.registry.AsyncControl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Verifies that threads created by KVLocal APIs are shutdown properly.
 */
@RunWith(FilterableParameterized.class)
public class KVLocalThreadsTest extends KVLocalTestModeBase {
    private KVLocal local;

    public KVLocalThreadsTest(TestMode testMode) {
        super(testMode);
    }

    @BeforeClass
    public static void ensureAsyncEnabled() {
        assumeTrue("KVLocal requires async", AsyncControl.serverUseAsync);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        /* Cleanup running store */
        if (local != null) {
            try {
                local.stop();
            } catch (Exception e) {
            }
            local = null;
        }
    }

    @Test
    public void testStart() throws Exception {
        listThreads("before starting KVLocal");
        String rootDir = makeTestDir("start");

        final KVLocalConfig.Builder builder = getConfigBuilder(rootDir);
        if (testMode != TestMode.UNIX_DOMAIN) {
            builder.setPort(6000);
        }
        final KVLocalConfig config = builder.build();

        local = KVLocal.start(config);
        local.stop();
        local = null;

        /* Wait a few seconds for threads to exit */
        Thread.sleep(5000);

        final int stopCount1 = listThreads("after stopping KVLocal");
        local = null;

        local = KVLocal.start(config);
        local.stop();
        local = null;

        /* Wait a few seconds for threads to exit */
        Thread.sleep(5000);

        final int stopCount2 =
            listThreads("after stopping KVLocal second time");

        local = KVLocal.start(config);
        local.stop();
        local = null;

        /* Wait a few seconds for threads to exit */
        Thread.sleep(5000);

        final int stopCount3 =
            listThreads("after stopping KVLocal third time");

        assertTrue("Increasing thread counts: 1: " + stopCount1 +
                   " 2: " + stopCount2 + " 3: " + stopCount3,
                   (stopCount1 <= stopCount2) &&
                   (stopCount2 <= stopCount3));
    }

    private String makeTestDir(String subDir) throws IOException {
        /*
         * Use the temp directory to avoid creating a pathname that is too long
         * for Unix domain sockets
         */
        final File testDir = File.createTempFile(subDir, "", null);
        tearDowns.add(() -> FileUtils.deleteDirectory(testDir));
        testDir.delete();
        testDir.mkdir();
        return testDir.getPath();
    }

    private int listThreads(String when) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Threads ").append(when);
        final Map<Thread, StackTraceElement[]> threadMap =
            Thread.getAllStackTraces();
        final int threadCount = threadMap.size();
        sb.append("\nTotal thread count: ").append(threadCount);
        for (final Entry<Thread, StackTraceElement[]> entry :
                 threadMap.entrySet()) {
            final Thread t = entry.getKey();
            sb.append("\nThread: ").append(t.getName())
                .append(": ").append(t.getState())
                .append(" id: ").append(threadId(t));
            if (logger.isLoggable(Level.FINE)) {
                for (final StackTraceElement elem : entry.getValue()) {
                    sb.append("\n").append(elem);
                }
            }
        }
        logger.severe(sb.toString());
        return threadCount;
    }
}
