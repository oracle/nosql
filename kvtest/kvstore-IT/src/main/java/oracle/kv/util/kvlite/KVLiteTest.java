/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util.kvlite;

import static oracle.kv.impl.util.TestUtils.NULL_PRINTSTREAM;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Exercise KVLite.
 */
@RunWith(FilterableParameterized.class)
public class KVLiteTest extends TestBase {

    private static final String testdir = TestUtils.getTestDir().toString();

    private static final String storeName = "kvstore";
    private static final String testhost = "localhost";
    private static final int startPort = 6000;
    private static final int haRange = 2;

    private PortFinder portFinder;
    private int testport;
    private KVLite kvlite;

    private boolean embedded;

    /**
     * Set the embedded value for this run of the test suite.
     */
    public KVLiteTest(boolean isEmbedded) {
        embedded = isEmbedded;
    }

    /**
     * @return the input data for parameterized tests.
     */
    @Parameters(name="embedded={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (embedded) {
            EmbeddedMode.setEmbedded(true);
        }
        portFinder = new PortFinder(startPort, haRange);
        TestStatus.setManyRNs(true);
        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (kvlite != null) {
            kvlite.stop(false);
            kvlite = null;
        }
        LoggerUtils.closeAllHandlers();
        if (embedded) {
            EmbeddedMode.setEmbedded(false);
        }
    }

    @Test
    public void testAdminWithThreads() {

        testport = portFinder.getRegistryPort();
        kvlite = new KVLite(testdir,
                            storeName,
                            testport,
                            true,
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            false,
                            true,
                            null,
                            -1);
        kvlite.setPolicyMap(mergeParameterMapDefaults(new ParameterMap()));
        kvlite.setVerbose(false);
        kvlite.setNumPartitions(10);
        kvlite.start(true);

        /*
         * increase coverage on simple getters
         */
        assertNotNull(kvlite.getSNA());
        assertNotNull(kvlite.getSNAPI());
        assertFalse(kvlite.getVerbose());
        assertFalse(kvlite.getEnableJmx());
        assertTrue(kvlite.getNumPartitions() == 10);
        assertNotNull(kvlite.getPolicyMap());

        /* quiet stderr */
        PrintStream ps = System.err;
        System.setErr(NULL_PRINTSTREAM);
        kvlite.shutdownStore(false);
        System.setErr(ps);

        kvlite = null;
    }

    @Test
    public void testAdminWithProcesses() {

        testport = portFinder.getRegistryPort();
        kvlite = new KVLite(testdir,
                            storeName,
                            testport,
                            true,
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            true,
                            true,
                            null,
                            -1);
        kvlite.setPolicyMap(mergeParameterMapDefaults(new ParameterMap()));
        kvlite.setVerbose(false);
        kvlite.setEnableJmx(true);
        kvlite.setNumPartitions(10);
        kvlite.start(true);

        /*
         * increase coverage on simple getters
         */
        assertNotNull(kvlite.getSNA());
        assertNotNull(kvlite.getSNAPI());
        assertFalse(kvlite.getVerbose());
        assertTrue(kvlite.getEnableJmx());
        assertTrue(kvlite.getNumPartitions() == 10);
        assertNotNull(kvlite.getPolicyMap());

        kvlite.stop(false);
        kvlite = null;
    }
    @Test
    public void testRepNodeWithThreads() {

        testport = portFinder.getRegistryPort();
        kvlite = new KVLite(testdir,
                            storeName,
                            testport,
                            false, /* no admin means use KVLiteRepNode */
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            false,
                            true,
                            null,
                            -1);
        kvlite.setPolicyMap(mergeParameterMapDefaults(new ParameterMap()));
        kvlite.setVerbose(false);
        kvlite.start(true);

        assertNotNull(kvlite.getSNA());
        assertNotNull(kvlite.getSNAPI());
        kvlite.stop(false);
        kvlite = null;
    }

    @Test
    public void testRepNodeWithProcesses() {

        PrintStream ps = System.out;
        PrintStream psErr = System.err;
        System.setOut(NULL_PRINTSTREAM);
        System.setErr(NULL_PRINTSTREAM);

        testport = portFinder.getRegistryPort();
        kvlite = new KVLite(testdir,
                            storeName,
                            testport,
                            false, /* no admin means use KVLiteRepNode */
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            true,
                            true,
                            null,
                            -1);
        kvlite.setPolicyMap(mergeParameterMapDefaults(new ParameterMap()));
        kvlite.setVerbose(true);
        kvlite.start(true);

        assertNotNull(kvlite.getSNA());
        assertNotNull(kvlite.getSNAPI());
        kvlite.stop(false);
        kvlite = null;

        System.setOut(ps);
        System.setErr(psErr);
    }

    @Test
    public void testMain() {

        testport = portFinder.getRegistryPort();

        PrintStream ps = System.err;
        System.setErr(NULL_PRINTSTREAM);

        ArrayList<String> args = new ArrayList<String>();

        args.add("-root");
        args.add(testdir);
        args.add("-store");
        args.add(storeName);
        args.add("-host");
        args.add(testhost);
        args.add("-port");
        args.add(Integer.toString(testport));
        args.add("-verbose");
        args.add("-storagedir");
        args.add(testdir);
        args.add("-partitions");
        args.add("10");

        KVLite.main(args.toArray(new String[0]));

        args.add("-shutdown");
        KVLite.main(args.toArray(new String[0]));

        /*
         * add more args to exercise some more path, but this will
         * ultimately fail because of the bad root dir. Generating a usage
         * message results in a process exit, which makes JUnit unhappy, so that
         * path remains unused.
         */
        args.add("-jmx");
        args.add("-nothreads");
        args.add("-harange");
        args.add("5000:5010");
        args.add("-servicerange");
        args.add("5000:5010");
        args.add("-mount");
        args.add("testdir");
        args.add("-root");
        args.add("root_does_not_exist");
        KVLite.main(args.toArray(new String[0]));

        System.setErr(ps);
    }

    @Test
    public void testMultiShardStore() {
        int numStorageNodes = 3;
        String testports = "";
        for (int i = 0; i < numStorageNodes; i++) {
            PortFinder pf = new PortFinder(startPort + (i + 1) *20, haRange);
            testports += Integer.toString(pf.getRegistryPort());
            if (i != numStorageNodes - 1) {
                testports += KVLite.DEFAULT_SPLIT_STR;
            }
        }
        kvlite = new KVLite(testdir,
                            storeName,
                            testports,
                            true,
                            testhost,
                            null,
                            null,
                            0,
                            null,
                            false,
                            true,
                            null,
                            -1,
                            3,
                            3,
                            3);
        kvlite.setPolicyMap(mergeParameterMapDefaults(new ParameterMap()));
        kvlite.setVerbose(false);
        kvlite.setEnableJmx(true);
        kvlite.setNumPartitions(30);
        kvlite.start(true);

        /*
         * increase coverage on simple getters
         */
        assertNotNull(kvlite.getSNA());
        assertNotNull(kvlite.getSNAPI());
        assertFalse(kvlite.getVerbose());
        assertTrue(kvlite.getEnableJmx());
        assertTrue(kvlite.getNumPartitions() == 30);
        assertNotNull(kvlite.getPolicyMap());

        kvlite.stop(false);
        kvlite = null;
    }
}
