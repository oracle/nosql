/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import oracle.kv.KVStoreConfig;
import oracle.kv.TestBase;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Client-side runner for using the ClientTestServices process.  Appropriate
 * for use in unit tests as well as standalone tests.
 */
public class ClientTestServicesRunner implements Closeable {

    private static List<String> EMPTY_LIST = Collections.emptyList();

    private final KVStoreConfig config;
    private Process process;
    private IOThread iothread;

    /**
     * Starts KVStore services in ClientTestServices process and initializes
     * KVStoreConfig.
     *
     * @param kvstoreName the name for the KVStore
     */
    public ClientTestServicesRunner(String kvstoreName)
        throws IOException {

        this(kvstoreName, EMPTY_LIST);
    }

    /**
     * Starts KVStore services in ClientTestServices process and initializes
     * KVStoreConfig.
     *
     * @param kvstoreName the name for the KVStore
     * @param testCommands - Additional commands passed to the
     * ClientTestServices process.  Supported commands are documented at
     * ClientTestServices#main.
     */
    public ClientTestServicesRunner(String kvstoreName,
                                    List<String> testCommands)
        throws IOException {

        assertNotNull("kvstoreName must not be null", kvstoreName);

        String jvmExtraArgs = System.getProperty("oracle.kv.jvm.extraargs");
        String javaVendor = System.getProperty("java.vendor");
        List<String> command = new ArrayList<String>();
        command.add("java");
        if (javaVendor.equals("Azul Systems, Inc.")) {
            command.add("-XX:+UseZingMXBeans");
        }
        command.add("-ea");
        JVMSystemUtils.addZingJVMArgs(command);
        if (jvmExtraArgs != null) {
            for (String arg : TestUtils.splitExtraArgs(jvmExtraArgs)) {
                command.add(arg);
            }
            command.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }

        /*
         * Need to pass these two properties set in build.xml to the new
         * java process. Then the test in the new java process can
         * capture these two properties.
         */
        command.add("-Dtest.je.env.runVerifier=" +
                    System.getProperty("test.je.env.runVerifier", "false"));
        command.add("-Dtest.je.env.verifierSchedule=" +
                    System.getProperty("test.je.env.verifierSchedule",
                                       "* * * * *"));

        command.add("-D" + TestUtils.DEST_DIR + "=" +
                    System.getProperty(TestUtils.DEST_DIR));
        command.add("-D" + TestUtils.NO_SSL_SYS_PRO + "=" +
                    System.getProperty(TestUtils.NO_SSL_SYS_PRO));
        command.add("-D" + SSLTestUtils.SSL_DIR_PROP + "=" +
                    System.getProperty(SSLTestUtils.SSL_DIR_PROP));
        command.add("-D" + TestBase.MR_TABLE_MODE_PROP + "=" +
                    TestBase.mrTableMode);
        command.add("-cp");

        /*
         * Replace kvclient with kvstore jar for the server.  Make sure that we
         * are running with the client jar and not server.
         */
        String cp = System.getProperty("java.class.path");

        boolean disableClientTestChecks =
            Boolean.getBoolean("test.disable.client.test.checks");
        if (!disableClientTestChecks) {
            String kvclient_regex = "kvclient-[0-9.]+(?:-SNAPSHOT)?\\.jar";
            String kvstore_regex = "kvstore-[0-9.]+(?:-SNAPSHOT)?\\.jar";
            assert(!Pattern.compile(kvstore_regex).matcher(cp).find()) :
                "** Use ant target 'testclient' rather than 'test' **\n" +
                "Classpath includes kvstore.jar and it should not: " + cp;
            assert(Pattern.compile(kvclient_regex).matcher(cp).find()) :
                "** Use ant target 'testclient' rather than 'test' **\n" +
                "Classpath does not include kvclient.jar and it should: " + cp;
        }
        cp = cp.replaceAll("kvclient", "kvstore");
        command.add(cp);
        boolean enableServerSideLogging =
            Boolean.getBoolean("test.enable.server.side.logging");
        if (enableServerSideLogging) {
            String loggingConfigFile =
                System.getProperty("java.util.logging.config.file");
            if (loggingConfigFile != null) {
                command.add("-Djava.util.logging.config.file=" +
                            loggingConfigFile);
            }
        }
        command.add("oracle.kv.impl.api.ops.ClientTestServices");
        command.add(kvstoreName);
        command.addAll(testCommands);
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        process = builder.start();

        try {
            /**
             * Read config from stdout
             */
            final BufferedReader reader = new BufferedReader
                (new InputStreamReader(process.getInputStream()));
            final String host;
            final String storename;
            final int reg1;
            final int reg2;

            final List<String> lines = new ArrayList<String>();
            for (int i = 0; i < 4; i++) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                lines.add(line);
            }
            final String msg = "Error in process input: " + lines;
            if (lines.size() != 4) {
                throw new IllegalStateException
                    (msg + ", expected 4 output lines from process");
            }
            storename = lines.get(0);
            host = lines.get(1);
            try {
                reg1 = Integer.parseInt(lines.get(2));
                reg2 = Integer.parseInt(lines.get(3));
            } catch (NumberFormatException e) {
                throw new IllegalStateException
                    (msg + ", " + e +
                     " line(0)=" + lines.get(0) + "\n" +
                     " line(1)=" + lines.get(1) + "\n" +
                     " line(2)=" + lines.get(2) + "\n" +
                     " line(3)=" + lines.get(3) + "\n");
            }

            config = new KVStoreConfig(storename, host + ":" + reg1,
                                       host + ":" + reg2);

            /*
             * Make the socket timeouts large enough, so they don't produce
             * spurious failures.
             */
            config.setSocketOpenTimeout(10000, TimeUnit.MILLISECONDS);
            config.setSocketReadTimeout(60000, TimeUnit.MILLISECONDS);

            iothread = new IOThread(reader, "ClientTestBaseMonitor");
            iothread.start();
        } catch (RuntimeException re) {
            close();
            throw re;
        } catch (IOException ioe) {
            close();
            throw ioe;
        }
    }

    public KVStoreConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        try {
            if (process != null) {
                process.destroy();
            }
        } finally {
            process = null;

            try {
                if (iothread != null) {
                    iothread.join();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                iothread = null;
            }
        }
    }

    /**
     * Read process output and send to stderr for debugging purposes.
     * Generally this will be empty.
     */
    private class IOThread extends Thread {
        private final BufferedReader reader;

        IOThread(BufferedReader reader, String name) {
            super(name);
            this.reader = reader;
        }

        @Override
        public void run() {
            try {

                /**
                 * Process may have already been stopped and nulled.
                 */
                if (process == null) {
                    return;
                }
                for (String line = reader.readLine();
                     line != null;
                     line = reader.readLine()) {
                    System.err.println("CTS: " + line);
                }
            } catch (Exception e) {
                System.err.println("IOThread exception: " + e);
            }
        }
    }
}
