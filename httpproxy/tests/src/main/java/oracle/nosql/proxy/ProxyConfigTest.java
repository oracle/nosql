/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStoreConfig;
import oracle.kv.Durability;
import oracle.nosql.proxy.util.TestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validate the proxy configuration
 */
public class ProxyConfigTest extends TestBase {
    private static PrintStream original;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * Filter out the stderr output from proxy startup
         */
        original = System.out;
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {}
        }));
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        if (original != null) {
            System.setErr(original);
        }
    }

    @Test
    public void testPrecedence() throws Exception {

        Properties fileProps = new Properties();
        fileProps.setProperty(Config.NUM_REQUEST_THREADS.paramName, "123");
        fileProps.setProperty(Config.MONITOR_STATS_ENABLED.paramName, "true");
        fileProps.setProperty(Config.IDLE_READ_TIMEOUT.paramName, "8888");
        String configFileName = createConfigFile(fileProps);


        Properties commandLine = new Properties();
        commandLine.setProperty(Config.NUM_REQUEST_THREADS.paramName, "456");
        commandLine.setProperty(Config.NUM_ACCEPT_THREADS.paramName, "5");
        commandLine.setProperty(Config.KV_REQUEST_TIMEOUT.paramName, "4000");
        commandLine.setProperty(Config.KV_CONSISTENCY.paramName, "ABSOLUTE");
        commandLine.setProperty(Config.CONFIG_FILE.paramName, configFileName);

        Config config = new Config(commandLine);
        KVStoreConfig kvConfig = config.getTemplateKVStoreConfig();

        /* don't validate all fields, just a few */
        assertEquals(456, config.getNumRequestThreads());
        assertEquals(5, config.getNumAcceptThreads());
        assertEquals(true, config.isMonitorStatsEnabled());
        assertEquals(8888, config.getIdleReadTimeout());
        assertEquals(4000, kvConfig.getRequestTimeout(TimeUnit.MILLISECONDS));
        assertEquals(Consistency.ABSOLUTE, kvConfig.getConsistency());
    }

    @Test
    public void testSslProtocols() throws Exception {
        Properties commandLine = new Properties();
        commandLine.setProperty(Config.SSL_PROTOCOLS.paramName,
                                "TLSv1.3,TLSv1.1");
        Config config = new Config(commandLine);
        assertEquals(config.getSSLProtocols().length, 2);

        commandLine.setProperty(Config.SSL_PROTOCOLS.paramName,
                                "TLSv1.4,TLSv1.1");
        try {
            new Config(commandLine);
        } catch (IllegalArgumentException e) {
        }

        commandLine.setProperty(Config.SSL_PROTOCOLS.paramName,
                                "TLSv1,");
        try {
            new Config(commandLine);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testKVDurability() throws Exception {
        String[] s = new String[]{"-" + Config.KV_DURABILITY.paramName, "COMMIT_ALL_SYNC"};
        Config c = new Config(s);
        KVStoreConfig kvConfig = c.makeTemplateKVStoreConfig();
        Durability durability = kvConfig.getDurability();
        assertEquals(durability.getMasterSync(), Durability.SyncPolicy.SYNC);
        assertEquals(durability.getReplicaSync(), Durability.SyncPolicy.SYNC);
        assertEquals(durability.getReplicaAck(),
                Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);

        s = new String[]{"-" + Config.KV_DURABILITY.paramName, "COMMIT_ALL_WRITE_NO_SYNC"};
        c = new Config(s);
        kvConfig = c.makeTemplateKVStoreConfig();
        durability = kvConfig.getDurability();
        assertEquals(durability.getMasterSync(), Durability.SyncPolicy.WRITE_NO_SYNC);
        assertEquals(durability.getReplicaSync(), Durability.SyncPolicy.WRITE_NO_SYNC);
        assertEquals(durability.getReplicaAck(),
                Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);

    }

    @Test
    public void testCommandLine() throws Exception {

    	String[] s = new String[] {"-foo"};
        try {
        	new Config(s);
        } catch (IllegalArgumentException e) {
        }

        s = new String[] {Config.STORE_NAME.paramName, "StagingStore"};
        try {
        	new Config(s);
        } catch (IllegalArgumentException e) {
        }

        s = new String[] {"-" + Config.STORE_NAME.paramName, "StagingStore"};
        Config config = new Config(s);
        assertEquals("StagingStore", config.getStoreName());

    }


    private String createConfigFile(Properties fileContents)
        throws Exception {

        File configFile = new File(getTestDir(), "mock.config.props");
        OutputStream output = new FileOutputStream(configFile);
        fileContents.store(output, "Mock config file");
        return configFile.getAbsolutePath();
    }

}
