/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assume.assumeTrue;

import org.junit.Test;
import org.junit.BeforeClass;


/**
 * Verify that latencies remain stable for asynchronous operations when the
 * number of concurrent requests is higher then the number of proxy
 * worker threads.
 *
 * These tests only runs against a local server and not minicloud.
 *
 * The tests use a KVLite that has a test hook that injects long
 * latencies into all requests
 */
public class SyncLatencyTest extends LatencyTestBase {

    /*
     * This test manages its own kvlite/proxy startup to control specific
     * setup properties to allow for a test hook that injects latency into
     * kvlite, and start proxy with only 2 worker threads
     *
     * note this hides the superclass static method so it won't be called
     */
    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        // this test doesn't run on minicloud or cloud test
        assumeTrue("Skip SyncLatencyTest in minicloud or cloud test",
                   !Boolean.getBoolean("usemc") &&
                   !Boolean.getBoolean("usecloud"));

        latencySetUp(false /*useAsync*/, 100 /*delayMs*/);
    }

    @Test
    public void testSyncGetPutLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // without async, we should see significantly higher latencies
        // when using more client threads than proxy threads
        testLatency("syncGetPutLatency",
            3 /*readThreads*/,
            3 /*writeThreads*/,
            3 /*rwThreads*/,
            0 /*qThreads*/,
            10 /*runSeconds*/,
            250 /*minReadLatencyMs*/,
            1000 /*maxReadLatencyMs*/,
            250 /*minWriteLatencyMs*/,
            1000 /*maxWriteLatencyMs*/,
            0 /*minQueryLatencyMs*/,
            0 /*maxQueryLatencyMs*/);
    }


    @Test
    public void testSyncQueryLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // without async, we should see significantly higher latencies
        // when using more client threads than proxy threads
        testLatency("syncQueryLatency",
            0 /*readThreads*/,
            0 /*writeThreads*/,
            0 /*rwThreads*/,
            8 /*qThreads*/,
            10 /*runSeconds*/,
            0 /*minReadLatencyMs*/,
            0 /*maxReadLatencyMs*/,
            0 /*minWriteLatencyMs*/,
            0 /*maxWriteLatencyMs*/,
            250 /*minQueryLatencyMs*/,
            1000 /*maxQueryLatencyMs*/);
    }


    @Test
    public void testSyncGetPutQueryLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // without async, we should see significantly higher latencies
        // when using more client threads than proxy threads
        testLatency("syncGetPutQueryLatency",
            2 /*readThreads*/,
            2 /*writeThreads*/,
            2 /*rwThreads*/,
            4 /*qThreads*/,
            10 /*runSeconds*/,
            250 /*minReadLatencyMs*/,
            1000 /*maxReadLatencyMs*/,
            250 /*minWriteLatencyMs*/,
            1000 /*maxWriteLatencyMs*/,
            250 /*minQueryLatencyMs*/,
            1000 /*maxQueryLatencyMs*/);
    }

}
