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
public class AsyncLatencyTest extends LatencyTestBase {

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

        // this test doesn't run on minicloud or cloud
        assumeTrue(!Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        latencySetUp(true /*useAsync*/, 100 /*delayMs*/);
    }

    @Test
    public void testAsyncGetPutLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // with async, we should be able to keep the same latencies
        // even when using more client threads than proxy threads
        testLatency("asyncGetPutLatency",
            3 /*readThreads*/,
            3 /*writeThreads*/,
            3 /*rwThreads*/,
            0 /*qThreads*/,
            10 /*runSeconds*/,
            90 /*minReadLatencyMs*/,
            150 /*maxReadLatencyMs*/,
            90 /*minWriteLatencyMs*/,
            150 /*maxWriteLatencyMs*/,
            0 /*minQueryLatencyMs*/,
            0 /*maxQueryLatencyMs*/);
    }


    @Test
    public void testAsyncQueryLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // This test has too many random failures in jenkins to be
        // useful. Most are due to lack of CPU or resources in those
        // test environments. So only run this test if verbose is
        // enabled, which isn't by default in jenkins.
        assumeTrue(verbose);

        // with async, we should be able to keep the same latencies
        // even when using more client threads than proxy threads
        testLatency("asyncQueryLatency",
            0 /*readThreads*/,
            0 /*writeThreads*/,
            0 /*rwThreads*/,
            8 /*qThreads*/,
            10 /*runSeconds*/,
            0 /*minReadLatencyMs*/,
            0 /*maxReadLatencyMs*/,
            0 /*minWriteLatencyMs*/,
            0 /*maxWriteLatencyMs*/,
            90 /*minQueryLatencyMs*/,
            250 /*maxQueryLatencyMs*/);
    }


    @Test
    public void testAsyncGetPutQueryLatency() throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        // with async, we should be able to keep the same latencies
        // even when using more client threads than proxy threads
        testLatency("asyncGetPutQueryLatency",
            2 /*readThreads*/,
            2 /*writeThreads*/,
            2 /*rwThreads*/,
            4 /*qThreads*/,
            10 /*runSeconds*/,
            90 /*minReadLatencyMs*/,
            170 /*maxReadLatencyMs*/,
            90 /*minWriteLatencyMs*/,
            170 /*maxWriteLatencyMs*/,
            90 /*minQueryLatencyMs*/,
            250 /*maxQueryLatencyMs*/);
    }

}
