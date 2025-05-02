/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.param;

import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;

import com.sleepycat.je.utilint.JVMSystemUtils;

import org.junit.Assume;
import org.junit.Test;

/** Tests for the {@link StorageNodeParams} class. */
public class StorageNodeParamsTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }


    /**
     * Test the behavior of the calculateRNHeapAndCache method to reduce the
     * heap size determined by the memoryMB parameter to support compressed
     * OOPs unless the size is 50% larger than the maximum compressed OOPs
     * size, non-compressed OOPs are specified explicitly, or the RN's maximum
     * heap size is specified explicitly.
     */
    @Test
    public void testCalculateRNHeapAndCacheForCompressedOOPs() {
        /* For Zing, cache sizes are unrelated to CompressedOOPs. */
        Assume.assumeFalse(JVMSystemUtils.ZING_JVM);

        final ParameterMap policyMap = new ParameterMap();

        final int maxCompressedMB =
            (int) (StorageNodeParams.MAX_COMPRESSED_OOPS_HEAP_SIZE >> 20);
        final int minUncompressedMB =
            (int) (StorageNodeParams.MIN_NONCOMPRESSED_OOPS_HEAP_SIZE >> 20);
        final int maxCompressedOopsHeapSizeMB =
            (int) (StorageNodeParams.MAX_COMPRESSED_OOPS_HEAP_SIZE >> 20);
        final int rnHeapMaxMB =
            (int) (StorageNodeParams.MAX_COMPRESSED_OOPS_HEAP_SIZE >> 20);

        String desc = "Maximum for compressed OOPs";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 1,
                             maxCompressedMB);

        desc = "Maximum for compressed OOPs, capacity 2";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 2,
                             2 * maxCompressedMB);

        desc = "Bigger than maximum for compressed OOPs";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 1,
                             maxCompressedMB + 1);

        desc = "Bigger than maximum for compressed OOPs, capacity 2";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 2,
                             2 * (maxCompressedMB + 1000));

        desc = "Bigger than maximum for compressed OOPs, capacity 2," +
            " rnHeapMaxMB also bigger than compressed OOPs max";
        policyMap.setParameter(ParameterState.SN_RN_HEAP_MAX_MB,
                               Integer.toString(maxCompressedMB + 1000));
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 2,
                             2 * (maxCompressedMB + 1000));
        policyMap.clear();

        desc = "Minimum for non-compressed OOPs";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 1,
                             minUncompressedMB);

        desc = "Minimum for non-compressed OOPs, capacity 2";
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 2,
                             2 * minUncompressedMB);

        desc = "Bigger than maximum for compressed OOPs, non-compressed OOPs" +
            " requested explicitly; limited by RN_HEAP_MAX_MB";
        policyMap.setParameter(
            ParameterState.JVM_MISC,
            "-Xblah -XX:+UseCompressedOops -XX:-UseCompressedOops");
        checkCalculateRNHeap(desc,
                             rnHeapMaxMB,
                             policyMap, 1,
                             maxCompressedMB + 1);

        desc = "Bigger than maximum for compressed OOPs, non-compressed OOPs" +
            " requested explicitly is allocated if SN_RN_HEAP_MAX_MB permits it";
        int rnMaxHeap2MB = 2 * maxCompressedOopsHeapSizeMB;
        policyMap.setParameter(ParameterState.SN_RN_HEAP_MAX_MB,
                               Integer.toString(rnMaxHeap2MB));
        checkCalculateRNHeap(desc,
                             rnMaxHeap2MB,
                             policyMap, 1,
                             rnMaxHeap2MB);
        policyMap.clear();

        desc = "Bigger than maximum for compressed OOPs, compressed OOPs" +
            " requested explicitly";
        policyMap.setParameter(
            ParameterState.JVM_MISC,
            "-XX:-UseCompressedOops -XX:+UseCompressedOops -Xblah");
        checkCalculateRNHeap(desc, maxCompressedMB, policyMap, 1,
                             maxCompressedMB + 1);
        policyMap.clear();

        desc = "Bigger than maximum for compressed OOPs, max heap size" +
            " specified explicitly";
        policyMap.setParameter(
            ParameterState.JVM_MISC, "-Xmx" + (maxCompressedMB + 1) + "m");
        checkCalculateRNHeap(desc, maxCompressedMB + 1, policyMap, 1, 1000);
        policyMap.clear();

        desc = "Very large memory requested explicitly";
        int big = 10 * 1024 * 1024; /* 10 TB */
        policyMap.setParameter(ParameterState.SN_RN_HEAP_MAX_MB,
                               Integer.toString(big));
        checkCalculateRNHeap(desc, big, policyMap, 1, big);
    }

    /**
     * Test parameter validation
     */
    @Test
    public void testParameterValidation() {
        StorageNodeParams snp = new StorageNodeParams("hostname", 5000, "");
        GlobalParams gp = new GlobalParams("Storename");
        snp.getMap().setParameter( ParameterState.COMMON_MGMT_CLASS,"ava");
        StorageNodeAgent.checkSNParams(snp.getMap(),gp.getMap());
    }

    @Test
    public void testExplicitCacheSizeLimits() {
        /* These checks can be tested under Zing (and other JVMs). */
        final ParameterMap policyMap = new ParameterMap();

        /*
         * Bump memoryMB for non-RN memory. Assume SN is hosting admin for now,
         * because this is assumed by calculateRNHeapAndCache.
         */
        final int nonRNHeapMB = StorageNodeParams.getNonRNHeapMB(0, true);

        String desc =
            "Cache size is reduced when it is greater than 90% of heap";
        /* Set cache size to 9500 MB */
        policyMap.setParameter(ParameterState.JE_CACHE_SIZE,
            String.valueOf(9500L * 1024L * 1024L));
        /* Check that cache size is reduced to 90% of 10000 MB heap size */
        checkCalculateRNCache(
            desc, 9000L * 1024L * 1024L, policyMap, 1, 10000 + nonRNHeapMB);

        desc = "Cache size is reduced when it is greater than heap";
        /* Set cache size to 8000 MB */
        policyMap.setParameter(ParameterState.JE_CACHE_SIZE,
            String.valueOf(8000L * 1024L * 1024L));
        /* Check that cache size is reduced to 90% of 4000 MB heap size */
        checkCalculateRNCache(
            desc, 3600L * 1024L * 1024L, policyMap, 1, 4000 + nonRNHeapMB);
    }

    private void checkCalculateRNHeap(String description,
                                      int expectedHeapMB,
                                      ParameterMap policyMap,
                                      int numRNs,
                                      int memoryMB) {
        assertEquals(description,
                     expectedHeapMB,
                     StorageNodeParams.calculateRNHeapAndCache(
                         policyMap,
                         numRNs /* capacity */,
                         numRNs /* numRNsOnSN */,
                         memoryMB,
                         /*
                          * Use 100% so that the memory and Java heap sizes are
                          * the same, for simplicity
                          */
                         100 /* rnHeapPercent */,
                         70 /* rnCachePercent */,
                         0 /* numArbs */)
                     .getHeapMB());
    }

    private void checkCalculateRNCache(String description,
                                       long expectedCacheBytes,
                                       ParameterMap policyMap,
                                       int numRNs,
                                       int memoryMB) {
        assertEquals(description,
                     expectedCacheBytes,
                     StorageNodeParams.calculateRNHeapAndCache(
                         policyMap,
                         numRNs /* capacity */,
                         numRNs /* numRNsOnSN */,
                         memoryMB,
                         /*
                          * Use 100% so that the memory and Java heap sizes are
                          * the same, for simplicity
                          */
                         100 /* rnHeapPercent */,
                         70 /* rnCachePercent */,
                         0 /* numArbs */)
                     .getCacheBytes());
    }
}
