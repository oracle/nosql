/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import static oracle.kv.impl.util.TestUtils.NULL_PRINTSTREAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Durability;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.Version;

/** Test DataCheckKV and DataCheckMain. */
public class DataCheckTestJUnit extends JUnitTestBase {

    private static final boolean VERBOSE = Boolean.getBoolean("verbose");

    private static final KVStoreConfig MOCK_CONFIG =
        new KVStoreConfig("kvstore", "localhost:1");

    /** A start or count value that is too large. */
    private static final long TOO_MANY_BLOCKS =
        DataCheckKV.MAX_INDEX + DataCheckKV.BLOCK_COUNT;

    /* Tests */

    @Test
    public void testMainNoArgs() {
        testMain(false);
    }

    @Test
    public void testMainStore() {
        testMain(false, "-store");
    }

    @Test
    public void testMainHost() {
        testMain(false, "-host");
    }

    @Test
    public void testMainPort() {
        testMain(false, "-port");
    }

    @Test
    public void testMainPhase() {
        testMain(false, "-phase");
    }

    @Test
    public void testExerciseIndexToKeynum() {
        DataCheckKV dc = createDataCheck();
        long index = 0xb5;
        long keynum = dc.exerciseIndexToKeynum(index, true);
        long indexAgain = dc.keynumToExerciseIndex(keynum, true);
        assertEquals(index, indexAgain);
    }

    @Test
    public void testUseLOBKeynum() {
        DataCheckKV dc = createDataCheck();
        /* Should use the same seed and LOB percent as in createDataCheck. */
        long seed = 42;
        double lobPercent = 0.01;
        assertTrue(dc.useLOB(getRandomKeynum(seed, lobPercent,
                                             true /* isLOB */)));
        assertFalse(dc.useLOB(getRandomKeynum(seed, lobPercent,
                                              false /* isLOB*/)));
    }

    @Test
    public void testKeynumToLOBKey() {
        DataCheckKV dc = createDataCheck();
        /* Should use the same seed and LOB percent as in createDataCheck. */
        long seed = 42;
        double lobPercent = 0.01;
        long keynum = getRandomKeynum(seed, lobPercent, true /* isLOB */);
        Key lobKey = dc.keynumToKey(keynum);
        assertTrue(lobKey.getMinorPath().get(0).endsWith(".lob"));
    }

    @Test
    public void testGetLOBPattern() {
        DataCheckKV dc = createDataCheck();
        String exePattern = "ea-index=0x00000000ea";
        String popPattern = "pp-index=0x00000000ea";
        String popVal = dc.getLOBPattern(
            new DataCheckKV.RepeatedInputStream(dc.lobSize, popPattern));
        String exeVal = dc.getLOBPattern(
            new DataCheckKV.RepeatedInputStream(dc.lobSize, exePattern));
        assertEquals(exePattern, exeVal);
        assertEquals(popPattern, popVal);
    }

    @Test(expected=NullPointerException.class)
    @SuppressWarnings("unused")
    public void testConstructorNullStore() {
        new DataCheckKV(null, MOCK_CONFIG, 0, null, -1, 0, NULL_PRINTSTREAM, 0,
            0, 0, false, 0, 0, 0, 0);
    }

    @Test(expected=NullPointerException.class)
    @SuppressWarnings("unused")
    public void testConstructorNullPrintStream() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0, null, 0,
            0, 0, false, 0, 0, 0, 0);
    }

    /*
     * Intentionally not calling DataCheckKV.populate, the test lies in the
     * constructor code.
     */
    @SuppressWarnings("unused")
    @Test(expected=IllegalArgumentException.class)
    public void testLOBOpTimeoutNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, -1, 0, false, 0, 0, 0, 0);
    }

    /*
     * Intentionally not calling DataCheckKV.populate, the test lies in the
     * constructor code.
     */
    @SuppressWarnings("unused")
    @Test(expected=IllegalArgumentException.class)
    public void testLOBOpPercentageNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, -1, false, 0, 0, 0, 0);
    }

    /*
     * Intentionally not calling DataCheckKV.populate, the test lies in the
     * constructor code.
     */
    @SuppressWarnings("unused")
    @Test(expected=IllegalArgumentException.class)
    public void testLOBOpPercentageTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, 100.1, false, 0, 0, 0, 0);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateStartNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(-DataCheckKV.BLOCK_COUNT, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateStartNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(37, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateStartTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(TOO_MANY_BLOCKS - DataCheckKV.BLOCK_COUNT,
                      DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateCountNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(0, -DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateCountNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(0, 3 * DataCheckKV.BLOCK_COUNT, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPopulateCountTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                      NULL_PRINTSTREAM, 0, 0, 0, false, 0 ,0, 0, 0)
            .populate(2, TOO_MANY_BLOCKS - (2 * DataCheckKV.BLOCK_COUNT),
                      2 * DataCheckKV.BLOCK_COUNT);
    }

    @Test
    public void testPopulateAgain() {
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                          NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        dc.populate(0, 0, 1);
        try {
            dc.populate(0, 0, 1);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void testPopulateFails() {
        MockStore store = new MockStore() {
            @Override
            public SortedSet<Key> multiGetKeys(Key parentKey,
                                               KeyRange subRange,
                                               Depth depth,
                                               Consistency consistency,
                                               long timeout,
                                               TimeUnit timeoutUnit) {
                throw new UnsupportedOperationException();
            }
        };
        DataCheckKV dc = new DataCheckKV(store, MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        try {
            dc.populate(0, DataCheckKV.BLOCK_COUNT, 1);
            fail("Expected TestFailedException");
        } catch (DataCheckKV.TestFailedException e) {
        }
    }

    /** Test that the parent key is not truncated to 16 bits [#22133]. */
    @Test
    public void testPopulateBigStart() {
        CapturePutMockStore store = new CapturePutMockStore() {
            /** Key is /m/populateBlock/BLOCK/... */
            @Override
            boolean matchKey(Key key) {
                List<String> majorPath = key.getMajorPath();
                return majorPath.size() == 3 &&
                    "m".equals(majorPath.get(0)) &&
                    "populateBlock".equals(majorPath.get(1));
            }
        };
        DataCheckKV dc = new DataCheckKV(store, MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        /* Use the largest legal block number. */
        long startBlock = (DataCheckKV.MAX_INDEX / DataCheckKV.BLOCK_COUNT) - 1;
        dc.populate(startBlock * DataCheckKV.BLOCK_COUNT,
                    DataCheckKV.BLOCK_COUNT, 1);
        assertEquals(startBlock,
                     Long.parseLong(store.putKey.getMajorPath().get(2), 16));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseStartNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(-DataCheckKV.BLOCK_COUNT, 0, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseStartNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(37, 0, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseStartTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(TOO_MANY_BLOCKS, DataCheckKV.BLOCK_COUNT, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseCountNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, -DataCheckKV.BLOCK_COUNT, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseCountNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, 37, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseCountTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, TOO_MANY_BLOCKS, 1, 50, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseReadPercentNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, 0, 1, -10, false);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExerciseReadPercentTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, 0, 1, 110, false);
    }

    @Test
    public void testExerciseAfterPopulate() {
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        dc.populate(0, 0, 1);
        try {
            dc.exercise(0, 0, 1, 50, false);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void testExerciseFails() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(0, 0, 1);
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        try {
            dc.exercise(0, DataCheckKV.BLOCK_COUNT, 1, 50, false);
            fail("Expected TestFailedException");
        } catch (DataCheckKV.TestFailedException e) {
        }
    }

    /** Test that the parent key is not truncated to 16 bits [#22133]. */
    @Test
    public void testExerciseBigStart() {
        CapturePutMockStore store = new CapturePutMockStore() {
            /** Key is /m/exerciseBlock/[AB]/BLOCK/... */
            @Override
            boolean matchKey(Key key) {
                List<String> majorPath = key.getMajorPath();
                return majorPath.size() == 4 &&
                    "m".equals(majorPath.get(0)) &&
                    "exerciseBlock".equals(majorPath.get(1));
            }
        };
        DataCheckKV dc = new DataCheckKV(store, MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        /* Use the largest legal block number. */
        long startBlock = (DataCheckKV.MAX_INDEX / DataCheckKV.BLOCK_COUNT) - 1;
        try {
            dc.exercise(startBlock * DataCheckKV.BLOCK_COUNT,
                        DataCheckKV.BLOCK_COUNT, 1, 50, false);
            fail("Expected TestFailedException");
        } catch (DataCheckKV.TestFailedException e) {
        }
        assertEquals(startBlock,
                     Long.parseLong(store.putKey.getMajorPath().get(3), 16));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckStartNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(-DataCheckKV.BLOCK_COUNT, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckStartNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(37, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckStartTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(TOO_MANY_BLOCKS, DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckCountNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(0, -DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckCountNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(0, 37, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCheckCountTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .check(0, TOO_MANY_BLOCKS, 1);
    }

    @Test
    public void testCheckAgain() {
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        dc.check(0, 0, 1);
        try {
            dc.check(0, 0, 1);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void testCheckFails() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .populate(0, 0, 1);
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .exercise(0, 0, 1, 50, true /* noCheck */);
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        try {
            dc.check(0, DataCheckKV.BLOCK_COUNT, 1);
            fail("Expected TestFailedException");
        } catch (DataCheckKV.TestFailedException e) {
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanStartNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(-DataCheckKV.BLOCK_COUNT, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanStartNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(37, 0, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanStartTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(TOO_MANY_BLOCKS - DataCheckKV.BLOCK_COUNT,
                      DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanCountNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(0, -DataCheckKV.BLOCK_COUNT, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanCountNotMultiple() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(0, 3 * DataCheckKV.BLOCK_COUNT, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCleanCountTooLarge() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0)
            .clean(2, TOO_MANY_BLOCKS - (2 * DataCheckKV.BLOCK_COUNT),
                      2 * DataCheckKV.BLOCK_COUNT);
    }

    @Test
    public void testCleanAgain() {
        DataCheckKV dc =
            new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        dc.clean(0, 0, 1);
        try {
            dc.clean(0, 0, 1);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void testCleanFails() {
        MockStore store = new MockStore() {
            @Override
            public SortedSet<Key> multiGetKeys(Key parentKey,
                                               KeyRange subRange,
                                               Depth depth,
                                               Consistency consistency,
                                               long timeout,
                                               TimeUnit timeoutUnit) {
                throw new UnsupportedOperationException();
            }
        };
        DataCheckKV dc = new DataCheckKV(store, MOCK_CONFIG, 0, null, -1, 0,
            NULL_PRINTSTREAM, 0, 0, 0, false, 0, 0, 0, 0);
        try {
            dc.clean(0, DataCheckKV.BLOCK_COUNT, 1);
            fail("Expected TestFailedException");
        } catch (DataCheckKV.TestFailedException e) {
        }
    }

    /* Instantiating the DataCheckKV instance executes the test */
    @SuppressWarnings("unused")
    @Test(expected=IllegalArgumentException.class)
    public void testScanMaxConcurrentRequestsNegative() {
        new DataCheckKV(new MockStore(), MOCK_CONFIG, 0, null, -1, 0,
                        NULL_PRINTSTREAM, 0, 0, 0, false, -1, 0, 0, 0);
    }

    /* Misc */

    static void testMain(boolean expectedResult, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            DataCheckMain.mainNoExit(new PrintStream(out), args);
            assertTrue(expectedResult);
        } catch (Throwable t) {
            assertFalse(expectedResult);
        } finally {
            if (VERBOSE) {
                System.out.println(out);
            }
        }
    }

    static DataCheckKV createDataCheck() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            return new DataCheckKV(new MockStore(),
                                   MOCK_CONFIG,
                                   5000 /* reportingInterval */,
                                   null /* partitions */,
                                   42 /* seed */,
                                   0 /* verbose */,
                                   new PrintStream(out) /* err */,
                                   0 /* block timeout */,
                                   0 /* LOB op timeout */,
                                   0.01 /* LOB percentage */,
                                   false /* do not use parallel scan */,
                                   0 /* scan max concurrent requests */,
                                   0 /* scan max results batches */,
                                   0 /* target client throughput */,
                                   0 /* max execution time */);
        } finally {
            if (VERBOSE) {
                System.out.println(out);
            }
        }
    }

    static long getRandomKeynum(long randomSeed,
                                double lobPercent,
                                boolean isLOB) {
        /*
         * Get the corresponding random number used by @code{permuteLOBKeynum}
         * in @link{DataCheckKV}.
         */
        Random random = new Random(randomSeed);
        long permuteSeed = 0l;
        for (int i = 0; i < 3; i++) {
            permuteSeed = random.nextLong();
        }
        /*
         * Get a 48 bit number randomly as the permuted keynum, and reserve the
         * higher 40 bits as base.
         */
        long permutedKeynumBase = random.nextLong() & 0xffffffff0000l;
        long lobThreshold = permutedKeynumBase +
                            (long)(DataCheckKV.BLOCK_MASK * lobPercent * 0.01);
        /*
         * A permuted keynum below the lobThreshold indicates it as a LOB and
         * vice verser.
         */
        Permutation p = new Permutation(permuteSeed);
        return isLOB ?
               p.untransformSixByteLong(lobThreshold - 1) :
               p.untransformSixByteLong(lobThreshold + 1);
    }

    /** A KVStore that captures the first matching put key. */
    static abstract class CapturePutMockStore extends MockStore {
        volatile Key putKey = null;
        abstract boolean matchKey(Key key);
        @Override
        public Version put(Key key,
                           Value value,
                           ReturnValueVersion prevValue,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit) {
            if (putKey == null && matchKey(key)) {
                putKey = key;
            }
            return null;
        }
    }
}
