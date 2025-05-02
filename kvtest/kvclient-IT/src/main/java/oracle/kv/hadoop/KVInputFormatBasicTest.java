/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Consistency.Time;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.ClientTestBase;
import oracle.kv.impl.util.FilterableParameterized;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for the ONDB/Hadoop InputFormat.
 */
@SuppressWarnings("deprecation")
@RunWith(FilterableParameterized.class)
public class KVInputFormatBasicTest extends ClientTestBase {

    /* The explicit timeout associated with requests. */
    private static final int N_MAJOR_KEYS = 10;
    private static final int N_MINOR_KEYS_PER_MAJOR_KEY = 3;
    private static final int N_RECS =
        N_MAJOR_KEYS * (N_MINOR_KEYS_PER_MAJOR_KEY + 1);

    public KVInputFormatBasicTest(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp() throws Exception {
        /*
         *  This test does not create any tables, so no need to run
         *  multi-region table mode.
         */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        super.setUp();
    }

    /**
     * A single test method is used to avoid the overhead of service setup and
     * teardown for every test. Note that in order to continue avoiding this
     * overhead, whenever a new test case is added to this test, that new
     * test case should be executed from within this single method.
     */
    @Test
    public void testAll()
        throws Exception {

        if (secure) {
            KVInputFormat.setKVSecurity(AUTOLOGIN_FILE_PATH);
        }

        try {
            doTest(N_RECS, N_RECS, null, null, null,
                   new Consistency.Time(1, null, 1, null));
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) /* CHECKSTYLE:OFF */ {
            /* expected */
        }/* CHECKSTYLE:ON */

        doTest(N_RECS, N_RECS, null, null, null, null);
        doTest(N_MAJOR_KEYS, N_RECS, "0", null, Depth.CHILDREN_ONLY, null);
        doTest(N_MINOR_KEYS_PER_MAJOR_KEY + 1, N_RECS, "0",
               "1", null, null);
        /*
         * Verify the different consistency options.
         */
        testConsistency(Consistency.NONE_REQUIRED);
        testConsistency(Consistency.NONE_REQUIRED_NO_MASTER);
        testConsistency(Consistency.ABSOLUTE);
        try {
            final Time time = new Time(0, null, 0, null);
            testConsistency(time);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException IAE) /* CHECKSTYLE:OFF */ {
            /* Expected */
        }/* CHECKSTYLE:ON */

        doTestReadWriteTimeoutUnit();
    }

    private void putSomeData()
        throws Exception {

        final List<String> majKey = new ArrayList<String>();
        /* First major key component is always "0". */
        majKey.add(0, "0");
        majKey.add(1, "0"); // Placeholder. Will get overrwritten.
        for (int i = 0; i < N_MAJOR_KEYS; i++) {
            majKey.set(1, "" + i);
            final Key key = Key.createKey(majKey);
            final Value val = Value.createValue("xx".getBytes());
            store.put(key, val);
            for (int j = 1; j <= N_MINOR_KEYS_PER_MAJOR_KEY; j++) {
                final Key subKey =
                    Key.createKey(majKey, (j + N_MAJOR_KEYS) + "");
                final Value subVal =
                    Value.createValue(("" + i + (j + N_MAJOR_KEYS)).getBytes());
                store.put(subKey, subVal);
            }
        }
    }

    private int deleteData()
        throws Exception {

        int cnt = 0;
        final Iterator<Key> iter =
            store.storeKeysIterator(Direction.UNORDERED, 0);
        while (iter.hasNext()) {
            final Key key = iter.next();
            store.delete(key);
            cnt++;
        }
        return cnt;
    }

    private void testConsistency(Consistency consistency)
        throws Exception {

        final String storeName = config.getStoreName();
        final String[] helperHosts = config.getHelperHosts();
        KVInputFormat.setKVStoreName(storeName);
        KVInputFormat.setKVHelperHosts(helperHosts);
        KVInputFormat.setBatchSize(0);
        KVInputFormat.setTimeout(0);
        KVInputFormat.setTimeoutUnit(null);

        if (consistency != null) {
            KVInputFormat.setConsistency(consistency);
        }

        final KVInputFormat kvif = new KVInputFormat();
        final List<InputSplit> splits = kvif.getSplits(null);
        for (InputSplit split : splits) {
            verifyInputSplitWriteRead(split);
        }
    }

    private void doTestReadWriteTimeoutUnit()
        throws Exception {

        final String storeName = config.getStoreName();
        final String[] helperHosts = config.getHelperHosts();
        KVInputFormat.setKVStoreName(storeName);
        KVInputFormat.setKVHelperHosts(helperHosts);
        KVInputFormat.setTimeoutUnit(TimeUnit.SECONDS);

        final KVInputFormat kvif = new KVInputFormat();
        final List<InputSplit> splits = kvif.getSplits(null);
        for (InputSplit split : splits) {
            verifyInputSplitWriteRead(split);
        }
    }

    /**
     * Test putting some data and then retrieving it back using the
     * KVInputStream.
     */
    private void doTest(int expectedNRecsFound,
                        int expectedNRecsDeleted,
                        String parentKey,
                        String subRange,
                        Depth depth,
                        Consistency consistency)
        throws Exception {

        try {
            putSomeData();

            final String storeName = config.getStoreName();
            final String[] helperHosts = config.getHelperHosts();
            KVInputFormat.setKVStoreName(storeName);
            KVInputFormat.setKVHelperHosts(helperHosts);
            if (parentKey != null) {
                KVInputFormat.setParentKey(Key.createKey(parentKey));
            } else {
                KVInputFormat.setParentKey(null);
            }

            if (subRange != null) {
                KVInputFormat.setSubRange
                    (new KeyRange(subRange, true, subRange + "0", true));
            } else {
                KVInputFormat.setSubRange(null);
            }

            if (depth != null) {
                KVInputFormat.setDepth(depth);
            } else {
                KVInputFormat.setDepth(null);
            }

            if (consistency != null) {
                KVInputFormat.setConsistency(consistency);
            } else {
                KVInputFormat.setConsistency(null);
            }

            final KVInputFormat kvif = new KVInputFormat();
            final List<InputSplit> splits = kvif.getSplits(null);
            int cnt = 0;
            for (InputSplit split : splits) {
                assertEquals(1, split.getLength());
                verifyInputSplitWriteRead(split);
                final RecordReader<Text, Text> rr =
                    kvif.createRecordReader(split, null);
                while (rr.nextKeyValue()) {
                    cnt++;
                    final Key key =
                        Key.fromString(rr.getCurrentKey().toString());
                    final String val = rr.getCurrentValue().toString();
                    final List<String> majPath = key.getMajorPath();
                    final List<String> minPath = key.getMinorPath();
                    if (minPath.size() == 0) {
                        assertTrue("xx".equals(val));
                    } else {
                        assertEquals(majPath.size(), 2);
                        assertEquals(minPath.size(), 1);
                        assertEquals(majPath.toArray()[1] + "" +
                                     minPath.toArray()[0], val);
                    }
                }
                assertNull(rr.getCurrentKey());
                assertNull(rr.getCurrentValue());
                rr.close();
            }
            assertEquals(expectedNRecsFound, cnt);
        } finally {
            assertEquals(expectedNRecsDeleted, deleteData());
        }
    }

    private void verifyInputSplitWriteRead(InputSplit split) {

        try {
            final KVInputSplit kvis = (KVInputSplit) split;
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);
            kvis.write(oos);
            oos.close();
            final ByteArrayInputStream bais =
                new ByteArrayInputStream(baos.toByteArray());
            final ObjectInputStream ois = new ObjectInputStream(bais);
            final KVInputSplit kvis2 = new KVInputSplit();
            kvis2.readFields(ois);
            final String[] locs = kvis2.getLocations();
            final int nLocations = locs.length;
            for (int i = 0; i < nLocations; i += 1) {
                assertEquals(locs[i], kvis.getLocations()[i]);
            }
            final String[] hhosts = kvis2.getKVHelperHosts();
            final int nHosts = hhosts.length;
            for (int i = 0; i < nHosts; i += 1) {
                assertEquals(hhosts[i], kvis.getKVHelperHosts()[i]);
            }
            assertEquals(kvis.getDirection(), kvis2.getDirection());
            assertEquals(kvis.getBatchSize(), kvis2.getBatchSize());
            assertEquals(kvis.getParentKey(), kvis2.getParentKey());
            assertEquals(kvis.getSubRange(), kvis2.getSubRange());
            assertEquals((kvis.getDepth() == null ?
                          Depth.PARENT_AND_DESCENDANTS :
                          kvis.getDepth()), kvis2.getDepth());
            assertEquals(kvis.getConsistency(), kvis2.getConsistency());
            assertEquals(kvis.getTimeout(), kvis2.getTimeout());
            assertEquals(kvis.getTimeoutUnit(), kvis2.getTimeoutUnit());
            assertEquals(kvis.getKVStoreName(), kvis2.getKVStoreName());
            assertEquals(kvis.getKVPart(), kvis2.getKVPart());
        } catch (Exception E) {
            E.printStackTrace();
            fail("verifyInputSplitWriteRead threw: " + E);
        }
    }
}
