/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.TestBase;
import oracle.kv.impl.rep.table.TableManager.IDBytesComparator;

import org.junit.Test;

public class GetTableTest extends TestBase {

    /**
     * Tests fix for [#27199], LOG_FILE_NOT_FOUND due to incorrect record
     * extinction filter. Also more generally exercises the IDBytesComparator
     * that was the source of this bug.
     */
    @Test
    public void testIDBytesComparator()
        throws UnsupportedEncodingException {

        final Comparator<byte[]> comp = new IDBytesComparator();
        final Map<byte[], String> map2Str = new TreeMap<>(comp);

        /*
         * First test the specific scenario that caused [#27199]. The
         * comparison was not stopping at the delimiter (the zero after 118)
         * so the given key (below) was considered greater than the {118,
         * -62, -128} key (above), causing the wrong branch to be taken in the
         * map tree search, and the {118} key was not found.
         */
        map2Str.clear();
        map2Str.put(new byte[] {118}, "118");
        map2Str.put(new byte[] {118, 82}, "118.82");
        map2Str.put(new byte[] {118, -62, -128}, "118.-62.-128");
        assertEquals("118", map2Str.get(new byte[] {118, 0, 1, 2, 3}));

        /*
         * Test other ad-hoc scenarios for good measure.
         */
        assertEquals(null, map2Str.get(new byte[] {117, 0, 1, 2, 3}));
        assertEquals(null, map2Str.get(new byte[] {119, 0, 1, 2, 3}));
        assertEquals(null, map2Str.get(new byte[] {118, 1, 0, 1, 2, 3}));
        assertEquals("118", map2Str.get(new byte[] {118, 0, 1, 2, 3}));

        /*
         * Same as above but with a two byte ID key.
         */
        map2Str.clear();
        map2Str.put(new byte[] {90, 91}, "90-91");
        map2Str.put(new byte[] {90, 91, 92}, "90.91.92");
        map2Str.put(new byte[] {90, -91, -128}, "90.-90.-128");

        assertEquals(null, map2Str.get(new byte[] {90, 90, 0, 1, 2, 3}));
        assertEquals(null, map2Str.get(new byte[] {92, 90, 0, 1, 2, 3}));
        assertEquals(null, map2Str.get(new byte[] {90, 91, 1, 0, 1, 2, 3}));
        assertEquals("90-91", map2Str.get(new byte[] {90, 91, 0, 1, 2, 3}));

        /*
         * Do a more exhaustive test of the first ID_MAX table IDs
         */
        final int ID_MAX = 10000;
        final Map<byte[], byte[]> map2Full = new TreeMap<>(comp);
        final byte[][] allIdBytes = new byte[ID_MAX][];
        final byte[][] allFullKeys = new byte[ID_MAX][];
        final byte[] suffix = {0, 1, 2, 3};

        for (int id = 1; id <= ID_MAX; ++id) {

            final String str = TableImpl.createIdString(id);
            final byte[] idBytes = str.getBytes("UTF8");
            final byte[] fullKey = new byte[idBytes.length + suffix.length];

            System.arraycopy(idBytes, 0, fullKey, 0, idBytes.length);
            System.arraycopy(suffix, 0, fullKey, idBytes.length, suffix.length);

            map2Full.put(idBytes, fullKey);
            assertEquals(id, map2Full.size());

            allIdBytes[id - 1] = idBytes;
            allFullKeys[id - 1] = fullKey;

            /*
             * Make sure that each key can be found in the map when used as
             * the table ID in a full key. When run with the bad comparator,
             * this can also be used to find the lowest table ID where the
             * problem occurs. This checks all ID_MAX states of the map.
             */
            for (int i = 0; i < id; ++i) {

                final byte[] nthIdBytes = allIdBytes[i];
                final byte[] nthFullKey = allFullKeys[i];
                final byte[] gotBytes = map2Full.get(nthFullKey);

                if (!Arrays.equals(nthFullKey, gotBytes)) {
                    fail("Lookup by full key did not find entry" +
                        " id=" + id +
                        " idBytes=" + Arrays.toString(idBytes) +
                        " nthIdBytes=" + Arrays.toString(nthIdBytes) +
                        " nthFullKey=" + Arrays.toString(nthFullKey) +
                        " gotBytes=" + Arrays.toString(gotBytes));
                }
            }

            /*
             * Call the comparator directly to test it outside of the
             * TreeMap code path.
             */
            assertEquals(0, comp.compare(idBytes, fullKey));
            assertEquals(0, comp.compare(fullKey, idBytes));

            /*
             * Check that the key we added is unequal to all keys previously
             * added. This checks comparisons for all permutations of key
             * pairs. We don't check specifically for less or greater here
             * because the ordering of the idBytes keys in the map is not
             * the same as the integer table ID order.
             */
            for (int i = 0; i < id - 1; ++i) {

                final byte[] nthIdBytes = allIdBytes[i];
                final byte[] nthFullKey = allFullKeys[i];

                assertTrue(comp.compare(idBytes, nthIdBytes) != 0);
                assertTrue(comp.compare(nthIdBytes, idBytes) != 0);

                assertTrue(comp.compare(idBytes, nthFullKey) != 0);
                assertTrue(comp.compare(nthFullKey, idBytes) != 0);

                assertTrue(comp.compare(fullKey, nthIdBytes) != 0);
                assertTrue(comp.compare(nthIdBytes, fullKey) != 0);

                assertTrue(comp.compare(fullKey, nthFullKey) != 0);
                assertTrue(comp.compare(nthFullKey, fullKey) != 0);
            }
        }
    }
}
