/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import oracle.kv.impl.api.KeySerializer;

import org.junit.Test;

/**
 * Tests Key class in isolation.
 */
public class KeyTest extends TestBase {

    /**
     * Keys listed in proper sort order.  Each string is a component of the
     * major or minor path and a null divides the two paths.
     */
    private static final String[][] orderedKeys = {
        new String[] { "" , null },
        new String[] { "", "A", null },
        new String[] { "", "A", null, "" },
        new String[] { "", "A", null, "A" },
        new String[] { "", "A", "", null },
        new String[] { "", "A", "", null, "" },
        new String[] { "", "A", "", null, "", "A" },
        new String[] { "", "A", "", null, "A" },
        new String[] { "", "A", "", null, "A", "" },
        new String[] { "A", null },
        new String[] { "A", null, "" },
        new String[] { "A", null, "A" },
        new String[] { "A", null, "B" },
        new String[] { "A", null, "B", "D" },
        new String[] { "A", null, "D" },
        new String[] { "A", "", null },
        new String[] { "A", "", "", null },
        new String[] { "A", "A", null },
        new String[] { "A", "B", null },
        new String[] { "A", "B", null, "C" },
        new String[] { "A", "B", null, "C", "D" },
        new String[] { "A", "B", "A", null },
        new String[] { "A", "C", null, "" },
        new String[] { "B", null },
    };

    private static final String[] keyRangeValues = {
        null, /* infinity */
        "",
        "alpha",
        "beta"
    };

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    /**
     * Tests Key.toByteArray and fromByteArray.  Via fromByteArray, tests the
     * Key.createKey signatures, which are all represented by orderedKeys.
     * Tests Key.equals and compareTo for equal keys. Tests getMajorPathLength.
     */
    @Test
    public void testBinarySerialization() {
        for (final Key k : getOrderedKeys()) {
            final String m = k.toString();

            /* Serialize and check byte length. */
            final byte[] b = k.toByteArray();
            assertEquals(m, expectedByteLen(k), b.length);

            /* Deserialize and check copy for equality. */
            final Key k2 = Key.fromByteArray(b);
            assertEquals(m, k, k2);
            assertEquals(m, 0, k.compareTo(k2));

            /* Serialize copy and check bytes for equality. */
            final byte[] b2 = k2.toByteArray();
            assertTrue(m, Arrays.equals(b, b2));

            /* Serialize major path only and check length. */
            final Key k3 = Key.createKey(k.getMajorPath());
            final byte[] b3 = k3.toByteArray();
            assertEquals(m, b3.length, Key.getMajorPathLength(b));
            assertEquals(m, b3.length, Key.getMajorPathLength(b3));
        }

        /* Test a long key whose length exceeds the limitation */
        String keyString = "";
        for (int i=0; i<Short.MAX_VALUE+1; i++) {
            keyString += "a";
        }
        Key key = Key.createKey(keyString);
        FaultException e = null;
        try {
            key.toByteArray();
        } catch(FaultException fe) {
            e = fe;
        }
        assertNotNull(e);
        assertTrue(e.getMessage().contains("exceeds maximum key"));
    }

    /**
     * Returns number of serialized bytes for given key. Assumes that each char
     * corresponds to only one UTF-8 byte.
     */
    private int expectedByteLen(Key k) {
        int len = 0;
        for (final String s : k.getMajorPath()) {
            len += s.length();
        }
        for (String s : k.getMinorPath()) {
            len += s.length();
        }
        len += k.getMajorPath().size() + k.getMinorPath().size() - 1;
        return len;
    }

    /**
     * Tests Key.toString and fromString.
     */
    @Test
    public void testStringSerialization() {
        for (final Key k : getOrderedKeys()) {
            /* Serialize. */
            final String s = k.toString();

            /* Deserialize and check copy for equality. */
            final Key k2 = Key.fromString(s);
            assertEquals(s, k, k2);
            assertEquals(s, 0, k.compareTo(k2));

            /* Serialize copy and check string for equality. */
            final String s2 = k2.toString();
            assertEquals(s, s2);
        }

        for (int c = 0; c <= Character.MAX_VALUE; c += 1) {
            /* Check that any character can appear in a path component. */
            final char ch = (char) c;
            final String path = "" + ch + "xxx" + ch + "xxx" + ch;
            final Key k = Key.createKey(path);
            final String s = k.toString();
            final Key k2 = Key.fromString(s);
            assertEquals(k, k2);

            /* Encoding should occur as prescribed. */
            if (shouldBeEncoded(ch)) {
                assertTrue(s, s.length() >= 15);
            } else {
                assertEquals(s, 10, s.length());
            }
        }

        /* Test example path from javadoc. */
        final String s = "/HasEncodedSlash:%2F,Zero:%00,AndSpace:%20";
        final Key k = Key.fromString(s);
        final String s2 = k.toString();
        assertEquals(s, s2);
    }

    @SuppressWarnings("unused")
    @Test
    public void testKeyRangeConstructionChecks() {
        /* start must be < end */
        try {
            new KeyRange("beta", true, "alpha", true);
            fail("Expected exception");
        } catch (IllegalArgumentException IAE) {
            // Expected
        }

        /* At least one of start or end must be passed to KeyRange(). */
        try {
            new KeyRange(null, true, null, true);
            fail("Expected exception");
        } catch (IllegalArgumentException IAE) {
            // Expected
        }
    }

    /**
     * Tests KeyRange.toString and fromString.
     */
    @Test
    public void testKeyRangeStringSerialization() {

        /* Negative testing. */
        String[] failureCases = {
            "I/alpha", "E/alpha", "alpha/I", "alpha/E",
            "I/alpha/E", "I/alpha/I", "I/alpha/beta", "E/alpha/beta",
            "I/alpha/beta/", "E/alpha/beta/",
            "/alpha/beta/I", "/alpha/beta/E", null, "", "/", "IXalpha/"
        };

        for (final String failure : failureCases) {
            try {
                KeyRange.fromString(failure);
                fail("expected exception for " + failure);
            } catch (IllegalArgumentException IAE) {
                /* ignore */
            }
        }

        /* Positive testing. */
        for (final String v1 : keyRangeValues) {
            for (final String v2 : keyRangeValues) {
                if (v1 == v2 ||
                    (v1 != null && v2 != null && (v1.compareTo(v2) >= 0))) {

                    /*
                     * KR's with null, null are not allowed.  It's probably not
                     * worth testing cases where v1 == v2 either, but it costs
                     * us next to nothing.
                     */
                    continue;
                }

                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < 2; j++) {
                        boolean startInclusive = (i == 0);
                        boolean endInclusive = (j == 0);
                        KeyRange kr = new KeyRange(v1, startInclusive,
                                                   v2, endInclusive);
                        String s = kr.toString();
                        assertTrue(s /* message */,
                                   kr.equals(KeyRange.fromString(s)));
                    }
                }
            }
        }
    }

    /**
     * See Key.toString javadoc.
     */
    private static final char[] ENCODED_ASCII = {
        0x22, 0x23, 0x25, 0x2F, 0x3C, 0x3E, 0x3F, 0x5B, 0x5C, 0x5D, 0x5E, 0x60,
        0x7B, 0x7C, 0x7D,
    };

    /**
     * See Key.toString javadoc.
     */
    private boolean shouldBeEncoded(char ch) {
        return (Arrays.binarySearch(ENCODED_ASCII, ch) >= 0) ||
               Character.isISOControl(ch) ||
               Character.isSpaceChar(ch);
    }

    /**
     * Tests Key.compareTo and Key.equals for unequal keys.  Ensures that the
     * comparator used for database keys gives the same result as
     * Key.compareTo.
     */
    @Test
    public void testCollation() {
        Key prev = null;
        for (final Key k : getOrderedKeys()) {
            final String m = k.toString();
            if (prev != null) {
                assertTrue(m, k.compareTo(prev) > 0);
                assertFalse(m, k.equals(prev));
                assertTrue(m, new Key.BytesComparator().compare
                              (k.toByteArray(), prev.toByteArray()) > 0);
            }
            prev = k;
        }
    }

    /**
     * Returns orderedKeys as a collection of Keys.
     */
    private Collection<Key> getOrderedKeys() {
        Collection<Key> keys = new ArrayList<Key>(orderedKeys.length);
        for (final String[] a : orderedKeys) {
            keys.add(stringArrayToKey(a));
        }
        return keys;
    }

    /**
     * Converts one element of orderedKeys to a Key.  Tests Key.getMajorPath
     * and getMinorPath.
     */
    private Key stringArrayToKey(String[] a) {
        final List<String> majorPath = new ArrayList<String>();
        final List<String> minorPath = new ArrayList<String>();
        boolean majorDone = false;
        for (final String s : a) {
            if (s == null) {
                assertFalse(majorDone);
                majorDone = true;
                continue;
            }
            if (majorDone) {
                minorPath.add(s);
            } else {
                majorPath.add(s);
            }
        }
        final Key k = Key.createKey(majorPath, minorPath);
        assertEquals(majorPath, k.getMajorPath());
        assertEquals(minorPath, k.getMinorPath());
        return k;
    }

    @Test
    public void testIsPrefix() {
        final Key k1 = Key.createKey("x");
        final Key k2 = Key.createKey("x", "y");
        final Key k3 = Key.createKey("x", Arrays.asList("y", "z"));
        final Key k4 = Key.createKey(Arrays.asList("x", "y"), "z");
        final Key k5 = Key.createKey(Arrays.asList("x", "y"),
                                     Arrays.asList("z", "a"));

        assertTrue(k1.isPrefix(k1));
        assertTrue(k1.isPrefix(k2));
        assertTrue(k1.isPrefix(k3));
        assertTrue(k1.isPrefix(k4));
        assertTrue(k1.isPrefix(k5));

        assertTrue(!k2.isPrefix(k1));
        assertTrue(k2.isPrefix(k2));
        assertTrue(k2.isPrefix(k3));
        assertTrue(!k2.isPrefix(k4));
        assertTrue(!k2.isPrefix(k5));

        assertTrue(!k3.isPrefix(k1));
        assertTrue(!k3.isPrefix(k2));
        assertTrue(k3.isPrefix(k3));
        assertTrue(!k3.isPrefix(k4));
        assertTrue(!k3.isPrefix(k5));

        assertTrue(!k4.isPrefix(k1));
        assertTrue(!k4.isPrefix(k2));
        assertTrue(!k4.isPrefix(k3));
        assertTrue(k4.isPrefix(k4));
        assertTrue(k4.isPrefix(k5));

        assertTrue(!k5.isPrefix(k1));
        assertTrue(!k5.isPrefix(k2));
        assertTrue(!k5.isPrefix(k3));
        assertTrue(!k5.isPrefix(k4));
        assertTrue(k5.isPrefix(k5));
    }

    @Test
    public void testIllegalKeys() {

        /* zero components */
        try {
            Key.createKey(new ArrayList<String>());
            fail();
        } catch (IllegalArgumentException expected) {
        }

        /* null component in major path */
        try {
            Key.createKey(Arrays.asList((String) null), Arrays.asList("a"));
            fail();
        } catch (IllegalArgumentException expected) {
        }

        /* null component in minor path */
        try {
            Key.createKey(Arrays.asList("a"), Arrays.asList((String) null));
            fail();
        } catch (IllegalArgumentException expected) {
        }

        /* string key does not being with slash */
        try {
            Key.fromString("a/b");
            fail();
        } catch (IllegalArgumentException expected) {
        }

        /* string key has illegal URI path */
        try {
            Key.fromString("/%%");
            fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testInternalKeys() {
        final Key ikey =
            stringArrayToKey(new String[] { "" , "major", null, "minor"});

        final KeySerializer ks = KeySerializer.ALLOW_INTERNAL_KEYSPACE;

        final Collection<Key> testKeys = getOrderedKeys();

        /* Ensure there is at least one internal key in the test set. */
        testKeys.add(ikey);

        /*
         * Check that the two methods are equivalent on an interesting
         * test set of keys.
         */
        for (Key k : getOrderedKeys()) {

            boolean isInternal = k.keySpaceIsInternal();

            byte kb[] = ks.toByteArray(k);

            assertEquals(isInternal, Key.keySpaceIsInternal(kb));
        }
    }
}
