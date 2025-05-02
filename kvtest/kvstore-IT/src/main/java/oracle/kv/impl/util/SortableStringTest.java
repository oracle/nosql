/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import oracle.kv.TestBase;

import org.junit.Test;

public class SortableStringTest extends TestBase {
    final int testCases;
    final Random random;

    public SortableStringTest() {
        testCases = 9999999;
        random = new Random();
    }

    private static void cmpInt(int i1, int i2, int stringLen) {
        if (!(i1 <= i2)) {
            throw new RuntimeException("Out of order! i1: " + i1 + ", i2: "
                                       + i2);
        }
        String s1 = SortableString.toSortable(i1, stringLen);
        String s2 = SortableString.toSortable(i2, stringLen);
        if (!(s1.compareTo(s2) <= 0)) {
            throw new RuntimeException("Conversion not in order");
        }
        int t1 = SortableString.intFromSortable(s1);
        int t2 = SortableString.intFromSortable(s2);
        if (t1 != i1 || t2 != i2) {
            System.out.println("i1, i2, s1, s2, t1, t2: " +
                               i1 + ", " + i2 + ", " +
                               s1 + ", " + s2 + ", " + t1 + ", " + t2);
            throw new RuntimeException("Couldn't convert back to original");
        }
    }

    private static void cmpLong(long l1, long l2, int stringLen) {
        if (!(l1 <= l2)) {
            throw new RuntimeException("Out of order! l1: " + l1 + ", l2: "
                                       + l2);
        }
        String s1 = SortableString.toSortable(l1, stringLen);
        String s2 = SortableString.toSortable(l2, stringLen);
        if (!(s1.compareTo(s2) <= 0)) {
            throw new RuntimeException("Conversion not in order");
        }
        long t1 = SortableString.longFromSortable(s1);
        long t2 = SortableString.longFromSortable(s2);
        if (t1 != l1 || t2 != l2) {
            throw new RuntimeException("Couldn't convert back to original");
        }
    }

    private static void cmpDouble(double d1, double d2) {
        if (!(d1 <= d2)) {
            throw new RuntimeException("Out of order! d1: " + d1 + ", d2: "
                                       + d2);
        }
        String s1 = SortableString.toSortable(d1);
        String s2 = SortableString.toSortable(d2);
        if (!(s1.compareTo(s2) <= 0)) {
            throw new RuntimeException("Conversion not in order");
        }
        double t1 = SortableString.doubleFromSortable(s1);
        double t2 = SortableString.doubleFromSortable(s2);
        if (t1 != d1 || t2 != d2) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (Double.doubleToRawLongBits(t1) != Double.doubleToRawLongBits(d1)) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (Double.doubleToRawLongBits(t2) != Double.doubleToRawLongBits(d2)) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (inc(dec(d1)) != d1 || dec(inc(d1)) != d1) {
            throw new RuntimeException("Inc/Dec broken for d1: " + d1);
        }

        if (inc(dec(d2)) != d2 || dec(inc(d2)) != d2) {
            throw new RuntimeException("Inc/Dec broken for d2: " + d2);
        }
    }

    private static void cmpBytes(byte[] bytes1, byte[] bytes2) {
        String s1 = SortableString.toSortable(bytes1);
        String s2 = SortableString.toSortable(bytes2);
        if (s1.compareTo(s2) > 0) {
            throw new RuntimeException("Conversion not in order");
        }

        byte[] t1 = SortableString.bytesFromSortable(s1);
        byte[] t2 = SortableString.bytesFromSortable(s2);
        if (compareBytes(t1, bytes1) != 0 || compareBytes(t2, bytes2) != 0) {
            throw new RuntimeException("Couldn't convert back to original");
        }
    }

    private static double inc(double d) {
        if (d == -0.0) {
            return Double.MIN_VALUE;
        }
        long tmp = Double.doubleToRawLongBits(d);
        return Double.longBitsToDouble((tmp < 0) ? tmp - 1 : tmp + 1);
    }

    private static double dec(double d) {
        if (d == -0.0) {
            return -Double.MIN_VALUE;
        }
        long tmp = Double.doubleToRawLongBits(d);
        return Double.longBitsToDouble((tmp < 0) ? tmp + 1 : tmp - 1);
    }

    private static void cmpFloat(float f1, float f2) {
        if (!(f1 <= f2)) {
            throw new RuntimeException("Out of order! f1: " + f1 + ", f2: "
                                       + f2);
        }
        String s1 = SortableString.toSortable(f1);
        String s2 = SortableString.toSortable(f2);
        if (!(s1.compareTo(s2) <= 0)) {
            throw new RuntimeException("Conversion not in order");
        }
        float t1 = SortableString.floatFromSortable(s1);
        float t2 = SortableString.floatFromSortable(s2);
        if (t1 != f1 || t2 != f2) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (Float.floatToRawIntBits(t1) != Float.floatToRawIntBits(f1)) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (Float.floatToRawIntBits(t2) != Float.floatToRawIntBits(f2)) {
            throw new RuntimeException("Couldn't convert back to original");
        }

        if (inc(dec(f1)) != f1 || dec(inc(f1)) != f1) {
            throw new RuntimeException("Inc/Dec broken for f1: " + f1);
        }

        if (inc(dec(f2)) != f2 || dec(inc(f2)) != f2) {
            throw new RuntimeException("Inc/Dec broken for f2: " + f2);
        }
    }

    private static float inc(float f) {
        if (f == -0.0) {
            return Float.MIN_VALUE;
        }
        int tmp = Float.floatToRawIntBits(f);
        return Float.intBitsToFloat((tmp < 0) ? tmp - 1 : tmp + 1);
    }

    private static float dec(float f) {
        if (f == -0.0) {
            return -Float.MIN_VALUE;
        }
        int tmp = Float.floatToRawIntBits(f);
        return Float.intBitsToFloat((tmp < 0) ? tmp + 1 : tmp - 1);
    }

    private void testInteger(int min, int max, int numInts) {
        /*
         * Test special case integer values
         */
        int[] special_ints = { Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1,
                                 0, 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE };
        for (int i = 1; i < special_ints.length; i++) {
            cmpInt(special_ints[i - 1], special_ints[i],
                   SortableString.encodingLength(Integer.MAX_VALUE));
        }

        /*
         * Now test the range, plus random values in the specified range
         */
        int stringLen = SortableString.encodingLength(min, max);
        cmpInt(min, max, stringLen);
        int[] ints = new int[numInts + special_ints.length];
        System.arraycopy(special_ints, 0, ints, 0, special_ints.length);
        for (int i = special_ints.length; i < ints.length; i++) {
            int range = max - min + 2;
            ints[i] = random.nextInt(range) + min;
        }
        Arrays.sort(ints);
        for (int i = 1; i < ints.length; i++) {
            min = ints[i - 1];
            max = ints[i];
            stringLen = SortableString.encodingLength(min, max);
            cmpInt(min, max, stringLen);
        }
    }

    @Test
    public void testInteger() {

        /* Full range of Integer */
        testInteger(Integer.MIN_VALUE, Integer.MAX_VALUE, testCases);

        /* Some edge cases */
        testInteger(0x20, 0x30, 5);
        testInteger(0x800, 0x801, 5);
        testInteger(0x800000, 0x20000000, 99999);

        /* Some "random" ranges */
        testInteger(10000, 10000000, 2000);
        testInteger(1, 10, 20);
        testInteger(0, 10000, 200);
        testInteger(100, 10000, 200);
        testInteger(-5, 10, 20);
        testInteger(-100, 0, 20);
        testInteger(-100, -10, 20);
        testInteger(-1000, 10, 20);
        testInteger(-15, -10, 20);
        testInteger(-5, -2, 20);

        /*
         * Look for changes in encoding length.  Debug code.
        int encodingSize = SortableString.encodingLength(0);
        for (int i = 0; i < 35000; i++) {
            if (SortableString.encodingLength(i) > encodingSize) {
                encodingSize = SortableString.encodingLength(i);
                System.out.println("Encoding change at: " +
                                   i + " to " + encodingSize);
            }
        }
        */
    }

    private void testLong(long min, long max, int numLongs) {
        /*
         * First special cases
         */
        long[] special_longs = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L,
                                 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE };
        for (int i = 1; i < special_longs.length; i++) {
            cmpLong(special_longs[i - 1], special_longs[i],
                    SortableString.encodingLength(Long.MAX_VALUE));
        }

        /*
         * Specifically the min/max
         */
        int stringLen = SortableString.encodingLength(min, max);
        cmpLong(min, max, stringLen);

        /*
         * Now the specials plus the parameters
         */
        long[] longs = new long[numLongs + special_longs.length];
        System.arraycopy(special_longs, 0, longs, 0, special_longs.length);

        for (int i = special_longs.length; i < longs.length; i++) {
            long range = max - min;
            if (range > Integer.MAX_VALUE) {
                range = Integer.MAX_VALUE;
            }
            longs[i] = min + random.nextInt((int) range);

        }
        Arrays.sort(longs);
        for (int i = 1; i < longs.length; i++) {
            min = longs[i - 1];
            max = longs[i];
            stringLen = SortableString.encodingLength(min, max);
            cmpLong(min, max, stringLen);
        }
    }

    /*
     * Random doesn't return anything > 48 bits so
     * of those explicitly
     */
    @Test
    public void testLong() {
        testLong(0L, 10000000L, testCases);
        testLong(0x20000000000000L, 0x7fffffffffffffL, 1000000);
        testLong(0x7ffffffffffffffL, 0x7fffffffffffffffL, 1000000);
    }

    @Test
    public void testDouble() {
        double[] special_doubles = { Double.NEGATIVE_INFINITY,
                                     -Double.MAX_VALUE, inc(-Double.MAX_VALUE),
                                     dec(-Double.MIN_NORMAL), -Double.MIN_NORMAL,
                                     inc(-Double.MIN_NORMAL), dec(-Double.MIN_VALUE),
                                     -Double.MIN_VALUE, inc(-Double.MIN_VALUE), -0.0, 0.0,
                                     dec(Double.MIN_VALUE), Double.MIN_VALUE,
                                     inc(Double.MIN_VALUE), dec(Double.MIN_NORMAL),
                                     Double.MIN_NORMAL, inc(Double.MIN_NORMAL),
                                     dec(Double.MAX_VALUE), Double.MAX_VALUE,
                                     Double.POSITIVE_INFINITY };
        for (int i = 1; i < special_doubles.length; i++) {
            cmpDouble(special_doubles[i - 1], special_doubles[i]);
        }
        double[] doubles = new double[testCases];
        System.arraycopy(special_doubles, 0, doubles, 0,
                         special_doubles.length);
        for (int i = special_doubles.length; i < doubles.length; i++) {
            doubles[i] = random.nextDouble()
                * (random.nextBoolean() ? Double.MAX_VALUE
                   : -Double.MAX_VALUE);
        }
        Arrays.sort(doubles);
        for (int i = 1; i < doubles.length; i++) {
            cmpDouble(doubles[i - 1], doubles[i]);
        }
    }

    @Test
    public void testFloat() {
        float[] special_floats = { Float.NEGATIVE_INFINITY,
                                     -Float.MAX_VALUE, inc(-Float.MAX_VALUE),
                                     dec(-Float.MIN_NORMAL), -Float.MIN_NORMAL,
                                     inc(-Float.MIN_NORMAL), dec(-Float.MIN_VALUE),
                                   -Float.MIN_VALUE, inc(-Float.MIN_VALUE),
                                   -0.0F, 0.0F,
                                     dec(Float.MIN_VALUE), Float.MIN_VALUE,
                                     inc(Float.MIN_VALUE), dec(Float.MIN_NORMAL),
                                     Float.MIN_NORMAL, inc(Float.MIN_NORMAL),
                                     dec(Float.MAX_VALUE), Float.MAX_VALUE,
                                     Float.POSITIVE_INFINITY };
        for (int i = 1; i < special_floats.length; i++) {
            cmpFloat(special_floats[i - 1], special_floats[i]);
        }
        float[] floats = new float[testCases];
        System.arraycopy(special_floats, 0, floats, 0,
                         special_floats.length);
        for (int i = special_floats.length; i < floats.length; i++) {
            floats[i] = random.nextFloat()
                * (random.nextBoolean() ? Float.MAX_VALUE
                   : -Float.MAX_VALUE);
        }
        Arrays.sort(floats);
        for (int i = 1; i < floats.length; i++) {
            cmpFloat(floats[i - 1], floats[i]);
        }
    }

    @Test
    public void testEncodingLengths() {
        int[] intOne = {-1, -0x1f, 0x1f, 0, 1};
        int[] intTwo = {-0x700, 0x7ff};
        testInts(intOne, 1);
        testInts(intTwo, 2);
    }

    private void testInts(int[] values, int expected) {
        for (int i : values) {
            assertEquals(expected, SortableString.encodingLength(i));
        }
    }

    @Test
    public void testBytes() {
        List<byte[]> list = new ArrayList<byte[]>();
        Random rand = new Random();
        int nbytes = 9;
        for (int i = 0; i < 20; i++) {
            byte[] bytes = genBytes(rand, nbytes);
            list.add(bytes);
        }
        list.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0});
        list.add(new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
                            (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
                            (byte)0xFF});

        Set<byte[]> sorted =
            new TreeSet<byte[]>(new Comparator<byte[]>() {
                @Override
                public int compare(byte[] bs1, byte[] bs2) {
                    int size = Math.min(bs1.length, bs2.length);
                    for (int i = 0; i < size; i++) {
                        byte b1 = bs1[i];
                        byte b2 = bs2[i];
                        if (b1 == b2) {
                            continue;
                        }
                        return (b1 & 0xff) - (b2 & 0xff);
                    }
                    return (bs1.length > size) ?
                            1 : ((bs2.length > size) ? -1 : 0);
                }
        });
        for (byte[] bytes : list) {
            sorted.add(bytes);
        }

        byte[] prev = null;
        for (byte[] bytes : sorted) {
            if (prev != null) {
                cmpBytes(prev, bytes);
            }
            prev = bytes;
        }
    }

    private byte[] genBytes(Random rand, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)rand.nextInt(256);
        }
        return bytes;
    }

    private static int compareBytes(byte[] bs1, byte[] bs2) {
        int size = Math.min(bs1.length, bs2.length);
        for (int i = 0; i < size; i++) {
            byte b1 = bs1[i];
            byte b2 = bs2[i];
            if (b1 == b2) {
                continue;
            }
            return (b1 & 0xff) - (b2 & 0xff);
        }
        return (bs1.length > size) ?
                1 : ((bs2.length > size) ? -1 : 0);
    }
}
