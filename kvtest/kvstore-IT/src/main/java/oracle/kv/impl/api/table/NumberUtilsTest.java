/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SortableString;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.tree.Key;

import org.junit.Test;

/**
 * Test methods in oracle.kv.impl.api.table.NumberUtils.
 */
public class NumberUtilsTest extends TestBase {

    private final Random rand = new Random(System.currentTimeMillis());

    @Test
    public void testSerialization()  {
        final int ival = randInt();
        final long lval = randLong();
        final float fval = randFloat();
        final double dval = randDouble();

        int[] ints = new int[]{
            Integer.MIN_VALUE,
            ival, -1 * ival,
            -1000000000,
            -999999999,
            -100000000,
            -99999999,
            -10000000,
            -9999999,
            -1000000,
            -999999,
            -100000,
            -99999,
            -10000,
            -9999,
            -1000,
            -99,
            -10,
            -1,
            0, 1,
            10,
            99,
            1000,
            9999,
            10000,
            99999,
            100000,
            999999,
            1000000,
            9999999,
            10000000,
            99999999,
            100000000,
            999999999,
            1000000000,
            Integer.MAX_VALUE,
        };

        long[] longs = new long[] {
            123456789012345L,
            -123456789012345L,
            lval, -1 * lval,
            10000000000L,
            99999999999L,
            -10000000000L,
            -99999999999L,
            100000000000L,
            999999999999L,
            -100000000000L,
            -999999999999L,
            1000000000000L,
            9999999999999L,
            -1000000000000L,
            -9999999999999L,
            10000000000000L,
            99999999999999L,
            -10000000000000L,
            -99999999999999L,
            100000000000000L,
            999999999999999L,
            -100000000000000L,
            -999999999999999L,
            1000000000000000L,
            9999999999999999L,
            -1000000000000000L,
            -9999999999999999L,
            10000000000000000L,
            99999999999999999L,
            -10000000000000000L,
            -99999999999999999L,
            100000000000000000L,
            999999999999999999L,
            -100000000000000000L,
            -999999999999999999L,
            Long.valueOf(Integer.MAX_VALUE) + 1,
            Long.valueOf(Integer.MIN_VALUE) - 1,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
        };

        float[] floats = new float[] {
            1.234567f, -87654.321f,
            fval, -1 * fval,
            Float.MIN_VALUE, -1 * Float.MIN_VALUE,
            Float.MAX_VALUE, -1 * Float.MAX_VALUE
        };

        double[] doubles = new double[] {
            1.23456789012345678,
            -12345678901234.56789012345678,
            dval, -1 * dval,
            Double.MIN_VALUE, -1 * Double.MIN_VALUE,
            Double.MAX_VALUE, -1 * Double.MAX_VALUE
        };

        BigDecimal[] bds = new BigDecimal[] {
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.ONE.scaleByPowerOfTen(Integer.MIN_VALUE + 1),
            BigDecimal.ONE.scaleByPowerOfTen(Integer.MAX_VALUE),
            new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE),
                           Integer.MIN_VALUE + 19),
            new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE),
                           Integer.MAX_VALUE),
            new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE),
                           Integer.MIN_VALUE + 19),
            new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE),
                           Integer.MAX_VALUE),
            new BigDecimal("-9999999999999999999.9999999999999999999"),
            new BigDecimal("-123456789.123456789"),
            new BigDecimal("-0.9999999999999999999999999999999999999"),
            new BigDecimal("-123456789.123456789E-700"),
            new BigDecimal("-123456789.123456789E-6700"),
            new BigDecimal("0.0"),
            new BigDecimal("0.9999999999999999999999999999999999999"),
            new BigDecimal("123456789.123456789"),
            new BigDecimal("9999999999999999999.9999999999999999999"),
            new BigDecimal("123456789.123456789E700"),
            new BigDecimal("123456789.123456789E6700"),
            new BigDecimal("1E1"),
            new BigDecimal("1.91E2"),
            new BigDecimal("1.821E16"),
            new BigDecimal("1.7321E17"),
            new BigDecimal("1.64321E63"),
            new BigDecimal("1.554321E64"),
            new BigDecimal("1.4654321E4095"),
            new BigDecimal("1.37654321E4096"),
            new BigDecimal("1.287654321E4097"),
            new BigDecimal("1.91E-4096"),
            new BigDecimal("1.821E-4095"),
            new BigDecimal("1.7321E-64"),
            new BigDecimal("1.64321E-63"),
            new BigDecimal("1.554321E-17"),
            new BigDecimal("1.4654321E-16"),
            new BigDecimal("1.37654321E-15"),
            new BigDecimal("1.287654321E-2"),
            new BigDecimal("1.1987654321E-1"),
            new BigDecimal("1E-0"),
            new BigDecimal("-1E-1"),
            new BigDecimal("-1.91E-2"),
            new BigDecimal("-1.821E-16"),
            new BigDecimal("-1.7321E-17"),
            new BigDecimal("-1.64321E-63"),
            new BigDecimal("-1.554321E-64"),
            new BigDecimal("-1.4654321E-4095"),
            new BigDecimal("-1.37654321E-4096"),
            new BigDecimal("-1.287654321E-4097"),
            new BigDecimal("-1"),
            new BigDecimal("-1E1"),
            new BigDecimal("-1.91E2"),
            new BigDecimal("-1.821E16"),
            new BigDecimal("-1.7321E17"),
            new BigDecimal("-1.64321E63"),
            new BigDecimal("-1.554321E64"),
            new BigDecimal("-1.4654321E4095"),
            new BigDecimal("-1.37654321E4096"),
            new BigDecimal("-1.287654321E4097"),
            new BigDecimal("-1.287654321E8191"),
            new BigDecimal("-1.287654321E8192"),
            new BigDecimal("-1.287654321E8193"),
        };

        List<BigDecimal> decimals = new ArrayList<BigDecimal>();
        for (int i: ints) {
            decimals.add(BigDecimal.valueOf(i));
        }
        for (long l: longs) {
            decimals.add(BigDecimal.valueOf(l));
        }
        for (float f: floats) {
            decimals.add(BigDecimal.valueOf(f));
        }
        for (double d: doubles) {
            decimals.add(BigDecimal.valueOf(d));
        }
        for (BigDecimal bd : bds) {
            decimals.add(bd);
        }

        Map<BigDecimal, byte[]> map = new TreeMap<BigDecimal, byte[]>();
        for(BigDecimal d : decimals) {
            byte[] bytes = roundTripWriteReadBigDecimal(d);
            byte[] prev = map.put(d, bytes);
            if (prev != null) {
                assertTrue("Wrong bytes, expect " + bytesToString(bytes) +
                           " actual " + bytesToString(prev),
                           Arrays.equals(bytes, prev));
            }
        }
        validateMap(map);
    }

    private byte[] roundTripWriteReadBigDecimal(BigDecimal bd) {
        Class<?> cls = getExpectedNumericClass(bd);
        byte[] bytes;
        if (cls == Integer.class || cls == Long.class) {
            bytes = NumberUtils.serialize(bd.longValueExact());
        } else {
            bytes = NumberUtils.serialize(bd);
        }
        assertTrue(bytes != null);
        Object object = NumberUtils.deserialize(bytes, false);

        /* Check the numeric object */
        assertTrue(object.getClass().equals(cls));

        /* Verify the numeric value */
        if (object instanceof Integer) {
            assertTrue(((Integer)object).intValue() == bd.intValueExact());
        } else if (object instanceof Long) {
            assertTrue(((Long)object).longValue() == bd.longValueExact());
        } else {
            assertTrue(((BigDecimal)object).compareTo(bd) == 0);
        }

        /* test NumberUtils.readTuple() */
        byte[] bytes1 = Arrays.copyOf(bytes, bytes.length + 2);
        bytes1[bytes1.length - 2] = 0x7F;
        bytes1[bytes1.length - 1] = 0x7F;
        byte[] bytes2 = NumberUtils.readTuple(new TupleInput(bytes1));
        assertTrue(Arrays.equals(bytes, bytes2));
        return bytes;
    }

    private Class<?> getExpectedNumericClass(BigDecimal bd) {
        try {
            bd.intValueExact();
            return Integer.class;
        } catch (ArithmeticException ae) {
        }

        try {
            bd.longValueExact();
            return Long.class;
        } catch (ArithmeticException ae) {
        }

        return BigDecimal.class;
    }

    private void validateMap(Map<BigDecimal, byte[]> map) {
        byte[] prevBytes = null;
        for (Entry<BigDecimal, byte[]> entry : map.entrySet()) {
            byte[] bytes = entry.getValue();
            if (prevBytes != null) {
                int ret = Key.compareUnsignedBytes(bytes, 0, bytes.length,
                                                   prevBytes, 0,
                                                   prevBytes.length);
                assertTrue(ret > 0);

                String ss = SortableString.toSortable(bytes);
                String pss = SortableString.toSortable(prevBytes);
                assertTrue(ss.compareTo(pss) > 0);
            }

            byte[] next = NumberUtils.nextUp(bytes);
            assertTrue(IndexImpl.compareUnsignedBytes(next, bytes) > 0);
            prevBytes = bytes;
        }
    }

    @Test
    public void testReadWriteExponent() {
        int[] signs = new int[] {-1, 0, 1};
        int[] exponents = new int[] {
            Integer.MIN_VALUE, Integer.MIN_VALUE + 1,   /* 5 bytes */
            -4097-0x1000000,                            /* 5 bytes */
            -4097-0xFFFFFF, -4097-0x10000,              /* 4 bytes */
            -4097-0xFFFF, -4097-0x100,                  /* 3 bytes */
            -4097-0xFF, -4098, -4097,                   /* 2 bytes */
            -4096, -4095, -1000, -500, -64, -18, -17,   /* 2 bytes */
            -16, -15, -2, -1, 0, 1, 10, 30, 50, 62, 63, /* 1 byte  */
            64, 65, 100, 1000, 3000, 4095, 4096,        /* 2 bytes */
            4097, 4098, 4097 + 0xFF,                    /* 2 bytes */
            4097 + 0x100, 4097 + 0xFFFF,                /* 3 bytes */
            4097 + 0x10000, 4097 + 0xFFFFFF,            /* 4 bytes */
            4097 + 0x1000000,                           /* 5 bytes */
            Integer.MAX_VALUE - 1, Integer.MAX_VALUE    /* 5 bytes */
        };

        for (int sign : signs) {
            byte[] last = null;
            for (int exp : exponents) {
                byte[] bytes = roundTripWriteReadExponent(sign, exp);
                if (last != null) {
                    int ret = IndexImpl.compareUnsignedBytes(bytes, last);
                    if (sign > 0) {
                        assertTrue(ret > 0);
                    } else if (sign == 0) {
                        assertTrue(ret == 0);
                    } else {
                        assertTrue(ret < 0);
                    }
                }
                last = Arrays.copyOf(bytes, bytes.length);
            }
        }
    }

    @Test
    public void testNextUpBytes() {
        BigDecimal[] bds = new BigDecimal[] {
           BigDecimal.ZERO,
           BigDecimal.ONE,
           BigDecimal.valueOf(-1),
           BigDecimal.valueOf(Long.MIN_VALUE),
           BigDecimal.valueOf(Long.MAX_VALUE),
           BigDecimal.valueOf(-1 * Double.MIN_VALUE),
           BigDecimal.valueOf(-1 * Double.MAX_VALUE),
           BigDecimal.valueOf(Double.MIN_VALUE),
           BigDecimal.valueOf(Double.MAX_VALUE),
           new BigDecimal("12345678901234567890.1234567890123456789"),
           new BigDecimal("-12345678901234567890.1234567890123456789"),
        };

        for (BigDecimal bd : bds) {
            byte[] bytes = NumberUtils.serialize(bd);
            byte[] next = NumberUtils.nextUp(bytes);

            BigDecimal inc = bd.add(new BigDecimal("1E-5"));
            byte[] incBytes = NumberUtils.serialize(inc);
            assertTrue(IndexImpl.compareUnsignedBytes(next, bytes) > 0);
            assertTrue(IndexImpl.compareUnsignedBytes(next, incBytes) < 0);
        }
    }

    private byte[] roundTripWriteReadExponent(int sign, int exponent) {

        int len = NumberUtils.getNumBytesExponent(sign, exponent);
        byte[] bytes = new byte[len];
        NumberUtils.writeExponent(bytes, 0, sign, exponent);

        NumberUtils.ReadBuffer in = new NumberUtils.ReadBuffer(bytes, 0);
        int exp = NumberUtils.readExponent(in, in.read(), sign);
        assertTrue((sign != 0) ? exp == exponent : exp == 0);
        return bytes;
    }

    private int randInt() {
        return rand.nextInt();
    }

    private long randLong() {
        return rand.nextLong();
    }

    private float randFloat() {
        return rand.nextFloat();
    }

    private double randDouble() {
        return rand.nextDouble();
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private static String bytesToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(hexArray[v >>> 4]);
            sb.append(hexArray[v & 0x0F]);
        }
        sb.append(",len=");
        sb.append(bytes.length);
        return sb.toString();
    }
}
