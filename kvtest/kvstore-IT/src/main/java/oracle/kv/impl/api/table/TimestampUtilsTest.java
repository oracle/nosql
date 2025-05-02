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

import static oracle.kv.impl.api.table.TimestampUtils.parseString;
import static oracle.kv.impl.api.table.TimestampUtils.RoundMode;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

import org.junit.Test;

import oracle.kv.TestBase;
import oracle.kv.impl.api.table.TimestampUtils.Interval;
import oracle.kv.impl.api.table.TimestampUtils.RoundUnit;
import oracle.kv.impl.api.table.TimestampUtils.BucketInterval;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.TimestampDef;
/**
 * Unit tests for methods in TimestampUtils class.
 */
public class TimestampUtilsTest extends TestBase {

    /**
     * Test TimestampUtils.roundToPrecision()
     */
    @Test
    public void testRoundToPrecision() {
        Timestamp ts = new Timestamp(new Date().getTime());
        int[] nanos = new int[] {
            999999999, 987654321, 123456789, 915000000, 915, 0
        };
        for (int nano : nanos) {
            ts.setNanos(nano);
            for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
                Timestamp rounded = TimestampUtils.roundToPrecision(ts, p);
                checkRoundedTimestamp(ts, rounded, p);
            }
        }

        ts = TimestampDefImpl.MAX_VALUE;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            Timestamp rounded = TimestampUtils.roundToPrecision(ts, p);
            assertTrue(rounded.compareTo(ts) <= 0);
        }

        ts = TimestampDefImpl.MIN_VALUE;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            Timestamp rounded = TimestampUtils.roundToPrecision(ts, p);
            assertTrue(rounded.compareTo(ts) >= 0);
        }
    }

    private void checkRoundedTimestamp(Timestamp org,
                                       Timestamp rounded,
                                       int precision) {
        if (precision == TimestampDefImpl.getMaxPrecision()) {
            assertTrue(org.equals(rounded));
            return;
        }
        int delta = (int)Math.pow
                (10, (TimestampDefImpl.getMaxPrecision() - precision)) / 2;
        if (org.compareTo(rounded) != 0) {
            if (rounded.after(org)) {
                Timestamp ts1 = TimestampUtils.plusNanos(org, delta);
                assertTrue(rounded.compareTo(ts1) == 0 || rounded.before(ts1));
            } else {
                Timestamp ts1 = TimestampUtils.minusNanos(org, delta);
                assertTrue(rounded.compareTo(ts1) == 0 || rounded.after(ts1));
            }
        }
    }

    /**
     * Test TimestampUtils.toBytes(), TimestampUtils.fromBytes().
     */
    @Test
    public void testConvertTimestampToFromBytes() {
        final Timestamp[] dates = new Timestamp[] {
            new Timestamp(0),
            new Timestamp(-1),
            new Timestamp(1),
            new Timestamp(3600000),
            new Timestamp(-3600000),
            new Timestamp(new Date().getTime()),
            TimestampDefImpl.MAX_VALUE,
            TimestampDefImpl.MIN_VALUE,
            new Timestamp(TimestampDefImpl.MAX_VALUE.getTime() - 1000),
            new Timestamp(TimestampDefImpl.MIN_VALUE.getTime() + 1000),
        };

        TreeMap<byte[], Timestamp> map =
            new TreeMap<byte[], Timestamp>(new Comparator<byte[]>() {
                @Override
                public int compare(byte[] bs1, byte[] bs2) {
                    return TimestampUtils.compareBytes(bs1, bs2);
                }
        });
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            for (Timestamp ts : dates) {
                Timestamp ts1 = TimestampUtils.roundToPrecision(ts, p);
                byte[] bytes = TimestampUtils.toBytes(ts1, p);
                Timestamp ts2 = TimestampUtils.fromBytes(bytes, p);
                assertTrue(ts2.equals(ts1));
                map.put(bytes, ts2);
            }
            /* Validate the ordering of 64bit timestamp */
            Timestamp last = null;
            for (Entry<byte[], Timestamp> entry : map.entrySet()) {
                Timestamp current = entry.getValue();
                if (last != null) {
                    assertTrue(last.compareTo(current) <= 0);
                }
                last = current;
            }
            map.clear();
        }
    }

    @Test
    public void testToBytesVarLength() {
        String[] values = new String[] {
            "2016-08-03",
            "2016-08-03T00:00:00",
            "2016-08-03T01:00:00",
            "2016-08-03T00:01:00",
            "2016-08-03T00:00:01",
        };
        int[] sizes = new int[] { 3, 3, 4, 5, 5 };

        String[] values2 = new String[] {
            "2016-08-03T12:11:01.9",
            "2016-08-03T12:11:01.99",
        };
        int[] sizes2 = new int[] { 6, 6 };

        String[] values3 = new String[] {
            "2016-08-03T12:11:01.899",
            "2016-08-03T12:11:01.9",
            "2016-08-03T12:11:01.901",
        };
        int[] sizes3 = new int[] { 7, 6 ,7 };

        String[] values9 = new String[] {
            "2016-08-03T12:11:01.536870911",
            "2016-08-03T12:11:01.536870912",
            "2016-08-03T12:11:01.536870913",
        };
        int[] sizes9 = new int[] { 9, 6, 9 };

        for (int p = 0; p <= 9; p++) {
            checkByteSize(values, p, sizes);
        }
        checkByteSize(values2, 2, sizes2);
        checkByteSize(values3, 3, sizes3);
        checkByteSize(values9, 9, sizes9);
    }

    private void checkByteSize(String[] values, int precision, int[] expSizes) {
        for (int i = 0; i < values.length; i++) {
            Timestamp ts = TimestampUtils.parseString(values[i]);
            byte[] bytes = TimestampUtils.toBytes(ts, precision);
            assertTrue(bytes.length == expSizes[i]);
        }
    }

    /**
     * Test TimestampUtils.compareBytes().
     */
    @Test
    public void testCompareBytes() {

        /*
         * Test a timestamp with date only that represented with different
         * precisions, the byte size with different precision is always 3,
         * the bytes value are all equaled.
         */
        Timestamp ts = new Timestamp(0);
        byte[] last = null;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length == 3);
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue(cmp == 0);
            }
            last = bytes.clone();
        }

        /*
         * Test a timestamp with date+time(no fractional seconds) that
         * represented with different precisions, the byte size with different
         * precision is always 5, the bytes value are all equaled.
         */
        ts = new Timestamp(1000);
        last = null;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length == 5);
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue(cmp == 0);
            }
            last = bytes.clone();
        }

        /*
         * Test a timestamp with millisecond that represented with different
         * precisions.
         */
        ts = new Timestamp(1234);
        last = null;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length <= TimestampUtils.getNumBytes(p));
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue((p < 4) ? cmp < 0 : cmp == 0);
            }
            last = bytes.clone();
        }

        /*
         * Test a timestamp with 9 digits nanosecond that represented with
         * different precisions
         */
        ts = new Timestamp(1000);
        ts.setNanos(123456789);
        last = null;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length <= TimestampUtils.getNumBytes(p));
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue((p < 5) ? cmp < 0 : cmp > 0);
            }
            last = bytes.clone();
        }

        ts = new Timestamp(1000);
        ts.setNanos(987654321);
        last = null;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length <= TimestampUtils.getNumBytes(p));
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue((p == 1) ? cmp == 0 :
                                      (p < 6) ? cmp > 0 : cmp < 0);
            }
            last = bytes.clone();
        }

        ts = new Timestamp(1000);
        last = null;
        int nanos = 0;
        for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            if (p > 0) {
                nanos = nanos * 10 + 9;
            }
            ts.setNanos
            (nanos * (int)Math.pow(10, TimestampDefImpl.getMaxPrecision() - p));

            byte[] bytes = roundTrip(ts, p);
            assertTrue(bytes.length == TimestampUtils.getNumBytes(p));
            if (last != null) {
                int cmp = TimestampUtils.compareBytes(last, p - 1, bytes, p);
                assertTrue(cmp < 0);
            }
            last = bytes.clone();
        }

        /*
         * Increases/decrease a timestamp value and compares its current and
         * previous value.
         */
        ts = new Timestamp(0);
        int num = 15;
        for (int p = 0; p <= 3; p++) {
            last = null;
            int delta = (int)Math.pow(10, 3 - p);
            for (int i = 0; i < num; i++) {
                ts = TimestampUtils.plusMillis(ts, delta);
                byte[] bytes = TimestampUtils.toBytes(ts, p);
                if (last != null) {
                    int cmp = TimestampUtils.compareBytes(last, bytes);
                    assertTrue(cmp < 0);
                }
                last = bytes.clone();
            }
            for (int i = 0; i < num; i++) {
                ts = TimestampUtils.minusMillis(ts, delta);
                byte[] bytes = TimestampUtils.toBytes(ts, p);
                if (last != null) {
                    int cmp = TimestampUtils.compareBytes(last, bytes);
                    assertTrue(cmp > 0);
                }
                last = bytes.clone();
            }
        }
        for (int p = 4; p <= TimestampDefImpl.getMaxPrecision(); p++) {
            last = null;
            int delta = (int)Math.pow(10, TimestampDefImpl.getMaxPrecision()-p);
            for (int i = 0; i < num; i++) {
                ts = TimestampUtils.plusNanos(ts, delta);
                byte[] bytes = TimestampUtils.toBytes(ts, p);
                if (last != null) {
                    int cmp = TimestampUtils.compareBytes(last, bytes);
                    assertTrue(cmp < 0);
                }
                last = bytes.clone();
            }
            for (int i = 0; i < num; i++) {
                ts = TimestampUtils.minusNanos(ts, delta);
                byte[] bytes = TimestampUtils.toBytes(ts, p);
                if (last != null) {
                    int cmp = TimestampUtils.compareBytes(last, bytes);
                    assertTrue(cmp > 0);
                }
                last = bytes.clone();
            }
        }

        /* Compares the byte[] and full size byte[] of timestamp. */
        final Timestamp[] dates = new Timestamp[] {
            new Timestamp(0),
            new Timestamp(-1),
            new Timestamp(1),
            new Timestamp(3600000),
            new Timestamp(-3600000),
            new Timestamp(new Date().getTime()),
            TimestampDefImpl.MAX_VALUE,
            TimestampDefImpl.MIN_VALUE,
            new Timestamp(TimestampDefImpl.MAX_VALUE.getTime() - 1000),
            new Timestamp(TimestampDefImpl.MIN_VALUE.getTime() + 1000),
        };

        Arrays.sort(dates);
        last = null;
        for (Timestamp t : dates) {
            for (int p = 0; p <= TimestampDefImpl.getMaxPrecision(); p++) {
                Timestamp ts1 = TimestampUtils.roundToPrecision(t, p);
                byte[] bs1 = TimestampUtils.toBytes(ts1, p);
                byte[] bs2 = Arrays.copyOf(bs1, TimestampUtils.getNumBytes(p));
                assertTrue(TimestampUtils.compareBytes(bs1, bs2) == 0);
                if (p == TimestampDefImpl.getMaxPrecision()) {
                    if (last != null) {
                        assertTrue(TimestampUtils.compareBytes(last, bs1) < 0);
                    }
                    last = bs1.clone();
                }
            }
        }
    }

    private byte[] roundTrip(Timestamp ts, int precision) {
        Timestamp ts1 = TimestampUtils.roundToPrecision(ts, precision);
        byte[] bytes = TimestampUtils.toBytes(ts1, precision);
        Timestamp ts2 = TimestampUtils.fromBytes(bytes, precision);
        assertTrue(ts1.equals(ts2));
        return bytes;
    }

    /**
     * Test TimestampUtils.parseString(timestampString, pattern, withZoneUTC)
     */
    @Test
    public void testParseStringWithPattern() {

        final String[] dateStrs = new String[] {
            "July 7 16 08-29-36-71",
            "2016-07-02T08:29:36.7 Asia/Shanghai",
            "2016-07-03T16:29:36.71 +07:00",
            "2016-07-04T16:29:36.712 +07:00",
            "2016-07-05T16:29:36.712345 +07:00",
            "2016-07-06T16:29:36.712345689 Asia/Shanghai",
            "2016-07-07T16:29:36.712345Asia/Shanghai",
            "2016-07-08T08:29:36.712345689 GMT+02:00",
            "2016-07-08T08:29:36 Pacific Time",
            "2016-07-08T08:29:36 Pacific Standard Time",
            "July 9 16"
        };

        final String[] patterns = new String[] {
            "MMMM d yy HH-mm-ss-SS",
            "yyyy-MM-dd'T'HH:mm:ss.S VV",
            "yyyy-MM-dd'T'HH:mm:ss.SS VV",
            "yyyy-MM-dd'T'HH:mm:ss.SSS VV",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS VV",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS VV",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSVV",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS O",
            "yyyy-MM-dd'T'HH:mm:ss vvvv",
            "yyyy-MM-dd'T'HH:mm:ss zzzz",
            "MMMM d yy"
        };

        assertTrue(dateStrs.length == patterns.length);
        for (int i = 0; i < dateStrs.length; i++) {
            String dateStr = dateStrs[i];
            try {
                String pattern = patterns[i];
                boolean hasZone = pattern.contains("V") ||
                                  pattern.contains("O") ||
                                  pattern.contains("z") ||
                                  pattern.contains("v");
                Timestamp ts1 =
                    TimestampUtils.parseString(dateStr, pattern, true);
                Timestamp ts2 =
                    TimestampUtils.parseString(dateStr, pattern, false);
                if (hasZone) {
                    assertTrue(ts1.compareTo(ts2) == 0);
                } else {
                    assertTrue(ts1.getTime() - ts2.getTime() ==
                               TimeZone.getDefault().getOffset(ts2.getTime()));
                }
            } catch (Exception ex) {
                fail("Failed to parse timestamp string: " + ex.getMessage());
            }
        }

        /* Invalid date string */
        try {
            TimestampUtils.parseString("invalid string",
                                       null /* pattern */,
                                       true /* withUTCZone */);
            fail("Expected to catch exception but not");
        } catch (IllegalArgumentException iae) {
        }

        /* The date string doesn't match the pattern specified */
        try {
            TimestampUtils.parseString("1999-01-01", "yyyy-mm",
                                       true /* withUTCZone */);
            fail("Expected to catch exception but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test TimestampUtils.parseString() with default pattern.
     */
    @Test
    public void testParseString() {

        final String[] dateStrs = new String[] {
            "-6383-01-01",
            "2016-07-01",
            "9999-12-31",
            "2016-07-01T08:29:36Z",
            "2016-07-02T08:29:36.9Z",
            "2016-07-03T08:29:36.98Z",
            "2016-07-04T08:29:36.987Z",
            "2016-07-05T08:29:36.9876Z",
            "2016-07-06T08:29:36.98765Z",
            "2016-07-07T08:29:36.987654Z",
            "2016-07-08T08:29:36.9876543Z",
            "2016-07-09T08:29:36.98765432Z",
            "2016-07-10T08:29:36.987654321Z",
        };

        /* Valid date strings */
        for (int i = 0; i < dateStrs.length; i++) {
            String dateStr = dateStrs[i];
            try {
                Timestamp ts = TimestampUtils.parseString(dateStr);
                String str = TimestampUtils.formatString(ts);
                assertTrue(dateStr.contains("T") ?
                           dateStr.equals(str) : str.contains(dateStr));
            } catch (IllegalArgumentException iae) {
                fail("Failed to parse timestamp string: " + iae.getMessage());
            }
        }

        try {
            TimestampUtils.parseString("2016-07-07T01:01:01.1234567890");
            fail("Expected to catch exception but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test TimestampUtils.formatString()
     */
    @Test
    public void testFormatString() {
        final Timestamp ts = new Timestamp(new Date().getTime());
        ts.setNanos(123456789);
        String[] patterns = new String[]{
            null,
            "MM-dd-yy HH:mm:ss.SSS VV",
            "dd MMM yyyy, HH:mm:ss.SS VV",
            "MMM dd, yyyy mm-HH-ss GSSS",
            "MM-dd-yy-HH-mm-ss.S",
            "MMM dd yy HH-mm-ss.SSS VV"
        };

        ZoneId localZone = TimestampUtils.localZone;
        for (String pattern : patterns) {
            try {
                String s1 = TimestampUtils.formatString(ts, pattern, null, 0);
                Timestamp ts1 = TimestampUtils.parseString(s1, pattern, true);

                String s2 = TimestampUtils.formatString(ts, pattern,
                                                        localZone, 0);
                Timestamp ts2 = TimestampUtils.parseString(s2, pattern, false);
                assertTrue(ts1.getTime() == ts2.getTime());
            } catch (Exception ex) {
                fail("Failed to format timestamp: " + ex.getMessage());
            }
        }

        /* Invalid pattern */
        try {
            TimestampUtils.formatString(ts, "invalid pattern", null, 0);
            fail("Expected to catch exception but not");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Test format string with default pattern for Timestamp with/without
         * fractional seconds
         */
        patterns = new String[] {
            null,
            TimestampDef.DEFAULT_PATTERN
        };

        TimestampValueImpl val;
        String s1;
        Timestamp v1;
        for (int i = 0; i < 9; i++) {
            val = (TimestampValueImpl)FieldValueFactory.createTimestamp(ts, i);
            for (String pattern:  patterns) {
                s1 = TimestampUtils.formatString(val, pattern,
                                                 TimestampUtils.UTCZone);
                assertTrue(s1.endsWith("Z"));
                v1 = TimestampUtils.parseString(s1, pattern, true);
                assertEquals(val.get(), v1);

                /* Trimed the "Z" and try to parse it again */
                s1 = s1.substring(0,  s1.length() - 1);
                v1 = TimestampUtils.parseString(s1, pattern, true);
                assertEquals(val.get(), v1);
            }
        }
    }

    @Test
    public void testFormatStringWithZone() {
        Timestamp ts0 = new Timestamp(System.currentTimeMillis());
        TimestampValueImpl tsv =
            (TimestampValueImpl)FieldValueFactory.createTimestamp(ts0, 3);

        String[] zones = {
            null,
            "UTC",
            "Z",
            "America/Chicago",
            "GMT+08:00",
            TimeZone.getDefault().getID(),
        };

        String pattern = "MMM d, yy HH:mm:ss.SSS VV";
        for (String zone : zones) {
            String s = TimestampUtils.formatString(tsv, pattern, zone);
            Timestamp ts1 = TimestampUtils.parseString(s, pattern,
                                                       true /* withUTCZone */);
            assertEquals(ts0, ts1);
        }
    }

    /**
     * Zone id, name or offset related symbols:
     *
     * V   time-zone ID    zone-id     America/Los_Angeles; Z; -08:30
     * v   generic time-zone name  zone-name   Pacific Time; PT
     * z   time-zone name  zone-name   Pacific Standard Time; PST
     * O   localized zone-offset   offset-O    GMT+8; GMT+08:00; UTC-08:00
     * X   zone-offset 'Z' for zero    offset-X    Z; -08; -0830; -08:30; -083015; -08:30:15
     * x   zone-offset     offset-x    +0000; -08; -0830; -08:30; -083015; -08:30:15
     * Z   zone-offset     offset-Z    +0000; -0800; -08:00
    */
    @Test
    public void testZoneSymbols() {
        String[] symbols = new String[] {
            "VV",
            "vvvv",
            "zzzz",
            "O", "OOOO",
            "X", "XX", "XXX", "XXXX", "XXXXX",
            "x", "xx", "xxx", "xxxx", "xxxxx",
            "Z", "ZZ", "ZZZ", "ZZZZ", "ZZZZZ",
        };

        String str = "2024-07-01T01:34:30.005";
        String[] zones = new String[] {
            "America/Los_Angeles",
            "GMT+07:30",
            "Z"
        };

        String base = "yyyy-MM-dd'T'HH:mm:ss.SSS ";

        TimestampValueImpl tsv =
            (TimestampValueImpl)FieldValueFactory.createTimestamp(str, 3);
        for (String symbol : symbols) {
            String pattern = base + symbol;
            for (String zone : zones) {
                String str1 = TimestampUtils.formatString(tsv, pattern, zone);
                Timestamp ts = TimestampUtils.parseString(str1, pattern, true);
                assertEquals(tsv.getTimestamp(), ts);
            }
        }

        /* Unsupported time zone symbols */
        symbols = new String[] {
            "MM/dd/yy v",
            "z MMM dd, yy",
            "yyyy-mm-dd'zzzz'zz",
            "yy zzzz mm-dd'zzzz' zzz"
        };

        for (String symbol : symbols) {
            String pattern = base + symbol;
            try {
                TimestampUtils.formatString(tsv, pattern, "America/Los_Angeles");
                fail("Expect to get iae");
            } catch (IllegalArgumentException e) {
                /* expected */
            }
        }

        /*
         * The abbreviation zone id is not allowed for 3rd argument 'zone' in
         * format_timestamp()
         */
        try {
            TimestampUtils.formatString(tsv, "MM/dd/yy VV", "PST");
            fail("Expect to get iae");
        } catch (IllegalArgumentException e) {
            /* expected */
        }
    }

    /**
     * Test below methods:
     *  TimestampUtils.createTimestamp(long microseconds)
     *  TimestampUtils.createTimestamp(long seconds, long nanosOfSecond)
     *  TimestampUtils.toMilliseconds(Timestamp ts)
     *  TimestampUtils.toMicroseconds(Timestamp ts)
     *  TimestampUtils.getSeconds(Timestamp ts)
     *  TimestampUtils.getNanosOfSecond(Timestamp ts)
     */
    @Test
    public void testBasicTimestampOp() {
        String[] strs = new String[] {
            "-6383-01-01T00:00:00Z",
            "-6383-01-01T00:00:00.000000001Z",
            "-6383-01-01T00:00:00.9Z",
            "-2016-07-21T14:56:01Z",
            "-2016-07-21T14:56:01.12Z",
            "-2016-07-21T14:56:01.123456789Z",
            "1969-12-31T23:59:59Z",
            "1969-12-31T23:59:59.999Z",
            "1969-12-31T23:59:59.999999999Z",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000000001Z",
            "1970-01-01T00:00:00.1111Z",
            "1970-01-01T00:00:00.999999999Z",
            "2016-07-21T14:56:01Z",
            "2016-07-21T14:56:01.12345Z",
            "2016-07-21T14:56:01.123456789Z",
            "9999-12-31T23:59:59Z",
            "9999-12-31T23:59:59.12345678Z",
            "9999-12-31T23:59:59.999999999Z",
        };

        Timestamp last = null;
        for(String str : strs) {
            Timestamp ts = TimestampUtils.parseString(str);
            assertTrue(TimestampUtils.formatString(ts).equals(str));
            long seconds = TimestampUtils.getSeconds(ts);
            int nanos = TimestampUtils.getNanosOfSecond(ts);
            long milliseconds = TimestampUtils.toMilliseconds(ts);
            Timestamp ts1 = TimestampUtils.createTimestamp(seconds, nanos);
            assertTrue(ts1.compareTo(ts) == 0);
            assertTrue(milliseconds == seconds * 1000 + nanos/1000000);
            if (last != null) {
                assertTrue(last.compareTo(ts) < 0);
            }
            last = ts;
        }
    }

    /**
     * Test TimestampUtils.addNanos()/minusNanos()
     */
    @Test
    public void testAddMinus() {
        Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            new Timestamp(new Date().getTime()),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE
        };

        int[] nanosToAdd = new int[] {
            1, 999999999, -1, -999999999
        };

        for (Timestamp ts : values) {
            for (long nanos : nanosToAdd) {
                Timestamp ts1 = TimestampUtils.plusNanos(ts, nanos);
                assertTrue((nanos > 0) ? ts1.compareTo(ts) > 0 :
                                         ts1.compareTo(ts) < 0);
                assertTrue(TimestampUtils.minusNanos(ts1, nanos).equals(ts));
            }
        }

        long[] millisToAdd = new long[] {
            1, 1469434908000L, -1, -1469434908000L
        };

        for (Timestamp ts : values) {
            for (long millis : millisToAdd) {
                Timestamp ts1 = TimestampUtils.plusMillis(ts, millis);
                assertTrue((millis > 0) ? ts1.compareTo(ts) > 0 :
                                          ts1.compareTo(ts) < 0);
                assertTrue(TimestampUtils.minusMillis(ts1, millis).equals(ts));
            }
        }
    }

    /**
     * Test TimestampUtils.getXXX(byte[])
     */
    @Test
    public void testGetComponents() {
        Timestamp ts = new Timestamp(0);
        byte[] bytes = TimestampUtils.toBytes(ts, 9);
        assertTrue(1970 == TimestampUtils.getYear(bytes));
        assertTrue(1 == TimestampUtils.getMonth(bytes));
        assertTrue(1 == TimestampUtils.getDay(bytes));
        assertTrue(0 == TimestampUtils.getHour(bytes));
        assertTrue(0 == TimestampUtils.getMinute(bytes));
        assertTrue(0 == TimestampUtils.getSecond(bytes));
        assertTrue(0 == TimestampUtils.getNano(bytes, 9));
        Timestamp ts1 = TimestampUtils.createTimestamp(new int[] {1970, 1, 1});
        assertTrue(ts1.equals(ts));

        ts.setNanos(987654321);
        bytes = TimestampUtils.toBytes(ts, 9);
        assertTrue(987654321 == TimestampUtils.getNano(bytes, 9));
        ts1 = TimestampUtils.createTimestamp
                (new int[] {1970, 1, 1, 0, 0, 0, 987654321});
        assertTrue(ts1.equals(ts));

        ts = TimestampDefImpl.MIN_VALUE;
        bytes = TimestampUtils.toBytes(ts, 9);
        assertTrue(-6383 == TimestampUtils.getYear(bytes));
        assertTrue(1 == TimestampUtils.getMonth(bytes));
        assertTrue(1 == TimestampUtils.getDay(bytes));
        assertTrue(0 == TimestampUtils.getHour(bytes));
        assertTrue(0 == TimestampUtils.getMinute(bytes));
        assertTrue(0 == TimestampUtils.getSecond(bytes));
        assertTrue(0 == TimestampUtils.getNano(bytes, 9));
        ts1 = TimestampUtils.createTimestamp(new int[] {-6383, 1, 1});
        assertTrue(ts1.equals(ts));

        ts = TimestampDefImpl.MAX_VALUE;
        for (int p = 0; p <= 9; p++) {
            Timestamp rounded = TimestampUtils.roundToPrecision(ts, p);
            bytes = TimestampUtils.toBytes(rounded, p);

            int year = TimestampUtils.getYear(bytes);
            int month = TimestampUtils.getMonth(bytes);
            int day = TimestampUtils.getDay(bytes);
            int hour = TimestampUtils.getHour(bytes);
            int minute = TimestampUtils.getMinute(bytes);
            int second = TimestampUtils.getSecond(bytes);
            int nano = TimestampUtils.getNano(bytes, p);

            assertTrue(9999 == year);
            assertTrue(12 == month);
            assertTrue(31 == day);
            assertTrue(23 == hour);
            assertTrue(59 == minute);
            assertTrue(59 == second);
            assertTrue(rounded.getNanos() == nano);

            ts1 = TimestampUtils.createTimestamp
                    (new int[] {year, month, day, hour, minute, second, nano});
            assertTrue(ts1.equals(rounded));
        }

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ts = Timestamp.valueOf("2016-10-09 14:27:49.123456789");
        cal.setTimeInMillis(ts.getTime());
        for (int p = 0; p <= 9; p++) {
            Timestamp rounded = TimestampUtils.roundToPrecision(ts, p);
            bytes = TimestampUtils.toBytes(rounded, p);

            int year = TimestampUtils.getYear(bytes);
            int month = TimestampUtils.getMonth(bytes);
            int day = TimestampUtils.getDay(bytes);
            int hour = TimestampUtils.getHour(bytes);
            int minute = TimestampUtils.getMinute(bytes);
            int second = TimestampUtils.getSecond(bytes);
            int nano = TimestampUtils.getNano(bytes, p);

            assertTrue(cal.get(Calendar.YEAR) == year);
            assertTrue(cal.get(Calendar.MONTH) + 1 == month);
            assertTrue(cal.get(Calendar.DAY_OF_MONTH) == day);
            assertTrue(cal.get(Calendar.HOUR_OF_DAY) == hour);
            assertTrue(cal.get(Calendar.MINUTE) == minute);
            assertTrue(cal.get(Calendar.SECOND) == second);
            assertTrue(rounded.getNanos() == nano);

            ts1 = TimestampUtils.createTimestamp
                    (new int[] {year, month, day, hour, minute, second, nano});
            assertTrue(ts1.equals(rounded));
        }
    }

    /**
     * Test TimestampUtils.createTimestamp(int[])
     */
    @Test
    public void testCreateTimestampFromComponents() {
        int[][] compsArray = new int[][] {
            new int[] {-6383, 1, 1},
            new int[] {-6383, 1, 1, 0, 0, 0, 1},
            new int[] {-6383, 1, 1, 0, 0, 0, 999999999},
            new int[] {-6383, 1, 1, 0, 0, 1},
            new int[] {-6383, 1, 1, 0, 1},
            new int[] {-6383, 1, 1, 1},
            new int[] {-6383, 12, 31},
            new int[] {-1, 1, 1},
            new int[] {-1, 1, 1, 0, 0, 0, 1},
            new int[] {-1, 1, 1, 0, 0, 0, 999999999},
            new int[] {-1, 1, 1, 0, 0, 1},
            new int[] {-1, 1, 1, 0, 1},
            new int[] {-1, 1, 1, 1},
            new int[] {0, 1, 1},
            new int[] {0, 1, 1, 0, 0, 0, 1},
            new int[] {0, 1, 1, 0, 0, 0, 9},
            new int[] {0, 1, 1, 0, 0, 1},
            new int[] {0, 1, 1, 0, 1},
            new int[] {0, 1, 1, 1},
            new int[] {1970, 1, 1},
            new int[] {1970, 1, 1, 0, 0, 0, 1},
            new int[] {1970, 1, 1, 0, 0, 0, 9},
            new int[] {1970, 1, 1, 0, 0, 1},
            new int[] {1970, 1, 1, 0, 1},
            new int[] {1970, 1, 1, 1},
            new int[] {2016, 10, 9},
            new int[] {2016, 10, 9, 20, 1, 47, 123},
            new int[] {2016, 10, 9, 20, 1, 47, 988545435},
            new int[] {2016, 10, 9, 20, 1, 48},
            new int[] {2016, 10, 9, 20, 2},
            new int[] {2016, 10, 9, 21},
            new int[] {9999, 12, 31},
            new int[] {9999, 12, 31, 23},
            new int[] {9999, 12, 31, 23, 59},
            new int[] {9999, 12, 31, 23, 59, 59},
            new int[] {9999, 12, 31, 23, 59, 59, 1},
            new int[] {9999, 12, 31, 23, 59, 59, 999999999},
        };

        Timestamp last = null;
        for (int[] comps : compsArray) {
            Timestamp ts = TimestampUtils.createTimestamp(comps);
            if (last != null) {
                assertTrue(last.compareTo(ts) < 0);
            }
            last = ts;
        }

        testInvalidComponents(new int[]{1970});
        testInvalidComponents(new int[]{1970, 1});
        testInvalidComponents(new int[]{-6384, 1, 1});
        testInvalidComponents(new int[]{10000, 1, 1});
        testInvalidComponents(new int[]{1970, 0, 1});
        testInvalidComponents(new int[]{1970, 13, 1});
        testInvalidComponents(new int[]{1970, 1, -1});
        testInvalidComponents(new int[]{1970, 1, 32});
        testInvalidComponents(new int[]{1970, 2, 30});
        testInvalidComponents(new int[]{1970, 1, 1, -1});
        testInvalidComponents(new int[]{1970, 1, 1, 24});
        testInvalidComponents(new int[]{1970, 1, 1, 0, -1});
        testInvalidComponents(new int[]{1970, 1, 1, 0, 60});
        testInvalidComponents(new int[]{1970, 1, 1, 0, 0, -1});
        testInvalidComponents(new int[]{1970, 1, 1, 0, 0, 60});
        testInvalidComponents(new int[]{1970, 1, 1, 0, 0, 0, -1});
        testInvalidComponents(new int[]{1970, 1, 1, 0, 0, 0, 1000000000});
    }

    private void testInvalidComponents(int[] components) {
        try {
            TimestampUtils.createTimestamp(components);
            fail("Expected to catch IllegalArgumentException but not: " +
                 Arrays.toString(components));
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test TimesatmpUtils.parseString().
     */
    @Test
    public void testParseStringWithDefaultPattern() {
        String[] strs = new String[] {
            "-6383-1-1",
            "-6383-01-01T00:00:00.000000000",
            "-2016-3-5T1",
            "-2016-3-5T01:03",
            "-2016-03-05T23:59:59.123456789",
            "2016-3-5",
            "2016-03-05",
            "2016-03-05T01",
            "2016-03-05T1:2",
            "2016-03-05T23:02:59",
            "2016-03-05T01:02:03.1",
            "2016-03-05T01:02:03.001",
            "2016-03-05T01:02:03.000001",
            "2016-03-05T01:02:03.987654321",
            "2016-3-5T1:2:3.000000001",
            "9999-12-31T23:59:59.999999999",
        };

        String[] expFmtStrs = new String[] {
            "-6383-01-01T00:00:00Z",
            "-6383-01-01T00:00:00Z",
            "-2016-03-05T01:00:00Z",
            "-2016-03-05T01:03:00Z",
            "-2016-03-05T23:59:59.123456789Z",
            "2016-03-05T00:00:00Z",
            "2016-03-05T00:00:00Z",
            "2016-03-05T01:00:00Z",
            "2016-03-05T01:02:00Z",
            "2016-03-05T23:02:59Z",
            "2016-03-05T01:02:03.1Z",
            "2016-03-05T01:02:03.001Z",
            "2016-03-05T01:02:03.000001Z",
            "2016-03-05T01:02:03.987654321Z",
            "2016-03-05T01:02:03.000000001Z",
            "9999-12-31T23:59:59.999999999Z",
        };

        for (int i = 0; i < strs.length; i++) {
            Timestamp ts = TimestampUtils.parseString(strs[i]);
            String ret = TimestampUtils.formatString(ts);
            assertTrue(ret.equals(expFmtStrs[i]));
        }

        testInvalidString("2016-01-01T");
        testInvalidString("2016-01-01T01:01:01.");
        testInvalidString("1000");
        testInvalidString("-");
        testInvalidString("-1000");
        testInvalidString("2016-01");
        testInvalidString("A-B-C");
        testInvalidString("1-1-2-1");
        testInvalidString("-6384-1-2");
        testInvalidString("10000-1-2");
        testInvalidString("9999-0-2");
        testInvalidString("2016-1-32");
        testInvalidString("2016-10-09T01:02:03:04");
        testInvalidString("2016-10-09TA");
        testInvalidString("2016-10-09T-1");
        testInvalidString("2016-10-09T24");
        testInvalidString("2016-10-09T23:-1");
        testInvalidString("2016-10-09T23:60");
        testInvalidString("2016-10-09T23:0:-1");
        testInvalidString("2016-10-09T23:59:60");
        testInvalidString("2016-10-09T23:59:0.0000000001");
        testInvalidString("2016-10-09T23:59:0.-1");
        testInvalidString("2016-10-09T23:-59:0");
        testInvalidString("2016-10-09T23:59:0.:1");
        testInvalidString("2016-10:09T23:59:0.:1");
        testInvalidString("2016-10-09T23:59:0T1");
        testInvalidString("2016-10T09T23:59:0T1");
        testInvalidString("2016-10-09T23:59:0.1.1");
        testInvalidString("2016-10.09T23:59:0.1.1");
        testInvalidString("2016-10T23:59");
    }

    /**
     * Test parse a Timestamp string with zone offset.
     */
    @Test
    public void testParseStringWithZoneOffset() {
        String[] strs = new String[] {
            "2017-12-05Z",
            "2017-12-05T01Z",
            "2017-12-05T1:2:0Z",
            "2017-12-05T01:02:03Z",
            "2017-12-05T01:02:03.123456789Z",
            "2017-12-05+01:00",
            "2017-12-05-02:01",
            "2017-12-05T12:39+00:00",
            "2017-12-05T01:02:03+00:00",
            "2017-12-05T01:02:03.123+00:00",
            "2017-12-01T01:02:03+02:01",
            "2017-12-01T01:02:03.123456789+02:01",
            "2017-12-01T01:02:03.0-03:00",
        };

        String[] expStrings = new String[] {
            "2017-12-05T00:00:00Z",
            "2017-12-05T01:00:00Z",
            "2017-12-05T01:02:00Z",
            "2017-12-05T01:02:03Z",
            "2017-12-05T01:02:03.123456789Z",
            "2017-12-04T23:00:00Z",
            "2017-12-05T02:01:00Z",
            "2017-12-05T12:39:00Z",
            "2017-12-05T01:02:03Z",
            "2017-12-05T01:02:03.123Z",
            "2017-11-30T23:01:03Z",
            "2017-11-30T23:01:03.123456789Z",
            "2017-12-01T04:02:03Z",
        };

        for (int i = 0; i < strs.length; i++) {
            Timestamp ts = TimestampUtils.parseString(strs[i]);
            String ret = TimestampUtils.formatString(ts);
            assertEquals(expStrings[i], ret);
        }
    }

    private void testInvalidString(String str) {
        try {
            TimestampUtils.parseString(str);
            fail("Expected to catch IllegalArgumentException but not: " + str);
        } catch (IllegalArgumentException iae)  {
        }
    }

    @Test
    public void testInterval() {
        String sval;
        Interval val;

        /*
         * Interval.parseString(String)
         */
        sval = "10 year 12 months 30 days 23 hours 59 minutes 59 seconds " +
               "500 milliseconds 999999 nanoseconds";
        val = Interval.parseString(sval);
        checkInterval(val, Period.of(10, 12, 0),
                      Duration.ofDays(30).plusHours(23).plusMinutes(59)
                          .plusSeconds(59).plusMillis(500)
                          .plusNanos(999999));
        /* upper case */
        val = Interval.parseString(sval.toUpperCase());
        checkInterval(val, Period.of(10, 12, 0),
                      Duration.ofDays(30).plusHours(23).plusMinutes(59)
                           .plusSeconds(59).plusMillis(500)
                           .plusNanos(999999));

        sval = "1 year 0 month";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ofYears(1), Duration.ZERO);

        sval = "- 10 years 6 months 5 days";
        val = Interval.parseString(sval);
        checkInterval(val, Period.of(-10, -6, 0), Duration.ofDays(-5));

        sval = "1 month 15 days";
        val = Interval.parseString(sval);
        checkInterval(val, Period.of(0, 1, 0), Duration.ofDays(15));

        sval = "-10 days";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofDays(-10));

        sval = "24 hours";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofHours(24));

        sval = "-10 minutes";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofMinutes(-10));

        sval = "5 minutes 30 seconds";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofMinutes(5).plusSeconds(30));

        sval = "- 1 minute 300 seconds 4 milliseconds";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO,
                      Duration.ofMinutes(1).plusSeconds(300)
                              .plusMillis(4).negated());

        sval = "12 hours 4 nanoseconds";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofHours(12).plusNanos(4));
        assertEquals(sval, val.toString());

        sval = "- 999 milliseconds";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ZERO, Duration.ofMillis(-999));
        assertEquals(sval, val.toString());

        sval = "9999 years 12 month  31 days 23 hours 59 minutes 59 seconds  " +
               "999999999 nanoseconds ";
        val = Interval.parseString(sval);
        checkInterval(val, Period.of(9999, 12, 0),
                      Duration.ofDays(31).plusHours(23).plusMinutes(59)
                          .plusSeconds(59).plusNanos(999999999));

        sval = "-      6383       years ";
        val = Interval.parseString(sval);
        checkInterval(val, Period.ofYears(-6383), Duration.ZERO);

        /*
         * Invalid interval strings
         */
        invalidInterval("1 hours 2");
        invalidInterval("1 hours -2 minutes");
        invalidInterval("1 hours 2 minutes 2 hours");
        invalidInterval("1 hours 2 hours");
        invalidInterval("1 months 1 year");
        invalidInterval("1 mins");
        invalidInterval("10000000000 days");
        invalidInterval("X day");

        /*
         * Interval.ofMilliseconds(long)
         */
        long millis;

        val = Interval.ofMilliseconds(0);
        checkInterval(val, Period.ZERO, Duration.ZERO);

        millis = System.currentTimeMillis();
        val = Interval.ofMilliseconds(millis);
        checkInterval(val, Period.ZERO, Duration.ofMillis(millis));

        millis = TimestampDefImpl.minValues[3].getTime();
        val = Interval.ofMilliseconds(millis);
        checkInterval(val, Period.ZERO, Duration.ofMillis(millis));

        millis = TimestampDefImpl.maxValues[3].getTime();
        val = Interval.ofMilliseconds(millis);
        checkInterval(val, Period.ZERO, Duration.ofMillis(millis));

        millis = TimestampDefImpl.MAX_VALUE.getTime() -
                 TimestampDefImpl.MIN_VALUE.getTime();
        val = Interval.ofMilliseconds(millis);
        checkInterval(val, Period.ZERO, Duration.ofMillis(millis));

        millis = -1 * millis;
        val = Interval.ofMilliseconds(millis);
        checkInterval(val, Period.ZERO, Duration.ofMillis(millis));
    }

    private void checkInterval(Interval itv, Period expYmd, Duration expTime) {
        assertEquals(expYmd, itv.getPeriod());
        assertEquals(expTime, itv.getTime());

        /* Round trip of interval and string */
        Interval itv1 = Interval.parseString(itv.toString());
        assertEquals(expYmd, itv1.getPeriod());
        assertEquals(expTime, itv1.getTime());
    }

    private void invalidInterval(String intervalStr) {
        try {
            Interval.parseString(intervalStr);
            fail("The interval string should be parsed failed: " + intervalStr);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
    }

    @Test
    public void testTimestampArith() {

        String from = "2021-11-19T23:59:59.999999999";
        doTimestampArith(from, "1 year 2 months 3 days 1 hours 30 minutes " +
                               "60 seconds 500 milliseconds 1 nanoseconds",
                         "2023-01-23T01:31:00.5");

        doTimestampArith(from, "1 year 2 months 3 days",
                         "2023-01-22T23:59:59.999999999");

        doTimestampArith(from, "-2 months 10 days 12 hours 999 milliseconds",
                         "2021-09-09T11:59:59.000999999");

        doTimestampArith(from, "1 hours 1 nanoseconds", "2021-11-20T01:00:00");
        doTimestampArith(from, "-30 minutes", "2021-11-19T23:29:59.999999999");

        doTimestampArith(from, "-7 hours 10 minutes 12345678 nanoseconds",
                         "2021-11-19T16:49:59.987654321");

        doTimestampArith("2020-02-28", "1 day", "2020-02-29");
        doTimestampArith("2021-02-28", "1 day", "2021-03-01");

        doTimestampArith("9999-12-31T23:59:59.999999999",
                         "- 16382 years 11 months 30 days 23 hours 59 minutes" +
                         " 59 seconds 999999999 nanoseconds",
                         "-6383-01-01");

        doTimestampArith("-6383-01-01T00:00:00.001",
                         "16382 years 11 months 30 days 23 hours " +
                         "59 minutes 59 seconds 998999999 nanoseconds",
                         "9999-12-31T23:59:59.999999999");
    }

    private void doTimestampArith(String tsStr, String itvStr, String expRes) {
        int prec = 0;
        int pos = tsStr.indexOf(".");
        if (pos > 0) {
            prec = tsStr.length() - pos - 1;
        }

        /*
         * test timestampAdd()
         */
        TimestampValueImpl val = FieldDefImpl.getTimeDef(prec).fromString(tsStr);
        Interval itv = Interval.parseString(itvStr);
        TimestampValueImpl res = TimestampUtils.timestampAdd(val, itv);

        assertTrue("Expect " +
                   TimestampUtils.formatString(
                       TimestampUtils.parseString(expRes)) +
                   ", actual " + res.toString(),
                   TimestampUtils.parseString(expRes).compareTo(res.get()) == 0);


        /*
         * test timestampDiff()
         */
        long msecs = TimestampUtils.timestampDiff(res, val);
        long tsDiff = res.get().getTime() - val.get().getTime();
        /* There might be a delta of 1 millisecond */
        assertTrue(tsDiff - msecs <= 1);

        TimestampDefImpl def3 = FieldDefImpl.getTimeDef(3);
        TimestampValueImpl t1 = def3.createTimestamp(res.get());
        TimestampValueImpl t2 = def3.createTimestamp(val.get());
        msecs = TimestampUtils.timestampDiff(t1, t2);
        tsDiff = t1.get().getTime() - t2.get().getTime();
        assertEquals(tsDiff, msecs);
    }

    @Test
    public void testRoundToUnit() {
        String[] values;
        Map<RoundMode, String> boundaries;

        /* YEAR unit */
        values = new String[] {
            "2024-01-01",
            "2024-06-30T23:59:59.999999999",
            "2024-07-01",
            "2024-12-31T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-01-01",
                            RoundMode.HALF_UP, "2024-07-01",
                            RoundMode.UP, "2025-01-01");

        runTestRoundToUnit(values, RoundUnit.YEAR, boundaries);

        /* IYEAR unit */
        values = new String[] {
            "2024-12-30",
            "2025-06-30T23:59:59.999999999",
            "2025-07-01",
            "2025-12-28T23:59:59.999999999"
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-12-30",
                            RoundMode.HALF_UP, "2025-07-01",
                            RoundMode.UP, "2025-12-29");

        runTestRoundToUnit(values, RoundUnit.IYEAR, boundaries);

        values = new String[] {
            "2025-12-29",
            "2026-06-30T23:59:59.999999999",
            "2026-07-01",
            "2027-01-03T23:59:59.999999999"
        };
        boundaries = Map.of(RoundMode.DOWN, "2025-12-29",
                            RoundMode.HALF_UP, "2026-07-01",
                            RoundMode.UP, "2027-01-04");

        runTestRoundToUnit(values, RoundUnit.IYEAR, boundaries);

        /* QUARTER unit */
        values = new String[] {
            "2024-04-01",
            "2024-05-15T23:59:59.999999999",
            "2024-05-16",
            "2024-06-30T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-04-01",
                            RoundMode.HALF_UP, "2024-05-16",
                            RoundMode.UP, "2024-07-01");

        runTestRoundToUnit(values, RoundUnit.QUARTER, boundaries);

        /* MONTH unit */
        values = new String[] {
            "2024-02-01",
            "2024-02-15T23:59:59.999999999",
            "2024-02-16",
            "2024-02-29T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-02-01",
                            RoundMode.HALF_UP, "2024-02-16",
                            RoundMode.UP, "2024-03-01");

        runTestRoundToUnit(values, RoundUnit.MONTH, boundaries);

        /* WEEK unit */
        values = new String[] {
            "2025-01-01",
            "2025-01-04T11:59:59.999999999",
            "2025-01-04T12",
            "2025-01-07T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2025-01-01",
                            RoundMode.HALF_UP, "2025-01-04T12",
                            RoundMode.UP, "2025-01-08");

        runTestRoundToUnit(values, RoundUnit.WEEK, boundaries);

        values = new String[] {
            "2025-02-12",
            "2025-02-15T11:59:59.999999999",
            "2025-02-15T12",
            "2025-02-18T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2025-02-12",
                            RoundMode.HALF_UP, "2025-02-15T12",
                            RoundMode.UP, "2025-02-19");

        runTestRoundToUnit(values, RoundUnit.WEEK, boundaries);

        /* IWEEK unit */
        values = new String[] {
            "2024-02-12",
            "2024-02-15T11:59:59.999999999",
            "2024-02-15T12",
            "2024-02-18T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-02-12",
                            RoundMode.HALF_UP, "2024-02-15T12",
                            RoundMode.UP, "2024-02-19");

        runTestRoundToUnit(values, RoundUnit.IWEEK, boundaries);

        /* DAY unit */
        values = new String[] {
            "2024-02-15",
            "2024-02-15T11:59:59.999999999",
            "2024-02-15T12:00:00",
            "2024-02-15T23:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-02-15",
                            RoundMode.HALF_UP, "2024-02-15T12:00:00",
                            RoundMode.UP, "2024-02-16");

        runTestRoundToUnit(values, RoundUnit.DAY, boundaries);

        /* Test default unit DAY */
        runTestRoundToUnit(values, null, boundaries);

        /* HOUR unit */
        values = new String[] {
            "2024-02-15T10",
            "2024-02-15T10:29:59.999999999",
            "2024-02-15T10:30:00",
            "2024-02-15T10:59:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-02-15T10",
                            RoundMode.HALF_UP, "2024-02-15T10:30:00",
                            RoundMode.UP, "2024-02-15T11");

        runTestRoundToUnit(values, RoundUnit.HOUR, boundaries);

        /* MINUTE unit */
        values = new String[] {
            "2024-02-15T10",
            "2024-02-15T10:00:29.999999999",
            "2024-02-15T10:00:30",
            "2024-02-15T10:00:59.999999999",
        };
        boundaries = Map.of(RoundMode.DOWN, "2024-02-15T10",
                            RoundMode.HALF_UP, "2024-02-15T10:00:30",
                            RoundMode.UP, "2024-02-15T10:01");

        runTestRoundToUnit(values, RoundUnit.MINUTE, boundaries);

        /* SECOND unit */
        values = new String[] {
            "2024-02-15T10:00:00",
            "2024-02-15T10:00:00.499999999",
            "2024-02-15T10:00:00.5",
            "2024-02-15T10:00:00.999999999",
        };

        boundaries = Map.of(RoundMode.DOWN, "2024-02-15T10:00:00",
                            RoundMode.HALF_UP, "2024-02-15T10:00:00.5",
                            RoundMode.UP, "2024-02-15T10:00:01");

        runTestRoundToUnit(values, RoundUnit.SECOND, boundaries);
    }

    private void runTestRoundToUnit(String[] values,
                                    RoundUnit unit,
                                    Map<RoundMode, String> boundaries) {

        Timestamp val;
        Timestamp ret;
        Timestamp exp;
        Timestamp boundary;
        Timestamp leftBoundary = parseString(boundaries.get(RoundMode.DOWN));

        for (RoundMode mode : RoundMode.values()) {
            boundary = parseString(boundaries.get(mode));
            for (String str : values) {
                val = parseString(str);
                ret = TimestampUtils.round(val, unit, mode);
                if (val.compareTo(leftBoundary) == 0) {
                    exp = leftBoundary;
                } else {
                    if (mode == RoundMode.HALF_UP) {
                        if (val.compareTo(boundary) >= 0) {
                            exp = parseString(boundaries.get(RoundMode.UP));
                        } else {
                            exp = parseString(boundaries.get(RoundMode.DOWN));
                        }
                    } else {
                        exp = parseString(boundaries.get(mode));
                    }
                }

                assertEquals("round(" + str + ", " + mode +
                             ") exp=" + TimestampUtils.formatString(exp) +
                             ", act=" + TimestampUtils.formatString(ret),
                             exp, ret);
            }
        }
    }

    @Test
    public void testGetCurrentBucket() {
        String[] timestampStrs = new String[] {
            "-6383-01-01",
            "-1000-02-23",
            "1969-12-31T23:59:59.999999999",
            "1970-01-01",
            "1970-01-01T00:00:00.000000001",
            "2024-04-23T11:29:29.000000001",
            "2024-04-23T11:29:29.5",
            "2024-04-23T11:29:30",
            "2024-04-23T11:30",
            "2024-04-23T12",
            "9999-12-31T23:59:59.999999999"
        };

        RoundUnit[] units = new RoundUnit[] {
            RoundUnit.WEEK,
            RoundUnit.DAY,
            RoundUnit.HOUR,
            RoundUnit.MINUTE,
            RoundUnit.SECOND
        };

        int[] unitNums = new int[] {1, 2, 5, 15};
        String[] originStrs = new String[] {
            "-6383-01-01",
            "1970-01-01",
            "1970-01-01T00:00:00.000000001",
            "1969-12-31T23:59:59.999999999",
            "1970-01-01T10:00:01.999",
            "2024-04-15",
            "2024-04-15T10:30:01.000000001",
            "2024-04-15T11:29:29",
            "9999-12-31T23:59:59.999999999",
        };

        BucketInterval interval;
        Timestamp ts;
        Timestamp origin;
        Timestamp bucket;
        for (String timestampStr : timestampStrs) {

            ts = TimestampUtils.parseString(timestampStr);

            for (String originStr : originStrs) {
                origin = TimestampUtils.parseString(originStr);

                for (int num : unitNums) {
                    for (RoundUnit unit : units) {
                        interval = new BucketInterval(num, unit);
                        bucket = TimestampUtils.getCurrentBucket(ts, interval,
                                                                 origin);
                        assertTrue(bucket.compareTo(ts) <= 0);

                        long bucketSize = TimestampUtils.getBucketSize(interval);
                        long originSecs = TimestampUtils.getSeconds(origin);
                        long bucketSecs = TimestampUtils.getSeconds(bucket);
                        long tsSecs = TimestampUtils.getSeconds(ts);
                        long diff = tsSecs - bucketSecs;

                        assertTrue((diff < bucketSize) ||
                                   (diff == bucketSize &&
                                       ts.getNanos() < bucket.getNanos()));
                        assertEquals(0, (bucketSecs - originSecs) % bucketSize);
                        assertEquals(origin.getNanos(), bucket.getNanos());
                    }
                }
            }
        }
    }

    @Test
    public void testInvalidArbitraryInterval() {
        String[] intervalStrs = {
            "abc",
            "1 day s",
            "0 day",
            "-1 day",
            "2 iweeks",
            "3 months"
        };

        for (String str : intervalStrs) {
            try {
                BucketInterval.parseString(str);
                fail("Expect to get IAE but not: " + str);
            } catch (IllegalArgumentException iae) {
                /* expect */
            }
        }
    }

    @Test
    public void testExtractFunctions() {
        Timestamp ts0 = TimestampUtils.parseString("2024-02-15T12:25:01.999");
        Timestamp ts1 = TimestampUtils.parseString("2024-01-01");
        Timestamp ts2 = TimestampUtils.parseString("2024-12-31");

        /* last_day */
        assertEquals(TimestampUtils.parseString("2024-02-29"),
                     TimestampUtils.getLastDay(ts0));
        assertEquals(TimestampUtils.parseString("2024-01-31"),
                     TimestampUtils.getLastDay(ts1));
        assertEquals(TimestampUtils.parseString("2024-12-31"),
                     TimestampUtils.getLastDay(ts2));

        /* quarter */
        assertEquals(1, TimestampUtils.getQuarter(ts0));
        assertEquals(1, TimestampUtils.getQuarter(ts1));
        assertEquals(4, TimestampUtils.getQuarter(ts2));

        /* day_of_month */
        assertEquals(15, TimestampUtils.getDayOfMonth(ts0));
        assertEquals(1, TimestampUtils.getDayOfMonth(ts1));
        assertEquals(31, TimestampUtils.getDayOfMonth(ts2));

        /* day_of_week */
        assertEquals(4, TimestampUtils.getDayOfWeek(ts0));
        assertEquals(1, TimestampUtils.getDayOfWeek(ts1));
        assertEquals(2, TimestampUtils.getDayOfWeek(ts2));

        /* day_of_year */
        assertEquals(46, TimestampUtils.getDayOfYear(ts0));
        assertEquals(1, TimestampUtils.getDayOfYear(ts1));
        assertEquals(366, TimestampUtils.getDayOfYear(ts2));
    }
}
