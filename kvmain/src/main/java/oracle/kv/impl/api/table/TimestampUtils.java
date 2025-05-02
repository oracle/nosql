/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.api.table;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Locale;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;

import oracle.kv.table.TimestampDef;

/**
 * This class provides utility methods for Timestamp value:
 *  - Serialization and deserialization.
 *  - Convert to/from a string
 *  - Other facility methods.
 */
public class TimestampUtils {

    public final static String UTC_SUFFIX = "Z";

    /* The max precision of Timestamp type. */
    final static int MAX_PRECISION = TimestampDefImpl.MAX_PRECISION;

    /* The UTC zone */
    final static ZoneId UTCZone = ZoneId.of(ZoneOffset.UTC.getId());

    /* The local zone */
    final static ZoneId localZone = ZoneId.systemDefault();

    /* The default Locale used by DateTimeFormatter */
    private final static Locale DEFAULT_LOCALE = Locale.ENGLISH;

    /*
     * The position and the number of bits for each component of Timestamp for
     * serialization
     */
    private final static int YEAR_POS = 0;
    private final static int YEAR_BITS = 14;

    private final static int MONTH_POS = YEAR_POS + YEAR_BITS;
    private final static int MONTH_BITS = 4;

    private final static int DAY_POS = MONTH_POS + MONTH_BITS;
    private final static int DAY_BITS = 5;

    private final static int HOUR_POS = DAY_POS + DAY_BITS;
    private final static int HOUR_BITS = 5;

    private final static int MINUTE_POS = HOUR_POS + HOUR_BITS;
    private final static int MINUTE_BITS = 6;

    private final static int SECOND_POS = MINUTE_POS + MINUTE_BITS;
    private final static int SECOND_BITS = 6;

    private final static int NANO_POS = SECOND_POS + SECOND_BITS;

    private final static int YEAR_EXCESS = 6384;

    private final static int NO_FRACSECOND_BYTES = 5;

    final static int MAX_BYTES = 9;

    private final static char compSeparators[] = { '-', '-', 'T', ':', ':', '.'};

    private final static String compNames[] = {
        "year", "month", "day", "hour", "minute", "second", "fractional second"
    };

    /* The rounding behavior */
    public static enum RoundMode {
        DOWN,
        HALF_UP,
        UP
    }

    /*
     * The default origin timestamp which the arbitrary interval buckets
     * align to is 1970-01-01
     */
    private static final Timestamp DEF_ROUND_ORIGIN = new Timestamp(0);

    /* The default rounding unit: DAY */
    private static final RoundUnit DEF_ROUND_UNIT = RoundUnit.DAY;

    /* The default bucket interval: 1 DAY */
    private static final BucketInterval DEF_BUCKET_INTERVAL =
            new BucketInterval(1, RoundUnit.DAY);

    /**
     * Methods to serialize/deserialize Timestamp to/from a byte array.
     */

     /**
     * Serialize a Timestamp value to a byte array:
     *
     * Variable-length bytes from 3 to 9 bytes:
     *  bit[0~13]  year - 14 bits
     *  bit[14~17] month - 4 bits
     *  bit[18~22] day - 5 bits
     *  bit[23~27] hour - 5 bits        [optional]
     *  bit[28~33] minute - 6 bits      [optional]
     *  bit[34~39] second - 6 bits      [optional]
     *  bit[40~71] fractional second    [optional with variable length]
     */
    static byte[] toBytes(Timestamp value, int precision) {

        final ZonedDateTime zdt = toUTCDateTime(value);
        int fracSeconds = fracSecondToPrecision(zdt.getNano(), precision);
        return toBytes(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
                       zdt.getHour(), zdt.getMinute(), zdt.getSecond(),
                       fracSeconds, precision);
    }

    public static byte[] toBytes(int year, int month, int day,
                                 int hour, int minute, int second,
                                 int fracSeconds, int precision) {

        byte[] bytes = new byte[getNumBytes(precision)];
        /* Year */
        writeBits(bytes, year + YEAR_EXCESS, YEAR_POS, YEAR_BITS);
        /* Month */
        writeBits(bytes, month, MONTH_POS, MONTH_BITS);
        /* Day */
        int pos = writeBits(bytes, day, DAY_POS, DAY_BITS);
        /* Hour */
        if (hour > 0) {
            pos = writeBits(bytes, hour, HOUR_POS, HOUR_BITS);
        }
        /* Minute */
        if (minute > 0) {
            pos = writeBits(bytes, minute, MINUTE_POS, MINUTE_BITS);
        }
        /* Second */
        if (second> 0) {
            pos = writeBits(bytes, second, SECOND_POS, SECOND_BITS);
        }
        /* Fractional second */
        if (fracSeconds > 0) {
            pos = writeBits(bytes, fracSeconds, NANO_POS,
                            getFracSecondBits(precision));
        }

        int nbytes = pos / 8 + 1;
        return (nbytes < bytes.length) ? Arrays.copyOf(bytes, nbytes): bytes;
    }

    static int fracSecondToPrecision(int nanos, int precision) {
        if (precision == 0) {
            return 0;
        }
        if (precision == MAX_PRECISION) {
            return nanos;
        }
        return nanos / (int)Math.pow(10, MAX_PRECISION - precision);
    }

    /**
     * Deserialize a Timestamp value from a byte array.
     */
    static Timestamp fromBytes(byte[] bytes, int precision) {
        int[] comps = extractFromBytes(bytes, precision);
        return createTimestamp(comps);
    }

    /**
     * Extracts the components of Timestamp from a byte array including year,
     * month, day, hour, minute, second and fractional second.
     */
    private static int[] extractFromBytes(byte[] bytes, int precision) {
        int[] comps = new int[7];
        /* Year */
        comps[0] = getYear(bytes);
        /* Month */
        comps[1] = getMonth(bytes);
        /* Day */
        comps[2] = getDay(bytes);
        /* Hour */
        comps[3] = getHour(bytes);
        /* Minute */
        comps[4] = getMinute(bytes);
        /* Second */
        comps[5] = getSecond(bytes);
        /* Nano */
        comps[6] = getNano(bytes, precision);
        return comps;
    }

    /**
     * Reads the year value from byte array.
     */
    static int getYear(byte[] bytes) {
        return readBits(bytes, YEAR_POS, YEAR_BITS) - YEAR_EXCESS;
    }

    /**
     * Reads the month value from byte array.
     */
    static int getMonth(byte[] bytes) {
        return readBits(bytes, MONTH_POS, MONTH_BITS);
    }

    /**
     * Reads the day of month value from byte array.
     */
    static int getDay(byte[] bytes) {
        return readBits(bytes, DAY_POS, DAY_BITS);
    }

    /**
     * Reads the hour value from byte array.
     */
    static int getHour(byte[] bytes) {
        if (HOUR_POS < bytes.length * 8) {
            return readBits(bytes, HOUR_POS, HOUR_BITS);
        }
        return 0;
    }

    /**
     * Reads the minute value from byte array.
     */
    static int getMinute(byte[] bytes) {
        if (MINUTE_POS < bytes.length * 8) {
            return readBits(bytes, MINUTE_POS, MINUTE_BITS);
        }
        return 0;
    }

    /**
     * Reads the second value from byte array.
     */
    static int getSecond(byte[] bytes) {
        if (SECOND_POS < bytes.length * 8) {
            return readBits(bytes, SECOND_POS, SECOND_BITS);
        }
        return 0;
    }

    /**
     * Reads the nanoseconds value from byte array.
     */
    static int getNano(byte[] bytes, int precision) {
        int fracSecond = getFracSecond(bytes, precision);
        if (fracSecond > 0 && precision < MAX_PRECISION) {
            fracSecond *= (int)Math.pow(10, MAX_PRECISION - precision);
        }
        return fracSecond;
    }

    /**
     * Get the fractional seconds from byte array
     */
    static int getFracSecond(byte[] bytes, int precision) {
        if (NANO_POS < bytes.length * 8) {
            int num = getFracSecondBits(precision);
            return readBits(bytes, NANO_POS, num);
        }
        return 0;
    }

    /**
     * Returns the week number within the year where a week starts on Sunday and
     * the first week has a minimum of 1 day in this year, in the range 1 ~ 54.
     */
    public static int getWeekOfYear(Timestamp ts) {
        return toUTCDateTime(ts).get(WeekFields.SUNDAY_START.weekOfYear());
    }

    /**
     * Returns the week number within the year based on IS0-8601 where a week
     * starts on Monday and the first week has a minimum of 4 days in this year,
     * in the range 1 ~ 53.
     */
    public static int getISOWeekOfYear(Timestamp ts) {
        return toUTCDateTime(ts).get(WeekFields.ISO.weekOfWeekBasedYear());
    }

    /**
     * Returns the last day of the month that contains the timestamp
     */
    public static Timestamp getLastDay(Timestamp ts) {
        ZonedDateTime zdt = toUTCDateTime(ts)
                                .with(TemporalAdjusters.lastDayOfMonth())
                                .truncatedTo(ChronoUnit.DAYS);
        return createTimestamp(zdt.toEpochSecond(), zdt.getNano());
    }

    /**
     * Returns the quarter of the year for the timestamp, in the range 1 ~ 4
     */
    public static int getQuarter(Timestamp ts) {
        return toUTCDateTime(ts).get(IsoFields.QUARTER_OF_YEAR);
    }

    /**
     * Returns the weekday index for the timestamp, from 1 to 7
     */
    public static int getDayOfWeek(Timestamp ts) {
        return toUTCDateTime(ts).getDayOfWeek().getValue();
    }

    /**
     * Returns the day of month for the timestamp, from 1 to 31
     */
    public static int getDayOfMonth(Timestamp ts) {
        return toUTCDateTime(ts).getDayOfMonth();
    }

    /**
     * Returns the day of year for the timestamp, from 1 to 365, or 366 in a
     * leap year
     */
    public static int getDayOfYear(Timestamp ts) {
        return toUTCDateTime(ts).getDayOfYear();
    }

    /*
     * Rounds the timestamp to the round interval specified by the interval,
     * the rounding behavior can be DOWN, HALF_UP and UP.
     *
     *  - YEAR
     *    returns the 1st day of a year, rounds up on July 1st
     *
     *  - IYEAR
     *    returns the 1st day of a year, rounds up on July 1st
     *
     *  - QUARTER
     *    returns 1st day of the quarter, rounds up on the 16th day of the 2nd
     *    month of the quarter
     *
     *  - MONTH
     *    returns the 1st day of the Month, rounds up on the 16th day
     *
     *  - WEEK
     *    returns same day of the week as the first day of the year, rounds up
     *    on the midday(12pm) of the 4th day in week
     *
     *  - IWEEK
     *    returns same day of the week as the first day of the calendar week as
     *    defined by the ISO 8601 standard, which is Monday, rounds up on the
     *    midday(12pm) of the 4th day in week
     *
     *  - DAY
     *    returns the beginning of the current day(12am), rounds up on the
     *    midday(12pm) of the current day
     *
     *  - HOUR
     *    returns the beginning of the current hour, rounds up on half hour of
     *    the current hour
     *
     *  - MINUTE
     *    returns the beginning of the current minute, rounds up on 30 seconds
     *    of the current minute
     *
     *  - SECOND
     *    returns the beginning of the current second, rounds up on 500
     *    milliseconds of the current second
     */
    public static Timestamp round(Timestamp ts, RoundUnit unit, RoundMode mode) {
        if (unit == null) {
            unit = DEF_ROUND_UNIT;
        }

        ZonedDateTime dt = toUTCDateTime(ts);
        ZonedDateTime bucket = getCurrentBucket(dt, unit);

        boolean roundUp = false;
        if (mode != RoundMode.DOWN && dt.compareTo(bucket) != 0) {
            if (mode == RoundMode.HALF_UP) {
                roundUp = needRoundUp(dt, bucket, unit);
            } else {
                roundUp = true;
            }
        }
        if (roundUp) {
            bucket = getNextBucket(bucket, unit);
        }
        return Timestamp.from(bucket.toInstant());
    }

    private static ZonedDateTime getCurrentBucket(ZonedDateTime dt,
                                                  RoundUnit unit) {
        ChronoUnit truncUnit = ChronoUnit.DAYS;
        switch(unit) {
        case YEAR:
            dt = dt.with(TemporalAdjusters.firstDayOfYear());
            break;
        case IYEAR:
            dt = dt.with(WeekFields.ISO.weekOfWeekBasedYear(), 1)
                   .with(WeekFields.ISO.dayOfWeek(), 1);
            break;
        case QUARTER:
            dt = dt.with(IsoFields.DAY_OF_QUARTER, 1L);
            break;
        case MONTH:
            dt = dt.with(TemporalAdjusters.firstDayOfMonth());
            break;
        case WEEK:
            DayOfWeek weekDay = dt.with(TemporalAdjusters.firstDayOfYear())
                                  .getDayOfWeek();
            if (dt.getDayOfWeek() != weekDay) {
                dt = dt.with(TemporalAdjusters.previous(weekDay));
            }
            break;
        case IWEEK:
            dt = dt.with(DayOfWeek.MONDAY);
            break;
        case DAY:
            break;
        case HOUR:
            truncUnit = ChronoUnit.HOURS;
            break;
        case MINUTE:
            truncUnit = ChronoUnit.MINUTES;
            break;
        case SECOND:
            truncUnit = ChronoUnit.SECONDS;
            break;
        default:
            throw new IllegalArgumentException(
                    "Unsupported round unit: " + unit);
        }
        return dt.truncatedTo(truncUnit);
    }

    /* Returns the beginning of current bucket that contains the Timestamp */
    public static Timestamp getCurrentBucket(Timestamp ts,
                                             BucketInterval interval,
                                             Timestamp origin) {

        if (interval == null) {
            interval = DEF_BUCKET_INTERVAL;
        }
        if (origin == null) {
            origin = DEF_ROUND_ORIGIN;
        }

        long bucketSize = getBucketSize(interval);
        long diff = (getSeconds(ts) - getSeconds(origin)) % bucketSize;
        if (diff < 0) {
            diff = bucketSize + diff;
        } else if (diff == 0) {
            /*
             * If the timestamp is less than the beginning of bucket, adjust to
             * the previous bucket.
             */
            if (ts.getNanos() < origin.getNanos()) {
                diff = bucketSize;
            }
        }

        ts = new Timestamp((getSeconds(ts) - diff) * 1000);
        ts.setNanos(origin.getNanos());
        return ts;
    }

    /* Returns the bucket size in seconds */
    static long getBucketSize(BucketInterval interval) {
        switch(interval.unit) {
        case WEEK:
            return interval.num * 604800;
        case DAY:
            return interval.num * 86400;
        case HOUR:
            return interval.num * 3600;
        case MINUTE:
            return interval.num * 60;
        case SECOND:
            return interval.num;
        default:
            throw new IllegalArgumentException(
                "Unsupported round unit with arbitrary interval: " + interval);
        }
    }

    /* Returns the beginning of next bucket */
    static ZonedDateTime getNextBucket(ZonedDateTime bucket,
                                       RoundUnit unit) {
        switch (unit) {
        case YEAR:
            return bucket.with(TemporalAdjusters.firstDayOfNextYear());
        case IYEAR:
            return bucket.with(IsoFields.WEEK_BASED_YEAR,
                               bucket.get(IsoFields.WEEK_BASED_YEAR) + 1);
        case QUARTER:
            return bucket.plusMonths(3);
        case MONTH:
            return bucket.with(TemporalAdjusters.firstDayOfNextMonth());
        case IWEEK:
            return bucket.with(TemporalAdjusters.next(DayOfWeek.MONDAY));
        case WEEK:
            return bucket.with(TemporalAdjusters.next(bucket.getDayOfWeek()));
        case DAY:
            return bucket.plusDays(1);
        case HOUR:
            return bucket.plusHours(1);
        case MINUTE:
            return bucket.plusMinutes(1);
        case SECOND:
            return bucket.plusSeconds(1);
        default:
            throw new IllegalArgumentException(
                    "Unsupported round unit: " + unit);
        }
    }

    /* Returns true if dt >= the middle of current bucket to do rounding up */
    private static boolean needRoundUp(ZonedDateTime dt,
                                       ZonedDateTime bucket,
                                       RoundUnit unit) {

        switch(unit) {
        case YEAR:
            return dt.getMonthValue() >= 7;
        case IYEAR: {
            ZonedDateTime middle =
                dt.with(WeekFields.ISO.weekOfWeekBasedYear(), 2)
                   .with(IsoFields.QUARTER_OF_YEAR, 3)
                   .with(IsoFields.DAY_OF_QUARTER, 1)
                   .truncatedTo(ChronoUnit.DAYS);
            return dt.compareTo(middle) >= 0;
        }
        case QUARTER:
            int middleMonth = dt.get(IsoFields.QUARTER_OF_YEAR) * 3 - 1;
            return (dt.getMonthValue() > middleMonth ||
                    (dt.getMonthValue() == middleMonth &&
                     dt.getDayOfMonth() >= 16));
        case MONTH:
            return dt.getDayOfMonth() >= 16;
        case IWEEK: {
            int ret = dt.getDayOfWeek().compareTo(DayOfWeek.THURSDAY);
            return ret > 0 || (ret == 0 && dt.getHour() >= 12);
        }
        case WEEK: {
            ZonedDateTime middle = bucket.plusDays(3)
                                         .truncatedTo(ChronoUnit.DAYS)
                                         .plusHours(12);
            return dt.compareTo(middle) >= 0;
        }
        case DAY:
            return dt.getHour() >= 12;
        case HOUR:
            return dt.getMinute() >= 30;
        case MINUTE:
            return dt.getSecond() >= 30;
        case SECOND:
            return dt.getNano() >= 500_000_000;
        default:
            throw new IllegalArgumentException(
                    "Unsupported round unit: " + unit);
        }
    }

    /**
     * Compares 2 byte arrays ignoring the trailing bytes with zero.
     */
    static int compareBytes(byte[] bs1, byte[] bs2) {

        int size = Math.min(bs1.length, bs2.length);
        int ret = compare(bs1, bs2, size);
        if (ret != 0) {
            return ret;
        }

        ret = bs1.length - bs2.length;
        if (ret != 0) {
            byte[] bs = (ret > 0) ? bs1 : bs2;
            for (int i = size; i < bs.length; i++) {
                 if (bs[i] != (byte)0x0) {
                    return ret;
                 }
            }
        }
        return 0;
    }

    /**
     * Compares 2 byte arrays, they can be with different precision.
     */
    static int compareBytes(byte[] bs1, int precision1,
                            byte[] bs2, int precision2) {

        if (precision1 == precision2 ||
            bs1.length <= NO_FRACSECOND_BYTES ||
            bs2.length <= NO_FRACSECOND_BYTES) {

            return compareBytes(bs1, bs2);
        }

        /* Compares the date and time without fractional second part */
        assert (bs1.length > NO_FRACSECOND_BYTES);
        assert (bs2.length > NO_FRACSECOND_BYTES);
        int ret = compare(bs1, bs2, NO_FRACSECOND_BYTES);
        if (ret != 0) {
            return ret;
        }

        /* Compares the fractional seconds */
        int fs1 = getFracSecond(bs1, precision1);
        int fs2 = getFracSecond(bs2, precision2);
        int base = (int)Math.pow(10, Math.abs(precision1 - precision2));
        return (precision1 < precision2) ?
                (fs1 * base - fs2) : -1 * (fs2 * base - fs1);
    }

    private static int compare(byte[] bs1, byte[] bs2, int len) {
        assert(bs1.length >= len && bs2.length >= len);
        for (int i = 0; i < len; i++) {
            byte b1 = bs1[i];
            byte b2 = bs2[i];
            if (b1 == b2) {
                continue;
            }
            return (b1 & 0xff) - (b2 & 0xff);
        }
        return 0;
    }

    /**
     * Returns the max number of byte that represents a timestamp with given
     * precision.
     */
    static int getNumBytes(int precision) {
        return NO_FRACSECOND_BYTES + (getFracSecondBits(precision) + 7) / 8;
    }

    /**
     * Returns the max number of bits for fractional second based on the
     * specified precision.
     *    precision    max         hex           bit #    byte #
     *        1         9           0x9             4       1
     *        2         99          0x63            7       1
     *        3         999         0x3E7           10      2
     *        4         9999        0x270F          14      2
     *        5         99999       0x1869F         17      3
     *        6         999999      0xF423F         20      3
     *        7         9999999     0x98967F        24      3
     *        8         99999999    0x5F5E0FF       27      4
     *        9         999999999   0x3B9AC9FF      30      4
     */
    private static final int[] nFracSecbits = new int[] {
        0, 4, 7, 10, 14, 17, 20, 24, 27, 30
    };

    private static int getFracSecondBits(int precision) {
       return nFracSecbits[precision];
    }

    /**
     * Methods to parse Timestamp from a string and convert Timestamp to
     * a string.
     */

    /**
     * Parses the timestamp string with default pattern and UTC zone.
     */
    public static Timestamp parseString(String timestampString) {
        return parseString(timestampString, null, true);
    }

    /**
     * Parses the timestamp string with the specified pattern to a Date.
     *
     * @param timestampString the sting to parse
     * @param pattern the pattern of timestampString
     * @param withZoneUTC true if UTC time zone is used as default zone
     * when parse the timestamp string, otherwise local time zone is used as
     * default zone.  If the timestampString contains time zone, then the zone
     * in timestampString will take precedence over the default zone.
     *
     * @return a String representation of the value
     *
     */
    public static Timestamp parseString(String timestampString,
                                        String pattern,
                                        boolean withZoneUTC) {
        /*
         * If the specified pattern is the default pattern and with UTC zone,
         * then call parseWithDefaultPattern(String) to parse the timestamp
         * string in a more efficient way.
         */
        if ((pattern == null ||
             pattern.equals(TimestampDef.DEFAULT_PATTERN)) &&
            withZoneUTC) {

            String tsStr = trimUTCZoneOffset(timestampString);
            /*
             * If no zone offset or UTC zone in timestamp string, then parse it
             * using parseWithDefaultPattern(). Otherwise, parse it with
             * DateTimeFormatter.
             */
            if (tsStr != null) {
                return parseWithDefaultPattern(tsStr);
            }
        }

        pattern = getPattern(pattern);
        ZoneId zoneId = (withZoneUTC ? UTCZone : localZone);
        try {
            DateTimeFormatter dtf = getDateTimeFormatter(pattern, zoneId,
                                                         0 /* nFracSecond */);
            TemporalAccessor ta = dtf.parse(timestampString);
            if (!ta.isSupported(ChronoField.YEAR) ||
                !ta.isSupported(ChronoField.MONTH_OF_YEAR) ||
                !ta.isSupported(ChronoField.DAY_OF_MONTH)) {

                throw new IllegalArgumentException("The timestamp string " +
                    "must contain year, month and day");
            }

            Instant instant = ZonedDateTime.from(ta).toInstant();
            return toTimestamp(instant);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                pattern + ": " + iae.getMessage(), iae);
        } catch (DateTimeParseException dtpe) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                pattern + ": " + dtpe.getMessage(), dtpe);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                pattern + ": " + dte.getMessage(), dte);
        }
    }

    /**
     * Trims the designator 'Z' or "+00:00" that represents UTC zone from the
     * Timestamp string if exists, return null if Timestamp string contains
     * non-zero offset.
     */
    private static String trimUTCZoneOffset(String ts) {
        if (ts.endsWith("Z")) {
            return ts.substring(0, ts.length() - 1);
        }
        if (ts.endsWith("+00:00")) {
            return ts.substring(0, ts.length() - 6);
        }

        if (!hasSignOfZoneOffset(ts)) {
            return ts;
        }
        return null;
    }

    /**
     * Returns true if the Timestamp string in default pattern contain the
     * sign of ZoneOffset: plus(+) or hyphen(-).
     *
     * If timestamp string in default pattern contains negative zone offset, it
     * must contain 3 hyphen(-), e.g. 2017-12-05T10:20:01-03:00.
     *
     * If timestamp  string contains positive zone offset, it must contain
     * plus(+) sign.
     */
    private static boolean hasSignOfZoneOffset(String ts) {
        if (ts.indexOf('+') > 0) {
            return true;
        }
        int pos = 0;
        for (int i = 0; i < 3; i++) {
            pos = ts.indexOf('-', pos + 1);
            if (pos < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parses the timestamp string in format of default pattern
     * "uuuu-MM-dd[THH:mm:ss[.S..S]]" with UTC zone.
     */
    private static Timestamp parseWithDefaultPattern(String ts){

        if (ts.isEmpty()) {
            raiseParseError(ts, "");
        }

        final int[] comps = new int[7];

        /*
         * The component that is currently being parsed, starting with 0
         * for the year, and up to 6 for the fractional seconds
         */
        int comp = 0;

        int val = 0;
        int ndigits = 0;

        int len = ts.length();
        boolean isBC = (ts.charAt(0) == '-');

        for (int i = (isBC ? 1 : 0); i < len; ++i) {

            char ch = ts.charAt(i);

            if (comp < 6) {

                switch (ch) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    val = val * 10 + (ch - '0');
                    ++ndigits;
                    break;
                default:
                    if (ch == compSeparators[comp]) {
                        checkAndSetValue(comps, comp, val, ndigits, ts);
                        ++comp;
                        val = 0;
                        ndigits = 0;

                    } else {
                        raiseParseError(
                            ts, "invalid character '" + ch +
                            "' while parsing component " + compNames[comp]);
                    }
                }
            } else {
                assert(comp == 6);

                switch (ch) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    val = val * 10 + (ch - '0');
                    ndigits++;
                    break;
                default:
                    raiseParseError(
                        ts, "invalid character '" + ch +
                        "' while parsing component " + compNames[comp]);
                }
            }
        }

        /* Set the last component */
        checkAndSetValue(comps, comp, val, ndigits, ts);

        if (comp < 2) {
            raiseParseError(
                ts, "the timestamp string must have at least the 3 " +
                "date components");
        }

        if (comp == 6 && comps[6] > 0) {

            if (ndigits > MAX_PRECISION) {
                raiseParseError(
                    ts, "the fractional-seconds part contains more than " +
                    MAX_PRECISION + " digits");
            } else if (ndigits < MAX_PRECISION) {
                /* Nanosecond *= 10 ^ (MAX_PRECISION - s.length()) */
                comps[6] *= (int)Math.pow(10, MAX_PRECISION - ndigits);
            }
        }

        if (isBC) {
            comps[0] = -comps[0];
        }

        return createTimestamp(comps);
    }

    private static void checkAndSetValue(
        int[] comps,
        int comp,
        int value,
        int ndigits,
        String ts) {

        if (ndigits == 0) {
            raiseParseError(
                ts, "component " + compNames[comp] + "has 0 digits");
        }

        comps[comp] = value;
    }

    private static void raiseParseError(String ts, String err) {

        String errMsg =
            ("Failed to parse the timestamp string '" + ts +
             "' with the pattern: " + TimestampDef.DEFAULT_PATTERN + ": ");

        throw new IllegalArgumentException(errMsg + err);
    }

    /**
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param value the Timestamp value object
     * @param pattern the pattern string
     * @param zone the time zone id
     *
     * @return the formatted string.
     */
    public static String formatString(TimestampValueImpl value,
                                      String pattern,
                                      String zone) {
        return formatString(value, pattern, getZoneId(zone));
    }

    public static String formatString(TimestampValueImpl value,
                                      String pattern,
                                      ZoneId zoneId) {
        if ((pattern == null ||
             pattern.equals(TimestampDef.DEFAULT_PATTERN)) &&
            (zoneId == null || zoneId == UTCZone)) {
           return stringFromBytes(value.getBytes(),
                                  value.getDefinition().getPrecision());
        }
        return formatString(value.get(), pattern, zoneId,
                            value.getDefinition().getPrecision());
    }

    /**
     * Extracts timestamp information from specified byte array and build a
     * string with default pattern format: "uuuu-MM-ddTHH:mm:ss[.S..S]"
     */
    private static String stringFromBytes(byte[] bytes, int precision) {
        int[] comps = extractFromBytes(bytes, precision);
        StringBuilder sb = new StringBuilder(TimestampDefImpl.DEF_STRING_FORMAT);
        if (precision > 0) {
            sb.append(".");
            sb.append("%0");
            sb.append(precision);
            sb.append("d").append(UTC_SUFFIX);
            int fs = comps[6] / (int)Math.pow(10, (MAX_PRECISION - precision));
            return String.format(sb.toString(), comps[0], comps[1], comps[2],
                                 comps[3], comps[4], comps[5], fs);
        }
        sb.append(UTC_SUFFIX);
        return String.format(sb.toString(), comps[0], comps[1], comps[2],
                             comps[3], comps[4], comps[5]);
    }

    /**
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param timestamp the Timestamp object
     * @param pattern the pattern string
     * @param withZoneUTC true if use UTC zone to format the Timestamp value,
     * otherwise local time zone is used.
     *
     * @return the formatted string.
     */
    public static String formatString(Timestamp timestamp,
                                      String pattern,
                                      boolean withZoneUTC) {
        return formatString(timestamp, pattern,
                            (withZoneUTC ? UTCZone : localZone),
                            0);
    }

    /**
     * For unit test only.
     *
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param timestamp the Timestamp object
     * @param pattern the pattern string
     * @param withZoneUTC true if use UTC zone to format the Timestamp value,
     * otherwise local time zone is used.
     * @param optionalFracSecond true if the fractional second are optional
     * with varied length from 1 to 9.
     * @param nFracSecond the number of digits of fractional second, if
     * optionalFracSecond is true, this argument is ignored.
     *
     * @return the formatted string.
     */
     static String formatString(Timestamp timestamp,
                                String pattern,
                                ZoneId zoneId,
                                int nFracSecond) {

        pattern = getPattern(pattern);
        if (zoneId == null) {
            zoneId = UTCZone;
        }
        try {
            ZonedDateTime zdt = toUTCDateTime(timestamp);
            return zdt.format(getDateTimeFormatter(pattern, zoneId, nFracSecond));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + pattern + "': " +
                iae.getMessage(), iae);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + pattern + "': " +
                dte.getMessage(), dte);
        }
    }

    /**
     * Formats the Timestamp value to a string using the default pattern with
     * fractional second.
     */
    static String formatString(Timestamp value) {
        return formatString(value, null /* pattern */, null /* zoneId */, 0);
    }

    /**
     * Timestamp value related methods.
     */

    /**
     * Rounds the fractional second of Timestamp according to the specified
     * precision.
     */
    static Timestamp roundToPrecision(Timestamp timestamp, int precision) {
        if (precision == MAX_PRECISION || timestamp.getNanos() == 0) {
            return timestamp;
        }

        long seconds = getSeconds(timestamp);
        int nanos = getNanosOfSecond(timestamp);
        double base = Math.pow(10, (MAX_PRECISION - precision));
        nanos = (int)(Math.round(nanos / base) * base);
        if (nanos == (int)Math.pow(10, MAX_PRECISION)) {
            seconds++;
            nanos = 0;
        }
        Timestamp ts = createTimestamp(seconds, nanos);
        if (ts.compareTo(TimestampDefImpl.MAX_VALUE) > 0 ) {
            ts = (Timestamp)TimestampDefImpl.MAX_VALUE.clone();
            nanos = (int)((int)(ts.getNanos() / base) * base);
            ts.setNanos((int)((int)(ts.getNanos() / base) * base));
        }
        return ts;
    }

    /**
     * Returns the number of milliseconds from the Java epoch of
     * 1970-01-01T00:00:00Z.
     */
    static long toMilliseconds(Timestamp timestamp) {
        return timestamp.getTime();
    }

    /**
     * Gets the number of seconds from the Java epoch of 1970-01-01T00:00:00Z.
     */
    public static long getSeconds(Timestamp timestamp) {
        long ms = timestamp.getTime();
        return ms > 0 ? (ms / 1000) : (ms - 999)/1000;
    }

    /**
     * Gets the nanoseconds of the Timestamp value.
     */
    public static int getNanosOfSecond(Timestamp timestamp) {
        return timestamp.getNanos();
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * nanoseconds subtracted.
     */
    static Timestamp minusNanos(Timestamp base, long nanosToSubtract) {
        return toTimestamp(base.toInstant().minusNanos(nanosToSubtract));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * nanoseconds added.
     */
    static Timestamp plusNanos(Timestamp base, long nanosToAdd) {
        return toTimestamp(base.toInstant().plusNanos(nanosToAdd));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * milliseconds subtracted.
     */
    static Timestamp minusMillis(Timestamp base, long millisToSubtract) {
        return toTimestamp(base.toInstant().minusMillis(millisToSubtract));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * milliseconds added.
     */
    static Timestamp plusMillis(Timestamp base, long millisToAdd) {
        return toTimestamp(base.toInstant().plusMillis(millisToAdd));
    }

    /**
     * Creates a Timestamp with given seconds since Java epoch and nanosOfSecond.
     */
    public static Timestamp createTimestamp(long seconds, int nanosOfSecond) {
        Timestamp ts = new Timestamp(seconds * 1000);
        ts.setNanos(nanosOfSecond);
        return ts;
    }

    /**
     * Creates a Timestamp from components: year, month, day, hour, minute,
     * second and nanosecond.
     */
    public static Timestamp createTimestamp(int[] comps) {
        ZonedDateTime zdt = createZonedDateTime(comps);
        return createTimestamp(zdt.toEpochSecond(), zdt.getNano());
    }

    private static ZonedDateTime createZonedDateTime(int[] comps) {

        if (comps.length < 3) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components, it should contain at least 3 components: year, " +
                "month and day, but only " + comps.length);
        } else if (comps.length > 7) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components, it should contain at most 7 components: year, " +
                "month, day, hour, minute, second and nanosecond, but has " +
                comps.length + " components");
        }

        int num = comps.length;
        for (int i = 0; i < num; i++) {
            validateComponent(i, comps[i]);
        }
        try {
            return ZonedDateTime.of(comps[0],
                                    comps[1],
                                    comps[2],
                                    ((num > 3) ? comps[3] : 0),
                                    ((num > 4) ? comps[4] : 0),
                                    ((num > 5) ? comps[5] : 0),
                                    ((num > 6) ? comps[6] : 0),
                                    UTCZone);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components: " + dte.getMessage());
        }
    }

    static ZonedDateTime toZonedDateTime(TimestampValueImpl val) {
        byte[] bytes = val.getBytes();
        return ZonedDateTime.of(getYear(bytes),
                                getMonth(bytes),
                                getDay(bytes),
                                getHour(bytes),
                                getMinute(bytes),
                                getSecond(bytes),
                                getNano(bytes, val.getPrecision()),
                                UTCZone);
    }

    /**
     * Validates the component of Timestamp, the component is indexed from 0 to
     * 6 that maps to year, month, day, hour, minute, second and nanosecond.
     */
    public static void validateComponent(int index, int value) {
        switch(index) {
            case 0: /* Year */
                if (value < TimestampDefImpl.MIN_YEAR ||
                    value > TimestampDefImpl.MAX_YEAR) {
                    throw new IllegalArgumentException("Invalid year, it " +
                            "should be in range from " +
                            TimestampDefImpl.MIN_YEAR + " to "+
                            TimestampDefImpl.MAX_YEAR + ": " + value);
                }
                break;
            case 1: /* Month */
                if (value < 1 || value > 12) {
                    throw new IllegalArgumentException("Invalid month, it " +
                            "should be in range from 1 to 12: " + value);
                }
                break;
            case 2: /* Day */
                if (value < 1 || value > 31) {
                    throw new IllegalArgumentException("Invalid day, it " +
                            "should be in range from 1 to 31: " + value);
                }
                break;
            case 3: /* Hour */
                if (value < 0 || value > 23) {
                    throw new IllegalArgumentException("Invalid hour, it " +
                            "should be in range from 0 to 23: " + value);
                }
                break;
            case 4: /* Minute */
                if (value < 0 || value > 59) {
                    throw new IllegalArgumentException("Invalid minute, it " +
                            "should be in range from 0 to 59: " + value);
                }
                break;
            case 5: /* Second */
                if (value < 0 || value > 59) {
                    throw new IllegalArgumentException("Invalid second, it " +
                            "should be in range from 0 to 59: " + value);
                }
                break;
            case 6: /* Nanosecond */
                if (value < 0 || value > 999999999) {
                    throw new IllegalArgumentException("Invalid second, it " +
                            "should be in range from 0 to 999999999: " + value);
                }
                break;
        }
    }

    /**
     * Converts Timestamp to ZonedDataTime at UTC zone.
     */
    private static ZonedDateTime toUTCDateTime(Timestamp timestamp) {
        return timestamp.toInstant().atZone(UTCZone);
    }

    /**
     * Converts a Instant object to Timestamp
     */
    private static Timestamp toTimestamp(Instant instant) {
        return createTimestamp(instant.getEpochSecond(), instant.getNano());
    }

    /**
     * Stores a int value to the byte buffer from the given position, returns
     * the the last position written.
     */
    private static int writeBits(byte[] bytes, int value, int pos, int len) {
        assert(value > 0 && pos + len <= bytes.length * 8);
        int ind = pos / 8;
        int lastPos = 0;
        for (int i = 0; i < len; i++) {
            int bi = (pos + i) % 8;
            if ((value & (1 << (len - i - 1))) != 0) {
                bytes[ind] |= (byte) (1 << (7 - bi));
                lastPos = pos + i;
            }
            if (bi == 7) {
                ind++;
            }
        }
        return lastPos;
    }

    /**
     * Reads the number of bits from byte array starting from the given
     * position, store them to a int and return.
     */
    private static int readBits(byte[] bytes, int pos, int len) {
        int value = 0;
        int ind = pos / 8;
        for (int i = 0; i < len && ind < bytes.length; i++) {
            int bi = (i + pos) % 8;
            if ((bytes[ind] & (1 << (7 - bi))) != 0) {
               value |= 1 << (len - 1 - i);
            }
            if (bi == 7) {
                ind++;
            }
        }
        return value;
    }

    /**
     * Returns the DateTimeFormatter with the given pattern.
     */
    private static DateTimeFormatter getDateTimeFormatter(String pattern,
                                                          ZoneId zoneId,
                                                          int nFracSecond) {
        DateTimeFormatterBuilder dtfb =
            new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);

        validatePattern(pattern);

        /* Append faction seconds and zone offset if default pattern is used */
        if (pattern.equals(TimestampDefImpl.DEFAULT_PATTERN)) {
            /* fraction seconds */
            if (nFracSecond > 0) {
                dtfb.appendFraction(ChronoField.NANO_OF_SECOND,
                                    nFracSecond, nFracSecond, true);
            } else {
                dtfb.optionalStart();
                dtfb.appendFraction(ChronoField.NANO_OF_SECOND,
                                    0, MAX_PRECISION, true);
                dtfb.optionalEnd();
            }

            /* zone offset */
            dtfb.optionalStart();
            dtfb.appendOffset("+HH:MM","Z");
            dtfb.optionalEnd();
        }

        return dtfb.toFormatter(DEFAULT_LOCALE).withZone(zoneId);
    }

    /**
     * This is to restrict abbreviation zone name usage, the following symbols
     * are not allowed: 'v', 'z', 'zz', 'zzz'.
     *
     * Note that 'vvvv' and 'zzzz' are for full display name of zone, they are
     * valid.
     *
     * See more information in Jira KVSTORE-2335, also see the supported time
     * zone symbols in the pattern in java doc:
     * https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html
     */
    private static void validatePattern(String pattern) {
        for (int pos = 0; pos < pattern.length(); pos++) {
            char cur = pattern.charAt(pos);
            if (cur == 'v' || cur == 'z') {
                int start = pos;
                pos++;
                for (; pos < pattern.length(); pos++) {
                    if (pattern.charAt(pos) != cur) {
                        break;
                    }
                }
                if (pos - start < 4) {
                    throw new IllegalArgumentException(
                        "Unsupported time zone symbols '" +
                        pattern.substring(start, pos) + "'");
                }
            } else if (cur == '\'') {
                /* skip literals */
                for (; pos < pattern.length() - 1; pos++) {
                    if (pattern.charAt(pos + 1) == '\'') {
                        pos++;
                        break;
                    }
                }
            }
        }
    }

    public static TimestampValueImpl timestampAdd(TimestampValueImpl src,
                                                  Interval interval) {

        ZonedDateTime dt = toZonedDateTime(src);

        if (!interval.getPeriod().equals(Period.ZERO)) {
            dt = dt.plus(interval.getPeriod());
        }
        if (!interval.getTime().equals(Duration.ZERO)) {
            dt = dt.plus(interval.getTime());
        }

        if (dt.getYear() > TimestampDefImpl.MAX_YEAR ||
            dt.getYear() < TimestampDefImpl.MIN_YEAR) {
            throw new IllegalArgumentException("Timestamp should be " +
                "in range from " + formatString(TimestampDefImpl.MIN_VALUE) +
                " to " + formatString(TimestampDefImpl.MAX_VALUE) + ": " +
                 formatString(createTimestamp(dt.toEpochSecond(),
                                              dt.getNano())));
        }

        byte[] bytes = toBytes(dt.getYear(), dt.getMonthValue(),
                               dt.getDayOfMonth(), dt.getHour(),
                               dt.getMinute(), dt.getSecond(), dt.getNano(),
                               FieldDefImpl.Constants.timestampDef
                               .getPrecision());

        return new TimestampValueImpl(FieldDefImpl.Constants.timestampDef,
                                      bytes);
    }

    public static long timestampDiff(TimestampValueImpl tv1,
                                     TimestampValueImpl tv2) {

        ZonedDateTime dt1 = toZonedDateTime(tv1);
        ZonedDateTime dt2 = toZonedDateTime(tv2);
        return dt2.until(dt1, ChronoUnit.MILLIS);
    }

    private static String getPattern(String pattern) {
        return (pattern == null) ? TimestampDef.DEFAULT_PATTERN : pattern;
    }

    private static ZoneId getZoneId(String name) {
        if (name != null) {
            try {
                return ZoneId.of(name);
            } catch (DateTimeException ex) {
                throw new IllegalArgumentException(
                    "Unknown time-zone ID: " + name);
            }
        }
        return UTCZone;
    }

    public static class Interval {

        private final static int SECONDS_PER_HOUR = 3600;
        private final static int SECONDS_PER_MINUTE = 60;
        private final static int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
        private final static int NANOS_PER_MILLI = 1000000;

        private enum Unit {
            YEAR,
            MONTH,
            DAY,
            HOUR,
            MINUTE,
            SECOND,
            MILLISECOND,
            NANOSECOND
        }

        private Period period;
        private Duration time;
        private boolean isNegative;

        Interval() {
            period = Period.ZERO;
            time = Duration.ZERO;
            isNegative = false;
        }

        /*
         * Parses the string of (<n> UNIT)+, constructing an interval object
         *
         * - The <n> is positive int or zero.
         * - The UNIT can be YEARS, MONTHS, DAYS, HOURS, MINUTES, SECONDS,
         *   MILLISECONDS, NANOSECONDS, must be specified from largest unit
         *   to smallest unit.
         */
        public static Interval parseString(String intervalStr) {
            boolean negative = false;

            String str = intervalStr.trim();
            if (str.startsWith("-")) {
                negative = true;
                str = str.substring(1).trim();
            }

            String[] comps = str.split("\\s+");
            Interval interval = new Interval();

            Unit unit = null;
            Unit prev = null;
            String sval;
            String sunit;

            for (int i = 0; i < comps.length; i++) {
                sval = comps[i];
                if (++i == comps.length) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr +
                        "', miss unit after " + sval + ", unit can be " +
                        Arrays.toString(Unit.values()));
                }

                sunit = comps[i];
                if (sunit.endsWith("S") || sunit.endsWith("s")) {
                    sunit = sunit.substring(0, sunit.length() - 1);
                }

                try {
                    unit = Unit.valueOf(sunit.toUpperCase());
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr +
                        "', unit can be " + Arrays.toString(Unit.values()));
                }

                if (prev != null && unit.compareTo(prev) <= 0) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr + "', " +
                        (prev.equals(unit) ? "duplicated unit" :
                            "unit should be specified in the order of " +
                            Arrays.toString(Unit.values())) +
                        ": " + unit);
                }

                /* The value should be positive or zero */
                int val = 0;
                try {
                    val = Integer.parseInt(sval);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                       "Invalid interval string '" + intervalStr +
                       "', the value for " + unit +
                       " must be a positiv int or zero: " + sval);
                }

                if (val < 0) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr +
                        "', the value for " + unit.name() +
                        " must be a positive int or zero: " + sval);
                }

                if (val > 0) {
                    switch (unit) {
                    case YEAR:
                        interval.setYears(val);
                        break;
                    case MONTH:
                        interval.setMonths(val);
                        break;
                    case DAY:
                        interval.setDays(val);
                        break;
                    case HOUR:
                        interval.setHours(val);
                        break;
                    case MINUTE:
                        interval.setMinutes(val);
                        break;
                    case SECOND:
                        interval.setSeconds(val);
                        break;
                    case MILLISECOND:
                        interval.setMillis(val);
                        break;
                    case NANOSECOND:
                        interval.setNanos(val);
                        break;
                    }
                }

                prev = unit;
            }

            if (negative) {
                interval.setNegative();
            }
            return interval;
        }

        public static Interval ofMilliseconds(long millis) {
            Interval interval = new Interval();
            if (millis < 0) {
                interval.setNegative();
            }
            interval.setMillis(Math.abs(millis));
            return interval;
        }

        private Interval setYears(int years) {
            period = period.withYears(years);
            return this;
        }

        private Interval setMonths(int months) {
            period = period.withMonths(months);
            return this;
        }

        private Interval setDays(int days) {
            time = time.plusDays(days);
            return this;
        }

        private Interval setHours(int hours) {
            time = time.plusHours(hours);
            return this;
        }

        private Interval setMinutes(long minutes) {
            time = time.plusMinutes(minutes);
            return this;
        }

        private Interval setSeconds(long seconds) {
            time = time.plusSeconds(seconds);
            return this;
        }

        private Interval setMillis(long millis) {
            time = time.plusMillis(millis);
            return this;
        }

        private Interval setNanos(long nanos) {
            time = time.plusNanos(nanos);
            return this;
        }

        private void setNegative() {
            isNegative = true;
        }

        public Period getPeriod() {
            if (isNegative) {
                return period.negated();
            }
            return period;
        }

        public Duration getTime() {
            if (isNegative) {
                return time.negated();
            }
            return time;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            if (period.equals(Period.ZERO) && time.equals(Duration.ZERO)) {
                return "0 second";
            }

            if (isNegative) {
                sb.append("-");
            }

            long val;
            if (!period.equals(Period.ZERO)) {
                val = period.getYears();
                if (val > 0) {
                    appendUnit(sb, val, Unit.YEAR);
                }

                val = period.getMonths();
                if (val > 0) {
                    appendUnit(sb, val, Unit.MONTH);
                }
            }


            if (!time.equals(Duration.ZERO)) {

                val = time.getSeconds();
                long days = val / SECONDS_PER_DAY;
                int hours = (int) (val % SECONDS_PER_DAY) / SECONDS_PER_HOUR;
                int mins = (int) ((val % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
                int secs = (int) (val % SECONDS_PER_MINUTE);

                if (days > 0) {
                    appendUnit(sb, days, Unit.DAY);
                }

                if (hours > 0) {
                    appendUnit(sb, hours, Unit.HOUR);
                }

                if (mins > 0) {
                    appendUnit(sb, mins, Unit.MINUTE);
                }

                if (secs > 0) {
                    appendUnit(sb, secs, Unit.SECOND);
                }

                int nanos = time.getNano();

                if (nanos % NANOS_PER_MILLI == 0) {
                    int millis = nanos / NANOS_PER_MILLI;
                    if (millis > 0) {
                        appendUnit(sb, millis, Unit.MILLISECOND);
                    }
                } else {
                    if (nanos > 0) {
                        appendUnit(sb, nanos, Unit.NANOSECOND);
                    }
                }
            }

            return sb.toString();
        }

        private static void appendUnit(StringBuilder sb, long val, Unit unit) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(val).append(" ").append(unit.name().toLowerCase());
            if (val > 1) {
                sb.append("s");
            }
        }
    }

    public enum RoundUnit {
        YEAR,
        IYEAR,
        QUARTER,
        MONTH,
        IWEEK,
        WEEK,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    /* The interval for rounding timestamp: <n> unit */
    public static class BucketInterval {

        private int num;
        private RoundUnit unit;

        BucketInterval(int num, RoundUnit unit) {
            this.num = num;
            this.unit = unit;
        }

        /*
         * Parses a string to RoundInterval, the interval can be in 2 formats:
         *   - single unit: YEAR, IYEAR, QUARTER, MONTH, WEEK, IWEEK, DAY,
         *                  HOUR, MINUTE, SECOND.
         *   - arbitrary interval: <n> unit
         *                        unit are: WEEK, DAY, HOUR, MINUTE and SECOND.
         */
        public static BucketInterval parseString(String intervalStr) {
            String[] comps = intervalStr.split(" ");
            if (comps.length > 2) {
                throw new IllegalArgumentException(
                    "Invalid interval string '" + intervalStr +
                    "', it should be in format of '<unit>' or '<n> <unit>'");
            }

            int num;
            String unitStr;
            RoundUnit unit;
            if (comps.length == 1) {
                num = 1;
                unitStr = comps[0];
            } else {
                /* <n> unit */
                try {
                    num = Integer.parseInt(comps[0]);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr +
                        "': " + comps[0] + " is not a valid integer");
                }
                if (num <= 0) {
                    throw new IllegalArgumentException(
                        "Invalid interval string '" + intervalStr +
                        "': " + comps[0] + " is not a positive integer");
                }
                unitStr = comps[1];
            }

            unitStr = unitStr.trim().toUpperCase();
            try {
                if (unitStr.endsWith("S")) {
                    unitStr = unitStr.substring(0, unitStr.length() - 1);
                }
                unit = RoundUnit.valueOf(unitStr);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                    "Invalid interval string '" + intervalStr +
                    "': unknown unit " + unitStr + ", the valid units are " +
                    Arrays.toString(RoundUnit.values()));
            }

            if (num > 1 && unit.ordinal() < RoundUnit.WEEK.ordinal()) {
                throw new IllegalArgumentException(
                    "Invalid arbitrary interval '" + intervalStr +
                    "', valid units are WEEK, DAY, HOUR, MINUTE and SECOND");
            }

            return new BucketInterval(num, unit);
        }

        public int getNum() {
            return num;
        }

        public RoundUnit getUnit() {
            return unit;
        }

        @Override
        public String toString() {
            if (num > 1) {
                return num + " " + unit.name() + "S";
            }
            return unit.name();
        }
    }
}
