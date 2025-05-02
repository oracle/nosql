/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Utility methods to parse/format a Timestamp from/to a string etc.
 */
public class TimestampUtil {

    /* The default pattern used to parse/format a Timestamp from/to a string */
    private final static String DEFAULT_PATTERN = "uuuu-MM-dd['T'HH:mm:ss]";

    /* The UTC zone */
    private final static ZoneId UTCZone = ZoneId.of(ZoneOffset.UTC.getId());

    /* The maxinum number of digits in fractional second */
    private final static int MAX_NUMBER_FRACSEC = 9;

    /*
     * The separator to conjunct components to a string, these are used parse a
     * string in default pattern. Allow ' ' in place of 'T' by default, which is
     * allowed by the spec
     */
    private final static char compSep[] = {'-', '-', 'T', ':', ':', '.'};
    private final static char compSep1[] = {'-', '-', ' ', ':', ':', '.'};

    /*
     * The name of components.
     */
    private final static String compNames[] = {
        "year", "month", "day", "hour", "minute", "second", "fractional second"
    };

    /**
     * Parses a string to a Timestamp Value, the string is in default pattern.
     * @param text the string value
     * @return the Timestamp value represented by the string
     */
    public static Timestamp parseString(String text) {
        return parseString(text, null, true, true);
    }

    /**
     * Parses a string to a Timestamp Value, the string is in specified pattern.
     * @param text the string value
     * @param pattern the regex pattern to use for parsing the string
     * @param withZoneUTC true if using UTC
     * @param optionalFracSecond optional fractional second to use
     * @return the resulting Timestamp
     */
    public static Timestamp parseString(String text,
                                        String pattern,
                                        boolean withZoneUTC,
                                        boolean optionalFracSecond) {
        requireNonNull(text, "Timestamp string must be non-null");

        /*
         * If the specified pattern is the default pattern and with UTC zone,
         * then call parseWithDefaultPattern(String) to parse the timestamp
         * string in a more efficient way.
         */
        boolean optionalZoneOffset = false;
        if (pattern == null || pattern.equals(DEFAULT_PATTERN)) {
            String tsStr = trimUTCZoneOffset(text);
            /*
             * If no zone offset or UTC zone offset in timestamp string, then
             * parse it using parseWithDefaultPattern(). Otherwise, parse it
             * with DateTimeFormatter.
             */
            if (tsStr != null) {
                return parseWithDefaultPattern(tsStr);
            }
            optionalZoneOffset = true;
        }

        String fmt = (pattern != null) ? pattern : DEFAULT_PATTERN;
        try {
            DateTimeFormatter dtf = getDateTimeFormatter(fmt,
                withZoneUTC, optionalFracSecond, optionalZoneOffset);
            TemporalAccessor ta = dtf.parse(text);
            if (!ta.isSupported(ChronoField.YEAR) ||
                !ta.isSupported(ChronoField.MONTH_OF_YEAR) ||
                !ta.isSupported(ChronoField.DAY_OF_MONTH)) {

                throw new IllegalArgumentException("The timestamp string " +
                    "must contain year, month and day");
            }
            Instant instant;
            boolean hasOffset = (ta.isSupported(ChronoField.OFFSET_SECONDS) &&
                ta.get(ChronoField.OFFSET_SECONDS) != 0);
            if (ta.isSupported(ChronoField.HOUR_OF_DAY)) {
                instant = hasOffset ? OffsetDateTime.from(ta).toInstant() :
                    Instant.from(ta);
            } else {
                instant = LocalDate.from(ta).atStartOfDay
                    ((hasOffset ? ZoneOffset.from(ta) : UTCZone)).toInstant();
            }
            return toTimestamp(instant);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + text + "' with the pattern: " + fmt + ": " +
                iae.getMessage(), iae);
        } catch (DateTimeParseException dtpe) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + text + "' with the pattern: " + fmt + ": " +
                dtpe.getMessage(), dtpe);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + text + "' with the pattern: " + fmt + ": " +
                dte.getMessage(), dte);
        }
    }

    /**
     * Trims the designator 'Z' or "+00:00" that represents UTC zone from the
     * Timestamp string if found, return null if Timestamp string contains
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
     * Formats a Timestamp to a string in default pattern.
     * @param ts the timestamp value
     * @return the string representation of the timestamp
     */
    public static String formatString(Timestamp ts) {
        requireNonNull(ts, "Timestamp must be non-null");
        return formatString(ts, null, true, true);
    }

    /**
     * Formats a Timestamp to a string in specified pattern.
     */
    private static String formatString(Timestamp timestamp,
                                       String pattern,
                                       boolean withZoneUTC,
                                       boolean optionalFracSecond) {
        requireNonNull(timestamp, "Timestamp must be non-null");
        String fmt = (pattern == null) ? DEFAULT_PATTERN : pattern;
        try {
            ZonedDateTime zdt = toUTCDateTime(timestamp);
            return zdt.format(getDateTimeFormatter(fmt,
                                                   withZoneUTC,
                                                   optionalFracSecond,
                                                   true));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + fmt + "': " + iae.getMessage(),
                iae);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + fmt + "': " + dte.getMessage(),
                dte);
        }
    }

    /**
     * Parses the timestamp string in format of default pattern
     * "uuuu-MM-dd[THH:mm:ss[.S..S]]" with UTC zone.
     */
    private static Timestamp parseWithDefaultPattern(String ts) {

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
                    if (ch == compSep[comp] || ch == compSep1[comp])  {
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
                if (comp != 6) {
                    throw new IllegalArgumentException(
                        "Invalid component index: " + comp +
                        ", it is expected to be 6");
                }

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

            if (ndigits > MAX_NUMBER_FRACSEC) {
                raiseParseError(
                    ts, "the fractional-seconds part contains more than " +
                    MAX_NUMBER_FRACSEC + " digits");
            } else if (ndigits < MAX_NUMBER_FRACSEC) {
                /* Nanosecond *= 10 ^ (MAX_PRECISION - s.length()) */
                comps[6] *= (int)Math.pow(10, MAX_NUMBER_FRACSEC - ndigits);
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
             "' with the pattern: " + DEFAULT_PATTERN + ": ");

        throw new IllegalArgumentException(errMsg + err);
    }

    /**
     * Validates the component of Timestamp, the component is indexed from 0 to
     * 6 that maps to year, month, day, hour, minute, second and nanosecond.
     */
    private static void validateComponent(int index, int value) {
        switch(index) {
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
     * Converts a Instant object to Timestamp
     */
    private static Timestamp toTimestamp(Instant instant) {
        return createTimestamp(instant.getEpochSecond(), instant.getNano());
    }

    /**
     * Creates a Timestamp with given seconds since Java epoch and nanosOfSecond
     * @param seconds the seconds value
     * @param nanoSeconds the nanoseconds value
     * @return the resulting Timestamp
     */
    public static Timestamp createTimestamp(long seconds, int nanoSeconds) {
        Timestamp ts = new Timestamp(seconds * 1000);
        ts.setNanos(nanoSeconds);
        return ts;
    }

    /**
     * Creates a Timestamp from components: year, month, day, hour, minute,
     * second and nanosecond.
     */
    private static Timestamp createTimestamp(int[] comps) {

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
            ZonedDateTime zdt = ZonedDateTime.of(comps[0],
                                                 comps[1],
                                                 comps[2],
                                                 ((num > 3) ? comps[3] : 0),
                                                 ((num > 4) ? comps[4] : 0),
                                                 ((num > 5) ? comps[5] : 0),
                                                 ((num > 6) ? comps[6] : 0),
                                                 UTCZone);
             return toTimestamp(zdt.toInstant());
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components: " + dte.getMessage());
        }
    }


    /**
     * Converts Timestamp to ZonedDataTime at UTC zone.
     */
    private static ZonedDateTime toUTCDateTime(Timestamp timestamp) {
        return toInstant(timestamp).atZone(UTCZone);
    }

    /**
     * Converts Timestamp to Instant
     */
    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(getSeconds(timestamp),
                                     getNanoSeconds(timestamp));
    }

    /**
     * Gets the number of seconds from the Epoch (1970-01-01T00:00:00Z).
     * @param timestamp the Timestamp
     * @return the long value
     */
    public static long getSeconds(Timestamp timestamp) {
        long ms = timestamp.getTime();
        return ms > 0 ? (ms / 1000) : (ms - 999)/1000;
    }

    /**
     * Gets the nanoseconds of the Timestamp value.
     * @param timestamp the Timestamp
     * @return the value in nanoseconds
     */
    public static int getNanoSeconds(Timestamp timestamp) {
        return timestamp.getNanos();
    }

    /**
     * Returns the DateTimeFormatter with the given pattern.
     */
    private static DateTimeFormatter getDateTimeFormatter
        (String pattern,
         boolean withZoneUTC,
         boolean optionalFracSecond,
         boolean optionalOffset) {

        DateTimeFormatterBuilder dtfb = new DateTimeFormatterBuilder();
        dtfb.appendPattern(pattern);
        if (optionalFracSecond) {
            dtfb.optionalStart();
            dtfb.appendFraction(ChronoField.NANO_OF_SECOND, 0,
                MAX_NUMBER_FRACSEC, true);
            dtfb.optionalEnd();
        }
        if (optionalOffset) {
            dtfb.optionalStart();
            dtfb.appendOffset("+HH:MM", "Z");
            dtfb.optionalEnd();
        }
        return dtfb.toFormatter().withZone
            (withZoneUTC ? UTCZone : ZoneId.systemDefault());
    }
}
