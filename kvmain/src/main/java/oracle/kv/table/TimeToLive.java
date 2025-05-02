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

package oracle.kv.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.util.FastExternalizable;

/**
 * TimeToLive is a utility class that represents a period of time, similar to
 * Java 8's java.time.Duration, but specialized to the needs of Oracle NoSQL
 * Database.
 * <p>
 * This class is restricted to durations of days and hours. It is only
 * used as input related to time to live (TTL) for {@link Row} instances,
 * set by using {@link Row#setTTL}. Construction allows only day and hour
 * durations for efficiency reasons. Durations of days are recommended as they
 * result in the least amount of storage consumed in a store.
 * <p>
 * Only positive durations are allowed on input, although negative durations
 * can be returned from {@link #fromExpirationTime} if the expirationTime is
 * in the past relative to the referenceTime.
 *
 * @since 4.0
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public final class TimeToLive implements Serializable, FastExternalizable {

    /**
     * The ordinal value for TimeUnit.DAYS.
     *
     * @hidden
     */
    public static final byte DAYS_ORDINAL = 6;

    /**
     * The ordinal value for TimeUnit.HOURS.
     *
     * @hidden
     */
    public static final byte HOURS_ORDINAL = 5;

    private static final long serialVersionUID = 1L;

    private final long value;
    private final TimeUnit unit;

    /**
     * A convenience constant that can be used as an argument to
     * {@link Row#setTTL} to indicate that the row should not expire.
     */
    public static final TimeToLive DO_NOT_EXPIRE =
        new TimeToLive(0, TimeUnit.DAYS);

    /**
     * Creates a duration using a period of hours.
     *
     * @param hours the number of hours in the duration, must be
     * a non-negative number
     *
     * @return the duration
     *
     * @throws IllegalArgumentException if a negative value is provided
     */
    public static TimeToLive ofHours(long hours) {
        return createTimeToLive(hours, TimeUnit.HOURS);
    }

    /**
     * Creates a duration using a period of 24 hour days.
     *
     * @param days the number of days in the duration, must be
     * a non-negative number
     *
     * @return the duration
     *
     * @throws IllegalArgumentException if a negative value is provided
     */
    public static TimeToLive ofDays(long days) {
        return createTimeToLive(days, TimeUnit.DAYS);
    }

    /**
     * Returns the number of days in this duration, which may be negative.
     *
     * @return the number of days
     */
    public long toDays() {
        return TimeUnit.DAYS.convert(value, unit);
    }

    /**
     * Returns the number of hours in this duration, which may be negative.
     *
     * @return the number of hours
     */
    public long toHours() {
        return TimeUnit.HOURS.convert(value, unit);
    }

    /**
     * Returns an absolute time representing the duration plus the absolute
     * time reference parameter. If an expiration time from the current time is
     * desired the parameter should be {@link System#currentTimeMillis}. If the
     * duration of this object is 0 ({@link #DO_NOT_EXPIRE}), indicating no
     * expiration time, this method will return 0, regardless of the reference
     * time.
     *
     * @param referenceTime an absolute time in milliseconds since January
     * 1, 1970.
     *
     * @return time in milliseconds, 0 if this object's duration is 0
     */
    public long toExpirationTime(long referenceTime) {
        if (value == 0) {
            return 0;
        }
        return referenceTime + toMillis();
    }

    /**
     * Returns an instance of TimeToLive based on an absolute expiration
     * time and a reference time. If a duration relative to the current time
     * is desired the referenceTime should be {@link System#currentTimeMillis}.
     * If the expirationTime is 0, the referenceTime is ignored and a
     * TimeToLive of duration 0 is created, indicating no expiration.
     * <p>
     * Days will be use as the primary unit of duration if the expiration time
     * is evenly divisible into days, otherwise hours are used.
     *
     * @param expirationTime an absolute time in milliseconds since January
     * 1, 1970
     *
     * @param referenceTime an absolute time in milliseconds since January
     * 1, 1970.
     *
     * @return a TimeToLive instance
     */
    public static TimeToLive fromExpirationTime(long expirationTime,
                                                long referenceTime) {
        final long MILLIS_PER_HOUR = 1000L * 60 * 60;

        if (expirationTime == 0) {
            return DO_NOT_EXPIRE;
        }

        /*
         * Calculate whether the given time in millis, when converted to hours,
         * rounding up, is not an even multiple of 24.
         */
        final long hours =
            (expirationTime + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR;
        boolean timeInHours = hours % 24 != 0;

        /*
         * This may result in a negative duration. This is ok and is documented.
         * If somehow the duration is 0, set it to -1 hours because 0 means
         * no expiration.
         */
        long duration = expirationTime - referenceTime;

        if (duration == 0) {
            /* very unlikely, but possible; set to -1 hours to avoid 0 */
            duration = -MILLIS_PER_HOUR;
            timeInHours = true;
        }
        if (timeInHours) {
            if (duration > 0 && duration < MILLIS_PER_HOUR) {
                return new TimeToLive(1, TimeUnit.HOURS);
            }
            return new TimeToLive(
                TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS),
                TimeUnit.HOURS);
        }
        if (duration > 0 && duration < MILLIS_PER_HOUR * 24) {
            return new TimeToLive(1, TimeUnit.DAYS);
        }
        return new TimeToLive(
                TimeUnit.DAYS.convert(duration, TimeUnit.MILLISECONDS),
                TimeUnit.DAYS);
    }

    /**
     * Equality compares the duration only if the units used for construction
     * are the same. If the units (ofHours vs ofDays) are different two
     * instances will not be equal even if their absolute durations are the
     * same unless both values are 0, which means no expiration.
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TimeToLive)) {
            return false;
        }
        TimeToLive otherTTL = (TimeToLive) other;
        if (value == 0 && otherTTL.value == 0) {
            return true;
        }
        return (unit == otherTTL.unit && value == otherTTL.value);
    }

    @Override
    public int hashCode() {
        /* include the high order bits */
        return (int) ((value >>> 32) ^ value) + unit.hashCode();
    }

    @Override
    public String toString() {
        return value + " " + unit.toString();
    }

    /**
     * Creates a TimeToLive instance. The value must be a non-negative number
     * Internal use only
     * @hidden
     */
    public static TimeToLive createTimeToLive(long value, TimeUnit unit) {
        if (value < 0) {
            throw new IllegalArgumentException(
                "TimeToLive does not support negative time periods");
        }
        return (value == 0) ? DO_NOT_EXPIRE : new TimeToLive(value, unit);
    }

    /**
     * Internal use only
     * @hidden
     *
     * Returns the numeric duration value
     */
    public long getValue() {
        return value;
    }

    /**
     * Internal use only
     * @hidden
     *
     * Returns the TimeUnit used for the duration
     */
    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * Private constructor. All construction is done via this constructor, which
     * validates the arguments.
     *
     * @param value value of time
     * @param unit unit of time, cannot be null
     */
    private TimeToLive(long value, TimeUnit unit) {

        if (unit != TimeUnit.DAYS && unit != TimeUnit.HOURS) {
            throw new IllegalArgumentException(
                "Invalid TimeUnit (" + unit + ") in TimeToLive construction." +
                "Must be DAYS or HOURS.");
        }

        this.value = value;
        this.unit = unit;
    }

    private long toMillis() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }

    /**
     * Reads an instance from the input stream, returning null if the serial
     * version shows that TTL is not supported.
     *
     * @hidden For internal use only
     */
    public static TimeToLive readFastExternal(DataInput in,
                                              @SuppressWarnings("unused")
                                                    short serialVersion)
        throws IOException {

        final int ttlVal = readTTLValue(in);
        final TimeUnit ttlUnit = readTTLUnit(in, ttlVal);
        return createTimeToLive(ttlVal, ttlUnit);
    }

    /**
     * Reads the TTL value from the input stream.
     *
     * @hidden For internal use only
     */
    public static int readTTLValue(DataInput in)
        throws IOException {

        return in.readInt();
    }

    /**
     * Reads the TimeUnit associated with the specified TTL value from the
     * input stream.
     *
     * @hidden For internal use only
     */
    public static TimeUnit readTTLUnit(DataInput in, int ttlVal)
        throws IOException {

        try {
            return convertTimeToLiveUnit(ttlVal,
                                         (ttlVal == 0) ? 0 : in.readByte());
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid value: " + e.getMessage(), e);
        }
    }

    /**
     * Converts an ordinal value for a TTL TimeUnit into the TimeUnit.
     */
    public static TimeUnit convertTimeToLiveUnit(int ttlVal,
                                                 byte unitOrdinal) {
        if (ttlVal == 0) {
            return TimeUnit.DAYS;
        }
        switch (unitOrdinal) {
        case DAYS_ORDINAL:
            return TimeUnit.DAYS;
        case HOURS_ORDINAL:
            return TimeUnit.HOURS;
        default:
            throw new IllegalArgumentException(
                "Unknown TimeUnit ordinal: " + unitOrdinal);
        }
    }

    /**
     * Returns the numeric duration value associated with a {@link TimeToLive},
     * or 0 if the ttl is null.
     *
     * @param ttl the ttl or null
     * @return the numeric duration value
     */
    public static int getTTLValue(TimeToLive ttl) {
        return (ttl != null) ? (int) ttl.getValue() : 0;
    }

    /**
     * Returns the TimeUnit associated with a {@link TimeToLive}, or
     * {TimeUnit#DAYS} if the ttl is null.
     *
     * @param ttl the ttl or null
     * @return the time unit
     */
    public static TimeUnit getTTLUnit(TimeToLive ttl) {
        return (ttl != null) ? ttl.getUnit() : TimeUnit.DAYS;
    }

    /**
     * Writes this object to the output stream. See static writeFastExternal
     * method for format.
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        writeFastExternal(out, serialVersion, (int)value, unit);
    }

    /**
     * Writes a TTL value to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeInt int}) {@code ttlVal}
     * <li> <i>[Optional]</i> ({@code byte}) {@code ttlUnit} // {@link
     *      TimeUnit#DAYS}=6, {@link TimeUnit#HOURS}=5, only present if ttlVal
     *      is not 0
     * </ol>
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param ttlVal the TTL value
     * @param ttlUnit the TTL time unit, may be null if ttVal is 0
     * @hidden For internal use only
     */
    public static void writeFastExternal(DataOutput out,
                                         short serialVersion,
                                         int ttlVal,
                                         TimeUnit ttlUnit)
        throws IOException {

        out.writeInt(ttlVal);
        if (ttlVal == 0) {
            return;
        }
        if ((ttlUnit != TimeUnit.DAYS) && (ttlUnit != TimeUnit.HOURS)) {
            throw new IllegalArgumentException(
                "Invalid TTL time unit: " + ttlUnit);
        }
        out.writeByte((byte) ttlUnit.ordinal());
    }
}
