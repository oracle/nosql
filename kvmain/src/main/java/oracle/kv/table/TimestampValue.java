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

import java.sql.Timestamp;

/**
 * TimestampValue extends {@link FieldValue} to represent a Timestamp value.
 *
 * @since 4.3
 */
public interface TimestampValue extends FieldValue {

    /**
     * Get the Timestamp value of this object.
     *
     * @return the Timestamp value
     */
    public Timestamp get();

    /**
     * Create a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public TimestampValue clone();

    /**
     * Formats the Timestamp value to a string with the default pattern
     * {@link TimestampDef#DEFAULT_PATTERN} in UTC zone.
     *
     * @return the formatted string
     *
     * @throws IllegalArgumentException if failed to format the Timestamp value
     * to a string with default pattern.
     */
    @Override
    public String toString();

    /**
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param pattern the pattern for the timestampString. If null, then default
     * pattern {@link TimestampDef#DEFAULT_PATTERN} is used to format to a
     * string.  The symbols that can be used to specify a pattern are described
     * in {@link java.time.format.DateTimeFormatter}.
     * @param withZoneUTC true if use UTC zone, otherwise the local time zone
     * is used.
     *
     * @return the formatted string
     *
     * @throws IllegalArgumentException if failed to format the Timestamp value
     * to a string in format of given pattern.
     */
    public String toString(String pattern, boolean withZoneUTC);

    /**
     * Returns the year of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the year
     */
    public int getYear();

    /**
     * Returns the month of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the month
     */
    public int getMonth();

    /**
     * Returns the day of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the day
     */
    public int getDay();

    /**
     * Returns the hour of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the hour
     */
    public int getHour();

    /**
     * Returns the minute of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the minute
     */
    public int getMinute();

    /**
     * Returns the second of the Timestamp represented by this TimestampValue
     * object.
     *
     * @return the second
     */
    public int getSecond();

    /**
     * Returns the nanoseconds of the Timestamp represented by this
     * TimestampValue object.
     *
     * @return the nanoseconds
     */
    public int getNano();

    /**
     * Returns the fractional seconds of the Timestamp represented by this
     * TimestampValue object. The range of the number returned depends on
     * the precision of the timestamp value. For example, if the precision
     * is 3 (milli seconds), the number will be between 0 and 999. If the
     * precision is 0, the returned number will always be 0.
     *
     * @return the fractional seconds.
     */
    public int getFracSecond();
}
