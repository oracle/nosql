/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.TimestampUtil.formatString;
import static oracle.nosql.nson.util.TimestampUtil.parseString;

import java.math.BigDecimal;
import java.sql.Timestamp;

import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing a timestamp value. TimestampValue
 * represents an instant in time in UTC. TimestampValue always stores its state
 * in UTC. Instances can be created from long values which represent the number
 * of milliseconds since the Epoch, 1970-01-01T00:00:00. They can also be
 * created from string values in a valid ISO 8601 format. String formats without
 * an explicit time zone are interpreted as UTC time.
 * <p>
 * TimestampValue is directly correlated to the database type of the same name
 * and should be used during value construction for fields of that datatype.
 */
public class TimestampValue extends FieldValue {

    /*
     * String representation is the simplest and it's what is
     * used in the protocol.
     */
    private final String value;

    /**
     * @hidden
     * Used by Nson.readFieldValue(), which deserializes a FieldValue
     * received from the proxy.
     * @param value the value
     * @param dummy not used
     */
    public TimestampValue(String value, int dummy) {
        this.value = value;
    }

    /**
     * Creates a new instance from milliseconds since the Epoch.
     *
     * @param millisecond the number of milliseconds since the Epoch,
     *  "1970-01-01T00:00:00".
     */
    public TimestampValue(long millisecond) {
        this(new Timestamp(millisecond));
    }

    /**
     * Creates a new instance using an ISO 8601 formatted string. If timezone
     * is not specified it is interpreted as UTC.
     *
     * @param value the string of a Timestamp in ISO 8601 format
     * "uuuu-MM-dd['T'HH:mm:ss[.f..f]]".
     */
    public TimestampValue(String value) {
        this(parseString(value));
    }

     /**
     * Creates a TimestampValue instance from a String with specified pattern.
     *
     * @param value a timestamp string in the format of the specified
     * {@code pattern} or the default pattern, "uuuu-MM-dd['T'HH:mm:ss[.f..f]]",
     * which is used if {@code pattern} is null.
     *
     * @param pattern the pattern for the timestampString. If null, then default
     * pattern "uuuu-MM-dd['T'HH:mm:ss[.f..f]]" is used to parse the string.
     * The symbols that can be used to specify a pattern are described in
     * {@link java.time.format.DateTimeFormatter}.
     *
     * @param useUTC if true the UTC time zone is used as default zone when
     * parsing the string, otherwise the local time zone is used.
     * If the timestampString has zone information, then that zone will be used.
     *
     * @throws IllegalArgumentException if the string cannot be parsed with the
     * the given pattern correctly.
     */
    public TimestampValue (String value,
                           String pattern,
                           boolean useUTC) {
        this(parseString(value,
                         pattern,
                         useUTC,
                         (pattern == null)));
    }

    /**
     * Creates a new instance from a Java {@link Timestamp}
     *
     * @param value the Timestamp value
     */
    public TimestampValue(Timestamp value) {
        super();
        this.value = formatString(value);
    }

    @Override
    public Type getType() {
        return Type.TIMESTAMP;
    }

    /**
     * Returns the Java {@link Timestamp} value of this object
     *
     * @return the Timestamp value
     */
    public Timestamp getValue() {
        return parseString(value);
    }

    /**
     * Returns the long value of this object as the number of millisecond
     * since the Epoch, 1970-01-01T00:00:00.
     *
     * @return the long value
     */
    @Override
    public long getLong() {
        return getValue().getTime();
    }

    /**
     * Returns a BigDecimal value for this object based on its long value.
     *
     * @return the BigDecimal value
     */
    @Override
    public BigDecimal getNumber() {
        return new BigDecimal(getLong());
    }

    /**
     * Returns the string value of this object, it is in the ISO 8601 format
     * "uuuu-MM-dd'T'HH:mm:ss[.f..f]" and the time zone is UTC.
     *
     * @return the string value
     */
    @Override
    public String getString() {
        return value;
    }

    @Override
    public int compareTo(FieldValue other) {

        // ???? Why not compare them as strings?
        if (other instanceof TimestampValue) {
            return getValue().compareTo(((TimestampValue)other).getValue());
        }
        throw new ClassCastException("Object is not an TimestampValue");
    }

    /**
     * Returns the JSON value of the instance. If
     * {@link JsonOptions#getTimestampAsLong} is true it is returned as a simple
     * long value; otherwise it is returned as a quoted string using the ISO
     * 8601 format.
     *
     * @param options a JsonOptions instance to indicate non-default values,
     * may be null.
     *
     * @return the string value.
     */
    @Override
    public String toJson(JsonOptions options) {
        if (options != null && options.getTimestampAsLong()) {
            return String.valueOf(getLong());
        }
        return "\"" + getString() + "\"";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof TimestampValue) {
            return compareTo((TimestampValue)other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.stringSize(value) + 4);
    }
}
