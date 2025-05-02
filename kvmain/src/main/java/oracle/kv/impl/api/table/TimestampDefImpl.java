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

import static oracle.kv.impl.api.table.TimestampUtils.parseString;
import static oracle.kv.impl.api.table.TimestampUtils.roundToPrecision;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.sql.Timestamp;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.table.TimestampDef;

/**
 * TimestampDefImpl implements the TimestampDef interface.
 */
public class TimestampDefImpl extends FieldDefImpl implements TimestampDef {

    private static final long serialVersionUID = 1L;

    public static final int MILLIS_PER_HOUR = 1000 * 3600;

    public static final int MILLIS_PER_DAY = 1000 * 3600 * 24;

    /* The max number of digits in the fractional part of second */
    public static final int MAX_PRECISION = 9;

    /* The default number of digits in the fractional part of second */
    public static final int DEF_PRECISION = 9;

    /* The format string for TimestampDef.DEFAULT_PATTERN */
    static final String DEF_STRING_FORMAT = "%04d-%02d-%02dT%02d:%02d:%02d";

    /* The maximum value supported is '9999-12-31T23:59:59.999999999 */
    static final Timestamp MAX_VALUE =
        TimestampUtils.createTimestamp(253402300799L, 999999999);

    /* The minimal value supported is '-6383-01-01T00:00:00.000000000 */
    static final Timestamp MIN_VALUE =
        TimestampUtils.createTimestamp(-263595168000L, 0);

    /* The minimal year */
    final static int MIN_YEAR =
        TimestampUtils.getYear(TimestampUtils.toBytes(MIN_VALUE, 9));

    /* The maximal year */
    final static int MAX_YEAR =
        TimestampUtils.getYear(TimestampUtils.toBytes(MAX_VALUE, 9));

    /* The maximum values for different precisions. */
    static final Timestamp[] maxValues = new Timestamp[MAX_PRECISION + 1];

    /* The minimal values for different precisions. */
    static final Timestamp[] minValues = new Timestamp[MAX_PRECISION + 1];

    static {
        for (int p = 0; p <= MAX_PRECISION; p++) {
            maxValues[p] = roundToPrecision(MAX_VALUE, p);
            minValues[p] = roundToPrecision(MIN_VALUE, p);
        }
    }

    /* The number of digits of fractional second */
    private final int precision;

    TimestampDefImpl(int precision) {
        this(precision, null);
    }

    TimestampDefImpl(int precision, String description) {
        super(Type.TIMESTAMP, description);
        this.precision = precision;
        validate();
    }

    private TimestampDefImpl(TimestampDefImpl impl) {
        super(impl);
        precision = impl.precision;
    }

    /**
     * Constructor for FastExternalizable
     */
    TimestampDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.TIMESTAMP);
        precision = readPackedInt(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code precision}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, precision);
    }

    /*
     * Public api methods from Object and FieldDef
     */
    @Override
    public TimestampDefImpl clone() {
        return new TimestampDefImpl(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof TimestampDefImpl) {
            TimestampDefImpl otherDef = (TimestampDefImpl) other;
            return (precision == otherDef.precision);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + ((Integer)precision).hashCode();
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public TimestampDef asTimestamp() {
        return this;
    }

    @Override
    public TimestampValueImpl createTimestamp(Timestamp value) {
        return new TimestampValueImpl(this, value);
    }

    @Override
    public TimestampValueImpl createTimestamp(String value) {
        return new TimestampValueImpl(this, value);
    }

    /**
     * This creates a TimestampValueImpl from the String format used
     * for sorted keys.
     */
    @Override
    public TimestampValueImpl createTimestamp(byte[] value) {
        return new TimestampValueImpl(this, value);
    }

    /*
     * Public api methods from TimestampDef
     */
    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public TimestampValueImpl fromString(String timestampString) {
        return fromString(timestampString, null, true);
    }

    @Override
    public TimestampValueImpl fromString(String timestampString,
                                         String pattern,
                                         boolean withZoneUTC) {

        if (timestampString == null) {
            throw new IllegalArgumentException("Timestamp string can not be " +
                "null");
        }
        return createTimestamp(parseString(timestampString,
                                           pattern,
                                           withZoneUTC));
    }

    @Override
    public TimestampValueImpl currentTimestamp() {
        return createTimestamp(new Timestamp(System.currentTimeMillis()));
    }

    /*
     * FieldDefImpl internal api methods
     */
    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isAny() ||
            superType.isAnyAtomic()) {
            return true;
        }

        if (superType.isTimestamp() &&
            precision <= superType.asTimestamp().getPrecision()) {
            return true;
        }

        return false;
    }

    /* Returns the max precision supported */
    static int getMaxPrecision() {
        return MAX_PRECISION;
    }

    /* Returns the max Timestamp with the precision */
    Timestamp getMaxValue() {
        return maxValues[precision];
    }

    /* Returns the min Timestamp with the precision */
    Timestamp getMinValue() {
        return minValues[precision];
    }

    /**
     * Returns the max number of byte that represents a timestamp with the
     * precision.
     */
    int getNumBytes() {
        return TimestampUtils.getNumBytes(precision);
    }

    private void validate() {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException
                ("Timestamp precision must be between 0 and " +
                 MAX_PRECISION + ", inclusive");
        }
    }
}
