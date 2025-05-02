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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;

/**
 * Implementation of sequences for table IDENTITY column.
 */
public class SequenceImpl {

    private SequenceImpl() {}


    /**
     * Get the sequence generator name for the identity column in a table.
     */
    public static String getSgName(TableImpl table) {
        if (!table.hasIdentityColumn()) {
            return null;
        }
        int identityColumn = table.getIdentityColumn();
        return getSgName(table, identityColumn);
    }

    /**
     * Get the sequence generator name for the given identity column in a
     * table.
     */
    public static String getSgName(TableImpl table, int identityColumn) {
        if (table.getIdentityColumn() != identityColumn) {
            throw new IllegalStateException("Wrong identity column number.");
        }
        String idname = table.getFieldMap().
            getFieldName(identityColumn);
        return table.getId() + "." + idname;
    }

    public static class SGKey {

        public final SGAttributesTableDesc.SGType sgType;
        public final String sgName;
        public long tableId;

        public SGKey(SGAttributesTableDesc.SGType type,
                     String name,
                     long id) {
            sgType = type;
            sgName = name;
            this.tableId = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SGKey)) {
                return false;
            }

            SGKey that = (SGKey) o;

            if (sgType != that.sgType) {
                return false;
            }
            if (tableId != that.tableId) {
                return false;
            }

            return sgName.equals(that.sgName);
        }

        @Override
        public int hashCode() {
            int result = sgType.hashCode();
            result = 31 * result + sgName.hashCode();
            result = 31 * result + (int) (tableId ^ (tableId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "SequenceKey{" +
                "sgType=" + sgType +
                ", sgName=" + sgName +
                ", tableId='" + tableId + '\'' +
                '}';
        }
    }

    /**
     * Sequence generator cache for Integer values.
     */
    public static class SGIntegerValues extends SGValues<Integer> {

        private SGIntegerValues(Type dataType, long increment) {
            super(dataType, increment);
        }

        private SGIntegerValues(DataInput in, short serialVersion)
            throws IOException {
            super(Type.INTEGER, in, serialVersion);
            if (in.readByte() != 0) {
                currentValue = in.readInt();
                lastValue = in.readInt();
            }
        }

        @Override
        public void update(BigDecimal newCurrentValue,BigDecimal newLastValue) {
            currentValue = newCurrentValue.intValue();
            lastValue = newLastValue.intValue();
        }

        /**
         * Writes this object to the output stream.
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether
         *      currentValue and lastValue are present</i>
         * <li> <i>[Choice]</i> ({@link Integer}) <i>currentValue</i>
         * <li> <i>[Choice]</i> ({@link Integer}) <i>lastValue</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            if (currentValue != null && lastValue != null) {
                out.writeBoolean(true);
                out.writeInt(currentValue);
                out.writeInt(lastValue);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    /**
     * Sequence generator cache for Long values.
     */
    public static class SGLongValues extends SGValues<Long> {

        private SGLongValues(Type dataType, long increment) {
            super(dataType, increment);
        }

        private SGLongValues(DataInput in, short serialVersion)
            throws IOException {
            super(Type.LONG, in, serialVersion);
            if (in.readByte() != 0) {
                currentValue = in.readLong();
                lastValue = in.readLong();
            }
        }

        @Override
        public void update(BigDecimal newCurrentValue,BigDecimal newLastValue) {
            currentValue = newCurrentValue.longValue();
            lastValue = newLastValue.longValue();
        }

        /**
         * Writes this object to the output stream.
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether
         *      currentValue and lastValue are present</i>
         * <li> <i>[Choice]</i> ({@link Long}) <i>currentValue</i>
         * <li> <i>[Choice]</i> ({@link Long}) <i>lastValue</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            if (currentValue != null && lastValue != null) {
                out.writeBoolean(true);
                out.writeLong(currentValue);
                out.writeLong(lastValue);
            }  else {
                out.writeBoolean(false);
            }
        }
    }

    /**
     * Sequence generator cache for Number (BigDecimal) values.
     */
    public static class SGNumberValues extends SGValues<BigDecimal> {

        private SGNumberValues(Type dataType, long increment) {
            super(dataType, increment);
        }

        private SGNumberValues(DataInput in, short serialVersion)
            throws IOException {
            super(Type.NUMBER, in, serialVersion);
            if (in.readByte() != 0) {
                currentValue = new BigDecimal(readString(in, serialVersion));
                lastValue = new BigDecimal(readString(in, serialVersion));
            }
        }

        @Override
        public void update(BigDecimal newCurrentValue,BigDecimal newLastValue) {
            currentValue = newCurrentValue;
            lastValue = newLastValue;
        }

        /**
         * Writes this object to the output stream.
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether
         *      currentValue and lastValue are present</i>
         * <li> <i>[Choice]</i> ({@link SerializationUtil#writeString String})
         * <i>currentValue</i>
         * <li> <i>[Choice]</i> ({@link SerializationUtil#writeString String})
         * <i>lastValue</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            if (currentValue != null && lastValue != null) {
                out.writeBoolean(true);
                SerializationUtil.writeString(out, serialVersion,
                                              currentValue.toString());
                SerializationUtil.writeString(out, serialVersion,
                                              lastValue.toString());
            } else {
                out.writeBoolean(false);
            }
        }
    }

    public static abstract class SGValues<T> implements FastExternalizable {
        private final Type dataType;

        public long increment;
        public long attrVersion;

        public T currentValue;
        public T lastValue;

        /**
         * Gets a new SGValues object of the specified type and increment.
         */
        public static SGValues<?> newInstance(Type dataType, long increment) {
            if (dataType == null) {
                throw new IllegalArgumentException("Data-type is required.");
            }
            switch (dataType) {
            case INTEGER:
                return new SGIntegerValues(dataType, increment);
            case LONG:
                return new SGLongValues(dataType, increment);
            case NUMBER:
                return new SGNumberValues(dataType, increment);
            default:
                throw new IllegalArgumentException("Sequence number must " +
                    "be one of the following numeric data types: " +
                    "INTEGER, LONG or NUMBER: " + dataType);
            }
        }

        protected SGValues(Type dataType, long increment) {
            this.dataType = dataType;
            this.increment = increment;
        }

        protected SGValues(Type dataType, DataInput in,
                           @SuppressWarnings("unused") short serialVersion)
            throws IOException {
            this.dataType = dataType;
            increment = in.readLong();
            attrVersion = in.readLong();
        }

        /**
         * Updates currentValue and lastValue with the specified values.
         */
        public abstract void update(BigDecimal newCurrentValue,
                                    BigDecimal newLastValue);

        private static SGValues<?> readFastExternal(DataInput in,
                                                    short serialVersion)
            throws IOException {

            final Type dataType = Type.readFastExternal(in, serialVersion);

            switch(dataType) {
            case INTEGER:
                return new SGIntegerValues(in, serialVersion);
            case LONG:
                return new SGLongValues(in, serialVersion);
            case NUMBER:
                return new SGNumberValues(in, serialVersion);
            default:
                throw new IllegalStateException("Sequence number must " +
                    "be one of the following numeric data types: " +
                    "INTEGER, LONG or NUMBER: " + dataType);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link Type}) <i>dataType})
         * <li> ({@link Long}) <i>increment</i>
         * <li> ({@link Long}) <i>attrVersion</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {
            dataType.writeFastExternal(out, serialVersion);
            out.writeLong(increment);
            out.writeLong(attrVersion);
        }

        public Type getType() {
            return dataType;
        }

        @Override
        public String toString() {
            return "SGValues{" +
                "dataType=" + dataType +
                ", increment=" + increment +
                ", attrVersion=" + attrVersion +
                ", currentValue=" + currentValue +
                ", lastValue=" + lastValue +
                '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SGValues)) {
                return false;
            }
            final SGValues<?> other = (SGValues<?>) obj;
            return (dataType == other.dataType) &&
                (increment == other.increment) &&
                (attrVersion == other.attrVersion) &&
                Objects.equals(currentValue, other.currentValue) &&
                Objects.equals(lastValue, other.lastValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataType, increment, attrVersion, currentValue,
                                lastValue);
        }
    }

    public static class SGAttrsAndValues implements FastExternalizable {

        private SGAttributes attributes = null;
        private SGValues<?> values = null;

        public SGAttrsAndValues() {
        }

        public SGAttrsAndValues(DataInput in, short serialVersion)
            throws IOException {
            if (in.readByte() != 0) {
                attributes = SGAttributes.
                    createSGAttributes(in, serialVersion);
            }
            if (in.readByte() != 0) {
                values = SGValues.readFastExternal(in, serialVersion);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerializationUtil#writeFastExternalOrNull SGAttributes
         *      or null})
         * <li> ({@link SerializationUtil#writeFastExternalOrNull SGValues
         *      or null})
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {
            writeFastExternalOrNull(out, serialVersion, attributes);
            writeFastExternalOrNull(out, serialVersion, values);
        }

        public void setAttributes(SGAttributes attributes) {
            this.attributes = attributes;
        }

        public void setValueCache(SGValues<?> values) {
            this.values = values;
        }

        public SGAttributes getAttributes() {
            return this.attributes;
        }

        public SGValues<?> getValues() {
            return this.values;
        }

        public boolean containsAttributes() {
            return attributes != null;
        }

        public boolean containsValues() {
            return values != null;
        }

        @Override
        public String toString() {
            return "SGAttrsAndValues{" +
                "attributes=" + attributes +
                ", values=" + values +
                '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SGAttrsAndValues)) {
                return false;
            }
            final SGAttrsAndValues other = (SGAttrsAndValues) obj;
            return Objects.equals(attributes, other.attributes) &&
                Objects.equals(values, other.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attributes, values);
        }
    }

    public static class SGAttributes implements FastExternalizable {

        private BigDecimal start = null;
        private Long increment = null;
        private BigDecimal max = null;
        private BigDecimal min = null;
        private Long cache = null;
        private boolean cycle = false;
        private long version;

        public SGAttributes(Row row) {
            if (row == null) {
                throw new IllegalArgumentException("Row must be non-null.");
            }

            start = row.get(SGAttributesTableDesc.COL_NAME_STARTWITH).
                asNumber().get();
            increment = row.get(SGAttributesTableDesc.COL_NAME_INCREMENTBY).
                asLong().get();
            FieldValue maxField =
                row.get(SGAttributesTableDesc.COL_NAME_MAXVALUE);

            if (!maxField.isNull()) {
                max = row.get(SGAttributesTableDesc.COL_NAME_MAXVALUE).
                    asNumber().get();
            } else {
                max = null;
            }

            FieldValue minField = row.
                get(SGAttributesTableDesc.COL_NAME_MINVALUE);

            if (!minField.isNull()) {
                min = row.get(SGAttributesTableDesc.COL_NAME_MINVALUE).
                    asNumber().get();
            } else {
                min = null;
            }

            cache = row.get(SGAttributesTableDesc.COL_NAME_CACHE).
                asLong().get();
            cycle = row.get(SGAttributesTableDesc.COL_NAME_CYCLE).
                asBoolean().get();
            version = row.get(SGAttributesTableDesc.COL_NAME_VERSION).
                asLong().get();
        }

        private SGAttributes(BigDecimal start,
                            Long increment,
                            BigDecimal max,
                            BigDecimal min,
                            Long cache,
                            boolean cycle,
                            long version) {
            this.start = start;
            this.increment = increment;
            this.max = max;
            this.min = min;
            this.cache = cache;
            this.cycle = cycle;
            this.version = version;
        }

        private static SGAttributes createSGAttributes(DataInput in,
                                                       short serialVersion)
            throws IOException {

            BigDecimal start;
            if (in.readByte() != 0) {
                start = new BigDecimal(readString(in, serialVersion));
            } else {
                start = null;
            }

            Long increment = in.readLong();
            BigDecimal max;
            if (in.readByte() != 0) {
                max = new BigDecimal(readString(in, serialVersion));
            } else {
                max = null;
            }

            BigDecimal min;
            if (in.readByte() != 0) {
                min = new BigDecimal(readString(in, serialVersion));
            } else {
                min = null;
            }

            Long cache = in.readLong();
            Boolean cycle = in.readBoolean();
            Long version = in.readLong();

            return new SGAttributes(start, increment, max, min, cache, cycle,
                version);
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether start
         *      is present</i>
         * <li> ({@link SerializationUtil#writeString String}) {@link
         *      #getStartValue start}
         * <li> ({@link DataOutput#writeLong long}) {@link #getIncrementValue
         *      increment}
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether max
         *      is present</i>
         * <li> ({@link SerializationUtil#writeString String}) {@link
         *      #getMaxValue max}
         * <li> ({@link DataOutput#writeBoolean boolean}) <i>whether min
         *      is present</i>
         * <li> ({@link SerializationUtil#writeString String}) {@link
         *      #getMinValue min}
         * <li> ({@link DataOutput#writeLong long}) {@link #getCacheValue
         *      cache}
         * <li> ({@link DataOutput#writeBoolean boolean})
         *      {@link #getCycle cycle}
         * <li> ({@link DataOutput#writeLong long}) {@link #getVersion
         *      version}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {

            if (start != null) {
                out.writeBoolean(true);
                writeString(out, serialVersion, start.toString());
            } else {
                out.writeBoolean(false);
            }

            out.writeLong(increment);

            if (max != null) {
                out.writeBoolean(true);
                writeString(out, serialVersion, max.toString());
            } else {
                out.writeBoolean(false);
            }

            if (min != null) {
                out.writeBoolean(true);
                writeString(out, serialVersion, min.toString());
            } else {
                out.writeBoolean(false);
            }
            out.writeLong(cache);
            out.writeBoolean(cycle);
            out.writeLong(version);

        }

        public void setVersion(long version) {
            this.version = version;
        }

        public long getVersion() {
            return version;
        }


        public BigDecimal getStartValue() {
            return start;
        }

        public Long getIncrementValue() {
            return increment;
        }

        public BigDecimal getMaxValue() {
            return max;
        }

        public BigDecimal getMinValue() {
            return min;
        }

        public Long getCacheValue() {
            return cache;
        }

        public boolean getCycle() {
            return cycle;
        }

        @Override
        public String toString() {
            return "SGAttributes {start: " + getStartValue() +
                   " increment: " + getIncrementValue() +
                   " max: " + getMaxValue() +
                   " min: " + getMinValue() +
                   " cache: " + getCacheValue() +
                   " cycle: " + getCycle() + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SGAttributes)) {
                return false;
            }
            final SGAttributes other = (SGAttributes) obj;
            return Objects.equals(start, other.start) &&
                Objects.equals(increment, other.increment) &&
                Objects.equals(max, other.max) &&
                Objects.equals(min, other.min) &&
                Objects.equals(cache, other.cache) &&
                (cycle == other.cycle) &&
                (version == other.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, increment, max, min, cache, cycle,
                                version);
        }
    }
}
