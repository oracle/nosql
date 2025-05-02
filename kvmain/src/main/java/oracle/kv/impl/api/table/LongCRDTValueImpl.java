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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MRCounterValueSerializer;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.FieldDef;

public class LongCRDTValueImpl extends LongValueImpl
    implements MRCounterValueSerializer {

    private static final long serialVersionUID = 1L;

    private final Map<Integer, LongValueImpl> fields = new TreeMap<>();

    LongCRDTValueImpl() {
        super(0);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        /*
         * The default value for CRDT is always 0 so there is
         *  no need to serilize this class.
         */
        fastExternalNotSupported();
    }

    @Override
    public LongCRDTValueImpl clone() {
        LongCRDTValueImpl copy = new LongCRDTValueImpl();
        for (Map.Entry<Integer, LongValueImpl> entry : fields.entrySet()) {
            copy.putMRCounterEntry(entry.getKey(), entry.getValue().clone());
        }
        return copy;
    }

    @Override
    public LongDefImpl getDefinition() {
        return FieldDefImpl.Constants.longMRCounterDef;
    }

    @Override
    public long sizeof() {
        long size = SizeOf.OBJECT_OVERHEAD + 2 * SizeOf.OBJECT_REF_OVERHEAD;
        size += (SizeOf.OBJECT_REF_OVERHEAD + SizeOf.TREEMAP_OVERHEAD);
        for (Map.Entry<Integer, LongValueImpl> entry : fields.entrySet()) {
            size += SizeOf.TREEMAP_ENTRY_OVERHEAD;
            size += SizeOf.OBJECT_OVERHEAD + 4; /* Size of region id. */
            size += entry.getValue().sizeof();
        }
        return size;
    }

    @Override
    public int hashCode() {
        int code = 1;
        for (Map.Entry<Integer, LongValueImpl> entry : fields.entrySet()) {
            code = 17 ^ code + entry.getKey().hashCode() +
                entry.getValue().hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof LongCRDTValueImpl) {
            LongCRDTValueImpl otherValue = (LongCRDTValueImpl) other;
            if (this == otherValue) {
                return true;
            }
            return fields.equals(otherValue.fields);
        }
        return false;
    }

    @Override
    public void setLong(long v) {
        throw new IllegalArgumentException("Cannot set value for MRCounter.");
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        throw new IllegalArgumentException("MRCounters cannot be keys.");
    }

    @Override
    public long get() {
        /* Get the sum of values of all regions. */
        long sum = 0;
        for (Integer s : fields.keySet()) {
            if (s > 0) {
                sum = sum + fields.get(s).get();
            } else {
                sum = sum - fields.get(s).get();
            }
        }
        return sum;
    }

    @Override
    public void toStringBuilder(StringBuilder sb,
                                DisplayFormatter formatter) {
        sb.append(toString());

    }

    @Override
    public boolean isMRCounter() {
        return true;
    }

    @Override
    public Map<Integer, FieldValueImpl> getMRCounterMap() {
        return Collections.unmodifiableMap(fields);
    }

    @Override
    public void putMRCounterEntry(Integer regionId, FieldValueImpl count) {

        if (!count.isLong()) {
            throw new IllegalArgumentException("The value should be a " +
                "long.");
        }
        LongValueImpl longValue = (LongValueImpl)count;
        if (longValue.isMRCounter() ||
            longValue.getValueType() == ValueType.LONG_RANGE_VALUE) {
            throw new IllegalArgumentException("Cannot put an MRCounter or " +
                "range value.");
        }
        fields.put(regionId, longValue);
    }

    @Override
    public void incrementMRCounter(FieldValueImpl val, int regionId) {

        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for incrementMRCounter: " + regionId);
        }

        if (!val.isLong() || val.isMRCounter()) {
            throw new IllegalArgumentException(
                "The increment value should be a non-negative Long");
        }

        long inc = ((LongValueImpl)val).get();

        if (inc < 0) {
            throw new IllegalArgumentException(
                "The increment value should be a non-negative Long");
        }

        LongValueImpl localCount =
            fields.computeIfAbsent(regionId,
            k -> FieldDefImpl.Constants.longDef.createLong(0));

        if (Long.MAX_VALUE - inc < localCount.get()) {
            throw new IllegalArgumentException(
                "The increment causes the counter for the local region " +
                "to overflow.");
        }

        LongValueImpl newLocalValue = FieldDefImpl.Constants.longDef.
            createLong(localCount.get() + inc);

        putMRCounterEntry(regionId, newLocalValue);
    }

    @Override
    public void decrementMRCounter(FieldValueImpl val, int regionId) {

        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for decrementMRCounter: " + regionId);
        }

        if (!val.isLong() || val.isMRCounter()) {
            throw new IllegalArgumentException(
                "The decrement value should be a non-negative Long.");
        }

        long dec = ((LongValueImpl)val).get();

        if (dec < 0) {
            throw new IllegalArgumentException(
                "The decrement value should be a non-negative Long");
        }

        LongValueImpl localCount =
            fields.computeIfAbsent(-regionId,
            k -> FieldDefImpl.Constants.longDef.createLong(0));

        if (Long.MAX_VALUE - dec < localCount.get()) {
            throw new IllegalArgumentException(
                "The decrement causes the counter for the local region " +
                "to overflow.");
        }

        LongValueImpl newLocalValue = FieldDefImpl.Constants.longDef.
            createLong(localCount.get() + dec);

        putMRCounterEntry(-regionId, newLocalValue);
    }

    @Override
    public FieldValueImpl convertMRCounterToPlainValue() {
        return FieldDefImpl.Constants.longDef.createLong(get());
    }

    @Override
    public FieldValueImpl castToOtherMRCounter(FieldDefImpl targetDef) {

        if (!targetDef.isMRCounter()) {
            throw new IllegalArgumentException(
                "The target column type=" + targetDef.getType() + " is not an" +
                " MRCounter");
        }

        if (targetDef.isLong()) {
            return this;
        }

        FieldValueImpl targetVal = targetDef.createCRDTValue();

        for (Map.Entry<Integer, LongValueImpl> entry : fields.entrySet()) {

            Integer i = entry.getKey();
            LongValueImpl val = entry.getValue();

            if (targetDef.isInteger()) {
                if (val.get() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "Unable to cast this long MRCounter to an integer " +
                        "MRCounter because an internal count exceeds the " +
                        "integer max value.");
                }

                targetVal.putMRCounterEntry(
                    i,
                    FieldDefImpl.Constants.integerDef.createInteger(
                        val.castAsInt()));

            } else if (targetDef.isNumber()){
                targetVal.putMRCounterEntry(i, val.castAsNumber());
            } else {
                throw new IllegalArgumentException("The target type : " +
                    targetDef.getType() + " is not allowed for an MRCounter");
            }
        }

        return targetVal;
    }

    @Override
    public MRCounterValueSerializer asMRCounterSerializer() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Entry<Integer, FieldValueSerializer>> iterator() {
        Map<Integer, ?> map = fields;
        return ((Map<Integer, FieldValueSerializer>)map).entrySet().iterator();
    }

    @Override
    public int size() {
        return fields.size();
    }

}
