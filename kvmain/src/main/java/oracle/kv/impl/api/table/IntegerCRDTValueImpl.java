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

public class IntegerCRDTValueImpl extends IntegerValueImpl
    implements MRCounterValueSerializer {

    private static final long serialVersionUID = 1L;

    private final Map<Integer, IntegerValueImpl> fields = new TreeMap<>();

    IntegerCRDTValueImpl() {
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
    public int get() {
        /* Get the sum of values of all regions. */
        int sum = 0;
        for (Integer s : fields.keySet()) {
            if (s > 0) {
                sum += fields.get(s).get();
            } else {
                sum -= fields.get(s).get();
            }
        }

        return sum;
    }

    @Override
    public void setInt(int v) {
        throw new IllegalArgumentException("Cannot set a value for MRCounter.");
    }

    @Override
    public IntegerCRDTValueImpl clone() {
        IntegerCRDTValueImpl copy = new IntegerCRDTValueImpl();
        for (Map.Entry<Integer, IntegerValueImpl> entry : fields.entrySet()) {
            copy.putMRCounterEntry(entry.getKey(), entry.getValue().clone());
        }
        return copy;
    }

    @Override
    public IntegerDefImpl getDefinition() {
        return FieldDefImpl.Constants.intMRCounterDef;
    }

    @Override
    public int hashCode() {
        int code = 1;
        for (Map.Entry<Integer, IntegerValueImpl> entry : fields.entrySet()) {
            code = 17 ^ code + entry.getKey().hashCode() +
                entry.getValue().hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof IntegerCRDTValueImpl) {
            IntegerCRDTValueImpl otherValue = (IntegerCRDTValueImpl) other;
            if (this == otherValue) {
                return true;
            }
            return fields.equals(otherValue.fields);
        }
        return false;
    }

    @Override
    public long sizeof() {
        long size = SizeOf.OBJECT_OVERHEAD + 2 * SizeOf.OBJECT_REF_OVERHEAD;
        size += (SizeOf.OBJECT_REF_OVERHEAD + SizeOf.TREEMAP_OVERHEAD);
        for (Map.Entry<Integer, IntegerValueImpl> entry : fields.entrySet()) {
            size += SizeOf.TREEMAP_ENTRY_OVERHEAD;
            size += SizeOf.OBJECT_OVERHEAD + 4; /* Size of integer. */
            size += entry.getValue().sizeof();
        }
        return size;
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        throw new IllegalArgumentException("MRCounters cannot be keys.");
    }

    @Override
    void validateStorageSize(int size) {
        throw new IllegalArgumentException("MRCounters cannot be keys.");
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
    public void putMRCounterEntry(Integer regionID, FieldValueImpl count) {

        if (!count.isInteger()) {
            throw new IllegalArgumentException(
                "The value should be an integer.");
        }

        IntegerValueImpl intValue = (IntegerValueImpl)count;

        if (intValue.isMRCounter() ||
            intValue.getValueType() == ValueType.INTEGER_RANGE_VALUE) {
            throw new IllegalArgumentException("Cannot put an MRCounter or " +
                "range value. ");
        }
        fields.put(regionID, intValue);
    }

    @Override
    public void incrementMRCounter(FieldValueImpl val, int regionId) {

        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for incrementMRCounter: " + regionId);
        }

        if (!val.isInteger() || val.isMRCounter()) {
            throw new IllegalArgumentException(
                "The increment value should a non-negative integer");
        }

        int inc = ((IntegerValueImpl)val).get();

        if (inc < 0) {
            throw new IllegalArgumentException(
                "The increment value should a non-negative integer");
        }

        IntegerValueImpl localCount =
            fields.computeIfAbsent(regionId,
            k -> FieldDefImpl.Constants.integerDef.createInteger(0));

        long newLocal = (long)localCount.get() + inc;

        if (newLocal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "The increment causes the counter for the local region " +
                "to overflow.");
        }

        IntegerValueImpl newLocalValue =
            FieldDefImpl.Constants.integerDef.createInteger((int)newLocal);

        putMRCounterEntry(regionId, newLocalValue);
    }

    @Override
    public void decrementMRCounter(FieldValueImpl val, int regionId) {

        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for decrementMRCounter: " + regionId);
        }

        if (!val.isInteger() || val.isMRCounter()) {
            throw new IllegalArgumentException(
                "The decrement value should be a non-negative integer.");
        }

        int dec = ((IntegerValueImpl)val).get();

        if (dec < 0) {
            throw new IllegalArgumentException(
                "The decrement value should be a non-negative integer");
        }

        IntegerValueImpl localCount =
            fields.computeIfAbsent(-regionId,
            k -> FieldDefImpl.Constants.integerDef.createInteger(0));

        long newLocal = (long)localCount.get() + dec;

        if (newLocal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "The decrement causes the counter for the local region " +
                "to overflow.");
        }

        IntegerValueImpl newLocalValue =
            FieldDefImpl.Constants.integerDef.createInteger((int)newLocal);

        putMRCounterEntry(-regionId, newLocalValue);

    }

    @Override
    public FieldValueImpl convertMRCounterToPlainValue() {
        return FieldDefImpl.Constants.integerDef.createInteger(get());
    }

    @Override
    public FieldValueImpl castToOtherMRCounter(FieldDefImpl targetDef) {

        if (!targetDef.isMRCounter()) {
            throw new IllegalArgumentException(
                "The target field type=" + targetDef.getType() + " is not an " +
                "MRCounter");
        }

        if (targetDef.isInteger()) {
            return this;
        }

        FieldValueImpl targetVal = targetDef.createCRDTValue();

        for (Map.Entry<Integer, IntegerValueImpl> entry : fields.entrySet()) {

            Integer i = entry.getKey();
            IntegerValueImpl val = entry.getValue();

            if (targetDef.isLong()) {
                targetVal.putMRCounterEntry(
                    i, FieldDefImpl.Constants.longDef.createLong(val.get()));
            } else if (targetDef.isNumber()){
                targetVal.putMRCounterEntry(i, val.castAsNumber());
            } else {
                throw new IllegalArgumentException("The target type : " +
                    targetDef.getType() + " is not allowed for an MRCounter");
            }
        }

        return targetVal;
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

    @Override
    public MRCounterValueSerializer asMRCounterSerializer() {
        return this;
    }

}
