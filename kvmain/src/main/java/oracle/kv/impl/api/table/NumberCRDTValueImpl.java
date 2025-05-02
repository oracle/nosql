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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MRCounterValueSerializer;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.FieldDef;

public class NumberCRDTValueImpl extends NumberValueImpl
    implements MRCounterValueSerializer {

    private static final long serialVersionUID = 1L;

    private final Map<Integer, NumberValueImpl> fields = new TreeMap<>();

    NumberCRDTValueImpl() {
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
    public NumberCRDTValueImpl clone() {
        NumberCRDTValueImpl copy = new NumberCRDTValueImpl();
        for (Map.Entry<Integer, NumberValueImpl> entry : fields.entrySet()) {
            copy.putMRCounterEntry(entry.getKey(), entry.getValue().clone());
        }
        return copy;
    }

    @Override
    public long sizeof() {
        long size = SizeOf.OBJECT_OVERHEAD + 2 * SizeOf.OBJECT_REF_OVERHEAD;
        size += (SizeOf.OBJECT_REF_OVERHEAD + SizeOf.TREEMAP_OVERHEAD);
        for (Map.Entry<Integer, NumberValueImpl> entry : fields.entrySet()) {
            size += SizeOf.TREEMAP_ENTRY_OVERHEAD;
            size += SizeOf.OBJECT_OVERHEAD + 4; /* Size of region id. */
            size += entry.getValue().sizeof();
        }
        return size;
    }

    @Override
    public NumberDefImpl getDefinition() {
        return FieldDefImpl.Constants.numberMRCounterDef;
    }

    @Override
    public int hashCode() {
        int code = 1;
        for (Map.Entry<Integer, NumberValueImpl> entry : fields.entrySet()) {
            code = 17 ^ code + entry.getKey().hashCode() +
                entry.getValue().hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof NumberCRDTValueImpl) {
            NumberCRDTValueImpl otherValue = (NumberCRDTValueImpl) other;
            if (this == otherValue) {
                return true;
            }
            return fields.equals(otherValue.fields);
        }
        return false;
    }

    @Override
    public BigDecimal getDecimal() {
        /* Get the sum of values of all regions. */
        BigDecimal sum = new BigDecimal(0);
        for (Integer s : fields.keySet()) {
            if (s > 0) {
                sum = sum.add(fields.get(s).get());
            } else {
                sum = sum.subtract(fields.get(s).get());
            }
        }
        return sum;
    }

    @Override
    protected int compareNumber(NumberValueImpl other) {
        return get().compareTo(other.get());
    }

    @Override
    public void setDecimal(BigDecimal v) {
        throw new IllegalArgumentException("Cannot set value for MRCounter.");
    }

    @Override
    public Object getNumericValue() {
        BigDecimal decimal = get();
        Object result = null;
        try {
            result = decimal.intValueExact();
        } catch (ArithmeticException e) {
            /*Cannot convert to an int*/
            try {
                result = decimal.longValueExact();
            } catch (ArithmeticException ae) {
                /*Cannot convert to a long*/
                return decimal;
            }
            return Long.valueOf((long)result);
        }
        return Integer.valueOf((int)result);
    }

    @Override
    public String formatForKey(FieldDef field1, int storageSize) {
        throw new IllegalArgumentException("MRCounters cannot be a keys.");
    }

    @Override
    FieldValueImpl getNextValue() {
        return new NumberValueImpl(get().add(new BigDecimal(1)));
    }

    @Override
    public byte[] getBytes() {
        return NumberUtils.serialize(get());
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

        if (!count.isNumber()) {
            throw new IllegalArgumentException("The value should be a " +
                "number.");
        }
        NumberValueImpl numberValue = (NumberValueImpl)count;
        if (numberValue.isMRCounter()) {
            throw new IllegalArgumentException("Cannot put an MRCounter.");
        }
        fields.put(regionId, numberValue);
    }

    @Override
    public void incrementMRCounter(FieldValueImpl val, int regionId) {
        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for incrementMRCounter: " + regionId);
        }
        update(val, regionId);
    }

    @Override
    public void decrementMRCounter(FieldValueImpl val, int regionId) {
        if (!Region.isMultiRegionId(regionId)) {
            throw new IllegalArgumentException(
                "Invalid Region Id for decrementMRCounter: " + regionId);
        }
        update(val, -regionId);
    }

    private void update(FieldValueImpl val, int regionId) {

        if (!val.isNumber() || val.isMRCounter() ||
            val.asNumber().get().compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException(
                "The " + (regionId < 0 ? "decrement " : "increment ") +
                "value should be a non-negative Number.");
        }

        NumberValueImpl localCount = fields.computeIfAbsent(regionId,
            k -> FieldDefImpl.Constants.numberDef.createNumber(0));

        NumberValueImpl newLocalValue =
            FieldDefImpl.Constants.numberDef.
            createNumber(localCount.get().add(val.asNumber().get()));

        putMRCounterEntry(regionId, newLocalValue);
    }

    @Override
    public FieldValueImpl convertMRCounterToPlainValue() {
        return FieldDefImpl.Constants.numberDef.createNumber(getDecimal());
    }

    @Override
    public FieldValueImpl castToOtherMRCounter(FieldDefImpl targetDef) {

        if (!targetDef.isMRCounter()) {
            throw new IllegalArgumentException(
                "The target column type=" + targetDef.getType() + " is not an" +
                " MRCounter");
        }

        if (targetDef.isNumber()) {
            return this;
        }

        FieldValueImpl targetVal = targetDef.createCRDTValue();

        for (Map.Entry<Integer, NumberValueImpl> entry : fields.entrySet()) {

            Integer i = entry.getKey();
            NumberValueImpl val = entry.getValue();

            if (targetDef.isLong()) {
                if (val.getDecimal().
                    compareTo(new BigDecimal(Long.MAX_VALUE)) > 0) {
                    throw new IllegalArgumentException(
                        "Unable to cast this number MRCounter to a long " +
                        "MRCounter because an internal count exceeds the " +
                        "long max value.");
                }

                targetVal.putMRCounterEntry(
                    i,
                    FieldDefImpl.Constants.longDef.createLong(
                        val.castAsLong()));

            } else if (targetDef.isInteger()){
                if (val.getDecimal().
                    compareTo(new BigDecimal(Integer.MAX_VALUE)) > 0) {
                    throw new IllegalArgumentException(
                        "Unable to cast this number MRCounter to an integer " +
                        "MRCounter because an internal count exceeds the " +
                        "integer max value.");
                }
                targetVal.putMRCounterEntry(i,
                    FieldDefImpl.Constants.integerDef.createInteger(
                        val.castAsInt()));
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
