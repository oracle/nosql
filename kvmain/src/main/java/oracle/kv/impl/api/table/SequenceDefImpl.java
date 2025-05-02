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

import java.io.Serializable;
import java.math.BigDecimal;

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Translator;
import oracle.kv.table.SequenceDef;

public class SequenceDefImpl implements SequenceDef, Serializable, Cloneable{

    private static final long serialVersionUID = 1L;

    private boolean isSetStart = false;
    private FieldValueImpl start = null;
    private boolean isSetIncrement = false;
    private FieldValueImpl increment = null;
    private boolean isSetMax = false;
    private FieldValueImpl max = null;
    private boolean isSetMin = false;
    private FieldValueImpl min = null;
    private boolean isSetCache = false;
    private FieldValueImpl cache = null;
    private boolean isSetCycle = false;
    private boolean cycle = false;

    public SequenceDefImpl(FieldDefImpl fdi,
                           Translator.IdentityDefHelper identityHelper) {

        if (identityHelper.isSetStart()) {
            setStartValue(init(fdi, identityHelper.getStart()));
        } else {
            start = init(fdi, "1");
        }

        if (identityHelper.isSetIncrement()) {
            setIncrementValue(init(fdi, identityHelper.getIncrement()));
        } else {
            increment = init(fdi, "1");
        }

        if (identityHelper.isSetMax()) {
            setMaxValue(init(fdi, identityHelper.getMax()));
        } else {
            max = init(fdi, "max");
        }

        if (identityHelper.isSetMin()) {
            setMinValue(init(fdi, identityHelper.getMin()));
        } else {
            min = init(fdi, "min");
        }

        if (identityHelper.isSetCache() && identityHelper.getCache() != null) {
            setCacheValue(init(fdi, identityHelper.getCache()));
        } else {
            cache = init(fdi, "1000");
        }

        if (identityHelper.isSetCycle()) {
            setCycle(identityHelper.getCycle());
        }
    }

    private FieldValueImpl init(FieldDefImpl fdi, String value) {
        switch (fdi.getType()) {
        case INTEGER:
            if (value.equals("max")) {
                return (FieldValueImpl)fdi.createInteger(Integer.MAX_VALUE);
            } else if (value.equals("min")) {
                return (FieldValueImpl)fdi.createInteger(Integer.MIN_VALUE);
            } else {
                return (FieldValueImpl)fdi.createInteger(Integer.parseInt(value));
            }
        case LONG:
            if (value.equals("max")) {
                return (FieldValueImpl)fdi.createLong(Long.MAX_VALUE);
            } else if (value.equals("min")) {
                return (FieldValueImpl)fdi.createLong(Long.MIN_VALUE);
            } else {
                return (FieldValueImpl)fdi.createLong(Long.parseLong(value));
            }
        case NUMBER:
            if (value.equals("max") || value.equals("min")) {
                return null;
            }
            return (FieldValueImpl)fdi.createNumber(new BigDecimal(value));
        default:
            throw new QueryException(
                "Identity field must be one of the following numeric " +
                "data types: INTEGER, LONG or NUMBER.");
        }
    }

    @Override
    public SequenceDefImpl clone() {
        try {
            return (SequenceDefImpl) super.clone();
        } catch (final CloneNotSupportedException ignore) {
        }
        return null;
    }

    @Override
    public boolean isSetStartValue() {
        return isSetStart;
    }

    public SequenceDefImpl setStartValue(FieldValueImpl value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }

        start = value;
        isSetStart = true;
        return this;
    }

    @Override
    public FieldValueImpl getStartValue() {
        return start;
    }

    @Override
    public boolean isSetIncrementValue() {
        return isSetIncrement;
    }

    public SequenceDefImpl setIncrementValue(FieldValueImpl value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }

        increment = value;
        isSetIncrement = true;
        return this;
    }

    @Override
    public FieldValueImpl getIncrementValue() {
        return increment;
    }

    @Override
    public boolean isSetMaxValue() {
        return isSetMax;
    }

    public SequenceDefImpl setMaxValue(FieldValueImpl value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }

        max = value;
        isSetMax = true;
        return this;
    }

    @Override
    public FieldValueImpl getMaxValue() {
        return max;
    }

    @Override
    public boolean isSetMinValue() {
        return isSetMin;
    }

    public SequenceDefImpl setMinValue(FieldValueImpl value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }

        min = value;
        isSetMin = true;
        return this;
    }

    @Override
    public FieldValueImpl getMinValue() {
        return min;
    }

    @Override
    public boolean isSetCacheValue() {
        return isSetCache;
    }

    public SequenceDef setCacheValue(FieldValueImpl value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }

        cache = value;
        isSetCache = true;
        return this;
    }

    @Override
    public FieldValueImpl getCacheValue() {
        return cache;
    }

    @Override
    public boolean isSetCycle() {
        return isSetCycle;
    }

    public SequenceDef setCycle(boolean cycle) {
        this.cycle = cycle;
        isSetCycle = true;
        return this;
    }

    @Override
    public boolean getCycle() {
        return cycle;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SequenceDef) {
            SequenceDef o = (SequenceDef)other;
            if (!equalsValue(start, o.getStartValue())) {
                return false;
            }
            if (!equalsValue(increment, o.getIncrementValue())) {
                return false;
            }
            if (!equalsValue(min, o.getMinValue())) {
                return false;
            }
            if (!equalsValue(max, o.getMaxValue())) {
                return false;
            }
            if (!equalsValue(cache, o.getCacheValue())) {
                return false;
            }
            return (cycle == o.getCycle());
        }
        return false;
    }

    private static boolean equalsValue(FieldValueImpl v1, FieldValueImpl v2) {
        if (v1 != null) {
            return v1.equals(v2);
        }
        return v2 == null;
    }

    @Override
    public int hashCode() {
        int code = 0;
        if (start != null) {
            code += start.hashcode();
        }
        if (increment != null) {
            code += increment.hashcode();
        }
        if (min != null) {
            code += min.hashcode();
        }
        if (max != null) {
            code += max.hashcode();
        }
        if (cache != null) {
            code += cache.hashcode();
        }
        code += Boolean.hashCode(cycle);
        return code;
    }

    @Override
    public String toString() {
        return "SequenceDefImpl[" +
            "start=" + start +
            ", increment=" + increment +
            ", max=" + max +
            ", min=" + min +
            ", cache=" + cache +
            ", cycle=" + cycle +
            ']';
    }
}
