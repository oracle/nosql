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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.NumberDef;

/**
 * NumberDefImpl implements the NumberDef interface.
 */
public class NumberDefImpl extends FieldDefImpl
    implements NumberDef {

    private static final long serialVersionUID = 1L;

    final private boolean isMRCounter;

    NumberDefImpl(String description) {
        super(Type.NUMBER, description);
        isMRCounter = false;
    }

    NumberDefImpl() {
        super(Type.NUMBER);
        isMRCounter = false;
    }

    private NumberDefImpl(NumberDefImpl impl) {
        super(impl);
        isMRCounter = impl.isMRCounter;
    }

    /**
     * Constructor for CRDT.
     */
    NumberDefImpl(boolean isMRCounter) {
        this(isMRCounter, null);
    }

    NumberDefImpl(boolean isMRCounter, String description) {
        super(Type.NUMBER, description);
        this.isMRCounter = isMRCounter;
    }

    /**
     * Constructor for FastExternalizable
     */
    NumberDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.NUMBER);

        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >= SerialVersion.COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1) {
            isMRCounter = in.readBoolean();
        } else {
            isMRCounter = false;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        if (isMRCounter) {
            out.writeBoolean(true);
        } else {
            out.writeBoolean(false);
        }
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public NumberDefImpl clone() {
        return new NumberDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (isMRCounter ? Boolean.hashCode(isMRCounter) : 0);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NumberDefImpl)) {
            return false;
        }
        return ((NumberDefImpl)other).isMRCounter == isMRCounter;
    }

    @Override
    public boolean isValidKeyField() {
        if (isMRCounter) {
            /* CRDT should not be primary key. */
            return false;
        }
        return true;
    }

    /*
     * BigDecimal can be an indexed field. A binary format exists that sorts
     * properly.
     */
    @Override
    public boolean isValidIndexField() {
        if (isMRCounter) {
            //TODO: Remove this check after index is supported on CRDT columns.
            return false;
        }
        return true;
    }

    @Override
    public NumberDef asNumber() {
        return this;
    }

    @Override
    public boolean isMRCounter() {
        return isMRCounter;
    }

    @Override
    public NumberValueImpl createNumber(int value) {
        return new NumberValueImpl(value);
    }

    @Override
    public NumberValueImpl createNumber(long value) {
        return new NumberValueImpl(value);
    }

    @Override
    public NumberValueImpl createNumber(float value) {
        return new NumberValueImpl(BigDecimal.valueOf(value));
    }

    @Override
    public NumberValueImpl createNumber(double value) {
        return new NumberValueImpl(BigDecimal.valueOf(value));
    }

    @Override
    public NumberValueImpl createNumber(BigDecimal value) {
        return new NumberValueImpl(value);
    }

    @Override
    public NumberValueImpl createNumberFromIndexField(String value) {
        return new NumberValueImpl(value);
    }

    @Override
    public NumberValueImpl createNumber(String value) {
        return new NumberValueImpl(new BigDecimal(value));
    }

    @Override
    NumberValueImpl createNumber(byte[] value) {
        return new NumberValueImpl(value);
    }

    public void validateValue(byte[] value) {
        NumberUtils.validateValue(value);
    }

    @Override
    public short getRequiredSerialVersion() {
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (!isMRCounter) {
            return super.getRequiredSerialVersion();
        }
        return (short) Math.max(SerialVersion.COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1,
                                super.getRequiredSerialVersion());
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyJsonAtomic() ||
            superType.isAnyAtomic() ||
            superType.isJson()) {
            return true;
        }

        return false;
    }

    @Override
    public NumberCRDTValueImpl createCRDTValue() {
        return new NumberCRDTValueImpl();
    }

    @Override
    public FieldDefImpl getCRDTElement() {
        return FieldDefImpl.Constants.numberDef;
    }

    @Override
    public Type getJsonCounterType() {
        return Type.JSON_NUM_MRCOUNTER;
    }
}
