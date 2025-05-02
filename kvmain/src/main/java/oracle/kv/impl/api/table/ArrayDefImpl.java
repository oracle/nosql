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

import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;

/**
 * ArrayDefImpl implements the ArrayDef interface.
 */
public class ArrayDefImpl extends FieldDefImpl implements ArrayDef {

    private static final long serialVersionUID = 1L;

    private final FieldDefImpl element;

    ArrayDefImpl(FieldDefImpl element, String description) {

        super(FieldDef.Type.ARRAY, description);
        if (element == null) {
            throw new IllegalArgumentException
                ("Array has no field and cannot be built");
        }

        this.element = element;
    }

    /**
     * This constructor is only used by test code.
     */
    ArrayDefImpl(FieldDefImpl element) {
        this(element, null);
    }

    private ArrayDefImpl(ArrayDefImpl impl) {
        super(impl);
        element = impl.element.clone();
    }

    /**
     * Constructor for FastExternalizable
     */
    ArrayDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.ARRAY);
        element = FieldDefImpl.readFastExternal(in, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link FieldDefImpl}) {@code element}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        element.writeFastExternal(out, serialVersion);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public ArrayDefImpl clone() {

        if (this == FieldDefImpl.Constants.arrayAnyDef ||
            this == FieldDefImpl.Constants.arrayJsonDef) {
            return this;
        }

        return new ArrayDefImpl(this);
    }

    @Override
    public int hashCode() {
        return element.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof ArrayDefImpl) {
            return element.equals(((ArrayDefImpl)other).getElement());
        }
        return false;
    }

    @Override
    public ArrayDef asArray() {
        return this;
    }

    @Override
    public ArrayValueImpl createArray() {
        return new ArrayValueImpl(this);
    }

    /*
     * Public api methods from ArrayDef
     */

    @Override
    public FieldDefImpl getElement() {
        return element;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return element.isPrecise();
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (this == superType) {
            return true;
        }

        if (superType.isArray()) {
            ArrayDefImpl supArray = (ArrayDefImpl)superType;
            return element.isSubtype(supArray.element);
        }

        if (superType.isJson()) {
            return element.isSubtype(Constants.jsonDef);
        }

        if (superType.isAny()) {
             return true;
        }

        return false;
    }

    /**
     * If called for an array the fieldName applies to a field in the array's
     * element, so pass it on.
     */
    @Override
    FieldDefImpl findField(String fieldName) {
        return element.findField(fieldName);
    }

    @Override
    public short getRequiredSerialVersion() {
        return element.getRequiredSerialVersion();
    }

    @Override
    int countTypes() {
        return element.countTypes() + 1; /* +1 for this field */
    }
}
