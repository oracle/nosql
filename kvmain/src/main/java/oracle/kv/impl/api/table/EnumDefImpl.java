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

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;

public class EnumDefImpl extends FieldDefImpl implements EnumDef {

    private static final long serialVersionUID = 1L;

    private final String[] values;

    /* requires names for records. */
    private String name;

    private transient int encodingLen;

    EnumDefImpl(final String[] values, final String description) {

        super(FieldDef.Type.ENUM, description);
        this.values = values;

        validate();
        initEncodingLen();
    }

    EnumDefImpl(
        final String name,
        final String[] values,
        final String description) {

        this(values, description);

        if (name == null) {
            throw new IllegalArgumentException
                ("Enumerations require a name");
        }

        this.name = name;
    }

    EnumDefImpl(final String name, String[] values) {
        this(name, values, null);
    }

    private EnumDefImpl(EnumDefImpl impl) {
        super(impl);
        this.name = impl.name;
        values = Arrays.copyOf(impl.values, impl.values.length);
        encodingLen = impl.encodingLen;
    }

    /**
     * Constructor for FastExternalizable
     */
    EnumDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.ENUM);
        name = readString(in, serialVersion);
        final int nValues = readNonNullSequenceLength(in);
        values = new String[nValues];
        for (int i = 0; i < nValues; i++) {
            values[i] = readString(in, serialVersion);
        }
        initEncodingLen();
    }

     /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@code name}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@code values} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeString String}) <i>value</i>
     *    </ol>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writeNonNullSequenceLength(out, values.length);
        for (String value : values) {
            writeString(out, serialVersion, value);
        }
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public EnumDefImpl clone() {
        return new EnumDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof EnumDefImpl) {
            EnumDefImpl otherDef = (EnumDefImpl) other;
            return Arrays.equals(values, otherDef.getValues());
        }
        return false;
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
    public EnumDef asEnum() {
        return this;
    }

    @Override
    public EnumValueImpl createEnum(String value) {
        return new EnumValueImpl(this, value);
    }

    /*
     * Public api methods from EnumDef
     */

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public String[] getValues() {
        return values;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isEnum()) {
            return this.equals(superType);
        }

        if (superType.isAny() || superType.isAnyAtomic()) {
            return true;
        }

        return false;
    }

    /*
     * local methods
     */

    public void setName(String n) {
        name = n;

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                "Enumeration types require a name");
        }
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initEncodingLen();
    }

    private void initEncodingLen() {
        encodingLen = SortableString.encodingLength(values.length);
        if (encodingLen < 2) {
            encodingLen = 2;
        }
    }

    public int indexOf(String enumValue) {
        for (int i = 0; i < values.length; i++) {
            if (enumValue.equals(values[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException
            ("Value is not valid for the enumeration: " + enumValue);
    }

    /**
     * TODO
     */
    int getEncodingLen() {
        return encodingLen;
    }

    /*
     * Simple value comparison function, used to avoid circular calls
     * between equals() and EnumValueImpl.equals().
     */
    public boolean valuesEqual(EnumDefImpl other) {
        return Arrays.equals(values, other.getValues());
    }

    /*
     * Make sure that the type definition is valid: Check for duplicate values
     * and validate the values of the enumeration strings themselves.
     */
    private void validate() {
        if (values == null || values.length < 1) {
            throw new IllegalArgumentException
                ("Enumerations requires one or more values");
        }

        HashSet<String> set = new HashSet<String>();
        for (String value: values) {
            validateStringValue(value);
            if (set.contains(value)) {
                throw new IllegalArgumentException
                    ("Duplicated enumeration value: " + value);
            }
            set.add(value);
        }
    }

    /**
     * Validates the value of the enumeration string.  The strings must
     * work for schema, which means avoiding special characters.
     */
    private void  validateStringValue(String value) {
        if (!value.matches(TableImpl.VALID_NAME_CHAR_REGEX)) {
            throw new IllegalArgumentException
                ("Enumeration string names may contain only " +
                 "alphanumeric values plus the character \"_\": " + value);
        }
    }

    /*
     * Used when creating a value of this enum type, to check that the value
     * is one of the allowed ones.
     */
    public void validateValue(String value) {
        for (String val : values) {
            if (val.equals(value)) {
                return;
            }
        }
        throw new IllegalArgumentException
            ("Invalid enumeration value '" + value +
             "', must be in values: " + Arrays.toString(values));
    }

    /**
     * Create the value represented by this index in the declaration
     */
    public EnumValueImpl createEnum(int index) {
        if (!isValidIndex(index)) {
            throw new IllegalArgumentException
                ("Index is out of range for enumeration: " + index);
        }
        return new EnumValueImpl(this, values[index]);
    }

    boolean isValidIndex(int index) {
        return (index < values.length);
    }

    /*
     * NOTE: this code is left here for future support of limited schema
     * evolution to add enumeration values.
     *
     * Enumeration values can only be added, and only at the end of the array.
     * This function ensures that this is done correctly.
    void addValue(String newValue) {
        if (SortableString.encodingLength(values.length + 1) >
            encodingLen) {
            throw new IllegalArgumentException
                ("Cannot add another enumeration value, too large: " +
                 values.length + 1);
        }
        String[] newArray = new String[values.length + 1];
        int i = 0;
        while (i < values.length) {
            newArray[i] = values[i++];
        }
        newArray[i] = newValue;
        values = newArray;
    }
    */
}
