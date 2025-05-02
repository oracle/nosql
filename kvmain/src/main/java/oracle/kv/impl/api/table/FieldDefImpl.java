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
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.table.AnyAtomicDef;
import oracle.kv.table.AnyDef;
import oracle.kv.table.AnyJsonAtomicDef;
import oracle.kv.table.AnyRecordDef;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.BinaryDef;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.BooleanDef;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleDef;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.FixedBinaryValue;
import oracle.kv.table.FloatDef;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerDef;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.JsonDef;
import oracle.kv.table.LongDef;
import oracle.kv.table.LongValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.NumberDef;
import oracle.kv.table.NumberValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.StringDef;
import oracle.kv.table.StringValue;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;

import com.fasterxml.jackson.core.JsonParser;

/**
 * Implements FieldDef
 */
public abstract class FieldDefImpl implements FieldDef, Serializable,
                                              FastExternalizable, Cloneable {

    private static final long serialVersionUID = 1L;

    /**
     * Store FieldDefImpl subclass instances in a separate class to avoid
     * static class initialization deadlocks.
     */
    public static class Constants {
        public static final IntegerDefImpl integerDef =  new IntegerDefImpl();
        public static final LongDefImpl longDef =  new LongDefImpl();
        public static final FloatDefImpl floatDef = new FloatDefImpl();
        public static final DoubleDefImpl doubleDef = new DoubleDefImpl();
        public static final StringDefImpl stringDef = new StringDefImpl();
        public static final BooleanDefImpl booleanDef = new BooleanDefImpl();
        public static final BinaryDefImpl binaryDef = new BinaryDefImpl();
        public static final NumberDefImpl numberDef = new NumberDefImpl();

        public static final TimestampDefImpl[] timestampDefs =
            new TimestampDefImpl[TimestampDefImpl.MAX_PRECISION + 1];

        static {
            for (int i = 0; i <= TimestampDefImpl.MAX_PRECISION; ++i) {
                timestampDefs[i] = new TimestampDefImpl(i);
            }
        }

        public static final TimestampDefImpl timestampDef =
            timestampDefs[TimestampDefImpl.MAX_PRECISION];

        public static final AnyDefImpl anyDef = new AnyDefImpl();
        public static final AnyRecordDefImpl anyRecordDef =
            new AnyRecordDefImpl();
        public static final AnyAtomicDefImpl anyAtomicDef =
            new AnyAtomicDefImpl();
        public static final JsonDefImpl jsonDef = new JsonDefImpl();
        public static final AnyJsonAtomicDefImpl anyJsonAtomicDef =
            new AnyJsonAtomicDefImpl();

        public static final MapDefImpl mapAnyDef = new MapDefImpl(anyDef);
        public static final MapDefImpl mapJsonDef = new MapDefImpl(jsonDef);
        public static final ArrayDefImpl arrayAnyDef =
            new ArrayDefImpl(anyDef);
        public static final ArrayDefImpl arrayJsonDef =
            new ArrayDefImpl(jsonDef);

        public static final EmptyDefImpl emptyDef = new EmptyDefImpl();

        public static final StringDefImpl uuidStringDef =
            new StringDefImpl(null, true, false);
        public static final StringDefImpl defaultUuidStrDef =
            new StringDefImpl(null, true, true);

        public static final IntegerDefImpl intMRCounterDef =
            new IntegerDefImpl(true);
        public static final LongDefImpl longMRCounterDef =
            new LongDefImpl(true);
        public static final NumberDefImpl numberMRCounterDef =
            new NumberDefImpl(true);
    }

    /*
     * Immutable properties.
     */
    private final Type type;

    private String description;


    static TimestampDefImpl getTimeDef(int prec) {
        return Constants.timestampDefs[prec];
    }

    /**
     * Convenience constructor.
     */
    FieldDefImpl(Type type) {
        this(type, null);
    }

    FieldDefImpl(Type type, String description) {
        this.type = type;
        this.description = description;
    }

    FieldDefImpl(FieldDefImpl impl) {
        type = impl.type;
        description = impl.description;
    }

    FieldDefImpl() {
        type = null;
        description = null;
    }

    /**
     * Constructor for FastExternalizable. Must be called by subclass's
     * FastExternalizable constructor.
     */
    protected FieldDefImpl(DataInput in, short serialVersion, Type type)
            throws IOException {
        this.type = type;
        description = readString(in, serialVersion);
    }

    /**
     * Reads a FieldDefImp from the input stream.
     */
    static FieldDefImpl readFastExternal(DataInput in, short serialVersion)
            throws IOException {

        final Type type = Type.readFastExternal(in, serialVersion);
        switch (type) {
        case INTEGER:
            return new IntegerDefImpl(in, serialVersion);
        case LONG:
            return new LongDefImpl(in, serialVersion);
        case DOUBLE:
            return new DoubleDefImpl(in, serialVersion);
        case FLOAT:
            return new FloatDefImpl(in, serialVersion);
        case STRING:
            return new StringDefImpl(in, serialVersion);
        case BINARY:
            return new BinaryDefImpl(in, serialVersion);
        case BOOLEAN:
            return new BooleanDefImpl(in, serialVersion);
        case NUMBER:
            return new NumberDefImpl(in, serialVersion);
        case FIXED_BINARY:
            return new FixedBinaryDefImpl(in, serialVersion);
        case ENUM:
            return new EnumDefImpl(in, serialVersion);
        case TIMESTAMP:
            return new TimestampDefImpl(in, serialVersion);
        case RECORD:
            return new RecordDefImpl(in, serialVersion);
        case MAP:
            return new MapDefImpl(in, serialVersion);
        case ARRAY:
            return new ArrayDefImpl(in, serialVersion);
        case ANY:
            return new AnyDefImpl(in, serialVersion);
        case ANY_ATOMIC:
            return new AnyAtomicDefImpl(in, serialVersion);
        case ANY_JSON_ATOMIC:
            return new AnyJsonAtomicDefImpl(in, serialVersion);
        case ANY_RECORD:
            return new AnyRecordDefImpl(in, serialVersion);
        case EMPTY:
            return new EmptyDefImpl(in, serialVersion);
        case JSON:
            return new JsonDefImpl(in, serialVersion);
        /*
         * The following two types do not have associated classes therefore
         * there is nothing to serialize.
         */
        case GEOMETRY:
        case POINT:
            throw new IllegalStateException("Field def type " + type +
                                            " cannot be serialized");
        default:
            throw new IllegalStateException("Unknown field def type: " + type);
        }
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@code #Type}) {@code type}
     * <li> ({@link SerializationUtil#writeString String}) {@code description}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        type.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, description);
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    @Override
    public FieldDefImpl clone() {
        try {
            return (FieldDefImpl) super.clone();
        } catch (CloneNotSupportedException ignore) {
        }
        return null;
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        throw new IllegalStateException(
            "Classes that implement FieldDefImpl must override equals");
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isType(FieldDef.Type type1) {
        return this.type == type1;
    }

    /**
     * Return true if this type can participate in a primary key.
     * Only simple fields can be part of a key.  Boolean type is not
     * allowed in keys (TODO: is there a valid case for this?).
     */
    @Override
    public boolean isValidKeyField() {
        return false;
    }

    @Override
    public boolean isValidIndexField() {
        return false;
    }

    public boolean isEmpty() {
        return type == Type.EMPTY;
    }

    @Override
    public boolean isAny() {
        return type == Type.ANY;
    }

    @Override
    public boolean isAnyRecord() {
        return type == Type.ANY_RECORD;
    }

    @Override
    public boolean isAnyAtomic() {
        return type == Type.ANY_ATOMIC;
    }

    @Override
    public boolean isAnyJsonAtomic() {
        return type == Type.ANY_JSON_ATOMIC;
    }

    public boolean isWildcard() {
        switch (type) {
        case ANY:
        case ANY_RECORD:
        case ANY_ATOMIC:
        case JSON:
        case ANY_JSON_ATOMIC:
            return true;
        default:
            return false;
        }
    }

    @Override
    public boolean isString() {
        return type == Type.STRING;
    }

    /**
     * Returns true if this is a StringDef with isUUID variable set to true,
     * false otherwise.
     */
    @Override
    public boolean isUUIDString() {
        return false;
    }

    @Override
    public boolean isInteger() {
        return type == Type.INTEGER;
    }

    @Override
    public boolean isLong() {
        return type == Type.LONG;
    }

    @Override
    public boolean isDouble() {
        return type == Type.DOUBLE;
    }

    @Override
    public boolean isFloat() {
        return type == Type.FLOAT;
    }

    @Override
    public boolean isNumber() {
        return type == Type.NUMBER;
    }

    @Override
    public boolean isBoolean() {
        return type == Type.BOOLEAN;
    }

    @Override
    public boolean isBinary() {
        return type == Type.BINARY;
    }

    @Override
    public boolean isFixedBinary() {
        return type == Type.FIXED_BINARY;
    }

    @Override
    public boolean isTimestamp() {
        return type == Type.TIMESTAMP;
    }

    @Override
    public boolean isArray() {
        return type == Type.ARRAY;
    }

    @Override
    public boolean isMap() {
        return type == Type.MAP;
    }

    @Override
    public boolean isRecord() {
        return type == Type.RECORD;
    }

    @Override
    public boolean isJson() {
        return type == Type.JSON;
    }

    public boolean mayBeJsonObject() {
        return (type == Type.JSON ||
                type == Type.ANY ||
                (type == Type.MAP && isSubtype(Constants.mapJsonDef)));
    }

    @Override
    public boolean isEnum() {
        return type == Type.ENUM;
    }

    @Override
    public boolean isAtomic() {
        switch (type) {
        case BOOLEAN:
        case NUMBER:
        case DOUBLE:
        case FLOAT:
        case INTEGER:
        case LONG:
        case STRING:
        case BINARY:
        case FIXED_BINARY:
        case ENUM:
        case TIMESTAMP:
        case ANY_JSON_ATOMIC:
        case ANY_ATOMIC:
            return true;
        default:
            return false;
        }
    }

    public boolean isJsonAtomic() {
        switch (type) {
        case BOOLEAN:
        case NUMBER:
        case DOUBLE:
        case FLOAT:
        case INTEGER:
        case LONG:
        case STRING:
        case ANY_JSON_ATOMIC:
            return true;
        default:
            return false;
        }
    }

    @Override
    public boolean isNumeric() {
        switch (type) {
        case DOUBLE:
        case FLOAT:
        case INTEGER:
        case LONG:
        case NUMBER:
            return true;
        default:
            return false;
        }
    }

    @Override
    public boolean isComplex() {
        switch (type) {
        case ANY:
        case JSON:
        case ANY_RECORD:
        case RECORD:
        case ARRAY:
        case MAP:
            return true;
        default:
            return false;
        }
    }

    /*
     * A "precise" type is a type that is fully specified, ie, it is not one of
     * the "any" types and, for complext types, it does not contain any of the
     * "any" types.
     */
    @Override
    public boolean isPrecise() {
        return true;
    }

    @Override
    public AnyDef asAny() {
        throw new ClassCastException
            ("Type is not Any: " + getClass());
    }

    @Override
    public AnyRecordDef asAnyRecord() {
        throw new ClassCastException
            ("Type is not AnyRecord: " + getClass());
    }

    @Override
    public AnyAtomicDef asAnyAtomic() {
        throw new ClassCastException
            ("Type is not AnyAtomic: " + getClass());
    }

    @Override
    public AnyJsonAtomicDef asAnyJsonAtomic() {
        throw new ClassCastException
            ("Type is not AnyAtomic: " + getClass());
    }

    @Override
    public BinaryDef asBinary() {
        throw new ClassCastException
            ("Type is not a Binary: " + getClass());
    }

    @Override
    public FixedBinaryDef asFixedBinary() {
        throw new ClassCastException
            ("Type is not a FixedBinary: " + getClass());
    }

    @Override
    public NumberDef asNumber() {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public BooleanDef asBoolean() {
        throw new ClassCastException
            ("Type is not a Boolean: " + getClass());
    }

    @Override
    public DoubleDef asDouble() {
        throw new ClassCastException
            ("Type is not a Double: " + getClass());
    }

    @Override
    public FloatDef asFloat() {
        throw new ClassCastException
            ("Type is not a Float: " + getClass());
    }

    @Override
    public IntegerDef asInteger() {
        throw new ClassCastException
            ("Type is not an Integer: " + getClass());
    }

    @Override
    public LongDef asLong() {
        throw new ClassCastException
            ("Type is not a Long: " + getClass());
    }

    @Override
    public StringDef asString() {
        throw new ClassCastException
            ("Type is not a String: " + getClass());
    }

    @Override
    public TimestampDef asTimestamp() {
        throw new ClassCastException
            ("Type is not a Timestmap: " + getClass());
    }

    @Override
    public EnumDef asEnum() {
        throw new ClassCastException
            ("Type is not an Enum: " + getClass());
    }

    @Override
    public ArrayDef asArray() {
        throw new ClassCastException
            ("Type is not an Array: " + getClass());
    }

    @Override
    public MapDef asMap() {
        throw new ClassCastException
            ("Type is not a Map: " + getClass());
    }

    @Override
    public RecordDef asRecord() {
        throw new ClassCastException
            ("Type is not a Record: " + getClass());
    }

    @Override
    public JsonDef asJson() {
        throw new ClassCastException
            ("Type is not a JsonDef: " + getClass());
    }

    @Override
    public ArrayValueImpl createArray() {
        throw new IllegalArgumentException
            ("Type cannot create an Array: " + getClass());
    }

    @Override
    public NumberValue createNumber(int value) {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public NumberValue createNumber(long value) {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public NumberValue createNumber(float value) {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public NumberValue createNumber(double value) {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public NumberValue createNumber(BigDecimal value) {
        throw new ClassCastException
            ("Type is not a Number: " + getClass());
    }

    @Override
    public BinaryValue createBinary(byte[] value) {
        throw new IllegalArgumentException
            ("Type cannot create a Binary: " + getClass());
    }

    @Override
    public FixedBinaryValue createFixedBinary(byte[] value) {
        throw new IllegalArgumentException
            ("Type cannot create a FixedBinary: " + getClass());
    }

    @Override
    public BooleanValue createBoolean(boolean value) {
        throw new IllegalArgumentException
            ("Type cannot create a Boolean: " + getClass());
    }

    @Override
    public DoubleValue createDouble(double value) {
        throw new IllegalArgumentException
            ("Type cannot create a Double: " + getClass());
    }

    @Override
    public FloatValue createFloat(float value) {
        throw new IllegalArgumentException
            ("Type cannot create a Float: " + getClass());
    }

    @Override
    public EnumValueImpl createEnum(String value) {
        throw new IllegalArgumentException
            ("Type cannot create an Enum: " + getClass());
    }

    @Override
    public IntegerValue createInteger(int value) {
        throw new IllegalArgumentException
            ("Type cannot create an Integer: " + getClass());
    }

    @Override
    public LongValue createLong(long value) {
        throw new IllegalArgumentException
            ("Type cannot create a Long: " + getClass());
    }

    @Override
    public TimestampValue createTimestamp(Timestamp value) {
        throw new IllegalArgumentException
            ("Type cannot create a Timestamp: " + getClass());
    }

    @Override
    public MapValueImpl createMap() {
        throw new IllegalArgumentException
            ("Type cannot create a Map: " + getClass());
    }

    @Override
    public RecordValueImpl createRecord() {
        throw new IllegalArgumentException
            ("Type cannot create a Record: " + getClass());
    }

    @Override
    public StringValue createString(String value) {
        throw new IllegalArgumentException
            ("Type cannot create a String: " + getClass());
    }

    @Override
    public FieldValue createJsonNull() {
        throw new IllegalArgumentException
            ("Type cannot create a JSON null value: " + getClass());
    }

    /*
     * Common utility to compare objects for equals() overrides.  It handles
     * the fact that one or both objects may be null.
     */
    boolean compare(Object o, Object other) {
        if (o != null) {
            return o.equals(other);
        }
        return (other == null);
    }

    public void setDescription(String descr) {
        description = descr;
    }

    /**
     * An internal interface for those fields which have a special encoding
     * length.  By default an invalid value is returned.  This is mostly useful
     * for testing.  It is only used by Integer and Long.
     */
    int getEncodingLength() {
        return -1;
    }

    public boolean hasMin() {
        return false;
    }

    public boolean hasMax() {
        return false;
    }

    /**
     * Returns the total number of type definitions defined for this field,
     * including the field itself. This defaults to 1 for atomic fields. This
     * method is overridden by maps, arrays, and records.
     */
    int countTypes() {
        return 1;
    }

    /**
     * Return whether this is a subtype of a given type.
     */
    public abstract boolean isSubtype(FieldDefImpl superType);

    /**
     * Get the union of this type and the given other type.
     */
    public FieldDefImpl getUnionType(FieldDefImpl other) {

        assert(!isEmpty() && !other.isEmpty());

        if (isSubtype(other)) {
            return other;
        }

        if (other.isSubtype(this)) {
            return this;
        }

        Type t1 = getType();
        Type t2 = other.getType();

        if (t1 == t2) {

            if (t1 == Type.RECORD || t1 == Type.ANY_RECORD) {
                return Constants.anyRecordDef;
            }

            if (t1 == Type.ARRAY) {
                ArrayDefImpl def1 = (ArrayDefImpl)this;
                ArrayDefImpl def2 = (ArrayDefImpl)other;
                FieldDefImpl edef1 = def1.getElement();
                FieldDefImpl edef2 = def2.getElement();

                FieldDefImpl eunion = edef1.getUnionType(edef2);

                return FieldDefFactory.createArrayDef(eunion);
            }

            if (t1 == Type.MAP) {
                MapDefImpl def1 = (MapDefImpl)this;
                MapDefImpl def2 = (MapDefImpl)other;
                FieldDefImpl edef1 = def1.getElement();
                FieldDefImpl edef2 = def2.getElement();

                FieldDefImpl eunion = edef1.getUnionType(edef2);

                return FieldDefFactory.createMapDef(eunion);
            }
        }

        if (isJsonAtomic() && other.isJsonAtomic()) {
            return Constants.anyJsonAtomicDef;
        }

        if (isSubtype(Constants.jsonDef) &&
            other.isSubtype(Constants.jsonDef)) {
            return Constants.jsonDef;
        }

        if (isAtomic() && other.isAtomic()) {
            return Constants.anyAtomicDef;
        }

        return Constants.anyDef;
    }

    /**
     * Returns the FieldDefImpl associated with the names in the iterator.
     *
     * This is used to parse dot notation for navigating fields within complex
     * field types such as Record.  Simple types don't support navigation so the
     * default implementation returns null.  This is used primarily when
     * locating field definitions associated with index fields.
     */
    FieldDefImpl findField(TablePath path, int pos) {

        assert(pos < path.numSteps());

        switch (getType()) {
        case RECORD: {
            RecordDefImpl rec = (RecordDefImpl)this;

            FieldDefImpl def = rec.getFieldMap().getFieldDef(path.getStep(pos++));
            if (def == null || pos >= path.numSteps()) {
                return def;
            }
            return def.findField(path, pos);
        }
        case MAP: {
            /*
             * The caller will have consumed all steps of the path leading
             * up to the MapDefImpl. The remaining components reference one
             * of several things. 1-3 are used by the R3.2 multi-key map
             * indexes. 4-5 are used by the original single-key map indexes
             * in 3.1.
             *  1. <i>path-to-here</i>.keys().  References the map's key, so
             *     it's a string.
             *  2. <i>path-to-here</i>.values().  References the map's element
             *     (value), so the element is returned.
             *  3. <i>path-to-here</i>.values().foo.  References "foo" within
             *     the map's element (value). This needs to be resolved by the
             *     element, so this case calls the findField() on the element.
             *  4. <i>path-to_here</i>.indexedKey.  References the element as
             *     well. This is used by R3.1-style map indexes, which index
             *     the value of a specific key, and the "indexedKey" is the key
             *     that's indexed. It does not match any actual metadata.
             *  5. <i>path-to-here</i>.indexedKey.moreStuff.  Similar to (3) but
             *     because in this path there is an index on the "indexedKey"
             *     map entry, the current field is just consumed (as is [],
             *     above) and the rest of the path is resolved by the element.
             */
            MapDefImpl map = (MapDefImpl)this;

            String step = path.getStep(pos++);

            /*
             * If the field is <path-to-map>.keys(), it's a string
             */
            if (TableImpl.KEYS.equalsIgnoreCase(step)) {
                if (pos < path.numSteps()) {
                    throw new IllegalArgumentException(
                        TableImpl.KEYS +
                        " must be the final component of the field");
                }
                return (FieldDefImpl)map.getKeyDefinition();
            }

            /*
             * If there are not further components the currentField is either
             * ".values()" or the value of an indexed key (single-key map index).
             * In both cases the target of the operation is the map's element
             * so return it.
             */
            if (pos >= path.numSteps()) {
                return map.getElement();
            }

            /*
             * There are more components, call the element to resolve them.
             */
            return map.getElement().findField(path, pos);
        }
        case ARRAY: {
            /*
             * Arrays are odd in that they have no field names, so when this
             * function is called its own name has already been consumed by
             * the caller so the name is passed directly to the element.
             *
             * If, somehow, this is called when the element is a simple type
             * it's findField method will return null, which is handled by
             * the caller.
             *
             * Examples:
             * arrayField.a -- address the "a" field of the array's element,
             * which must be a map or record
             * arrayField.a.b address the "b" field of the field contained in
             * the "a" field of the array's element.
             */
            ArrayDefImpl arr = (ArrayDefImpl)this;

            /*
             * Peek at the current component.  If it is [], consume it,
             * and keep going. This allows operations to target the element
             * itself vs a field contained in the element.
             */
            String step = path.getStep(pos++);

            if (TableImpl.BRACKETS.equals(step)) {
                if (pos >= path.numSteps()) {
                    return arr.getElement();
                }
            } else {
                /* restore the state for the element to use */
                --pos;
            }

            /*
             * Do not consume the current name, and pass everything to the
             * element.
             */
            return arr.getElement().findField(path, pos);
        }
        default:
            return null;
        }
    }

    /**
     * Returns the FieldDef associated with the single field name.  By default
     * this is null, for simple types.  Complex types override this to
     * potentially return non-null FieldDef instances.
     */
    @SuppressWarnings("unused")
    FieldDefImpl findField(String fieldName) {
        return null;
    }

    public String toJsonString() {
        /* default is pretty print */
        return toJsonString(true);
    }

    public String toJsonString(boolean pretty) {

        TableImpl.JsonFormatter handler = TableImpl.createJsonFormatter(pretty);
        TableImpl.walkFieldDefInfo(this, handler);
        return handler.toString();
    }

    public String getFieldName() {
        return null;
    }

    public String getDDLString() {
        StringBuilder sb = new StringBuilder();
        display(sb, new DisplayFormatter());
        return sb.toString();
    }

    /*
     * Another, non-AVRO, way to display a type. It uses the DDL format.
     * It is used for displaying types in query plans and in error messages.
     */
    public void display(StringBuilder sb, DisplayFormatter formatter) {
        switch (getType()) {
        case ANY:
            sb.append("Any");
            break;
        case ANY_ATOMIC:
            sb.append("AnyAtomic");
            break;
        case ANY_RECORD:
            sb.append("AnyRecord");
            break;
        case ANY_JSON_ATOMIC:
            sb.append("AnyJsonAtomic");
            break;
        case JSON:
            sb.append("Json");
            if (hasJsonMRCounter()) {
                JsonDefImpl jdef = (JsonDefImpl)this;
                Map<String, Type> paths = jdef.allMRCounterFieldsInternal();
                int i = 0;
                sb.append("(");
                for (Map.Entry<String, Type> entry : paths.entrySet()) {
                    if (i > 0) {
                        /* add separator if not first entry */
                        sb.append(", ");
                    }
                    sb.append(entry.getKey());
                    sb.append(" AS ");
                    sb.append(entry.getValue());
                    sb.append(" MR_Counter");
                    ++i;
                }
                sb.append(")");
            }
            break;
        case EMPTY:
            sb.append("Empty");
            break;
        case GEOMETRY:
            sb.append("Geometry");
            break;
        case POINT:
            sb.append("Point");
            break;
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
        case BOOLEAN:
        case BINARY: {
            String typeName = getType().toString();
            typeName = (typeName.substring(0, 1).toUpperCase() +
                        typeName.substring(1).toLowerCase());
            sb.append(typeName);

            if (isUUIDString()) {
                sb.append("(UUID");
                if (((StringDefImpl)this).isGenerated()) {
                    sb.append(", GENERATED");
                }
                sb.append(")");
            }

            if (isMRCounter()) {
                sb.append(" AS MR_Counter");
            }
            break;
        }
        case ENUM: {
            sb.append("Enum(");
            String[] values = asEnum().getValues();
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append('"');
                sb.append(values[i]);
                sb.append('"');
            }
            sb.append(")");
            break;
        }
        case TIMESTAMP: {
            sb.append("Timestamp(");
            sb.append(asTimestamp().getPrecision());
            sb.append(")");
            break;
        }
        case FIXED_BINARY: {
            sb.append("Binary(");
            sb.append(asFixedBinary().getSize());
            sb.append(")");
            break;
        }
        case ARRAY: {
            ArrayDefImpl def = (ArrayDefImpl)this;
            FieldDefImpl edef = def.getElement();
            sb.append("Array(");
            if (!edef.isRecord()) {
                edef.display(sb, formatter);
            } else {
                formatter.incIndent();
                sb.append("\n");
                formatter.indent(sb);
                edef.display(sb, formatter);
                formatter.decIndent();
                formatter.indent(sb);
            }
            sb.append(")");
            break;
        }
        case MAP: {
            MapDefImpl def = (MapDefImpl)this;
            FieldDefImpl edef = def.getElement();
            sb.append("Map(");
            if (!edef.isRecord()) {
                edef.display(sb, formatter);
            } else {
                formatter.incIndent();
                sb.append("\n");
                formatter.indent(sb);
                edef.display(sb, formatter);
                formatter.decIndent();
                formatter.indent(sb);
            }
            sb.append(")");
            break;
        }
        case RECORD: {
            RecordDefImpl def = (RecordDefImpl)this;
            sb.append("RECORD(\n");
            formatter.incIndent();
            int numFields = def.getNumFields();

            for (int i = 0; i < numFields; ++i) {
                formatter.indent(sb);
                sb.append(def.getFieldName(i)).append(" : ");

                FieldDefImpl fdef = def.getFieldDef(i);
                if (!fdef.isRecord()) {
                    fdef.display(sb, formatter);
                } else {
                    sb.append("\n");
                    formatter.incIndent();
                    formatter.indent(sb);
                    fdef.display(sb, formatter);
                    formatter.decIndent();
                }

                if (i < numFields - 1) {
                    sb.append(",\n");
                }
            }

            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append(")");
            break;
        }
        case JSON_INT_MRCOUNTER:
        case JSON_LONG_MRCOUNTER:
        case JSON_NUM_MRCOUNTER:
            throw new IllegalArgumentException("Unexpected type: " +
                getType());
        default:
            break;
        }
    }

    public void displayAsJson(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        switch (getType()) {
        case ANY:
            sb.append("\"Any\"");
            break;
        case ANY_ATOMIC:
            sb.append("\"AnyAtomic\"");
            break;
        case ANY_RECORD:
            sb.append("\"AnyRecord\"");
            break;
        case ANY_JSON_ATOMIC:
            sb.append("\"AnyJsonAtomic\"");
            break;
        case JSON:
            sb.append("\"Json\"");
            break;
        case EMPTY:
            sb.append("\"Empty\"");
            break;
        case GEOMETRY:
            sb.append("\"Geometry\"");
            break;
        case POINT:
            sb.append("\"Point\"");
            break;
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
        case BOOLEAN:
        case BINARY: {
            String typeName = getType().toString();
            typeName = (typeName.substring(0, 1).toUpperCase() +
                        typeName.substring(1).toLowerCase());
            sb.append("\"").append(typeName).append("\"");
            break;
        }
        case ENUM: {
            sb.append("\"Enum(");
            String[] values = asEnum().getValues();
            for (int i = 0; i < values.length; i++) {
                sb.append(values[i]);
                if (i < values.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")\"");
            break;
        }
        case TIMESTAMP: {
            sb.append("\"Timestamp(");
            sb.append(asTimestamp().getPrecision());
            sb.append(")\"");
            break;
        }
        case FIXED_BINARY: {
            sb.append("\"Binary(");
            sb.append(asFixedBinary().getSize());
            sb.append(")\"");
            break;
        }
        case ARRAY: {
            ArrayDefImpl def = (ArrayDefImpl)this;
            FieldDefImpl edef = def.getElement();
            sb.append("{ \"Array\" : ");
            if (edef.isAtomic()) {
                edef.displayAsJson(sb, formatter, verbose);
                sb.append(" }");
            } else {
                sb.append("\n");
                formatter.incIndent();
                formatter.indent(sb);
                edef.displayAsJson(sb, formatter, verbose);
                formatter.decIndent();
                sb.append("\n");
                formatter.indent(sb);
                sb.append("}");
            }
            break;
        }
        case MAP: {
            MapDefImpl def = (MapDefImpl)this;
            FieldDefImpl edef = def.getElement();
            sb.append("{ \"Map\" : ");
            if (edef.isAtomic()) {
                edef.displayAsJson(sb, formatter, verbose);
                sb.append(" }");
            } else {
                sb.append("\n");
                formatter.incIndent();
                formatter.indent(sb);
                edef.displayAsJson(sb, formatter, verbose);
                formatter.decIndent();
                sb.append("\n");
                formatter.indent(sb);
                sb.append("}");
            }
            break;
        }
        case RECORD: {
            RecordDefImpl def = (RecordDefImpl)this;
            int numFields = def.getNumFields();

            sb.append("{ \"Record\" : {\n");

            formatter.incIndent();
            formatter.incIndent();

            for (int i = 0; i < numFields; ++i) {
                formatter.indent(sb);
                sb.append("\"").append(def.getFieldName(i)).append("\" : ");

                FieldDefImpl fdef = def.getFieldDef(i);
                if (fdef.isAtomic()) {
                    fdef.displayAsJson(sb, formatter, verbose);
                } else {
                    sb.append("\n");
                    formatter.incIndent();
                    formatter.indent(sb);
                    fdef.displayAsJson(sb, formatter, verbose);
                    formatter.decIndent();
                }

                if (i < numFields - 1) {
                    sb.append(",\n");
                }
            }

            formatter.decIndent();
            sb.append("\n");
            formatter.indent(sb);
            sb.append("}");

            formatter.decIndent();
            sb.append("\n");
            formatter.indent(sb);
            sb.append("}");
            break;
        }
        case JSON_INT_MRCOUNTER:
        case JSON_LONG_MRCOUNTER:
        case JSON_NUM_MRCOUNTER:
            throw new IllegalArgumentException("Unexpected type: " +
                getType());
        default:
            break;
        }
    }

    /*
     * The following 4 methods are used to construct DM values out of strings
     * that are the serialized version of primary key values (see
     * createAtomicFromKey() below).
     */
    @SuppressWarnings("unused")
    IntegerValueImpl createInteger(String value) {
        throw new IllegalArgumentException("Type cannot create an Integer: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    LongValueImpl createLong(String value) {
        throw new IllegalArgumentException("Type cannot create a Long: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    DoubleValueImpl createDouble(String value) {
        throw new IllegalArgumentException("Type cannot create a Double: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    FloatValueImpl createFloat(String value) {
        throw new IllegalArgumentException("Type cannot create a Float: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    NumberValueImpl createNumber(byte[] value) {
        throw new IllegalArgumentException("Type cannot create a Number: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    NumberValueImpl createNumber(String value) {
        throw new IllegalArgumentException("Type cannot create a Number: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    NumberValueImpl createNumberFromIndexField(String value) {
        throw new IllegalArgumentException("Type cannot create a Number: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    BooleanValueImpl createBoolean(String value) {
        throw new IllegalArgumentException("Type cannot create a Boolean: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    TimestampValueImpl createTimestamp(byte[] value) {
        throw new IllegalArgumentException("Type cannot create a Timestamp: " +
                                           getClass());
    }

    @SuppressWarnings("unused")
    public TimestampValueImpl createTimestamp(String value) {
        throw new IllegalArgumentException("Type cannot create a Timestamp: " +
                                           getClass());
    }

    /**
     * Creates an instance of counter CRDT value.
     * @return an instance of counter CRDT value.
     */
    public FieldValueImpl createCRDTValue() {
        throw new IllegalArgumentException(
            "Type cannot create an MR counter : " + getClass());
    }

    /**
     * Create FieldValue instances from String formats for primary keys.
     */
    static FieldValueImpl createValueFromKeyString(
        String value,
        FieldDefImpl type) {

        switch (type.getType()) {
        case INTEGER:
            return type.createInteger(value);
        case LONG:
            return type.createLong(value);
        case STRING:
            if (type.isUUIDString()) {
                return StringValueImpl.createUUIDFromKey(value);
            }
            return (FieldValueImpl)type.createString(value);
        case DOUBLE:
            return type.createDouble(value);
        case FLOAT:
            return type.createFloat(value);
        case NUMBER:
            return type.createNumberFromIndexField(value);
        case ENUM:
            return EnumValueImpl.createFromKey((EnumDefImpl)type, value);
        case BOOLEAN:
            return type.createBoolean(value);
        case TIMESTAMP:
            return type.createTimestamp(value);
        default:
            throw new IllegalCommandException("Type is not allowed in a key: " +
                                              type.getType());
        }
    }

    /**
     * Create FieldValue instances from Strings that are stored "naturally"
     * for the data type. This is opposed to the String encoding used for
     * key components.
     */
    public static FieldValue createValueFromString(String value,
                                                   final FieldDef def) {

        final InputStream jsonInput;

        switch (def.getType()) {
        case INTEGER:
            return def.createInteger(Integer.parseInt(value));
        case LONG:
            return def.createLong(Long.parseLong(value));
        case STRING:
            return def.createString(value);
        case DOUBLE:
            return def.createDouble(Double.parseDouble(value));
        case FLOAT:
            return def.createFloat(Float.parseFloat(value));
        case NUMBER:
            return def.createNumber(new BigDecimal(value));
        case BOOLEAN:
            /*
             * Boolean.parseBoolean simply does a case-insensitive comparison
             * to "true" and assigns that value. This means any other string
             * results in false.
             */
            return def.createBoolean(Boolean.parseBoolean(value));
        case ENUM:
            return def.createEnum(value);
        case BINARY:
            return ((BinaryDefImpl)def).fromString(value);
        case FIXED_BINARY:
            return ((FixedBinaryDefImpl)def).fromString(value);
        case TIMESTAMP:
            return ((TimestampDefImpl)def).fromString(value);
        case RECORD:
            final RecordValueImpl recordValue = (RecordValueImpl)def.createRecord();
            jsonInput =  new ByteArrayInputStream(value.getBytes());
            ComplexValueImpl.createFromJson(recordValue, jsonInput, false);
            return recordValue;
        case ARRAY:
            final ArrayValueImpl arrayValue = (ArrayValueImpl)def.createArray();
            jsonInput =  new ByteArrayInputStream(value.getBytes());
            ComplexValueImpl.createFromJson(arrayValue, jsonInput, false);
            return arrayValue;
        case MAP:
            final MapValueImpl mapValue = (MapValueImpl)def.createMap();
            jsonInput =  new ByteArrayInputStream(value.getBytes());
            ComplexValueImpl.createFromJson(mapValue, jsonInput, false);
            return mapValue;
        case JSON:
        case ANY_JSON_ATOMIC:
            jsonInput =  new ByteArrayInputStream(value.getBytes());
            try {
                JsonParser jp = TableJsonUtils.createJsonParser(jsonInput);
                return JsonDefImpl.createFromJson(jp, true);
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Failed to parse JSON input: " + ioe.getMessage());
            }
        case ANY:
            FieldDef[] types = new FieldDef[] {
                Constants.jsonDef, Constants.timestampDef, Constants.binaryDef
            };
            for (FieldDef type : types) {
                try {
                    return createValueFromString(value, type);
                } catch (Exception e) {
                }
            }

            throw new IllegalArgumentException(
                "Failed to parse string to any of the available types");
        default:
            throw new IllegalArgumentException(
                "Type not yet implemented: " + def.getType());
        }
    }

    /**
     * Creates a FieldValue based on the type and this FieldDef.
     * Only atomic types are supported.  This is called from IndexKey
     * deserialization when dealing with putting values into sparsely
     * populated nested types.  Type abstraction is handled here rather
     * than creating per-type overloads.
     *
     * Type is passed explicitly because the type of this instance may be JSON
     * but the type needed for construction needs to be the actual scalar data
     * type.
     */
    FieldValue createValue(FieldDef.Type otype, Object value) {

        switch (otype) {
        case INTEGER:
            return createInteger((Integer) value);
        case STRING:
            return createString((String) value);
        case LONG:
            return createLong((Long) value);
        case DOUBLE:
            return createDouble((Double) value);
        case FLOAT:
            return createFloat((Float) value);
        case NUMBER:
            return createNumber((byte[]) value);
        case BOOLEAN:
            return createBoolean((Boolean) value);
        case ENUM:
            return ((EnumDefImpl) this).createEnum((Integer) value);
        case TIMESTAMP:
            if (value instanceof String) {
                return createTimestamp((String) value);
            }
            return createTimestamp((byte[]) value);
        default:
            throw new IllegalStateException
                ("Type not supported by createValue: " + otype);
        }
    }

    /*
     * For testing.
     */
    FieldValue createValue(Object value) {
        return createValue(getType(), value);
    }

    /**
     * Returns the minimum version of the server that can execute this
     * feature. Features that require a higher minimum version should
     * override this method.
     */
    public short getRequiredSerialVersion() {
        return SerialVersion.MINIMUM;
    }

    /**
     * @return the definition of the object that represents the count for a
     * region in the CRDT.
     */
    public FieldDefImpl getCRDTElement() {
        throw new IllegalArgumentException(
            "Type cannot create a CRDT: " + getClass());
    }

    public static FieldDefImpl getCRDTDef(Type type) {
        if (type == Type.INTEGER) {
            return Constants.intMRCounterDef;
        } else if (type == Type.LONG) {
            return Constants.longMRCounterDef;
        } else if (type == Type.NUMBER) {
            return Constants.numberMRCounterDef;
        } else {
            throw new IllegalArgumentException("Cannot create a def with " +
                "CRDT for type: " + type);
        }
    }

    public boolean hasJsonMRCounter() {
        return false;
    }

    /**
     * @return the json counter type used for json seriliazation in
     * TableImpl.serializeJson().
     */
    public Type getJsonCounterType() {
        throw new IllegalArgumentException("Type is not valid " +
            "JSON MR_Counter type.");

    }

}
