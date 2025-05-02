/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

/**
 * @hidden
 * Only used by hidden scan interface
 *
 * A {@link FieldValue} instance representing an empty value. This type
 * is only relevant inside a table field of type JSON and only when that
 * field is indexed. It is used in index keys to represent
 * a <em>missing</em> value for the indexed field. This is different from a
 * {@link JsonNullValue} value, which is a concrete value. It is also different
 * from NullValue which represents a null/missing field in a fully-typed
 * field in the table schema as opposed to a JSON field
 */
public class EmptyValue extends FieldValue {

    private static final EmptyValue INSTANCE = new EmptyValue();

    private EmptyValue() {
        super();
    }

    @Override
    public Type getType() {
        return Type.EMPTY;
    }

    /**
     * Returns an instance (singleton) of EmptyValue.
     *
     * @return the value
     */
    public static EmptyValue getInstance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof EmptyValue) {
            return 0;
        }
        /* TODO: sort empty types? */
        return -1;
    }

    @Override
    public String toJson(JsonOptions options) {
        return "\"EMPTY\"";
    }

    @Override
    public boolean equals(Object other) {
        return other == INSTANCE;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return 0;
    }
}
