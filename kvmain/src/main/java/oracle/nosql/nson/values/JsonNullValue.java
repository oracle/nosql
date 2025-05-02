/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

/**
 * A {@link FieldValue} instance representing an explicit JSON <i>null</i>
 * value in a JSON object or array.
 * On input this type can only be used in a table field of type JSON. This
 * is an immutable singleton object.
 */
public class JsonNullValue extends FieldValue {

    private static final JsonNullValue INSTANCE = new JsonNullValue();

    private JsonNullValue() {
        super();
    }

    @Override
    public Type getType() {
        return Type.JSON_NULL;
    }

    /**
     * Returns an instance (singleton) of JsonNullValue.
     *
     * @return the value
     */
    public static JsonNullValue getInstance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof JsonNullValue) {
            return 0;
        }
        /* TODO: sort empty types? */
        return -1;
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return "null";
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
