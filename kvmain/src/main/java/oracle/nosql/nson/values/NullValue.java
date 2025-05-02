/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

/**
 * A {@link FieldValue} instance representing a <i>null</i> or missing value
 * in a fully-typed schema. This type only exists in index keys on a fully-typed
 * field and never inside indexed JSON.
 */
public class NullValue extends FieldValue {

    private static final NullValue INSTANCE = new NullValue();

    private NullValue() {
        super();
    }

    @Override
    public Type getType() {
        return Type.NULL;
    }

    /**
     * Returns an instance (singleton) of NullValue.
     *
     * @return the value
     */
    public static NullValue getInstance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof NullValue) {
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
