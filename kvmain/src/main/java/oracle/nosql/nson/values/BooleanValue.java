/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

/**
 * A {@link FieldValue} instance representing a boolean value. Boolean values
 * are singleton final instances and never constructed.
 */
public abstract class BooleanValue extends FieldValue {

    private static final BooleanValue TRUE = new TrueBooleanValue();
    private static final BooleanValue FALSE = new FalseBooleanValue();

    /**
     * Returns a singleton instance representing a true BooleanValue
     *
     * @return the value
     */
    public static BooleanValue trueInstance() {
        return TRUE;
    }

    /**
     * Returns a singleton instance representing a false BooleanValue
     *
     * @return the value
     */
    public static BooleanValue falseInstance() {
        return FALSE;
    }

    /**
     * Returns a singleton instance representing the boolean value provided.
     *
     * @param value the value
     *
     * @return the value
     */
    public static BooleanValue getInstance(boolean value) {
        return (value ? TRUE : FALSE);
    }

    /**
     * Returns the boolean value of the field
     *
     * @return the value
     */
    abstract public boolean getValue();

    private BooleanValue() {
        super();
    }

    @Override
    public String getString() {
        return toJson();
    }

    @Override
    public Type getType() {
        return Type.BOOLEAN;
    }

    private static class TrueBooleanValue extends BooleanValue {
        @Override
        public boolean getValue() {
            return true;
        }

        @Override
        public String toJson(JsonOptions options) {
            return "true";
        }
    }

    private static class FalseBooleanValue extends BooleanValue {
        @Override
        public boolean getValue() {
            return false;
        }

        @Override
        public String toJson(JsonOptions options) {
            return "false";
        }
    }

    @Override
    public int compareTo(FieldValue other) {
        return ((Boolean)getValue()).compareTo(other.getBoolean());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BooleanValue) {
            return getValue() == ((BooleanValue)other).getValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Boolean) getValue()).hashCode();
    }

    @Override
    public long sizeof() {
        return 0;
    }
}
