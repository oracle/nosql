/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

/**
 * JsonOptions allows applications to configure how JSON is parsed on input
 * and generated on output. An instance of this class can be associated with
 * methods that produce and consume JSON to control mappings between JSON and
 * the types in the Oracle NoSQL Database.
 * <p>
 * All of the fields in this object are boolean and therefore default to
 * <em>false</em>.
 */
/*
 * NOTE: See https://github.com/FasterXML/jackson-core/wiki/JsonParser-Features
 * for more possible options.
 */
public class JsonOptions {
    private boolean numericAsNumber;
    private boolean timestampAsString;
    private boolean timestampAsLong;
    private boolean prettyPrint;
    private boolean nonNumericNumbers;
    private boolean allowComments;
    private boolean allowSingleQuotes;
    private boolean maintainInsertionOrder;

    /**
     * Static {@link JsonOptions} instance with default values for all but
     * the pretty-print option, which is set to true.
     */
    public static final JsonOptions PRETTY =
        new JsonOptions().setPrettyPrint(true);

    /**
     * Tells the JSON parser on input to represent all numeric values as
     * {@link NumberValue}. This can consume extra space but ensures that there
     * is no loss of precision. This option has no effect when representing
     * {@link FieldValue} instances as JSON on output.
     * <p>
     * If the resulting object is later inserted into a table it must still
     * match the schema for the table. If the value cannot be cast to the
     * expected type without loss of precision an exception will be thrown
     * on insert.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setNumericAsNumber(boolean value) {
        numericAsNumber = value;
        return this;
    }

    /**
     * Tells the system to represent {@link TimestampValue} instances in a
     * string representation when exported as JSON. This option has no effect
     * on JSON when used as input. The string format on output conforms to
     * ISO 8601.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setTimestampAsString(boolean value) {
        timestampAsString = value;
        return this;
    }

    /**
     * Tells the system to represent {@link TimestampValue} instances in a
     * long representation when exported as JSON. This option has no effect
     * on JSON when used as input. The format is the number of milliseconds
     * since the Epoch, 1970-01-01T00:00:00.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setTimestampAsLong(boolean value) {
        timestampAsLong = value;
        return this;
    }

    /**
     * Tells the system to pretty print JSON on output.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setPrettyPrint(boolean value) {
        prettyPrint = value;
        return this;
    }

    /**
     * Tells the system to allow non-numeric values as numbers, such as NaN,
     * on JSON input.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setAllowNonNumericNumbers(boolean value) {
        nonNumericNumbers = value;
        return this;
    }

    /**
     * Tells the system to allow C/Java style comments embedded in JSON of the
     * format \/* comment *\/ on input. Comments will not be maintained by
     * the system and are stripped on input.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setAllowComments(boolean value) {
        allowComments = value;
        return this;
    }

    /**
     * Tells the system to allow single quotes instead of double quotes in JSON
     * on input.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setAllowSingleQuotes(boolean value) {
        allowSingleQuotes = value;
        return this;
    }

    /**
     * Tells the system to use a Map that maintains insertion order when
     * parsing JSON.
     *
     * @param value true or false
     *
     * @return this
     */
    public JsonOptions setMaintainInsertionOrder(boolean value) {
        maintainInsertionOrder = value;
        return this;
    }

    /* getters */

    /**
     * Returns whether numeric values will be mapped to {@link NumberValue}
     * when parsed from JSON input.
     *
     * @return true if numerics will be mapped
     */
    public boolean getNumericAsNumber() {
        return numericAsNumber;
    }

    /**
     * Returns whether {@link TimestampValue} instances will be mapped to
     * a string representation when output as JSON.
     *
     * @return true if a string format is used
     */
    public boolean getTimestampAsString() {
        return timestampAsString;
    }

    /**
     * Returns whether {@link TimestampValue} instances will be mapped to
     * a long representation when output as JSON.
     *
     * @return true if a long format is used
     */
    public boolean getTimestampAsLong() {
        return timestampAsLong;
    }

    /**
     * Returns whether JSON will be pretty-printed on output.
     *
     * @return true if pretty printing is set
     */
    public boolean getPrettyPrint() {
        return prettyPrint;
    }

    /**
     * Returns whether non-numeric numbers, such as NaN or INF are allowed
     * on input.
     *
     * @return true if non-numeric numbers are allowed.
     */
    public boolean getAllowNonNumericNumbers() {
        return nonNumericNumbers;
    }

    /**
     * Returns whether comments embedded in JSON are allowed on input.
     *
     * @return true if comments are allowed.
     */
    public boolean getAllowComments() {
        return allowComments;
    }

    /**
     * Returns whether single quotes are allowed in JSON on input.
     *
     * @return true if single quotes are allowed.
     */
    public boolean getAllowSingleQuotes() {
        return allowSingleQuotes;
    }

    /**
     * Returns whether parsed JSON will use a Map that maintains insertion
     * order.
     *
     * @return true if maintaining insertion order
     */
    public boolean getMaintainInsertionOrder() {
        return maintainInsertionOrder;
    }
}
