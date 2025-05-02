/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

/**
 * The classes in this package are used to represent and manipulate data in the
 * Oracle NoSQL Database. A table is defined using a
 * fixed schema that describes the data that table will hold. All data classes
 * in this package are instances of {@link
 * oracle.nosql.nson.values.FieldValue}. There is a well-defined mapping
 * between a table row and instances of {@link
 * oracle.nosql.nson.values.FieldValue}.  For example a row is mapped to a
 * {@link oracle.nosql.nson.values.MapValue}, which is a simple map of {@link
 * oracle.nosql.nson.values.FieldValue} instances, keyed by a string.
 * <p>
 * The classes in this package do not have access to the table schema, which
 * means applications must be written knowing the schema. On input the
 * constructed data is validated against the table schema and if it does not
 * conform and exception is thrown.
 * <table>
 *   <caption>The mappings between driver types and database types</caption>
 *   <tr><th>Database Type</th><th>Class</th></tr>
 *   <tr><td>ARRAY</td><td>{@link oracle.nosql.nson.values.ArrayValue}</td></tr>
 *   <tr><td>BINARY</td><td>{@link oracle.nosql.nson.values.BinaryValue}</td></tr>
 *   <tr><td>BOOLEAN</td>
 *       <td>{@link oracle.nosql.nson.values.BooleanValue}</td></tr>
 *   <tr><td>DOUBLE</td>
 *       <td>{@link oracle.nosql.nson.values.DoubleValue}</td></tr>
 *   <tr><td>ENUM</td><td>{@link oracle.nosql.nson.values.StringValue}</td></tr>
 *   <tr><td>FIXED_BINARY</td>
 *       <td>{@link oracle.nosql.nson.values.BinaryValue}</td></tr>
 *   <tr><td>INTEGER</td>
 *       <td>{@link oracle.nosql.nson.values.IntegerValue}</td></tr>
 *   <tr><td>LONG</td><td>{@link oracle.nosql.nson.values.LongValue}</td></tr>
 *   <tr><td>MAP</td><td>{@link oracle.nosql.nson.values.MapValue}</td></tr>
 *   <tr><td>RECORD</td><td>{@link oracle.nosql.nson.values.MapValue}</td></tr>
 *   <tr><td>STRING</td><td>{@link oracle.nosql.nson.values.StringValue}</td></tr>
 *   <tr><td>TIMESTAMP</td>
 *       <td>{@link oracle.nosql.nson.values.TimestampValue}</td></tr>
 *   <tr><td>NUMBER</td><td>{@link oracle.nosql.nson.values.NumberValue}</td></tr>
 * </table>
 * <p>
 * Note that there are several database types that do not have direct
 * equivalents. These are the types that require schema for their definition:
 * <ul>
 * <li>ENUM. Enumerations require a schema. When an application fetches a
 * row with an enumeration its value will be mapped to StringValue. On input,
 * a StringValue must be created to represent an ENUM</li>
 * <li>FIXED_BINARY. This is a specialization of binary that uses a fixed
 * number of bytes. It is mapped to BinaryValue that is validated on input.</li>
 * <li>RECORD. This is a fixed-schema map. In this package it is represented
 * as a MapValue, which is more flexible, but is validated on input.</li>
 * </ul>
 * The database types of MAP and ARRAY are fixed-type in that they contain a
 * single type, for example a MAP of INTEGER.
 * {@link oracle.nosql.nson.values.MapValue} and
 * {@link oracle.nosql.nson.values.ArrayValue} are not
 * fixed-type. On input the types of the elements of these collections must
 * match the schema.
 * <p>
 * <strong>JSON Mappings</strong>
 * <p>ArrayValue are not
 * fixed-type. On input the types of the elements of these collections must
 * match the schema.
 * <p>
 * <strong>JSON Mappings</strong>
 * <p>
 * JSON is commonly used as a format for data and there are also well-defined
 * mappings between JSON types and {@link oracle.nosql.nson.values.FieldValue}
 * instances in this package. It is
 * a common pattern to construct a row (MapValue) from JSON and generate JSON
 * from a row. Methods on these classes make this pattern easy to use.
 * <p>
 * Because the data model supported in this package is richer than JSON it is
 * necessary to define potentially ambiguous mappings as well as provide the
 * ability to affect mappings that are flexible. The following table
 * defines the mappings from JSON types to instances of
 * {@link oracle.nosql.nson.values.FieldValue}.
 * <table>
 *   <caption>Mappings from JSON to driver types</caption>
 *   <tr><th>JSON Type</th><th>Class</th></tr>
 *   <tr><td>ARRAY</td><td>{@link oracle.nosql.nson.values.ArrayValue}</td></tr>
 *   <tr><td>BOOLEAN</td>
 *       <td>{@link oracle.nosql.nson.values.BooleanValue}</td></tr>
 *   <tr><td>NUMBER</td><td>One of the numeric values</td></tr>
 *   <tr><td>OBJECT</td><td>{@link oracle.nosql.nson.values.MapValue}</td></tr>
 *   <tr><td>STRING</td><td>{@link oracle.nosql.nson.values.StringValue}</td></tr>
 * </table>
 * <p>
 * There are several situations that require further descriptions. First, JSON
 * has only a single numeric type. By default JSON numbers will be mapped to
 * the most appropriate numeric type (IntegerValue, LongValue, DoubleValue,
 * NumberValue). For example if the number is a valid Java integer it will be an
 * IntegerValue. If it is a valid Java long it is mapped to LongValue. Floating
 * point numbers are generally mapped to DoubleValue. If the JSON numeric cannot
 * fit in a long or double then it is mapped to NumberValue which an hold any
 * numeric value, but at some additional cost.
 * <p>
 * There is optional configuration that allows applications to control these
 * numeric mappings in the following ways:
 * <ul>
 * <li>Default numeric mappings (best fit) on input</li>
 * <li>Map all numbers into NumberValue on input</li>
 * <li>On output, represent Timestamp as a String</li>
 * <li>On output, represent Timestamp as a long</li>
 * </ul>
 * <p>
 * {@link oracle.nosql.nson.values.FieldValue} instances are
 * not thread-safe and are not intended to be shared.
 */

package oracle.nosql.nson.values;
