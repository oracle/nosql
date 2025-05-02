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

package oracle.kv.table;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * ArrayValue extends {@link FieldValue} to add methods appropriate for array
 * values.
 *
 * @since 3.0
 */
public interface ArrayValue extends FieldValue {

    /**
     * @return a deep copy of this object.
     */
    @Override
    public ArrayValue clone();

    /**
     * Returns a String representation of the value.  The value is returned
     * is a JSON string, and is the same as that returned by
     * {@link FieldValue#toJsonString}.
     *
     * @return a String representation of the value
     */
    @Override
    public String toString();

    /**
     * Returns the ArrayDef that defines the content of this array.
     *
     * @return an ArrayDef
     */
    @Override
    ArrayDef getDefinition();

    /**
     * Gets the value at the specified index.
     *
     * @param index the index to use for the get
     *
     * @return the value at the index or null if none exists
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     */
    FieldValue get(int index);

    /**
     * Returns the size of the array.
     *
     * @return the size of the array
     */
    int size();

    /**
     * Returns the array values as an unmodifiable list.
     *
     * @return the list of values
     */
    List<FieldValue> toList();

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to add
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(FieldValue value);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, FieldValue value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, FieldValue value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(int value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(int[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, int value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, int value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(long value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(long[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, long value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, long value);

    /**
     * Adds a new value at the end of the array.  This method is used to add
     * a string into an array of type String. The String value is not parsed or
     * interpreted. The methods {@link #addJson} and {@link #addEnum} exist to
     * add String values of those types.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(String value);

    /**
     * Adds an array of new values at the end of the array.  This method is
     * used to add an array of strings into an array of type String. The String
     * values are not parsed or interpreted. The methods {@link #addJson} and
     * {@link #addEnum} exist to add String values of those types.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(String[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right. This method is used to add a string into an array of type
     * String. The String value is not parsed or interpreted. The methods
     * {@link #setJson} and {@link #setEnum} exist to add String values of
     * those types.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, String value);

    /**
     * Set the value at the specified index. This method replaces any existing
     * value at that index. This method is used to set a string into an array of
     * type String. The String value is not parsed or interpreted. The methods
     * {@link #setJson} and {@link #setEnum} exist to add String values of
     * those types.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, String value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     *
     * @return this
     */
    ArrayValue add(double value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(double[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, double value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, double value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(float value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(float[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, float value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, float value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int index, int value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue setNumber(int index, int value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(long value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(long[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int index, long value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue setNumber(int index, long value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(float value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(float[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int index, float value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue setNumber(int index, float value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(double value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(double[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int index, double value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue setNumber(int index, double value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(BigDecimal value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(BigDecimal[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue addNumber(int index, BigDecimal value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.4
     */
    ArrayValue setNumber(int index, BigDecimal value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(boolean value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(boolean[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, boolean value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, boolean value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(byte[] value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(byte[][] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue add(int index, byte[] value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue set(int index, byte[] value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addFixed(byte[] value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addFixed(byte[][] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue addFixed(int index, byte[] value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue setFixed(int index, byte[] value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addEnum(String value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addEnum(String[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue addEnum(int index, String value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     */
    ArrayValue setEnum(int index, String value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue add(Timestamp value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue add(Timestamp[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue add(int index, Timestamp value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue set(int index, Timestamp value);

    /**
     * Sets the value at the specified index with an empty RecordValue,
     * replacing any existing value at that index. The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    RecordValue setRecord(int index);

    /**
     * Adds a new RecordValue to the end of the array.  The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return an empty RecordValue
     */
    RecordValue addRecord();

    /**
     * Inserts a new RecordValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right.  The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return an empty RecordValue
     */
    RecordValue addRecord(int index);

    /**
     * Sets the value at the specified index with an empty MapValue,
     * replacing any existing value at that index.  The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty MapValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     */
    MapValue setMap(int index);

    /**
     * Adds a new MapValue to the end of the array.  The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @return an empty MapValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    MapValue addMap();

    /**
     * Inserts a new MapValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right. The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return an empty MapValue
     */
    MapValue addMap(int index);

    /**
     * Sets the value at the specified index with an empty ArrayValue,
     * replacing any existing value at that index.  The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty ArrayValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    ArrayValue setArray(int index);

    /**
     * Adds a new ArrayValue to the end of the array. The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return an empty ArrayValue
     */
    ArrayValue addArray();

    /**
     * Inserts a new ArrayValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right.  The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return an empty ArrayValue
     */
    ArrayValue addArray(int index);

    /**
     * Adds a JSON null value at the end of the array.
     *
     * @throws IllegalArgumentException if the type of the array is not JSON.
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue addJsonNull();

    /**
     * Inserts a JSON null value at the specified index.  This does not
     * replace an existing value, all values at or above the index are
     * shifted to the right.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the type of the array is not JSON.
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue addJsonNull(int index);

    /**
     * Set the value at the specified index to JSON null. This method replaces
     * any existing value at that index.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the type of the array is not JSON.
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.3
     */
    ArrayValue setJsonNull(int index);

    /**
     * Adds arbitrary JSON to the end of the array.
     *
     * @param jsonInput a JSON string
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @since 4.2
     */
    ArrayValue addJson(String jsonInput);

    /**
     * Adds arbitrary JSON to the end of the array.
     *
     * @param jsonInput a Reader over JSON
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @since 4.2
     */
    ArrayValue addJson(Reader jsonInput);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param jsonInput a JSON string
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.2
     */
    ArrayValue addJson(int index, String jsonInput);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param jsonInput a JSON Reader
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.2
     */
    ArrayValue addJson(int index, Reader jsonInput);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param jsonInput a JSON string
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.2
     */
    ArrayValue setJson(int index, String jsonInput);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param jsonInput a JSON Reader
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index &lt; 0 || index &gt;= size())
     *
     * @return this
     *
     * @since 4.2
     */
    ArrayValue setJson(int index, Reader jsonInput);
}
