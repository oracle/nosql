/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import oracle.nosql.nson.util.SizeOf;

/**
 * ArrayValue represents an array of {@link FieldValue} instances. It behaves
 * in a similar manner to {@link ArrayList} with operations to set and add
 * entries. ArrayValue indexes are zero-based.
 */
public class ArrayValue extends FieldValue implements Iterable<FieldValue> {

    private final ArrayList<FieldValue> array;

    /**
     * Creates an empty ArrayValue
     */
    public ArrayValue() {
        super();
        array = new ArrayList<FieldValue>();
    }

    /**
     * Creates an empty ArrayValue with the specified size
     *
     * @param size the starting size for the array
     */
    public ArrayValue(int size) {
        super();
        array = new ArrayList<FieldValue>(size);
    }

    @Override
    public Type getType() {
        return Type.ARRAY;
    }

    /**
     * @hidden
     * @return the internal array
     */
    public ArrayList<FieldValue> getArrayInternal() {
        return array;
    }

    /**
     * Returns the size of the array
     *
     * @return the size
     */
    public int size() {
        return array.size();
    }

    /**
     * Returns the type of the field at the specified index.
     *
     * @param index the index of the value to return, zero-based
     *
     * @return the type of the field at the specified index
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue.Type getType(int index) {
        FieldValue val = array.get(index);
        return (val == null ? null : val.getType());
    }

    /**
     * Returns the field at the specified index.
     *
     * @param index the index of the value to return, zero-based
     *
     * @return the field at the specified index
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue get(int index) {
        return array.get(index);
    }

    /**
     * Adds the field to the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(FieldValue value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        array.add(value);
        return this;
    }

    /**
     * Inserts the field at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, FieldValue value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        array.add(index, value);
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, FieldValue value) {
        requireNonNull(value, "ArrayValue.set: value must be non-null");
        return array.set(index, value);
    }

    /**
     * Removes the element at the specified position, shifting any subsequent
     * elements to the left.
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue remove(int index) {
        return array.remove(index);
    }

    /**
     * Adds all of the values in the Stream to the end of the array in the
     * order they are returned by the stream.
     *
     * @param stream the Stream
     *
     * @return this
     */
    public ArrayValue addAll(Stream<? extends FieldValue> stream) {
        requireNonNull(stream, "ArrayValue.addAll: stream must be non-null");
        stream.forEachOrdered(v -> array.add(v));
        return this;
    }

    /**
     * Inserts all of the elements in the specified Stream into the array,
     * starting at the specified position. Shifts the element currently at that position
     * and any subsequent elements to the right (increases their indices). New elements
     * are added in the order that they are returned by the stream.
     *
     * @param index the index to use
     *
     * @param stream the Stream
     *
     * @return this
     */
    public ArrayValue addAll(int index, Stream<? extends FieldValue> stream) {
        requireNonNull(stream, "ArrayValue.addAll: stream must be non-null");
        stream.forEachOrdered(new Consumer<FieldValue>() {
                int i = 0;
                @Override public void accept(FieldValue v) {
                    array.add(index + i++, v);
                }
            });
        return this;
    }

    /**
     * Adds all of the values in the Iterator to the end of the array in the
     * order they are returned by the iterator.
     *
     * @param iter the iterator
     *
     * @return this
     */
    public ArrayValue addAll(Iterator<? extends FieldValue> iter) {
        requireNonNull(iter, "ArrayValue.addAll: iter must be non-null");
        while (iter.hasNext()) {
            array.add(iter.next());
        }
        return this;
    }

    /**
     * Inserts all of the elements in the specified Iterator into the array,
     * starting at the specified position. Shifts the element currently at that position
     * and any subsequent elements to the right (increases their indices). New elements
     * are added in the order that they are returned by the iterator.
     *
     * @param index the index to use
     *
     * @param iter the iterator
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue addAll(int index, Iterator<? extends FieldValue> iter) {
        requireNonNull(iter, "ArrayValue.addAll: iter must be non-null");
        int i = 0;
        while (iter.hasNext()) {
            array.add(index + i++, iter.next());
        }
        return this;
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(int value) {
        return add(new IntegerValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, int value) {
        array.add(index, new IntegerValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, int value) {
        return set(index, new IntegerValue(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(long value) {
        return add(new LongValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, long value) {
        array.add(index, new LongValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, long value) {
        return set(index, new LongValue(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(double value) {
        return add(new DoubleValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, double value) {
        array.add(index, new DoubleValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, double value) {
        return set(index, new DoubleValue(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(BigDecimal value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        return add(new NumberValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, BigDecimal value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        array.add(index, new NumberValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, BigDecimal value) {
        requireNonNull(value, "ArrayValue.set: value must be non-null");
        return set(index, new NumberValue(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(boolean value) {
        return add(BooleanValue.getInstance(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, boolean value) {
        array.add(index, BooleanValue.getInstance(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, boolean value) {
        return set(index, BooleanValue.getInstance(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(String value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        return add(new StringValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, String value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        array.add(index, new StringValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, String value) {
        requireNonNull(value, "ArrayValue.set: value must be non-null");
        return set(index, new StringValue(value));
    }

    /**
     * Adds a new value at the end of the array
     *
     * @param value the value to add
     *
     * @return this
     */
    public ArrayValue add(byte[] value) {
        return add(new BinaryValue(value));
    }

    /**
     * Inserts a new value at the specified index. Shifts the element at that
     * position and any subsequent elements to the right.
     *
     * @param index the index to use
     *
     * @param value the value to add
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public ArrayValue add(int index, byte[] value) {
        requireNonNull(value, "ArrayValue.add: value must be non-null");
        array.add(index, new BinaryValue(value));
        return this;
    }

    /**
     * Replaces the element at the specified position with the new value.
     *
     * @param value the value to set
     *
     * @param index the index to use
     *
     * @return the element previously at the specified position
     *
     * @throws IndexOutOfBoundsException if the index is out of the range
     * of the array.
     */
    public FieldValue set(int index, byte[] value) {
        requireNonNull(value, "ArrayValue.set: value must be non-null");
        return set(index, new BinaryValue(value));
    }

    @Override
    public int compareTo(FieldValue other) {
        requireNonNull(other, "ArrayValue.compareTo: other must be non-null");
        ArrayValue otherImpl = other.asArray();
        int minSize = (size() <= otherImpl.size() ? size() : otherImpl.size());
        for (int i = 0; i < minSize; i++) {
            FieldValue val = get(i);
            int ret = val.compareTo(otherImpl.get(i));
            if (ret != 0) {
                return ret;
            }
        }

        if (minSize < otherImpl.size()) {
            return -1;
        } else if (minSize < size()) {
            return 1;
        }

        /* they must be equal */
        return 0;
    }

    @Override
    public Iterator<FieldValue> iterator() {
        return array.iterator();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ArrayValue) {
            return array.equals(((ArrayValue)other).array);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return array.hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {

        long size = (SizeOf.OBJECT_OVERHEAD +
                     SizeOf.OBJECT_REF_OVERHEAD +
                     SizeOf.ARRAYLIST_OVERHEAD +
                     SizeOf.objectArraySize(array.size()));

        for (FieldValue elem : array) {
            size += elem.sizeof();
        }

        return size;
    }

    /**
     * @hidden
     * Sorts the elements of the array according to the given comparator.
     *
     * @param comparator The Comparator to use for comparing the array
     * elements. It must not be null.
     */
    public void sort(Comparator<FieldValue> comparator) {
        requireNonNull(comparator, "Comparator must be non-null");
        array.sort(comparator);
    }
}
