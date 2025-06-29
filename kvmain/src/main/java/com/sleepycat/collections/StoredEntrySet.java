/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.collections;

import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
/* <!-- begin JE only --> */
import com.sleepycat.je.EnvironmentFailureException; // for javadoc
import com.sleepycat.je.OperationFailureException; // for javadoc
/* <!-- end JE only --> */
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.RuntimeExceptionWrapper;

/**
 * The Set returned by Map.entrySet().  This class may not be instantiated
 * directly.  Contrary to what is stated by {@link Map#entrySet} this class
 * does support the {@link #add} and {@link #addAll} methods.
 *
 * <p>The {@link java.util.Map.Entry#setValue} method of the Map.Entry objects
 * that are returned by this class and its iterators behaves just as the {@link
 * StoredIterator#set} method does.</p>
 *
 * @author Mark Hayes
 */
public class StoredEntrySet<K, V>
    extends StoredCollection<Map.Entry<K, V>>
    implements Set<Map.Entry<K, V>> {

    StoredEntrySet(DataView mapView) {

        super(mapView);
    }

    /**
     * Adds the specified element to this set if it is not already present
     * (optional operation).
     * This method conforms to the {@link Set#add} interface.
     *
     * @param mapEntry must be a {@link java.util.Map.Entry} instance.
     *
     * @return true if the key-value pair was added to the set (and was not
     * previously present).
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws UnsupportedOperationException if the collection is read-only.
     *
     * @throws ClassCastException if the mapEntry is not a {@link
     * java.util.Map.Entry} instance.
     *
     * @throws RuntimeExceptionWrapper if a checked exception is thrown,
     * including a {@code DatabaseException} on BDB (C edition).
     */
    public boolean add(Map.Entry<K, V> mapEntry) {

        return add(mapEntry.getKey(), mapEntry.getValue());
    }

    /**
     * Removes the specified element from this set if it is present (optional
     * operation).
     * This method conforms to the {@link Set#remove} interface.
     *
     * @param mapEntry is a {@link java.util.Map.Entry} instance to be removed.
     *
     * @return true if the key-value pair was removed from the set, or false if
     * the mapEntry is not a {@link java.util.Map.Entry} instance or is not
     * present in the set.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws UnsupportedOperationException if the collection is read-only.
     *
     * @throws RuntimeExceptionWrapper if a checked exception is thrown,
     * including a {@code DatabaseException} on BDB (C edition).
     */
    public boolean remove(Object mapEntry) {

        if (!(mapEntry instanceof Map.Entry)) {
            return false;
        }
        DataCursor cursor = null;
        boolean doAutoCommit = beginAutoCommit();
        try {
            cursor = new DataCursor(view, true);
            Map.Entry entry = (Map.Entry) mapEntry;
            OperationStatus status =
                cursor.findBoth(entry.getKey(), entry.getValue(), true);
            if (status == OperationStatus.SUCCESS) {
                cursor.delete();
            }
            closeCursor(cursor);
            commitAutoCommit(doAutoCommit);
            return (status == OperationStatus.SUCCESS);
        } catch (Exception e) {
            closeCursor(cursor);
            throw handleException(e, doAutoCommit);
        }
    }

    /**
     * Returns true if this set contains the specified element.
     * This method conforms to the {@link Set#contains} interface.
     *
     * @param mapEntry is a {@link java.util.Map.Entry} instance to be checked.
     *
     * @return true if the key-value pair is present in the set, or false if
     * the mapEntry is not a {@link java.util.Map.Entry} instance or is not
     * present in the set.
     *
     * <!-- begin JE only -->
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * <!-- end JE only -->
     *
     * @throws RuntimeExceptionWrapper if a checked exception is thrown,
     * including a {@code DatabaseException} on BDB (C edition).
     */
    public boolean contains(Object mapEntry) {

        if (!(mapEntry instanceof Map.Entry)) {
            return false;
        }
        DataCursor cursor = null;
        try {
            cursor = new DataCursor(view, false);
            Map.Entry entry = (Map.Entry) mapEntry;
            OperationStatus status =
                cursor.findBoth(entry.getKey(), entry.getValue(), false);
            return (status == OperationStatus.SUCCESS);
        } catch (Exception e) {
            throw StoredContainer.convertException(e);
        } finally {
            closeCursor(cursor);
        }
    }

    // javadoc is inherited
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[");
        StoredIterator i = null;
        try {
            i = storedIterator();
            while (i.hasNext()) {
                Map.Entry entry = (Map.Entry) i.next();
                if (buf.length() > 1) buf.append(',');
                Object key = entry.getKey();
                Object val = entry.getValue();
                if (key != null) buf.append(key.toString());
                buf.append('=');
                if (val != null) buf.append(val.toString());
            }
            buf.append(']');
            return buf.toString();
        } catch (Exception e) {
            throw StoredContainer.convertException(e);
        } finally {
            StoredIterator.close(i);
        }
    }

    Map.Entry<K, V> makeIteratorData(BaseIterator iterator,
                                     DatabaseEntry keyEntry,
                                     DatabaseEntry priKeyEntry,
                                     DatabaseEntry valueEntry) {

        return new StoredMapEntry(view.makeKey(keyEntry, priKeyEntry),
                                  view.makeValue(priKeyEntry, valueEntry),
                                  this, iterator);
    }

    boolean hasValues() {

        return true;
    }
}
