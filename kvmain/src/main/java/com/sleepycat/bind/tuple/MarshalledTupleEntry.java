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

package com.sleepycat.bind.tuple;

/**
 * A marshalling interface implemented by key, data or entity classes that
 * are represented as tuples.
 *
 * <p>Key classes implement this interface to marshal their key entry.  Data or
 * entity classes implement this interface to marshal their data entry.
 * Implementations of this interface must have a public no arguments
 * constructor so that they can be instantiated by a binding, prior to calling
 * the {@link #unmarshalEntry} method.</p>
 *
 * <p>Note that implementing this interface is not necessary when the object is
 * a Java simple type, for example: String, Integer, etc. These types can be
 * used with built-in bindings returned by {@link
 * TupleBinding#getPrimitiveBinding}.</p>
 *
 * @author Mark Hayes
 */
public interface MarshalledTupleEntry {

    /**
     * Construct the key or data tuple entry from the key or data object.
     *
     * @param dataOutput is the output tuple.
     */
    void marshalEntry(TupleOutput dataOutput);

    /**
     * Construct the key or data object from the key or data tuple entry.
     *
     * @param dataInput is the input tuple.
     */
    void unmarshalEntry(TupleInput dataInput);
}
