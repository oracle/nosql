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

package oracle.kv;

/**
 * Generic interface for translating between {@link Value}s (stored byte
 * arrays) and typed objects representing that value.  In other words, this
 * interface is used for serialization and deserialization of {@link Value}s.
 * 
 * @param <T> is the type of the deserialized object that is passed to {@link
 * #toValue toValue} and returned by {@link #toObject toObject}.  The specific
 * type depends on the particular binding that is used.
 *
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public interface ValueBinding<T> {

    /**
     * After doing a read operation using a {@link KVStore} method, the user
     * calls {@code toObject} with the {@link Value} obtained from the read
     * operation.
     *
     * @param value the {@link Value} obtained from a {@link KVStore} read
     * operation method.
     *
     * @return the deserialized object.
     *
     * @throws RuntimeException if a parameter value is disallowed by the
     * binding
     */
    public T toObject(Value value)
        throws RuntimeException;

    /**
     * Before doing a write operation, the user calls {@code toValue} passing
     * an object she wishes to store.  The resulting {@link Value} is then
     * passed to the write operation method in {@link KVStore}.
     *
     * @param object the object the user wishes to store, or at least
     * serialize.
     *
     * @return the serialized object.
     *
     * @throws RuntimeException if a parameter value is disallowed by the
     * binding
     */
    public Value toValue(T object)
        throws RuntimeException;
}
