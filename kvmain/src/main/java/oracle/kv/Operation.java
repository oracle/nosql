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
 * Denotes an Operation in a sequence of operations passed to the {@link
 * KVStore#execute KVStore.execute} method.
 *
 * <p>Operation instances are created only by {@link OperationFactory} methods
 * and the Operation interface should not be implemented by the
 * application.</p>
 *
 * @see OperationFactory
 * @see KVStore#execute KVStore.execute
 */
public interface Operation {

    /**
     * The type of operation, as determined by the method used to create it.
     */
    public enum Type {

        /**
         * An operation created by {@link OperationFactory#createPut}.
         */
        PUT,

        /**
         * An operation created by {@link OperationFactory#createPutIfAbsent}.
         */
        PUT_IF_ABSENT,

        /**
         * An operation created by {@link OperationFactory#createPutIfPresent}.
         */
        PUT_IF_PRESENT,

        /**
         * An operation created by {@link OperationFactory#createPutIfVersion}.
         */
        PUT_IF_VERSION,

        /**
         * An operation created by {@link OperationFactory#createDelete}.
         */
        DELETE,

        /**
         * An operation created by {@link
         * OperationFactory#createDeleteIfVersion}.
         */
        DELETE_IF_VERSION,
    }

    /**
     * Returns the Key associated with the operation.
     */
    Key getKey();

    /**
     * Returns the operation Type.
     */
    Type getType();

    /**
     * Returns whether this operation should cause the {@link KVStore#execute
     * KVStore.execute} transaction to abort when the operation fails.
     */
    boolean getAbortIfUnsuccessful();
}
