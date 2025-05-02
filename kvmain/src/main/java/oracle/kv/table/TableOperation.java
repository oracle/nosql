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

/**
 * Denotes a TableOperation in a sequence of operations passed to the {@link
 * TableAPI#execute TableAPI.execute} method.
 *
 * <p>TableOperation instances are created only by
 * {@link TableOperationFactory} methods
 * and the TableOperation interface should not be implemented by the
 * application.</p>
 *
 * @see TableOperationFactory
 * @see TableAPI#execute TableAPI.execute
 *
 * @since 3.0
 */
public interface TableOperation {

    /**
     * The type of operation, as determined by the method used to create it.
     */
    public enum Type {

        /**
         * An operation created by
         * {@link TableOperationFactory#createPut}.
         */
        PUT,

        /**
         * An operation created by
         * {@link TableOperationFactory#createPutIfAbsent}.
         */
        PUT_IF_ABSENT,

        /**
         * An operation created by
         * {@link TableOperationFactory#createPutIfPresent}.
         */
        PUT_IF_PRESENT,

        /**
         * An operation created by
         * {@link TableOperationFactory#createPutIfVersion}.
         */
        PUT_IF_VERSION,

        /**
         * An operation created by {@link TableOperationFactory#createDelete}.
         */
        DELETE,

        /**
         * An operation created by {@link
         * TableOperationFactory#createDeleteIfVersion}.
         */
        DELETE_IF_VERSION,
    }

    /**
     * Returns the Row associated with the operation if it is a put operation,
     * otherwise return null.
     *
     * @return the row or null
     */
    Row getRow();

    /**
     * Returns the PrimaryKey associated with the operation if it is a
     * delete operation, otherwise return null.
     *
     * @return the primary key or null
     */
    PrimaryKey getPrimaryKey();

    /**
     * Returns the operation Type.
     *
     * @return the type
     */
    Type getType();

    /**
     * Returns whether this operation should cause the {@link TableAPI#execute
     * TableAPI.execute} transaction to abort when the operation fails.
     *
     * @return true if operation failure should cause the entire execution to
     * abort
     */
    boolean getAbortIfUnsuccessful();

    /**
     * @hidden
     * Use by cloud proxy
     *
     * Sets whether absolute expiration time will be modified during update.
     * If false and the operation updates a record, the record's expiration
     * time will not change.
     */
    void setUpdateTTL(boolean value);

    /**
     * @hidden
     * Use by cloud proxy
     *
     * Returns true if the absolute expiration time is to be modified during
     * update operations.
     */
    boolean getUpdateTTL();
}
