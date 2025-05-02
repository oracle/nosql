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

import oracle.kv.Version;

/**
 * The Result associated with the execution of a TableOperation.
 *
 * @see TableOperationFactory
 * @see TableAPI#execute execute
 *
 * @since 3.0
 */
public interface TableOperationResult {

    /**
     * Whether the operation succeeded.  A put or delete operation may be
     * unsuccessful if the row or version was not matched.
     */
    boolean getSuccess();

    /**
     * For a put operation, the version of the new row.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put operation.
     * </li>
     * <li>The put operation did not succeed.
     * </li>
     * </ul>
     */
    Version getNewVersion();

    /**
     * For a put or delete operation, the version of the previous row
     * associated with the key.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put or delete operation.
     * </li>
     * <li>A previous value did not exist for the given row.
     * </li>
     * <li>The {@code prevReturn} {@link ReturnRow} parameter
     * specified that the version should not be returned.
     * </li>
     * <li>For a {@link TableOperationFactory#createPutIfVersion putIfVersion}
     * or {@link TableOperationFactory#createDeleteIfVersion deleteIfVersion}
     * operation, the {@code matchVersion} parameter matched the version of the
     * previous value.
     * </li>
     * </ul>
     */
    Version getPreviousVersion();

    /**
     * For a put or delete operation, the previous value associated with
     * the row.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put or delete operation.
     * </li>
     * <li>A previous value did not exist for the given row.
     * </li>
     * <li>The {@code prevReturn} {@link ReturnRow} parameter
     * specified that the row should not be returned.
     * </li>
     * <li>For a {@link TableOperationFactory#createPutIfVersion putIfVersion}
     * or {@link TableOperationFactory#createDeleteIfVersion deleteIfVersion}
     * operation, the {@code matchVersion} parameter matched the version of the
     * previous value.
     * </li>
     * </ul>
     */
    Row getPreviousRow();

    /**
     * For a put or delete operation, the previous expiration time
     * associated with the row.
     *
     * <p>Is 0 if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put or delete operation.
     * </li>
     * <li>A previous value did not exist for the given row.
     * </li>
     * <li>The {@code prevReturn} {@link ReturnRow} parameter
     * specified that neither the row nor version should be returned.
     * </li>
     * <li>For a {@link TableOperationFactory#createPutIfVersion putIfVersion}
     * or {@link TableOperationFactory#createDeleteIfVersion deleteIfVersion}
     * operation, the {@code matchVersion} parameter matched the version of the
     * previous value.
     * </li>
     * </ul>
     *
     * @since 4.1
     */
    long getPreviousExpirationTime();

}
