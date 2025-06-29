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
 * A factory to create operations that can be batched for execution by {@link
 * TableAPI#execute TableAPI.execute}.
 * <p>
 * Each operation created here operates on a single row and matches the
 * corresponding operation defined in TableAPI. For example, the Operation
 * generated by the {@link #createPut createPut} method corresponds to the
 * {@link TableAPI#put put} method. The argument pattern for creating the
 * operation is similar. It differs in the following respects:
 * <ul>
 * <li>
 * The writeOptions argument is not passed, since that argument applies to the
 * execution of the entire batch of operations and is passed in to the {@link
 * TableAPI#execute execute} method.
 * </li>
 * <li>
 * {@link ReturnRow.Choice} is passed instead of
 * {@link ReturnRow}.
 * </li>
 * <li>
 * An additional argument, {@code abortIfUnsuccessful} is passed.
 * </li>
 * </ul>
 * <p>
 * The return values associated with operations similarly match the
 * descriptions for the corresponding methods described in in {@link TableAPI}.
 * They are, however, retrieved differently: the status, return value, previous
 * value and version are packaged together in {@link TableOperationResult}.
 * </p>
 *
 * @since 3.0
 */
public interface TableOperationFactory {

    /**
     * Create a Put operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned
     * operation are identical to that of the {@link TableAPI#put put} method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the {@link TableAPI#put put}
     * method returns null.
     *
     * @return the created Put operation
     *
     * @see TableAPI#put put
     * @see TableAPI#execute execute
     */
    TableOperation createPut(Row row,
                             ReturnRow.Choice prevReturn,
                             boolean abortIfUnsuccessful);

    /**
     * Create a Put operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned
     * operation are identical to that of the
     * {@link TableAPI#putIfAbsent putIfAbsent} method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the {@link TableAPI#put put}
     * method returns null.
     *
     * @return the created Put operation
     *
     * @see TableAPI#putIfAbsent putIfAbsent
     * @see TableAPI#execute execute
     */
    TableOperation createPutIfAbsent(Row row,
                                     ReturnRow.Choice prevReturn,
                                     boolean abortIfUnsuccessful);

    /**
     * Create a Put operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned operation
     * are identical to that of the {@link TableAPI#putIfPresent putIfPresent}
     * method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the {@link TableAPI#put put}
     * method returns null.
     *
     * @return the created Put operation
     *
     * @see TableAPI#putIfPresent putIfPresent
     * @see TableAPI#execute execute
     */
    TableOperation createPutIfPresent(Row row,
                                      ReturnRow.Choice prevReturn,
                                      boolean abortIfUnsuccessful);

    /**
     * Create a Put operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned operation
     * are identical to that of the {@link TableAPI#putIfVersion putIfVersion}
     * method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the {@link TableAPI#put put}
     * method returns null.
     *
     * @return the created Put operation
     *
     * @see TableAPI#putIfVersion putIfVersion
     * @see TableAPI#execute execute
     */
    TableOperation createPutIfVersion(Row row,
                                      Version versionMatch,
                                      ReturnRow.Choice prevReturn,
                                      boolean abortIfUnsuccessful);

    /**
     * Create a Delete operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned
     * operation are identical to that of the
     * {@link TableAPI#delete delete} method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the
     * {@link TableAPI#delete delete}
     * method returns null.
     *
     * @return the created Delete operation
     *
     * @see TableAPI#delete delete
     * @see TableAPI#execute execute
     */
    TableOperation createDelete(PrimaryKey key,
                                ReturnRow.Choice prevReturn,
                                boolean abortIfUnsuccessful);

    /**
     * Create a Delete operation suitable for use as an argument to the {@link
     * TableAPI#execute execute} method. The semantics of the returned
     * operation are identical to that of the
     * {@link TableAPI#deleteIfVersion deleteIfVersion} method.
     *
     * <p>The result of evaluating the operation, or the resulting exception,
     * after the call to {@link TableAPI#execute execute} is available as an
     * {@link TableOperationResult}.</p>
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * {@link TableAPI#execute execute} transaction to abort when the operation
     * fails, where failure is the condition when the
     * {@link TableAPI#deleteIfVersion deleteIfVersion}
     * method returns null.
     *
     * @return the created Delete operation
     *
     * @see TableAPI#deleteIfVersion deleteIfVersion
     * @see TableAPI#execute execute
     */
    TableOperation createDeleteIfVersion(PrimaryKey key,
                                         Version versionMatch,
                                         ReturnRow.Choice prevReturn,
                                         boolean abortIfUnsuccessful);
}
