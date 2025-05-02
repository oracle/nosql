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
 * Used to indicate a failure to execute a sequence of operations.
 *
 * @see KVStore#execute execute
 */
public class OperationExecutionException extends ContingencyException {

    private static final long serialVersionUID = 1L;

    private final Operation failedOperation;
    private final int failedOperationIndex;
    private final OperationResult failedOperationResult;

    /**
     * For internal use only.
     * @hidden
     */
    public OperationExecutionException(Operation failedOperation,
                                       int failedOperationIndex,
                                       OperationResult failedOperationResult) {
        super("Failed operation: " + failedOperation +
              ", index: " + failedOperationIndex +
              ", result: " + failedOperationResult);
        this.failedOperation = failedOperation;
        this.failedOperationIndex = failedOperationIndex;
        this.failedOperationResult = failedOperationResult;
    }

    /**
     * The operation that caused the execution to be aborted.
     */
    public Operation getFailedOperation() {
        return failedOperation;
    }

    /**
     * The result of the operation that caused the execution to be aborted.
     */
    public OperationResult getFailedOperationResult() {
        return failedOperationResult;
    }

    /**
     * The list index of the operation that caused the execution to be aborted.
     */
    public int getFailedOperationIndex() {
        return failedOperationIndex;
    }
}
