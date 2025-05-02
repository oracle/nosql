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

import java.util.List;

import oracle.kv.ContingencyException;

/**
 * Provides information about a failure from the sequence of operations
 * executed by {@link TableAPI#execute(List, WriteOptions)
 * TableAPI.execute(List&lt;TableOperation&gt;, WriteOptions)}
 */
public class TableOpExecutionException extends ContingencyException {

    private static final long serialVersionUID = 1L;

    private final TableOperation failedOperation;
    private final int failedOperationIndex;
    private final TableOperationResult failedOperationResult;

    /* The number of KB read and written during this operation */
    private final int readKB;
    private final int writeKB;

    /**
     * For internal use only.
     * @hidden
     */
    public TableOpExecutionException
        (TableOperation failedOperation,
         int failedOperationIndex,
         TableOperationResult failedOperationResult,
         int readKB,
         int writeKB) {

        super("Failed table operation, type: " + failedOperation.getType() +
              ", operation index in list: " + failedOperationIndex);
        this.failedOperation = failedOperation;
        this.failedOperationIndex = failedOperationIndex;
        this.failedOperationResult = failedOperationResult;
        this.readKB = readKB;
        this.writeKB = writeKB;
    }

    /**
     * Returns the operation that caused the execution to be aborted.
     * @return the operation that caused the execution to be aborted
     */
    public TableOperation getFailedOperation() {
        return failedOperation;
    }

    /**
     * Returns the result of the operation that caused the execution to be
     * aborted.
     * @return the result of the operation that caused the execution to be
     * aborted
     */
    public TableOperationResult getFailedOperationResult() {
        return failedOperationResult;
    }

    /**
     * Returns the list index of the operation that caused the execution to be
     * aborted.
     * @return the list index of the operation that caused the execution to be
     * aborted
     */
    public int getFailedOperationIndex() {
        return failedOperationIndex;
    }

    /**
     * Returns the number of KB read during this operation. It may include
     * records read from the store but not returned in this result.
     * @return the number of KB read during this operation
     *
     * @hidden For use by cloud proxy
     */
    public int getReadKB() {
        return readKB;
    }

    /**
     * Returns the number of KB written during this operation.
     * @return the number of KB written during this operation
     *
     * @hidden For use by cloud proxy
     */
    public int getWriteKB() {
        return writeKB;
    }
}
