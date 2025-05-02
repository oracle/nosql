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

package oracle.kv.impl.api.query;

import java.util.concurrent.TimeUnit;

import oracle.kv.ExecutionFuture;
import oracle.kv.StatementResult;

/**
 * An implementation of ExecutionFuture that encapsulates a DML query.  At
 * this time such queries are always synchronous.
 *
 * @since 4.0
 */
public class DmlFuture implements ExecutionFuture {

    private final StatementResult result;

    public DmlFuture(final StatementResult result) {
        this.result = result;
    }

    /**
     * Cancel always fails -- the query is done.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public StatementResult get() {
        return result;
    }

    @Override
    public StatementResult get(long timeout, TimeUnit unit) {
        return result;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public StatementResult updateStatus() {
        return result;
    }

    @Override
    public StatementResult getLastStatus() {
        return result;
    }

    @Override
    public String getStatement() {

        /*
         * TODO: Should this get stored? if so, some work on PreparedStatement
         * is necessary.
         */
        return null;
    }

    @Override
    public byte[] toByteArray() {

        /* TODO: maybe create a serialization of this */
        throw new IllegalArgumentException(
            "Cannot create a byte array from a query that is not DDL");
    }
}
