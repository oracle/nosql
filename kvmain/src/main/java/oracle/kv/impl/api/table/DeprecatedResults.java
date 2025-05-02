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

package oracle.kv.impl.api.table;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.FaultException;

/**
 * This class contains wrapper classes meant solely to support the API change
 * to move TableAPI.{execute, executeSync} to a higher level package, and their
 * new home in KVStore.{execute, executeSync}.
 *
 * The TableAPI methods were changed to merely dispatch to the new KVStore
 * methods, in order to avoid duplication of the underlying implementation.
 * Since the result classes, ExecutionFuture and StatementResult also moved
 * from oracle.kv.table to oracle.kv, these wrapper classes translate the new
 * result classes to the old result classes.  This class can be removed when
 * the TableAPI.execute* methods are removed.
 */
@Deprecated
class DeprecatedResults {

    /**
     * Convert an oracle.kv.ExecutionFuture into an oracle.kv.ExecutionFuture.
     */
    static class ExecutionFutureWrapper
        implements oracle.kv.table.ExecutionFuture {

        /** The new methods return this kind of ExecutionFuture */
        final private oracle.kv.ExecutionFuture storeFuture;

        ExecutionFutureWrapper(oracle.kv.ExecutionFuture storeFuture) {
            this.storeFuture = storeFuture;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return storeFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public oracle.kv.table.StatementResult get()
            throws CancellationException,
                   ExecutionException, 
                   InterruptedException {
            return new StatementResultWrapper(storeFuture.get());
        }

        @Override
        public oracle.kv.table.StatementResult get(long timeout, TimeUnit unit)
            throws InterruptedException, 
                   TimeoutException,
                   ExecutionException {
            return new StatementResultWrapper(storeFuture.get(timeout, unit));
        }

        @Override
        public boolean isCancelled() {
            return storeFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return storeFuture.isDone();
        }

        @Override
        public oracle.kv.table.StatementResult updateStatus() 
            throws FaultException {
            return new StatementResultWrapper(storeFuture.updateStatus());
        }

        @Override
        public oracle.kv.table.StatementResult getLastStatus() {
            return new StatementResultWrapper(storeFuture.getLastStatus());
        }

        @Override
        public String getStatement() {
            return storeFuture.getStatement();
        }
    }

    /**
     * Convert an oracle.kv.StatementResult into an oracle.kv.StatementResult.
     */
    static class StatementResultWrapper
        implements oracle.kv.table.StatementResult {

        /** The new methods return this kind of result */
        private final oracle.kv.StatementResult storeResult;

        StatementResultWrapper(oracle.kv.StatementResult storeResult) {
            this.storeResult = storeResult;
        }

        @Override
        public int getPlanId() {
            return storeResult.getPlanId();
        }

        @Override
        public String getInfo() {
            return storeResult.getInfo();
        }

        @Override
        public String getInfoAsJson() {
            return storeResult.getInfoAsJson();
        }

        @Override
        public String getErrorMessage() {
            return storeResult.getErrorMessage();
        }

        @Override
        public boolean isSuccessful() {
            return storeResult.isSuccessful();
        }

        @Override
        public boolean isDone() {
            return storeResult.isDone();
        }

        @Override
        public boolean isCancelled() {
            return storeResult.isCancelled();
        }
    }
}
