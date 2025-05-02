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

import java.util.concurrent.TimeUnit;

/**
 * Thrown by {@link KVStore#storeIterator(Direction, int, Key, KeyRange, Depth,
 * Consistency, long, TimeUnit, StoreIteratorConfig)} when an exception
 * occurs. The underlying exception may be retrieved using the {@link
 * #getCause()} method. storeIterator results sets are generally retrieved in
 * batches using a specific key for each batch. If an exception occurs during a
 * retrieval, the key used to gather that batch of records is available to the
 * application with the {@link #getKey} method. This might be useful, for
 * instance, to determine approximately how far an iteration had progressed
 * when the exception occurred.
 *
 * A StoreIteratorException being thrown from {@link
 * ParallelScanIterator#next()} method does not necessarily close or invalidate
 * the iterator. Repeated calls to next() may or may not cause an exception to
 * be thrown. It is incumbent on the caller to determine the type of exception
 * and act accordingly.
 */
public class StoreIteratorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final Key key;

    /**
     * For internal use only.
     * @hidden
     */
    public StoreIteratorException(Throwable cause, Key key) {
        super(cause.getMessage(), cause);
        this.key = key;
    }

    /**
     * Returns the key which was used to retrieve the current batch of records
     * in the iteration. Because batches are generally more than a single
     * record so the key may not be the key of the same record which caused the
     * fault.
     */
    public Key getKey() {
        return key;
    }
}
