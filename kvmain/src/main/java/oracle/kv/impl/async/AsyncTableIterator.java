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

package oracle.kv.impl.async;

import oracle.kv.table.TableIterator;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An interface that represents the operations needed to support asynchronous
 * iteration as implemented by {@link AsyncIterationHandleImpl}, in addition to
 * standard table iteration.
 *
 * @param <E> the type of the iteration element
 */
public interface AsyncTableIterator<E> extends TableIterator<E> {

    /**
     * Returns the next element that is available locally without blocking, or
     * null if none is available.  Also returns null if the iterator is closed.
     * If null is returned and the iterator is not closed, the associated
     * iteration handle's {@link IterationHandleNotifier#notifyNext} method
     * will be called when more elements become available or when the iterator
     * is closed.
     *
     * @return the next element or {@code null}
     */
    @Nullable E nextLocal();

    /**
     * Returns whether the iteration has been closed. Call {@link
     * #getCloseException} to determine if the iteration was closed cleanly or
     * due to failure.
     *
     * @return whether the iteration has been closed
     */
    boolean isClosed();

    /**
     * Returns the exception that caused the iteration to be closed, or null if
     * not known or the close was not due to failure.
     *
     * @return the cause of the close of the iteration or null
     */
    @Nullable Throwable getCloseException();
}
