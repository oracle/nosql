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

package oracle.kv.impl.async.exception;

/**
 *
 * INTERNAL: Exception classes for the async package.
 *
 * <p>There are three base classes: {@link ContextWriteException}, {@link
 * DialogException} and {@link ConnectionException}.
 *
 * <p>{@link ContextWriteException} occurs when {@link DialogContext#write} is
 * called.
 *
 * <p>{@link DialogException} is passed to {@link DialogHandler#onAbort} and
 * represents the cause of the dialog being aborted.
 *
 * <p>{@link ConnectionException} is the exception occuring for a connection
 * which is also used as the cause of the {@link DialogException} when a dialog
 * is being aborted because of the connection is discarded.
 */
