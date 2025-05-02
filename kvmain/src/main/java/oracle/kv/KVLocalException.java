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
 * Generic exception class for generating runtime exceptions for KVLocal.
 *
 * <p>
 * The KVLocalException is thrown in the following situations:
 * <ol>
 * <li> An error occurs when starting or stopping an embedded NoSQL
 * Database instance
 * <li> An error occurs when a request is made to an embedded NoSQL
 * Database instance
 * </ol>
 *
 * @since 22.1
 */
public class KVLocalException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Constructs an instance of <code>KVLocalException</code> with the
     * specified message.
     *
     * @param message the detail message
     * @hidden For internal use only
     */
    public KVLocalException(String message) {
        this(message, null);
    }

    /**
     * Constructs an instance of <code>KVLocalException</code> with the
     * specified cause.
     *
     * @param cause the cause
     * @hidden For internal use only
     */
    public KVLocalException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /** Constructs an instance of <code>KVLocalException</code> with the
     * specified message and cause.
     *
     * @param msg the detail message
     * @param cause the cause
     * @hidden For internal use only
     */
    public KVLocalException(String msg, Throwable cause) {
        super(msg + " (" + KVVersion.CURRENT_VERSION.getNumericVersionString()
            + ")", cause);
    }
}
