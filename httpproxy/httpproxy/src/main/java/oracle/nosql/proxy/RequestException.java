/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

/**
 * A base exception for all request handling exceptions that may include
 * an explicit error code.
 */
public class RequestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int errorCode;

    public RequestException(int errorCode, String msg) {
        /* create a throwable with no stack trace */
        super(msg, null, true, false);
        this.errorCode = errorCode;
    }

    public RequestException(int errorCode, String msg, Throwable cause) {
        /* create a throwable with no stack trace */
        super(msg, cause, true, false);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    /* disable stack traces for RequestExceptions */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
