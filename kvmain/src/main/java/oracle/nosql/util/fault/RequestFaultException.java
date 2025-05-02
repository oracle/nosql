/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

/**
 * A wrapper exception used to wrap existing exceptions when their fault
 * domain is restricted to the request.
 *
 * For example, IllegalArgumentException to indicate contexts in which they are
 * expected, wrap this exception and throw RequestFaultException instead.
 */
public class RequestFaultException extends RuntimeException
    implements RequestFault {

    private static final long serialVersionUID = 1L;

    /* The error associated with this exception. */
    private final ErrorCode error;

    /**
     * Constructor for a wrapped fault exception
     *
     * @param fault the exception being wrapped to indicate its
     * fault domain
     * @param error the error to be used to describe the exception
     */
    public RequestFaultException(Throwable fault, ErrorCode error) {
        super(fault);
        this.error = error;
    }

    /**
     * Constructor for a wrapped fault exception
     *
     * @param msg the message describing this error
     * @param fault the exception being wrapped to indicate its
     * fault domain
     * @param error the error to be used to describe the exception
     */
    public RequestFaultException(String msg, Throwable fault, ErrorCode error) {
        super(msg, fault);
        this.error = error;
    }

    public RequestFaultException(String msg, ErrorCode error) {
        super(msg);
        this.error = error;
    }

    @Override
    public ErrorCode getError() {
        return error;
    }
}
