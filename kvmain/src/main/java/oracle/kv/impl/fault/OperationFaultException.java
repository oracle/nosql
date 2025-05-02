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

package oracle.kv.impl.fault;

/**
 * Informs the {@link ProcessFaultHandler} that the fault is localized to a
 * specific operation and that the process can continue to function normally.
 * <p>
 * Since the fault handling design requires that this exception be used
 * entirely within a process, it's not Serializable.
 *
 * @see SystemFaultException
 */
@SuppressWarnings("serial")
public class OperationFaultException extends RuntimeException {

    /**
     * Constructor making provision for an exception message.
     *
     * @param msg the exception message
     */
    public OperationFaultException(String msg) {
        super(msg);
    }

    /**
     * Constructor to wrap a fault.
     *
     * @param msg a message further explaining the error
     * @param fault the exception being wrapped
     */
    public OperationFaultException(String msg, Throwable fault) {
        super(msg, fault);
        assert fault != null;
    }
}
