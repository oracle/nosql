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
 * Signals that a database handler is not ready yet.
 * <p>
 * Since the fault handling design requires that this exception be used
 * entirely within a process, it's not Serializable.
 */
@SuppressWarnings("serial")
public class DatabaseNotReadyException extends OperationFaultException {

    /**
     * Constructor making provision for an exception message.
     *
     * @param msg the exception message
     */
    public DatabaseNotReadyException(String msg) {
        super(msg);
    }
}
