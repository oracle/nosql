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

package oracle.kv.impl.arb.admin;

import oracle.kv.impl.fault.OperationFaultException;

/**
 * Exception indicating that an invalid operation occurred while a ArbNode
 * was in its startup sequence.
 */
public class IllegalArbNodeServiceStateException extends OperationFaultException {

    private static final long serialVersionUID = 1L;

    public IllegalArbNodeServiceStateException(String message) {
        super(message);
    }
}
