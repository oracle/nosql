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
 * A fault handler for the async endpoint group.
 */
public interface AsyncEndpointGroupFaultHandler {

    public static final AsyncEndpointGroupFaultHandler DEFAULT = (r) -> r.run();


    /**
     * Executes the runnable inside a process level fault handler that forces a
     * shutdow if the operations throws unexpected exception.
     */
    void execute(Runnable r);
}
