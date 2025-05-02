/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.tree;

/**
 * Error to indicate that a bottom level IN is not empty during a
 * delete subtree operation.
 */
public class NodeNotEmptyException extends Exception {

    private static final long serialVersionUID = 933349511L;

    /*
     * Throw this static instance, in order to reduce the cost of
     * fill in the stack trace.
     */
    public static final NodeNotEmptyException NODE_NOT_EMPTY =
        new NodeNotEmptyException();

    /* Make the constructor public for serializability testing. */
    public NodeNotEmptyException() {
    }
}
