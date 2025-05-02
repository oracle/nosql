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
 * Used to indicate a status condition or result that is not expressed through
 * the return value, and that should normally be handled by the caller of the
 * method.
 */
public abstract class ContingencyException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    protected ContingencyException(String msg) {
        super(msg);
    }
}
