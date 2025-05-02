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

package oracle.kv.impl.query.compiler;

/**
 * An exception class that is thrown when an attempt is made to perform a DDL
 * query on a client and the query needs to be executed on the server side
 * in an admin.  This exception allows callers to catch it and re-route the
 * query to the proper execution environment.
 */
public class DdlException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DdlException(String message) {
        super(message);
    }
}
