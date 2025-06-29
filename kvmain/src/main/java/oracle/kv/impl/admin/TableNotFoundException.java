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
package oracle.kv.impl.admin;

public class TableNotFoundException extends IllegalCommandException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of <code>TableNotFoundException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public TableNotFoundException(String msg) {
        super(msg);
    }
}
