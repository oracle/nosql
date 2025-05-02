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

import oracle.kv.util.ErrorMessage;

/**
 * Exception to indicate that an administrative command issued by the user is
 * not permissible at the current time due to the Admin not being ready.
 *
 * The command should be reissued, possibly after a delay, either by the user,
 * or by the CLI.
 */
public class AdminNotReadyException extends IllegalCommandException {
    private static final long serialVersionUID = 1L;

    public AdminNotReadyException(String message) {
        super(message, ErrorMessage.NOSQL_5300, CommandResult.NO_CLEANUP_JOBS);
    }

    public AdminNotReadyException(String message, Throwable t) {
        super(message, t, ErrorMessage.NOSQL_5300,
              CommandResult.NO_CLEANUP_JOBS);
    }
}
