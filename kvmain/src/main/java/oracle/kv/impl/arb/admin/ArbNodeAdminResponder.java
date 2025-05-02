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

import static oracle.kv.impl.async.StandardDialogTypeFamily.ARB_NODE_ADMIN_TYPE_FAMILY;

import java.util.concurrent.Executor;
import java.util.logging.Logger;

import oracle.kv.impl.async.JavaSerialResponder;

/**
 * A responder (server-side) async dialog handler for ArbNodeAdmin.
 */
class ArbNodeAdminResponder extends JavaSerialResponder<ArbNodeAdmin> {

    ArbNodeAdminResponder(ArbNodeAdmin server,
                          Executor executor,
                          Logger logger) {
        super(server, ArbNodeAdmin.class, executor,
              ARB_NODE_ADMIN_TYPE_FAMILY, logger);
    }
}
