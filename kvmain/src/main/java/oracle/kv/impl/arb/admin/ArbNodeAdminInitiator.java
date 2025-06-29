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

import java.util.logging.Logger;

import oracle.kv.impl.async.JavaSerialInitiatorProxy;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;

/**
 * A client-side implementation of ArbNodeAdmin using async.
 */
public class ArbNodeAdminInitiator {

    public static ArbNodeAdmin createProxy(CreatorEndpoint endpoint,
                                           DialogType dialogType,
                                           long timeoutMs,
                                           Logger logger) {
        return JavaSerialInitiatorProxy.createProxy(
            ArbNodeAdmin.class, ARB_NODE_ADMIN_TYPE_FAMILY, endpoint,
            dialogType, timeoutMs, logger);
    }
}
