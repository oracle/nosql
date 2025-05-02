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

package oracle.kv.impl.client.admin;

import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;

import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncInitiatorProxy;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.VersionedRemoteAsyncImpl;

/**
 * An initiator (client-side) implementation of {@link
 * AsyncClientAdminService}.
 */
public class AsyncClientAdminServiceInitiator {
    public static AsyncClientAdminService createProxy(CreatorEndpoint endpoint,
                                                      DialogType dialogType,
                                                      Logger logger) {
        return AsyncInitiatorProxy.createProxy(
            AsyncClientAdminService.class, CLIENT_ADMIN_SERVICE_TYPE_FAMILY,
            endpoint, dialogType, logger);
    }

    public static ClientAdminService createSyncProxy(CreatorEndpoint endpoint,
                                                     DialogType dialogType,
                                                     long timeoutMs,
                                                     Logger logger) {
        return VersionedRemoteAsyncImpl.createProxy(
            ClientAdminService.class,
            createProxy(endpoint, dialogType, logger), timeoutMs);
    }
}
