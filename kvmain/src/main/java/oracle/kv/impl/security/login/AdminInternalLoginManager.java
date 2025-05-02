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
package oracle.kv.impl.security.login;

import java.util.logging.Logger;

import oracle.kv.impl.topo.ResourceId;

/**
 * Extends the LoginManager for Admin for the getHandle method with cachedOnly
 * behavior.
 */
public class AdminInternalLoginManager extends InternalLoginManager {


    /**
     * Constructs the login manager.
     */
    public AdminInternalLoginManager(TopologyResolver topoResolver,
                                     Logger logger)
    {
        super(topoResolver, logger);
    }

    /**
     * {@inheritDoc}
     *
     * Always returns {@code null} if cachedOnly is set to true. Because the
     * admin login manager resolves the SN information from the admin database,
     * in a sense, the handle is never cached and has to be obtained every time.
     * This has a performance overhead, but Admin has a light load and low
     * performance needs.
     *
     * This non-caching behavior is significant because upper layers often
     * assume that with cachedOnly flag set, the invocation is local and
     * non-blocking. The admin database access is blocking and therefore break
     * this assumption. There is yet another impact which is discovered in
     * [KVSTORE-2425]. The blocking access to the admin database cannot be
     * interrupted because JE must invalidate the environment upon
     * InterruptedException. On the other hand, the upper layer may interrupt
     * this invocation because it may be part of the request execution in a
     * query scan. The scan executor might be shutting down with request
     * execution tasks being cancelled with interruption.
     */
    @Override
    public LoginHandle getHandle(ResourceId target, boolean cachedOnly) {
        if (cachedOnly) {
            return null;
        }
        return super.getHandle(target, false);
    }
}
