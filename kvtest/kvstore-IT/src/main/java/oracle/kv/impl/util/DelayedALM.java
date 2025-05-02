/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A LoginManager implementation that is suitable for logging into an Admin,
 * with the property that the verification of login is deferred until a
 * CS handle is acquired.
 */
public class DelayedALM extends AdminLoginManager {
    private final String[] hostPorts;
    private final LoginCredentials creds;

    public DelayedALM(String[] hostPorts, LoginCredentials creds) {
        super((creds == null) ? null : creds.getUsername(), true,
              LoggerUtils.getLogger(DelayedALM.class, "test"));
        this.hostPorts = hostPorts;
        this.creds = creds;
    }

    @Override
    protected void initializeLoginHandle() {
        bootstrap(hostPorts, creds);
    }
}
