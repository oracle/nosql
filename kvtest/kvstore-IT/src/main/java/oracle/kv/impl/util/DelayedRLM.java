/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import oracle.kv.KVStoreException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.login.RepNodeLoginManager;

/**
 * A LoginManager implementation that is suitable for logging into a
 * RepNodeAdmin, with the property that the verification of login is deferred
 * until a RepNodeAdmin handle is acquired.
 */
public class DelayedRLM extends RepNodeLoginManager {

    private final String[] hostPorts;
    private final LoginCredentials creds;
    private final String storeName;

    public DelayedRLM(String[] hostPorts,
                      LoginCredentials creds,
                      String storeName) {
        super((creds == null) ? null : creds.getUsername(),
              true /* autoRenew */);
        this.hostPorts = hostPorts;
        this.creds = creds;
        this.storeName = storeName;
    }

    @Override
    protected void initializeLoginHandle() {
        try {
            bootstrap(hostPorts, creds, storeName);
        } catch (KVStoreException kvse) {
            throw new IllegalStateException(
                "Failed to login to RepNode", kvse);
        }
    }
}
