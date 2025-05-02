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

package oracle.kv.impl.security;

import oracle.kv.impl.security.login.LoginManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A set of login credentials that allow a KVStore component to log in to a
 * KVStore on behalf of a user. These are processed locally on the client
 * side. That is, this class isn't actually sent over the wire. Instead,
 * a ProxyCredentials instance is substituted for the proxyLogin call and the
 * internalManager is used locally to enable the RepNodeLoginManager to have
 * access to server authentication. Serialization of this class should not
 * be relied upon, as the associated LoginManager is not included in the
 * serialized form.
 */
public class ClientProxyCredentials extends ProxyCredentials {

    private static final long serialVersionUID = 1L;

    /*
     * Used on the "client" side of the server-server connection to supply the
     * login token needed to authorize the user of the proxyLogin operation.
     */
    private final transient LoginManager internalManager;

    /**
     * Create a Credentials object that enables proxyLogin operations.
     */
    public ClientProxyCredentials(KVStoreUserPrincipal user,
                                  LoginManager internalManager) {
        super(user);
        this.internalManager = internalManager;
    }

    /** Instances of this class should not be serialized. */
    public ClientProxyCredentials(DataInput in, short sv) throws IOException {
        super(in, sv);
        throw new IllegalStateException(
            "Unexpected attempt to deserialize an instance of" +
            " ClientProxyCredentials");
    }

    /** Instances of this class should not be serialized. */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        throw new IllegalStateException(
            "Unexpected attempt to serialize an instance of" +
            " ClientProxyCredentials");
    }

    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.CLIENT_PROXY;
    }

    /**
     * Returns the internal login manager.
     */
    public LoginManager getInternalManager() {
        return internalManager;
    }
}
