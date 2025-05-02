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

import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.ResourceId.ResourceType;

/**
 * LoginHandle defines the interface by which RMI interface APIs acquire
 * LoginTokens for called methods.
 */
public abstract class LoginHandle {
    private final AtomicReference<LoginToken> loginToken;

    protected LoginHandle(LoginToken loginToken) {
        this.loginToken = new AtomicReference<LoginToken>(loginToken);
    }

    /**
     * Get the current LoginToken value.
     */
    public LoginToken getLoginToken() {
        return loginToken.get();
    }

    /**
     * Attempt to update the LoginToken to be a new value.
     * If the current token is not the same identity as the old token,
     * the update is not performed.
     * @return true if the update was performed.
     */
    protected boolean updateLoginToken(LoginToken oldToken,
                                       LoginToken newToken) {
        return loginToken.compareAndSet(oldToken, newToken);
    }

    /**
     * Attempt to renew the token to a later expiration time.
     * Returns null if unable to renew.
     */
    public abstract LoginToken renewToken(LoginToken currToken)
        throws SessionAccessException;

    /**
     * Logout the session associated with the login token.
     */
    public abstract void logoutToken()
        throws SessionAccessException;

    /**
     * Report whether this login handle supports authentication to the
     * specified resouce type.
     */
    public abstract boolean isUsable(ResourceType rtype);
}
