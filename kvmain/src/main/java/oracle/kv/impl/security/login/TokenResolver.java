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

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;

/**
 * An abstract interface for determining the capabilities associated with
 * a LoginToken.
 */

public interface TokenResolver {

    /**
     * Attempt to resolve a login token to a Subject.
     * @param token the login token to resolve
     * @return a Subject if token resolution succeeded, or null otherwise.
     * @throws SessionAccessException if an operational failure prevents
     *   access to the session referenced by the token.
     */
    Subject resolve(LoginToken token)
        throws SessionAccessException;
}
