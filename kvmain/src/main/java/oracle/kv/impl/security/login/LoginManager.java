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

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;

/**
 * LoginManager defines the interface by which RMI interface APIs acquire
 * LoginTokens for called methods.
 */
public interface LoginManager {

    /**
     * Get the username associated with the LoginManager.
     * @return the associated user name, or null if there is no associated
     * username, as with internal logins.
     */
    String getUsername();

    /**
     * Get a local login appropriate for the specified target.
     *
     * @param target the target host/port being accessed
     * @param rtype the type of resource being accessed
     * @return a LoginHandle appropriate for accessing the specified
     * resource type
     * @throws UnsupportedOperationException if the implementation does not
     * support the specified resource type
     */
    default LoginHandle getHandle(HostPort target, ResourceType rtype) {
        return getHandle(target, rtype, false /* cachedOnly */);
    }

    /**
     * Get a local login appropriate for the specified target and specify
     * whether only a cached value should be returned. If cachedOnly is true,
     * the call will not block and, if nothing is cached, it will return null.
     * Calls to obtain a new login handle may block, so callers may want to
     * make these calls in a different thread.
     *
     * @param target the target host/port being accessed
     * @param rtype the type of resource being accessed
     * @param cachedOnly whether to return null if no value is cached
     * @return a LoginHandle appropriate for accessing the specified
     * resource type or null
     * @throws UnsupportedOperationException if the implementation does not
     * support the specified resource type
     */
    LoginHandle getHandle(HostPort target,
                          ResourceType rtype,
                          boolean cachedOnly);

    /**
     * Get a login appropriate for the specified target resource.
     * Some implementations might not support this method.
     *
     * @throws UnsupportedOperationException if the implementation has no
     *    support for this method
     * @throws IllegalStateException if the implementation has support for
     *    this method, but does not have enough state to resolve resource ids
     */
    default LoginHandle getHandle(ResourceId target) {
        return getHandle(target, false /* cachedOnly */);
    }

    /**
     * Get a login appropriate for the specified target resource, and specify
     * whether only a cached value should be returned. If cachedOnly is true,
     * the call will not block and, if nothing is cached, it will return null.
     * Calls to obtain a new login handle may block, so callers may want to
     * make these calls in a different thread.
     *
     * @throws UnsupportedOperationException if the implementation has no
     *    support for this method
     * @throws IllegalStateException if the implementation has support for
     *    this method, but does not have enough state to resolve resource ids
     */
    LoginHandle getHandle(ResourceId target, boolean cachedOnly);

    /*
     * Log out the user against all known targets
     */
    void logout()
        throws SessionAccessException;

}
