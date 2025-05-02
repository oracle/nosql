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

/**
 * An abstract interface for resolving the RoleInstance object according to the
 * name of the role.  It is possible that such information will be fetched from
 * the security metadata of Admin and RepNode, or even from third party
 * authorization products. Such semantics will be deferred to the
 * implementation of this interface.
 */
public interface RoleResolver {

    /**
     * Obtains a RoleInstance object by resolving the role name
     *
     * @param roleName name of role to resolve
     * @return RoleInstance object. May be null if nothing can be found using
     * the roleName
     */
    RoleInstance resolve(String roleName);
}
