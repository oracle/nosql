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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple role resolver for only those KVStore built-in roles.
 */
public class KVBuiltInRoleResolver implements RoleResolver {

    /* A role map to store all system predefined roles */
    private static final Map<String, RoleInstance> roleMap =
        new HashMap<String, RoleInstance>();

    static {
        roleMap.put(RoleInstance.DBADMIN.name(), RoleInstance.DBADMIN);
        roleMap.put(RoleInstance.SYSADMIN.name(), RoleInstance.SYSADMIN);
        roleMap.put(RoleInstance.INTERNAL.name(), RoleInstance.INTERNAL);
        roleMap.put(RoleInstance.READONLY.name(), RoleInstance.READONLY);
        roleMap.put(RoleInstance.WRITEONLY.name(), RoleInstance.WRITEONLY);
        roleMap.put(RoleInstance.READWRITE.name(), RoleInstance.READWRITE);
        roleMap.put(RoleInstance.PUBLIC.name(), RoleInstance.PUBLIC);
        roleMap.put(RoleInstance.WRITESYSTABLE.name(),
                    RoleInstance.WRITESYSTABLE);

        /* Used for backward compatibility with R3 nodes during upgrade */
        roleMap.put(RoleInstance.ADMIN.name(), RoleInstance.ADMIN);
        roleMap.put(RoleInstance.AUTHENTICATED.name(), RoleInstance.AUTHENTICATED);
    }

    @Override
    public RoleInstance resolve(String roleName) {
        return resolveRole(roleName);
    }

    public static RoleInstance resolveRole(String roleName) {
        return roleMap.get(RoleInstance.getNormalizedName(roleName));
    }

    /* For test purpose */
    Collection<RoleInstance> getAllRoles() {
        return roleMap.values();
    }
}
