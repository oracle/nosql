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

package oracle.kv.impl.admin.plan.task;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * This class contains helper methods regarding namespace plans.
 */
public class NamespacePlanGenerator {

    /**
     * Returns the set of namespaces that userName owns.
     */
    public static Set<TableMetadata.NamespaceImpl> getOwnedNamespaces(
        TableMetadata md, SecurityMetadata secMd, String userName) {

        if (md == null || secMd == null || secMd.getUser(userName) == null) {
            return Collections.emptySet();
        }

        Set<String> namespaces = md.listNamespaces();

        final KVStoreUser user = secMd.getUser(userName);
        final ResourceOwner owner =
            new ResourceOwner(user.getElementId(), user.getName());

        Set<TableMetadata.NamespaceImpl> owned = null;
        for (String ns : namespaces) {
            TableMetadata.NamespaceImpl nsImpl = md.getNamespace(ns);
            if (nsImpl != null && owner.equals(nsImpl.getOwner())) {
                if (owned == null) {
                    owned = new HashSet<TableMetadata.NamespaceImpl>();
                }
                owned.add(nsImpl);
            }
        }

        if (owned == null) {
            return Collections.emptySet();
        }
        return owned;
    }

    /**
     * Returns the roles that have privileges on the specified namespace.
     */
    public static Set<String> getInvolvedRoles(String namespace,
        SecurityMetadata secMd) {

        if (secMd == null) {
            return Collections.emptySet();
        }

        Set<String> involvedRoles = null;
        for (RoleInstance role : secMd.getAllRoles()) {
            for (KVStorePrivilege priv : role.getPrivileges()) {
                if ((priv instanceof NamespacePrivilege) &&
                    NameUtils.namespaceEquals(
                        ((NamespacePrivilege) priv).getNamespace(),
                        namespace) ) {

                    if (involvedRoles == null) {
                        involvedRoles = new HashSet<>();
                    }
                    involvedRoles.add(role.name());
                }
            }
        }

        if (involvedRoles == null) {
            return Collections.emptySet();
        }

        return involvedRoles;
    }
}
