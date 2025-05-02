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

import java.util.Set;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan.RolePlan;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

public class GrantRolesToRole extends UpdateMetadata<SecurityMetadata> {


    private static final long serialVersionUID = 1L;
    private final String grantee;
    private final Set<String> roles;

    public GrantRolesToRole(RolePlan plan,
                            String grantee,
                            Set<String> roles) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();
        if (secMd == null || secMd.getRole(grantee) == null) {
            throw new IllegalCommandException(
                "Role with name " + grantee + " does not exist in store");
        }

        this.grantee = grantee;
        this.roles = roles;

        /* Detect circular role grant */
        for (String roleName : roles) {
            checkCircularGrants(roleName, secMd);
        }
    }

    private void checkCircularGrants(String roleName,
                                     SecurityMetadata secMd) {
        if (roleName.equalsIgnoreCase(grantee)) {
            throw new IllegalCommandException(
                "Could not complete grant, circular role grant detected");
        }
        final RoleInstance role = secMd.getRole(roleName);

        /* no need to check if role is system build-in role */
        if (role == null) {
            return;
        }

        for (String subRole : role.getGrantedRoles()) {
            checkCircularGrants(subRole, secMd);
        }
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {
        /* Return null if role does not exist */
        if (secMd.getRole(grantee) == null) {
            return null;
        }
        final RoleInstance newCopy = secMd.getRole(grantee).clone();
        secMd.updateRole(
            newCopy.getElementId(), newCopy.grantRoles(roles));
        getPlan().getAdmin().saveMetadata(secMd, txn);

        return secMd;
    }

    /**
     * Returns true if this GrantRolesToRole will end up granting the same
     * roles to the same role. Checks that grantee and role set
     * are the same.
     */
    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }

        GrantRolesToRole other = (GrantRolesToRole) t;
        if (!grantee.equalsIgnoreCase(other.grantee)) {
            return false;
        }

        return roles.equals(other.roles);
    }
}
