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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MultiMetadataPlan;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Remove table privileges from role.
 */
public class RemoveTablePrivileges extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final MultiMetadataPlan multiMetadataPlan;
    private final String roleName;
    private final Set<TablePrivilege> privileges;

    public RemoveTablePrivileges(MultiMetadataPlan plan,
                                 String roleName,
                                 Set<TablePrivilege> privileges) {
        super(null);

        this.multiMetadataPlan = plan;

        /* caller verifies parameters */
        this.roleName = roleName;
        this.privileges = privileges;
    }

    @Override
    protected SecurityMetadata getMetadata() {
        return multiMetadataPlan.getSecurityMetadata();
    }

    @Override
    protected AbstractPlan getPlan() {
        return this.multiMetadataPlan;
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {
        if (secMd.getRole(roleName) != null) {
            final RoleInstance roleCopy = secMd.getRole(roleName).clone();
            secMd.updateRole(roleCopy.getElementId(),
                             roleCopy.revokeTablePrivileges(privileges));
            getPlan().getAdmin().saveMetadata(secMd, txn);
        }
        return secMd;
    }

    @Override
    protected SecurityMetadata getMetadata(Transaction txn) {
        return multiMetadataPlan.getSecurityMetadata(txn);
    }
}
