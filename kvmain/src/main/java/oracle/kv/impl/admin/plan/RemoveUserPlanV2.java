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

package oracle.kv.impl.admin.plan;

import java.util.List;
import java.util.Set;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.plan.task.NamespacePlanGenerator;
import oracle.kv.impl.admin.plan.task.RemoveNamespace;
import oracle.kv.impl.admin.plan.task.RemoveTablePrivileges;
import oracle.kv.impl.admin.plan.task.RemoveUserV2;
import oracle.kv.impl.admin.plan.task.RevokeNamespacePrivileges;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;

/**
 * Remove user plan supporting cascade option.
 */
class RemoveUserPlanV2 extends MultiMetadataPlan {

    private static final long serialVersionUID = 1L;

    RemoveUserPlanV2(String planName,
                     Planner planner,
                     String userName,
                     boolean cascade) {
        super((planName != null) ? planName : "Remove User", planner);

        final AdminServiceParams params = getAdmin().getParams();
        final SecurityParams securityParams = params.getSecurityParams();

        if (!securityParams.isSecure()) {
            throw new IllegalCommandException("Cannot execute " + planName +
                    " plan. " + planName +
                    " plan requires a secure store to be performed " +
                    "successfully.");
        }

        /*
         * Find and drop tables this user owned.  Note that the tables will be
         * listed such that child tables come before their parent tables so
         * that they are removed in the right order.
         */
        final List<TableImpl> ownedTables = TablePlanGenerator.
            getOwnedTables(getTableMetadata(), getSecurityMetadata(), userName);

        if (!ownedTables.isEmpty()) {
            /* Must specify cascade option if user owns tables */
            if (!cascade) {
                RemoveUserPlan.ownsTableWarning(ownedTables);
            }

            /*
             * Check if current user has DROP_ANY_TABLE and DROP_ANY_INDEX
             * privileges
             */
            final ExecutionContext execCtx = ExecutionContext.getCurrent();
            if (!execCtx.hasPrivilege(SystemPrivilege.DROP_ANY_TABLE) ||
                !execCtx.hasPrivilege(SystemPrivilege.DROP_ANY_INDEX)) {
                throw new ClientAccessException(
                    new UnauthorizedException(
                        "DROP_ANY_TABLE and DROP_ANY_INDEX privileges are " +
                        "required in order to drop user with cascade."));
            }
        }
        for (TableImpl table : ownedTables) {
            TablePlanGenerator.addRemoveIndexTasks(this,
                    table.getInternalNamespace(),
                    table.getFullName(),
                    planner.getAdmin());

            TablePlanGenerator.addRemoveTableTasks(this,
                                                   planner.getAdmin().
                                                           getCurrentTopology(),
                                                   table.getInternalNamespace(),
                                                   table.getFullName(),
                                                   true /*removeChildTables*/);

            /*
             * Find roles having privileges on this table, and remove
             * table privileges from these roles.
             */
            final Set<String> involvedRoles = TablePlanGenerator.
                getInvolvedRoles(table.getId(), getSecurityMetadata());

            for (String role : involvedRoles) {
                addTask(new RemoveTablePrivileges(
                    this, role, TablePrivilege.getAllTablePrivileges(
                        table.getInternalNamespace(), table.getId(),
                        table.getFullName())));
            }
        }

        /* check namespaces */
        Set<TableMetadata.NamespaceImpl> ownedNamespaces =
            NamespacePlanGenerator.getOwnedNamespaces(getTableMetadata(),
                getSecurityMetadata(), userName);

        if (!ownedNamespaces.isEmpty()) {
            /* Must specify cascade option if user owns namespaces */
            if (!cascade) {
                RemoveUserPlan.ownsNamespaceWarning(ownedNamespaces);
            }

            /*
             * Check if current user has DROP_ANY_NAMESPACE privileges
             */
            final ExecutionContext execCtx = ExecutionContext.getCurrent();
            if (!execCtx.hasPrivilege(SystemPrivilege.DROP_ANY_NAMESPACE)) {
                throw new ClientAccessException(
                    new UnauthorizedException(
                        "DROP_ANY_NAMESPACE privilege is " +
                            "required in order to drop user with cascade."));
            }
        }
        for (TableMetadata.NamespaceImpl namespace : ownedNamespaces) {
            addTask(new RemoveNamespace(this, namespace.getNamespace(),
                cascade));

            /*
             * Find roles having privileges on this namespace, and remove
             * namespace privileges from these roles.
             */
            final Set<String> involvedRoles = NamespacePlanGenerator.
                getInvolvedRoles(namespace.getNamespace(),
                                 getSecurityMetadata());

            for (String role : involvedRoles) {
                addTask(new RevokeNamespacePrivileges(this, role,
                    namespace.getNamespace(),
                    NamespacePrivilege.getAllNamespacePrivileges(
                        namespace.getNamespace())));
            }
        }

        addTask(RemoveUserV2.newInstance(this, userName));
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        return SystemPrivilege.sysoperPrivList;
    }

    @Override
    protected Set<MetadataType> getMetadataTypes() {
        return TABLE_SECURITY_TYPES;
    }
}
