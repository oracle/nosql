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
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.RemoveUser;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * A new plan for removing users supporting remove their data as well.  For
 * now, if the user being dropped owns any table, an ICE will be thrown to tell
 * users they should drop all the owned tables before dropping the user. In
 * future, an option of CASCADE will be provided to help drop all the owned
 * tables automatically.
 */
public class RemoveUserPlan extends SecurityMetadataPlan {
    private static final long serialVersionUID = 1L;
    private final String userName;

    public RemoveUserPlan(String planName,
                          Planner planner,
                          String userName,
                          boolean cascade) {
        super((planName != null) ? planName : "Remove User", planner);

        final SecurityMetadata secMd = getMetadata();
        this.userName = userName;

        if (secMd != null && secMd.getUser(userName) != null) {
            if (cascade) {
                throw new IllegalCommandException(
                    "The CASCADE option is not yet supported in this version");
            }
            ensureNotOwnsTable();

            /* Remove user */
            addTask(RemoveUser.newInstance(this, userName));
        }
    }

    @Override
    public void preExecuteCheck(boolean force, Logger plannerlogger) {
        super.preExecuteCheck(force, plannerlogger);
        /*
         * Need to check if any owned table before each execution of this plan,
         * since the table metadata may have been changed before execution.
         */
        ensureNotOwnsTable();
    }

    public static void ownsTableWarning(List<TableImpl> ownedTables) {
        final StringBuilder sb = new StringBuilder();
        for (TableImpl table : ownedTables) {
            sb.append(table.getFullName()).append(", ");
        }
        throw new IllegalCommandException(
            "Cannot drop a user that owns tables: " + sb.toString() +
            "please retry after dropping all these tables or drop this" +
            " user with CASCADE option");
    }

    public static void ownsNamespaceWarning(
        Set<TableMetadata.NamespaceImpl> ownedNamespaces) {
        final StringBuilder sb = new StringBuilder();
        for (TableMetadata.NamespaceImpl namespace : ownedNamespaces) {
            sb.append(namespace.getNamespace()).append(", ");
        }
        throw new IllegalCommandException(
            "Cannot drop a user that owns namespaces: " + sb.toString() +
                "please retry after dropping all these namespaces or drop " +
                "this user with CASCADE option");
    }

    private void ensureNotOwnsTable() {
        final TableMetadata tableMd = planner.getAdmin().getMetadata(
            TableMetadata.class, MetadataType.TABLE);
        final SecurityMetadata secMd = planner.getAdmin().getMetadata(
            SecurityMetadata.class, MetadataType.SECURITY);
        final List<TableImpl> ownedTables = TablePlanGenerator.
            getOwnedTables(tableMd, secMd, userName);

        if (!ownedTables.isEmpty()) {
            ownsTableWarning(ownedTables);
        }
    }
}
