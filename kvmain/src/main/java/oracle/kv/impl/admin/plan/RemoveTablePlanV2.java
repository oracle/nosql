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

import static oracle.kv.impl.api.table.NameUtils.NAMESPACE_SEPARATOR;

import java.util.List;
import java.util.Set;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.ResourceOwner;

/**
 * Remove table plan supporting table privileges automatic cleanup.
 */
public class RemoveTablePlanV2 extends MultiMetadataPlan {

    private static final long serialVersionUID = 1L;

    private final ResourceOwner tableOwner;
    private final long tableId;

    /*
     * This field contains the full namespace name, ie:
     * [tenant@][namespace:][parent_name.]name
     * It would be nice to have it split up but this implies adding a new V3
     * class.
     */
    private final String tableFullName;

    private final boolean toRemoveIndex;

    protected RemoveTablePlanV2(String planName,
                                String tableNamespace,
                                String tableName,
                                Planner planner) {
        super((planName != null) ? planName : "Remove Table", planner);

        final TableImpl table = TablePlanGenerator.
            getAndCheckTable(tableNamespace, tableName, getTableMetadata());

        tableOwner = table.getOwner();
        tableId = table.getId();
        this.tableFullName = table.getFullNamespaceName();
        toRemoveIndex = !table.getIndexes().isEmpty();
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return getFullNameFromFullNsName(tableFullName);
    }

    public String getTableNamespace() {
        return getNamespaceFromFullNsName(tableFullName);
    }

    /**
     * Returns the table name (including the parent name) from the full name.
     * Namespace name: [[tenant@]namespace:][parent_name.]name
     * Namespace: [tenant@]namespace - always contains tenant@ if one is set.
     * Table name: name
     * Full name: [parent_name.]name - contains parent_name if this is a child.
     */
    private String getFullNameFromFullNsName(String tableFullNsName) {
        int nsSeparatorIndex = tableFullNsName.indexOf(NAMESPACE_SEPARATOR);
        if (nsSeparatorIndex == -1) {
            return tableFullNsName;
        }
        return tableFullNsName.substring(nsSeparatorIndex + 1);
    }

    private String getNamespaceFromFullNsName(String tableFullNsName) {
        int nsSeparatorIndex = tableFullNsName.indexOf(NAMESPACE_SEPARATOR);
        if (nsSeparatorIndex == -1) {
            return null;
        }
        return tableFullNsName.substring(0, nsSeparatorIndex);
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        return TablePlanGenerator.
            getRemoveTableRequiredPrivs(tableOwner, getTableNamespace(),
                toRemoveIndex, tableId);
    }

    @Override
    protected Set<MetadataType> getMetadataTypes() {
        return TABLE_SECURITY_TYPES;
    }
}
