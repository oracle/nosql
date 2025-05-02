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

import static oracle.kv.impl.admin.plan.task.AddTable.tableMetadataNotFound;
import static oracle.kv.impl.admin.plan.task.EvolveTable.tableDoesNotExist;

import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.FieldDef;
import oracle.kv.impl.admin.IndexAlreadyExistsException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Transaction;

/**
 * Creates a new index.  The index is not visible until it is populated.
 */
public class StartAddIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String indexName;
    /* The tableName is the full table path. */
    private final String tableName;
    private final String namespace;
    private final String[] indexedFields;
    private final String description;
    private final FieldDef.Type[] indexedTypes;
    private final boolean skipNulls;
    private final boolean isUnique;

    /*
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private final long tableId;

    /**
     */
    @SuppressWarnings("unused")
    public StartAddIndex(MetadataPlan<TableMetadata> plan,
                         String namespace,
                         String indexName,
                         String tableName,
                         String[] indexedFields,
                         FieldDef.Type[] indexedTypes,
                         boolean indexNulls,
                         boolean isUnique,
                         String description) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
        this.indexedFields = indexedFields;
        this.indexedTypes = indexedTypes;
        this.skipNulls = !indexNulls;
        this.isUnique = isUnique;
        this.description = description;

        /*
         * Make sure that the table metadata exists and validate that (1) the
         * table exists, 2) the index does not exist and (2) the state passed
         * in can be used for the index.  The simplest way to do the latter is
         * to create a transient copy of the index.
         */
        final TableMetadata md = getMetadata();
        if (md == null) {
            throw tableMetadataNotFound();
        }
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            throw tableDoesNotExist(namespace, tableName);
        }
        if (table.getIndex(indexName) != null) {
            throw indexAlreadyExist(namespace, tableName, indexName);
        }
        tableId = table.getId();
        /*
         * Invoking the constructor will verify the field arguments and also
         * check if there is already an index (with a different name) which has
         * the same fields.
         */
        new IndexImpl(indexName, table,
                      Arrays.asList(indexedFields),
                      (indexedTypes != null ?
                       Arrays.asList(indexedTypes):
                       null),
                      !skipNulls,
                      isUnique,
                      description);
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        /*
         * Note that we only lock the index and not the table. This is to allow
         * other table operations to take place.
         */
        LockUtils.lockIndex(planner, getPlan(),
                            namespace, tableName, indexName);
    }
    
    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        final TableImpl table = md.getTable(namespace, tableName);

        /*
         * The table could have been removed and recreated since the task was
         * created. Note that tableId could be 0 if the task was from a
         * previous version. Skip the check if so.
         */
        if ((table == null) ||                                  /* gone */
            table.isDeleting() ||                               /* going */
            ((tableId != 0) && (tableId != table.getId()))) {   /* returned */
            throw tableDoesNotExist(namespace, tableName);
        }
        final Admin admin = getPlan().getAdmin();
        final List<String> fieldNames = Arrays.asList(indexedFields);
        final List<FieldDef.Type> types =
                    (indexedTypes == null) ? null : Arrays.asList(indexedTypes);

        /* If the index does not exist, add it */
        final IndexImpl existing = (IndexImpl)table.getIndex(indexName);
        if (existing == null) {
            md.addIndex(namespace,
                        indexName,
                        tableName,
                        fieldNames,
                        types,
                        !skipNulls,
                        isUnique,
                        description);
            admin.saveMetadata(md, txn);
            TxnUtil.localCommit(txn, admin.getLogger());
            MDTableUtil.updateTable(table, md, admin);
            return md;
        }
        /* Could be a restart, update the MD system table if needed. */
        MDTableUtil.updateTable(table, md, admin);

        /*
         * Check to see if the existing index is the one we were adding. If
         * so this is a restart and we are done. Otherwise, some other index
         * was created with the same name.
         */
        if (!existing.compareIndexFields(fieldNames) ||
            ((types == null) ? (existing.getTypes() != null) :
                               !types.equals(existing.getTypes()))) {
            throw indexAlreadyExist(namespace, tableName, indexName);
        }
        return md;
    }

    static IndexAlreadyExistsException indexAlreadyExist(String namespace,
                                                         String tableName,
                                                         String indexName) {
        return new IndexAlreadyExistsException("Index " + indexName +
            " already exists in table " +
            NameUtils.makeQualifiedName(namespace, tableName));
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           indexName);
    }

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

        StartAddIndex other = (StartAddIndex) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (tableName.equalsIgnoreCase(other.tableName) &&
                indexName.equalsIgnoreCase(other.indexName) &&
                Arrays.equals(indexedFields, other.indexedFields));
    }
}
