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
import static oracle.kv.impl.admin.plan.task.StartAddIndex.indexAlreadyExist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Transaction;

/**
 * Creates a new index.  The index is not visible until it is populated.
 */
public class StartAddTextIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String indexName;
    private final String tableName;
    private final String namespace;
    private final AnnotatedField[] ftsFields;
    private final Map<String,String> properties;
    private final String description;
    /*
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private final long tableId;

    @SuppressWarnings("unused")
    public StartAddTextIndex(MetadataPlan<TableMetadata> plan,
                             String namespace,
                             String indexName,
                             String tableName,
                             AnnotatedField[] ftsFields,
                             Map<String,String> properties,
                             String description) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
        this.ftsFields = ftsFields;
        this.properties = properties;
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
        final String nsName = NameUtils.makeQualifiedName(namespace, tableName);

        if (table == null) {
            throw tableDoesNotExist(namespace, tableName);
        }
        if (table.getTextIndex(indexName) != null) {
            throw indexAlreadyExist(namespace, tableName, indexName);
        }
        tableId = table.getId();

        List<String> fieldNames = new ArrayList<>(ftsFields.length);
        Map<String,String> annotations = new HashMap<>(ftsFields.length);
        IndexImpl.populateMapFromAnnotatedFields(Arrays.asList(ftsFields),
                                                 fieldNames,
                                                 annotations);
        new IndexImpl(indexName, table, fieldNames, null, true, false,
                      annotations,
                      properties, description);
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

        if ((table == null) || table.isDeleting() ||
            ((tableId != 0) && (tableId != table.getId()))) {
            throw tableDoesNotExist(namespace, tableName);
        }
        final Admin admin = getPlan().getAdmin();

        if (table.getIndex(indexName) == null) {
            md.addTextIndex(namespace,
                            indexName,
                            tableName,
                            Arrays.asList(ftsFields),
                            properties,
                            description);
            getPlan().getAdmin().saveMetadata(md, txn);
            TxnUtil.localCommit(txn, admin.getLogger());
        }
        MDTableUtil.updateTable(table, md, admin);
        return md;
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

        StartAddTextIndex other = (StartAddTextIndex) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (tableName.equalsIgnoreCase(other.tableName) &&
                indexName.equalsIgnoreCase(other.indexName) &&
                Arrays.equals(ftsFields, other.ftsFields));
    }
}
