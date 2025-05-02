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
package oracle.kv.impl.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.admin.DdlHandler.DdlOperation;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.AccessCheckUtils.TableContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;

/**
 * This class represents the table Ddl operations and their execution.
 * TableDDLOperations are generated after parsing a table ddl statement.
 */
abstract class TableDdlOperation  implements DdlOperation {

    private final String opName;
    private final TableImpl table;

    TableDdlOperation(String opName, TableImpl table) {
        this.opName = opName;
        this.table = table;
    }

    TableImpl getTable() {
        return table;
    }

    String opName() {
        return opName;
    }

    /**
     * Operation of creating table, needing CREATE_ANY_TABLE privilege.
     */
    public static class CreateTable extends TableDdlOperation {
        private final boolean ifNotExists;
        private final SequenceDef sequenceDef;

        public CreateTable(TableImpl table,
                           TableLimits limits,
                           boolean ifNotExists,
                           SequenceDef sequenceDef) {
            super("CREATE TABLE", table);
            if (limits != null) {
                table.setTableLimits(limits);
            }
            this.ifNotExists = ifNotExists;
            this.sequenceDef = sequenceDef;
        }

        @Override
        public OperationContext getOperationCtx() {
            return new OperationContext() {
                @Override
                public String describe() {
                    final String nsName = getTable().getFullNamespaceName();
                    return opName() + (ifNotExists ? " IF NOT EXISTS " : " ") +
                        nsName;
                }

                @Override
                public List<? extends KVStorePrivilege>
                    getRequiredPrivileges() {

                    return Collections.singletonList(
                        new NamespacePrivilege.CreateTableInNamespace(
                            getTable().getInternalNamespace()));
                }
            };
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final TableImpl table = getTable();
            String exceptionMsg;
            try {
                final Admin admin = ddlHandler.getAdmin();

                /* tableExistsAndEqual will throw if it exists and not equal */
                if (ifNotExists && tableExistsAndEqual(ddlHandler)) {
                    ddlHandler.operationSucceeds();
                    return;
                }

                final int planId = admin.getPlanner().createAddTablePlan
                    ("CreateTable", table, table.getParentName(), false,
                        sequenceDef);
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (TableAlreadyExistsException taee) {
                if (ifNotExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = taee.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            final String nsName = getTable().getFullNamespaceName();
            ddlHandler.operationFails(opName() + " failed for table " +
                                      nsName + ": " + exceptionMsg);
        }

        /**
         * Returns true if the table exists in the current metadata and is the
         * same as the table parameter.
         *
         * If the named table does not exist return false
         * If the named table exists and the definitions match, return true.
         * If the named table exists and the definitions don't match, throw
         * an exception, handled by the caller.
         */
        private boolean tableExistsAndEqual(DdlHandler ddlHandler) {
            final TableImpl table = getTable();
            final TableMetadata md = ddlHandler.getTableMetadata();
            assert md != null;
            final TableImpl existing = md.getTable(table.getInternalNamespace(),
                                                   table.getFullName());
            if (existing != null) {
                if (existing.fieldsEqual(table)) {
                    return true;
                }
                throw new IllegalCommandException
                    ("Table exists but definitions do not match");
            }
            return false;
        }
    }

    /**
     * Operation of evolving table, needing EVOLVE_TABLE privilege.
     */
    public static class EvolveTable extends TableDdlOperation {
        private final TableContext opCtx;
        private final int tableVersion;

        public EvolveTable(TableImpl table, int tableVersion) {
            super("EVOLVE TABLE", table);
            assert(tableVersion > 0);
            this.tableVersion = tableVersion;
            opCtx = new TableContext(opName(), table,
                new TablePrivilege.EvolveTable(table.getId(),
                    table.getInternalNamespace()));
        }

        @Override
        public OperationContext getOperationCtx() {
            return opCtx;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final TableImpl table = getTable();
            try {
                final int planId =
                    ddlHandler.getAdmin().getPlanner().createEvolveTablePlan(
                        "AlterTable", table.getInternalNamespace(),
                        table.getFullName(), tableVersion, table.getFieldMap(),
                        table.getDefaultTTL(), null, false,
                        table.getIdentityColumnInfo(),
                        table.getIdentitySequenceDef(),
                        table.isChild() ? null : table.getRemoteRegions());
                ddlHandler.approveAndExecute(planId);
            } catch (IllegalCommandException ice) {
                ddlHandler.operationFails("ALTER TABLE failed for table " +
                       table.getFullName() + ": " + ice.getMessage());
            }
        }
    }

    /**
     * Operation of dropping table, needing DROP_ANY_TABLE privilege and
     * DROP_INDEX (if to remove data) for non-owners.
     */
    public static class DropTable extends TableDdlOperation {
        private final OperationContext opCtx;
        private final boolean ifExists;
        private final String tableName;
        private final String namespace;

        public DropTable(String namespace,
                         String tableName,
                         TableImpl tableIfExists,
                         boolean ifExists) {
            super("DROP TABLE", tableIfExists);

            this.ifExists = ifExists;
            this.tableName = tableName;
            this.namespace = namespace;

            if (tableIfExists == null) {
                opCtx = new NoTableOpContext(tableName);
            } else {
                final List<KVStorePrivilege> privsToCheck = new ArrayList<>();
                if (!tableIfExists.getIndexes().isEmpty()) {
                    privsToCheck.add(
                        new TablePrivilege.DropIndex(tableIfExists.getId(),
                            namespace));
                }
                privsToCheck.add(
                    new NamespacePrivilege.DropTableInNamespace(namespace));
                opCtx = new TableContext(opName(), tableIfExists,
                                         privsToCheck);
            }
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            String exceptionMsg;
            try {
                final int planId = admin.getPlanner().createRemoveTablePlan
                    ("DropTable", namespace, tableName);
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (TableNotFoundException tnfe) {
                if (ifExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = tnfe.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed for table " +
                                      tableName + ": " + exceptionMsg);
        }

        @Override
        public OperationContext getOperationCtx() {
            return opCtx;
        }
    }

    /**
     * Create an index (normal or full-text). Requires CREATE_INDEX privilege.
     */
    public static class CreateIndex extends TableDdlOperation {
        private final OperationContext opCtx;
        private final boolean ifNotExists;
        private final String indexName;
        private final String tableName;
        private final String namespace;
        private final String[] newFields;
        private final FieldDef.Type[] newTypes;
        private final boolean indexNulls;
        private final boolean isUnique;
        private final AnnotatedField[] annotatedFields;
        private final Map<String, String> properties;
        private final String indexComments;
        private final boolean override;
        private final boolean isFullText;

        public CreateIndex(TableImpl tableIfExists,
                           String namespace,
                           String tableName,
                           String indexName,
                           String[] newFields,
                           FieldDef.Type[] newTypes,
                           boolean indexNulls,
                           boolean isUnique,
                           AnnotatedField[] annotatedFields,
                           Map<String, String> properties,
                           String indexComments,
                           boolean ifNotExists,
                           boolean override) {
            super("CREATE [FULLTEXT] INDEX" +
                  (ifNotExists ? " IF NOT EXISTS" : "") ,
                  tableIfExists);

            this.ifNotExists = ifNotExists;
            this.indexName = indexName;
            this.tableName = tableName;
            this.namespace = namespace;
            this.newFields = newFields;
            this.newTypes = newTypes;
            this.indexNulls = indexNulls;
            this.isUnique = isUnique;
            this.annotatedFields = annotatedFields;
            this.properties = properties;
            this.indexComments = indexComments;
            this.override = override;
            isFullText = (annotatedFields != null);

            assert newFields == null || annotatedFields == null;

            if (tableIfExists == null) {
                opCtx = new NoTableOpContext(tableName);
            } else {
                opCtx = new TableContext(
                    opName(), tableIfExists,
                    new TablePrivilege.CreateIndex(tableIfExists.getId(),
                        namespace));
            }
        }

        @Override
        public OperationContext getOperationCtx() {
            return opCtx;
        }

        /**
         * Returns true if the index in the current tableDdl instance exists
         * and is equal to the fields of the current tableDdl.
         */
        private boolean indexExistsAndEqual(DdlHandler ddlHandler) {
            final TableMetadata md = ddlHandler.getTableMetadata();
            assert md != null;
            final TableImpl idxTable = md.getTable(namespace, tableName);
            if (idxTable != null) {
                IndexImpl index = (IndexImpl) idxTable.getIndex(indexName);
                if (index != null) {
                    if (isFullText) {
                        if (properties == null) {
                            if (!index.getProperties().isEmpty()) {
                                return false;
                            }
                        } else if (!Objects.equals(properties,
                                                   index.getProperties())) {
                            return false;
                        }
                        List<AnnotatedField> fields =
                            index.getFieldsWithAnnotations();
                        return compareAnnotatedFields(fields);
                    }
                    /*
                     * At this time, purposely do not compare the type
                     * arrays, if they exist, when checking index equality.
                     * This prevents creation of 2 indexes with fields like
                     * this, for example:
                     *  (json.a as string) (json.a)
                     * For this test, those will be considered equivalent.
                     */
                    return compareFields(index);
                }
            }
            return false;
        }

        /**
         * Compare our list of regular fields against the given list.
         */
        private boolean compareFields(IndexImpl index) {
            return index.compareIndexFields(Arrays.asList(newFields));
        }

        /**
         * Compare our list of annotated fields against the given list.
         * This method is package-visible for testing purposes.
         */
        boolean compareAnnotatedFields(List<AnnotatedField> fields) {
            if (annotatedFields.length == fields.size()) {
                for (int i = 0; i < annotatedFields.length; i++) {
                    if (!annotatedFields[i].equals(fields.get(i))) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            String exceptionMsg;
            try {
                if (ifNotExists && indexExistsAndEqual(ddlHandler)) {
                    ddlHandler.operationSucceeds();
                    return;
                }

                int planId;
                if (!isFullText) {
                    planId = admin.getPlanner().createAddIndexPlan
                        ("CreateIndex", namespace, indexName,
                         tableName, newFields,
                         newTypes, indexNulls, isUnique, indexComments, false);
                } else {
                    planId = admin.getPlanner().createAddTextIndexPlan
                        ("CreateTextIndex", namespace, indexName, tableName,
                         annotatedFields, properties, indexComments,
                         override);
                }
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (IndexAlreadyExistsException iaee) {
                if (ifNotExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = iaee.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            final String nsName =
                NameUtils.makeQualifiedName(namespace, tableName);
            ddlHandler.operationFails(
                                "CREATE " + (isFullText ? "[FULLTEXT] " : "") +
                                "INDEX failed for table " +
                                nsName + ", index " + indexName + ": " +
                                exceptionMsg);
        }
    }

    /**
     * Operation of dropping index, needing DROP_INDEX privilege.
     */
    public static class DropIndex extends TableDdlOperation {
        private final OperationContext opCtx;
        private final boolean ifExists;
        private final String indexName;
        private final String tableName;
        private final String namespace;
        private final boolean override;

        public DropIndex(String namespace,
                         String tableName,
                         TableImpl tableIfExists,
                         String indexName,
                         boolean ifExists,
                         boolean override) {

            super("DROP INDEX", tableIfExists);
            this.ifExists = ifExists;
            this.indexName = indexName;
            this.tableName = tableName;
            this.namespace = namespace;
            this.override = override;

            if (tableIfExists == null) {
                opCtx = new NoTableOpContext(tableName);
            } else {
                opCtx = new TableContext(opName(), tableIfExists,
                    new TablePrivilege.DropIndex(tableIfExists.getId(),
                        namespace));
            }
        }

        @Override
        public OperationContext getOperationCtx() {
            return opCtx;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            String exceptionMsg;
            try {
                final int planId = admin.getPlanner().createRemoveIndexPlan
                    ("DropIndex", namespace, indexName, tableName, override);
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (IndexNotFoundException infe) {
                if (ifExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = infe.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            final String nsName =
                NameUtils.makeQualifiedName(namespace, tableName);
            ddlHandler.operationFails("DROP INDEX failed for table " + nsName +
                                      ", index " + indexName + ": " +
                                      exceptionMsg);
        }
    }


    /**
     * TODO for show and describe:
     *  o implement tabular output for tables and fields.  This may involve
     *  a new table formatter class or two
     *
     *  o implement index output (both types)
     *  o all indexes?
     *
     * Much of this display formatting should be modularized into formatting
     * interfaces and moved to other locations and perhaps be considered for
     * the public API (e.g. TableFormatter, IndexFormatter, etc).
     */

    /**
     * Operation of showing table or index, needing DBVIEW privilege.
     */
    public static class ShowTableOrIndex extends TableDdlOperation {

        private final String tableName;
        private final String namespace;
        private final boolean isShowTables;
        private final boolean showIndexes;
        private final boolean asJson;

        public ShowTableOrIndex(String namespace,
                                String tableName,
                                boolean isShowTables,
                                boolean showIndexes,
                                boolean asJson) {
            super("ShowTableOrIndex", null /* no need table */);
            this.tableName = tableName;
            this.namespace = namespace;
            this.isShowTables = isShowTables;
            this.showIndexes = showIndexes;
            this.asJson = asJson;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {

            String resultString = null;
            final TableMetadata md = ddlHandler.getTableMetadata();

            /* show tables */
            if (isShowTables) {
                List<String> tableList = md.listTables(namespace);
                resultString = formatList(namespace,
                                          "tables",
                                          tableList,
                                          asJson);
                ddlHandler.operationSucceeds();
            } else {
                /* table or index */
                final String nsName =
                    NameUtils.makeQualifiedName(namespace, tableName);
                TableImpl table = md.getTable(namespace, tableName);
                if (table == null) {
                    ddlHandler.operationFails(
                        "Table does not exist: " + nsName);
                    return;
                }

                ddlHandler.operationSucceeds();
                if (showIndexes) {
                    resultString = formatList
                        (namespace, "indexes",
                         new ArrayList<String>(table.getIndexes().keySet()),
                         asJson);
                } else {
                    resultString = formatTableNames(namespace, table, asJson);
                }
            }
            ddlHandler.setResultString(resultString);
        }

        private static String formatTableNames(String namespace,
                                               TableImpl table,
                                               boolean asJson) {
            Table current = table;
            while (current.getParent() != null) {
                current = current.getParent();
            }
            List<String> tableNames = new ArrayList<String>();
            listTableHierarchy(current, tableNames);
            return formatList(namespace, "tableHierarchy" , tableNames, asJson);
        }

        private static void listTableHierarchy(Table table,
                                               List<String> tableNames) {
            tableNames.add(table.getFullName());
            for (Table t : table.getChildTables().values()) {
                listTableHierarchy(t, tableNames);
            }
        }

        /**
         * Formats the list of tables.  If asJson is true a JSON output format
         * is used, otherwise it is a CRLF separated string of names.
         * JSON:  {"tables" : ["t1", "t2", ..., "tN"]}
         */
        private static String formatList(String namespace,
                                         String listName,
                                         List<String> list,
                                         boolean asJson) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            if (asJson) {
                sb.append("{");
                if (namespace != null) {
                    sb.append("\"namespace\": ").append("\"").append(namespace)
                        .append("\"\n");
                }
                sb.append("\"").append(listName).append("\"");
                sb.append(" : [");
                for (String s : list) {
                    if (!first) {
                        sb.append(",");
                    }
                    first = false;
                    sb.append("\"");
                    sb.append(s);
                    sb.append("\"");
                }
                sb.append("]}");
            } else {
                sb.append(listName);
                if (namespace != null) {
                    sb.append("(namespace ").append(namespace).append(")");
                }
                for (String s : list) {
                    /*
                     * Indent list members by 2 spaces.
                     */
                    sb.append("\n  ");
                    sb.append(s);
                }
            }
            return sb.toString();
        }

        @Override
        public OperationContext getOperationCtx() {
            return new OperationContext() {
                @Override
                public String describe() {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("SHOW");
                    if (asJson) {
                        sb.append(" AS JSON");
                    }
                    if (isShowTables) {
                        sb.append(" TABLES");
                    } else if (showIndexes) {
                        sb.append( " INDEXES ON ");
                        sb.append(tableName);
                    } else {
                        sb.append(" TABLE ");
                        sb.append(tableName);
                    }
                    return sb.toString();
                }
                @Override
                public List<? extends KVStorePrivilege>
                    getRequiredPrivileges() {
                        return SystemPrivilege.dbviewPrivList;
                }
            };
        }
    }

    /**
     * Operation of describing table, needing DBVIEW privilege.
     */
    public static class DescribeTable extends TableDdlOperation {
        private final String tableName;
        private final String namespace;
        private final String indexName;
        private final boolean asJson;
        private final List<List<String>> fieldPaths;

        public DescribeTable(String namespace,
                             String tableName,
                             String indexName,
                             List<List<String>> fieldPaths,
                             boolean asJson) {
            super("DESCRIBE TABLE", null /* table */);

            assert tableName != null;
            this.asJson = asJson;
            this.namespace = namespace;
            this.tableName = tableName;
            this.indexName = indexName;
            this.fieldPaths = fieldPaths;
        }

        /**
         * TODO:
         *  o implement tabular output for tables and fields.  This may involve
         *  a new table formatter class or two
         *
         * Much of this display formatting should be modularized into formatting
         * interfaces and moved to other locations and perhaps be considered for
         * the public API (e.g. TableFormatter, IndexFormatter, etc).
         */

        @Override
        public void perform(DdlHandler ddlHandler) {

            String resultString = null;

            final TableMetadata md = ddlHandler.getTableMetadata();
            final String nsName = NameUtils.makeQualifiedName(namespace,
                tableName);
            TableImpl table = md.getTable(namespace, tableName);
            if (table == null) {
                ddlHandler.operationFails("Table does not exist: " + nsName);
                return;
            }
            if (indexName != null) {
                Index index = table.getIndex(indexName);
                if (index == null) {
                    ddlHandler.operationFails(
                        "Index does not exist: " + indexName + ", on table " +
                        nsName);
                    return;
                }
                resultString = formatIndex((IndexImpl)index, asJson);
            } else {

                if (asJson && fieldPaths == null) {
                    if (table.hasIdentityColumn()) {
                        /* get sequence definition from SYS$SGAttributesTable */
                        TableAPIImpl api = (TableAPIImpl)ddlHandler
                                              .getAdmin()
                                              .getInternalKVStore()
                                              .getTableAPI();
                        SequenceDef def = api.getSequenceDef(table);
                        table.setIdentitySequenceDef(def);
                    }
                }
                /*
                 * formatTable can throw IAE.
                 */
                try {
                    resultString = table.formatTable(asJson, fieldPaths,
                                                     md.getRegionMapper());
                    if (resultString == null) {
                        return;
                    }
                } catch (IllegalArgumentException iae) {
                    ddlHandler.operationFails(iae.getMessage());
                    return;
                }
            }
            ddlHandler.setResultString(resultString);
        }

        private static String formatIndex(IndexImpl index,
                                          boolean asJson1) {
            return index.formatIndex(asJson1);
        }

        @Override
        public OperationContext getOperationCtx() {
            return new OperationContext() {
                @Override
                public String describe() {
                    return opName() + ": " + tableName;
                }
                @Override
                public List<? extends KVStorePrivilege>
                    getRequiredPrivileges() {
                        return SystemPrivilege.dbviewPrivList;
                }
            };
        }
    }

    /**
     * A special context for the case where the table does not exist, but we
     * need to do some check first, e.g., in the case with "IF EXISTS" option.
     */
    private static final class NoTableOpContext
        implements OperationContext {
        private final String phantomTable;
        NoTableOpContext(String phantomTable) {
            this.phantomTable = phantomTable;
        }
        @Override
        public String describe() {
            return "Operation on an non-existing table: " + phantomTable;
        }
        @Override
        public List<? extends KVStorePrivilege>
            getRequiredPrivileges() {
                return SystemPrivilege.dbviewPrivList;
        }
    }
}
