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

import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import oracle.kv.impl.api.table.NameUtils;

public abstract class TablePrivilege extends KVStorePrivilege {

    private static final long serialVersionUID = 1L;

    /**
     * Namespace of the specified table, used for display only.
     */
    private final String tableNamespace;

    /**
     * Name of the specified table, used for display only.
     */
    private final String tableName;

    /**
     * Unique ID of the specified table, used together with privilege label
     * to distinguish one table privilege instance from another.
     */
    private final long tableId;

    /**
     * Table privilege creator.
     */
    interface CreatePrivilege {
        TablePrivilege createPrivilege(long tableId,
            String tableNamespace, String tableName);
    }

    /*
     * A convenient map of table privilege creators.
     */
    private static final EnumMap<KVStorePrivilegeLabel, CreatePrivilege>
    tablePrivCreateMap = new EnumMap<KVStorePrivilegeLabel, CreatePrivilege>(
        KVStorePrivilegeLabel.class);

    static {
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.DELETE_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new DeleteTable(id, tableNamespace, tableName);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.READ_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new ReadTable(id, tableNamespace, tableName);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.INSERT_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new InsertTable(id, tableNamespace, tableName);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.EVOLVE_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new EvolveTable(id, tableNamespace, tableName);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.CREATE_INDEX,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new CreateIndex(id, tableNamespace, tableName);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.DROP_INDEX,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id,
                    String tableNamespace, String tableName) {
                    return new DropIndex(id, tableNamespace, tableName);
                }
            });
    }

    /*
     * A convenient map of table privilege label and the implying namespace
     * privilege label.
     */
    private static final EnumMap<KVStorePrivilegeLabel, KVStorePrivilegeLabel>
        tableNsPrivsMap = new EnumMap<>(KVStorePrivilegeLabel.class);
    static {
        tableNsPrivsMap.put(KVStorePrivilegeLabel.DELETE_TABLE,
                            KVStorePrivilegeLabel.DELETE_IN_NAMESPACE);
        tableNsPrivsMap.put(KVStorePrivilegeLabel.READ_TABLE,
                            KVStorePrivilegeLabel.READ_IN_NAMESPACE);
        tableNsPrivsMap.put(KVStorePrivilegeLabel.INSERT_TABLE,
                            KVStorePrivilegeLabel.INSERT_IN_NAMESPACE);
        tableNsPrivsMap.put(KVStorePrivilegeLabel.EVOLVE_TABLE,
                            KVStorePrivilegeLabel.EVOLVE_TABLE_IN_NAMESPACE);
        tableNsPrivsMap.put(KVStorePrivilegeLabel.CREATE_INDEX,
                            KVStorePrivilegeLabel.CREATE_INDEX_IN_NAMESPACE);
        tableNsPrivsMap.put(KVStorePrivilegeLabel.DROP_INDEX,
                            KVStorePrivilegeLabel.DROP_INDEX_IN_NAMESPACE);
    }

    /**
     * Get implying namespace privilege label of given table privilege label.
     */
    public static KVStorePrivilegeLabel
        implyingNamespacePrivLabel(KVStorePrivilegeLabel tablePrivLabel) {
        final KVStorePrivilegeLabel nsPrivLabel =
            tableNsPrivsMap.get(tablePrivLabel);
        if (nsPrivLabel == null) {
            throw new IllegalStateException(
                "Privilege implication code error, " + tablePrivLabel +
                " doesn't have an implying namespace privilege label defined");
        }
        return nsPrivLabel;
    }

    private TablePrivilege(KVStorePrivilegeLabel privLabel,
                           long tableId,
                           String tableNamespace,
                           String tableName) {
        super(privLabel);

        this.tableId = tableId;
        this.tableNamespace = tableNamespace;
        this.tableName = tableName;
    }

    private TablePrivilege(KVStorePrivilegeLabel privLabel,
                           long tableId) {
        this(privLabel, tableId, null /*tableNamespace*/, null /*tableName*/);
    }

    static TablePrivilege readTablePrivilege(DataInput in,
                                             short serialVersion,
                                             KVStorePrivilegeLabel privLabel)
        throws IOException
    {
        final long tableId = readPackedLong(in);
        final String tableNamespace = readString(in, serialVersion);
        final String tableName = readString(in, serialVersion);
        return get(privLabel, tableId, tableNamespace, tableName);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writePackedLong(out, tableId);
        writeString(out, serialVersion, tableNamespace);
        writeString(out, serialVersion, tableName);
    }

    /**
     * Gets a specific table privilege instance according to the specific
     * label and table information.  It is used in the case that builds a table
     * privilege instance according to user-input privilege name. In other
     * cases, it is recommended to directly get the instances via constructors
     * for efficiency.
     *
     * @param privLabel label of the privilege
     * @param tableId table id
     * @param tableNamespace table namespace
     * @param tableName table name
     * @return table privilege instance specified by the label
     */
    public static TablePrivilege get(KVStorePrivilegeLabel privLabel,
                                     long tableId,
                                     String tableNamespace,
                                     String tableName) {
        if (privLabel.getType() != PrivilegeType.TABLE) {
            throw new IllegalArgumentException(
                "Could not obtain a table privilege with a non-table " +
                "privilege label " + privLabel);
        }

        final CreatePrivilege creator = tablePrivCreateMap.get(privLabel);
        if (creator == null) {
            throw new IllegalArgumentException(
                "Could not find a table privilege with label of " + privLabel);
        }
        return creator.createPrivilege(tableId, tableNamespace, tableName);
    }

    /**
     * Return all table privileges on table with given name and id.
     */
    public static Set<TablePrivilege>
        getAllTablePrivileges(String tableNamespace, long tableId,
            String tableName) {

        final Set<TablePrivilege> tablePrivs = new HashSet<TablePrivilege>();
        for (KVStorePrivilegeLabel privLabel : tablePrivCreateMap.keySet()) {
            tablePrivs.add(get(privLabel, tableId, tableNamespace, tableName));
        }
        return tablePrivs;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        final TablePrivilege otherTablePriv = (TablePrivilege) other;
        return tableId == otherTablePriv.tableId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + super.hashCode();
        result = result * prime + (int) (tableId ^ (tableId >>> 32));
        return result;
    }

    /**
     * A table privilege appears in the form of PRIV_NAME(TABLE_NAME)
     */
    @Override
    public String toString() {
        return String.format("%s(%s)", getLabel(),
            NameUtils.makeQualifiedName(tableNamespace, tableName));
    }

    public final static class ReadTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.READ_ANY,
                                     SystemPrivilege.READ_ANY_TABLE };

        public ReadTable(long tableId,
            String tableNamespace, String tableName) {
            super(KVStorePrivilegeLabel.READ_TABLE, tableId,
                tableNamespace, tableName);
        }

        public ReadTable(long tableId) {
            super(KVStorePrivilegeLabel.READ_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class InsertTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.INSERT_ANY_TABLE };

        public InsertTable(long tableId,
            String tableNamespace, String tableName) {
            super(KVStorePrivilegeLabel.INSERT_TABLE, tableId,
                tableNamespace, tableName);
        }

        public InsertTable(long tableId) {
            super(KVStorePrivilegeLabel.INSERT_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DeleteTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.DELETE_ANY_TABLE };

        public DeleteTable(long tableId,
            String tableNamespace, String tableName) {
            super(KVStorePrivilegeLabel.DELETE_TABLE, tableId,
                tableNamespace, tableName);
        }

        public DeleteTable(long tableId) {
            super(KVStorePrivilegeLabel.DELETE_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class EvolveTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private final KVStorePrivilege[] implyingPrivs;

        public EvolveTable(long tableId,
            String tableNamespace, String tableName) {
            super(KVStorePrivilegeLabel.EVOLVE_TABLE, tableId,
                tableNamespace, tableName);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.EVOLVE_ANY_TABLE,
                new NamespacePrivilege.EvolveTableInNamespace(tableNamespace)
            };
        }

        public EvolveTable(long tableId, String tableNamespace) {
            super(KVStorePrivilegeLabel.EVOLVE_TABLE, tableId);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.EVOLVE_ANY_TABLE,
                new NamespacePrivilege.EvolveTableInNamespace(tableNamespace)
            };
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class CreateIndex extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private final KVStorePrivilege[] implyingPrivs;

        public CreateIndex(long tableId,
            String tableNamespace, String tableName) {
            super(KVStorePrivilegeLabel.CREATE_INDEX, tableId,
                tableNamespace, tableName);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.CREATE_ANY_INDEX,
                new NamespacePrivilege.CreateIndexInNamespace(tableNamespace)
            };
        }

        public CreateIndex(long tableId, String tableNamespace) {
            super(KVStorePrivilegeLabel.CREATE_INDEX, tableId);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.CREATE_ANY_INDEX,
                new NamespacePrivilege.CreateIndexInNamespace(tableNamespace)
            };
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DropIndex extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private final KVStorePrivilege[] implyingPrivs;

        public DropIndex(long tableId,
            String tableNamespace, String tableName){
            super(KVStorePrivilegeLabel.DROP_INDEX, tableId,
                tableNamespace, tableName);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.DROP_ANY_INDEX,
                new NamespacePrivilege.DropIndexInNamespace(tableNamespace)
            };
        }

        public DropIndex(long tableId, String tableNamespace) {
            super(KVStorePrivilegeLabel.DROP_INDEX, tableId);
            implyingPrivs = new KVStorePrivilege[] {
                SystemPrivilege.SYSDBA,
                SystemPrivilege.DROP_ANY_INDEX,
                new NamespacePrivilege.DropIndexInNamespace(tableNamespace)
            };
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }
}
