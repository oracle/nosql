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

import static oracle.kv.impl.api.table.NameUtils.SYSTEM_NAMESPACE_PREFIX;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.table.TableAPI.SYSDEFAULT_NAMESPACE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NamespaceNotFoundException;
import oracle.kv.impl.admin.TableNotFoundException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.AddIndexPlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.AddNamespacePlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.AddTablePlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.EvolveTablePlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.RegionPlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.RemoveIndexPlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.RemoveNamespacePlan;
import oracle.kv.impl.admin.plan.DeployTableMetadataPlan.SetTableLimitPlan;
import oracle.kv.impl.admin.plan.task.AddNamespace;
import oracle.kv.impl.admin.plan.task.AddTable;
import oracle.kv.impl.admin.plan.task.CompleteAddIndex;
import oracle.kv.impl.admin.plan.task.EvolveTable;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RegionTask.CreateRegion;
import oracle.kv.impl.admin.plan.task.RegionTask.DropRegion;
import oracle.kv.impl.admin.plan.task.RegionTask.SetLocalRegionName;
import oracle.kv.impl.admin.plan.task.RemoveIndex;
import oracle.kv.impl.admin.plan.task.RemoveIndexV2;
import oracle.kv.impl.admin.plan.task.RemoveNamespace;
import oracle.kv.impl.admin.plan.task.RemoveTablePrivileges;
import oracle.kv.impl.admin.plan.task.RemoveTableV2;
import oracle.kv.impl.admin.plan.task.SetTableLimits;
import oracle.kv.impl.admin.plan.task.StartAddIndex;
import oracle.kv.impl.admin.plan.task.StartAddTextIndex;
import oracle.kv.impl.admin.plan.task.UpdateMetadata;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.plan.task.WaitForAddIndex;
import oracle.kv.impl.admin.plan.task.WaitForRemoveTableData;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.IdentityColumnInfo;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.tif.ElasticsearchHandler;
import oracle.kv.impl.tif.TextIndexFeeder;
import oracle.kv.impl.tif.esclient.restClient.ESRestClient;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;


/**
 * Static utility class for generating plans for secondary indexes.
 *
 * Exception handling note.  This code runs in the context of the admin service
 * and the rule in the admin is that non-fatal runtime exceptions are thrown as
 * IllegalCommandException.  For that reason these methods catch and rethrow
 * exceptions from called methods.
 */
public class TablePlanGenerator {

    /* Prevent construction */
    private TablePlanGenerator() {}

    /**
     * Creates a plan to add a table.
     */
    static DeployTableMetadataPlan
        createAddTablePlan(String planName,
                           Planner planner,
                           TableImpl table,
                           String parentName,
                           boolean systemTable,
                           SequenceDef sequenceDef) {

        checkStoreVersion(planner.getAdmin(),
                SerialVersion.getKVVersion(table.getRequiredSerialVersion()),
                table.isMultiRegion());

        String tableName = table.getFullName();
        String namespace = table.getInternalNamespace();
        if (namespace != null) {
            TableImpl.validateNamespace(namespace);
        }

        final String fullPlanName = makeName(planName, namespace,
                                             tableName, null);
        final DeployTableMetadataPlan plan =
                new AddTablePlan(fullPlanName, planner, namespace,
                                 systemTable /* systemTable == systemPlan */);

        try {
            plan.addTask(new AddTable(plan,
                                      table,
                                      parentName,
                                      sequenceDef));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add table: " + iae.getMessage(), iae);
        }
        return plan;
    }

    /**
     * Check if the store supports the required version. If it does not
     * an IllegalCommandException is thrown.
     */
    public static void checkStoreVersion(Admin admin,
                                         KVVersion requiredVersion) {
         checkStoreVersion(admin, requiredVersion, false /*isMRTable*/);
    }

    /**
     * Check if the store supports the required version. If it does not
     * an IllegalCommandException is thrown. If isMRTable is true additional
     * checks are made whether the store is running Enterprise Edition and
     * that the MR agent supports the required version.
     */
    private static void checkStoreVersion(Admin admin,
                                          KVVersion requiredVersion,
                                          boolean isMRTable) {
        final KVVersion storeVersion = admin.getStoreVersion();
        if (storeVersion.compareTo(requiredVersion) < 0 ) {
            throw new IllegalCommandException("Store version is not capable " +
                "of executing plan. Required version is " +
                requiredVersion.getNumericVersionString() +
                ", store version is " +
                storeVersion.getNumericVersionString());
        }

        if (!isMRTable) {
            return;
        }

        if (!VersionUtil.isEnterpriseEdition(storeVersion)) {
            throw new IllegalCommandException("Operation is only supported" +
                                              " by Enterprise Edition. The " +
                                              " store version is " +
                                              storeVersion.getVersionString());
        }

        /*
         * Make sure the agent version supports the table.
         */
        final KVVersion maxVersion =
                            admin.getMRTServiceManager().getMaxVersion();
        if (maxVersion == null) {
            /*
             * null is returned if the MR request/response tables are not
             * created, or there is an error accessing them.
             */
            throw new IllegalCommandException("Cannot execute plan," +
                                              " multi-region error or" +
                                              " not ready");
        }
        if (VersionUtil.compareMinorVersion(maxVersion, requiredVersion) < 0) {
            throw new IllegalCommandException("Multi-region agent version is" +
                " not capable of executing plan. Required version is " +
                requiredVersion.getNumericVersionString() +
                ", multi-region agent version is " +
                maxVersion.getNumericVersionString());
        }
    }

    /**
     * Creates a plan to evolve a table.
     *
     * The table version is the version of the table used as a basis for this
     * evolution.  It is used to verify that only the latest version of a table
     * is evolved.
     */
    static DeployTableMetadataPlan
        createEvolveTablePlan(String planName,
                              Planner planner,
                              String namespace,
                              String tableName,
                              int tableVersion,
                              FieldMap newFieldMap,
                              TimeToLive ttl,
                              TimeToLive beforeImgTTL,
                              String description,
                              boolean systemTable,
                              IdentityColumnInfo newIdentityColumnInfo,
                              SequenceDef sequenceDefChange,
                              Set<Integer> regions) {

        checkTableName(tableName);
        if (newFieldMap == null || newFieldMap.isEmpty()) {
            throw new IllegalCommandException("Fields cannot be null or empty");
        }

        checkStoreVersion(planner.getAdmin(),
            SerialVersion.getKVVersion(newFieldMap.getRequiredSerialVersion()),
            regions != null);

        final String fullPlanName = makeName(planName, namespace,
                                             tableName, null);
        final DeployTableMetadataPlan plan;
        plan = new EvolveTablePlan(fullPlanName,
                                   namespace, tableName, planner,
                                   /* systemTable == systemPlan */
                                   systemTable);

        tableName = getRealTableName(namespace, tableName, plan.getMetadata());
        try {
            plan.addTask(new EvolveTable(plan,
                                         namespace,
                                         tableName,
                                         tableVersion,
                                         newFieldMap,
                                         ttl,
                                         beforeImgTTL,
                                         description,
                                         systemTable,
                                         newIdentityColumnInfo,
                                         sequenceDefChange,
                                         regions));

        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to evolve table: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to remove a table.
     */
    static AbstractPlan createRemoveTablePlan(String planName,
                                              Planner planner,
                                              Topology topology,
                                              String namespace,
                                              String tableName) {
        checkTableName(tableName);

        final String fullPlanName = makeName(planName, namespace,
                                             tableName, null);
        final RemoveTablePlanV2 plan =
                new RemoveTablePlanV2(fullPlanName, namespace,
                                      tableName, planner);
        addRemoveTablePrivsTasks(plan);

        final Admin admin = planner.getAdmin();
        tableName = getRealTableName(namespace, tableName,
                                     admin.getTableMetadata());

        /*
         * To remove data, we first mark the table for deletion and
         * broadcast that change. This will trigger the RNs to remove the
         * table data from it's respective shard. The plan will wait for all
         * RNs to finish. Once the data is deleted, the table object can be
         * removed.
         */
        try {
            addRemoveIndexTasks(plan, namespace, tableName, admin);
            addRemoveTableTasks(plan, topology, namespace, tableName,
                               false /*removeChildTables*/);
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to remove table: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to set limits on a table.
     */
    static DeployTableMetadataPlan
        createSetTableLimitPlan(String planName,
                                Planner planner,
                                String namespace,
                                String tableName,
                                TableLimits newLimits) {
        checkTableName(tableName);

        final String fullPlanName = makeName(planName,
                                             namespace, tableName, null);
        final DeployTableMetadataPlan plan =
                            new SetTableLimitPlan(fullPlanName,
                                                  namespace, tableName,
                                                  planner);
        final String realTableName =
                getRealTableName(namespace, tableName, plan.getMetadata());
        try {
            plan.addTask(new SetTableLimits(plan,
                                            namespace, realTableName,
                                            newLimits));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to set table limits: " + iae.getMessage(), iae);
        }
        return plan;
    }

    /**
     * Add a task to remove each index defined on the table.  Do this before
     * removing data as indexes are affected and performance would be quite bad
     * otherwise.
     */
    static void addRemoveIndexTasks(final AbstractPlan plan,
                                    final String namespace,
                                    final String tableName,
                                    final Admin admin) {
        final TableMetadata md = admin.getTableMetadata();
        if (md != null) {
            final boolean cascade = plan instanceof RemoveUserPlanV2 ||
                    plan instanceof RemoveNamespacePlan;
            final TableImpl table = md.checkForRemove(namespace, tableName,
                                                      true, /*indexes allowed*/
                                                      /*child tables allowed*/
                                                      cascade);
            for (String indexName : table.getIndexes().keySet()) {
                if (VersionUtil.getJavaMajorVersion() >= 11) {
                    checkTextIndexForRemoval(plan, admin, namespace, indexName,
                            tableName, false);
                }

                try {
                    if (plan instanceof RemoveTablePlanV2 || cascade) {
                        plan.addTask(RemoveIndexV2.newInstance(
                                         (MultiMetadataPlan)plan,
                                                        namespace,
                                                        indexName,
                                                        tableName));
                    } else {
                        plan.addTask(RemoveIndex.newInstance(
                                         (DeployTableMetadataPlan )plan,
                                                 namespace,
                                                 indexName,
                                                 tableName));
                    }
                } catch (IllegalArgumentException iae) {
                    throw new IllegalCommandException
                        ("Failed to remove index: " + iae.getMessage(), iae);
                }
            }
        }
    }

    static private void addRemoveTablePrivsTasks(final RemoveTablePlanV2 plan) {
        final Set<String> involvedRoles =
            getInvolvedRoles(plan.getTableId(), plan.getSecurityMetadata());

        for (String role : involvedRoles) {
            plan.addTask(new RemoveTablePrivileges(
                plan, role, TablePrivilege.getAllTablePrivileges(
                    plan.getTableNamespace(), plan.getTableId(),
                    plan.getTableName() )));
        }
    }

    /**
     * Adds the necessary tasks to remove the specified table.
     */
    static void addRemoveTableTasks(final MultiMetadataPlan plan,
                                    final Topology topology,
                                    final String namespace,
                                    final String tableName,
                                    final boolean removeChildTables) {

        /* 1. Mark the table for deletion */
        plan.addTask(RemoveTableV2.newInstance(plan, namespace, tableName,
                                               removeChildTables,
                                               true /*markForDelete*/));


        /* 2. Wait for the shards to remove the table data */
        final ParallelBundle bundle = new ParallelBundle();
        for (RepGroupId id : topology.getRepGroupIds()) {
            bundle.addTask(new WaitForRemoveTableData(plan,
                                                      id,
                                                      namespace,
                                                      tableName));
        }
        plan.addTask(bundle);

        /* 3. Remove the table from the metadata */
        plan.addTask(RemoveTableV2.newInstance(plan, namespace, tableName,
                                               removeChildTables,
                                               false /*markForDelete*/));
    }

    /**
     * Creates a plan to add an index.
     * This operates in 3 parts
     * 1.  Update metadata to include the new index, which is in state
     *     "Populating". In that state it will be populated and used on
     *      RepNodes but will not appear to users in metadata.
     * 2.  Ask all RepNode masters to populate the index in a parallel bundle
     * 3.  Update the metadata again with the state "Ready" on the index to make
     *     it visible to users.
     */
    static DeployTableMetadataPlan createAddIndexPlan(String planName,
                                                      Planner planner,
                                                      Topology topology,
                                                      String namespace,
                                                      String indexName,
                                                      String tableName,
                                                      String[] indexedFields,
                                                      FieldDef.Type[] indexedTypes,
                                                      boolean indexNulls,
                                                      boolean isUnique,
                                                      String description,
                                                      boolean systemTable) {
        checkTableName(tableName);
        checkIndexName(indexName);

        /* Only internal callers are allowed to add an index to a system table */
        if (!systemTable &&
            tableName.startsWith(TableImpl.SYSTEM_TABLE_PREFIX)) {
            throw new IllegalCommandException
                    ("Cannot add index " + indexName + " on system table: " +
                     NameUtils.makeQualifiedName(namespace, null, tableName));

        }
        if (indexedFields == null) {    // TODO - check for empty?
            throw new IllegalCommandException("Indexed fields cannot be null");
        }

        /* Introduction of index ids requires all nodes in store to be in
           QUERY_VERSION_16 */
        checkStoreVersion(planner.getAdmin(),
                          SerialVersion.getKVVersion(QUERY_VERSION_16));

        final String fullPlanName = makeName(planName, namespace,
                                             tableName, indexName);
        final DeployTableMetadataPlan plan =
                new AddIndexPlan(fullPlanName, namespace, tableName,
                                 planner, systemTable);

        tableName = getRealTableName(namespace, tableName, plan.getMetadata());

        /*
         * Create the index, not-yet-visible
         */
        try {
            plan.addTask(new StartAddIndex(plan,
                                           namespace,
                                           indexName,
                                           tableName,
                                           indexedFields,
                                           indexedTypes,
                                           indexNulls,
                                           isUnique,
                                           description));

            /*
             * Wait for the added index to be populated. This may take a while.
             */
            final ParallelBundle bundle = new ParallelBundle();
            for (RepGroupId id : topology.getRepGroupIds()) {
                bundle.addTask(new WaitForAddIndex(plan,
                                                   id,
                                                   namespace,
                                                   indexName,
                                                   tableName));
            }
            plan.addTask(bundle);

            /*
             * Complete the job, make the index visible
             */
            plan.addTask(new CompleteAddIndex(plan,
                                              namespace,
                                              indexName,
                                              tableName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add index: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to remove an index.
     * @param override
     */
    @SuppressWarnings("unused")
    static DeployTableMetadataPlan
        createRemoveIndexPlan(String planName,
                              Planner planner,
                              Topology topology,
                              String namespace,
                              String indexName,
                              String tableName,
                              boolean override) {
        checkTableName(tableName);
        checkIndexName(indexName);

        final String fullPlanName = makeName(planName, namespace,
                                             tableName, indexName);
        final DeployTableMetadataPlan plan =
               new RemoveIndexPlan(fullPlanName, namespace, tableName, planner);

        checkTextIndexForRemoval(plan, planner.getAdmin(), namespace,
                                 indexName, tableName, override);

        tableName = getRealTableName(namespace, tableName, plan.getMetadata());
        try {
            plan.addTask(RemoveIndex.newInstance(plan, namespace,
                                         indexName, tableName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to remove index: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to add a text index.
     * @param override
     */
    static DeployTableMetadataPlan
        createAddTextIndexPlan(String planName,
                               Planner planner,
                               String namespace,
                               String indexName,
                               String tableName,
                               AnnotatedField[] ftsFields,
                               Map<String,String> properties,
                               String description,
                               boolean override) {
        if (VersionUtil.getJavaMajorVersion() < 11) {
            throw new IllegalCommandException
                ("Can not add TextIndex, as it requires Java 11 or later,"
                 + " but running Java version " +
                 System.getProperty("java.version"));
        }
        checkTableName(tableName);
        checkIndexName(indexName);
        if (ftsFields == null || ftsFields.length == 0) {
            throw new IllegalCommandException
                ("The set of text-indexed fields cannot be null or empty");
        }

        final Admin admin = planner.getAdmin();
        final Parameters p = admin.getCurrentParameters();

        ParameterMap pm = Utils.verifyAndGetSearchParams(p);
        final String esClusterName = pm.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_NAME).asString();

        if ("".equals(esClusterName)) {
            throw new IllegalCommandException
                ("An Elasticsearch cluster must be registered with the store "+
                 " before a text index can be created.");
        }


        /* now ready to deploy the plan to create text index */
        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(makeName(planName, namespace,
                                                 tableName, indexName),
                                        planner);
        tableName = getRealTableName(namespace, tableName, plan.getMetadata());

        /*
         * Create the index, not-yet-visible
         */
        try {

            /*
             * StartAddTextIndex's constructor will verify that there is not
             * already an index with the given name.
             */
            plan.addTask(new StartAddTextIndex(plan,
                                               namespace,
                                               indexName,
                                               tableName,
                                               ftsFields,
                                               properties,
                                               description));

            /*
             * If a stale ES index with the target index's name exists, balk,
             * or remove it straightaway, depending on the value of override.
             * We already know that the Admin's table metadata does not know
             * about it.
             */
            checkTextIndexForCreation(admin, namespace, indexName,
                                      tableName, override);

            /* TODO: do we want to wait for the index to be ready? */
            plan.addTask(new CompleteAddIndex(plan,
                                              namespace,
                                              indexName,
                                              tableName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add index: " + iae.getMessage(), iae);
        }

        return plan;
    }

    public static DeployTableMetadataPlan createBroadcastTableMDPlan
        (Planner planner) {
        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan("Broadcast Table MD", planner);

        plan.addTask(new UpdateMetadata<>(plan));
        return plan;
    }

    public static TableImpl getAndCheckTable(String namespace,
                                             String name,
                                             TableMetadata md) {
        if (md != null) {
            final TableImpl table = md.getTable(namespace, name, false);
            if (table != null) {
                return table;
            }
        }
        throw new TableNotFoundException(
            "Table does not exist: " + name);
    }

    public static List<? extends KVStorePrivilege>
        getRemoveTableRequiredPrivs(ResourceOwner tableOwner,
                                    String namespace,
                                    boolean toRemoveIndex,
                                    long tableId) {

        final ResourceOwner currentUser = SecurityUtils.currentUserAsOwner();
        if ((currentUser != null) && (currentUser.equals(tableOwner))) {
            /* The owner, checks only USRVIEW to ensure authentication */
            return SystemPrivilege.usrviewPrivList;
        }
        final List<KVStorePrivilege> privsToCheck =
            new ArrayList<KVStorePrivilege>();
        if (toRemoveIndex) {
            privsToCheck.add(new TablePrivilege.DropIndex(tableId, namespace));
        }
        privsToCheck.add(
            new NamespacePrivilege.DropTableInNamespace(namespace));
        privsToCheck.add(SystemPrivilege.DROP_ANY_TABLE);
        return privsToCheck;
    }

    static Set<String> getInvolvedRoles(long tableId, SecurityMetadata secMd) {
        if (secMd == null) {
            return Collections.emptySet();
        }
        Set<String> involvedRoles = null;
        for (RoleInstance role : secMd.getAllRoles()) {
            for (KVStorePrivilege priv : role.getPrivileges()) {
                if ((priv instanceof TablePrivilege) &&
                     ((TablePrivilege) priv).getTableId() == tableId) {

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

    /**
     * Gets a list of tables owned by a particular user.  Note that child
     * tables are included before their parents in the list.  This allows
     * using the returned list to create list of tasks for remove cascade
     * plans.
     *
     * @param tableMd table metadata object
     * @param secMd security metadata object
     * @param userName user name
     *
     * @return list of tables owned by the user
     */
    static List<TableImpl> getOwnedTables(TableMetadata tableMd,
                                         SecurityMetadata secMd,
                                         String userName) {
        if (tableMd == null || tableMd.getTables().isEmpty()) {
            return Collections.emptyList();
        }
        if (secMd == null || secMd.getUser(userName) == null) {
            return Collections.emptyList();
        }

        List<TableImpl> tables = null;
        final KVStoreUser user = secMd.getUser(userName);
        final ResourceOwner owner =
            new ResourceOwner(user.getElementId(), user.getName());

        for (Table table : tableMd.getTables().values()) {
            final TableImpl tableImpl = (TableImpl)table;

            if (owner.equals(tableImpl.getOwner())) {
                if (tables == null) {
                    tables = new ArrayList<>();
                }
                addTablesToList(owner, tableImpl, tables);
            }
        }

        if (tables == null) {
            return Collections.emptyList();
        }
        return tables;
    }

    private static void addTablesToList(ResourceOwner owner,
                                       TableImpl table,
                                       List<TableImpl> tables) {
        if (tables == null) {
            throw new IllegalArgumentException(
                "Set to store tables should not be null");
        }
        /* Add child tables before parent, needed for cascade removal plans */
        for (Table table1 : table.getChildTables().values()) {
            addTablesToList(owner, (TableImpl) table1, tables);
        }
        tables.add(table);
    }

    private static void checkTableName(String tableName) {
        if (tableName == null) {
            throw new IllegalCommandException("Table path cannot be null");
        }
    }

    private static void checkIndexName(String indexName) {
        if (indexName == null) {
            throw new IllegalCommandException("Index name cannot be null");
        }
    }

    /**
     * Returns the real table name for the table name, making it insensitive to
     * case.  If the table does not exist, return the argument and allow the
     * caller to continue.  This is called for plans where the table may or may
     * not exist.
     */
    private static String getRealTableName(String namespace,
                                           String tableName,
                                           TableMetadata md) {
        if (md != null) {
            final TableImpl table = md.getTable(namespace, tableName, false);
            if (table != null) {
                return table.getFullName();
            }
        }
        return tableName;
    }

    /**
     * Create a plan or task name that puts more information in the log stream.
     */
    public static String makeName(String planName,
                                  String namespace,
                                  String tableName,
                                  String indexName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(planName);
        return makeName(sb, namespace, tableName, indexName).toString();
    }

    /**
     * Adds a table/index name to the specified builder.
     */
    public static StringBuilder makeName(StringBuilder sb,
                                         String namespace,
                                         String tableName,
                                         String indexName) {
        sb.append(" ");
        if (namespace != null) {
            sb.append(namespace).append(":");
        }
        if (tableName != null) {
            sb.append(tableName);
        }
        if (indexName != null) {
            sb.append(":");
            sb.append(indexName);
        }
        return sb;
    }

    /*
     * Helper method for createAddTextIndexPlan.  Enforces the rule about
     * removing an existing text index that has the same name as the one we are
     * about to create.  The override boolean controls whether the offending
     * index is actually removed or not.
     */
    private static void checkTextIndexForCreation(Admin admin,
                                                  String namespace,
                                                  String indexName,
                                                  String tableName,
                                                  boolean override) {

        final Parameters p = admin.getCurrentParameters();
        final ParameterMap pm = Utils.verifyAndGetSearchParams(p);
        final String esClusterName = pm.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_NAME).asString();
        final String esMembers = pm.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_MEMBERS).asString();
        final boolean isSecure = pm.getOrDefault
                (ParameterState.SN_SEARCH_CLUSTER_SECURE).asBoolean();
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(p.getGlobalParams()
                                              .getKVStoreName(),
                                              namespace,
                                              tableName,
                                              indexName);

        ESRestClient esClient = null;

        try {
              esClient =
                ElasticsearchHandler.createESRestClient(esClusterName,
                                                        esMembers,
                                                        isSecure,
                                                        admin);

            if (ElasticsearchHandler.existESIndex(esIndexName,
                                                  esClient.admin())) {

                if (override) {
                    /* Ensure that the index does not exist. */
                    ElasticsearchHandler.deleteESIndex(esIndexName,
                                                       esClient.admin(),
                                                       admin.getLogger());
                } else {
                    throw new IllegalCommandException
                        ("The command cannot proceed, because an index " +
                         "by the name " + esIndexName + " already exists in " +
                         "the Elasticsearch cluster.  This index does not " +
                         "correspond to a NoSQL Database text index " +
                         "belonging to this store.  You may force its " +
                         "removal by re-issuing the command with OVERRIDE.");
                }
            }

        } catch (Exception e) {
            if (e instanceof IllegalCommandException) {
                throw (IllegalCommandException) e;
            }
            if (e instanceof IOException) {
                throw new IllegalCommandException(
                        "Problems in communication with ES." +
                        e);
            }
            /* Don't advise to use OVERRIDE in this case. */
            throw new IllegalCommandException
                ("The command cannot proceed, because the " +
                 "Elasticsearch cluster is not available.", e);
        } finally {
            if (esClient != null) {
                esClient.close();
            }
        }
    }

    /*
     * Helper method for createRemoveIndexPlan.  Before dropping a text index,
     * ensure ES cluster is healthy before proceeding with the drop index plan.
     */
    private static void checkTextIndexForRemoval(AbstractPlan plan,
                                                 Admin admin,
                                                 String namespace,
                                                 String indexName,
                                                 String tableName,
                                                 boolean override) {
        /* If the version < 11, we wont even create the text index,
         * then don't bother checking.
         *
         */
        if (VersionUtil.getJavaMajorVersion() < 11) {
            return;
        }

        /* If the override property is true, then don't bother checking. */
        if (override) {
            return;
        }

        /* Consult metadata to determine whether the index is a text index. */
        final TableMetadata md = plan.getAdmin().getTableMetadata();
        final TableImpl tbl =
            (md != null) ? md.getTable(namespace, tableName, true) : null;
        final Index idx = (tbl != null) ? tbl.getIndex(indexName) : null;

        if (idx == null || !idx.getType().equals(Index.IndexType.TEXT)) {
            return;
        }

        /* If it's a text index, then verify cluster health. */
        final Parameters p = admin.getCurrentParameters();
        final ParameterMap pm = Utils.verifyAndGetSearchParams(p);
        final String esClusterName = pm.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_NAME).asString();
        final String esMembers = pm.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_MEMBERS).asString();
        final boolean secure = pm.getOrDefault
                (ParameterState.SN_SEARCH_CLUSTER_SECURE).asBoolean();

        /*
         * ES cluster must be "healthy."  See isClusterHealthy for
         * a discussion of what "healthy" means.
         */
        final String errForm =
            "The DROP operation cannot proceed, because it involves " +
            "the text index %s,%nand %s.%n" +
            "Dropping text indexes in this situation can cause%n" +
            "inconsistencies between NoSQL Database and Elasticsearch.%n" +
            "If you wish to proceed despite this concern, issue %n" +
            "the command 'DROP INDEX %s on %s OVERRIDE' to eliminate%n" +
            "the offending text index.  Otherwise, please try%n" +
            "the command again when the Elasticsearch cluster is healthy.";

        ESRestClient esClient = null;
        try {
            esClient =
             ElasticsearchHandler.createESRestClient(esClusterName,
                                                     esMembers,
                                                     secure,
                                                     admin);

            if (!ElasticsearchHandler.isClusterHealthy
                (esMembers, esClient.admin())) {

                throw new IllegalCommandException
                    (String.format
                     (errForm, indexName,
                      "the Elasticsearch cluster's health level is low",
                      indexName, tableName));
            }
        } catch (Exception e) {
            if (e instanceof IllegalCommandException) {
                throw (IllegalCommandException) e;
            }
            if (e instanceof IOException) {
                throw new IllegalCommandException(
                        "Problems in communication with ES." +
                        e);
            }
            throw new IllegalCommandException
                (String.format
                 (errForm, indexName,
                  "the Elasticsearch cluster is not available",
                  indexName, tableName), e);
        } finally {
                if (esClient != null) {
                    esClient.close();
                }

        }
    }

    public static Plan createAddNamespacePlan(String planName, Planner planner,
        String namespace) {

        if (namespace == null ||
            SYSDEFAULT_NAMESPACE_NAME.equalsIgnoreCase(namespace)) {
            throw new IllegalCommandException(
                "Invalid namespace name, this namespace name is reserved: " +
                    namespace);
        }

        if ( namespace.toLowerCase().startsWith(SYSTEM_NAMESPACE_PREFIX) ) {
            throw new IllegalCommandException(
                "Invalid namespace name, names starting with " +
                SYSTEM_NAMESPACE_PREFIX + " are reserved: " + namespace);
        }

        TableImpl.validateNamespace(namespace);

        final String fullPlanName = makeName(planName, namespace, null, null);
        final DeployTableMetadataPlan plan =
            new AddNamespacePlan(fullPlanName, planner);

        try {
            plan.addTask(new AddNamespace(plan, namespace));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add namespace: " + iae.getMessage(), iae);
        }
        return plan;
    }

    static Plan createRemoveNamespacePlan(String planName,
                                          Planner planner,
                                          Topology topology,
                                          String namespace,
                                          boolean cascade) {
        if (namespace == null ||
            SYSDEFAULT_NAMESPACE_NAME.equalsIgnoreCase(namespace)) {
            throw new IllegalCommandException(
                "Cannot remove the system reserved namespace: " +
                NameUtils.switchToExternalUse(namespace));
        }

        TableImpl.validateNamespace(namespace);

        final String fullPlanName = makeName(planName, namespace,
            null, null);
        final MultiMetadataPlan plan =
            new RemoveNamespacePlan(fullPlanName, planner, namespace);

        try {
            if (cascade) {
                final List<TableImpl> nsTables = TablePlanGenerator
                    .getTablesInNamespace(namespace, plan);

                for (Table table : nsTables) {
                    TableImpl tableImpl = (TableImpl)table;
                    long tableId = tableImpl.getId();
                    String internalNs = tableImpl.getInternalNamespace();
                    String tableName = table.getFullName();
                    addRemoveIndexTasks(plan, namespace, tableName,
                            planner.getAdmin());

                    addRemoveTableTasks(plan, topology,
                                        namespace, tableName,
                                        true /*removeChildTables*/);

                    /*
                     * Find roles having privileges on this table, and remove
                     * table privileges from these roles.
                     */
                    final Set<String> involvedRoles = TablePlanGenerator.
                        getInvolvedRoles(tableId, plan.getSecurityMetadata());

                    for (String role : involvedRoles) {
                        plan.addTask(new RemoveTablePrivileges(
                            plan, role, TablePrivilege.getAllTablePrivileges(
                            internalNs, tableId, table.getFullName())));
                    }
                }
            }

            plan.addTask(new RemoveNamespace(plan, namespace, cascade));
        } catch (NamespaceNotFoundException nnfe) {
            throw nnfe;
        } catch (IllegalCommandException iae) {
            throw new IllegalCommandException
                ("Failed to remove namespace: '" + namespace + "': " +
                    iae.getMessage(), iae);
        }

        return plan;
    }

    private static List<TableImpl> getTablesInNamespace(String namespace,
        MultiMetadataPlan plan) {
        TableMetadata md = plan.getTableMetadata();
        Collection<Table>  topTables = md.getTables(namespace).values();
        List<TableImpl> tables = new ArrayList<>();
        for(Table t: topTables) {
            addTablesToList(plan.getOwner(), (TableImpl)t, tables);
        }
        return tables;
    }

    public static Plan createAddRegionPlan(String planName,
                                           Planner planner,
                                           String regionName) {
        TableImpl.validateRegionName(regionName);
        final DeployTableMetadataPlan plan = new RegionPlan(planName, planner);
        try {
            plan.addTask(new CreateRegion(plan, regionName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                        ("Failed to create region " + regionName + ": " +
                         iae.getMessage(), iae);
        }
        return plan;
    }

    public static Plan createDropRegionPlan(String planName,
                                            Planner planner,
                                            String regionName) {
        TableImpl.validateRegionName(regionName);
        final DeployTableMetadataPlan plan = new RegionPlan(planName, planner);
        try {
            plan.addTask(new DropRegion(plan, regionName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                            ("Failed to drop region " + regionName + ": " +
                             iae.getMessage(), iae);
        }
        return plan;
    }

    public static Plan createSetLocalRegionNamePlan(String planName,
                                                    Planner planner,
                                                    String regionName) {
        TableImpl.validateRegionName(regionName);
        final DeployTableMetadataPlan plan = new RegionPlan(planName, planner);
        try {
            plan.addTask(new SetLocalRegionName(plan, regionName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException(
                                            "Failed to set local region name " +
                                              regionName + ": " +
                                              iae.getMessage(), iae);
        }
        return plan;
    }
}
