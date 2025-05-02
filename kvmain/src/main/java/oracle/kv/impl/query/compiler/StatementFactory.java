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

package oracle.kv.impl.query.compiler;

import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.table.FieldDef;

import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.SequenceDef;

/**
 * This is an interface that has a number of callbacks implemented, one for each
 * top-level query statement. At this time it's limited to DDL statements, but
 * may be expanded to include DML statements.
 *
 * An instance of this method is passed to query compilation.  It is optional.
 * If a query requires one of these methods and a StatementFactory is not
 * available, as will be the case when compiling on the client side, an
 * exception is thrown, telling the client that the query must be passed to
 * an admin.
 *
 * The interfaces do not return any state. Errors should throw an exception
 * that implements RuntimeException.
 */

public interface StatementFactory {

    public void createTable(TableImpl table,
                            boolean ifNotExists,
                            SequenceDef sequenceDef);

    public void dropTable(String namespace,
                          String tableName,
                          TableImpl table,
                          boolean ifExists);

    /**
     * Only one of fieldArray or annotatedFields is non-null. The latter is
     * used for full text index creation.  Properties can be null.
     * @param override
     */
    public void createIndex(String namespace,
                            String tableName,
                            TableImpl table,
                            String indexName,
                            String[] fieldArray,
                            FieldDef.Type[] typeArray,
                            boolean indexNulls,
                            boolean isUnique,
                            AnnotatedField[] annotatedFields,
                            Map<String,String> properties,
                            String indexComment,
                            boolean ifNotExists,
                            boolean override);

    public void dropIndex(String namespace,
                          String tableName,
                          TableImpl table,
                          String indexName,
                          boolean ifExists,
                          boolean override);

    public void evolveTable(TableImpl table, int tableVersion);

    public void describeTable(String namespace,
                              String tableName,
                              String indexName,
                              List<List<String>> schemaPaths,
                              boolean describeAsJson);

    public void showTableOrIndex(String namespace,
                                 String tableName,
                                 boolean showTables,
                                 boolean showIndexes,
                                 boolean asJson);

    public void showNamespaces(boolean asJson);

    public void showRegions(boolean asJson);

    /*
     * Security methods that read state
     */
    public void showUser(String userName,
                         boolean asJson);

    public void showRole(String role,
                         boolean asJson);

    /*
     * Security methods that modify state
     */
    public void createUser(String userName,
                           boolean isEnabled,
                           boolean isAdmin,
                           final String pass,
                           Long passLifetimeMillis);

    public void createExternalUser(String userName,
                                   boolean isEnabled,
                                   boolean isAdmin);

    public void alterUser(String userName,
                          Boolean isEnabled,
                          final String pass,
                          boolean retainPassword,
                          boolean clearRetainedPassword,
                          Long passLifetimeMillis);

    public void dropUser(String userName, boolean cascade);

    public void createRole(String role);

    public void dropRole(String role);

    public void grantRolesToUser(String userName,
                                 String[] roles);

    public void grantRolesToRole(String roleName,
                                 String[] roles);

    public void revokeRolesFromUser(String userName,
                                    String[] roles);

    public void revokeRolesFromRole(String roleName,
                                    String[] roles);

    public void grantPrivileges(String roleName,
                                String namespace,
                                String tableName,
                                Set<String> privilegeSet);

    public void revokePrivileges(String roleName,
                                 String namespace,
                                 String tableName,
                                 Set<String> privilegeSet);

    public void createNamespace(String namespace, boolean ifNotExists);

    public void dropNamespace(String namespace, boolean ifExists,
        boolean cascade);

    public void grantNamespacePrivileges(String roleName,
        String namespace,
        Set<String> privilegeSet);

    public void revokeNamespacePrivileges(String roleName,
        String namespace,
        Set<String> privilegeSet);

    void createRegion(String regionName);

    void dropRegion(String regionName);

    void setLocalRegionName(String regionName);
}
