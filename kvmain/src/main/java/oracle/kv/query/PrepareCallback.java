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

package oracle.kv.query;

import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;
import oracle.kv.impl.api.table.TableMetadataHelper;

/**
 * @hidden
 * Internal use only
 *
 * This is an interface that is used by query compilation to call back to users
 * when it encounters specific syntax in a query. At this time the callbacks
 * occur for:
 * 1. table name
 * 2. "if not exists" on table creation
 * 3. "if exists" on table drop
 *
 * This interface is only used by query preparation.
 *
 * Others can be added as required, possibly including setter methods.
 *
 * @since 4.4
 */
public interface PrepareCallback {
    /** @hidden For internal use only */
    public enum QueryOperation {
        CREATE_TABLE,
        ALTER_TABLE,
        DROP_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        SELECT,
        UPDATE,
        CREATE_USER,
        ALTER_USER,
        DROP_USER,
        CREATE_ROLE,
        DROP_ROLE,
        GRANT,
        REVOKE,
        DESCRIBE,
        SHOW,
        CREATE_NAMESPACE,
        DROP_NAMESPACE,
        INSERT,
        DELETE,
        CREATE_REGION,
        DROP_REGION,
        SET_LOCAL_REGION
    }

    /**
     * Called when the table name is encountered during parsing.
     *
     * @param tableName is a fully-qualified name of the format
     * tableName[.childName]*
     */
    default void tableName(String tableName) {}

    /**
     * Called when the index name is encountered during parsing.
     *
     * @param indexName is a simple string
     */
    default void indexName(String indexName) {}

    /**
     * Called when the namespace name is encountered during parsing.
     *
     * @param namespaceName is the namespace
     */
    default void namespaceName(String namespaceName) {}

    /**
     * Called when a region name is encountered during parsing.
     *
     * @param regionName a region name
     */
    default void regionName(String regionName) {}

    /**
     * Called when the query operation type is encountered during parsing.
     *
     * @param queryOperation the operation type
     */
    default void queryOperation(QueryOperation queryOperation) {}

    /**
     * Called when "if not exists" if encountered during parsing. The call will
     * not happen if that clause is not found, which means that the users must
     * assume a default state
     */
    default void ifNotExistsFound() {}

    /**
     * Called when "if exists" if encountered during parsing. The call will
     * not happen if that clause is not found, which means that the users must
     * assume a default state
     */
    default void ifExistsFound() {}

    /**
     * Returns true if the caller wants the prepare to complete if possible.
     * If the caller is only interested in the callback information and
     * not the result, return false. The latter will work in the absence
     * of table metadata required for complete preparation.
     */
    default boolean prepareNeeded() {
        return false;
    }

    /**
     * Called when create FULLTEXT index. The call will not happen if statement
     * is not to create FULLTEXT index, which means that the users must assume
     * a default state
     */
    default void isTextIndex() {}

    /**
     * Called when a new table has been constructed during query
     * compilation. This happens only if the operation is
     * CREATE_TABLE or ALTER_TABLE. It allows the caller to do additional
     * validation of the table if desired.
     */
    @SuppressWarnings("unused")
    default void newTable(Table table) {}

    /**
     * Returns an instance of TableMetadataHelper if available to the
     * implementing class. This allows preparation of DDL statements with
     * child tables or schema evolution to succeed in the prepare-only case.
     */
    default TableMetadataHelper getMetadataHelper() {
        return null;
    }

    /**
     * Called when the namespace name is encountered during parsing. Allows the
     * callback to map the name to something else
     *
     * @param namespaceName is the namespace
     * @return the mapped name.
     */
    default String mapNamespaceName(String namespaceName) {
        return namespaceName;
    }

    /**
     * Called when the table name is encountered during parsing. Allows the
     * callback to map the name to something else
     *
     * @param tableName is the table name
     * @return the mapped name.
     */
    default String mapTableName(String tableName) {
        return tableName;
    }

    default void indexFields(@SuppressWarnings("unused") String[] fields) {
    }

    default void indexFieldTypes(@SuppressWarnings("unused")
                                 FieldDef.Type[] types) {
    }

    /**
     * Called in 2 cases:
     * 1. "with schema frozen" found in create table
     * 2. "freeze schema" found in alter table
     */
    default void freezeFound() {
    }

    /**
     * Called in 2 cases:
     * 1. "with schema frozen force?" in create table
     * 2. "freeze schema force?" in alter table
     */
    default void freezeFound(@SuppressWarnings("unused") boolean force) {
    }

    /**
     * Called if "unfreeze schema" found in alter table
     */
    default void unfreezeFound() {
    }

    /**
     * Called if "using TTL &lt;time_to_live&gt;" found in alter table
     */
    default void alterTtlFound() {
    }

    /**
     * Called if a JSON collection is found in a table creation
     */
    default void jsonCollectionFound() {
    }
}
