/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.security;

import java.util.Map;

/**
 * The instance has request security and access context.
 */
public interface AccessContext {
    public static final String INTERNAL_OCID_PREFIX = "ocid1_nosqltable_";
    public static final String EXTERNAL_OCID_PREFIX = "ocid1.nosqltable.";

    public static enum Type {
        KV,
        IAM,
        INSECURE
    }

    /**
     * Get access context type.
     */
    public Type getType();

    /**
     * Get authorization string.
     */
    public default String getAuthString() {
        return null;
    }

    /**
     * Set KV namespace to access.
     */
    public default void setNamespace(String namespace) {
        if (namespace == null) {
            return;
        }
    }

    /**
     * Get KV namespace to access.
     */
    public default String getNamespace() {
        return null;
    }

    /**
     * Get principal id of this subject.
     */
    public default String getPrincipalId() {
        return null;
    }

    /**
     * Get tenant id of the subject.
     */
    public default String getTenantId() {
        return null;
    }

    public default void setCompartmentId(String compartmentId) {
        if (compartmentId == null) {
            return;
        }
    }

    /**
     * The domain name in cloud meatadata that subject is authorized to access.
     * - IAM compartment id
     */
    public default String getCompartmentId() {
        return null;
    }

    /**
     * Reset the table name mapping.
     */
    public default void resetTableNameMapping() {
        return;
    }

    /**
     * Get table identifier.
     */
    public default String getTableId(String tableName) {
        return tableName;
    }

    /**
     * Get original table name passed by users.
     */
    public default String getOriginalTableName(String tableName) {
        return tableName;
    }

    /**
     * Get mapping table name in store.
     */
    public default String getMapTableName(String tableName) {
        return tableName;
    }

    /**
     * Get the table identifier in external format.
     */
    public default String getTableIdExternal(String tableName) {
        return tableName;
    }

    /**
     * Map given table identifier to its external format.
     */
    public default String mapToExternalId(String tableId) {
        return tableId;
    }

    /**
     * Map given table identifier to its internal format.
     */
    public default String mapToInternalId(String tableId) {
        return tableId;
    }

    /**
     * Moving accessing table to given compartment.
     * @param destCompartmentId destination compartment id
     */
    public default void moveCompartment(String destCompartmentId) {
        return;
    }

    /**
     * Setting new tags to accessing table.
     *
     * @param freeform free-form tags
     * @param defined predefined tags
     */
    public default void setNewTags(Map<String, String> freeform,
                                   Map<String, Map<String, Object>> defined,
                                   Map<String, Map<String, Object>> system) {
        return;
    }

    /**
     * Return tag slug authorized by access checker.
     */
    public default byte[] getAuthorizedTags() {
        return null;
    }

    /**
     * Return existing tags associated with the accessing table.
     */
    public default String[] getExistingTags(byte[] tags) {
        return null;
    }

    /**
     * Return destination compartment id.
     */
    public default String getDestCompartmentId() {
        return null;
    }

    /**
     * Return access checking request id.
     */
    public default String getRequestId() {
        return null;
    }

    public default boolean isTableInactive() {
        return false;
    }

    public default void resetTableInactive() {
    }

    public default String getOboToken() {
        return null;
    }

    public default void setIsInternalDdl(boolean value) {
    }

    public static AccessContext NULL_KV_CTX = new AccessContext() {

        @Override
        public Type getType() {
            return Type.KV;
        }
    };
}
