/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 * To serialize a Java object into a Json string:
 *   Foo foo;
 *   String jsonPayload = objectMapper.writeValueAsString(foo);
 *
 * To deserialize a Json string into this object:
 *   Foo foo = objectMapper.readValue(<jsonstring>, Foo.class);
 */
public class TableDDLInputs {
    /*
     * Version 2 changes:
     *   Added support for auto scaling tables. The actual change is in the
     *   TableLimits class, which is a field in this class.
     *   Limits Mode was added.
     */
    @SuppressWarnings("unused")
    private static final int AUTO_SCALING_TABLE_VERSION = 2;

    /*
     * Version 3 changes:
     *   Support for MR tables, add String oboToken.
     */
    public static final int MR_TABLE_VERSION = 3;

    /*
     * Version 4 changes:
     *  Add indexKeySize to support cross-region indexKeySize limit.
     */
    public static final int INDEX_KEY_SIZE_VERSION = 4;

    private static final int CURRENT_VERSION = INDEX_KEY_SIZE_VERSION;
    private int version;

    private String statement;
    private String tenantId;
    private String compartmentId;
    private byte[] matchETag;
    private boolean ifNotExists;
    private TableLimits tableLimits;
    private byte[] tags;
    private String retryToken;

    /*
     * Specify true to create an auto reclaimable table(also known as always
     * free tier table). Auto reclaimable table will be reclaimed automatically
     * if table no data operation for a long while(by default is 90 days).
     * This only works for create-table DDL.
     */
    private boolean autoReclaimable;

    /* obo token, used by MR table ddl */
    private String oboToken;

    /* index key size */
    private int indexKeySize;

    /* Needed for serialization */
    public TableDDLInputs() {
    }

    public TableDDLInputs(String statement,
                          String tenantId,
                          String compartmentId,
                          boolean ifNotExists,
                          TableLimits tableLimits,
                          boolean autoReclaimable) {
        this(statement, tenantId, compartmentId, null /* matchETag */,
             ifNotExists, tableLimits, null /* tags */, autoReclaimable,
             null /* retryToken */);
    }

    public TableDDLInputs(String statement,
                          String tenantId,
                          String compartmentId,
                          byte[] matchETag,
                          boolean ifNotExists,
                          TableLimits tableLimits,
                          byte[] tags,
                          boolean autoReclaimable,
                          String retryToken) {
        this.version = CURRENT_VERSION;
        this.statement = statement;
        this.tenantId = tenantId;
        this.compartmentId = compartmentId;
        this.matchETag = matchETag;
        this.ifNotExists = ifNotExists;
        this.tableLimits = tableLimits;
        this.tags = tags;
        this.autoReclaimable = autoReclaimable;
        this.retryToken = retryToken;
    }

    public int getVersion() {
        return version;
    }

    /**
     * @return the statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Set the ddl statement. Used by jersey to deserialize json
     */
    public void setStatement(String statement) {
        this.statement = statement;
    }

    /**
     * @return the tenantId
     */
    public String getTenantId() {
        return tenantId;
    }

    /**
     * Set the tenantId. Used by jersey to deserialize json
     */
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /**
     * @return the compartmentId
     */
    public String getCompartmentId() {
        return compartmentId;
    }

    /**
     * Set the compartmentId. Used by jersey to deserialize json
     */
    public void setCompartmentId(String compartmentId) {
        this.compartmentId = compartmentId;
    }

    public byte[] getMatchETag() {
        return matchETag;
    }

    /**
     * @return the ifNotExists
     */
    public boolean getIfNotExists() {
        return ifNotExists;
    }

    /**
     * Set the ifNotExists. Used by jersey to deserialize json
     */
    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return the tableLimits
     */
    public TableLimits getTableLimits() {
        return tableLimits;
    }

    /**
     * Set the table limits. Used by jersey to deserialize json
     */
    public void setTableLimits(TableLimits tableLimits) {
        this.tableLimits = tableLimits;
    }

    public void setTags(byte[] tags) {
        this.tags = tags;
    }

    public byte[] getTags() {
        return tags;
    }

    public boolean isAutoReclaimable() {
        return autoReclaimable;
    }

    public void setAutoReclaimable(boolean autoReclaimable) {
        this.autoReclaimable = autoReclaimable;
    }

    public String getRetryToken() {
        return retryToken;
    }

    public void setOboToken(String oboToken) {
        this.oboToken = oboToken;
    }

    public String getOboToken() {
        return oboToken;
    }

    public void setIndexKeySize(int size) {
        indexKeySize = size;
    }

    public int getIndexKeySize() {
        return indexKeySize;
    }

    @Override
    public String toString() {
        return "TableDDLInputs [statement=" + statement +
            ", tenantId=" + tenantId +
            ", compartmentId=" + compartmentId +
            ", matchETag=" + (matchETag != null ? "<not-null>" : "null") +
            ", ifNotExists=" + ifNotExists +
            ", autoReclaimable=" + autoReclaimable +
            ", tableLimits=" + tableLimits +
            ((oboToken != null) ? ", oboToken=<non-null>]" : "]");
    }
}
