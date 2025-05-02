/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.util.Arrays;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 */
public class StoreInfo {
    /*
     * - version 2, added tableRequestLimits
     * - version 3, added isMultiRegion, isInitialized
     *              These 2 information are actually for table not store, but
     *              they are sent back to the proxy in single GetStoreResponse
     *              called when caching a TableEntry in the proxy, so add them
     *              to this class.
     */
    private static final int MR_TABLE_VERSION = 3;
    private static final int CURRENT_VERSION = MR_TABLE_VERSION;
    private int version;

    private String datastoreName;
    private String[] helperhosts;
    private TableRequestLimits tableRequestLimits;
    private boolean isMultiRegion;
    private boolean isInitialized;

    /* Needed for serialization */
    public StoreInfo() {
    }

    public StoreInfo(String datastoreName,
                     String[] helperhosts,
                     TableRequestLimits tableRequestLimits,
                     boolean isMultiRegion,
                     boolean isInitialized) {
        this.version = CURRENT_VERSION;
        this.datastoreName = datastoreName;
        this.helperhosts = helperhosts;
        this.tableRequestLimits = tableRequestLimits;
        this.isMultiRegion = isMultiRegion;
        this.isInitialized = isInitialized;
    }

    public int getVersion() {
        return version;
    }

    /**
     * @return the datastoreName
     */
    public String getDatastoreName() {
        return datastoreName;
    }

    /**
     * @return the helperhosts
     */
    public String[] getHelperhosts() {
        return helperhosts;
    }

    /**
     * @return the tableRequestLimits
     */
    public TableRequestLimits getTableRequestLimits() {
        return tableRequestLimits;
    }

    public boolean isMultiRegion() {
        return isMultiRegion;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "StoreInfo [datastoreName=" + datastoreName + ", helperhosts="
                + Arrays.toString(helperhosts) + ", tableRequestLimits="
                + tableRequestLimits + ", isMultiRegion=" + isMultiRegion
                + ", isInitialized=" + isInitialized
                + ", version=" + version + "]";
    }
}
