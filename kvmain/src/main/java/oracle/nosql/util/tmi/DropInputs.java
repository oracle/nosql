/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 */
public class DropInputs {
    /*
     * Version 2 changes:
     *   Support for MR tables, add String oboToken.
     */
    private static final int MR_TABLE_VERSION = 2;

    private static final int CURRENT_VERSION = MR_TABLE_VERSION;

    private int version;

    private boolean ifExists;
    private String tenantId;
    private String compartmentId;
    private byte[] matchETag;
    /* obo token, used by MR table ddl */
    private String oboToken;

    /* Need for serialization */
    public DropInputs() {
    }

    public DropInputs(boolean ifExists,
                      String tenantId,
                      String compartmentId) {
        this(ifExists, tenantId, compartmentId, null /* matchETag */);
    }

    public DropInputs(boolean ifExists,
                      String tenantId,
                      String compartmentId,
                      byte[] matchETag) {
        this.version = CURRENT_VERSION;
        this.ifExists = ifExists;
        this.tenantId =tenantId;
        this.compartmentId = compartmentId;
        this.matchETag = matchETag;
    }

    public int getVersion() {
        return version;
    }

    /**
     * @return the ifExists
     */
    public boolean getIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    /**
     * @return the tenantId
     */
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /**
     * @return the compartmentId
     */
    public String getCompartmentId() {
        return compartmentId;
    }

    public void setCompartmentId(String compartmentId) {
        this.compartmentId = compartmentId;
    }

    public byte[] getMatchETag() {
        return matchETag;
    }

    public void setOboToken(String oboToken) {
        this.oboToken = oboToken;
    }

    public String getOboToken() {
        return oboToken;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "DropTableInputs [ifExists=" + ifExists + ", tenantId="
                + tenantId + ", compartmentId=" + compartmentId
                + ", matchETag=" + (matchETag != null ? "<non-null>" : "null")
                + ((oboToken != null) ? ", oboToken=<non-null>]" : "]");
    }
}
