/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import static oracle.nosql.util.tmi.TableRequestLimits.getFreeTableDefaultInstance;
import static oracle.nosql.util.tmi.TableRequestLimits.getStandardTableDefaultInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import oracle.nosql.common.json.JsonUtils;

/**
 * A class to encapsulate per-tenant limits. These are generally defaulted, but
 * some may be configurable. Each "environment" has its own way to store and
 * return this information. These are only associated with DDL operations as
 * they can be potentially expensive to retrieve.
 * Note: TenantLimits is internal class, will not expose to customer.
 * We put it in cloudutils project so it can be used by proxy to set tenant
 * limits for test against minicloud.
 */
public class TenantLimits {

    /* a default singleton */
    private static TenantLimits defaults = getNewDefault();

    /*
     * When modifying the version, be sure to update equals(),
     * hashcode() and evolveIfOldVersion()
     */

    /*
     * Version 5 adds support for datastore choice for test tenancies based on 
     * a tablename prefixed with the name of a valid datastore.
     */

    private static final int DSTORE_CHOICE_VERSION = 5;

    /*
     * Version 4 adds support for MR tables.
     */
    private static final int MR_TABLE_VERSION = 4;
    /*
     * Version 3 adds support for auto scaling tables. It also adds table
     * request limits for standard table and free table. And it adds some
     * fields that apply to the configuration of free tables too.
     */
    private static final int AUTO_SCALING_TABLE_VERSION = 3;
    /*
     * Version 2 adds support for free tables with the addition of the
     * numFreeTables field.
     */
    private static final int FREE_TABLE_VERSION = 2;
    private static final int CURRENT_VERSION = MR_TABLE_VERSION;
    private int version;

    private int numTables;        /* The maximum number of tables this
                                   * tenant can have concurrently */

    private int tenantSize;       /* Maximum cumulative size
                                   * provisioned across all tables
                                   * belonging to the tenant. Gigabytes */

    private int tenantReadUnits;   /* Maximum cumulative read throughput
                                    * provisioned across all tables
                                    * belonging to the tenant.  ReadUnits */

    private int tenantWriteUnits;  /* Maximum cumulative write throughput
                                    * provisioned across all tables
                                    * belonging to the tenant. WriteUnits */

    private int ddlRequestsRate;   /* Maximum rate of DDL operations,
                                    * including create, drop, alter.
                                    * Operations/minute. */

    private int tableLimitReductionsRate; /* Maximum rate of table limits
                                           * reductions. Operations/day */

    private int numFreeTables;        /* The maximum number of free tables this
                                       * tenant can have concurrently */

    private int numAutoScalingTables; /* The maximum number of auto scaling
                                       * tables this tenant can have
                                       * concurrently */

    private int autoScalingTableReadUnits;  /* Auto scaling table ceiling read
                                             * limits. */

    private int autoScalingTableWriteUnits; /* Auto scaling table ceiling write
                                             * limits. */

    private int billingModeChangeRate; /* Maximum rate of table billing mode
                                        * changes. Operations/day */

    /*
     * The maximum amount of time, in hours, a free tier table can be idle
     * before it will become to IDLE_P1 state. We will send out the first
     * reminder notification when it becomes IDLE_P1 state.
     * Default is 30 days.
     */
    private int freeTableToIdleP1Hours;
    /*
     * The maximum amount of time, in hours, a free tier table can be idle
     * before it will become to IDLE_P2 state. We will send out the second
     * reminder notification when it becomes IDLE_P2 state.
     * Default is 75 days.
     */
    private int freeTableToIdleP2Hours;
    /*
     * The maximum amount of time, in hours, a free tier table can be idle
     * before it will be reclaimed.
     * Default is 90 days.
     */
    private int freeTableMaxIdleHours;

    /* The permissible replication regions. */
    private List<String> availableReplicationRegions;
    /* Maximum replicas of MR Table */
    private int mrReplicaCount;

    /* limits for standard table */
    private TableRequestLimits standardTableLimits;

    /* limits for free tier table */
    private TableRequestLimits freeTableLimits;
    
    /* specifies if a specific datastore can be targeted using the tablename */
    private boolean allowDstoreChoice;

    public TenantLimits(int numTables,
                        int tenantSize,
                        int tenantReadUnits,
                        int tenantWriteUnits,
                        int ddlRequestsRate,
                        int tableLimitReductionsRate,
                        int numFreeTables,
                        int numAutoScalingTables,
                        int autoScalingTableReadUnits,
                        int autoScalingTableWriteUnits,
                        int billingModeChangeRate,
                        int freeTableToIdleP1Hours,
                        int freeTableToIdleP2Hours,
                        int freeTableMaxIdleHours,
                        List<String> availableReplicationRegions,
                        int mrReplicaCount,
                        TableRequestLimits standardTableLimits,
                        TableRequestLimits freeTableLimits,
                        boolean allowDstoreChoice) {

        this.version = CURRENT_VERSION;
        this.numTables = numTables;
        this.tenantSize = tenantSize;
        this.tenantReadUnits = tenantReadUnits;
        this.tenantWriteUnits = tenantWriteUnits;
        this.ddlRequestsRate = ddlRequestsRate;
        this.tableLimitReductionsRate = tableLimitReductionsRate;
        this.numFreeTables = numFreeTables;
        this.numAutoScalingTables = numAutoScalingTables;
        this.autoScalingTableReadUnits = autoScalingTableReadUnits;
        this.autoScalingTableWriteUnits = autoScalingTableWriteUnits;
        this.billingModeChangeRate = billingModeChangeRate;
        this.freeTableToIdleP1Hours = freeTableToIdleP1Hours;
        this.freeTableToIdleP2Hours = freeTableToIdleP2Hours;
        this.freeTableMaxIdleHours = freeTableMaxIdleHours;
        this.availableReplicationRegions =
            availableReplicationRegions == null ?
                null :
                new ArrayList<>(availableReplicationRegions);
        this.mrReplicaCount = mrReplicaCount;
        this.standardTableLimits = standardTableLimits;
        this.freeTableLimits = freeTableLimits;
        this.allowDstoreChoice = allowDstoreChoice;
    }

    public TenantLimits(TenantLimits other) {
        this(other.numTables,
             other.tenantSize,
             other.tenantReadUnits,
             other.tenantWriteUnits,
             other.ddlRequestsRate,
             other.tableLimitReductionsRate,
             other.numFreeTables,
             other.numAutoScalingTables,
             other.autoScalingTableReadUnits,
             other.autoScalingTableWriteUnits,
             other.billingModeChangeRate,
             other.freeTableToIdleP1Hours,
             other.freeTableToIdleP2Hours,
             other.freeTableMaxIdleHours,
             other.availableReplicationRegions,
             other.mrReplicaCount,
             new TableRequestLimits(other.standardTableLimits),
             new TableRequestLimits(other.freeTableLimits),
             other.allowDstoreChoice);
    }

    /* no-arg constructor for Json serialization */
    public TenantLimits() {
    }

    public static TenantLimits getNewDefault() {
        return new TenantLimits(30,    /* numTables ? */
                                5000,  /* size in GB -- 5TB */
                                100000,/* tenantReadUnits */
                                40000, /* tenantWriteUnits */
                                4,     /* ddlRequestRate (ops/minute) */
                                4,     /* tableLimitReductionRate (ops/day) */
                                0,     /* max free tables */
                                0,     /* max auto scaling tables */
                                12000, /* Auto scaling table read limits */
                                7500,  /* Auto scaling table write limits */
                                1,     /* billingModeChangeRate (ops/day) */
                                30 * 24, /* First reminder after free table
                                          * is idle for 30 days */
                                75 * 24, /* Second reminder after free table
                                          * is idle for 75 days */
                                90 * 24, /* Reclaimed after free table
                                          * is idle for 90 days */
                                Collections.emptyList(),
                                5, /* max replicas of MR Table */
                                getStandardTableDefaultInstance(),
                                getFreeTableDefaultInstance(),
                                false);
    }

    public int getVersion() {
        return version;
    }

    public static TenantLimits getDefaults() {
        return defaults;
    }

    public int getNumTables() {
        return numTables;
    }

    public TenantLimits setNumTables(int numTables) {
        this.numTables = numTables;
        return this;
    }

    public int getTenantSize() {
        return tenantSize;
    }

    public TenantLimits setTenantSize(int tenantSize) {
        this.tenantSize = tenantSize;
        return this;
    }

    public int getTenantReadUnits() {
        return tenantReadUnits;
    }

    public TenantLimits setTenantReadUnits(int tenantReadUnits) {
        this.tenantReadUnits = tenantReadUnits;
        return this;
    }

    public int getTenantWriteUnits() {
        return tenantWriteUnits;
    }

    public TenantLimits setTenantWriteUnits(int tenantWriteUnits) {
        this.tenantWriteUnits = tenantWriteUnits;
        return this;
    }

    public int getDdlRequestsRate() {
        return ddlRequestsRate;
    }

    public TenantLimits setDdlRequestsRate(int ddlRequestsRate) {
        this.ddlRequestsRate = ddlRequestsRate;
        return this;
    }

    public int getTableLimitReductionsRate() {
        return tableLimitReductionsRate;
    }

    public TenantLimits setTableLimitReductionsRate(
        int tableLimitReductionsRate) {
        this.tableLimitReductionsRate = tableLimitReductionsRate;
        return this;
    }

    public int getNumFreeTables() {
        return numFreeTables;
    }

    public TenantLimits setNumFreeTables(int numFreeTables) {
        this.numFreeTables = numFreeTables;
        return this;
    }

    public int getNumAutoScalingTables() {
        return numAutoScalingTables;
    }

    public TenantLimits setNumAutoScalingTables(int numAutoScalingTables) {
        this.numAutoScalingTables = numAutoScalingTables;
        return this;
    }

    public int getAutoScalingTableReadUnits() {
        return autoScalingTableReadUnits;
    }

    public TenantLimits setAutoScalingTableReadUnits(int readUnits) {
        this.autoScalingTableReadUnits = readUnits;
        return this;
    }

    public int getAutoScalingTableWriteUnits() {
        return autoScalingTableWriteUnits;
    }

    public TenantLimits setAutoScalingTableWriteUnits(int writeUnits) {
        this.autoScalingTableWriteUnits = writeUnits;
        return this;
    }

    public int getBillingModeChangeRate() {
        return billingModeChangeRate;
    }

    public TenantLimits setBillingModeChangeRate(int billingModeChangeRate) {
        this.billingModeChangeRate = billingModeChangeRate;
        return this;
    }

    public int getFreeTableToIdleP1Hours() {
        return freeTableToIdleP1Hours;
    }

    public TenantLimits setFreeTableToIdleP1Hours(int freeTableToIdleP1Hours) {
        this.freeTableToIdleP1Hours = freeTableToIdleP1Hours;
        return this;
    }

    public TenantLimits setFreeTableToIdleP2Hours(int freeTableToIdleP2Hours) {
        this.freeTableToIdleP2Hours = freeTableToIdleP2Hours;
        return this;
    }

    public int getFreeTableToIdleP2Hours() {
        return freeTableToIdleP2Hours;
    }

    public int getFreeTableMaxIdleHours() {
        return freeTableMaxIdleHours;
    }

    public TenantLimits setFreeTableMaxIdleHours(int freeTableMaxIdleHours) {
        this.freeTableMaxIdleHours = freeTableMaxIdleHours;
        return this;
    }

    public List<String> getAvailableReplicationRegions() {
        return availableReplicationRegions;
    }

    public void setAvailableReplicationRegions(
        List<String> availableReplicationRegions) {
        this.availableReplicationRegions =
            availableReplicationRegions == null ?
                null :
                new ArrayList<>(availableReplicationRegions);
    }

    public int getMrReplicaCount() {
        return mrReplicaCount;
    }

    public void setMrReplicaCount(int mrReplicaCount) {
        this.mrReplicaCount = mrReplicaCount;
    }

    public TableRequestLimits getStandardTableLimits() {
        return standardTableLimits;
    }

    public TenantLimits
        setStandardTableLimits(TableRequestLimits standardTableLimits) {
        this.standardTableLimits = standardTableLimits;
        return this;
    }

    public TableRequestLimits getFreeTableLimits() {
        return freeTableLimits;
    }

    public TenantLimits setFreeTableLimits(TableRequestLimits freeTableLimits) {
        this.freeTableLimits = freeTableLimits;
        return this;
    }

    public boolean getAllowDstoreChoice() {
        return allowDstoreChoice;
    }

    public TenantLimits setAllowDstoreChoice(boolean allowDstoreChoice) {
        this.allowDstoreChoice = allowDstoreChoice;
        return this;
    }

    public static void setDefaults(TenantLimits defaults) {
        TenantLimits.defaults = defaults;
    }

    public boolean evolveIfOldVersion() {
        if (version < FREE_TABLE_VERSION) {
            numFreeTables = 0;
        }
        if (version < AUTO_SCALING_TABLE_VERSION) {
            numAutoScalingTables = 0;
            autoScalingTableReadUnits = 12000;
            autoScalingTableWriteUnits = 7500;
            billingModeChangeRate = 1;
            freeTableToIdleP1Hours = 30 * 24;
            freeTableToIdleP2Hours = 75 * 24;
            freeTableMaxIdleHours = 90 * 24;
            standardTableLimits = getStandardTableDefaultInstance();
            freeTableLimits = getFreeTableDefaultInstance();
        }
        if (version < MR_TABLE_VERSION) {
            mrReplicaCount = 5;
        }
        /* Current version */
        if (version < DSTORE_CHOICE_VERSION) {
            allowDstoreChoice = false;
            version = DSTORE_CHOICE_VERSION;
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + autoScalingTableReadUnits;
        result = prime * result + autoScalingTableWriteUnits;
        result = prime * result + billingModeChangeRate;
        result = prime * result + ddlRequestsRate;
        result = prime * result
            + ((freeTableLimits == null) ? 0 : freeTableLimits.hashCode());
        result = prime * result + freeTableMaxIdleHours;
        result = prime * result + freeTableToIdleP1Hours;
        result = prime * result + freeTableToIdleP2Hours;
        result = prime * result + numAutoScalingTables;
        result = prime * result + numFreeTables;
        result = prime * result + numTables;
        result = prime * result
            + ((standardTableLimits == null) ? 0 :
                                               standardTableLimits.hashCode());
        result = prime * result + tableLimitReductionsRate;
        result = prime * result + tenantReadUnits;
        result = prime * result + tenantSize;
        result = prime * result + tenantWriteUnits;
        result = prime * result
            + ((availableReplicationRegions == null) ?
                0 : availableReplicationRegions.hashCode());
        result = prime * result + mrReplicaCount;
        result = prime * result + (allowDstoreChoice ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TenantLimits other = (TenantLimits) obj;
        if (autoScalingTableReadUnits != other.autoScalingTableReadUnits) {
            return false;
        }
        if (autoScalingTableWriteUnits != other.autoScalingTableWriteUnits) {
            return false;
        }
        if (billingModeChangeRate != other.billingModeChangeRate) {
            return false;
        }
        if (ddlRequestsRate != other.ddlRequestsRate) {
            return false;
        }
        if (freeTableLimits == null) {
            if (other.freeTableLimits != null) {
                return false;
            }
        } else if (!freeTableLimits.equals(other.freeTableLimits)) {
            return false;
        }
        if (freeTableMaxIdleHours != other.freeTableMaxIdleHours) {
            return false;
        }
        if (freeTableToIdleP1Hours != other.freeTableToIdleP1Hours) {
            return false;
        }
        if (freeTableToIdleP2Hours != other.freeTableToIdleP2Hours) {
            return false;
        }
        if (numAutoScalingTables != other.numAutoScalingTables) {
            return false;
        }
        if (numFreeTables != other.numFreeTables) {
            return false;
        }
        if (numTables != other.numTables) {
            return false;
        }
        if (standardTableLimits == null) {
            if (other.standardTableLimits != null) {
                return false;
            }
        } else if (!standardTableLimits.equals(other.standardTableLimits)) {
            return false;
        }
        if (tableLimitReductionsRate != other.tableLimitReductionsRate) {
            return false;
        }
        if (tenantReadUnits != other.tenantReadUnits) {
            return false;
        }
        if (tenantSize != other.tenantSize) {
            return false;
        }
        if (tenantWriteUnits != other.tenantWriteUnits) {
            return false;
        }
        if (availableReplicationRegions == null) {
            if (other.availableReplicationRegions != null) {
                return false;
            }
        } else if (!availableReplicationRegions.equals(
            other.availableReplicationRegions)) {
            return false;
        }
        if (mrReplicaCount != other.mrReplicaCount) {
            return false;
        }
        if (allowDstoreChoice != other.allowDstoreChoice) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}

