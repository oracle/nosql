/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.common.json.JsonUtils;

/**
 * This class is to encapsulate the result of list tables and tenancy auto
 * reclaimable metadata, to support the new REST API.
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 */
public class TableCollection {

    private List<TableInfo> tableInfos;
    /* The maximum number of reclaimable tables allowed in the tenancy. */
    private int maxAutoReclaimableTables;
    /* The current number of reclaimable tables in the tenancy. */
    private int autoReclaimableTables;
    /* The maximum number of auto scaling tables allowed in the tenancy. */
    private int maxAutoScalingTables;
    /* The current number of auto scaling tables in the tenancy. */
    private int autoScalingTables;
    /* The permissible replication regions. */
    private List<String> availableReplicationRegions;

    public TableCollection() {
    }

    public TableCollection(List<TableInfo> tableInfos,
                           int maxAutoReclaimableTables,
                           int autoReclaimableTables,
                           int maxAutoScalingTables,
                           int autoScalingTables,
                           List<String> availableReplicationRegions) {
        this.maxAutoReclaimableTables = maxAutoReclaimableTables;
        this.autoReclaimableTables = autoReclaimableTables;
        this.maxAutoScalingTables = maxAutoScalingTables;
        this.autoScalingTables = autoScalingTables;
        this.tableInfos = tableInfos == null ? null :
                                               new ArrayList<>(tableInfos);
        this.availableReplicationRegions =
            availableReplicationRegions == null ?
                null :
                new ArrayList<>(availableReplicationRegions);
    }

    public List<TableInfo> getTableInfos() {
        return tableInfos;
    }

    public int getMaxAutoReclaimableTables() {
        return maxAutoReclaimableTables;
    }

    public int getAutoReclaimableTables() {
        return autoReclaimableTables;
    }

    public int getMaxAutoScalingTables() {
        return maxAutoScalingTables;
    }

    public int getAutoScalingTables() {
        return autoScalingTables;
    }

    public List<String> getAvailableReplicationRegions() {
        return availableReplicationRegions;
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
