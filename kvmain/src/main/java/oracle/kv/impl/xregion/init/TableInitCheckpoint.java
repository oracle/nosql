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

package oracle.kv.impl.xregion.init;

import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_AGENT_ID;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_SOURCE_REGION;
import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_TABLE_NAME;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.NOT_START;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Version;
import oracle.kv.impl.systables.MRTableInitCkptDesc;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.json.JsonUtils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Object represents table initialization checkpoint
 */
public class TableInitCheckpoint implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int CURRENT_VERSION = 1;
    /**
     * Read options
     */
    private static final ReadOptions CKPT_TABLE_READ_OPT =
        new ReadOptions(Consistency.ABSOLUTE, 0, null);
    /**
     * Write options. It ensures persistence on master and acknowledged by
     * majority of replicas, strong enough to tolerate master failure.
     * <p>
     * It is worthwhile to ensure the persistence on master of checkpoint
     * table at slightly more overhead compared with using
     * {@link oracle.kv.Durability.SyncPolicy#NO_SYNC} or
     * {@link oracle.kv.Durability.SyncPolicy#WRITE_NO_SYNC} because in the
     * case of table copy resumption, it is usually more expensive to re-copy
     * a big set of rows from the remote region and persist them locally. The
     * default checkpoint interval is defined in
     * {@link oracle.kv.impl.xregion.service.JsonConfig#DEFAULT_ROWS_REPORT_PROGRESS_INTV}.
     * Unless {@link oracle.kv.Durability.SyncPolicy#SYNC} causes significant
     * performance issue, it is better to have stronger durability.
     */
    public static final WriteOptions CKPT_TABLE_WRITE_OPT =
        new WriteOptions(Durability.COMMIT_SYNC, 0, null);

    /**
     * Max attempts to access table init checkpoint table
     */
    private static final int MAX_RETRY_TIC_TABLE = 2;
    /**
     * Version
     */
    private final int version;
    /**
     * Agent id
     */
    private final String agentId;
    /**
     * Source region name
     */
    private final String sourceRegion;
    /**
     * Target region name
     */
    private final String targetRegion;
    /**
     * Timestamp of checkpoint
     */
    private final long timestamp;
    /**
     * Table name
     */
    private final String table;
    /**
     * Table id at local region
     */
    private final long tableId;
    /**
     * Table id at remote region
     */
    private final long remoteTableId;
    /**
     * Last persisted primary key in JSON format, null if no checkpoint has
     * been made. It is encrypted for any secure store.
     */
    private volatile String primaryKey;
    /**
     * Initialization state
     */
    private final TableInitStat.TableInitState state;
    /**
     * Optional message with the state, e.g., the reason of error if the init
     * state is {@link TableInitStat.TableInitState#ERROR}
     */
    private volatile String message = null;

    public TableInitCheckpoint(@NonNull String agentId,
                               @NonNull String sourceRegion,
                               @NonNull String targetRegion,
                               @NonNull String table,
                               long tableId,
                               long remoteTableId,
                               @Nullable String primaryKey,
                               long timestamp,
                               TableInitStat.TableInitState state) {
        this(CURRENT_VERSION, agentId, sourceRegion, targetRegion, table,
             tableId, remoteTableId, primaryKey, timestamp, state);
    }

    private TableInitCheckpoint(int version,
                                String agentId,
                                String sourceRegion,
                                String targetRegion,
                                String table,
                                long tableId,
                                long remoteTableId,
                                String primaryKey,
                                long timestamp,
                                TableInitStat.TableInitState state) {
        this.version = version;
        this.agentId = agentId;
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        this.table = table;
        this.tableId = tableId;
        this.remoteTableId = remoteTableId;
        this.primaryKey = primaryKey;
        this.timestamp = timestamp;
        this.state = state;
        if (NOT_START.equals(state) && primaryKey != null) {
            throw new IllegalArgumentException("Non-null primary key with " +
                                               "state=" + NOT_START);
        }
    }

    public int getVersion() {
        return version;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getTargetRegion() {
        return targetRegion;
    }

    public String getSourceRegion() {
        return sourceRegion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTable() {
        return table;
    }

    public long getTableId() {
        return tableId;
    }

    public long getRemoteTableId() {
        return remoteTableId;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public TableInitStat.TableInitState getState() {
        return state;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInitCheckpoint)) {
            return false;
        }
        final TableInitCheckpoint other = (TableInitCheckpoint) obj;
        return version == other.version &&
               agentId.equals(other.agentId) &&
               targetRegion.equals(other.targetRegion) &&
               sourceRegion.equals(other.sourceRegion) &&
               table.equals(other.table) &&
               tableId == other.tableId &&
               remoteTableId == other.remoteTableId &&
               Objects.equals(primaryKey, other.primaryKey) &&
               timestamp == other.timestamp &&
               state.equals(other.state);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(version) +
               agentId.hashCode() +
               targetRegion.hashCode() +
               sourceRegion.hashCode() +
               table.hashCode() +
               Long.hashCode(tableId) +
               Long.hashCode(remoteTableId) +
               Objects.hashCode(primaryKey) +
               Long.hashCode(timestamp) +
               state.hashCode();
    }

    @Override
    public String toString() {
        return JsonUtils.prettyPrint(this);
    }

    /**
     * Reads a checkpoint from table
     *
     * @param mdMan        metadata manager
     * @param agentId      agent id
     * @param sourceRegion source region name
     * @param tableName    table name
     * @return an optional checkpoint
     */
    public static Optional<TableInitCheckpoint> read(ServiceMDMan mdMan,
                                                     String agentId,
                                                     String sourceRegion,
                                                     String tableName) {

        final Table table = mdMan.getLocalTableRetry(
            MRTableInitCkptDesc.TABLE_NAME, MAX_RETRY_TIC_TABLE);
        if (table == null) {
            return Optional.empty();
        }

        final PrimaryKey pkey = table.createPrimaryKey();
        pkey.put(COL_NAME_AGENT_ID, agentId);
        pkey.put(COL_NAME_SOURCE_REGION, sourceRegion);
        pkey.put(COL_NAME_TABLE_NAME, tableName);
        final Row row = mdMan.readRetry(pkey, CKPT_TABLE_READ_OPT,
                                        MAX_RETRY_TIC_TABLE);
        if (row == null) {
            return Optional.empty();
        }
        final String json =
            row.get(MRTableInitCkptDesc.COL_NAME_CHECKPOINT).asString().get();
        try {
            final TableInitCheckpoint val = JsonUtils.readValue(
                json, TableInitCheckpoint.class);
            return Optional.of(val);
        } catch (RuntimeException re) {
            final String err = "Cannot convert json to checkpoint" + ", " +
                re.getMessage() + ", json=" + json;
            throw new IllegalStateException(err, re);
        }
    }

    /**
     * Writes checkpoint to system table
     *
     * @param mdMan    metadata manager
     * @param ckpt     checkpoint to write
     * @return an optional version
     */
    public static Optional<Version> write(ServiceMDMan mdMan,
                                          TableInitCheckpoint ckpt) {
        final Table table = mdMan.getLocalTableRetry(
            MRTableInitCkptDesc.TABLE_NAME, MAX_RETRY_TIC_TABLE);
        if (table == null) {
            return Optional.empty();
        }

        final Row row = table.createRow();
        row.put(COL_NAME_AGENT_ID, ckpt.getAgentId());
        row.put(COL_NAME_SOURCE_REGION,
                ckpt.getSourceRegion());
        row.put(COL_NAME_TABLE_NAME, ckpt.getTable());
        row.put(MRTableInitCkptDesc.COL_NAME_CHECKPOINT,
                JsonUtils.prettyPrint(ckpt));
        final Version ver = mdMan.putRetry(row, CKPT_TABLE_WRITE_OPT,
                                           MAX_RETRY_TIC_TABLE);
        if (ver == null) {
            return Optional.empty();
        }
        return Optional.of(ver);
    }

    /**
     * Deletes checkpoint from system table
     *
     * @param mdMan    metadata manager
     * @param agentId  agent id
     * @param region   source region
     * @param table    table name
     */
    public static void del(ServiceMDMan mdMan,
                           String agentId,
                           String region,
                           String table) {

        final Table sysTable = mdMan.getLocalTableRetry(
            MRTableInitCkptDesc.TABLE_NAME, MAX_RETRY_TIC_TABLE);
        if (sysTable == null) {
            return;
        }

        final PrimaryKey pkey = sysTable.createPrimaryKey();
        pkey.put(COL_NAME_AGENT_ID, agentId);
        pkey.put(COL_NAME_SOURCE_REGION, region);
        pkey.put(COL_NAME_TABLE_NAME, table);
        mdMan.deleteRetry(pkey, CKPT_TABLE_WRITE_OPT, MAX_RETRY_TIC_TABLE);
    }

    public void setPrimaryKey(String val) {
        primaryKey = val;
    }

    public void setMessage(String msg) {
        message = msg;
    }

    public String getMessage() {
        return message;
    }
}