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

package oracle.kv.impl.xregion.agent.mrt;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.BaseTableTransferThread;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.impl.xregion.agent.TargetTableEvolveException;
import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableUtils;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.dbi.TTL;

/**
 * Object represents the table transfer of region agent for multi-region table.
 */
public class MRTTableTransferThread extends BaseTableTransferThread {

    /**
     * sample interval in ms used in rate limit logger
     */
    private static final int RATE_LIMIT_LOGGER_SAMPLE_INTV_MS = 10 * 1000;
    /**
     * max number of objects in rate limit logger
     */
    private static final int MAX_OBJ_RATE_LIMIT_LOGGER = 32;
    /**
     * sleep time in ms in before retry fetching the local table
     */
    private static final int SLEEP_MS_LOCAL_TABLE = 1000;

    /**
     * target region
     */
    private final RegionInfo tgtRegion;

    /**
     * target Table API
     */
    private final TableAPIImpl tgtAPI;

    /**
     * subscriber associated with the MRT, null in unit test
     */
    private final MRTSubscriber subscriber;

    /**
     * write options to local store
     */
    private final WriteOptions wo;

    /**
     * rate limiting logger
     */
    private final RateLimitingLogger<String> rlLogger;
    /**
     * Timeout in ms of table transfer
     */
    private final long timeoutMs;

    /**
     * Constructs an instance of table transfer thread
     *
     * @param parent     parent region agent
     * @param subscriber subscriber of the mrt
     * @param table      table to transfer
     * @param srcRegion  source region to transfer table from
     * @param tgtRegion  target region to write table to
     * @param srcAPI     source region table api
     * @param tgtAPI     target region table api
     * @param timeoutMs  timeout in ms
     * @param logger     private logger
     */
    public MRTTableTransferThread(RegionAgentThread parent,
                                  MRTSubscriber subscriber,
                                  Table table,
                                  RegionInfo srcRegion,
                                  RegionInfo tgtRegion,
                                  TableAPIImpl srcAPI,
                                  TableAPIImpl tgtAPI,
                                  long timeoutMs,
                                  Logger logger) {

        super("MRTTrans-from-" + srcRegion.getName() + "-to-" +
              tgtRegion.getName() + "-" +
              (parent == null ? "unittest" : parent.getAgentSubscriberId()) +
              "-" + table.getFullNamespaceName(),
              parent, table, srcRegion, srcAPI, logger);

        this.subscriber = subscriber;
        this.tgtRegion = tgtRegion;
        this.tgtAPI = tgtAPI;
        this.timeoutMs = timeoutMs;
        rlLogger = (subscriber != null) ? subscriber.getRlLogger() :
            /* no subscriber, unit test only */
            new RateLimitingLogger<>(RATE_LIMIT_LOGGER_SAMPLE_INTV_MS,
                                     MAX_OBJ_RATE_LIMIT_LOGGER, logger);
        wo = (subscriber == null) ? null/* unit test only */ :
            subscriber.getWriteOptions();
    }

    @Override
    protected long getTICTableId() {
        return table.getId();
    }

    @Override
    protected void pushRow(Row srcRow) {

        /* put or delete with resolve */
        int attempts = 0;
        Row tgtRow;
        final String tableName = table.getFullNamespaceName();
        final long tableId = table.getId();
        final boolean isTombstone = ((RowImpl) srcRow).isTombstone();
        while (subscriber == null /* unit test only */ ||
               !subscriber.shutdownRequested()) {
            try {
                attempts++;
                tgtRow = transform(srcRow);
                if (tgtRow == null) {
                    return;
                }
                final boolean succ;
                if (isTombstone) {
                    /* translate tombstones to deletions */
                    succ = tgtAPI.deleteResolve((PrimaryKey) tgtRow, null, wo);
                } else {
                    succ = tgtAPI.putResolve(tgtRow, null, wo);
                }

                /* stats update */
                final MRTableMetrics tm =
                    getMetrics().getTableMetrics(tableName);
                final String src = srcRegion.getName();
                final TableInitStat st = tm.getRegionInitStat(src);
                if (isTombstone) {
                    st.incrTransTombstones(1);
                } else {
                    st.incrTransferredRows(1);
                }
                if (TTL.isExpired(srcRow.getExpirationTime())) {
                    st.incrExpired(1);
                }
                /* size of source row */
                final long sz = getRowSize(srcRow);
                st.incrTransBytes(sz);
                if (succ) {
                    if (isTombstone) {
                        st.incrPersistedTombstones(1);
                    } else {
                        st.incrPersistedRows(1);
                    }
                    //TODO: Using source row size as estimate for target row
                    // for now. Use RowImpl.storageSize when it is available
                    st.incrPersistBytes(sz);
                }
                logger.finest(() -> lm("Transfer row from table=" + tableName +
                                       ", persisted?=" + succ));
                break;
            } catch (WrappedClientException wce) {
                final Throwable reason = wce.getCause();
                if (reason instanceof MetadataNotFoundException) {
                    final MetadataNotFoundException mnfe =
                        (MetadataNotFoundException) reason;
                    mnfeHandler(mnfe, tableName, tableId);
                    throw mnfe;
                }
                logger.warning(lm("Cannot transfer table=" +
                                  ServiceMDMan.getTrace(table) +
                                  ", error=" + wce +
                                  (logger.isLoggable(Level.FINE) ?
                                      LoggerUtils.getStackTrace(wce) : "")));
                throw wce;
            } catch (MetadataNotFoundException mnfe) {
                mnfeHandler(mnfe, tableName, tableId);
                logger.warning(lm("Cannot transfer table=" +
                                  ServiceMDMan.getTrace(table) +
                                  ", error=" + mnfe +
                                  (logger.isLoggable(Level.FINE) ?
                                      LoggerUtils.getStackTrace(mnfe) : "")));
                throw mnfe;
            } catch (FaultException fe) {
                if (parent == null) {
                    /* just continue if in unit test without parent */
                    continue;
                }
                final String fault = fe.getFaultClassName();
                if (TargetTableEvolveException.class.getName().equals(fault)) {
                    /* table has evolved at local store */
                    logger.info(lm("Target table=" + tableName +
                                   " (id=" + tableId + ") has evolved" +
                                   ", error=" + getFEtrace(fe)));
                    /* refresh the target table and retry */
                    final TableImpl refresh = waitForRefreshInit(table);
                    if (refresh == null) {
                        final String err =
                            "Table=" + ServiceMDMan.getTrace(table) + " has " +
                            "been removed from evolve table list, terminate " +
                            "table transfer";
                        logger.info(lm(err));
                        throw new IllegalStateException(err);
                    }
                    /* verify that refreshed local table has the same id */
                    final TableImpl tbs = refresh;
                    final long tid = tbs.getId();
                    if (tid != tableId) {
                        final String err =
                            "The refreshed target table has a mismatched " +
                            "table id=" + tid + ", while the target table " +
                            "id=" + tableId + ", the table has been " +
                            "dropped and recreated. The transfer for old " +
                            "table will terminate, and the create request of " +
                            "new table will re-initialize the table" +
                            " from region=" + srcRegion.getName();
                        parent.getMdMan().addDroppedTable(tableName, tableId);
                        logger.warning(lm(err));
                        throw new MetadataNotFoundException(
                            err, tbs.getSequenceNumber());
                    }

                    /* found a new table instance, retry putResolve */
                    table = refresh;
                    logger.fine(() -> lm("Retry with new instance of table=" +
                                         ServiceMDMan.getTrace(table)));
                }
                final String msg =
                    "Cannot push row after attempts=" + attempts +
                    ", will retry" +
                    ", target table=" + ServiceMDMan.getTrace(table) +
                    ", source=" + srcRegion.getName() +
                    ", target=" + tgtRegion.getName() +
                    ", error=" + fe.getFaultClassName();
                rlLogger.log(tableName +
                             fe.getFaultClassName(),
                             Level.WARNING, lm(msg));
            }
        }
    }

    @Override
    protected String dumpStat() {
        final MRTAgentMetrics m = (MRTAgentMetrics) getMetrics();
        final String tableName = table.getFullNamespaceName();
        return "transfer stat for table " + tableName +
               "\nsource region: " + srcRegion.getName() +
               "\ntarget region: " + tgtRegion.getName() +
               "\n" + m.getTableMetrics(tableName);
    }

    /**
     * Returns the size of row in bytes
     */
    private static long getRowSize(Row row) {
        //TODO: use storageSize when it is available
        final long keySize = TableUtils.getKeySize(row);
        final long dataSize = (row instanceof PrimaryKeyImpl) ? 0 :
            TableUtils.getDataSize(row);
        return keySize + dataSize;
    }

    /**
     * Transforms the source table row to target table row, skip if in unit
     * test without parent
     */
    private Row transform(Row srcRow) {
        final Row tgtRow;
        final boolean isTombstone = ((RowImpl) srcRow).isTombstone();
        if (subscriber == null) {
            /* unit test only */
            if (!isTombstone) {
                /* convert source row to json */
                final String json = srcRow.toJsonString(false);
                /* convert json to target table row and reconcile the difference */
                tgtRow = table.createRowFromJson(json, false);
            } else {
                /* a tombstone row from table iterator "*/
                final PrimaryKey pkey = srcRow.createPrimaryKey();
                /* convert source pkey to json */
                final String json = pkey.toJsonString(false);
                /* convert json to key and reconcile the difference */
                tgtRow = table.createPrimaryKeyFromJson(json, false);
            }
            /* set a new region id > 1, unit test only  */
            ((RowImpl) tgtRow).setRegionId(2);
            ((RowImpl) tgtRow).setExpirationTime(srcRow.getExpirationTime());
            return tgtRow;
        }

        /* normal cases */
        if (isTombstone) {
            tgtRow = subscriber.transformRow(srcRow,
                                             StreamOperation.Type.DELETE,
                                             table,
                                             true);
        } else {
            tgtRow = subscriber.transformRow(srcRow,
                                             StreamOperation.Type.PUT,
                                             table,
                                             true);
        }
        return tgtRow;
    }

    /**
     * Gets FaultException trace without remote stack
     */
    private static String getFEtrace(FaultException fe) {
        return fe.getClass().getName() + ", msg=" + fe.getMessage() +
               ", fault class=" + fe.getFaultClassName() +
               ", resource id=" + fe.getResourceId();
    }

    private void mnfeHandler(MetadataNotFoundException mnfe,
                             String tableName, long tableId) {
        if (parent == null) {
            /* if in unit test without parent thread */
            throw mnfe;
        }
        logger.warning(lm("Target table=" + tableName +
                          " (id=" + tableId + ")" +
                          " not found, error=" + mnfe +
                          (logger.isLoggable(Level.FINE) ?
                              LoggerUtils.getStackTrace(mnfe) : "")));
        parent.getMdMan().addDroppedTable(tableName, tableId);
    }

    private TableImpl waitForRefreshInit(TableImpl curr) {
        final long oldVer = curr.getTableVersion();
        final String tableName = curr.getFullNamespaceName();
        final long tableId = curr.getId();
        while (!shutdownRequested()) {
            /*
             * table initialization is a blocking request, only look at
             * pending evolve table list
             */
            final TableImpl refresh =
                (TableImpl) parent.getPendingEvolveTable(tableId);
            if (refresh != null && refresh.getTableVersion() > oldVer) {
                /* table has refreshed, no wait */
                logger.info(lm("Table=" + ServiceMDMan.getTrace(curr) +
                               " has evolved to=" +
                               ServiceMDMan.getTrace(refresh)));
                return refresh;
            }
            synchronized (this) {
                try {
                    final String err =
                        "Initialization wait for table=" +
                        ServiceMDMan.getTrace(curr) +
                        " to refresh from version=" + oldVer +
                        (refresh == null ? ", table not available" :
                            ", to table=" + ServiceMDMan.getTrace(refresh));
                    rlLogger.log(err, Level.INFO, lm(err));
                    wait(SLEEP_MS_LOCAL_TABLE);
                } catch (InterruptedException e) {
                    logger.fine(() -> "Interrupted in waiting for refresh " +
                                      "table=" + tableName +
                                      ", old version=" + oldVer);
                    return null;
                }
            }
        }

        logger.info(lm("Shutdown requested, stop waiting for refreshing " +
                       "current table=" + ServiceMDMan.getTrace(curr) +
                       " in initialization"));
        return null;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}
