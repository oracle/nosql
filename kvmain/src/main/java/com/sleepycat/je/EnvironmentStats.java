/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je;

import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ACTIVE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_AVAILABLE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DELETIONS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DISK_READS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ENTRIES_READ;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNQUEUE_HITS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_EXPIRED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_EXTINCT;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MAX_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MIN_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_DBS_INCOMPLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_DBS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_DB_QUEUE_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LN_QUEUE_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PREDICTED_MAX_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PREDICTED_MIN_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE_MAP;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RESERVED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_REVISAL_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TOTAL_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TWO_PASS_RUNS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_BIN_DELTA_DELETES;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_BIN_DELTA_GETS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_BIN_DELTA_INSERTS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_BIN_DELTA_UPDATES;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BT_OP_ROOT_SPLITS;
import static com.sleepycat.je.dbi.DbiStatDefinition.BACKUP_COPY_FILES_COUNT;
import static com.sleepycat.je.dbi.DbiStatDefinition.BACKUP_COPY_FILES_MS;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_CREATION_TIME;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_ADMIN_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_CACHE_UTILIZATION;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_DATA_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_LOCK_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_SHARED_CACHE_TOTAL_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_TOTAL_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_DELETE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_DELETE_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_INSERT;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_INSERT_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_POSITION;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_SEARCH;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_SEARCH_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_UPDATE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_DELETE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_INSERT;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_POSITION;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_SEARCH;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_SEARCH_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_UPDATE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_BLIND_OPS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS_RATIO;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_BINS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_BIN_DELTAS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_COMPACT_KEY;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_NO_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_SPARSE_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_UPPER_INS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_DIRTY_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_EVICTION_RUNS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_LNS_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MOVED_TO_PRI2_LRU;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MUTATED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_PUT_BACK;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_SKIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_STRIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_TARGETED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_ROOT_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_SHARED_CACHE_ENVS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.FULL_BIN_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI1_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI2_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.THREAD_UNAVAILABLE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH_MISS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_CURSORS_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_DBCLOSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_NON_EMPTY_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_PROCESSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_QUEUE_SIZE;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_SPLIT_BINS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_BYTES_READ_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FILE_OPENS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FSYNC_95_MS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FSYNC_99_MS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FSYNC_AVG_MS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FSYNC_MAX_MS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_LOG_FSYNCS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_OPEN_FILES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_READS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_READ_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_WRITES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_WRITE_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_READS_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_READS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_READ_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_WRITES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_WRITE_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITES_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNCS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNC_REQUESTS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_N_GROUP_COMMIT_REQUESTS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_TIMEOUTS;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_BUFFER_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_LOG_BUFFERS;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_MISS;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_NOT_RESIDENT;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_NO_FREE_BUFFER;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_END_OF_LOG;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_ITEM_BUFFER_POOL_EMPTY;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_ITEM_BUFFER_TOO_SMALL;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_FAULT_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_ITERATOR_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_TEMP_BUFFER_WRITES;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_CHECKPOINTS;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_DELTA_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_BIN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPTID;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_END;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_INTERVAL;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_START;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_OWNERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_REQUESTS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.beforeimage.BeforeImageIndexStatDefinition;
import com.sleepycat.je.cleaner.CleanerStatDefinition;
import com.sleepycat.je.cleaner.EraserStatDefinition;
import com.sleepycat.je.dbi.BTreeStatDefinition;
import com.sleepycat.je.dbi.DbiStatDefinition;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.evictor.EvictorStatDefinition;
import com.sleepycat.je.incomp.INCompStatDefinition;
import com.sleepycat.je.log.LogStatDefinition;
import com.sleepycat.je.recovery.CheckpointStatDefinition;
import com.sleepycat.je.txn.LockStatDefinition;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.DbCacheSize;
import com.sleepycat.je.util.DbVerifyStatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TaskCoordinator;

/**
 * Statistics for a single environment. Statistics provide indicators for
 * system monitoring and performance tuning.
 *
 * <p>Each statistic has a name and a getter method in this class. For example,
 * the {@code cacheTotalBytes} stat is returned by the {@link
 * #getCacheTotalBytes()} method. Statistics are categorized into several
 * groups, for example, {@code cacheTotalBytes} is in the {@code Cache}
 * group. Each stat and group has a name and a description.</p>
 *
 * <p>Viewing the statistics through {@link #toString()} shows the stat names
 * and values organized by group. Viewing the stats with {@link
 * #toStringVerbose()} additionally shows the description of each stat and
 * group.</p>
 *
 * <p>Statistics are periodically output in CSV format to the je.stat.csv file
 * (see {@link EnvironmentConfig#STATS_COLLECT}). The column header in the .csv
 * file has {@code group:stat} format, where 'group' is the group name and
 * 'stat' is the stat name. In Oracle NoSQL DB, in the addition to the .csv
 * file, JE stats are output in the .stat files.</p>
 *
 * <p>Stat values may also be obtained via JMX via Oracle NoSQL DB in JSON
 * format. The JSON format uses property names of the form {@code
 * group_stat} where 'group' is the group name and 'stat' is the stat name.</p>
 *
 * <p>The stat groups are listed below. Each group name links to a summary of
 * the statistics in the group.</p>
 *
 * <table>
 *  <caption style="display:none">Statistics groups and descriptions</caption>
 *     <tr>
 *         <th>Group Name</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td><a href="#cache">{@value
 *         com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#cleaner">{@value
 *         com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#log">{@value
 *         com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.log.LogStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#incomp">{@value
 *         com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.incomp.INCompStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#ckpt">{@value
 *         com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#lock">{@value
 *         com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.txn.LockStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#throughput">{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#btreeop">{@value
 *         com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_DESC}
 *         </td>
 *     </tr>
 *     <!--
 *     <tr>
 *         <td><a href="#taskcoord">{@value
 *         com.sleepycat.je.utilint.TaskCoordinator.StatDefs#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.utilint.TaskCoordinator.StatDefs#GROUP_DESC}
 *         </td>
 *     </tr>
 *     -->
 *     <tr>
 *         <td><a href="#env">{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_DESC}
 *         </td>
 *     <!-- Hidden: For internal use: automatic backups
 *     <tr>
 *         <td><a href="#env">{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_DESC}
 *         </td>
 *     -->
 * </table>
 *
 * <p>The following sections describe each group of stats along with some
 * common strategies for using them for monitoring and performance tuning.</p>
 *
 * <h2><a id="cache">Cache Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_DESC}</p>
 *
 * <p>The JE cache resides in the Java heap. The vast majority of the cache is
 * occupied by Btree nodes, including internal nodes (INs) and leaf nodes
 * (LNs). INs contain record keys while each LN contain a single record's key
 * and data.</p>
 *
 * <p>Each IN refers to a configured maximum number of child nodes ({@link
 * EnvironmentConfig#NODE_MAX_ENTRIES}). The INs form a Btree of at least 2
 * levels. With a large data set the Btree will normally have 4 or 5 levels.
 * The top level is a single node, the root IN. Levels are numbered from the
 * bottom up, starting with level 1 for bottom level INs (BINs). Levels are
 * added at the top when the root IN splits.</p>
 *
 * <h3><a id="cacheSizing">Cache Statistics: Sizing</a></h3>
 *
 * <p>Operation performance is often directly proportional to how much of the
 * active data set is cached. BINs and LNs form the vast majority of the cache.
 * Caching of BINs and LNs has different performance impacts, and behavior
 * varies depending on which {@link CacheMode} is used.</p>
 *
 * <p>Cache current usage is indicated by the following stats. Note that
 * there is currently no stat for the number of LNs in the cache.</p>
 *
 * <table style="margin-left: 2em">
 *  <caption style='display:none'>Statistics accessors and descriptions</caption>
 *  <tr><td>{@link #getCacheTotalBytes}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#MB_TOTAL_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getCacheUtilization}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#MB_CACHE_UTILIZATION_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCachedBINs}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCachedBINDeltas}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BIN_DELTAS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCachedUpperINs}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_UPPER_INS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>A cache miss is considered a miss only when the object is not found in
 * either cache. Misses often result in file I/O and are a primary indicator
 * of cache performance. Fetches (access requests) and misses are indicated
 * by:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsFetch}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNLNsFetchMiss}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_MISS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINsFetch}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINsFetchMiss}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINDeltasFetchMiss}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_FETCH_MISS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNFullBINsMiss}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#FULL_BIN_MISS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNUpperINsFetch}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNUpperINsFetchMiss}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_MISS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When the number of LN misses ({@code nLNsFetchMiss}) or the number of
 * BIN misses ({@code nBINsFetchMiss + nFullBINsMiss}) are significant, the
 * JE cache may be undersized, as discussed below. But note that it is not
 * practical to correlate the number of fetches and misses directly to
 * application operations, because LNs are sometimes
 * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}, BINs are sometimes
 * accessed multiple times per operation, and internal Btree accesses are
 * included in the stat values.</p>
 *
 * <p>Ideally, all BINs and LNs for the active data set should fit in cache so
 * that operations do not result in fetch misses, which often perform random
 * read I/O. When this is not practical, which is often the case for large
 * data sets, the next best thing is to ensure that all BINs fit in cache,
 * so that an operation will perform at most one random read I/O to fetch
 * the LN. The {@link DbCacheSize} javadoc describes how to size the cache
 * to ensure that all BINs and/or LNs fit in cache.</p>
 *
 * <p>Normally {@link EnvironmentConfig#MAX_MEMORY_PERCENT} determines the JE
 * cache size as a value relative to the JVM heap size, i.e., the heap size
 * determines the cache size.</p>
 *
 * <p>For configuring cache size and behavior, see:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#MAX_MEMORY_PERCENT}</li>
 *     <li>{@link EnvironmentConfig#MAX_MEMORY}</li>
 *     <li>{@link EnvironmentConfig#setCacheMode(CacheMode)}</li>
 *     <li>{@link CacheMode}</li>
 *     <li>{@link DbCacheSize}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, a sizing exercise and {@link DbCacheSize} are
 * used to determine the cache size needed to hold all BINs in memory. The
 * memory available to each node is used for the 32 GB heap for the JVM
 * process (so that CompressedOops may be used) and the remainder is available
 * for OS buffers.</p>
 *
 * <p>It is also important not to configured the cache size too large, relative
 * to the JVM heap size. If there is not enough free space in the heap, Java
 * GC pauses may become a problem. Increasing the default value for {@code
 * MAX_MEMORY_PERCENT}, or setting {@code MAX_MEMORY} (which overrides {@code
 * MAX_MEMORY_PERCENT}), should be done carefully.</p>
 *
 * <p>Java GC performance may also be improved by using {@link
 * CacheMode#EVICT_LN}. Record data sizes should also be kept below 1 MB to
 * avoid "humongous objects" (see Java GC documentation).</p>
 *
 * <p>When using Oracle NoSQL DB, by default, {@code MAX_MEMORY_PERCENT} is
 * set to 70% and {@link CacheMode#EVICT_LN} is used. The LOB (large object)
 * API is implemented using multiple JE records per LOB where the data size of
 * each record is 1 MB or less.</p>
 *
 * <p>When a shared cache is configured, the cache may be shared by multiple JE
 * Environments in a single JVM process. See:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#SHARED_CACHE}</li>
 *     <li>{@link #getSharedCacheTotalBytes()}</li>
 *     <li>{@link #getNSharedCacheEnvironments()}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, the JE shared cache feature is not used
 * because each node only uses a single JE Environment.</p>
 *
 * <h3><a id="cacheSizeOptimizations">Cache Statistics: Size
 * Optimizations</a></h3>
 *
 * <p>Since a large portion of an IN consists of record keys, JE uses
 * {@link DatabaseConfig#setKeyPrefixing(boolean) key prefix compression}.
 * Ideally, key suffixes are small enough to be stored using the {@link
 * EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH compact key format}. The
 * following stat indicates the number of INs using this compact format:</p>
 *
 * <table style="margin-left: 2em" border="">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNINCompactKeyIN}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_COMPACT_KEY_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Configuration params impacting key prefixing and the compact key format
 * are:</p>
 * <ul>
 *     <li>{@link DatabaseConfig#setKeyPrefixing(boolean)}</li>
 *     <li>{@link EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH}</li>
 * </ul>
 *
 * <p>Enabling key prefixing for all databases is strongly recommended. When
 * using Oracle NoSQL DB, key prefixing is always enabled.</p>
 *
 * <p>Another configuration param impacting BIN cache size is {@code
 * TREE_MAX_EMBEDDED_LN}. There is currently no stat indicating the number of
 * embedded LNs. See:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN}</li>
 * </ul>
 *
 * <h3><a id="cacheUnexpectedSizes">Cache Statistics: Unexpected
 * Sizes</a></h3>
 *
 * <p>Although the Btree normally occupies the vast majority of the cache, it
 * is possible that record locks occupy unexpected amounts of cache when
 * large transactions are used, or when cursors or transactions are left open
 * due to application bugs. The following stat indicates the amount of cache
 * used by record locks:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getLockBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#MB_LOCK_BYTES_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>To reduce the amount of memory used for record locks:</p>
 * <ul>
 *     <li>Use a small number of write operations per transaction. Write
 *     locks are held until the end of a transaction.</li>
 *     <li>For transactions using RepeatableRead isolation (the default), use
 *     a small number of read operations per transaction.</li>
 *     <li>To read large numbers of records, use {@link
 *     TransactionConfig#setReadCommitted ReadCommitted} isolation or use a
 *     null Transaction parameter (which implies ReadCommitted). With
 *     ReadCommitted isolation, locks are released after each read operation.
 *     Using {@link LockMode#READ_UNCOMMITTED} will also avoid record locks,
 *     but does not provide any transactional guarantees.</li>
 *     <li>Ensure that all cursors and transactions are closed
 *     promptly.</li>
 * </ul>
 *
 * <p>Note that the above guidelines are also important for reducing contention
 * when records are accessed concurrently from multiple threads and
 * transactions. When using Oracle NoSQL DB, the application should avoid
 * performing a large number of write operations in a single request. For read
 * operations, NoSQL DB uses ReadCommitted isolation to avoid accumulation of
 * locks.</p>
 *
 * <h3><a id="cacheEviction">Cache Statistics: Eviction</a></h3>
 *
 * <p>Eviction is removal of Btree node from the cache in order to make room
 * for newly added nodes. See {@link CacheMode} for a description of
 * eviction.</p>
 *
 * <p>Normally eviction is performed via background threads in the eviction
 * thread pool. Disabling the eviction pool threads is not recommended.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#ENV_RUN_EVICTOR}</li>
 * </ul>
 *
 * <p>Eviction stats are important indicator of cache efficiency and provide a
 * deeper understanding of cache behavior. Cache eviction is indicated by:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsEvicted}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_LNS_EVICTED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesMutated}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MUTATED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesEvicted}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_EVICTED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNDirtyNodesEvicted}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_DIRTY_NODES_EVICTED_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When analyzing Java GC performance, the most relevant stats are {@code
 * NLNsEvicted}, {@code NNodesMutated} and {@code NNodesEvicted}, which all
 * indicate eviction from the cache based on LRU. Large values for these stats
 * indicate that many old generation Java objects are being GC'd, which is
 * often a cause of GC pauses.</p>
 *
 * <p>Note that {@link CacheMode#EVICT_LN} is used or when LNs are {@link
 * EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}, {@code NLNsEvicted} will
 * be close to zero because LNs are not evicted based on LRU. If any of the
 * three values are large, this points to a potential GC performance problem.
 * The GC logs should be consulted to confirm this.</p>
 *
 * <p>Large values for {@code NDirtyNodesEvicted} indicate that the cache is
* severely undersized and there is a risk of using all available disk space and
* severe performance problems. Dirty nodes are evicted last (after evicting all
* non-dirty nodes) because they must be written to disk. This causes excessive
* writing and JE log cleaning may be unproductive.</p>
 *
 * <p>Another type of eviction tuning for the cache involves changing the
 * number of bytes evicted each time an evictor thread is awoken:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_EVICT_BYTES}</li>
 * </ul>
 *
 * <p>If the number of bytes is too large, it may cause a noticeable spike in
 * eviction activity, reducing resources available to other threads. If the
 * number of bytes is too small, the overhead of waking the evictor threads
 * more often may be noticeable. The default values for this parameter is
 * generally a good compromise. This parameter also impacts critical eviction,
 * which is described next.</p>
 *
 * <h3><a id="cacheCriticalEviction">Cache Statistics: Critical
 * Eviction</a></h3>
 *
 * <p>The following stats indicate that critical eviction is occurring:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNBytesEvictedCritical}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CRITICAL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesEvictedCacheMode}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CACHEMODE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesEvictedDeamon}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_DAEMON_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesEvictedEvictorThread}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_EVICTORTHREAD_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesEvictedManual}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_MANUAL_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Eviction is performed by eviction pool threads, calls to {@link
 * Environment#evictMemory()} in application background threads, or via {@link
 * CacheMode#EVICT_LN} or {@link CacheMode#EVICT_BIN}. If these mechanisms are
 * not sufficient to evict memory from cache as quickly as CRUD operations are
 * adding memory to cache, then critical eviction comes into play. Critical
 * eviction is performed in-line in the thread performing the CRUD operation,
 * which is very undesirable since it increases operation latency.</p>
 *
 * <p>Critical eviction in the cache is indicated by large values for
 * {@code NBytesEvictedCritical}, as compared to the other {@code
 * NBytesEvictedXXX} stats.</p>
 *
 * <p>Additional stats indicating that background eviction threads may be
 * insufficient are:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNThreadUnavailable}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#THREAD_UNAVAILABLE_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Critical eviction can sometimes be reduced by changing {@link
 * EnvironmentConfig#EVICTOR_CRITICAL_PERCENTAGE} or modifying the eviction
 * thread pool parameters.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_CRITICAL_PERCENTAGE}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_CORE_THREADS}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_MAX_THREADS}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_KEEP_ALIVE}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, {@code EVICTOR_CRITICAL_PERCENTAGE} is set to
 * 20% rather than using the JE default of 0%.</p>
 *
 * <p>In the cache, critical eviction uses the same parameter as background
 * eviction for determining how many bytes to evict at one time:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_EVICT_BYTES}</li>
 * </ul>
 *
 * <p>Be careful when increasing this value, since this will cause longer
 * operation latencies when critical eviction is occurring in the cache.</p>
 *
 * <h3><a id="cacheLRUListContention">Cache Statistics: LRU List
 * Contention</a></h3>
 *
 * <p>Another common tuning issue involves thread contention on the cache LRU
 * lists, although there is no stat to indicate such contention. Since each
 * time a node is accessed it must be moved to the end of the LRU list, a
 * single LRU list would cause contention among threads performing CRUD
 * operations. By default there are 4 LRU lists for each cache. If contention
 * is noticeable on internal Evictor.LRUList method, consider increasing the
 * number of LRU lists:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_N_LRU_LISTS}</li>
 * </ul>
 *
 * <p>However, note that increasing the number of LRU lists will decrease the
 * accuracy of the LRU.</p>
 *
 * <h3><a id="cacheDebugging">Cache Statistics: Debugging</a></h3>
 *
 * <p>The following cache stats are unlikely to be needed for monitoring or
 * tuning, but are sometimes useful for debugging and testing.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getDataBytes}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getAdminBytes}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#MB_ADMIN_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesTargeted}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_TARGETED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesStripped}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_STRIPPED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesPutBack}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_PUT_BACK_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesMovedToDirtyLRU}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MOVED_TO_PRI2_LRU_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNNodesSkipped}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_SKIPPED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNRootNodesEvicted}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_ROOT_NODES_EVICTED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINsFetchMissRatio}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_RATIO_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINSparseTarget}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_SPARSE_TARGET_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINNoTarget}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_NO_TARGET_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getMixedLRUSize}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#PRI1_LRU_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getDirtyLRUSize}</td>
 *   <td>
 *    {@value com.sleepycat.je.evictor.EvictorStatDefinition#PRI2_LRU_SIZE_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Likewise, the following cache configuration params are unlikely to be
 * needed for tuning, but are sometimes useful for debugging and testing.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#ENV_DB_EVICTION}</li>
 *     <li>{@link EnvironmentConfig#TREE_MIN_MEMORY}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_FORCED_YIELD}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_ALLOW_BIN_DELTAS}</li>
 * </ul>
 *
 * <h2><a id="cleaner">Cleaning Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_DESC}</p>
 *
 * <p>The JE cleaner is responsible for "disk garbage collection" within
 * JE's log structured (append only) storage system. Data files (.jdb files),
 * which are also called log files, are cleaned and deleted as their
 * contents become obsolete. See this
 * <a href="{@docRoot}/../GettingStartedGuide/logfilesrevealed.html">
 * introduction to JE data files</a>.</p>
 *
 * <h3><a id="cleanerUtil">Cleaning Statistics: Utilization</a></h3>
 *
 * <p>By utilization we mean the ratio of utilized size to the total size of
 * the <a href="#cleanerDiskSpace">active data files</a>. The cleaner is run
 * when overall utilization (for all active files) drops below the target
 * utilization, which is specified by {@link
 * EnvironmentConfig#CLEANER_MIN_UTILIZATION}. The cleaner attempts to
 * maintain overall utilization at the target level. In addition, a file
 * will be cleaned if its individual utilization drops below {@link
 * EnvironmentConfig#CLEANER_MIN_FILE_UTILIZATION}, irrespective of overall
 * utilization.</p>
 *
 * <p>Current (actual) utilization is indicated by the following stats.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getCurrentMinUtilization}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MIN_UTILIZATION_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getCurrentMaxUtilization}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MAX_UTILIZATION_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If TTL is not used, the minimum and maximum utilization will be the
 * same. If TTL is used, the minimum and maximum define a range that bounds
 * the actual utilization. The current utilization is not known precisely when
 * TTL is used because of the potential overlap between expired data and
 * data that has become obsolete due to updates or deletions. See
 * <a href="#cleanerTTL">Cleaning Statistics: TTL and expired data</a>.</p>
 *
 * <p>If the cleaner is successfully maintaining the target utilization, the
 * current utilization (indicated by the above stats) is normally slightly
 * lower than the target utilization. This is because the cleaner is not
 * activated until current utilization drops below the target utilization and
 * it takes time for the cleaner to free space and raise the current
 * utilization. If the current utilization is significantly lower than the
 * target utilization (e.g., more than five percentage points lower), this
 * typically means the cleaner is unable to maintain the target utilization.
 * (When the minimum and maximum utilization stats are unequal, we recommend
 * using the maximum utilization for this determination.)</p>
 *
 * <p>When the cleaner is unable to maintain the target utilization, it will
 * clean files continuously in an attempt to reach the target. This will use
 * significant system resources in the best case and will use all available
 * disk space in the worst case, so the source of the problem should be
 * identified and corrected using the guidelines below.</p>
 * <ul>
 *     <li>One possibility is that the cleaner is unable to keep up simply
 *     because there are many more application threads generating waste than
 *     there are cleaner threads. To rule this out, try increasing the
 *     number of {@link EnvironmentConfig#CLEANER_THREADS cleaner threads}.
 *     For example, the NoSQL DB product uses two cleaner threads.</li>
 *
 *     <li>The cleaner may be able to keep up with generated waste, but due to
 *     cleaning efficiency factors (explained in the next section) it may
 *     not be able to maintain the configured target utilization, or it may be
 *     consuming large amounts of resources in order to do so. In this case,
 *     configuring a lower {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION
 *     target utilization} is one solution. For example, the NoSQL DB product
 *     uses a target utilization of 40%. See the next section for additional
 *     guidelines.</li>
 *
 *     <li>In extreme cases, cleaning efficiency factors make it impossible for
 *     the cleaner to make forward progress, meaning that more obsolete space
 *     is generated by cleaning than can be reclaimed. This will eventually
 *     result in using all available disk space. To avoid this, follow the
 *     guidelines above and in the next section.</li>
 * </ul>
 *
 * <h3><a id="cleanerEfficiency">Cleaning Efficiency</a></h3>
 *
 * <p>The general guidelines for ensuring that the cleaner can maintain
 * the target utilization are:</p>
 * <ol>
 *     <li><em>ensure that the JE cache is <a href="#cacheSizing">sized
 *     appropriately</a></em>, and</li>
 *
 *     <li><em>avoid large record keys, especially when the data size is small
 *     yet too large to be embedded in the BIN (as discussed below)</em>.</li>
 * </ol>
 * <p>This remainder of this section is intended to help understand the
 * reasons for these recommendations and to aid in advanced tuning.</p>
 *
 * <p>A JE data file consists mainly of Btree nodes, which include internal
 * nodes (INs) and leaf nodes (LNs). Each IN contains the keys of roughly
 * {@link EnvironmentConfig#NODE_MAX_ENTRIES 100 records}, while an LN
 * contains the key and data of a single record. When the cleaner processes
 * a data file it migrates (copies) active LNs to the end of the log, and
 * dirties their parent BINs (bottom internal nodes). Active INs are dirtied
 * but not immediately copied. The next checkpoint will then write the INs
 * dirtied as a result of cleaning the file:
 * <ul>
 *     <li>The BIN parents of the active LNs from the cleaned file.</li>
 *     <li>The active INs from the cleaned file, and their parent INs.</li>
 * </ul>
 * <p>Finally, now that the persistent form of the Btree contains no
 * references to the cleaned file, the file can be deleted. (In HA
 * environments the file is not deleted immediately as
 * <a href="#cleanerDiskSpace">will be discussed</a>.)</p>
 *
 * <p>When LNs are migrated, logging of their dirtied parent BINs causes the
 * previous version of these BINs to become obsolete. In many cases the
 * previous version may be in a different file than the cleaned file. So
 * although the cleaner reclaims the space for the obsolete data in the
 * cleaned file, it also creates some amount of additional obsolete space.</p>
 *
 * <p>The ratio of reclaimed space to additional obsolete space determines the
 * maximum utilization that can result from cleaning. If this maximum is less
 * than the target utilization, the cleaner will run continuously in an attempt
 * to reach the target and will consume large amounts of system resources.
 * Several factors influence how much additional obsolete space is created:</p>
 * <ul>
 *     <li>The lower the utilization of the file selected for cleaning, the
 *     less active LNs are migrated. This means less parent BINs are dirtied
 *     and logged, and therefore less obsolete space is created. For this
 *     reason, specifying a lower target utilization will cause cleaning to be
 *     less expensive. Also, this is why JE always selects files with the
 *     lowest utilization for cleaning. Some application workloads vary over
 *     time and create a mix of high and low utilization files, while others
 *     are consistent over time and all files have the same utilization;
 *     cleaning will be more efficient for workloads of the first type.</li>
 *
 *     <li>A special case is when a records' data is stored (embedded) in the
 *     BIN. This is referred to as an <em>embedded LN</em>. Embedded LNs are
 *     not migrated by the cleaner (they are no longer needed after
 *     transaction processing), so embedded LNs do not cause the creation of
 *     additional obsolete space during cleaning. LNs are embedded in two
 *     situations:
 *     <ol>
 *         <li>All LNs are embedded in a {@link
 *         DatabaseConfig#setSortedDuplicates DB with duplicate keys}. Such
 *         databases are normally {@link SecondaryDatabase}s.</li>
 *
 *         <li>In a DB where duplicate keys are not allowed (which is the
 *         default for a {@link Database}), LNs are embedded when the data
 *         size is no larger than
 *         {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN}. Such databases
 *         are normally primary databases, but in rare cases can be
 *         {@link SecondaryDatabase}s.</li>
 *     </ol>
 *     </li>
 *
 *     <li>When non-embedded LNs have a relatively large data size, less
 *     LNs per file are migrated and therefore less obsolete space is
 *     created. On the other hand, when the data size is small, yet too
 *     large to be {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded},
 *     significant amounts of obsolete space may be created by cleaning. This
 *     is because many LNs are migrated per file, and for each of these LNs a
 *     BIN is dirtied. {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN} can be
 *     increased to solve this problem in some cases, but (as described in its
 *     javadoc) increasing this value will increase BIN cache usage and
 *     should be done with caution.</li>
 *
 *     <li>When LNs have a relatively large key size, their parent BINs are
 *     also larger. When these LNs are not embedded, the larger BIN size means
 *     that more obsolete space is created by cleaning. Even when the LNs
 *     <em>are</em> embedded, normal write operations will create more
 *     obsolete space and BIN cache usage will be increased.</li>
 *
 *     <li>For the reasons stated above, a worst case for creation of obsolete
 *     space during cleaning is when LNs have large keys and small data, yet
 *     not small enough to be embedded.</li>
 *
 *     <li>The larger the {@link EnvironmentConfig#CHECKPOINTER_BYTES_INTERVAL
 *     checkpoint interval}, the more likely it is that migration of two or
 *     more LNs will dirty a single parent BIN (assuming the absence of cache
 *     eviction). This causes less BINs to be logged as a result of migration,
 *     so less obsolete space is created. In other words, increasing the
 *     checkpoint interval increases write absorption. This is true for
 *     ordinary record write operations as well as LN migration.</li>
 * </ul>
 *
 * <p>Even when cleaning does not create significant amounts of additional
 * obsolete space, an <a href="#cacheSizing">undersized cache</a> can still
 * prevent the cleaner from maintaining the target utilization when eviction
 * of dirty BINs occurs. When eviction causes logging of dirty BINs, this
 * reduces or even cancels out the write absorption benefits that normally
 * occur due to periodic checkpoints. In the worst case, every record write
 * operation causes a BIN to be written as well, which means that large
 * amounts of obsolete data will be created at a high rate. The
 * {@link #getNDirtyNodesEvicted()} cache statistic can help to identify this
 * problem.</p>
 *
 * <p>Even when cleaning is maintaining the target utilization, it may
 * consume large amounts of system resources in order to do so. The
 * following indicators of cleaning activity can be used to get a rough idea
 * of the level of cleaning activity.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNCleanerRuns}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RUNS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCleanerEntriesRead}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ENTRIES_READ_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>As mentioned earlier, configuring a lower
 * {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION target utilization} is one
 * way to reduce cleaner resource consumption.</p>
 *
 * <p>The read IO caused by cleaning is indicated by the following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNCleanerDiskRead}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DISK_READS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The impact of cleaner read IO can sometimes be reduced by increasing the
 * {@link EnvironmentConfig#CLEANER_READ_SIZE read buffer size}.</p>
 *
 * <p>The write IO caused by cleaning is due to {@link #getNLNsMigrated()
 * active LN migration} and by logging of INs that were dirtied by the
 * cleaner. Both of these costs can be reduced by decreasing the
 * {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION target utilization}.
 * Logging of dirty INs can also be reduced by using smaller key sizes,
 * especially when the data size is small, yet too large to be embedded in
 * the BIN.</p>
 *
 * <p>When a workload involves inserting and deleting large numbers of
 * records, another way of increasing cleaner efficiency is to remove the
 * records using {@link WriteOptions#setTTL(int) TTL} or {@link
 * ExtinctionFilter record extinction}, rather than performing transactional
 * record deletions. When records have expired or become extinct, the cleaner
 * can discard the LNs without a Btree lookup as described in the next
 * section. Also, because there are no transactional deletions there is less
 * cleaner metadata and less writing overall.</p>
 *
 * <h3><a id="cleanerProcessing">Cleaning Statistics: Processing
 * Details</a></h3>
 *
 * <p>This section describes details of cleaner file processing. The stats in
 * this section are useful for internal analysis and debugging.</p>
 *
 * <p>When the cleaner processes a data file, it reads the Btree entries: LNs,
 * BIN-deltas, BINs and upper INs. The number of entries processed is
 * {@link #getNCleanerEntriesRead()}. (There are a small number of additional
 * non-Btree entries in each file that are always obsolete and are completely
 * ignored by the cleaner.)</p>
 *
 * <p>The first step of processing a Btree entry is to determine if it is
 * known-obsolete. A known-obsolete entry is one of the following:</p>
 * <ul>
 *     <li>A Btree entry that was recorded as obsolete in the cleaner's
 *     per-file metadata during transaction processing.</li>
 *
 *     <li>A Btree entry that belongs to a Database that has been
 *     {@link Environment#removeDatabase removed} or
 *     {@link Environment#truncateDatabase truncated}. Note that DBs are
 *     added to a pending DB queue if the removal or truncation is not yet
 *     complete; this is discussed in the next section.</li>
 *
 *     <li>An LN entry representing a record deletion in the transaction
 *     log.</li>
 *
 *     <li>An LN that has {@link #getNLNsExpired() expired}. Expired LNs
 *     result from the use of {@link WriteOptions#setTTL(int) TTL}.</li>
 *
 *     <li>An LN that has become {@link #getNLNsExtinct() extinct}. Extinct
 *     LNs result from using an {@link ExtinctionFilter} along with the
 *     {@link Environment#discardExtinctRecords discardExtinctRecords}
 *     method.</li>
 * </ul>
 *
 * <p>Known-obsolete entries are very inexpensive to process because no
 * Btree lookup is required to determine that they are obsolete, and they
 * can simply be discarded. The number of known-obsolete entries is the sum
 * of the following {@code XxxObsolete} stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsObsolete}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_OBSOLETE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINsObsolete}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_OBSOLETE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINDeltasObsolete}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_OBSOLETE_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Entries that are not known-obsolete must be processed by performing a
 * Btree lookup to determine whether they're active or obsolete. These are
 * indicated by the following {@code XxxCleaned} stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsCleaned}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_CLEANED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINsCleaned}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_CLEANED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINDeltasCleaned}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_CLEANED_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The sum of the {@code XxxObsolete} and {@code XxxCleaned} stats
 * is the {@link #getNCleanerEntriesRead() total processed}:</p>
 *
 * <pre style="margin-left: 2em">CleanerEntriesRead =
 *     (LNsObsolete + INsObsolete + BINDeltasObsolete) +
 *     (LNsCleaned + INsCleaned + BINDeltasCleaned)</pre>
 *
 * <p>The number of expired and extinct LNs are broken out as separate stats.
 * These are a subset of the known-obsolete LNs:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsExpired}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXPIRED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNLNsExtinct}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXTINCT_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If the Btree lookup does not find the entry, then it is actually
 * obsolete. This can happen for two reasons:</p>
 * <ul>
 *     <li>The obsolete entry was not recorded as obsolete in the cleaner
 *     metadata during transaction processing. The recording of this
 *     metadata is not always guaranteed.</li>
 *
 *     <li>The entry became obsolete during processing of the file. The cleaner
 *     loads its metadata when file processing starts, and this metadata is not
 *     updated during file processing.</li>
 * </ul>
 * <p>Such entries are indicated by the {@code XxxDead} stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsDead}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_DEAD_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINsDead}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_DEAD_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINDeltasDead}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_DEAD_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If the entry is active in the Btree, it must be preserved by the
 * cleaner. Such entries are indicated by the following stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNsMigrated}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MIGRATED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNLNsLocked}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_LOCKED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNINsMigrated}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_MIGRATED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBINDeltasMigrated}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_MIGRATED_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The stats above have the following meanings:</p>
 * <ul>
 *     <li>{@link #getNLNsMigrated() Migrated LNs} are logged when they
 *     are processed by the cleaner.</li>
 *
 *     <li>{@link #getNLNsLocked() Locked LNs} cannot be processed
 *     immediately and they are added to a pending queue. Pending LNs are
 *     discussed in the next section.</li>
 *
 *     <li>{@link #getNINsMigrated() Migrated INs} are simply marked dirty,
 *     and they will be logged by the next checkpoint.</li>
 *
 *     <li>{@link #getNBINDeltasMigrated() Migrated BIN-deltas} are also
 *     simply marked dirty.</li>
 * </ul>
 *
 * <p>The stats above provide a break down of cleaned entries as follows:</p>
 * <ul>
 *     <li>{@code LNsCleaned = LNsDead + LNsMigrated + LNsMarked +
 *     LNsLocked}</li>
 *
 *     <li>{@code INsCleaned = INsDead + INsMigrated}</li>
 *
 *     <li>{@code BINDeltasCleaned = BINDeltasDead + BINDeltasMigrated}</li>
 * </ul>
 *
 * <p>When LNs are processed, a queue is used to reduce Btree lookups.
 * LNs are added to the queue when cleaning is needed (they are not
 * known-obsolete). When the queue fills, the oldest LN in the queue is
 * processed. If the LN is found in the Btree, the other LNs in the queue are
 * checked to see if they have the same parent BIN. If so, these LNs can
 * be processed while the BIN is latched, without an additional Btree lookup.
 * The number of such LNs is indicated by the following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLNQueueHits}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNQUEUE_HITS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The LN queue is most beneficial when LNs are inserted or updated in
 * key order. The maximum size of the queue, expressed as its maximum memory
 * size, can be changed via the
 * {@link EnvironmentConfig#CLEANER_LOOK_AHEAD_CACHE_SIZE} param.</p>
 *
 * <h3><a id="cleanerPending">Cleaning Statistics: Pending LNs and
 * DBs</a></h3>
 *
 * <p>When the cleaner is processing a Btree entry (LN or IN) there are two
 * cases where completion of cleaning (and deletion of the file) must be
 * deferred.</p>
 * <ul>
 *     <li>If an LN that is potentially active (not known-obsolete) is
 *     write-locked, the cleaner cannot determine whether it must be
 *     migrated until the locking transaction ends, either by aborting or
 *     committing.</li>
 *
 *     <li>If an LN or IN belongs to a Database that is in the process of
 *     being removed or truncated, the LN or IN is considered
 *     known-obsolete but cleaner must wait until the DB removal/truncation
 *     is complete before the file can be deleted.</li>
 * </ul>
 *
 * <p>If one of these conditions occurs, the LN or DB is added to a pending
 * queue. The cleaner will periodically process the entries in the queue and
 * attempt to resolve them as follows.</p>
 * <ul>
 *     <li>When a pending LN is no longer write-locked, a Btree lookup is
 *     performed and the LN is either migrated or considered dead. The LN is
 *     removed from the pending queue.</li>
 *
 *     <li>When removal/truncation is complete for a pending DB, the DB
 *     is simply removed from the pending queue.</li>
 * </ul>
 *
 * <p>When there are no more pending LNs and DBs for a given file then
 * cleaning of the file will be considered complete and it will become a
 * candidate for deletion after the next checkpoint. If a pending entry
 * causes file deletion to be delayed, because the pending entries cannot be
 * resolved before the next checkpoint, a WARNING level message is logged
 * with more information about the pending entries.</p>
 *
 * <p>The following stats indicate the size of the pending LN queues, how many
 * LNs in the queue have been processed, and of those processed how many
 * remain unresolved because the record is still write-locked.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getPendingLNQueueSize}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LN_QUEUE_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNPendingLNsProcessed}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_PROCESSED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNPendingLNsLocked}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_LOCKED_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If pending LNs remain unresolved, this could mean an application or JE
 * bug has prevented a write-lock from being released. This could happen,
 * for example, if the application fails to end a transaction or close a
 * cursor. For such bugs, closing and re-opening the Environment is usually
 * needed to allow file deletion to proceed. If this occurs for multiple files
 * and is not resolved, it can eventually lead to an out-of-disk situation.</p>
 *
 * <p>The following stats indicate the size of the pending DB queue, how many
 * DBs in the queue have been processed, and of those processed how many
 * remain unresolved because the removal/truncation is still incomplete.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getPendingDBQueueSize}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DB_QUEUE_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNPendingDBsProcessed}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_PROCESSED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNPendingDBsIncomplete}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_INCOMPLETE_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If pending DBs remain unresolved, this may indicate that the
 * asynchronous portion of DB removal/truncation is taking longer than
 * expected. After a DB removal/truncation transaction is committed, JE
 * asynchronously counts the data for the DB obsolete.</p>
 *
 * <h3><a id="cleanerTTL">Cleaning Statistics: TTL and expired data</a></h3>
 *
 * <p>When the {@link WriteOptions#setTTL(int) TTL} feature is used, the
 * obsolete portion of the log includes data that has expired. An expiration
 * histogram is stored for each file and is used to compute the expired size.
 * The current {@link #getCurrentMinUtilization() minimum} and {@link
 * #getCurrentMaxUtilization() maximum} utilization are the lower and upper
 * bounds of computed utilization. They are different only when the TTL
 * feature is used, and some data in the file has expired while other data
 * has become obsolete for other reasons, such as record updates, record
 * deletions or checkpoints. In this case the strictly obsolete size and the
 * expired size may overlap because they are maintained separately.</p>
 *
 * <p>If the two sizes overlap completely then the minimum utilization is
 * correct, while if there is no overlap then the maximum utilization is
 * correct. Both utilization values trigger cleaning, but when there is
 * significant overlap, the cleaner will perform two-pass cleaning. The
 * following stats indicate the use of two-pass cleaning:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNCleanerTwoPassRuns}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TWO_PASS_RUNS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCleanerRevisalRuns}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REVISAL_RUNS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>In the first pass of two-pass cleaning, the file is read to recompute
 * obsolete and expired sizes, but the file is not cleaned. As a result of
 * recomputing the expired sizes, the strictly obsolete and expired sizes
 * will no longer overlap, and the minimum and maximum utilization will be
 * equal. If the file should still be cleaned, based on the recomputed
 * utilization, it is cleaned as usual, and in this case the number of
 * {@link #getNCleanerTwoPassRuns() two-pass runs} is incremented.</p>
 *
 * <p>If the file should not be cleaned because its recomputed utilization is
 * higher than expected, the file will not be cleaned. Instead, its recomputed
 * expiration histogram, which now has size information that does not overlap
 * with the strictly obsolete data, is stored for future use. By storing the
 * revised histogram, the cleaner can select the most appropriate files for
 * cleaning in the future. In this case the number of {@link
 * #getNCleanerRevisalRuns() revisal runs} is incremented, and the number of
 * {@link #getNCleanerRuns() total runs} is not incremented.
 *
 * <h3><a id="cleanerDiskSpace">Cleaning Statistics: Disk Space
 * Management</a></h3>
 *
 * <p>The JE cleaner component is also responsible for checking and enforcing
 * the {@link EnvironmentConfig#MAX_DISK} and {@link
 * EnvironmentConfig#FREE_DISK} limits, and for protecting cleaned files from
 * deletion while they are in use by replication, backups, etc. This
 * process is described in the {@link EnvironmentConfig#MAX_DISK} javadoc. The
 * stats related to disk space management are:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getActiveLogSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ACTIVE_LOG_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getAvailableLogSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_AVAILABLE_LOG_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getReservedLogSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RESERVED_LOG_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getProtectedLogSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getProtectedLogSizeMap()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_MAP_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getTotalLogSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TOTAL_LOG_SIZE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCleanerDeletions()}</td>
 *   <td>
 *    {@value com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DELETIONS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The space taken by all data files, {@code totalLogSize}, is divided into
 * categories according to these stats as illustrated below.</p>
 * <pre>
 *     /--------------------------------------------------\
 *     |                                                  |
 *     | Active files -- have not been cleaned            |
 *     |                 and cannot be deleted            |
 *     |                                                  |
 *     |             Utilization =                        |
 *     |    (utilized size) / (total active size)         |
 *     |                                                  |
 *     |--------------------------------------------------|
 *     |                                                  |
 *     | Reserved files -- have been cleaned and          |
 *     |                   can be deleted                 |
 *     |                                                  |
 *     | /----------------------------------------------\ |
 *     | |                                              | |
 *     | | Protected files -- temporarily in use by     | |
 *     | |                    replication, backups, etc.| |
 *     | |                                              | |
 *     | \----------------------------------------------/ |
 *     |                                                  |
 *     \--------------------------------------------------/
 * </pre>
 *
 * <p>A key point is that reserved data files will be deleted by JE
 * automatically to prevent violation of a disk limit, as long as the files
 * are not protected. This has two important implications:</p>
 * <ul>
 *     <li>The {@link #getCurrentMinUtilization() current utilization} stats
 *     are calculated based only on the active data files. Reserved files are
 *     ignored in this calculation.</li>
 *
 *     <li>The {@link #getAvailableLogSize() availableLogSize} stat includes
 *     the size of the reserved files that are not protected. These files
 *     will be deleted automatically, if this is necessary to allow write
 *     operations.</li>
 * </ul>
 *
 * <p>We strongly recommend using {@code availableLogSize} to monitor disk
 * usage and take corrective action well before this value reaches zero.
 * Monitoring the file system free space is not a substitute for this, since
 * the data files include reserved files that will be deleted by JE
 * automatically.</p>
 *
 * <p>Applications should normally define a threshold for {@code
 * availableLogSize} and raise an alert of some kind when the threshold is
 * reached. When this happens applications may wish to free space (by
 * deleting records, for example) or expand storage capacity. If JE write
 * operations are needed as part of this procedure, corrective action
 * must be taken while there is still enough space available to perform the
 * write operations.</p>
 *
 * <p>For example, to free space by deleting records requires enough space to
 * log the deletions, and enough temporary space for the cleaner to reclaim
 * space for the deleted records. As described in the sections above, the
 * cleaner uses more disk space temporarily in order to migrate LNs, and a
 * checkpoint must be performed before deleting the cleaned files.</p>
 *
 * <p>How much available space is needed is application specific and testing
 * may be required to determine the application's {@code availableLogSize}
 * threshold. Note that the default {@link EnvironmentConfig#FREE_DISK}
 * value, five GB, may or may not be large enough to perform the application's
 * recovery procedure. The default {@code FREE_DISK} limit is intended to
 * reserve space for recovery when application monitoring of
 * {@code availableLogSize} fails and emergency measures must be taken.</p>
 *
 * <p>If {@code availableLogSize} is unexpectedly low, it is possible that
 * protected files are preventing space from being reclaimed. This could be
 * due to replication, backups, etc. See {@link #getReservedLogSize()} and
 * {@link #getProtectedLogSizeMap()} for more information.</p>
 *
 * <p>It is also possible that data files cannot be deleted due to read-only
 * processes. When one process opens a JE environment in read-write mode and
 * one or more additional processes open the environment in {@link
 * EnvironmentConfig#setReadOnly(boolean) read-only} mode, the read-only
 * processes will prevent the read-write process from deleting data files.
 * For this reason, long running read-only processes are strongly
 * discouraged in a production environment. When data file deletion is
 * prevented for this reason, a SEVERE level message is logged with more
 * information.</p>
 *
 * <h2><a id="log">I/O Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.log.LogStatDefinition#GROUP_DESC}</p>
 *
 * <h3><a id="logFileAccess">I/O Statistics: File Access</a></h3>
 *
 * <p>JE accesses data files (.jdb files) via Java's standard file system
 * APIs. Because opening a file is relatively expensive, an LRU-based
 * cache of open file handles is maintained. The stats below indicate how
 * many cached file handles are currently open and how many open file
 * operations have taken place.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNOpenFiles()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_OPEN_FILES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNFileOpens()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_FILE_OPENS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>To prevent expensive file open operations during record read operations,
 * set {@link EnvironmentConfig#LOG_FILE_CACHE_SIZE} to the maximum number of
 * data files expected in the Environment.</p>
 *
 * <p>Note that JE may open the same file more than once. If a read operation
 * in one thread is accessing a file via its cached handle and another thread
 * attempts to read from the same file, a temporary handle is opened just for
 * the duration of the read. The {@link #getNFileOpens()} stat includes open
 * operations for both cached file handles and temporary file handles.
 * Therefore, this stat cannot be used to determine whether the file cache
 * is too small.</p>
 *
 * <p>When a file read is performed, it is always possible for the read buffer
 * size to be smaller than the log entry being read. This is because JE's
 * append-only log contains variable sized entries rather than pages. If the
 * read buffer is too small to contain the entire entry, a repeat read with a
 * larger buffer must be performed. These additional reads can be reduced by
 * monitoring the following two stats and increasing the read buffer size as
 * described below.</p>
 *
 * <p>When Btree nodes are read at known file locations (by user API
 * operations, for example), the following stat indicates the number of
 * reads for which the thread-local buffer is too small:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNItemBufferTooSmall()}</td>
 *   <td>
 *     {@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_TOO_SMALL_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When the number of {@link #getNItemBufferTooSmall()} is significant,
 * consider increasing {@link EnvironmentConfig#LOG_FAULT_READ_SIZE}.</p>
 *
 * <p>When data files are read sequentially (by the cleaner, for example) the
 * following stat indicates the number of repeat reads:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNRepeatIteratorReads()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_REPEAT_ITERATOR_READS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When the number of {@link #getNRepeatIteratorReads()} is significant,
 * consider increasing {@link EnvironmentConfig#LOG_ITERATOR_MAX_SIZE}.</p>
 *
 * <p>The two groups of stats below indicate JE file system reads and writes
 * as number of operations and number of bytes. These stats are roughly
 * divided into random and sequential operations by assuming that storage
 * devices can optimize for sequential access if two consecutive operations
 * are performed one MB or less apart in the same file. This categorization
 * is approximate and may differ from the actual number depending on the
 * type of disks and file system, disk geometry, and file system cache
 * size.</p>
 *
 * <p>The JE file read and write stats can sometimes be useful for debugging
 * or for getting a rough idea of I/O characteristics. However, monitoring
 * of system level I/O stats (e.g., using {@code iostat}) gives a more
 * accurate picture of actual I/O since access via the buffer cache is
 * not included. In addition the JE stats are not broken out by operation
 * type and therefore don't add a lot of useful information to the system
 * level I/O stats, other than the rough division of random and sequential
 * I/O.</p>
 *
 * <p>The JE file read stats are:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNRandomReads()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNRandomReadBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READ_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNSequentialReads()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNSequentialReadBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READ_BYTES_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>JE file read stats include file access resulting from the following
 * operations. Because internal operations are included, it is not practical
 * to correlate these stats directly to user operations.</p>
 * <ul>
 *     <li>User read operations via the JE APIs, e.g.,
 *     {@link Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
 *     ReadOptions) Database.get} and
 *     {@link Cursor#get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)
 *     Cursor.get}.</li>
 *
 *     <li>Utility operations that access records such as {@link
 *     EnvironmentConfig#VERIFY_BTREE Btree verification}.</li>
 *
 *     <li>In a replicated environment, reads are performed by replication and
 *     mastership changes (syncup). In all environments, reads are performed
 *     by recovery (Environment open) and <a href="#cleaner">log cleaning</a>.
 *     Log cleaning is typically a significant contributor to read I/O.</li>
 *
 *     <li>Note that the reads above can cause more read I/O than expected
 *     when {@link #getNRepeatFaultReads()} or
 *     {@link #getNRepeatIteratorReads()} are consistently non-zero.</li>
 *
 *     <li>Note that while {@link EnvironmentConfig#VERIFY_LOG log
 *     verification} does perform read I/O, this I/O is <em>not</em> included
 *     in the JE file read stats. The same is true for a
 *     {@link com.sleepycat.je.rep.NetworkRestore} in a replicated
 *     environment: the read I/O on the source node is not counted in the JE
 *     file read stats.</li>
 * </ul>
 *
 * <p>The JE file write stats are:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNRandomWrites()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNRandomWriteBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITE_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNSequentialWrites()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNSequentialWriteBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITE_BYTES_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>JE file write stats include file access resulting from the following
 * operations. As with the read stats, because internal operations are
 * included it is not practical to correlate the write stats directly to user
 * operations.</p>
 * <ul>
 *     <li>User write operations via the JE APIs, e.g.,
 *     {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
 *     WriteOptions) Database.put} and
 *     {@link Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)
 *     Cursor.put}.</li>
 *
 *     <li>A small number of internal record write operations are performed
 *     to maintain JE internal data structures, e.g., cleaner metadata.</li>
 *
 *     <li>Writes are performed by <a href="#ckpt">checkpointing</a> and
 *     <a href="#cacheEviction">eviction</a>. Checkpoints are typically a
 *     significant contributor to write I/O.</li>
 *
 *     <li>Note that while {@link com.sleepycat.je.rep.NetworkRestore} does
 *     perform write I/O, this I/O is <em>not</em> included in the JE file
 *     write stats.</li>
 * </ul>
 *
 * <h3><a id="logCritical">I/O Statistics: Logging Critical Section</a></h3>
 *
 * <p>JE uses an append-only storage system where each log entry is
 * assigned an LSN (log sequence number). The LSN is a 64-bit integer
 * consisting of two 32-bit parts: the file number is the high order
 * 32-bits and the file offset is the low order 32-bits.</p>
 *
 * <p>LSNs are used in the Btree to reference child nodes from their parent
 * node. Therefore a node's LSN is assigned when the node is written,
 * including the case where the write is <a href="#logBuffer">buffered</a>.
 * The next LSN to be assigned is indicated by the following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getEndOfLog()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_END_OF_LOG_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>LSN assignment and assignment of <a href="#logBuffer">log buffer</a>
 * space must be performed serially, and therefore these operations occur in
 * a logging critical section. In general JE strives to do as little
 * additional work as possible in the logging critical section. However, in
 * certain cases additional operations are performed in the critical section
 * and these generally impact performance negatively. These special cases
 * will be noted in the sections that follow.</p>
 *
 * <h3><a id="logBuffer">I/O Statistics: Log Buffers</a></h3>
 *
 * <p>A set of JE log buffers is used to buffer writes. When write operations
 * use {@link SyncPolicy#NO_SYNC}, a file write is not performed until a log
 * buffer is filled. This positively impacts performance by reducing the
 * number of file writes. Note that checkpoint writes use {@code NO_SYNC}, so
 * this benefits performance even when user operations do not use
 * {@code NO_SYNC}.</p>
 *
 * <p>(When {@link SyncPolicy#SYNC} or {@link SyncPolicy#WRITE_NO_SYNC}
 * is used, the required file write and fsync are performed using a group
 * commit mechanism, which is described <a href="#logFsync">further
 * below</a>.)</p>
 *
 * <p>The size and number of log buffers is configured using
 * {@link EnvironmentConfig#LOG_BUFFER_SIZE},
 * {@link EnvironmentConfig#LOG_NUM_BUFFERS} and
 * {@link EnvironmentConfig#LOG_TOTAL_BUFFER_BYTES}. The resulting total size
 * and number of buffers is indicated by the following stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getBufferBytes()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LBFP_BUFFER_BYTES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNLogBuffers()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LBFP_LOG_BUFFERS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The default buffer size (one MB) is expected to be optimal for most
 * applications. In NoSQL DB, the default buffer size is used. However, if an
 * individual entry (e.g., a BIN or LN) is larger than the buffer size, the
 * log buffer mechanism is bypassed and this can negatively impact
 * performance. When writing such an entry, the write occurs in the critical
 * section using a temporary buffer, and any dirty log buffers all also
 * written in the critical section. When this occurs it is indicated by the
 * following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNTempBufferWrites()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_TEMP_BUFFER_WRITES_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When {@link #getNTempBufferWrites()} is consistently non-zero, consider
 * increasing the log buffer size.</p>
 *
 * <p>The number of buffers also impacts write performance when many threads
 * are performing write operations. The use of multiple buffers allows one
 * writing thread to flush the completed dirty buffers while other writing
 * threads add entries to "clean" buffers (that have already been written).</p>
 *
 * <p>If many threads are adding to clean buffers while the completed dirty
 * buffers are being written, it is possible that no more clean buffers will
 * be available for adding entries. When this happens, the dirty buffers
 * are flushed in the critical section, which can negatively impact
 * performance. This is indicated by the following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNNoFreeBuffer()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LBFP_NO_FREE_BUFFER_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When {@link #getNNoFreeBuffer()} is consistently non-zero, consider
 * increasing the number of log buffers.</p>
 *
 * <p>The number of log buffers also impacts read performance. JE read
 * operations use the log buffers to read entries that were recently written.
 * This occurs infrequently in the case of user read operations via the JE
 * APIs, since recently written data is infrequently read and is often
 * resident in the <a href="#cache">cache</a>. However, it does occur
 * frequently and is an important factor in the following cases:</p>
 * <ul>
 *     <li>A transaction abort reads the entries written by the transaction
 *     in order to undo them. These entries should normally be available in
 *     the log buffers. Avoiding file reads reduces the latency of aborted
 *     transactions.</li>
 *
 *     <li>In a replicated environment, the master node must read recently
 *     written entries needed by replicas or secondary nodes. By reading these
 *     entries from the log buffers, the likelihood of the need for file reads
 *     is reduced, and this can prevent lagging replicas from falling further
 *     behind.</li>
 * </ul>
 *
 * <p>Because of the last point above involving replication, in NoSQL DB the
 * number of log buffers is set to 16. In general we recommend configuring 16
 * buffers or more for a replicated environment.</p>
 *
 * <p>The following stats indicate the number of requests to read log entries
 * by LSN, and the number that were not found in the log buffers.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNNotResident()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LBFP_NOT_RESIDENT_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNCacheMiss()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LBFP_MISS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>In general these two stats are used only for internal JE debugging and
 * are not useful to the application. This is because
 * {@link #getNNotResident()} is roughly the sum of the
 * {@code VLSNIndex nMisses} replication stat and the cache fetch miss stats:
 * {@link #getNLNsFetchMiss()}, {@link #getNBINsFetchMiss()},
 * {@link #getNFullBINsMiss} and {@link #getNUpperINsFetchMiss()}.</p>
 *
 * <h3><a id="logItemBuffer">I/O Statistics: Log Item Buffers</a></h3>
 *
 * <p>Log item buffers are used when fetching a log entry from disk and for
 * serializing a log entry before logging it. When fetching, the buffer is only
 * needed until the entry has been deserialized. When logging, the buffer is
 * only needed for the duration of the logging operation, or for replicated
 * entries, until it has been sent to all active replicas. To avoid
 * unnecessary memory allocation and its associated Java GC costs,
 * thread-local item buffers are used for fetching and for logging
 * non-replicated items, and a pool of item buffers is used for logging
 * replicated items.</p>
 *
 * <p>Thread-local item buffers are used if
 * {@link EnvironmentConfig#LOG_ITEM_THREAD_LOCAL} is true (the default)
 * and a pool of item buffers is used if
 * {@link EnvironmentConfig#LOG_ITEM_POOL_SIZE} is non-zero (the default is
 * non-zero).</p>
 *
 * <p>The following stats indicate the number of items that could not use a
 * a thread-local or pooled buffer because the shared buffers were too small
 * or the buffer pool was not large enough. In these cases, a transient buffer
 * had to be allocated for each item fetched or logged.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNItemBufferTooSmall()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_TOO_SMALL_NAME}
 *   </td></tr>
 *  <tr><td>{@link #getNItemBufferPoolEmpty()}</td>
 *   <td>{@value com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_POOL_EMPTY_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>If these stats indicate that a significant number of logged items cannot
 * use the shared buffers, and reducing GC costs is desired, consider
 * increasing {@link EnvironmentConfig#LOG_FAULT_READ_SIZE} or
 * {@link EnvironmentConfig#LOG_ITEM_POOL_SIZE}.</p>
 *
 * <h3><a id="logWriteQueue">I/O Statistics: The Write Queue</a></h3>
 *
 * <p>JE performs special locking to prevent an fsync and a file write from
 * executing concurrently.
 * TODO: Why is this disallowed for all file systems?.</p>
 *
 * <p>The write queue is a single, low-level buffer that reduces blocking due
 * to a concurrent <a href="#logFsync">fsync</a> and file write request.
 * When a write of a dirty log buffer is needed to free a log buffer for a
 * {@link SyncPolicy#NO_SYNC} operation (i.e., durability is not required),
 * the write queue is used to hold the data temporarily and allow a log
 * buffer to be freed.</p>
 *
 * <p>Use of the write queue is strongly recommended since there is no known
 * drawback to using it. It is enabled by default and in NoSQL DB. However,
 * it can be disabled if desired by setting
 * {@link EnvironmentConfig#LOG_USE_WRITE_QUEUE} to {@code false}.</p>
 *
 * <p>The following stats indicate use of the write queue for satisfying
 * file write and read requests. Note that when the write queue is enabled,
 * all file read requests must check the write queue to avoid returning stale
 * data.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNWritesFromWriteQueue()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITES_FROM_WRITEQUEUE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesWrittenFromWriteQueue()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNReadsFromWriteQueue()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_READS_FROM_WRITEQUEUE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBytesReadFromWriteQueue()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_READ_FROM_WRITEQUEUE_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The default size of the write queue (one MB) is expected to be adequate
 * for most applications. Note that the write queue size should never be
 * smaller than the log buffer size (which is also one MB by default). In
 * NoSQL DB, the default sizes for the write queue and the log buffer are
 * used.</p>
 *
 * <p>However, when many {@code NO_SYNC} writes are requested during an fsync,
 * some write requests may have to block until the fsync is complete. This
 * is indicated by the following stats:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNWriteQueueOverflow()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNWriteQueueOverflowFailures()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>When a {@code NO_SYNC} write request occurs during an fsync and the
 * size of the write request's data is larger than the free space in the
 * write queue, the {@link #getNWriteQueueOverflow()} stat is incremented.
 * When this stat is consistently non-zero, consider the following possible
 * reasons and remedies:</p>
 * <ul>
 *     <li>The log buffer size may be larger than the write queue size. Ensure
 *     that the {@link EnvironmentConfig#LOG_WRITE_QUEUE_SIZE} is at least as
 *     large as the {@link EnvironmentConfig#LOG_BUFFER_SIZE}.</li>
 *
 *     <li>An individual log entry (e.g., a BIN or LN) may be larger than the
 *     log buffer size, as indicated by the {@link #getNTempBufferWrites()}
 *     stat. Consider increasing {@code LOG_WRITE_QUEUE_SIZE} and/or
 *     {@code LOG_BUFFER_SIZE} such that {@link #getNTempBufferWrites()} is
 *     consistently zero.</li>
 *
 *     <li>When multiple threads perform {@code NO_SYNC} write requests
 *     during a single fync, the write queue may not be large enough to
 *     prevent overflows. After the two causes above have been ruled out,
 *     consider increasing the {@code LOG_WRITE_QUEUE_SIZE}.</li>
 * </ul>
 *
 * <p>When such a write queue overflow occurs, JE will wait for the fsync to
 * complete, empty the write queue by writing it to the file, and attempt
 * again to add the data to the write queue. If this fails again because
 * there is still not enough free space in the write queue, then the
 * {@link #getNWriteQueueOverflowFailures()} stat is incremented. In this
 * case the data is written to the file rather than adding it to the write
 * queue, even though this may require waiting for an fsync to complete.</p>
 *
 * <p>If {@link #getNWriteQueueOverflowFailures()} is consistently non-zero,
 * the possible causes are the same as those listed above, and the remedies
 * described above should be applied.</p>
 *
 * <h3><a id="logFsync">I/O Statistics: Fsync and Group Commit</a></h3>
 *
 * <p>When {@link SyncPolicy#SYNC} or {@link SyncPolicy#WRITE_NO_SYNC} is
 * used for transactional write operations, the required file write and fsync
 * are performed using a <em>group commit</em> mechanism. In the presence of
 * concurrent transactions, this mechanism often allows performing a single
 * write and fsync for multiple transactions, while still ensuring that the
 * write and fsync are performed before the transaction {@code commit()}
 * method (or the {@code put()} or {@code delete()} operation method in this
 * case of auto-commit) returns successfully.</p>
 *
 * <p>First note that not all file write and fsync operations are due to
 * user transaction commits, and not all fsyncs use the group commit
 * mechanism.</p>
 * <ul>
 *     <li>In several cases a transaction is committed for an internal
 *     database, or a special log entry is written for an internal operation,
 *     with {@code SYNC} or {@code WRITE_NO_SYNC} durability. In these cases
 *     the group commit mechanism <em>is</em> used.</li>
 *
 *     <li>When {@link Environment#flushLog} is called by the application,
 *     a file write is performed and, if the {@code fsync} parameter is true,
 *     an fsync is also performed. A file write and fsync are also performed
 *     after an interval of no write activity, as determined by
 *     {@link EnvironmentConfig#LOG_FLUSH_NO_SYNC_INTERVAL} and
 *     {@link EnvironmentConfig#LOG_FLUSH_SYNC_INTERVAL}. In these cases the
 *     group commit mechanism is <em>not</em> used.</li>
 *
 *     <li>In several cases an fsync is performed internally that does
 *     <em>not</em> use the group commit mechanism. These cases include:
 *     completion of the last data file when a new file is created, flushing
 *     of metadata before deleting a cleaned file, and Environment open and
 *     close.</li>
 * </ul>
 *
 * <p>The following stats describe all fsyncs performed by JE, whether or not
 * the group commit mechanism is used.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNLogFSyncs()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_LOG_FSYNCS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getFSyncAvgMs()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_AVG_MS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getFSync95Ms()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_95_MS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getFSync99Ms()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_99_MS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getFSyncMaxMs()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_MAX_MS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Long fsync times often result in long transaction latencies. When this
 * is indicated by the above stats, be sure to ensure that the linux page
 * cache has been tuned to permit the OS to write asynchronously to disk
 * whenever possible. For the NoSQL DB product this is described under
 * <a href="https://docs.oracle.com/cd/NOSQL/html/AdminGuide/linuxcachepagetuning.html">Linux
 * Page Cache Tuning</a>. To aid in diagnosing long fsyncs, a WARNING level
 * message is logged when the maximum fsync time exceeds
 * {@link EnvironmentConfig#LOG_FSYNC_TIME_LIMIT}</p>
 *
 * <p>The following stats indicate when group commit is requested for a write
 * operation. Group commit requests include all user transactions with {@code
 * SYNC} or {@code WRITE_NO_SYNC} durability, as well as the internal JE write
 * operations that use group commit.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNGroupCommitRequests()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_N_GROUP_COMMIT_REQUESTS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNFSyncRequests()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNC_REQUESTS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>All group commit requests result in a group commit operation that
 * flushes all dirty log buffers and the write queue using a file write.
 * In addition, requests using {@code SYNC} durability will cause the group
 * commit operation to include an fsync.</p>
 *
 * <p>Because group commit operations are performed serially, while a group
 * commit is executing in one thread, one or more other threads may be
 * waiting to perform a group commit. The group commit mechanism works by
 * forming a group containing the waiting threads. When the prior group
 * commit is finished, a single group commit is performed on behalf of the
 * new group in one of this group's threads, which is called the
 * <em>leader</em>. The other threads in the group are called
 * <em>waiters</em> and they proceed only after the leader has finished the
 * group commit.</p>
 *
 * <p>If a waiter thread waits longer than
 * {@link EnvironmentConfig#LOG_FSYNC_TIMEOUT} for the leader to finish the
 * group commit operation, the waiter will remove itself from the group and
 * perform a group commit operation independently. The number of such
 * timeouts is indicated by the following stat:</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNFSyncTimeouts()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_TIMEOUTS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>The timeout is intended to prevent waiter threads from waiting
 * indefinitely due to an unexpected problem. If {@link #getNFSyncTimeouts()}
 * is consistently non-zero and the application is performing normally in
 * other respects, consider increasing
 * {@link EnvironmentConfig#LOG_FSYNC_TIMEOUT}.</p>
 *
 * <p>The following stat indicates the number of group commit operations that
 * included an fsync. There is currently no stat available indicating the
 * number of group commit operations that did not include an fsync.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNFSyncs()}</td>
 *   <td>
 *    {@value com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNCS_DESC}
 *   </td></tr>
 * </table>
 *
 * <p>Note that {@link #getNFSyncs()} is a subset of the
 * {@link #getNLogFSyncs()} total that is described further above.</p>
 *
 * <h2><a id="incomp">Node Compression Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_DESC}</p>
 *
 * <!--
 *     Also responsible for reclaiming memory for expired records.
 * -->
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getSplitBins()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_SPLIT_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getDbClosedBins()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_DBCLOSED_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getCursorsBins()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_CURSORS_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNonEmptyBins()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_NON_EMPTY_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getProcessedBins()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_PROCESSED_BINS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getInCompQueueSize()}</td>
 *   <td>
 *    {@value com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_QUEUE_SIZE_DESC}
 *   </td></tr>
 * </table>
 *
 * <h2><a id="ckpt">Checkpoint Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_DESC}</p>
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNCheckpoints()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_CHECKPOINTS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getLastCheckpointInterval()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_INTERVAL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNFullINFlush()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_IN_FLUSH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNFullBINFlush()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_BIN_FLUSH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNDeltaINFlush()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_DELTA_IN_FLUSH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getLastCheckpointId()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPTID_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getLastCheckpointStart()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_START_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getLastCheckpointEnd()}</td>
 *   <td>
 *    {@value com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_END_DESC}
 *   </td></tr>
 * </table>
 *
 * <h2><a id="lock">Lock Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.txn.LockStatDefinition#GROUP_DESC}</p>
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getNReadLocks()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_READ_LOCKS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNWriteLocks()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_WRITE_LOCKS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNOwners()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_OWNERS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNRequests()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_REQUESTS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNTotalLocks()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_TOTAL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNWaits()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNWaiters()}</td>
 *   <td>
 *    {@value com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITERS_DESC}
 *   </td></tr>
 * </table>
 *
 * <h2><a id="throughput">Operation Throughput Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_DESC}</p>
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getPriSearchOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriSearchFailOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_FAIL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecSearchOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecSearchFailOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_FAIL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriPositionOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_POSITION_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecPositionOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_POSITION_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriInsertOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriInsertFailOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_FAIL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecInsertOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_INSERT_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriUpdateOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_UPDATE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecUpdateOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_UPDATE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriDeleteOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getPriDeleteFailOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_FAIL_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getSecDeleteOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_DELETE_DESC}
 *   </td></tr>
 * </table>
 *
 * <h2><a id="btreeop">Btree Operation Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_DESC}</p>
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getRelatchesRequired()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_RELATCHES_REQUIRED_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getRootSplits()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_ROOT_SPLITS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBinDeltaGetOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_GETS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBinDeltaInsertOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_INSERTS_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBinDeltaUpdateOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_UPDATES_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getNBinDeltaDeleteOps()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_DELETES_DESC}
 *   </td></tr>
 * </table>
 *
 * <h2><a id="env">Miscellaneous Environment-Wide Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_DESC}</p>
 *
 * <p>The following statistics are available. More information will be
 * provided in a future release.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getEnvironmentCreationTime()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#ENV_CREATION_TIME_DESC}
 *   </td></tr>
 * </table>
 *
 * <!-- Hidden: For internal use: automatic backups
 * <h2><a id="#backup">Automatic Backup Statistics</a></h2>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_NAME}
 * <br>Description: {@value
 * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_DESC}</p>
 *
 * <p>The following statistics are available, but for internal use.</p>
 *
 * <table style="margin-left: 2em" border="1">
 *  <caption style='display:none'>Accessors and definitions</caption>
 *  <tr><td>{@link #getBackupCopyFilesCount()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_COUNT_DESC}
 *   </td></tr>
 *  <tr><td>{@link #getBackupCopyFilesMs()}</td>
 *   <td>
 *    {@value com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_MS_DESC}
 *   </td></tr>
 * </table>
 * -->
 *
 * @see <a href="{@docRoot}/../jconsole/JConsole-plugin.html">Viewing
 * Statistics with JConsole</a>
 */
public class EnvironmentStats implements Serializable {

    /*
    find/replace:
    public static final StatDefinition\s+(\w+)\s*=\s*new StatDefinition\(\s*(".*"),\s*(".*")(,?\s*\w*\.?\w*)\);
    public static final String $1_NAME =\n        $2;\n    public static final String $1_DESC =\n        $3;\n    public static final StatDefinition $1 =\n        new StatDefinition(\n            $1_NAME,\n            $1_DESC$4);
    */

    private static final long serialVersionUID = 1734048134L;

    private StatGroup incompStats;
    private StatGroup cacheStats;
    private StatGroup ckptStats;
    private StatGroup cleanerStats;
    private StatGroup logStats;
    private StatGroup lockStats;
    private StatGroup envImplStats;
    private StatGroup backupStats;
    private StatGroup btreeOpStats;
    private StatGroup throughputStats;
    private StatGroup taskCoordinatorStats;
    private StatGroup eraserStats;
    private StatGroup dbVerifyStats;
	private StatGroup bImgStats;

    /**
     * @hidden
     * Internal use only.
     */
    public EnvironmentStats() {
        incompStats = new StatGroup(INCompStatDefinition.GROUP_NAME,
                                    INCompStatDefinition.GROUP_DESC);

        cacheStats = new StatGroup(EvictorStatDefinition.GROUP_NAME,
                                   EvictorStatDefinition.GROUP_DESC);
        ckptStats = new StatGroup(CheckpointStatDefinition.GROUP_NAME,
                                  CheckpointStatDefinition.GROUP_DESC);
        cleanerStats = new StatGroup(CleanerStatDefinition.GROUP_NAME,
                                     CleanerStatDefinition.GROUP_DESC);
        logStats = new StatGroup(LogStatDefinition.GROUP_NAME,
                                 LogStatDefinition.GROUP_DESC);
        lockStats = new StatGroup(LockStatDefinition.GROUP_NAME,
                                  LockStatDefinition.GROUP_DESC);
        envImplStats = new StatGroup(DbiStatDefinition.ENV_GROUP_NAME,
                                     DbiStatDefinition.ENV_GROUP_DESC);
        backupStats = new StatGroup(DbiStatDefinition.BACKUP_GROUP_NAME,
                                    DbiStatDefinition.BACKUP_GROUP_DESC);
        btreeOpStats = new StatGroup(
            BTreeStatDefinition.BT_OP_GROUP_NAME,
            BTreeStatDefinition.BT_OP_GROUP_DESC);

        throughputStats =
            new StatGroup(DbiStatDefinition.THROUGHPUT_GROUP_NAME,
                          DbiStatDefinition.THROUGHPUT_GROUP_DESC);

        taskCoordinatorStats =
            new StatGroup(TaskCoordinator.StatDefs.GROUP_NAME,
                          TaskCoordinator.StatDefs.GROUP_DESC);

        eraserStats =
            new StatGroup(EraserStatDefinition.GROUP_NAME,
                          EraserStatDefinition.GROUP_DESC);

        dbVerifyStats =
            new StatGroup(DbVerifyStatDefinition.DB_VERIFY_GROUP_NAME,
                          DbVerifyStatDefinition.DB_VERIFY_GROUP_DESC);
        
        bImgStats = 
        		new StatGroup(BeforeImageIndexStatDefinition.GROUP_NAME,
                        BeforeImageIndexStatDefinition.GROUP_DESC);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public List<StatGroup> getStatGroups() {
        /*
         * If this deserialized object was serialized prior to the addition of
         * certain stat groups, these fields will be null. Initialize them now.
         */
        if (eraserStats == null) {
            eraserStats = new StatGroup(
                EraserStatDefinition.GROUP_NAME,
                EraserStatDefinition.GROUP_DESC);
        }
        if (taskCoordinatorStats == null) {
            taskCoordinatorStats = new StatGroup(
                TaskCoordinator.StatDefs.GROUP_NAME,
                TaskCoordinator.StatDefs.GROUP_DESC);
        }
        if (btreeOpStats == null) {
            btreeOpStats = new StatGroup(
                BTreeStatDefinition.BT_OP_GROUP_NAME,
                BTreeStatDefinition.BT_OP_GROUP_DESC);

        }
        if (backupStats == null) {
            backupStats = new StatGroup(
                DbiStatDefinition.BACKUP_GROUP_NAME,
                DbiStatDefinition.BACKUP_GROUP_DESC);
        }
        
		if (bImgStats == null) {
			bImgStats = new StatGroup(
				BeforeImageIndexStatDefinition.GROUP_NAME,
				BeforeImageIndexStatDefinition.GROUP_DESC);
		}

        return Arrays.asList(
            logStats, cacheStats, cleanerStats, incompStats,
            ckptStats, envImplStats, backupStats, btreeOpStats, lockStats,
            throughputStats, taskCoordinatorStats, eraserStats, dbVerifyStats, bImgStats);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public Map<String, StatGroup> getStatGroupsMap() {
        final HashMap<String, StatGroup> map = new HashMap<>();
        for (StatGroup group : getStatGroups()) {
            map.put(group.getName(), group);
        }
        return map;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setStatGroup(StatGroup sg) {

        switch (sg.getName()) {
        case INCompStatDefinition.GROUP_NAME:
            incompStats = sg;
            break;
        case EvictorStatDefinition.GROUP_NAME:
            cacheStats = sg;
            break;
        case CheckpointStatDefinition.GROUP_NAME:
            ckptStats = sg;
            break;
        case CleanerStatDefinition.GROUP_NAME:
            cleanerStats = sg;
            break;
        case LogStatDefinition.GROUP_NAME:
            logStats = sg;
            break;
        case LockStatDefinition.GROUP_NAME:
            lockStats = sg;
            break;
        case BTreeStatDefinition.BT_OP_GROUP_NAME:
            btreeOpStats = sg;
            break;
        case DbiStatDefinition.ENV_GROUP_NAME:
            envImplStats = sg;
            break;
        case DbiStatDefinition.BACKUP_GROUP_NAME:
            backupStats = sg;
            break;
        case DbiStatDefinition.THROUGHPUT_GROUP_NAME:
            throughputStats = sg;
            break;
        case TaskCoordinator.StatDefs.GROUP_NAME:
            taskCoordinatorStats = sg;
            break;
        case EraserStatDefinition.GROUP_NAME:
            eraserStats = sg;
            break;
        case DbVerifyStatDefinition.DB_VERIFY_GROUP_NAME:
            dbVerifyStats = sg;
            break;
        case BeforeImageIndexStatDefinition.GROUP_NAME:
            bImgStats = sg;
            break;
        default:
            throw EnvironmentFailureException.unexpectedState(
                "Invalid stat group name in setStatGroup " + sg.getName());
        }
    }

    /**
     * @hidden
     * Internal use only
     * For JConsole plugin support.
     */
    public static String[] getStatGroupTitles() {
        List<StatGroup> groups = new EnvironmentStats().getStatGroups();
        final String[] titles = new String[groups.size()];
        for (int i = 0; i < titles.length; i += 1) {
            titles[i] = groups.get(i).getName();
        }
        return titles;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setThroughputStats(StatGroup stats) {
        throughputStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setINCompStats(StatGroup stats) {
        incompStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setTaskCoordinatorStats(StatGroup stats) {
        taskCoordinatorStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setEraserStats(StatGroup stats) {
        eraserStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setCkptStats(StatGroup stats) {
        ckptStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setCleanerStats(StatGroup stats) {
        cleanerStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setLogStats(StatGroup stats) {
        logStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setMBAndEvictorStats(StatGroup clonedMBStats,
                                     StatGroup clonedEvictorStats) {
        cacheStats = clonedEvictorStats;
        cacheStats.addAll(clonedMBStats);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setLockStats(StatGroup stats) {
        lockStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setEnvStats(StatGroup stats) {
        envImplStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setBackupStats(StatGroup stats) {
        backupStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setBtreeOpStats(StatGroup stats) {
        btreeOpStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setDbVerifyStats(StatGroup stats) {
        dbVerifyStats = stats;
    }
    
    /**
     * @hidden
     * Internal use only.
     */
    public void setBImgStats(StatGroup stats) {
        bImgStats = stats;
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#ENV_CREATION_TIME_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#ENV_CREATION_TIME_NAME}</p>
     *
     * @see <a href="#env">Miscellaneous Environment-Wide Statistics</a>
     */
    public long getEnvironmentCreationTime() {
        return envImplStats.getLong(ENV_CREATION_TIME);
    }

    /* INCompressor stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_CURSORS_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_CURSORS_BINS_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getCursorsBins() {
        return incompStats.getLong(INCOMP_CURSORS_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_DBCLOSED_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_DBCLOSED_BINS_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getDbClosedBins() {
        return incompStats.getLong(INCOMP_DBCLOSED_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_QUEUE_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_QUEUE_SIZE_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getInCompQueueSize() {
        return incompStats.getLong(INCOMP_QUEUE_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_NON_EMPTY_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_NON_EMPTY_BINS_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getNonEmptyBins() {
        return incompStats.getLong(INCOMP_NON_EMPTY_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_PROCESSED_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_PROCESSED_BINS_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getProcessedBins() {
        return incompStats.getLong(INCOMP_PROCESSED_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_SPLIT_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.incomp.INCompStatDefinition#INCOMP_SPLIT_BINS_NAME}</p>
     *
     * @see <a href="#incomp">Node Compression Statistics</a>
     */
    public long getSplitBins() {
        return incompStats.getLong(INCOMP_SPLIT_BINS);
    }

    /* Checkpointer stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPTID_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPTID_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getLastCheckpointId() {
        return ckptStats.getLong(CKPT_LAST_CKPTID);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_CHECKPOINTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_CHECKPOINTS_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getNCheckpoints() {
        return ckptStats.getLong(CKPT_CHECKPOINTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_IN_FLUSH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_IN_FLUSH_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getNFullINFlush() {
        return ckptStats.getLong(CKPT_FULL_IN_FLUSH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_BIN_FLUSH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_FULL_BIN_FLUSH_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getNFullBINFlush() {
        return ckptStats.getLong(CKPT_FULL_BIN_FLUSH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_DELTA_IN_FLUSH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_DELTA_IN_FLUSH_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getNDeltaINFlush() {
        return ckptStats.getLong(CKPT_DELTA_IN_FLUSH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_INTERVAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_INTERVAL_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getLastCheckpointInterval() {
        return ckptStats.getLong(CKPT_LAST_CKPT_INTERVAL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_START_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_START_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getLastCheckpointStart() {
        return ckptStats.getLong(CKPT_LAST_CKPT_START);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_END_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.recovery.CheckpointStatDefinition#CKPT_LAST_CKPT_END_NAME}</p>
     *
     * @see <a href="#ckpt">Checkpoint Statistics</a>
     */
    public long getLastCheckpointEnd() {
        return ckptStats.getLong(CKPT_LAST_CKPT_END);
    }

    /* Cleaner stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MIN_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MIN_UTILIZATION_NAME}</p>
     *
     * @see <a href="#cleanerUtil">Cleaner Statistics: Utilization</a>
     * @since 6.5
     */
    public int getCurrentMinUtilization() {
        return cleanerStats.getInt(CLEANER_MIN_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MAX_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MAX_UTILIZATION_NAME}</p>
     *
     * @see <a href="#cleanerUtil">Cleaner Statistics: Utilization</a>
     * @since 6.5
     */
    public int getCurrentMaxUtilization() {
        return cleanerStats.getInt(CLEANER_MAX_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PREDICTED_MIN_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PREDICTED_MIN_UTILIZATION_NAME}</p>
     *
     * @see <a href="#cleanerUtil">Cleaner Statistics: Utilization</a>
     * @since 18.1
     */
    public int getPredictedMinUtilization() {
        return cleanerStats.getInt(CLEANER_PREDICTED_MIN_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PREDICTED_MAX_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PREDICTED_MAX_UTILIZATION_NAME}</p>
     *
     * @see <a href="#cleanerUtil">Cleaner Statistics: Utilization</a>
     * @since 18.1
     */
    public int getPredictedMaxUtilization() {
        return cleanerStats.getInt(CLEANER_PREDICTED_MAX_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RUNS_NAME}</p>
     *
     * This includes {@link #getNCleanerTwoPassRuns() two-pass runs} but not
     * {@link #getNCleanerRevisalRuns() revisal runs}.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNCleanerRuns() {
        return cleanerStats.getLong(CLEANER_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TWO_PASS_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TWO_PASS_RUNS_NAME}</p>
     *
     * @see <a href="#cleanerTTL">Cleaner Statistics: TTL and expired data</a>
     * @since 6.5.0
     */
    public long getNCleanerTwoPassRuns() {
        return cleanerStats.getLong(CLEANER_TWO_PASS_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REVISAL_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REVISAL_RUNS_NAME}</p>
     *
     * @see <a href="#cleanerTTL">Cleaner Statistics: TTL and expired data</a>
     * @since 6.5.0
     */
    public long getNCleanerRevisalRuns() {
        return cleanerStats.getLong(CLEANER_REVISAL_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DELETIONS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DELETIONS_NAME}</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     */
    public long getNCleanerDeletions() {
        return cleanerStats.getLong(CLEANER_DELETIONS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LN_QUEUE_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LN_QUEUE_SIZE_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public int getPendingLNQueueSize() {
        return cleanerStats.getInt(CLEANER_PENDING_LN_QUEUE_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DB_QUEUE_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DB_QUEUE_SIZE_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public int getPendingDBQueueSize() {
        return cleanerStats.getInt(CLEANER_PENDING_DB_QUEUE_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DISK_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DISK_READS_NAME}</p>
     *
     * @see <a href="#cleanerEfficiency">Cleaning Efficiency</a>
     */
    public long getNCleanerDiskRead() {
        return cleanerStats.getLong(CLEANER_DISK_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ENTRIES_READ_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ENTRIES_READ_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     * @see <a href="#cleanerEfficiency">Cleaning Efficiency</a>
     */
    public long getNCleanerEntriesRead() {
        return cleanerStats.getLong(CLEANER_ENTRIES_READ);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_OBSOLETE_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNINsObsolete() {
        return cleanerStats.getLong(CLEANER_INS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_CLEANED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNINsCleaned() {
        return cleanerStats.getLong(CLEANER_INS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_DEAD_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNINsDead() {
        return cleanerStats.getLong(CLEANER_INS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_MIGRATED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNINsMigrated() {
        return cleanerStats.getLong(CLEANER_INS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_OBSOLETE_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNBINDeltasObsolete() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_CLEANED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNBINDeltasCleaned() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_DEAD_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNBINDeltasDead() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_MIGRATED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNBINDeltasMigrated() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_OBSOLETE_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsObsolete() {
        return cleanerStats.getLong(CLEANER_LNS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXPIRED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXPIRED_NAME}</p>
     *
     * This total does not included embedded LNs.
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsExpired() {
        return cleanerStats.getLong(CLEANER_LNS_EXPIRED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXTINCT_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXTINCT_NAME}</p>
     *
     * This total does not included embedded LNs.
     *
     * @see ExtinctionFilter
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsExtinct() {
        return cleanerStats.getLong(CLEANER_LNS_EXTINCT);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_CLEANED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsCleaned() {
        return cleanerStats.getLong(CLEANER_LNS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_DEAD_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsDead() {
        return cleanerStats.getLong(CLEANER_LNS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_LOCKED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_LOCKED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsLocked() {
        return cleanerStats.getLong(CLEANER_LNS_LOCKED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MIGRATED_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNsMigrated() {
        return cleanerStats.getLong(CLEANER_LNS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNQUEUE_HITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNQUEUE_HITS_NAME}</p>
     *
     * @see <a href="#cleanerProcessing">Cleaning Statistics: Processing
     * Details</a>
     */
    public long getNLNQueueHits() {
        return cleanerStats.getLong(CLEANER_LNQUEUE_HITS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_PROCESSED_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public long getNPendingLNsProcessed() {
        return cleanerStats.getLong(CLEANER_PENDING_LNS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_LOCKED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_LOCKED_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public long getNPendingLNsLocked() {
        return cleanerStats.getLong(CLEANER_PENDING_LNS_LOCKED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_PROCESSED_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public long getNPendingDBsProcessed() {
        return cleanerStats.getLong(CLEANER_PENDING_DBS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_INCOMPLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_DBS_INCOMPLETE_NAME}</p>
     *
     * @see <a href="#cleanerPending">Cleaning Statistics: Pending LNs and
     * DBs</a>
     */
    public long getNPendingDBsIncomplete() {
        return cleanerStats.getLong(CLEANER_PENDING_DBS_INCOMPLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ACTIVE_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ACTIVE_LOG_SIZE_NAME}</p>
     *
     * <p>The {@link #getCurrentMinUtilization() log utilization} is the
     * percentage of activeLogSize that is currently referenced or active.</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     * @since 7.5
     */
    public long getActiveLogSize() {
        return cleanerStats.getLong(CLEANER_ACTIVE_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RESERVED_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RESERVED_LOG_SIZE_NAME}</p>
     *
     * <p>Deletion of reserved files may be postponed for several reasons.
     * This occurs if an active file is protected (by a backup, for example),
     * and then the file is cleaned and becomes a reserved file. See
     * {@link #getProtectedLogSizeMap()} for more information. In a
     * standalone JE environment, reserved files are normally deleted very
     * soon after being cleaned.</p>
     *
     * <p>In an HA environment, reserved files are retained because they might
     * be used for replication to electable nodes that have been offline
     * for the {@link com.sleepycat.je.rep.ReplicationConfig#FEEDER_TIMEOUT}
     * interval or longer, or to offline secondary nodes. The replication
     * stream position of these nodes is unknown, so whether these files could
     * be used to avoid a network restore, when bringing these nodes online,
     * is also unknown. The files are retained just in case they can be used
     * for such replication. Files are reserved for replication on both master
     * and replicas, since a replica may become a master at a future time.
     * Such files will be deleted (oldest file first) to make room for a
     * write operation, if the write operation would have caused a disk limit
     * to be violated.</p>
     *
     * <p>In NoSQL DB, this retention of reserved files has the additional
     * benefit of supplying the replication stream to subscribers of the
     * Stream API, when such subscribers need to replay the stream from an
     * earlier point in time.</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     * @since 7.5
     */
    public long getReservedLogSize() {
        return cleanerStats.getLong(CLEANER_RESERVED_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_NAME}</p>
     *
     * <p>Reserved files are protected for reasons described by {@link
     * #getProtectedLogSizeMap()}.</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     * @since 7.5
     */
    public long getProtectedLogSize() {
        return cleanerStats.getLong(CLEANER_PROTECTED_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_MAP_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_MAP_NAME}</p>
     *
     * <p>{@link #getReservedLogSize() Reserved} data files are temporarily
     * {@link #getProtectedLogSize() protected} for a number of reasons. The
     * keys in the protected log size map are the names of the protecting
     * entities, and the values are the number of bytes protected by each
     * entity. The type and format of the entity names are as follows:</p>
     *
     * <pre>
     *    Backup-N
     *    DiskOrderedCursor-N
     *    Syncup-N
     *    Feeder-N
     *    NetworkRestore-N
     * </pre>
     *
     * <p>Where:</p>
     * <ul>
     *     <li>
     *         {@code Backup-N} represents a {@link DbBackup} in progress,
     *         i.e., for which {@link DbBackup#startBackup()} has been called
     *         and {@link DbBackup#endBackup()} has not yet been called. All
     *         active files are initially protected by the backup, but these
     *         are not reserved files ond only appear in the map if they are
     *         cleaned and become reserved after the backup starts. Files
     *         are not protected if they have been copied and
     *         {@link DbBackup#removeFileProtection(String)} has been called.
     *         {@code N} is a sequentially assigned integer.
     *     </li>
     *     <li>
     *         {@code Syncup-N} represents an in-progress negotiation between
     *         a master and replica node in an HA replication group to
     *         establish a replication stream. This is a normally a very short
     *         negotiation and occurs when a replica joins the group or after
     *         an election is held. During syncup, all reserved files are
     *         protected.
     *         {@code N} is the node name of the other node involved in the
     *         syncup, i.e, if this node is a master then it is the name of
     *         the replica, and vice versa.
     *     </li>
     *     <li>
     *         {@code Feeder-N} represents an HA master node that is supplying
     *         the replication stream to a replica. Normally data in active
     *         files is being supplied and this data is not in the reserved
     *         or protected categories. But if the replica is lagging, data
     *         from reserved files may be supplied, and in that case will be
     *         protected and appear in the map.
     *         {@code N} is the node name of the replica receiving the
     *         replication stream.
     *     </li>
     *     <li>
     *         {@code NetworkRestore-N} represents an HA replica or master
     *         node that is supplying files to a node that is performing a
     *         {@link com.sleepycat.je.rep.NetworkRestore}. The files supplied
     *         are all active files plus the two most recently written
     *         reserved files. The two reserved files will appear in the map,
     *         as well as any of the active files that were cleaned and became
     *         reserved during the network restore. Files that have already
     *         been copied by the network restore are not protected.
     *         {@code N} is the client ID of the node performing the
     *         {@link com.sleepycat.je.rep.NetworkRestore}.
     *     </li>
     * </ul>
     *
     * <p>When more than one entity is included in the map, in general the
     * largest value points to the entity primarily responsible for
     * preventing reclamation of disk space. Note that the values normally
     * sum to more than {@link #getProtectedLogSize()}, since protection often
     * overlaps.</p>
     *
     * <p>The string format of this stat consists of {@code name=size} pairs
     * separated by semicolons, where name is the entity name described
     * above and size is the number of protected bytes.</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     * @since 7.5
     */
    public SortedMap<String, Long> getProtectedLogSizeMap() {
        return cleanerStats.getMap(CLEANER_PROTECTED_LOG_SIZE_MAP);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_AVAILABLE_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_AVAILABLE_LOG_SIZE_NAME}</p>
     *
     * <p>This is the amount that can be logged by write operations, and
     * other JE activity such as checkpointing, without violating a disk
     * limit. The files making up {@code reservedLogSize} can be deleted to
     * make room for these write operations, so {@code availableLogSize} is
     * the sum of the current disk free space and the reserved size that is not
     * protected ({@code reservedLogSize} - {@code protectedLogSize}). The
     * current disk free space is calculated using the disk volume's free
     * space, {@link EnvironmentConfig#MAX_DISK} and {@link
     * EnvironmentConfig#FREE_DISK}.</p>
     *
     * <p>Note that when a record is written, the number of bytes includes JE
     * overheads for the record. Also, this causes Btree metadata to be
     * written during checkpoints, and other metadata is also written by JE.
     * So the space occupied on disk by a given set of records cannot be
     * calculated by simply summing the key/data sizes.</p>
     *
     * <p>Also note that {@code availableLogSize} will be negative when a disk
     * limit has been violated, representing the amount that needs to be freed
     * before write operations are allowed.</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     * @see EnvironmentConfig#MAX_DISK
     * @see EnvironmentConfig#FREE_DISK
     * @since 7.5
     */
    public long getAvailableLogSize() {
        return cleanerStats.getLong(CLEANER_AVAILABLE_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TOTAL_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TOTAL_LOG_SIZE_NAME}</p>
     *
     * @see <a href="#cleanerDiskSpace">Cleaning Statistics: Disk Space
     * Management</a>
     */
    public long getTotalLogSize() {
        return cleanerStats.getLong(CLEANER_TOTAL_LOG_SIZE);
    }

    /* LogManager stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_MISS_NAME}</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     */
    public long getNCacheMiss() {
        return logStats.getAtomicLong(LBFP_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_END_OF_LOG_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_END_OF_LOG_NAME}</p>
     *
     * <p>Note that the log entries prior to this position may not yet have
     * been flushed to disk.  Flushing can be forced using a Sync or
     * WriteNoSync commit, or a checkpoint.</p>
     *
     * @see <a href="#logCritical">I/O Statistics: Logging Critical Section</a>
     */
    public long getEndOfLog() {
        return logStats.getLong(LOGMGR_END_OF_LOG);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNCS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNCS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     */
    public long getNFSyncs() {
        return logStats.getAtomicLong(FSYNCMGR_FSYNCS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNC_REQUESTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_FSYNC_REQUESTS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     */
    public long getNFSyncRequests() {
        return logStats.getLong(FSYNCMGR_FSYNC_REQUESTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_TIMEOUTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_TIMEOUTS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     */
    public long getNFSyncTimeouts() {
        return logStats.getLong(FSYNCMGR_TIMEOUTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_AVG_MS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_AVG_MS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     *
     * @since 18.3
     */
    public long getFSyncAvgMs() {
        return logStats.getLong(FILEMGR_FSYNC_AVG_MS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_95_MS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_95_MS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     *
     * @since 18.3
     */
    public long getFSync95Ms() {
        return logStats.getLong(FILEMGR_FSYNC_95_MS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_99_MS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_99_MS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     *
     * @since 18.3
     */
    public long getFSync99Ms() {
        return logStats.getLong(FILEMGR_FSYNC_99_MS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_MAX_MS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FSYNC_MAX_MS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     *
     * @since 18.3
     */
    public long getFSyncMaxMs() {
        return logStats.getLong(FILEMGR_FSYNC_MAX_MS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_N_GROUP_COMMIT_REQUESTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FSYNCMGR_N_GROUP_COMMIT_REQUESTS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     *
     * @since 18.1, although the stat was output by {@link #toString} and
     * appeared in the je.stat.csv file in earlier versions.
     */
    public long getNGroupCommitRequests() {
        return logStats.getLong(FSYNCMGR_N_GROUP_COMMIT_REQUESTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_LOG_FSYNCS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_LOG_FSYNCS_NAME}</p>
     *
     * @see <a href="#logFsync">I/O Statistics: Fsync and Group Commit</a>
     */
    public long getNLogFSyncs() {
        return logStats.getLong(FILEMGR_LOG_FSYNCS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_LOG_BUFFERS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_LOG_BUFFERS_NAME}</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     */
    public int getNLogBuffers() {
        return logStats.getInt(LBFP_LOG_BUFFERS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READS_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRandomReads() {
        return logStats.getLong(FILEMGR_RANDOM_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READ_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_READ_BYTES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRandomReadBytes() {
        return logStats.getLong(FILEMGR_RANDOM_READ_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRandomWrites() {
        return logStats.getLong(FILEMGR_RANDOM_WRITES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITE_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_RANDOM_WRITE_BYTES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRandomWriteBytes() {
        return logStats.getLong(FILEMGR_RANDOM_WRITE_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READS_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNSequentialReads() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READ_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_READ_BYTES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNSequentialReadBytes() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_READ_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNSequentialWrites() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_WRITES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITE_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_SEQUENTIAL_WRITE_BYTES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNSequentialWriteBytes() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_WRITE_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_READ_FROM_WRITEQUEUE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_READ_FROM_WRITEQUEUE_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNBytesReadFromWriteQueue() {
        return logStats.getLong(FILEMGR_BYTES_READ_FROM_WRITEQUEUE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNBytesWrittenFromWriteQueue() {
        return logStats.getLong(FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_READS_FROM_WRITEQUEUE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_READS_FROM_WRITEQUEUE_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNReadsFromWriteQueue() {
        return logStats.getLong(FILEMGR_READS_FROM_WRITEQUEUE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITES_FROM_WRITEQUEUE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITES_FROM_WRITEQUEUE_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNWritesFromWriteQueue() {
        return logStats.getLong(FILEMGR_WRITES_FROM_WRITEQUEUE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNWriteQueueOverflow() {
        return logStats.getLong(FILEMGR_WRITEQUEUE_OVERFLOW);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES_NAME}</p>
     *
     * @see <a href="#logWriteQueue">I/O Statistics: The Write Queue</a>
     */
    public long getNWriteQueueOverflowFailures() {
        return logStats.getLong(FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_BUFFER_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_BUFFER_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns
     * only the amount used by this environment.</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     */
    public long getBufferBytes() {
        return logStats.getLong(LBFP_BUFFER_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_NOT_RESIDENT_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_NOT_RESIDENT_NAME}</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     */
    public long getNNotResident() {
        return logStats.getAtomicLong(LBFP_NOT_RESIDENT);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_REPEAT_FAULT_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_REPEAT_FAULT_READS_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRepeatFaultReads() {
        return logStats.getLong(LOGMGR_REPEAT_FAULT_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_REPEAT_ITERATOR_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_REPEAT_ITERATOR_READS_NAME}</p>
     *
     * <p>This happens during scans of the log during activities like
     * environment open (recovery) or log cleaning. The repeat iterator reads}
     * can be reduced by increasing
     * {@link EnvironmentConfig#LOG_ITERATOR_MAX_SIZE}.</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public long getNRepeatIteratorReads() {
        return logStats.getLong(LOGMGR_REPEAT_ITERATOR_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_TEMP_BUFFER_WRITES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_TEMP_BUFFER_WRITES_NAME}</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     */
    public long getNTempBufferWrites() {
        return logStats.getLong(LOGMGR_TEMP_BUFFER_WRITES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_NO_FREE_BUFFER_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LBFP_NO_FREE_BUFFER_NAME}</p>
     *
     * @see <a href="#logBuffer">I/O Statistics: Log Buffers</a>
     *
     * @since 18.1, although the stat was output by {@link #toString} and
     * appeared in the je.stat.csv file in earlier versions.
     */
    public long getNNoFreeBuffer() {
        return logStats.getLong(LBFP_NO_FREE_BUFFER);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FILE_OPENS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_FILE_OPENS_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public int getNFileOpens() {
        return logStats.getInt(FILEMGR_FILE_OPENS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_OPEN_FILES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#FILEMGR_OPEN_FILES_NAME}</p>
     *
     * @see <a href="#logFileAccess">I/O Statistics: File Access</a>
     */
    public int getNOpenFiles() {
        return logStats.getInt(FILEMGR_OPEN_FILES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_TOO_SMALL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_TOO_SMALL_NAME}</p>
     *
     * @see <a href="#logItemBuffer">I/O Statistics: Log Item Buffers</a>
     */
    public long getNItemBufferTooSmall() {
        return logStats.getLong(LOGMGR_ITEM_BUFFER_TOO_SMALL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_POOL_EMPTY_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.log.LogStatDefinition#LOGMGR_ITEM_BUFFER_POOL_EMPTY_NAME}</p>
     *
     * @see <a href="#logItemBuffer">I/O Statistics: Log Item Buffers</a>
     */
    public long getNItemBufferPoolEmpty() {
        return logStats.getLong(LOGMGR_ITEM_BUFFER_POOL_EMPTY);
    }

    /* Return Evictor stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_EVICTION_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_EVICTION_RUNS_NAME}</p>
     *
     * <p>When an evictor thread is awoken it performs eviction until
     * {@link #getCacheTotalBytes()} is at least
     * {@link EnvironmentConfig#EVICTOR_EVICT_BYTES} less than the
     * {@link EnvironmentConfig#MAX_MEMORY_PERCENT total cache size}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cache">Cache Statistics</a>
     */
    public long getNEvictionRuns() {
        return cacheStats.getLong(EVICTOR_EVICTION_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_TARGETED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_TARGETED_NAME}</p>
     *
     * <p>An eviction target may actually be evicted, or skipped, or put back
     * to the LRU, potentially after partial eviction (stripping) or
     * BIN-delta mutation is done on it.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesTargeted() {
        return cacheStats.getLong(EVICTOR_NODES_TARGETED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_EVICTED_NAME}</p>
     *
     * <p>Does not include {@link #getNLNsEvicted() LN eviction} or
     * {@link #getNNodesMutated() BIN-delta mutation}.
     * Includes eviction of {@link #getNDirtyNodesEvicted() dirty nodes} and
     * {@link #getNRootNodesEvicted() root nodes}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNNodesEvicted() {
        return cacheStats.getLong(EVICTOR_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_ROOT_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_ROOT_NODES_EVICTED_NAME}</p>
     *
     * <p>The root node of a Database is only evicted after all other nodes in
     * the Database, so this implies that the entire Database has fallen out of
     * cache and is probably closed.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNRootNodesEvicted() {
        return cacheStats.getLong(EVICTOR_ROOT_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_DIRTY_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_DIRTY_NODES_EVICTED_NAME}</p>
     *
     * <p>When a dirty IN is evicted from cache, the IN must be logged. See
     * {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNDirtyNodesEvicted() {
        return cacheStats.getLong(EVICTOR_DIRTY_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_LNS_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_LNS_EVICTED_NAME}</p>
     *
     * <p>When a BIN is considered for eviction based on LRU, if the BIN
     * contains resident LNs in cache, it is stripped of the LNs rather
     * than being evicted. This stat reflects LNs evicted in this manner, but
     * not LNs evicted as a result of using {@link CacheMode#EVICT_LN}. Also
     * note that {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded} LNs
     * are evicted immediately and are not reflected in this stat value.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNLNsEvicted() {
        return cacheStats.getLong(EVICTOR_LNS_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_STRIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_STRIPPED_NAME}</p>
     *
     * <p>If space can be reclaimed by stripping a BIN, this prevents mutating
     * the BIN to a BIN-delta or evicting the BIN entirely.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesStripped() {
        return cacheStats.getLong(EVICTOR_NODES_STRIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MUTATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MUTATED_NAME}</p>
     *
     * <p>When a BIN is considered for eviction based on LRU, if the BIN
     * can be mutated to a BIN-delta, it is mutated rather than being evicted.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNNodesMutated() {
        return cacheStats.getLong(EVICTOR_NODES_MUTATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_PUT_BACK_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_PUT_BACK_NAME}</p>
     *
     * <p>Reasons for putting back a target IN are:</p>
     * <ul>
     *     <li>The IN was accessed by an operation while the evictor was
     *     processing it.</li>
     *     <li>To prevent the cache usage for Btree objects from falling below
     *     {@link EnvironmentConfig#TREE_MIN_MEMORY}.</li>
     * </ul>
     *
     * <p>See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesPutBack() {
        return cacheStats.getLong(EVICTOR_NODES_PUT_BACK);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MOVED_TO_PRI2_LRU_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MOVED_TO_PRI2_LRU_NAME}</p>
     *
     * <p>When an off-cache is not configured, dirty nodes are evicted last
     * from the cache by moving them to a 2nd priority LRU list.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesMovedToDirtyLRU() {
        return cacheStats.getLong(EVICTOR_NODES_MOVED_TO_PRI2_LRU);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_SKIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_SKIPPED_NAME}</p>
     *
     * <p>Reasons for skipping a target IN are:</p>
     * <ul>
     *     <li>It has already been evicted by another thread.</li>
     *     <li>It cannot be evicted because concurrent activity added resident
     *     child nodes.</li>
     *     <li>It cannot be evicted because it is dirty and the environment is
     *     read-only.</li>
     * </ul>
     * <p>See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesSkipped() {
        return cacheStats.getLong(EVICTOR_NODES_SKIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#THREAD_UNAVAILABLE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#THREAD_UNAVAILABLE_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNThreadUnavailable() {
        return cacheStats.getAtomicLong(THREAD_UNAVAILABLE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_SHARED_CACHE_ENVS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_SHARED_CACHE_ENVS_NAME}</p>
     *
     * <p>This method says nothing about whether this environment is using
     * the shared cache or not.</p>
     *
     */
    public int getNSharedCacheEnvironments() {
        return cacheStats.getInt(EVICTOR_SHARED_CACHE_ENVS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_NAME}</p>
     *
     * <p>Note that the number of LN fetches does not necessarily correspond
     * to the number of records accessed, since some LNs may be
     * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNLNsFetch() {
        return cacheStats.getAtomicLong(LN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINsFetch() {
        return cacheStats.getAtomicLong(BIN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNUpperINsFetch() {
        return cacheStats.getAtomicLong(UPPER_IN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_MISS_NAME}</p>
     *
     * <p>Note that the number of LN fetches does not necessarily correspond
     * to the number of records accessed, since some LNs may be
     * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNLNsFetchMiss() {
        return cacheStats.getAtomicLong(LN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_NAME}</p>
     *
     * <p>This is the portion of {@link #getNBINsFetch()} that resulted in a
     * fetch miss. The fetch may be for a full BIN or BIN-delta
     * ({@link #getNBINDeltasFetchMiss()}), depending on whether a BIN-delta
     * currently exists (see {@link EnvironmentConfig#TREE_BIN_DELTA}).
     * However, additional full BIN fetches occur when mutating a BIN-delta to
     * a full BIN ({@link #getNFullBINsMiss()}) whenever this is necessary for
     * completing an operation.</p>
     *
     * <p>Therefore, the total number of BIN fetch misses
     * (including BIN-deltas) is:</p>
     *
     * <p style="margin-left: 2em">{@code nFullBINsMiss + nBINsFetchMiss}</p>
     *
     * <p>And the total number of full BIN (vs BIN-delta) fetch misses is:</p>
     *
     * <p style="margin-left: 2em">{@code nFullBINsMiss + nBINsFetchMiss -
     * nBINDeltasFetchMiss}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINsFetchMiss() {
        return cacheStats.getAtomicLong(BIN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_FETCH_MISS_NAME}</p>
     *
     * <p>This represents the portion of {@code nBINsFetchMiss()} that fetched
     * BIN-deltas rather than full BINs. See {@link #getNBINsFetchMiss()}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINDeltasFetchMiss() {
        return cacheStats.getAtomicLong(BIN_DELTA_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#FULL_BIN_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#FULL_BIN_MISS_NAME}</p>
     *
     * <p>Note that this stat does not include full BIN misses that are
     * <i>not</i> due to BIN-delta mutations. See
     * {@link #getNBINsFetchMiss()}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNFullBINsMiss() {
        return cacheStats.getAtomicLong(FULL_BIN_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_MISS_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNUpperINsFetchMiss() {
        return cacheStats.getAtomicLong(UPPER_IN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_RATIO_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_RATIO_NAME}</p>
     *
     * <p>This stat can be misleading because it does not include the number
     * of full BIN fetch misses resulting from BIN-delta mutations ({@link
     * #getNFullBINsMiss()}. It may be improved, or perhaps deprecated, in a
     * future release.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public float getNBINsFetchMissRatio() {
        return cacheStats.getFloat(BIN_FETCH_MISS_RATIO);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_BLIND_OPS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_BLIND_OPS_NAME}</p>
     *
     * <p>Note that this stat is misplaced. It should be in the
     * {@value com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME} group
     * and will probably be moved there in a future release.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see EnvironmentConfig#TREE_BIN_DELTA
     */
    public long getNBINDeltaBlindOps() {
        return cacheStats.getAtomicLong(BIN_DELTA_BLIND_OPS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_UPPER_INS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_UPPER_INS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested and a shared cache is configured.</em></p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedUpperINs() {
        return cacheStats.getLong(CACHED_UPPER_INS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BINS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested and a shared cache is configured.</em></p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedBINs() {
        return cacheStats.getLong(CACHED_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BIN_DELTAS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BIN_DELTAS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested and a shared cache is configured.</em></p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedBINDeltas() {
        return cacheStats.getLong(CACHED_BIN_DELTAS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_SPARSE_TARGET_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_SPARSE_TARGET_NAME}</p>
     *
     * <p>Each IN contains an array of references to child INs or LNs. When
     * there are between one and four children resident, the size of the array
     * is reduced to four. This saves a significant amount of cache memory for
     * BINs when {@link CacheMode#EVICT_LN} is used, because there are
     * typically only a small number of LNs resident in cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNINSparseTarget() {
        return cacheStats.getLong(CACHED_IN_SPARSE_TARGET);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_NO_TARGET_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_NO_TARGET_NAME}</p>
     *
     * <p>Each IN contains an array of references to child INs or LNs. When
     * there are no children resident, no array is allocated. This saves a
     * significant amount of cache memory for BINs when {@link
     * CacheMode#EVICT_LN} is used, because there are typically only a small
     * number of LNs resident in cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNINNoTarget() {
        return cacheStats.getLong(CACHED_IN_NO_TARGET);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_COMPACT_KEY_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_COMPACT_KEY_NAME}</p>
     *
     * @see <a href="#cacheSizeOptimizations">Cache Statistics: Size
     * Optimizations</a>
     *
     * @see EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH
     */
    public long getNINCompactKeyIN() {
        return cacheStats.getLong(CACHED_IN_COMPACT_KEY);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI2_LRU_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI2_LRU_SIZE_NAME}</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see #getNNodesMovedToDirtyLRU()
     */
    public long getDirtyLRUSize() {
        return cacheStats.getLong(PRI2_LRU_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI1_LRU_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI1_LRU_SIZE_NAME}</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see #getNNodesMovedToDirtyLRU()
     */
    public long getMixedLRUSize() {
        return cacheStats.getLong(PRI1_LRU_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_EVICTORTHREAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_EVICTORTHREAD_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedEvictorThread() {
        return cacheStats.getLong(
            EvictionSource.EVICTORTHREAD.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_MANUAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_MANUAL_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedManual() {
        return cacheStats.getLong(
            EvictionSource.MANUAL.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CACHEMODE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CACHEMODE_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedCacheMode() {
        return cacheStats.getLong(
            EvictionSource.CACHEMODE.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CRITICAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CRITICAL_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedCritical() {
        return cacheStats.getLong(
            EvictionSource.CRITICAL.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_DAEMON_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_DAEMON_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedDeamon() {
        return cacheStats.getLong(
            EvictionSource.DAEMON.getNumBytesEvictedStatDef());
    }

    /* MemoryBudget stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_SHARED_CACHE_TOTAL_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_SHARED_CACHE_TOTAL_BYTES_NAME}</p>
     *
     * <p>If this
     * environment uses the shared cache, this method returns the total size of
     * the shared cache, i.e., the sum of the {@link #getCacheTotalBytes()} for
     * all environments that are sharing the cache.  If this environment does
     * <i>not</i> use the shared cache, this method returns zero.</p>
     *
     * <p>To get the configured maximum cache size, see {@link
     * EnvironmentMutableConfig#getCacheSize}.</p>
     */
    public long getSharedCacheTotalBytes() {
        return cacheStats.getLong(MB_SHARED_CACHE_TOTAL_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_TOTAL_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_TOTAL_BYTES_NAME}</p>
     *
     * <p>This method returns the sum of {@link #getDataBytes}, {@link
     * #getAdminBytes}, {@link #getLockBytes} and {@link #getBufferBytes}.</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * <p>To get the configured maximum cache size, see {@link
     * EnvironmentMutableConfig#getCacheSize}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getCacheTotalBytes() {
        return cacheStats.getLong(MB_TOTAL_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_CACHE_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_CACHE_UTILIZATION_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the utilization for this environment.</p>
     *
     * <p>To get the configured maximum cache size, see {@link
     * EnvironmentMutableConfig#getCacheSize}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public int getCacheUtilization() {
        return cacheStats.getInt(MB_CACHE_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getDataBytes() {
        return cacheStats.getLong(MB_DATA_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_ADMIN_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_ADMIN_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getAdminBytes() {
        return cacheStats.getLong(MB_ADMIN_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_LOCK_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_LOCK_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheUnexpectedSizes">Cache Statistics: Unexpected
     * Sizes</a>
     */
    public long getLockBytes() {
        return cacheStats.getLong(MB_LOCK_BYTES);
    }

    /* Btree operation stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_RELATCHES_REQUIRED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_RELATCHES_REQUIRED_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getRelatchesRequired() {
        return btreeOpStats.getLong(BT_OP_RELATCHES_REQUIRED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_ROOT_SPLITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_ROOT_SPLITS_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getRootSplits() {
        return btreeOpStats.getLong(BT_OP_ROOT_SPLITS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_GETS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_GETS_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getNBinDeltaGetOps() {
        return btreeOpStats.getAtomicLong(BT_OP_BIN_DELTA_GETS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_INSERTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_INSERTS_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getNBinDeltaInsertOps() {
        return btreeOpStats.getAtomicLong(BT_OP_BIN_DELTA_INSERTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_UPDATES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_UPDATES_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getNBinDeltaUpdateOps() {
        return btreeOpStats.getAtomicLong(BT_OP_BIN_DELTA_UPDATES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_DELETES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.BTreeStatDefinition#BT_OP_BIN_DELTA_DELETES_NAME}</p>
     *
     * @see <a href="#btreeop">Btree Operation Statistics</a>
     */
    public long getNBinDeltaDeleteOps() {
        return btreeOpStats.getAtomicLong(BT_OP_BIN_DELTA_DELETES);
    }

    /* Lock stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_OWNERS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_OWNERS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested.</em></p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public int getNOwners() {
        return lockStats.getInt(LOCK_OWNERS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_TOTAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_TOTAL_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested.</em></p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public int getNTotalLocks() {
        return lockStats.getInt(LOCK_TOTAL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_READ_LOCKS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_READ_LOCKS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested.</em></p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public int getNReadLocks() {
        return lockStats.getInt(LOCK_READ_LOCKS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WRITE_LOCKS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WRITE_LOCKS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested.</em></p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public int getNWriteLocks() {
        return lockStats.getInt(LOCK_WRITE_LOCKS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITERS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITERS_NAME}
     * <br><em>Zero is returned when {@link StatsConfig#setFast fast stats}
     * are requested.</em></p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public int getNWaiters() {
        return lockStats.getInt(LOCK_WAITERS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_REQUESTS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_REQUESTS_NAME}</p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public long getNRequests() {
        return lockStats.getLong(LOCK_REQUESTS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.txn.LockStatDefinition#LOCK_WAITS_NAME}</p>
     *
     * @see <a href="#lock">Lock Statistics</a>
     */
    public long getNWaits() {
        return lockStats.getLong(LOCK_WAITS);
    }

    /* Throughput stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_NAME}</p>
     * <p>
     * This operation corresponds to one of the following API calls:
     * <ul>
     *     <li>
     *         A successful {@link Cursor#get(DatabaseEntry, DatabaseEntry,
     *         Get, ReadOptions) Cursor.get} or {@link
     *         Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     *         ReadOptions) Database.get} call with {@link Get#SEARCH}, {@link
     *         Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     *         Get#SEARCH_BOTH_GTE}.
     *     </li>
     *     <li>
     *         A successful {@link SecondaryCursor#get(DatabaseEntry,
     *         DatabaseEntry, DatabaseEntry, Get, ReadOptions)
     *         SecondaryCursor.get} or {@link
     *         SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     *         DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} call
     *         when the primary data is requested (via the {@code data} param).
     *         This call internally performs a key search operation in the
     *         primary DB in order to return the data.
     *     </li>
     * </ul>
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriSearchOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_SEARCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_FAIL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_SEARCH_FAIL_NAME}</p>
     * <p>
     * This operation corresponds to a call to {@link Cursor#get(DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) Cursor.get} or {@link
     * Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) Database.get} with {@link Get#SEARCH}, {@link
     * Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     * Get#SEARCH_BOTH_GTE}, when the specified key is not found in the DB.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriSearchFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_SEARCH_FAIL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with
     * {@link Get#SEARCH}, {@link Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or
     * {@link Get#SEARCH_BOTH_GTE}.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecSearchOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_SEARCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_FAIL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_SEARCH_FAIL_NAME}</p>
     * <p>
     * This operation corresponds to a call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with {@link
     * Get#SEARCH}, {@link Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     * Get#SEARCH_BOTH_GTE}, when the specified key is not found in the DB.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecSearchFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_SEARCH_FAIL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_POSITION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_POSITION_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#get(DatabaseEntry, DatabaseEntry, Get, ReadOptions) Cursor.get}
     * or {@link Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) Database.get} with {@link Get#FIRST}, {@link Get#LAST},
     * {@link Get#NEXT}, {@link Get#NEXT_DUP}, {@link Get#NEXT_NO_DUP},
     * {@link Get#PREV}, {@link Get#PREV_DUP} or {@link Get#PREV_NO_DUP}.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriPositionOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_POSITION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_POSITION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_POSITION_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with
     * {@link Get#FIRST}, {@link Get#LAST},
     * {@link Get#NEXT}, {@link Get#NEXT_DUP}, {@link Get#NEXT_NO_DUP},
     * {@link Get#PREV}, {@link Get#PREV_DUP} or {@link Get#PREV_NO_DUP}.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecPositionOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_POSITION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} in one of the following cases:
     * <ul>
     *     <li>
     *         When {@link Put#NO_OVERWRITE} or {@link Put#NO_DUP_DATA} is
     *         specified.
     *     </li>
     *     <li>
     *         When {@link Put#OVERWRITE} is specified and the key was inserted
     *         because it previously did not exist in the DB.
     *     </li>
     * </ul>
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriInsertOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_INSERT);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_FAIL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_INSERT_FAIL_NAME}</p>
     * <p>
     * This operation corresponds to a call to {@link Cursor#put(DatabaseEntry,
     * DatabaseEntry, Put, WriteOptions) Cursor.put} or {@link
     * Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} with {@link Put#NO_OVERWRITE} or {@link
     * Put#NO_DUP_DATA}, when the key could not be inserted because it
     * previously existed in the DB.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriInsertFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_INSERT_FAIL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_INSERT_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_INSERT_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put}, for a primary DB with an associated
     * secondary DB. A secondary record is inserted when inserting a primary
     * record with a non-null secondary key, or when updating a primary record
     * and the secondary key is changed to to a non-null value that is
     * different than the previously existing value.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecInsertOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_INSERT);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_UPDATE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_UPDATE_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} in one of the following cases:
     * <ul>
     *     <li>
     *         When {@link Put#OVERWRITE} is specified and the key previously
     *         existed in the DB.
     *     </li>
     *     <li>
     *         When calling {@code Cursor.put} with {@link Put#CURRENT}.
     *     </li>
     * </ul>
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriUpdateOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_UPDATE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_UPDATE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_UPDATE_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put}, when a primary record is updated and its
     * TTL is changed. The associated secondary records must also be updated to
     * reflect the change in the TTL.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecUpdateOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_UPDATE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_NAME}</p>
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#delete() Cursor.delete}, {@link Database#delete(Transaction,
     * DatabaseEntry, WriteOptions) Database.delete}, {@link
     * SecondaryCursor#delete() SecondaryCursor.delete} or {@link
     * SecondaryDatabase#delete(Transaction, DatabaseEntry, WriteOptions)
     * SecondaryDatabase.delete}.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriDeleteOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_DELETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_FAIL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_PRI_DELETE_FAIL_NAME}</p>
     * <p>
     * This operation corresponds to a call to {@link
     * Database#delete(Transaction, DatabaseEntry,
     * WriteOptions) Database.delete} or {@link
     * SecondaryDatabase#delete(Transaction, DatabaseEntry, WriteOptions)
     * SecondaryDatabase.delete}, when the key could not be deleted because it
     * did not previously exist in the DB.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getPriDeleteFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_DELETE_FAIL);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_DELETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#THROUGHPUT_SEC_DELETE_NAME}</p>
     * <p>
     * This operation corresponds to one of the following API calls:
     * <ul>
     *     <li>
     *         A successful call to {@link Cursor#delete() Cursor.delete} or
     *         {@link Database#delete(Transaction, DatabaseEntry,
     *         WriteOptions) Database.delete}, that deletes a primary record
     *         containing a non-null secondary key.
     *     </li>
     *     <li>
     *         A successful call to {@link SecondaryCursor#delete()
     *         SecondaryCursor.delete} or {@link
     *         SecondaryDatabase#delete(Transaction, DatabaseEntry,
     *         WriteOptions) SecondaryDatabase.delete}.
     *     </li>
     *     <li>
     *         A successful call to {@link Cursor#put(DatabaseEntry,
     *         DatabaseEntry, Put, WriteOptions) Cursor.put} or {@link
     *         Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     *         WriteOptions) Database.put} that updates a primary record and
     *         changes its previously non-null secondary key to null.
     *     </li>
     * </ul>
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     *
     * @see <a href="#throughput">Operation Throughput Statistics</a>
     */
    public long getSecDeleteOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_DELETE);
    }

    /* TaskCoordinator stats. */

    /**
     * @hidden
     * <p>{@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#REAL_PERMITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#REAL_PERMITS_NAME}</p>
     * <p>
     * This counter is effectively incremented each time a call to
     * {@link TaskCoordinator#acquirePermit} returns a real permit and is
     * decremented {@link TaskCoordinator#releasePermit} releases a real
     * permit.
     *
     * @see <a href="#taskcoord">Task Coordinator Statistics</a>
     */
    public long getRealPermits() {
        return taskCoordinatorStats.getInt(
            TaskCoordinator.StatDefs.REAL_PERMITS);
    }

    /**
     * @hidden
     * <p>{@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#DEFICIT_PERMITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#DEFICIT_PERMITS_NAME}</p>
     * <p>
     * This counter is effectively incremented each time a call to
     * {@link TaskCoordinator#acquirePermit} returns a deficit permit and is
     * decremented {@link TaskCoordinator#releasePermit} releases a deficit
     * permit.
     * <p>
     * This number should typically be zero. If it is consistently positive
     * it indicates that the application load is high and housekeeping tasks
     * are being throttled.
     *
     * @see <a href="#taskcoord">Task Coordinator Statistics</a>
     */
    public long getDeficitPermits() {
        return taskCoordinatorStats.getInt(
            TaskCoordinator.StatDefs.DEFICIT_PERMITS);
    }

    /**
     * @hidden
     * <p>{@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#APPLICATION_PERMITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.utilint.TaskCoordinator.StatDefs#APPLICATION_PERMITS_NAME}</p>
     * <p>
     * This number changes as a result of calls to
     * {@link TaskCoordinator#setAppPermitPercent}. Increasing this percentage
     * results in more permits being reserved by the application, while
     * reducing the percentage decreases this number.
     *
     * @see <a href="#taskcoord">Task Coordinator Statistics</a>
     */
    public long getApplicationPermits() {
        return taskCoordinatorStats.getInt(
            TaskCoordinator.StatDefs.APPLICATION_PERMITS);
    }

    /**
     * @hidden For internal use: automatic backups
     *
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_COUNT_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_COUNT_NAME}</p>
     *
     * @see <a href="#backup">Automatic Backup Statistics</a>
     */
    public int getBackupCopyFilesCount() {
        return backupStats.getInt(BACKUP_COPY_FILES_COUNT);
    }

    /**
     * @hidden For internal use: automatic backups
     *
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_MS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_GROUP_NAME}
     * <br>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#BACKUP_COPY_FILES_MS_NAME}</p>
     *
     * @see <a href="#backup">Automatic Backup Statistics</a>
     */
    public long getBackupCopyFilesMs() {
        return backupStats.getLong(BACKUP_COPY_FILES_MS);
    }

    /**
     * Returns a String representation of the stats in the form of
     * &lt;stat&gt;=&lt;value&gt;
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (StatGroup group : getStatGroups()) {
            sb.append(group.toString());
        }
        return sb.toString();
    }

    /**
     * Returns a String representation of the stats which includes stats
     * descriptions in addition to &lt;stat&gt;=&lt;value&gt;
     */
    public String toStringVerbose() {
        StringBuilder sb = new StringBuilder();
        for (StatGroup group : getStatGroups()) {
            sb.append(group.toStringVerbose());
        }
        return sb.toString();
    }

    /**
     * @hidden
     * Internal use only.
     * JConsole plugin support: Get tips for stats.
     */
    public Map<String, String> getTips() {
        Map<String, String> tipsMap = new HashMap<>();
        for (StatGroup group : getStatGroups()) {
            group.addToTipMap(tipsMap);
        }
        return tipsMap;
    }

}
