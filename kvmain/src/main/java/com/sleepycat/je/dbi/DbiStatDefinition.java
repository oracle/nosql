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

package com.sleepycat.je.dbi;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE EnvironmentImpl and MemoryBudget statistics.
 */
public class DbiStatDefinition {

    /*
     * Note that MB group is not user-visible. This internal group is used
     * only to copy the MB stats to the Cache group (EvictorStatDefinition).
     */
    static final String MB_GROUP_NAME = "Cache Layout";
    static final String MB_GROUP_DESC =
        "Allocation of resources in the cache.";

    public static final String ENV_GROUP_NAME = "Environment";
    public static final String ENV_GROUP_DESC =
        "Miscellaneous environment wide statistics.";

    public static final String THROUGHPUT_GROUP_NAME = "Op";
    public static final String THROUGHPUT_GROUP_DESC =
        "Throughput statistics for JE calls.";

    public static final String BACKUP_GROUP_NAME = "Backup";
    public static final String BACKUP_GROUP_DESC =
        "Automatic backups statistics.";

    /* The following stat definitions are used in MemoryBudget. */

    public static final String MB_SHARED_CACHE_TOTAL_BYTES_NAME =
        "sharedCacheTotalBytes";
    public static final String MB_SHARED_CACHE_TOTAL_BYTES_DESC =
        "Total amount of the shared JE cache in use, in bytes.";
    public static final StatDefinition MB_SHARED_CACHE_TOTAL_BYTES =
        new StatDefinition(
            MB_SHARED_CACHE_TOTAL_BYTES_NAME,
            MB_SHARED_CACHE_TOTAL_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_TOTAL_BYTES_NAME =
        "cacheTotalBytes";
    public static final String MB_TOTAL_BYTES_DESC =
        "Total amount of JE cache in use, in bytes.";
    public static final StatDefinition MB_TOTAL_BYTES =
        new StatDefinition(
            MB_TOTAL_BYTES_NAME,
            MB_TOTAL_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_CACHE_UTILIZATION_NAME =
        "cacheUtilization";
    public static final String MB_CACHE_UTILIZATION_DESC =
        "The cache utilization as a percentage of the configured" +
            " max size.";
    public static final StatDefinition MB_CACHE_UTILIZATION =
        new StatDefinition(
            MB_CACHE_UTILIZATION_NAME,
            MB_CACHE_UTILIZATION_DESC,
            StatType.CUMULATIVE);

    public static final String MB_DATA_BYTES_NAME =
        "dataBytes";
    public static final String MB_DATA_BYTES_DESC =
        "Amount of JE cache used for holding data, keys and internal " +
            "Btree nodes, in bytes.";
    public static final StatDefinition MB_DATA_BYTES =
        new StatDefinition(
            MB_DATA_BYTES_NAME,
            MB_DATA_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_ADMIN_BYTES_NAME =
        "adminBytes";
    public static final String MB_ADMIN_BYTES_DESC =
        "Number of bytes of JE cache used for admin activies " +
            "(currently unused), in bytes.";
    public static final StatDefinition MB_ADMIN_BYTES =
        new StatDefinition(
            MB_ADMIN_BYTES_NAME,
            MB_ADMIN_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_LOCK_BYTES_NAME =
        "lockBytes";
    public static final String MB_LOCK_BYTES_DESC =
        "Number of bytes of JE cache used for holding locks and transactions," +
            " in bytes.";
    public static final StatDefinition MB_LOCK_BYTES =
        new StatDefinition(
            MB_LOCK_BYTES_NAME,
            MB_LOCK_BYTES_DESC,
            StatType.CUMULATIVE);

    /* The following stat definitions are used in EnvironmentImpl. */

    public static final String ENV_CREATION_TIME_NAME =
        "environmentCreationTime";
    public static final String ENV_CREATION_TIME_DESC =
        "System time when the Environment was opened. ";
    public static final StatDefinition ENV_CREATION_TIME =
        new StatDefinition(
            ENV_CREATION_TIME_NAME,
            ENV_CREATION_TIME_DESC,
            StatType.CUMULATIVE);
    /* The following stat definitions are used for throughput. */

    public static final String THROUGHPUT_PRI_SEARCH_NAME =
        "priSearch";
    public static final String THROUGHPUT_PRI_SEARCH_DESC =
        "Number of successful primary DB key search operations.";
    public static final StatDefinition THROUGHPUT_PRI_SEARCH =
        new StatDefinition(
            THROUGHPUT_PRI_SEARCH_NAME,
            THROUGHPUT_PRI_SEARCH_DESC);

    public static final String THROUGHPUT_PRI_SEARCH_FAIL_NAME =
        "priSearchFail";
    public static final String THROUGHPUT_PRI_SEARCH_FAIL_DESC =
        "Number of failed primary DB key search operations.";
    public static final StatDefinition THROUGHPUT_PRI_SEARCH_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_SEARCH_FAIL_NAME,
            THROUGHPUT_PRI_SEARCH_FAIL_DESC);

    public static final String THROUGHPUT_SEC_SEARCH_NAME =
        "secSearch";
    public static final String THROUGHPUT_SEC_SEARCH_DESC =
        "Number of successful secondary DB key search operations.";
    public static final StatDefinition THROUGHPUT_SEC_SEARCH =
        new StatDefinition(
            THROUGHPUT_SEC_SEARCH_NAME,
            THROUGHPUT_SEC_SEARCH_DESC);

    public static final String THROUGHPUT_SEC_SEARCH_FAIL_NAME =
        "secSearchFail";
    public static final String THROUGHPUT_SEC_SEARCH_FAIL_DESC =
        "Number of failed secondary DB key search operations.";
    public static final StatDefinition THROUGHPUT_SEC_SEARCH_FAIL =
        new StatDefinition(
            THROUGHPUT_SEC_SEARCH_FAIL_NAME,
            THROUGHPUT_SEC_SEARCH_FAIL_DESC);

    public static final String THROUGHPUT_PRI_POSITION_NAME =
        "priPosition";
    public static final String THROUGHPUT_PRI_POSITION_DESC =
        "Number of successful primary DB position operations.";
    public static final StatDefinition THROUGHPUT_PRI_POSITION =
        new StatDefinition(
            THROUGHPUT_PRI_POSITION_NAME,
            THROUGHPUT_PRI_POSITION_DESC);

    public static final String THROUGHPUT_SEC_POSITION_NAME =
        "secPosition";
    public static final String THROUGHPUT_SEC_POSITION_DESC =
        "Number of successful secondary DB position operations.";
    public static final StatDefinition THROUGHPUT_SEC_POSITION =
        new StatDefinition(
            THROUGHPUT_SEC_POSITION_NAME,
            THROUGHPUT_SEC_POSITION_DESC);

    public static final String THROUGHPUT_PRI_INSERT_NAME =
        "priInsert";
    public static final String THROUGHPUT_PRI_INSERT_DESC =
        "Number of successful primary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_PRI_INSERT =
        new StatDefinition(
            THROUGHPUT_PRI_INSERT_NAME,
            THROUGHPUT_PRI_INSERT_DESC);

    public static final String THROUGHPUT_PRI_INSERT_FAIL_NAME =
        "priInsertFail";
    public static final String THROUGHPUT_PRI_INSERT_FAIL_DESC =
        "Number of failed primary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_PRI_INSERT_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_INSERT_FAIL_NAME,
            THROUGHPUT_PRI_INSERT_FAIL_DESC);

    public static final String THROUGHPUT_SEC_INSERT_NAME =
        "secInsert";
    public static final String THROUGHPUT_SEC_INSERT_DESC =
        "Number of successful secondary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_SEC_INSERT =
        new StatDefinition(
            THROUGHPUT_SEC_INSERT_NAME,
            THROUGHPUT_SEC_INSERT_DESC);

    public static final String THROUGHPUT_PRI_UPDATE_NAME =
        "priUpdate";
    public static final String THROUGHPUT_PRI_UPDATE_DESC =
        "Number of successful primary DB update operations.";
    public static final StatDefinition THROUGHPUT_PRI_UPDATE =
        new StatDefinition(
            THROUGHPUT_PRI_UPDATE_NAME,
            THROUGHPUT_PRI_UPDATE_DESC);

    public static final String THROUGHPUT_SEC_UPDATE_NAME =
        "secUpdate";
    public static final String THROUGHPUT_SEC_UPDATE_DESC =
        "Number of successful secondary DB update operations.";
    public static final StatDefinition THROUGHPUT_SEC_UPDATE =
        new StatDefinition(
            THROUGHPUT_SEC_UPDATE_NAME,
            THROUGHPUT_SEC_UPDATE_DESC);

    public static final String THROUGHPUT_PRI_DELETE_NAME =
        "priDelete";
    public static final String THROUGHPUT_PRI_DELETE_DESC =
        "Number of successful primary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_PRI_DELETE =
        new StatDefinition(
            THROUGHPUT_PRI_DELETE_NAME,
            THROUGHPUT_PRI_DELETE_DESC);

    public static final String THROUGHPUT_PRI_DELETE_FAIL_NAME =
        "priDeleteFail";
    public static final String THROUGHPUT_PRI_DELETE_FAIL_DESC =
        "Number of failed primary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_PRI_DELETE_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_DELETE_FAIL_NAME,
            THROUGHPUT_PRI_DELETE_FAIL_DESC);

    public static final String THROUGHPUT_SEC_DELETE_NAME =
        "secDelete";
    public static final String THROUGHPUT_SEC_DELETE_DESC =
        "Number of successful secondary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_SEC_DELETE =
        new StatDefinition(
            THROUGHPUT_SEC_DELETE_NAME,
            THROUGHPUT_SEC_DELETE_DESC);

    /* Stat definitions for automatic backups */

    public static final String BACKUP_COPY_FILES_COUNT_NAME =
        "backupCopyFilesCount";
    public static final String BACKUP_COPY_FILES_COUNT_DESC =
        "Number of files copied to the archive by the most recent automatic" +
        " backup.";
    public static final StatDefinition BACKUP_COPY_FILES_COUNT =
        new StatDefinition(BACKUP_COPY_FILES_COUNT_NAME,
                           BACKUP_COPY_FILES_COUNT_DESC);

    public static final String BACKUP_COPY_FILES_MS_NAME = "backupCopyFilesMs";
    public static final String BACKUP_COPY_FILES_MS_DESC =
        "The total amount of time in milliseconds taken to copy files to the" +
        " archive by the most recent automatic backup.";
    public static final StatDefinition BACKUP_COPY_FILES_MS =
        new StatDefinition(BACKUP_COPY_FILES_MS_NAME,
                           BACKUP_COPY_FILES_MS_DESC);
}
