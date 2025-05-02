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

package com.sleepycat.je.cleaner;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE cleaner statistics.
 */
public class CleanerStatDefinition {

    public static final String GROUP_NAME = "Cleaning";
    public static final String GROUP_DESC =
        "Log cleaning involves garbage collection of data " +
            "files in the append-only storage system.";

    public static final String CLEANER_RUNS_NAME =
        "nCleanerRuns";
    public static final String CLEANER_RUNS_DESC =
        "Number of files processed by the cleaner, including two-pass runs.";
    public static final StatDefinition CLEANER_RUNS =
        new StatDefinition(
            CLEANER_RUNS_NAME,
            CLEANER_RUNS_DESC);

    public static final String CLEANER_TWO_PASS_RUNS_NAME =
        "nTwoPassRuns";
    public static final String CLEANER_TWO_PASS_RUNS_DESC =
        "Number of cleaner runs that resulted in two-pass runs.";
    public static final StatDefinition CLEANER_TWO_PASS_RUNS =
        new StatDefinition(
            CLEANER_TWO_PASS_RUNS_NAME,
            CLEANER_TWO_PASS_RUNS_DESC);

    public static final String CLEANER_REVISAL_RUNS_NAME =
        "nRevisalRuns";
    public static final String CLEANER_REVISAL_RUNS_DESC =
        "Number of potential cleaner runs that revised expiration info, " +
        "but did result in any cleaning.";
    public static final StatDefinition CLEANER_REVISAL_RUNS =
        new StatDefinition(
            CLEANER_REVISAL_RUNS_NAME,
            CLEANER_REVISAL_RUNS_DESC);

    public static final String CLEANER_DELETIONS_NAME =
        "nCleanerDeletions";
    public static final String CLEANER_DELETIONS_DESC =
        "Number of cleaner file deletions.";
    public static final StatDefinition CLEANER_DELETIONS =
        new StatDefinition(
            CLEANER_DELETIONS_NAME,
            CLEANER_DELETIONS_DESC);

    public static final String CLEANER_PENDING_LN_QUEUE_SIZE_NAME =
        "pendingLNQueueSize";
    public static final String CLEANER_PENDING_LN_QUEUE_SIZE_DESC =
        "Number of LNs pending because they were locked.";
    public static final StatDefinition CLEANER_PENDING_LN_QUEUE_SIZE =
        new StatDefinition(
            CLEANER_PENDING_LN_QUEUE_SIZE_NAME,
            CLEANER_PENDING_LN_QUEUE_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PENDING_DB_QUEUE_SIZE_NAME =
        "pendingDBQueueSize";
    public static final String CLEANER_PENDING_DB_QUEUE_SIZE_DESC =
        "Number of DBs pending because DB removal/truncation was " +
            "incomplete.";
    public static final StatDefinition CLEANER_PENDING_DB_QUEUE_SIZE =
        new StatDefinition(
            CLEANER_PENDING_DB_QUEUE_SIZE_NAME,
            CLEANER_PENDING_DB_QUEUE_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_INS_OBSOLETE_NAME =
        "nINsObsolete";
    public static final String CLEANER_INS_OBSOLETE_DESC =
        "Number of known-obsolete INs.";
    public static final StatDefinition CLEANER_INS_OBSOLETE =
        new StatDefinition(
            CLEANER_INS_OBSOLETE_NAME,
            CLEANER_INS_OBSOLETE_DESC);

    public static final String CLEANER_INS_CLEANED_NAME =
        "nINsCleaned";
    public static final String CLEANER_INS_CLEANED_DESC =
        "Number of potentially active INs.";
    public static final StatDefinition CLEANER_INS_CLEANED =
        new StatDefinition(
            CLEANER_INS_CLEANED_NAME,
            CLEANER_INS_CLEANED_DESC);

    public static final String CLEANER_INS_DEAD_NAME =
        "nINsDead";
    public static final String CLEANER_INS_DEAD_DESC =
        "Number of INs that were not found in the Btree.";
    public static final StatDefinition CLEANER_INS_DEAD =
        new StatDefinition(
            CLEANER_INS_DEAD_NAME,
            CLEANER_INS_DEAD_DESC);

    public static final String CLEANER_INS_MIGRATED_NAME =
        "nINsMigrated";
    public static final String CLEANER_INS_MIGRATED_DESC =
        "Number of active INs that were migrated by dirtying them.";
    public static final StatDefinition CLEANER_INS_MIGRATED =
        new StatDefinition(
            CLEANER_INS_MIGRATED_NAME,
            CLEANER_INS_MIGRATED_DESC);

    public static final String CLEANER_BIN_DELTAS_OBSOLETE_NAME =
        "nBINDeltasObsolete";
    public static final String CLEANER_BIN_DELTAS_OBSOLETE_DESC =
        "Number of known-obsolete BIN-deltas.";
    public static final StatDefinition CLEANER_BIN_DELTAS_OBSOLETE =
        new StatDefinition(
            CLEANER_BIN_DELTAS_OBSOLETE_NAME,
            CLEANER_BIN_DELTAS_OBSOLETE_DESC);

    public static final String CLEANER_BIN_DELTAS_CLEANED_NAME =
        "nBINDeltasCleaned";
    public static final String CLEANER_BIN_DELTAS_CLEANED_DESC =
        "Number of potentially active BIN-deltas.";
    public static final StatDefinition CLEANER_BIN_DELTAS_CLEANED =
        new StatDefinition(
            CLEANER_BIN_DELTAS_CLEANED_NAME,
            CLEANER_BIN_DELTAS_CLEANED_DESC);

    public static final String CLEANER_BIN_DELTAS_DEAD_NAME =
        "nBINDeltasDead";
    public static final String CLEANER_BIN_DELTAS_DEAD_DESC =
        "Number of BIN-deltas that were not found in the Btree.";
    public static final StatDefinition CLEANER_BIN_DELTAS_DEAD =
        new StatDefinition(
            CLEANER_BIN_DELTAS_DEAD_NAME,
            CLEANER_BIN_DELTAS_DEAD_DESC);

    public static final String CLEANER_BIN_DELTAS_MIGRATED_NAME =
        "nBINDeltasMigrated";
    public static final String CLEANER_BIN_DELTAS_MIGRATED_DESC =
        "Number of active BIN-deltas that were migrated by dirtying them.";
    public static final StatDefinition CLEANER_BIN_DELTAS_MIGRATED =
        new StatDefinition(
            CLEANER_BIN_DELTAS_MIGRATED_NAME,
            CLEANER_BIN_DELTAS_MIGRATED_DESC);

    public static final String CLEANER_LNS_OBSOLETE_NAME =
        "nLNsObsolete";
    public static final String CLEANER_LNS_OBSOLETE_DESC =
        "Number of known-obsolete LNs.";
    public static final StatDefinition CLEANER_LNS_OBSOLETE =
        new StatDefinition(
            CLEANER_LNS_OBSOLETE_NAME,
            CLEANER_LNS_OBSOLETE_DESC);

    public static final String CLEANER_LNS_EXPIRED_NAME =
        "nLNsExpired";
    public static final String CLEANER_LNS_EXPIRED_DESC =
        "Number of known-obsolete LNs that were expired.";
    public static final StatDefinition CLEANER_LNS_EXPIRED =
        new StatDefinition(
            CLEANER_LNS_EXPIRED_NAME,
            CLEANER_LNS_EXPIRED_DESC);

    public static final String CLEANER_LNS_EXTINCT_NAME =
        "nLNsExtinct";
    public static final String CLEANER_LNS_EXTINCT_DESC =
        "Number of known-obsolete LNs that were extinct.";
    public static final StatDefinition CLEANER_LNS_EXTINCT =
        new StatDefinition(
            CLEANER_LNS_EXTINCT_NAME,
            CLEANER_LNS_EXTINCT_DESC);

    public static final String CLEANER_LNS_CLEANED_NAME =
        "nLNsCleaned";
    public static final String CLEANER_LNS_CLEANED_DESC =
        "Number of potentially active LNs.";
    public static final StatDefinition CLEANER_LNS_CLEANED =
        new StatDefinition(
            CLEANER_LNS_CLEANED_NAME,
            CLEANER_LNS_CLEANED_DESC);

    public static final String CLEANER_LNS_DEAD_NAME =
        "nLNsDead";
    public static final String CLEANER_LNS_DEAD_DESC =
        "Number of LNs that were not found in the Btree.";
    public static final StatDefinition CLEANER_LNS_DEAD =
        new StatDefinition(
            CLEANER_LNS_DEAD_NAME,
            CLEANER_LNS_DEAD_DESC);

    public static final String CLEANER_LNS_LOCKED_NAME =
        "nLNsLocked";
    public static final String CLEANER_LNS_LOCKED_DESC =
        "Number of potentially active LNs that were added to the pending " +
            "queue because they were locked.";
    public static final StatDefinition CLEANER_LNS_LOCKED =
        new StatDefinition(
            CLEANER_LNS_LOCKED_NAME,
            CLEANER_LNS_LOCKED_DESC);

    public static final String CLEANER_LNS_MIGRATED_NAME =
        "nLNsMigrated";
    public static final String CLEANER_LNS_MIGRATED_DESC =
        "Number of active LNs that were migrated by logging them.";
    public static final StatDefinition CLEANER_LNS_MIGRATED =
        new StatDefinition(
            CLEANER_LNS_MIGRATED_NAME,
            CLEANER_LNS_MIGRATED_DESC);

    public static final String CLEANER_LNQUEUE_HITS_NAME =
        "nLNQueueHits";
    public static final String CLEANER_LNQUEUE_HITS_DESC =
        "Number of potentially active LNs that did not require a separate " +
            "Btree lookup.";
    public static final StatDefinition CLEANER_LNQUEUE_HITS =
        new StatDefinition(
            CLEANER_LNQUEUE_HITS_NAME,
            CLEANER_LNQUEUE_HITS_DESC);

    public static final String CLEANER_PENDING_LNS_PROCESSED_NAME =
        "nPendingLNsProcessed";
    public static final String CLEANER_PENDING_LNS_PROCESSED_DESC =
        "Number of pending LNs that were re-processed.";
    public static final StatDefinition CLEANER_PENDING_LNS_PROCESSED =
        new StatDefinition(
            CLEANER_PENDING_LNS_PROCESSED_NAME,
            CLEANER_PENDING_LNS_PROCESSED_DESC);

    public static final String CLEANER_PENDING_LNS_LOCKED_NAME =
        "nPendingLNsLocked";
    public static final String CLEANER_PENDING_LNS_LOCKED_DESC =
        "Number of pending LNs that were still locked.";
    public static final StatDefinition CLEANER_PENDING_LNS_LOCKED =
        new StatDefinition(
            CLEANER_PENDING_LNS_LOCKED_NAME,
            CLEANER_PENDING_LNS_LOCKED_DESC);

    public static final String CLEANER_PENDING_DBS_PROCESSED_NAME =
        "nPendingDBsProcessed";
    public static final String CLEANER_PENDING_DBS_PROCESSED_DESC =
        "Number of pending DBs that were re-processed.";
    public static final StatDefinition CLEANER_PENDING_DBS_PROCESSED =
        new StatDefinition(
            CLEANER_PENDING_DBS_PROCESSED_NAME,
            CLEANER_PENDING_DBS_PROCESSED_DESC);

    public static final String CLEANER_PENDING_DBS_INCOMPLETE_NAME =
        "nPendingDBsIncomplete";
    public static final String CLEANER_PENDING_DBS_INCOMPLETE_DESC =
        "Number of pending DBs for which DB removal/truncation " +
            "was still incomplete.";
    public static final StatDefinition CLEANER_PENDING_DBS_INCOMPLETE =
        new StatDefinition(
            CLEANER_PENDING_DBS_INCOMPLETE_NAME,
            CLEANER_PENDING_DBS_INCOMPLETE_DESC);

    public static final String CLEANER_ENTRIES_READ_NAME =
        "nCleanerEntriesRead";
    public static final String CLEANER_ENTRIES_READ_DESC =
        "Number of log entries processed by the cleaner.";
    public static final StatDefinition CLEANER_ENTRIES_READ =
        new StatDefinition(
            CLEANER_ENTRIES_READ_NAME,
            CLEANER_ENTRIES_READ_DESC);

    public static final String CLEANER_DISK_READS_NAME =
        "nCleanerDisksReads";
    public static final String CLEANER_DISK_READS_DESC =
        "Number of disk reads by the cleaner.";
    public static final StatDefinition CLEANER_DISK_READS =
        new StatDefinition(
            CLEANER_DISK_READS_NAME,
            CLEANER_DISK_READS_DESC);

    public static final String CLEANER_ACTIVE_LOG_SIZE_NAME =
        "activeLogSize";
    public static final String CLEANER_ACTIVE_LOG_SIZE_DESC =
        "Bytes used by all active data files: files required " +
            "for basic JE operation.";
    public static final StatDefinition CLEANER_ACTIVE_LOG_SIZE =
        new StatDefinition(
            CLEANER_ACTIVE_LOG_SIZE_NAME,
            CLEANER_ACTIVE_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_RESERVED_LOG_SIZE_NAME =
        "reservedLogSize";
    public static final String CLEANER_RESERVED_LOG_SIZE_DESC =
        "Bytes used by all reserved data files: files that have been " +
            "cleaned and can be deleted if they are not protected.";
    public static final StatDefinition CLEANER_RESERVED_LOG_SIZE =
        new StatDefinition(
            CLEANER_RESERVED_LOG_SIZE_NAME,
            CLEANER_RESERVED_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PROTECTED_LOG_SIZE_NAME =
        "protectedLogSize";
    public static final String CLEANER_PROTECTED_LOG_SIZE_DESC =
        "Bytes used by all protected data files: the subset of reserved " +
            "files that are temporarily protected and cannot be deleted.";
    public static final StatDefinition CLEANER_PROTECTED_LOG_SIZE =
        new StatDefinition(
            CLEANER_PROTECTED_LOG_SIZE_NAME,
            CLEANER_PROTECTED_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PROTECTED_LOG_SIZE_MAP_NAME =
        "protectedLogSizeMap";
    public static final String CLEANER_PROTECTED_LOG_SIZE_MAP_DESC =
        "A breakdown of protectedLogSize as a map of protecting " +
            "entity name to protected size in bytes.";
    public static final StatDefinition CLEANER_PROTECTED_LOG_SIZE_MAP =
        new StatDefinition(
            CLEANER_PROTECTED_LOG_SIZE_MAP_NAME,
            CLEANER_PROTECTED_LOG_SIZE_MAP_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_AVAILABLE_LOG_SIZE_NAME =
        "availableLogSize";
    public static final String CLEANER_AVAILABLE_LOG_SIZE_DESC =
        "Bytes available for write operations when unprotected reserved " +
            "files are deleted: " +
            "free space + reservedLogSize - protectedLogSize.";
    public static final StatDefinition CLEANER_AVAILABLE_LOG_SIZE =
        new StatDefinition(
            CLEANER_AVAILABLE_LOG_SIZE_NAME,
            CLEANER_AVAILABLE_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_TOTAL_LOG_SIZE_NAME =
        "totalLogSize";
    public static final String CLEANER_TOTAL_LOG_SIZE_DESC =
        "Total bytes used by data files on disk: " +
            "activeLogSize + reservedLogSize.";
    public static final StatDefinition CLEANER_TOTAL_LOG_SIZE =
        new StatDefinition(
            CLEANER_TOTAL_LOG_SIZE_NAME,
            CLEANER_TOTAL_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_MIN_UTILIZATION_NAME =
        "minUtilization";
    public static final String CLEANER_MIN_UTILIZATION_DESC =
        "Lower bound for current log utilization as a percentage.";
    public static final StatDefinition CLEANER_MIN_UTILIZATION =
        new StatDefinition(
            CLEANER_MIN_UTILIZATION_NAME,
            CLEANER_MIN_UTILIZATION_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_MAX_UTILIZATION_NAME =
        "maxUtilization";
    public static final String CLEANER_MAX_UTILIZATION_DESC =
        "Upper bound for current log utilization as a percentage.";
    public static final StatDefinition CLEANER_MAX_UTILIZATION =
        new StatDefinition(
            CLEANER_MAX_UTILIZATION_NAME,
            CLEANER_MAX_UTILIZATION_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PREDICTED_MIN_UTILIZATION_NAME =
        "minPredictedUtilization";
    public static final String CLEANER_PREDICTED_MIN_UTILIZATION_DESC =
        "Lower bound for predicted log utilization as a percentage.";
    public static final StatDefinition CLEANER_PREDICTED_MIN_UTILIZATION =
        new StatDefinition(
            CLEANER_PREDICTED_MIN_UTILIZATION_NAME,
            CLEANER_PREDICTED_MIN_UTILIZATION_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PREDICTED_MAX_UTILIZATION_NAME =
        "maxPredictedUtilization";
    public static final String CLEANER_PREDICTED_MAX_UTILIZATION_DESC =
        "Upper bound for predicted log utilization as a percentage.";
    public static final StatDefinition CLEANER_PREDICTED_MAX_UTILIZATION =
        new StatDefinition(
            CLEANER_PREDICTED_MAX_UTILIZATION_NAME,
            CLEANER_PREDICTED_MAX_UTILIZATION_DESC,
            StatType.CUMULATIVE);
}
