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

import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.EXTINCT;
import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.MAYBE_EXTINCT;
import static com.sleepycat.je.utilint.JETaskCoordinator.JE_DATA_ERASURE_TASK;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.ExtinctionFilter;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MetadataStore;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.ErasedLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StringStat;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TracerFormatter;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.FormatUtil;

/**
 * Erases obsolete data from disk during a configured interval. [#26954]
 *
 * Basics
 * ======
 * - The purpose of the erasure feature is to erase obsolete user data within
 *   some reasonable period after becoming obsolete, and to do this without
 *   adding significant overhead that might impact application performance.
 *
 * - There are no strict guarantees about when erasure is complete. This
 *   means it is best to think of erasure as a "best effort". Some examples
 *   where data is not always erased according to the erasure period are:
 *
 *   1. If a node is down for an extended time, obviously no erasure will
 *      occur until it comes back up. After it comes up, there may not be
 *      enough time left in the cycle for erasure to be completed. The rate
 *      of erasure is not increased in this situation. The erasure rate is
 *      kept constant throughout each cycle to avoid resource usage spikes.
 *
 *   2. Data at the tail end of the data log will not be erased because it is
 *      needed for recovery and replication. In this case, data will be
 *      eventually erased as more writing eventually occurs.
 *
 *   3. UINs (upper internal nodes in the Btree) are also erased, and
 *      therefore keys in UINs, which can be considered user data, are also
 *      erased. Completeness and performance for this process is achieved
 *      by piggybacking the slot-update on top of UIN flush during
 *      checkpointing.
 *
 *   4. From a performance viewpoint, erasure is designed so that each erasure
 *      cycle will be completed over a multi-day period. If the erasure period
 *      (see below) is set to a small value for testing, the cycle may not
 *      be completed and this is acceptable behavior. The minimum time
 *      needed to complete the cycle is dependent on the total size of the
 *      files to be erased in the cycle, the load on the system, and other
 *      factors.
 *
 * - Currently the erasure feature is hidden in the JE API. It may be exposed
 *   in the future.
 *
 * - The eraser overall may be enabled or disabled using the following param.
 *   It is disabled by default.
 *
 *     je.env.runEraser -- boolean, default: false, mutable: yes
 *
 * - JE Erasure supports different levels of erasure. Only erasure of data in
 *   deleted DBs (removed or truncated) or only erasure of extinct records are
 *   supported. Erasure of all committed deletes, updates or expired user
 *   records and corresponding B+-tree nodes is also supported. JE keeps track
 *   of records that have become obsolete. This is not a 100% complete list
 *   but covers most of the records. "knownObsoleteEntries" option is used to
 *   erase these entries in a less resource intensive manner.
 *   "eraseAllObsoleteEntries" option uses a relatively more resource intensive
 *   tree lookup based comprehensive system to weed out all obsolete entries.
 *
 *     je.erase.deletedDatabases -- boolean, default: true, mutable: yes
 *     je.erase.extinctRecords -- boolean, default: true, mutable: yes
 *     je.erase.knownObsoleteEntries -- boolean, default: true, mutable: yes
 *     je.erase.eraseAllObsoleteEntries -- boolean, default: true, mutable: yes
 *
 *   By setting both of the first two parameters to true in NoSQL DB, obsolete
 *   data will be removed for dropped tables and dropped indexes. Setting
 *   "je.erase.knownObsoleteEntries" to true will additionally erase all
 *   obsolete records tracked by JE and all expired records. By setting
 *   "je.erase.eraseAllObsoleteEntries", we can ensure erasure of all obsolete
 *   log records with any user data.
 *
 * - During each N day cycle, we erase all files that were N days old (or
 *   older) at the start of the cycle. Therefore, the first erasure for a file
 *   occurs when it is between N and N*2 days old. Thereafter, the file is
 *   erased every N days. The erasure period is set using the following param,
 *   which is zero by default; the period must be explicitly set to perform
 *   erasure.
 *
 *     je.erase.period -- duration, default: 0, min: 0, max: none, mutable: yes
 *
 * - Here's an example for how the erasure period works in practice.
 *
 *     Let's say the erasure period is 30 min. At the start of each 30 min
 *     erasure cycle, the data files older than 30 min are selected. Those
 *     files should normally be erased by the end of the cycle. Then a new
 *     cycle starts and a new set of files older than 30 minutes is
 *     selected.
 *
 *     Let's assume that writing is continuous and the checkpoint interval
 *     is fairly small, and therefore we won't run into the problem that
 *     data at the end of the log cannot be erased. (This problem is
 *     described in more detail in a later section.)
 *
 *     Then at any time, roughly speaking, all data that became obsolete more
 *     than 60 minutes ago should be erased. The erasure period is meant to be
 *     half of the desired interval after which obsolete data should disappear.
 *
 *     The erasure doesn't have to be complete exactly at the end of each
 *     erasure period. In production we expect the erasure period to be set to
 *     less than half of the desired interval to allow for impreciseness,
 *     problems, etc. For example, if the desired interval is 60 days in
 *     production, the erasure period would normally be set to around 25 days.
 *
 *  - Here's an example of how params could be set to ensure that obsolete
 *    data is removed within 60 days of becoming obsolete:
 *
 *      je.env.runEraser=true
 *      je.erase.deletedDatabases=true
 *      je.erase.extinctRecords=true
 *      je.erase.period=25 days
 *
 * - WARNING: Erasure cannot be used for applications that rely on the usual
 *   immutable nature of JE data files. For example, if DbBackup is used to
 *   create a snapshot using file links, the assumption that the files will
 *   not be modified by JE no longer holds when erasure is enabled. For
 *   example, NoSQL DB snapshots (which are just links to data files) are
 *   invalidated by data erasure. To perform a valid backup, NoSQL DB must
 *   actually copy the files while they're protected by DbBackup (between
 *   the calls to startBackup and endBackup).
 *
 * Trace Logging
 * =============
 * Trace logging is performed for the following erasure cycle events. All
 * messages are INFO-level messages except when stated otherwise.
 *
 *  - A fresh cycle starts. This occurs at startup, or after the prior
 *    cycle finishes and its end time passes, or when the prior cycle is
 *    aborted because its end time has passed.
 *
 *  - A cycle finishes because erasure of all files is complete. The
 *    message is logged immediately after all files have been erased, even
 *    if the cycle end time has not yet passed. The eraser will then be
 *    idle until the cycle end time passes.
 *
 *  - A cycle aborts because its end time has passed but is incomplete
 *    because not all files were erased. A fresh cycle is then started.
 *    This is a WARNING-level message when files cannot be erased because
 *    they are protected for reasons that are expected to be short lived
 *    (backup, replication) but have persisted until the end of the cycle.
 *    In other situations this is a INFO-level message, for example:
 *    - when the files are needed for recovery,
 *    - when the files are needed to retain at least 1000 VLSNs (or
 *      {@link com.sleepycat.je.rep.impl.RepParams#MIN_VLSN_INDEX_SIZE}),
 *    - when the files do not contain any VLSNs and therefore the VLSNIndex
 *      cannot be truncated at any specific point (this is a corner case).
 *
 *  - A cycle is suspended because the JE Environment is closed and not
 *    all files were erased. The cycle will be resumed at the next
 *    Environment open, if the cycle end time has not passed.
 *
 *  - An incomplete cycle is resumed at startup. The cycle was incomplete
 *    at the last Environment close, and its end time has not yet passed.
 *
 *  - An incomplete cycle cannot be resumed at startup because its end
 *    time has now passed. A fresh cycle is then started.
 *
 * All messages start with ERASER and include the cycle start/end times, the
 * files processed, and the files that are yet to be processed.
 *
 * Stats
 * =====
 * Stat Group: Eraser
 *
 * eraserCycleStart - Erasure cycle start time (UTC).
 * eraserCycleEnd - Erasure cycle end time (UTC).
 * eraserFilesRemaining - Number of files still to be processed in erasure
 *                        cycle.
 * eraserFilesErased - Number of files erased by overwriting obsolete entries.
 * eraserFilesDeleted - Number of reserved files deleted by the eraser.
 * eraserFilesAlreadyDeleted - Number of reserved files deleted coincidentally
 *                             by the cleaner.
 * eraserFSyncs - Number of fsyncs performed by the eraser.
 * eraserReads - Number of file reads performed by the eraser.
 * eraserReadBytes - Number of bytes read by the eraser.
 * eraserWrites - Number of file writes performed by the eraser.
 * eraserWriteBytes - Number of bytes written by the eraser.
 *
 * Erasing data at the end of log
 * ==============================
 * When writing stops, the user data at the tail end of the log may need
 * erasure at some point. This would happen if a table is dropped and then
 * there is no writing for a long time (or very little writing). This is a
 * corner case and not one we expect in production, but it can happen in
 * production and it certainly happens in tests.
 *
 * Currently JE does not allow cleaning of any file in the recovery interval.
 * This is to ensure that recovery works, of course. The same restriction
 * applies to erasure. Erasure is also prohibited on uncommitted obsolete
 * data and certain obsolete B+-tree nodes outside of recovery interval
 * which the recovery might need.
 * If we were to treat any slot referencing an erased LSN as if the slot were
 * deleted, we might get recovery to work. But this would be complex to analyze
 * and test thoroughly, especially for IN replay. Therefore if we need to erase
 * items in the recovery interval, we would need to detect this situation and
 * force a checkpoint before erasing.
 *
 * In addition, to erase all entries at the end of the log means that the
 * VLSNIndex would need to be completely truncated, i.e., made empty. This
 * means the node could not be used as a feeder when functioning as a master,
 * and could not perform syncup when functioning as a replica. Rather than
 * emptying the VLSNIndex completely we could log a benign replicated entry
 * so there is at least one entry. But that doesn't address the broader
 * problem of syncup. Perhaps this doesn't matter when writing has stopped
 * for an extended period because the replicas will be up-to-date. And
 * perhaps network restore is fine for other unusual cases. But it is a risk.
 * Right now we always leave at least 1,000 VLSNs in the VLSNIndex to guard
 * against problems, but we don't really know what problems might arise. So
 * it would take some work to figure this out and test it.
 *
 * Therefore, we simply do not erase obsolete data when it appears at the tail
 * end of the log. One way to explain this is to say that we can't erase
 * data at the very end of the transaction log, because this would prevent
 * recovery in certain situations. We do log a warning message in this
 * situation.
 *
 * Aborting Erasure
 * ================
 * Erasure of a file is aborted in the following cases.
 *
 * - The file is needed for a backup or network restore. In both cases, it is
 *   DbBackup.startBackup that aborts the erasure. The file aborted will then
 *   be protected and won't be selected again for erasure until the backup or
 *   network restore is finished.
 *
 * - The extinction filter returns EXTINCT_MAYBE. This is due to a temporary
 *   situation at startup time, while NoSQL DB (or another app) has not yet
 *   initialized its metadata (table metadata in the case of NoSQL DB). It
 *   will be retried repeatedly until this no longer occurs.
 *
 *     Discarded idea: We could add a way to know that the filter is fully
 *     initialized and the eraser thread could delay starting until then. But
 *     we would still have to abort if MAYBE_EXTINCT is returned after that
 *     point. So for simplicity we just do the abort.
 *
 * Reserved Files, VLSNIndex, Cleaning
 * ===================================
 * - Reserved files are an exception. Because an erased file cannot be used for
 *   replication, reserved files are deleted rather than erasing them.
 *   Therefore, reserved files are never older than N*2 days.
 *
 * - Before erasing a file covered by the VLSNIndex, we truncate the
 *   VLSNIndex to remove the file from its range. Because the VLSNIndex range
 *   never retreats (only advances), files protected by the VLSNIndex will
 *   never have been erased.
 *
 * - However, other files may be erased and subsequently become protected. This
 *   is OK, because such protection only needs to guarantee that files are not
 *   changed while they are protected. These include:
 *    - Backup and network restore.
 *    - DiskOrderedCursor and Database.count.
 *
 * - Cleaning is not coordinated with erasure (except for the treatment of
 *   reserved files discussed above). A given file may be erased and cleaned
 *   concurrently, and this should not cause problems. This is a waste of
 *   resources, but since it is unlikely we do not try to detect it or
 *   optimize for it. Such coordination would add a lot of complexity.
 *
 * Throttling
 * ==========
 * Throttling is performed by estimating the total amount of work in the
 * cycle at the beginning of the cycle, dividing up the total cycle time
 * by this work amount, and throttling (waiting) at various points in
 * order to spread the work out fairly evenly over the cycle period.
 * The {@link WorkThrottle} class calculates the wait time based on work
 * done so far.
 *
 * It is possible that we cannot complete the work within the cycle period,
 * for example, because a node is down for an extended period. In such
 * cases we do _not_ attempt to catch up by speeding up the rate of work,
 * since this could cause performance spikes. Instead we intentionally
 * overestimate the amount of work, leaving spare time to account for such
 * problems. In the end, if erasure of all selected files cannot be
 * completed by the end of the cycle, the cycle will be aborted and a new
 * cycle is started with a recalculated set of files. This is acceptable
 * behavior in unusual conditions.
 *
 *   Note: Such problems could be addressed differently by integrating
 *   with the TaskCoordinator. In that case we would use a different
 *   approach entirely: we would simply work at the maximum rate allowed by
 *   the TaskCoordinator. But for this to work well, other JE components would
 *   also need to be integrated with the TaskCoordinator. So for now we simply
 *   perform work at a fixed rate within each cycle.
 *
 * Before the cycle starts we have to open each file to get its creation
 * time, which has a cost. Throttling for that one time task is performed
 * separately in {@link #startCycle}. The remaining time in the cycle is also
 * allocated by {@link #startCycle}, which initializes {@link #cycleThrottle}
 * and related fields. Work is divided as described below.
 *
 * For each file to be erased there are several components of work:
 *
 * 1. We may have to read file, or parts of files, for two reasons that
 *    are in addition to the file erasure process itself:
 *
 *    a. We may have to read the file to find its last VLSN, to determine
 *       where to truncate the VLSNIndex. In the worst case scenario we have
 *       to do this for every file, but it is much more likely that it will
 *       only have to be done for a small fraction of the files, and only a
 *       fraction (the end) of each file will normally be read. In addition,
 *       each time we truncate the VLSNIndex we perform an fsync; however,
 *       in the normal case we do this only once per cycle.
 *
 *    b. We read a file redundantly when erasure of a file is aborted and
 *       restarted later. See Aborting Erasure above.
 *
 * 2. Read through the file and overwrite the type byte for each entry
 *    that should be erased. We know the length of each file the read cost
 *    is known. We don't know how many erasures will take place, but the
 *    worst case is that every entry is erased.
 *
 *    Note that reserved files are simply deleted rather than erasing them
 *    and this is cheaper than erasure. So when there are reserved files,
 *    we will overestimate the amount of work.
 *
 * 3. Lookup the tree for each entry that can't be determined as obsolete by
 *    following methods :
 *    a. known obsolete (tracked by JE)
 *    b. record belonging to deleted DB.
 *    c. record marked as extinct.
 *    d. expired record (based on TTL)
 *    e. record marked as deleted.
 *    f. records for immediately obsolete entries or embedded LNs.
 *
 * 4. Before any overwriting, at the time we determine that at least one entry
 *    must be erased, touch the file and perform an fsync to ensure that the
 *    lastModifiedTime is updated persistently. The fsync is assumed to be
 *    expensive.
 *
 * 5. After overwriting all type bytes, perform a second fsync to make the
 *    type changes persistent. The fsync is assumed to be expensive.
 *
 * 6. Overwrite the item in each entry that was erased. This cost is
 *    unknown, but the worst case is that every entry is erased.
 *
 * 7. Perform the third and final fsync to make the erasure persistent. The
 *    fsync is assumed to be expensive.
 *
 * Work units are defined as follows.
 *
 * - For component 1 we assign a work unit to each byte read (the file
 *   length). This is a very large overestimate and is intended to
 *   account for processing delays, such as when a node is down.
 *
 * - For component 2 we also assign a work unit to each byte read (the file
 *   length). The overwrite of type bytes is variable and included in this
 *   cost for simplicity.
 *
 * - TODO: work estimation for component 3.
 *
 * - For components 4, 5, 6 and 7 together we also assign the file length as
 *   a very rough estimate, and this work is divided between these components
 *   as follows:
 *     18% for step 4
 *     16% for step 5
 *     50% for step 6
 *     16% for step 7
 *
 * Therefore the total amount of work is simply three times the length of the
 * files to be erased.
 *
 * Other
 * =====
 * - To support network restore, before erasing a file we remove its cached
 *   info (which includes a checksum) from the response cache in LogFileFeeder.
 *   See {@link
 *   com.sleepycat.je.rep.impl.RepImpl#clearedCachedFileChecksum(String)}.
 *
 * - Prohibiting erasure of protected files prevents changes to the file while
 *   it is being read by the protecting entity. Even so, if we read from the
 *   log buffer cache or tip cache, we could also get different results than
 *   reading directly from the file. To be safe, erasure could clear any
 *   cached data for the file. But is this necessary? No, because reading
 *   from the tip cache and log buffer cache is done only by feeders, and
 *   the files read by feeders are protected from erasure.
 *
 * - BtreeVerifier, when configured to read LNs, checks for LSN references to
 *   erased entries since it simply reads the LNs via a cursor. If the LN has
 *   been erased and is not extinct, an error is reported as usual.
 *
 * - The checksum for the LOG_ERASED type cannot be verified, and its
 *   {@link LogEntryHeader#hasChecksum()} method will return false. Because
 *   an entry may be erased in the middle of checksum calculation, the header
 *   may have to be re-read from disk in rare cases. See
 *   {@link com.sleepycat.je.log.ChecksumValidator#validate(long, long, LogEntryHeader)}.
 *
 * - The LOG_ERASED type is not counted as an LN or IN, which could throw off
 *   utilization counts. This may only impact tests and debugging. A thorough
 *   analysis of this issue has not been performed.
 *
 *
 * Erasure of Keys from B-tree (from Bins, Bin-Deltas and UINs)
 * ============================================================
 *
 * We now support the removal of obsolete keys slots from the tree. In order to
 * make this as efficient as possible we try to piggyback this slot removal on
 * top of checkpointing when the INs and BINs are being anyways flushed to
 * disk.
 *
 * How do we do this?
 * ------------------
 *
 * Unlike before, during each compression and erasure cycle if we detect that
 * slots can be removed, we make the BIN dirty so that modified BINS would be
 * compressed and flushed in a subsequent checkpoint. Compression takes care of
 * slot removal during the flushing that happens as part of checkpoint.To make
 * this performant:-
 *
 *   1. We do this step only for full BINs.
 *   2. Also we dirty BINs during compression only when 0th key has been
 *      changed.
 *
 * How soon does a BIN/BIN-Delta get rid of obsolete slots?
 * --------------------------------------------------------
 *
 * This should happen within 2 consecutive erasure cycles after a BIN becomes
 * dirty.
 * Note:- UIN key handling and obsolete offset logic is dependent on
 * checkpoints. It is assumed that because erasure cycle is set in terms of
 * days in production systems, therefore between each of these erasure cycles
 * there will be multiple checkpoints.
 *
 * 1. If a full BIN becomes dirty or it's 0th element has changed then it would
 *    be handled in the subsequent checkpoint. During erasure if we detect a
 *    BIN to contain a deleted or expired item, the BIN would be dirtied and
 *    would be taken care of during the next checkpoint.
 *    Note this step removes keys from tree. A subsequent erasure-cycle, would
 *    get rid of the BIN/BIN-Deltas that became obsolete as a result of the
 *    above check point and get rid of the keys from disk.
 *
 * 2. For BIN-Deltas, we ensure this happens within two erasure cycles. At the
 *    end of each cycle we persistently store an LSN corresponding to the start
 *    of that cycle. We call this the 'oldestAllowedObsoleteLSN'. During a
 *    subsequent erasure cycle, if we come across a BIN-Delta with an LSN older
 *    than this oldestAllowedLSN, we mutate this BIN-Delta to a dirty full-BIN.
 *    From there, it would be handled like any other dirty full BIN.
 *
 *
 * How are UINS handled?
 * ---------------------
 *
 * UINs are always processed after all their modified children have been
 * processed during checkpoint. We take advantage of this to propagate removal
 * of obsolete slots upwards. If the 0th slot in the child doesn't match the
 * slot in parent, then parent is updated with the newer value. This is done
 * recursively to remove obsolete slots in UINs. Note that correctness also
 * demands that obsolete slots in UINs be processed after processing the slots
 * of their children.
 *
 * Given that inserts might be happening while we are trying to update UINs,
 * So we shouldn't update a UIN slot if the 0th slot of the child doesn't
 * represent the smallest key in subtree with the child as root. If we don't do
 * this, portions of tree might become unreachable. As shown below, Latch
 * Coupling is not enough to prevent such a scenario.
 *
 * Latch Coupling is not enough for Big trees
 * ------------------------------------------
 *
 * Consider the following scenario :-
 *
 *            -------------10|50|90----------------
 *          /                  \                   \
 *       0|30                 50|70                 \
 *      /    \                /   \                  \
 *   10|20 30|40          50|60 70|80              90|100
 *                        /
 *                   50|55
 *
 *   50 is deleted, checkpoint starts and all parents except root are updated
 *   with 55
 *
 *            -------------10|50|90----------------
 *          /                  \                   \
 *       0|30                55|70                  \
 *      /    \                /   \                  \
 *   10|20 30|40           55|60 70|80             90|100
 *                         /
 *                       55
 *
 *   53 is inserted before root is updated and before checkpoint is finished
 *
 *            -------------10|50|90----------------
 *          /                  \                   \
 *       0|30                55|70                  \
 *      /    \               /    \                  \
 *   10|20 30|40          55|60 70|80              90|100
 *                        /
 *                     53|55
 *
 *   Now root is updated with 55 as part of checkpoint, so 53 becomes
 *   unreachable
 *
 *            -------------10|55|90----------------
 *          /                  \                   \
 *       0|30                55|70                  \
 *      /    \               /    \                  \
 *   10|20 30|40          55|60  70|80             90|100
 *                        /
 *                     53|55
 *
 *
 *  To take care of this we ensure that during insert, the flag
 *  IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE is cleared for all UINs/BINs where 0th
 *  slot isn't the smallest value in the subtree represented by that slot. If
 *  this flag is not set for a BIN/UIN, the corresponding slot is not updated
 *  in it's parent during checkpoint.
 *
 *
 * How are upgrades handled?
 *
 * In case of upgrade or crash-recovery, for all BINs we have to assume that
 * id-key doesn't represent the lowest value and for all UINs we have to assume
 * that neither 0th slot nor id-key represent the lowest value in the subtree
 * corresponding to the 0th slot. So any removal of obsolete keys happens in
 * bottom-up manner starting with BINs when they get dirty or are made dirty by
 * erasure/compression etc.
 *
 * What are the main concerns for key-value erasure from B-tree?
 *
 *   1. Performance Impact
 *
 *      We can't do any extra scans or processing for the removal of obsolete
 *      keys. Even the act of dirtying BINS or mutating BIN-deltas to full BINs
 *      has to be minimized. Attempt has been made to avoid additional latching
 *      or locking .
 *
 *   2. Stability Impact
 *
 *      We can't touch any of the core tree algorithms. The process of removal
 *      of obsolete BINS can be delayed. It's okay if it happens eventually but
 *      we can't interfere in normal tree processing. Any misstep would impact
 *      routing in tree and wrongly setting a value can make a subtree
 *      inaccessible. So this has undergone rigorous testing. Also, update of
 *      slots in parents should be done with a value that is guaranteed to be
 *      the smallest in the subtree rooted at the child.
 */
public class DataEraser extends StoppableThread implements EnvConfigObserver {

    /*
     * Get permit immediately, and do not have a timeout for the amount of time
     * the permit can be held.  Should only be changed for testing.
     */
    public static long PERMIT_WAIT_MS = 0;
    public static long PERMIT_TIME_TO_HOLD_MS = 0;
    private static final int MAX_FILE_INFO_MS = 1000;
    private static final int MIN_WORK_DELAY_MS = 5;
    private static final int MAX_SLEEP_MS = 100;
    private static final int FOREVER_TIMEOUT_MS = 5 * 60 * 1000;
    private static final int NO_UNPROTECTED_FILES_DELAY_MS = 1000;
    private static final int MAX_ERASURE_ENTRIES = 5 * 1024;
    private static final String TEST_ERASE_PERIOD = "test.erasePeriod";
    static final String ERASE_IN = "EraseIN:";
    static final String ERASE_LN = "EraseLN:";

    private static TestHook<TestEvent> testEventHook;

    private static final DateFormat DATE_FORMAT =
        TracerFormatter.makeDateFormat();

    private static final int WRITE_WORK_PCT = 50;
    private static final int FSYNC1_WORK_PCT = 18;
    private static final int FSYNC2_WORK_PCT = 16;
    private static final int FSYNC3_WORK_PCT = 16;

    private final Cleaner cleaner;
    private final FileProtector fileProtector;
    private final FileManager fileManager;
    private final Logger logger;
    private volatile boolean shutdownRequested = false;
    private volatile boolean enabled = false;
    private int terminateMillis;
    private volatile long cycleMs = 0;
    private boolean eraseDeletedDbs = false;
    private boolean eraseExtinctRecords = false;
    private boolean eraseKnownObsoleteEntries = false;
    private boolean eraseAllObsoleteEntries = false;
    private int pollCheckMs;
    private final long[] eraseOffsets = new long[MAX_ERASURE_ENTRIES];
    private final int[] eraseSizes = new int[MAX_ERASURE_ENTRIES];
    private byte[] zeros = new byte[10 * 1024];
    private final Object pollMutex = new Object();
    private final NavigableMap<Long, FileInfo> fileInfoCache = new TreeMap<>();
    private WorkThrottle cycleThrottle;
    private long totalCycleWork;
    private String lastProtectedFilesMsg;
    private Level lastProtectedFilesMsgLevel;
    private int lookAheadCacheSize;
    private final SuspectObsoleteTracker suspectObsoleteTracker;
    private long currentCycleStartLSN;

    /**
     * Data stored in metadataStore for handling long-lived
     * bin-deltas.
     */
    private long oldestAllowedObsoleteLSN;
    private static final int ERASER_METADATA_VERSION = 1;

    /**
     * The eraser is single-threaded but there is occasional multi-threaded
     * access to the following fields due to stat loading.
     */
    private volatile long startTime;
    private volatile long endTime;
    private volatile long completionTime;
    private NavigableSet<Long> filesRemaining =
        Collections.emptyNavigableSet();
    private NavigableSet<Long> hardLinkedFiles =
        Collections.emptyNavigableSet();
    private NavigableSet<Long> filesCompleted =
        Collections.emptyNavigableSet();
    private final AtomicInteger filesErased = new AtomicInteger();
    private final AtomicInteger filesDeleted = new AtomicInteger();
    private final AtomicInteger filesAlreadyDeleted = new AtomicInteger();
    private final AtomicInteger fSyncs = new AtomicInteger();
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong writeBytes = new AtomicLong();

    /**
     * currentFileMutex protects currentFile and abortCurrentFile.
     */
    private final Object currentFileMutex = new Object();
    private volatile Long currentFile;
    private boolean abortCurrentFile;
    private int abortTimeoutMs;
    private static boolean disableKnownObsoletesCheck = false;

    /**
     * Whether the eraser should participate in critical eviction.  Ideally
     * the eraser would not participate in eviction, since that would reduce
     * the cost of cleaning.  However, the eraser can add large numbers of
     * nodes to the cache.  By not participating in eviction, other threads
     * could be kept in a constant state of eviction and would effectively
     * starve.  Therefore, this setting is currently enabled.
     *
     * Identical to Cleaner.DO_CRITICAL_EVICTION setting.
     */
    static final boolean DO_CRITICAL_EVICTION = true;

    public DataEraser(final EnvironmentImpl envImpl) {

        super(envImpl, "JEErasure",
              envImpl.getFileManager().getCleanerStatsCollector());
        cleaner = envImpl.getCleaner();
        fileProtector = envImpl.getFileProtector();
        fileManager = envImpl.getFileManager();
        logger = LoggerUtils.getLogger(getClass());
        suspectObsoleteTracker =
            new SuspectObsoleteTracker(envImpl);

        envConfigUpdate(envImpl.getConfigManager(), null);
        envImpl.addConfigObserver(this);
    }

    @Override
    public void envConfigUpdate(
        final DbConfigManager configManager,
        final EnvironmentMutableConfig ignore) {

        /*
         * If the TEST_ERASE_PERIOD system property is specified and
         * ENV_RUN_ERASER is not specified, enable erasure and use the test
         * period.
         */
        final boolean runErase;
        final String testErasePeriod = System.getProperty(TEST_ERASE_PERIOD);

        if (testErasePeriod != null &&
            !configManager.isSpecified(EnvironmentParams.ENV_RUN_ERASER)) {

            runErase = true;
            cycleMs = PropUtil.parseLongDuration(testErasePeriod);
            eraseDeletedDbs = true;
            eraseExtinctRecords = true;
            eraseKnownObsoleteEntries = true;
            eraseAllObsoleteEntries = true;
        } else {

            runErase = configManager.getBoolean(
                EnvironmentParams.ENV_RUN_ERASER);

            cycleMs = configManager.getLongDuration(
                EnvironmentParams.ERASE_PERIOD);

            eraseDeletedDbs = configManager.getBoolean(
                EnvironmentParams.ERASE_DELETED_DATABASES);

            eraseExtinctRecords = configManager.getBoolean(
                EnvironmentParams.ERASE_EXTINCT_RECORDS);

            eraseKnownObsoleteEntries = configManager.getBoolean(
                EnvironmentParams.ERASE_KNOWN_OBSOLETE_ENTRIES);

            eraseAllObsoleteEntries = configManager.getBoolean(
                EnvironmentParams.ERASE_ALL_OBSOLETE_ENTRIES);

            if (eraseAllObsoleteEntries) {
                eraseDeletedDbs = true;
                eraseExtinctRecords = true;
                eraseKnownObsoleteEntries = true;
            }
        }

        lookAheadCacheSize = configManager
            .getInt(EnvironmentParams.CLEANER_LOOK_AHEAD_CACHE_SIZE);

        enabled = runErase && cycleMs > 0 &&
            (eraseDeletedDbs || eraseExtinctRecords);

        terminateMillis = configManager.getDuration(
            EnvironmentParams.EVICTOR_TERMINATE_TIMEOUT);

        pollCheckMs = Math.min(MAX_SLEEP_MS, terminateMillis / 4);

        abortTimeoutMs = configManager.getDuration(
            EnvironmentParams.ERASE_ABORT_TIMEOUT);
    }

    public StatGroup loadStats(StatsConfig config) {

        final StatGroup statGroup = new StatGroup(
            EraserStatDefinition.GROUP_NAME, EraserStatDefinition.GROUP_DESC);

        /* Add CUMULATIVE stats. */

        final DateFormat dateFormat = TracerFormatter.makeDateFormat();

        new StringStat(
            statGroup, EraserStatDefinition.ERASER_CYCLE_START,
            (startTime == 0) ? "" : dateFormat.format(new Date(startTime)));

        new StringStat(
            statGroup, EraserStatDefinition.ERASER_CYCLE_END,
            (endTime == 0) ? "" : dateFormat.format(new Date(endTime)));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_REMAINING,
            filesRemaining.size());

        /* INCREMENTAL stats must be cleared. */

        final boolean clear = config.getClear();

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_ERASED,
            getStat(filesErased, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_DELETED,
            getStat(filesDeleted, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FILES_ALREADY_DELETED,
            getStat(filesAlreadyDeleted, clear));

        new IntStat(
            statGroup, EraserStatDefinition.ERASER_FSYNCS,
            getStat(fSyncs, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_READS,
            getStat(reads, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_READ_BYTES,
            getStat(readBytes, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_WRITES,
            getStat(writes, clear));

        new LongStat(
            statGroup, EraserStatDefinition.ERASER_WRITE_BYTES,
            getStat(writeBytes, clear));

        return statGroup;
    }

    private long getStat(final AtomicLong val, final boolean clear) {
        return clear ? val.getAndSet(0) : val.get();
    }

    private int getStat(final AtomicInteger val, final boolean clear) {
        return clear ? val.getAndSet(0) : val.get();
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public int initiateSoftShutdown() {
        shutdownRequested = true;
        synchronized (pollMutex) {
            pollMutex.notify();
        }
        return terminateMillis;
    }

    public void startThread() {

        if (enabled &&
            !envImpl.isMemOnly() &&
            !envImpl.isReadOnly() &&
            !isAlive()) {

            /*
             * Ensure metadata DB is opened before Environment ctor finishes,
             * to avoid timing problems in tests that are sensitive to the
             * number of open DBs.
             */
            envImpl.getMetadataStore().openDb();

            start();
        }
    }

    @Override
    public void run() {
        boolean isInitialized = false;
        boolean isStarted = false;

        /*
         * Internal exceptions are used to signal state changes, and they are
         * handled in this method. Other than ShutdownRequestedException, all
         * such exceptions must be handled inside the while loop.
         */
        try {
            while (true) {
                checkShutdown();
                try {
                    if (!isInitialized) {
                        isInitialized = true;
                        if (resumeCycle()) {
                            isStarted = true;
                        }
                    }
                    if (!isStarted) {
                        startCycle();
                        isStarted = true;
                    }
                    if (checkForCycleEnd()) {
                        waitForEnabled();
                        isStarted = false;
                        continue;
                    }
                    final Long file = getNextFile();
                    if (file != null) {
                        try {
                            if (!isFileHardLinked(file)) {
                                eraseFile(file);
                                filesCompleted.add(file);
                                filesRemaining.remove(file);
                                hardLinkedFiles.remove(file);
                                storeState();
                            } else {
                                /*
                                 * Remove this file with hard-links from
                                 * filesRemaining, so we can accurately detect
                                 * cycle end.
                                 */
                                filesRemaining.remove(file);
                                hardLinkedFiles.add(file);
                            }
                        } finally {
                            clearCurrentFile();
                        }
                    } else {
                        waitForCycleEnd();
                        waitForEnabled();
                        isStarted = false;
                    }
                } catch (NoUnprotectedFilesException e) {
                    waitForUnprotectedFiles(e);
                } catch (ErasureDisabledException e) {
                    waitForEnabled();
                } catch (PeriodChangedException e) {
                    logPeriodChanged();
                    isStarted = false;
                } catch (AbortCurrentFileException e) {
                    /* Continue. */
                }
            }
        } catch (ShutdownRequestedException e) {
            if (!filesRemaining.isEmpty()) {
                logCycleSuspend();
            }
        }
    }

    /**
     * If we can load the last stored state and the endTime of the last cycle
     * has not yet arrived, resume execution and return true.
     * Otherwise, return false.
     */
    private boolean resumeCycle() {

        if (!loadState()) {
            return false;
        }

        if (TimeSupplier.currentTimeMillis() >= endTime) {
            if (!filesRemaining.isEmpty()) {
                logCycleCannotResume();
            }
            return false;
        }

        logCycleResume();

        final WorkThrottle throttle =
            createFileInfoThrottle(filesRemaining.size(), cycleMs);

        /* Populate file info cache for files remaining in cycle. */
        for (final Long file : filesRemaining) {

            fileInfoCache.put(
                file,
                new FileInfo(getFileCreationTime(file), getFileLength(file)));

            throttle.throttle(1);
        }

        cycleThrottle = new WorkThrottle(totalCycleWork, endTime - startTime);
        return true;
    }

    /**
     * Resets per-cycle info, including determining the set of files to be
     * erased in the next cycle.
     */
    private void startCycle() {

        /* Ignore config changes during calculations. */
        final long localCycleMs = cycleMs;

        startTime = TimeSupplier.currentTimeMillis();
        endTime = startTime + localCycleMs;
        final long fileAgeCutoff = startTime - localCycleMs;
        filesCompleted = Collections.synchronizedNavigableSet(new TreeSet<>());
        filesRemaining = Collections.synchronizedNavigableSet(new TreeSet<>());
        hardLinkedFiles =
            Collections.synchronizedNavigableSet(new TreeSet<>());
        completionTime = 0;

        final NavigableSet<Long> allFiles =
            fileProtector.getAllCompletedFiles();

        final WorkThrottle throttle =
            createFileInfoThrottle(allFiles.size(), localCycleMs);

        logCycleInit(allFiles.size());

        /*
         * Iterate through all files except for the last file. We can't erase
         * the last file anyway, because it is in the recovery interval.
         */
        for (final Long file : allFiles) {

            /*
             * Trying using cached file info from previous cycles. A file in a
             * previous cycle will normally have a creationTime that qualifies,
             * but we check again in case the erasure period has changed.
             */
            final FileInfo prevInfo = fileInfoCache.get(file);
            if (prevInfo != null) {
                if (prevInfo.creationTime <= fileAgeCutoff) {
                    filesRemaining.add(file);
                }
                continue;
            }

            final long creationTime = getFileCreationTime(file);

            throttle.throttle(1);

            if (creationTime > fileAgeCutoff) {
                continue;
            }

            filesRemaining.add(file);

            fileInfoCache.put(
                file, new FileInfo(creationTime, getFileLength(file)));
        }

        /*
         * Prevent the file cache from growing without bounds. The cache need
         * only contain an entry for all filesToErase.
         */
        fileInfoCache.navigableKeySet().retainAll(filesRemaining);

        /*
         * Use three times the sum of the file lengths as the work for the
         * remaining time in the cycle, as described in the class comments.
         */
        totalCycleWork = filesRemaining.stream()
            .mapToLong(file -> fileInfoCache.get(file).length)
            .sum() * 3;

        cycleThrottle = new WorkThrottle(totalCycleWork, endTime - startTime);

        currentCycleStartLSN =
            envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn();

        logCycleStart();
    }

    /**
     * Creates a throttle for reading file info when a cycle is started or
     * restored.
     *
     * Use only a small portion of the cycle for collecting file info.
     * Use at most 0.1% of the cycle time and at most MAX_FILE_INFO_MS
     * per file. The idea is just to do a reasonable amount of throttling
     * between calls to getFileCreationTime.
     */
    private WorkThrottle createFileInfoThrottle(final int files,
                                                final long localCycleMs) {
        final long workTime = Math.min(
            localCycleMs / 1000,
            files * MAX_FILE_INFO_MS);

        return new WorkThrottle(files, workTime);
    }

    /**
     * Returns the file creation time by opening the file and reading its
     * header entry.
     *
     * @return creation time, or Long.MAX_VALUE if the file does not exist.
     */
    private long getFileCreationTime(final long file) {
        try {
            return fileManager.getFileHeaderTimestamp(file).getTime();

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } catch (FileNotFoundException e) {
            /* File was deleted by the cleaner. */
            return Long.MAX_VALUE;
        }
    }

    private long getFileLength(final long file) {
        return new File(fileManager.getFullFileName(file)).length();
    }

    /**
     * Returns true, if hard links exist for a file being considered for
     * erasure.
     */
    private boolean isFileHardLinked(final long file) {
        final Path path = Paths.get(fileManager.getFullFileName(file));
        try {
            if ((int) Files.getAttribute(path, "unix:nlink") > 1) {
                return true;
            }
        } catch (IOException e) {
            return false;
        }

        return false;
    }

    /**
     * Return the next unprotected file to be processed.
     *
     * <p>First return files that are unprotected, to defer VLSNIndex
     * truncation until it is necessary.</p>
     *
     * <p>If all files are protected, truncate the VLSNIndex and try again.
     * This is the same approach used by {@link Cleaner#manageDiskUsage}.</p>
     *
     * <p>A WARNING level is specified in the NoUnprotectedFilesException
     * when files are protected for reasons that are expected to be short
     * lived (backup, replication). The WARNING is actually logged only if
     * these reasons persist until the end of the erasure cycle.</p>
     *
     * @return the next file to be processed, or null if all files have been
     * processed.
     *
     * @throws NoUnprotectedFilesException if all remaining files are
     * protected.
     */
    private Long getNextFile() {

        lastProtectedFilesMsg = null;
        lastProtectedFilesMsgLevel = null;

        if (filesRemaining.isEmpty()) {
            return null;
        }

        Long file = getNextUnprotectedFile();
        if (file != null) {
            return file;
        }

        if (!cleaner.isFileDeletionEnabled()) {
            throw new NoUnprotectedFilesException(
                "Test mode prohibits VLSNIndex truncation.",
                Level.INFO);
        }

        final long lastRecoveryFile = getFirstFileInRecoveryInterval();

        if (filesRemaining.first() >= lastRecoveryFile) {
            throw new NoUnprotectedFilesException(
                "All remaining files are in the recovery interval," +
                    " lastRecoveryFile=0x" +
                    Long.toHexString(lastRecoveryFile) + ".",
                Level.INFO);
        }

        /*
         * Determine whether it is worthwhile and possible to truncate the
         * VLSNIndex.
         */
        if (!envImpl.isReplicated()) {
            /* VLSNIndex does not exist in a non-replicated env. */
            throw new NoUnprotectedFilesException(
                "Protected files are not in the recovery interval.",
                Level.WARNING);
        }

        final long vlsnIndexStartFile = fileProtector.getVLSNIndexStartFile();

        if (vlsnIndexStartFile > filesRemaining.last()) {
            throw new NoUnprotectedFilesException(
                "Protected files are not in the recovery interval and not " +
                "protected by the VLSNIndex vlsnIndexStartFile=0x" +
                    Long.toHexString(vlsnIndexStartFile) + ".",
                Level.WARNING);
        }

        final Pair<Long, Long> truncateInfo = getVLSNIndexTruncationInfo();

        if (truncateInfo == null) {
            /* This is a corner case and seems unlikely to ever happen. */
            throw new NoUnprotectedFilesException(
                "Cannot truncate VLSNIndex because no remaining files " +
                    "contain VLSNs.",
                Level.INFO);
        }

        /*
         * Truncate the VLSNIndex, then try again to get an unprotected file.
         */
        if (!envImpl.tryTruncateVlsnHead(
                truncateInfo.first(), truncateInfo.second())) {
            /*
             * There are several reasons for tryTruncateVlsnHead to return
             * false as explained by VLSNTracker.tryTruncateFromHead(VLSN,
             * long, LogItemCache) javadoc.  Only one of them -- that the file
             * is protected -- should cause a WARNING message. To avoid false
             * alarms we log an INFO message here for now. TODO: Return more
             * info from tryTruncateVlsnHead so we can log a WARNING when
             * appropriate.
             */
            throw new NoUnprotectedFilesException(
                "VLSNIndex already truncated or cannot be truncated further.",
                Level.INFO);
        }

        file = getNextUnprotectedFile();
        if (file != null) {
            return file;
        }

        throw new NoUnprotectedFilesException(
            "Protected files are not in the recovery interval and " +
                "VLSNIndex was successfully truncated.",
            Level.WARNING);
    }

    /**
     * Returns a human readable description of what may be preventing erasure.
     * These are the reasons that {@link NoUnprotectedFilesException} may be
     * thrown by {@link #getNextFile()}.
     */
    private String getFileProtectionMessage() {

        final long firstRecoveryFile = getFirstFileInRecoveryInterval();

        return "Files protected by the current recovery interval: [" +
            FormatUtil.asHexString(
                filesRemaining.tailSet(firstRecoveryFile)) +
            "]. Other protected files: " +
            fileProtector.getProtectedFileMap(filesRemaining) +
            ". FirstRecoveryFile: 0x" + Long.toHexString(firstRecoveryFile) +
            ". FirstVLSNIndexFile: 0x" +
            Long.toHexString(fileProtector.getVLSNIndexStartFile()) + ".";
    }

    /**
     * Returns the first file remaining that is unprotected. If a file is
     * selected, the currentFile field is updated to support aborting.
     * This method synchronizes with {@link #abortErase} to ensure that a
     * file passed to abortErase will not be selected again for erasure.
     */
    private Long getNextUnprotectedFile() {

        synchronized (currentFileMutex) {

            final Long file =
                fileProtector.getFirstUnprotectedFile(filesRemaining);

            if (file == null) {
                return null;
            }

            if (file >= getFirstFileInRecoveryInterval()) {
                return null;
            }

            currentFile = file;
            abortCurrentFile = false;
            return file;
        }
    }

    /**
     * Called after a file returned by {@link #getNextUnprotectedFile()} is no
     * longer being processed.
     */
    private void clearCurrentFile() {

        synchronized (currentFileMutex) {
            currentFile = null;
            abortCurrentFile = false;
        }
    }

    /**
     * Returns the file being processed or null. A volatile field is accessed
     * without synchronization.
     */
    Long getCurrentFile() {
        return currentFile;
    }

    /**
     * Used to ensure that erasure of a file stops before coping that file
     * during a backup or network restore. A short wait may be performed to
     * ensure that the eraser thread notices the abort and any in-process
     * writes are completed.
     *
     * @param fileSet erasure of the current file is aborted if the current
     * file is protected by this protected file set. The given fileSet must
     * be protected at the time this method is called.
     *
     * @throws EraserAbortException if we can't abort erasure of a target
     * file within {@link EnvironmentParams#ERASE_ABORT_TIMEOUT}. The
     * timeout is long, so this should not happen unless the eraser thread
     * is wedged or starved.
     */
    public void abortErase(final FileProtector.ProtectedFileSet fileSet) {

        final Long abortFile;

        /*
         * Synchronize with getNextUnprotectedFile to ensure that a file
         * passed to abortErase will not be selected again for erasure.
         */
        synchronized (currentFileMutex) {

            /*
             * The check for isReservedFile is used to avoid aborting when we
             * will not erase the file. This is a minor optimization and is
             * not required for correctness.
             */
            if (currentFile != null &&
                fileSet.isProtected(currentFile, null) &&
                !fileProtector.isReservedFile(currentFile)) {

                abortCurrentFile = true;
                abortFile = currentFile;
            } else {
                return;
            }
        }

        /*
         * Wait for the eraser thread to notice that the abortCurrentFile flag
         * is set, at which time it will set currentFile to null. The wait is
         * necessary to ensure that a write will not be performed after this
         * method returns.
         *
         * It is important that we return ASAP, so we use Object.notify to
         * wake up any waiters. All other polling is done with Object.wait on
         * the pollMutex object, to ensure we can abort promptly.
         */
        if (PollCondition.await(1, abortTimeoutMs,
            () -> {
                synchronized (pollMutex) {
                    pollMutex.notify();
                }
                synchronized (currentFileMutex) {
                    return !abortFile.equals(currentFile);
                }
            })) {
            return; /* Aborted. */
        }

        /*
         * We don't expect this to ever occur, because the timeout is quite
         * long and all erasure operations are checking the abortCurrentFile
         * flag quite frequently. EraserAbortException is thrown (rather than
         * EnvironmentWedgedException) because we don't want to shut down the
         * environment when DbBackup.start fails.
         */
        final String msg = "Unable to abort erasure of file 0x" +
            Long.toHexString(abortFile) + " within " +
            abortTimeoutMs + "ms.";

        LoggerUtils.warning(logger, envImpl, msg);
        throw new EraserAbortException(msg);
    }

    /**
     * Returns the file number of the firstActiveLsn for the last completed
     * checkpoint. Files GTE this file would be in the recovery interval if
     * a crash occurs, and are protected from erasure.
     */
    private long getFirstFileInRecoveryInterval() {

        return DbLsn.getFileNumber(
            envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn());
    }

    /**
     * Returns the {endVLSN,endFile} pair that is needed to truncate the
     * VLSNIndex.
     *
     * Returns null if no remaining files-to-erase contain VLSNs. A file that
     * does not contain VLSNs (is "barren" may contain BINs or migrated LNs
     * for data that needs erasure. However, this is expected to be a rare
     * condition so we tolerate it. It will be corrected during a subsequent
     * cycle if any file following this cycle's files need erasure and do
     * contain VLSNs, or the VLSNIndex is truncated for other reasons, for
     * example, to reclaim disk space.
     *
     * Selects the last file-to-erase containing a VLSN. This reduces the
     * number of (fairly expensive) VLSNIndex truncations to the minimum.
     * However, it means that the index will be truncated for a large number
     * of files at once, rather than truncating it incrementally.
     */
    private Pair<Long, Long> getVLSNIndexTruncationInfo() {

        for (final Long file : filesRemaining.descendingSet()) {

            checkContinue();

            /*
             * We avoid searching the file if it happens to be a reserved
             * file, since the last VLSN is known to the FileProtector.
             */
            long lastVlsn = fileProtector.getReservedFileLastVLSN(file);

            if (lastVlsn != INVALID_VLSN) {
                if (VLSN.isNull(lastVlsn)) {
                    continue;
                }
                return new Pair<>(lastVlsn, file);
            }

            /* Must search. */
            lastVlsn = searchFileForLastVLSN(file);

            if (lastVlsn != INVALID_VLSN) {
                if (VLSN.isNull(lastVlsn)) {
                    continue;
                }
                return new Pair<>(lastVlsn, file);
            }
        }

        return null;
    }

    /**
     * Returns the last VLSN in the given file, or NULL_VLSN if the file does
     * contain any VLSNs, or null if the file does not exist.
     *
     * Only VLSNs in replicated entries are considered. VLSNs in migrated LNs
     * are ignored.
     */
    private long searchFileForLastVLSN(final Long file) {

        final FileInfo fileInfo = fileInfoCache.get(file);

        if (fileInfo != null && fileInfo.lastVlsn != INVALID_VLSN) {
            return fileInfo.lastVlsn;
        }

        final long fileLength = (fileInfo != null) ?
            fileInfo.length : getFileLength(file);

        boolean forward;
        long startLsn;
        long finishLsn;
        long endOfFileLsn;

        try {
            /*
             * If file+1 exists, read its header to get the offset of the
             * previous entry, and then read the file backwards to find the
             * last VLSN.
             */
            final long prevOffset =
                fileManager.getFileHeaderPrevOffset(file + 1);

            forward = false;
            startLsn = DbLsn.makeLsn(file, prevOffset);
            finishLsn = DbLsn.makeLsn(file, 0);
            endOfFileLsn = DbLsn.makeLsn(file, fileLength);

        } catch (FileNotFoundException e) {
            /*
             * If file+1 does not exist, read the entire file from the start.
             */
            forward = true;
            startLsn = DbLsn.makeLsn(file, 0);
            finishLsn = DbLsn.NULL_LSN;
            endOfFileLsn = DbLsn.NULL_LSN;

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);
        }

        final FileReader reader = new FileReader(
            envImpl, cleaner.readBufferSize, forward, startLsn,
            file /*singleFileNumber*/, endOfFileLsn, finishLsn) {

            @Override
            protected boolean processEntry(ByteBuffer entryBuffer) {

                final int readOps = getAndResetNReads();
                reads.addAndGet(readOps);
                readBytes.addAndGet(readOps * cleaner.readBufferSize);

                cycleThrottle.throttle(currentEntryHeader.getEntrySize());
                checkContinue();

                /* Skip the data, no need to materialize the entry. */
                skipEntry(entryBuffer);
                return true;
            }

            @Override
            protected boolean isTargetEntry() {
                /*
                 * Only consider visible replicated entries, including erased
                 * entries.
                 */
                if (!entryIsReplicated() || currentEntryHeader.isInvisible()) {
                    return false;
                }
                return true;
            }
        };

        long lastVlsn = NULL_VLSN;

        try {
            while (reader.readNextEntryAllowExceptions()) {
                lastVlsn = reader.getLastVlsn();

                if (lastVlsn == INVALID_VLSN || VLSN.isNull(lastVlsn)) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Replicated entries must have a VLSN.");
                }

                if (!forward) {
                    break;
                }
            }
        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);

        } catch (FileNotFoundException e) {
            return INVALID_VLSN;
        }

        if (fileInfo != null) {
            fileInfo.lastVlsn = lastVlsn;
        }

        return lastVlsn;
    }

    public static void disableKnownObsoleteCheck(boolean value) {
        disableKnownObsoletesCheck = value;
    }

    /**
     * Erases the targeted entries in the given file and makes the changes
     * persistent using fsync.
     */
    private void eraseFile(final Long file) {

        /*
         * If a reserved file can be deleted, this is cheaper than erasure.
         * Use 25% of the file size for throttling.
         *
         * If cleaner.deleteReservedFile returns false then we proceed with
         * erasure. This is safe because the VLSNIndex has been truncated.
         */
        final FileInfo currentFileInfo = fileInfoCache.get(file);
        final long fileLength = currentFileInfo.length;

        final CurrentFileProcessingInfo fileProcessingInfo =
            new CurrentFileProcessingInfo();

        if (fileProtector.isReservedFile(file) &&
            cleaner.deleteReservedFile(file, "ERASER")) {

            filesDeleted.incrementAndGet();
            cycleThrottle.throttle((fileLength * 25) / 100);
            return;
        }

        final EraserReader reader = new EraserReader(file);
        final DbCache dbCache = new DbCache(envImpl);

        final FileProcessor.LookAheadCache lookAheadCache =
            new FileProcessor.LookAheadCache(lookAheadCacheSize);

        fileProcessingInfo.entriesToErase = 0;
        fileProcessingInfo.firstWriteTime = 0;

        for (int i = 0; i < eraseOffsets.length; i += 1) {
            eraseOffsets[i] = 0;
            eraseSizes[i] = 0;
        }

        boolean completed = false;

        final PackedOffsets obsoleteOffsets =
            envImpl.getUtilizationProfile().getObsoleteDetailPacked(
                file, false /*logUpdate*/, this::checkContinue,
                true /*obsoleteBeforeCkpt*/);

        final PackedOffsets.Iterator obsoleteIter = obsoleteOffsets.iterator();
        long nextObsolete = -1;

        fileProcessingInfo.fullFileName = fileManager.getFullFileName(file);
        fileProcessingInfo.raf = null;

        try {
            final TreeLocation location = new TreeLocation();

            /* Clear checksum saved by network restore. */
            envImpl.clearedCachedFileChecksum(
                new File(fileProcessingInfo.fullFileName).getName());

            while (reader.readNextEntryAllowExceptions()) {

                dbCache.clearCachePeriodically();

                final long logLsn = reader.getLastLsn();
                final long fileOffset = DbLsn.getFileOffset(logLsn);
                long expirationTime = 0;
                DbCache.DbInfo dbInfo = null;
                DatabaseId dbId;
                final int headerSize = reader.header.getSize();
                int itemSize = reader.header.getItemSize();
                int entrySize = reader.header.getEntrySize();

                /*
                 * Saving the current offset helps in calculations for work
                 * based throttling (when a file is being erased in multiple
                 * stages).
                 */
                fileProcessingInfo.currentFileOffset = fileOffset;

                while (nextObsolete < fileOffset && obsoleteIter.hasNext()) {
                    nextObsolete = obsoleteIter.next();
                }
                final boolean isKnownObsolete = (nextObsolete == fileOffset);

                boolean doErase = false;
                final LockManager lockManager =
                    envImpl.getTxnManager().getLockManager();

                if (reader.isErased) {

                    final ErasedLogEntry erasedEntry =
                        (ErasedLogEntry) reader.logEntry;

                    if (!erasedEntry.isAllZeros()) {
                        doErase = true;
                    } else {
                        /*
                         * The following makes sure that we don't perform
                         * any treeLookupErasure checks or make a call to
                         * updateEraseEntries() for entries that are already
                         * erased.
                         */
                        continue;
                    }
                } else if (isKnownObsolete && eraseKnownObsoleteEntries &&
                    !disableKnownObsoletesCheck) {
                    /*
                     * Erase all records that were marked as obsolete. These
                     * records are guaranteed not be to be locked or in use by
                     * any transaction or replication or recovery.
                     */
                    doErase = true;
                } else {
                    dbId = reader.logEntry.getDbId();
                    dbInfo = dbCache.getDbInfo(dbId);

                    if (dbInfo.deleting) {
                        /*
                         * If DbInfo.deleting is true the DB will be deleted
                         * soon. In this case the eraser will currently do
                         * Btree lookups and (with delete-handling changes)
                         * add obsolete offsets for INs and LNs in the DB.
                         * This is a unnecessary and could cause a lot of
                         * double counting. Instead we should just ignore all
                         * entries in the DB. The entries will be erased in
                         * the next pass, at which time DbInfo.deleted will be
                         * true.
                         */
                        continue;
                    } else if (dbInfo.deleted) {
                        /*
                         * All LNs and BINs in deleted DBs can be erased. But
                         * when eraseDeletedDbs is false, if the DB is deleted
                         * then we cannot erase its extinct records either,
                         * because we don't have the DB name and dups status.
                         *
                         * If the DB is being deleted (DbInfo.deleting is
                         * true) then we cannot erase its active INs because
                         * they may not have been counted obsolete yet.
                         * However, it is OK to erase its LNs and obsolete
                         * INs, and we do that further below.
                         */
                        if (eraseDeletedDbs) {
                            doErase = true;
                        }
                    } else if (reader.isLN) {
                        final LNLogEntry<?> lnEntry =
                            (LNLogEntry<?>) reader.logEntry;

                        lnEntry.postFetchInit(dbInfo.dups);

                        if (eraseKnownObsoleteEntries) {
                            if (lnEntry.isDeleted()) {
                                /*
                                 * Delete records don't carry data but carry keys
                                 * so erasure is important.
                                 */
                                doErase = true;
                            }

                            /* "Immediately obsolete" LNs can be discarded. */
                            if (!doErase &&
                                (dbInfo.isLNImmediatelyObsolete ||
                                    lnEntry.isEmbeddedLN())) {
                                doErase = true;
                            }

                            if (!doErase) {
                                expirationTime =
                                    lnEntry.getExpirationTime();

                                if (envImpl.expiresWithin(expirationTime, 0
                                        - envImpl.getTtlLnPurgeDelay())) {

                                    /* expired entry */

                                    if (lockManager
                                            .isLockUncontended(logLsn)) {
                                        /*
                                         * Ignore any locked records. These
                                         * will be handled during a subsequent
                                         * erasure cycle.
                                         */
                                        doErase = true;
                                    } else {
                                        /*
                                         * For an expired record that is
                                         * locked, it is better to give up
                                         * (ignore the entry) than to continue
                                         * and do the Btree lookup.
                                         */
                                        continue;
                                    }

                                }
                            }
                        }

                        if (!doErase && eraseExtinctRecords) {
                            final ExtinctionFilter.ExtinctionStatus status =
                                envImpl.getExtinctionStatus(
                                    dbInfo.name, dbInfo.dups, dbInfo.internal,
                                    lnEntry.getKey(), null);

                            if (status == EXTINCT) {
                                /*
                                 * All extinct LNs (in non-deleted DBs) can be
                                 * erased.
                                 */
                                doErase = true;
                            } else if (status == MAYBE_EXTINCT) {
                                /*
                                 * This is a crude way to restart and give the
                                 * the app a chance to initialize its metadata.
                                 */
                                throw new AbortCurrentFileException(file);
                            }
                        }
                    }
                }

                /* Evict before processing each entry. */
                if (DO_CRITICAL_EVICTION) {
                    envImpl.daemonEviction(true /*backgroundIO*/);
                }

                /* The entry is not known to be obsolete -- process it now. */
                assert lookAheadCache != null;

                /* Tree lookup based erasure */
                if (!eraseAllObsoleteEntries || doErase) {
                    /* Skip Tree lookup based erasure */
                } else if (reader.isLN) {
                    final LNLogEntry<?> lnEntry = reader.getLNLogEntry();

                    if (dbInfo == null) {
                        dbId = reader.logEntry.getDbId();
                        dbInfo = dbCache.getDbInfo(dbId);
                    }

                    lnEntry.postFetchInit(dbInfo.dups);
                    final LN targetLN = lnEntry.getLN();
                    final byte[] key = lnEntry.getKey();
                    dbId = reader.logEntry.getDbId();

                    /*
                     * Note that the final check for a deleted DB is
                     * performed in processLNErase.
                     */
                    lookAheadCache.add(
                        DbLsn.getFileOffset(logLsn),
                        new LNInfo(
                            targetLN, dbId, key, expirationTime,
                            lnEntry.getModificationTime(), headerSize,
                            itemSize));

                    if (lookAheadCache.isFull()) {
                        processLNErase(file, location,
                            lookAheadCache, dbCache, fileProcessingInfo);
                    }
                } else {
                    /*
                     * Do the final check for a deleted DB, prior to
                     * processing (and potentially migrating) an IN.
                     */
                    dbId = reader.logEntry.getDbId();
                    dbInfo = dbCache.getDbImpl(dbId);

                    if (dbInfo.deleting) {
                        continue;
                    } else if (dbInfo.deleted) {
                        doErase = true;
                    } else if (reader.isIN || reader.isBIN) {

                        final DatabaseImpl db = dbInfo.dbImpl;
                        final IN targetIN = reader.getIN(db);
                        targetIN.setDatabase(db);

                        doErase =
                            isINObsolete(targetIN, db, logLsn, entrySize);
                    } else if (reader.isBINDelta) {
                        final BIN delta = reader.getBINDelta();
                        doErase =
                            isBINDeltaObsolete(delta, dbInfo.dbImpl, logLsn,
                                entrySize);
                    } else {
                        assert false;
                    }
                }

                if (doErase) {
                    updateEraseEntries(fileProcessingInfo, fileOffset,
                        headerSize, itemSize, fileLength, false);
                }
            }

            /* Process remaining queued LNs. */
            while (!lookAheadCache.isEmpty()) {
                if (DataEraser.DO_CRITICAL_EVICTION) {
                    envImpl.daemonEviction(true /*backgroundIO*/);
                }

                processLNErase(file, location,
                    lookAheadCache, dbCache, fileProcessingInfo);

                /* Sleep if background read/write limit was exceeded. */
                envImpl.sleepAfterBackgroundIO();
            }

            /* Call releaseDbImpls before eraseEntries, which throttles. */
            dbCache.releaseDbImpls();

            eraseEntries(fileProcessingInfo, false);
            filesErased.incrementAndGet();
            completed = true;

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } catch (FileNotFoundException e) {
            /* File was deleted by the cleaner. */
            filesAlreadyDeleted.incrementAndGet();

        } catch (IOException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_WRITE,
                "Exception erasing file 0x" + Long.toHexString(file),
                e);

        } finally {

            if (fileProcessingInfo.firstWriteTime != 0) {
                LoggerUtils.info(
                    logger, envImpl,
                    "ERASER attempted to erase " +
                        (fileProcessingInfo.entriesToErase +
                            fileProcessingInfo.totalEntriesErased) +
                        " entries in file 0x" + Long.toHexString(file) +
                        ", first write at " +
                        formatTime(fileProcessingInfo.firstWriteTime) +
                        ", erasure is " +
                        (completed ? "complete" : "incomplete") + ".");
            }

            dbCache.releaseDbImpls();

            if (fileProcessingInfo.raf != null) {
                try {
                    fileProcessingInfo.raf.close();
                } catch (IOException e) {
                    LoggerUtils.warning(
                        logger, envImpl,
                        "DataEraser.eraseFile exception when closing " +
                            "file 0x" + Long.toHexString(file) + ": " + e);
                }
            }
        }
    }

    /**
     * This method updates the type of the record to obsolete in its header
     * and adds a corresponding entry into the fileInfo for the contents to be
     * zeroed out later.
     */
    private void updateEraseEntries(CurrentFileProcessingInfo processingInfo,
                                    long fileOffset,
                                    int headerSize,
                                    int itemSize,
                                    long fileLength,
                                    boolean latchHeld)
        throws FileNotFoundException, IOException {

        if (processingInfo.raf == null) {
            processingInfo.firstWriteTime = TimeSupplier.currentTimeMillis();
            processingInfo.raf =
                fileManager.openFileReadWrite(processingInfo.fullFileName);
            touchAndFsync(processingInfo.raf, fileLength, latchHeld);
        }

        writeErasedType(processingInfo.raf, fileOffset);

        processingInfo.entriesToErase = addEraseEntry(
            processingInfo,
            fileOffset + headerSize,
            itemSize,
            latchHeld);
    }

    /**
     * This method just checks if the BIN-delta is obsolete. It returns TRUE
     * if it is obsolete.
     */
    private boolean isBINDeltaObsolete(BIN deltaClone,
                                       DatabaseImpl db,
                                       long logLsn,
                                       int entrySize) {
        BIN bin = null;

        /* Search for the BIN's parent by level, to avoid fetching the BIN. */
        deltaClone.setDatabase(db);
        deltaClone.latch(CacheMode.UNCHANGED);

        final SearchResult result = db.getTree().getParentINForChildIN(
            deltaClone, true /*useTargetLevel*/,
            true /*doFetch*/, CacheMode.UNCHANGED);

        try {
            if (!result.exactParentFound) {
                /*
                 * TODO: This should never happen because it implies the tree
                 * is empty or the number of levels has been reduced, which
                 * currently never happens. If we do this in the future we'll
                 * have to check for durability by comparing the tree root LSN
                 * to the anchorLsn.
                 */
                return false;
            }

            final long treeLsn = result.parent.getLsn(result.index);

            /*
             * Need to make use of maxAnchorLsn condition to account for
             * recovery accessing/using obsolete BINS.
             */
            final long maxAnchorLsn =
                envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn();

            if (treeLsn == DbLsn.NULL_LSN) {
                /*
                 * Without NodeId any inference based on treelsn comparison
                 * is meaningless.
                 */
                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        deltaClone.getLogType(),
                        entrySize);

                return false;
            }

            final int cmp = DbLsn.compareTo(treeLsn, logLsn);
            if (cmp == 0) {
                return false;
            }

            bin = (BIN) result.parent.fetchIN(
                result.index, CacheMode.UNCHANGED);

            final long binId = bin.getNodeId();

            if (binId != deltaClone.getNodeId()) {
                /*
                 * treeLsn comparison is meaningless if these are different
                 * nodes. But since the node is clearly not in the tree, it
                 * can be processed after a subsequent checkpoint.
                 */
                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        deltaClone.getLogType(),
                        entrySize);

                return false;
            }

            /*
             * We are here because treeLsn != logLsn. In this case the log
             * entry is obsolete.
             *
             * If cmp is > 0 then log entry is obsolete because it is older
             * than the version in the tree.
             *
             * If cmp is < 0 then log entry is also obsolete, because the old
             * parent slot was deleted and we're now looking at a completely
             * different IN due to the by-level search above.
             *
             * But only If the entry corresponding to treeLsn was created
             * before checkpoint, then we can authoritatively say that logLsn
             * entry won't be required by recovery and we can erase it.
             */
            if (DbLsn.compareTo(treeLsn, maxAnchorLsn) < 0) {
                return true;
            }

            handleBinDeltaForErasure(bin, null, treeLsn);

            return false;
        } finally {
            /*
             * If the BIN was not resident, evict it immediately to avoid
             * impacting the cache.
             */
            if (bin != null) {
                bin.latchNoUpdateLRU();
                result.parent.releaseLatch();

                if (bin.getFetchedCold()) {
                    /* This releases the latch. */
                    envImpl.getEvictor().doCacheModeEvict(
                        bin, CacheMode.EVICT_BIN);
                } else {
                    bin.releaseLatch();
                }
            }

            if (result.parent != null) {
                result.parent.releaseLatchIfOwner();
            }
        }
    }

    /**
     * Diagnostic Wrapper for the main method for figuring out if an IN is
     * obsolete or not.
     */
    private boolean isINObsolete(IN inClone,
                                 DatabaseImpl db,
                                 long logLsn,
                                 int entrySize) {

        boolean obsolete = false;
        boolean completed = false;

        try {
            Tree tree = db.getTree();
            assert tree != null;

            obsolete = isINObsolete(tree, db, inClone, logLsn, entrySize);

            completed = true;
            return obsolete;
        } finally {
            logFine(ERASE_IN, inClone, logLsn, completed, obsolete);
        }
    }

    /**
     * Given a clone of an IN that has been taken out of the log, try to find
     * it in the tree and verify that it is the current one in the log. If we
     * fail to find the IN in the tree, then that means the IN is obsolete.
     * This method is heavily inspired from the processIN() method used for
     * cleaner.
     *
     * Returns true if the IN is obsolete else false.
     */
    private boolean isINObsolete(Tree tree,
                                 DatabaseImpl db,
                                 IN inClone,
                                 long logLsn,
                                 int entrySize) {

        /* Find the lsn corresponding to the latest checkpoint */
        final long maxAnchorLsn =
            envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn();

        /* Check if inClone is the root. */
        if (inClone.isRoot()) {
            IN rootIN =
                FileProcessor.isRoot(tree, db, inClone, logLsn, maxAnchorLsn);
            if (rootIN == null) {

                /*
                 * inClone is a root, but no longer in use.
                 * Hence, obsolete = true.
                 */
                return true;
            }

            rootIN.releaseLatch();
            return false;
        }

        /* It's not the root.  Can we find it, and if so, is it current? */
        inClone.latch(CacheMode.UNCHANGED);
        SearchResult result = null;
        BIN bin = null;
        IN inFromTree = null;
        try {
            result = tree.getParentINForChildIN(
                inClone, true /*useTargetLevel*/,
                true /*doFetch*/, CacheMode.UNCHANGED);

            if (!result.exactParentFound) {
                /*
                 * TODO: This should never happen because it implies the tree
                 * is empty or the number of levels has been reduced, which
                 * currently never happens. If we do this in the future we'll
                 * have to check for durability by comparing the tree root LSN
                 * to the anchorLsn.
                 */
                return false;
            }

            /* Note that treeLsn may be for a BIN-delta, see below. */
            IN parent = result.parent;
            long treeLsn = parent.getLsn(result.index);

            /*
             * We need nodeId and valid treeLsn for a meaningful comparison.
             */
            if (treeLsn == DbLsn.NULL_LSN) {

                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        inClone.getLogType(),
                        entrySize);

                return false;
            }

            /*
             * If tree and log LSNs are equal, then we've found the exact IN
             * we read from the log.  We know the treeLsn is not for a
             * BIN-delta, because it is equal to LSN of the IN (or BIN) we
             * read from the log.
             */
            if (treeLsn == logLsn) {
                return false;
            }

            /*
             * If the tree and log LSNs are unequal, then we must get both the
             * nodeId and full version LSN in case the tree LSN is actually
             * for a BIN-delta.
             * The only way to do that is to fetch the IN in the tree; however,
             * we only need the delta not the full BIN.
             */
            inFromTree = parent.fetchIN(result.index, CacheMode.UNCHANGED);

            final long inId = inFromTree.getNodeId();

            if (inId != inClone.getNodeId()) {
                /*
                 * treeLsn comparison is meaningless if these are different
                 * nodes
                 */
                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        inClone.getLogType(),
                        entrySize);

                return false;
            }

            if (inClone.isUpperIN()) {
                /*
                 * We are here because treeLsn != logLsn. Unless the update
                 * happened before maxAnchorLsn, the parent could be pointing
                 * logLsn at checkpoint time and hence logLsn would be required
                 * at recovery time.
                 */
                if (DbLsn.compareTo(treeLsn, maxAnchorLsn) < 0) {
                    return true;
                }

                return false;
            }

            /* Log entry is not for an upperIN. */
            bin = (BIN) inFromTree;

            /* Get the fullLsn in case parent points to a binDelta. */
            treeLsn = bin.getLastFullLsn();

            /* Now compare LSNs, since we know treeLsn is the full version. */
            final int compareVal = DbLsn.compareTo(treeLsn, logLsn);

            /*
             * If cmp is > 0 then log entry is obsolete because it is older
             * than the version in the tree.
             *
             * If cmp is < 0 then log entry is also obsolete, because the old
             * parent slot was deleted and we're now looking at a completely
             * different IN due to the by-level search above.
             */
            if (compareVal != 0) {
                /*
                 * We are here because treeLsn != logLsn. Unless the update
                 * happened before maxAnchorLsn, the parent could be pointing
                 * logLsn at checkpoint time and hence log entry would be
                 * required at recovery time.
                 */
                if (DbLsn.compareTo(treeLsn, maxAnchorLsn) < 0) {
                    return true;
                }
            }

            handleBinDeltaForErasure(bin, inClone, treeLsn);

            return false;
        } finally {
            if (inFromTree != null) {
                inFromTree.latchNoUpdateLRU();
                result.parent.releaseLatch();

                /*
                 * Make sure that a bin with a key corresponding to obsolete
                 * data, gets compressed. To ensure this mark it dirty, if it's
                 * not already dirty. A flush during eviction or checkpoint
                 * will trigger the required compression. If its identifier
                 * key is updated in the process, then the corresponding parent
                 * IN will be updated.
                 */
                if (bin != null && !bin.getDirty()) {
                    if (bin.shouldCompressObsoleteKeys()) {
                        bin.setDirty(true);
                    }
                }

                /*
                 * If the BIN was not resident, evict it heap immediately to
                 * avoid impacting the cache.
                 */
                if (inFromTree.getFetchedCold()) {
                    /* This releases the latch. */
                    envImpl.getEvictor().doCacheModeEvict(
                        inFromTree, CacheMode.EVICT_BIN);
                } else {
                    inFromTree.releaseLatch();
                }
            }

            if (result != null && result.exactParentFound) {
                result.parent.releaseLatchIfOwner();
            }
        }
    }

    /*
     * This method ensures that a bin-delta encountered during an erasure
     * cycle is taken care of before the end of next erasure cycle.
     */
    private void handleBinDeltaForErasure(BIN binDelta, IN inClone,
                                          long treeLsn) {
        /*
         * If this is the first time we are encountering this BIN-delta,
         * during an erasure cycle, then we will tag it. We hope that
         * keys in this delta and corresponding will get handled before
         * the next erasure cycle.
         * If that doesn't happen, then it means this delta didn't see
         * much action since last erasure cycle and to ensure that it
         * gets handled before the next cycle, we need to make sure
         * corresponding full-BIN gets compressed. To ensure this we
         * mutate it to full-BIN, mark that BIN dirty and set prohibit
         * delta.
         */
        binDelta.latch(CacheMode.UNCHANGED);

        try {
            if (binDelta.isBINDelta()) {
                /*
                 * The following condition ensure that in spite of scenarios
                 * involving crashes, erasure-cycle suspension etc. very-old
                 * bin-deltas are removed from the system within two
                 * erasure cycles.
                 */
                if (DbLsn.compareTo(treeLsn, oldestAllowedObsoleteLSN) < 0) {
                    if (inClone != null) {
                        binDelta.mutateToFullBIN((BIN) inClone, false);
                    } else {
                        binDelta.mutateToFullBIN(false);
                    }
                    migrateIN(binDelta);
                }
            }
        } finally {
            binDelta.releaseLatchIfOwner();
        }
    }

    private void migrateIN(IN in) {

        /*
         * IN is still in the tree.  Dirty it.  Checkpoint or eviction
         * will write it out.
         *
         * Prohibit the next delta, since the original version must be
         * made obsolete.
         *
         * Compress to reclaim space for expired slots, including dirty
         * slots.
         */
        in.setDirty(true);
        in.setProhibitNextDelta(true);
        envImpl.lazyCompress(in, true /*compressDirtySlots*/);
        in.releaseLatch();
    }

    /**
     * Processes the first LN in the look ahead cache and removes it from the
     * cache. While the BIN is latched, look through the BIN for other LNs in
     * the cache; if any match, process them to avoid a tree search later.
     *
     * Note: This method is a heavily inspired from the corresponding method
     * for cleaner. The difference being in case of cleaner we intend to find
     * if the LN is active whereas here we try to find if the LN is obsolete
     * and if so then we queue it up for erasure.
     */
    private void processLNErase(
        final Long fileNum,
        final TreeLocation location,
        final FileProcessor.LookAheadCache lookAheadCache,
        final DbCache dbCache,
        CurrentFileProcessingInfo fileProcessingInfo) throws IOException {

        /* Get the first LN from the queue. */
        final Long fileOffset = lookAheadCache.nextOffset();
        final LNInfo info = lookAheadCache.remove(fileOffset);

        final LN lnFromLog = info.getLN();
        final byte[] keyFromLog = info.getKey();
        final long logLsn = DbLsn.makeLsn(fileNum, fileOffset);
        final FileInfo currentFileInfo = fileInfoCache.get(fileNum);
        final long fileLength = currentFileInfo.length;

        boolean obsolete = false;     // The LN is no longer in use.

        /* Status variables are used to generate debug tracing info. */
        boolean completed = false;    // This method completed.

        /*
         * Do the final check for a deleted DB, prior to processing (and
         * potentially migrating) the LN. If the DB has been deleted,
         * perform the housekeeping tasks for an obsolete LN.
         */
        final DatabaseId dbId = info.getDbId();
        final DbCache.DbInfo dbInfo = dbCache.getDbImpl(dbId);

        if (dbInfo.deleted) {

            updateEraseEntries(fileProcessingInfo, fileOffset,
                info.getHeaderSize(), info.getItemSize(), fileLength, false);

            logFine(ERASE_LN, lnFromLog, logLsn, true, true);

            return;
        } else if (dbInfo.deleting) {
            /*
             * This will be processed in the next erasure cycle when
             * dbInfo.deleted is set to true.
             */
            return;
        }

        final DatabaseImpl db = dbInfo.dbImpl;

        BIN bin = null;

        try {
            final Tree tree = db.getTree();
            assert tree != null;

            /* Find parent of this LN. */
            final boolean parentFound = tree.getParentBINForChildLN(
                location, keyFromLog, false /*splitsAllowed*/,
                false /*blindDeltaOps*/, CacheMode.UNCHANGED);

            bin = location.bin;
            final int index = location.index;

            if (!parentFound && bin == null) {
                /*
                 * TODO: This should never happen because it implies the tree
                 *  is empty, which currently never happens. If we do this in
                 *  the future we'll have to check for durability by comparing
                 *  the tree root LSN to the anchorLsn.
                 */
                obsolete = false;
                completed = true;
                return;
            } else if (!parentFound) {
                /*
                 * The bin returned above is not the original parent and we
                 * don't have information available to guarantee that the LN
                 * deletion happened before the checkpoint or that it is
                 * durable. Hence we don't erase logLsn now. We queue it for
                 * erasure after subsequent checkpoint.
                 */
                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        lnFromLog.getGenericLogType(),
                        info.getHeaderSize() + info.getItemSize());

                return;
            }

            final long maxAnchorLsn =
                envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn();

            final long treeParentLsn = bin.getLastLoggedLsn();

            final boolean binDirtyCheck = !bin.getDirty() &&
                treeParentLsn != DbLsn.NULL_LSN &&
                DbLsn.compareTo(treeParentLsn, maxAnchorLsn) < 0;

            /*
             * Now we're at the BIN parent for this LN.  If knownDeleted, LN is
             * deleted and can be purged.
             */
            obsolete = isFoundLNObsolete(
                info, logLsn, bin.getLsn(index), bin, index, binDirtyCheck);

            if (obsolete) {
                updateEraseEntries(fileProcessingInfo, fileOffset,
                    info.getHeaderSize(), info.getItemSize(), fileLength,
                    true);
            }

            completed = true;

            /*
             * For all other non-deleted LNs in this BIN, lookup their LSN
             * in the LN queue and process any matches.
             */
            for (int i = 0; i < bin.getNEntries(); i += 1) {

                final long binLsn = bin.getLsn(i);

                if (i != index &&
                    DbLsn.getFileNumber(binLsn) == fileNum) {

                    final Long myOffset = DbLsn.getFileOffset(binLsn);
                    final LNInfo myInfo = lookAheadCache.remove(myOffset);

                    /* If the offset is in the cache, it's a match. */
                    if (myInfo != null) {
                        boolean foundLNObsolete;

                        /*
                         * Not counting entries with bin.isEntryPendingDeleted
                         * set as obsolete. Since the record may be
                         * write-locked and the writing txn may abort.
                         */
                        foundLNObsolete =
                            isFoundLNObsolete(myInfo, binLsn, binLsn,
                                bin, i, binDirtyCheck);

                        if (foundLNObsolete) {
                            updateEraseEntries(fileProcessingInfo,
                                fileOffset, myInfo.getHeaderSize(),
                                myInfo.getItemSize(), fileLength, true);

                            logFine(ERASE_LN, myInfo.getLN(), binLsn, true,
                                true);
                        }
                    }
                }
            }

            /*
             * If the BIN was not resident, evict it immediately to avoid
             * impacting the cache.
             */
            if (bin.getFetchedCold()) {

                final BIN binToEvict = bin;
                bin = null;

                /* This releases the latch. */
                envImpl.getEvictor().doCacheModeEvict(
                    binToEvict, CacheMode.UNCHANGED);
            }
        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }

            logFine(ERASE_LN, lnFromLog, logLsn, completed, obsolete);
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void logFine(String action,
                 Node node,
                 long logLsn,
                 boolean completed,
                 boolean obsolete) {

        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            sb.append(action);
            if (node instanceof IN) {
                sb.append(" node=");
                sb.append(((IN) node).getNodeId());
            }
            sb.append(" logLsn=");
            sb.append(DbLsn.getNoFormatString(logLsn));
            sb.append(" complete=").append(completed);
            sb.append(" obsolete=").append(obsolete);

            LoggerUtils.logMsg(logger, envImpl, Level.FINE, sb.toString());
        }
    }


    /**
     * Detects Obsolete status of a given LN(based purely on tree search).
     * Returns the obsolete status as result. Returns true for obsolete LNs
     * and returns false for active LNs.
     *
     * Note : This has been inspired from processFoundLN() for the cleaner.
     * But unlike processFoundLN(), this method aims to find obsolete LNs and
     * no writing or migration happens here.
     *
     * Note2: We don't perform the check against lastCheckpointLSN for LNs.
     *
     * @param info identifies the LN log entry.
     *
     * @param logLsn is the LSN of the log entry.
     *
     * @param treeLsn is the LSN found in the tree.
     *
     * @param bin is the BIN found in the tree; is latched on method entry and
     * exit.
     *
     * @param index is the BIN index found in the tree.
     *
     * @param binDirtyCheck
     * @return true for obsolete LNs and false for active LNs.
     */
    private boolean isFoundLNObsolete(final LNInfo info,
                                      final long logLsn,
                                      final long treeLsn,
                                      final BIN bin,
                                      final int index,
                                      boolean binDirtyCheck) {

        final LN lnFromLog = info.getLN();

        final DatabaseImpl db = bin.getDatabase();

        /* Status variables are used to generate debug tracing info. */
        boolean obsolete = false;  // The LN is no longer in use.
        boolean completed = false; // This method completed.

        try {
            final Tree tree = db.getTree();
            assert tree != null;

            /*
             * Before queuing an LN for erasure, we must lock it and then check
             * to see whether it is obsolete or active.
             *
             * 1. If the LSN in the tree and in the log are the same, then its
             * active and should be ignored by erasure.
             *
             * 2. If the LSN in the tree is < the LSN in the log, the log entry
             * is obsolete, because this LN has been rolled back to a previous
             * version by a txn that aborted.
             *
             * 3. If the LSN in the tree is > the LSN in the log, the log entry
             * is obsolete, because the LN was advanced forward by some
             * now-committed txn.
             *
             * 4. If the LSN in the tree is a null LSN, the log entry is
             * obsolete. A slot should only have a null LSN if an insertion
             * record was aborted, which means it is obsolete.
             */
            if (treeLsn == DbLsn.NULL_LSN) {
                /*
                 * We can't be sure about if the bin returned above was the
                 * original parent and so we can't use this treeLsn value
                 * to make any inference.
                 */
                obsolete = false;
                completed = true;

                suspectObsoleteTracker
                    .countObsoleteNode(logLsn,
                        lnFromLog.getGenericLogType(),
                        info.getHeaderSize() + info.getItemSize());

                return false;
            }

            /*
             * Now we're at the BIN parent for this LN.  If knownDeleted, LN is
             * deleted and can be purged.
             */
            if (binDirtyCheck && bin.isEntryKnownDeleted(index)) {
                obsolete = true;
                completed = true;
                return true;
            }

            /*
             * If treeLsn == logLsn, then this LN is definitely active and
             * we should return false now.
             */
            if (treeLsn == logLsn) {
                obsolete = false;
                completed = true;

                return false;
            }

            final long maxAnchorLsn =
                envImpl.getCheckpointer().getLastCheckpointFirstActiveLsn();

            /*
             * If the treeLsn is older than maxAnchorLsn, then there is no
             * need to acquire lock to check if the txn that created treeLsn
             * has committed.
             *
             * Also, we are here because we found treeLsn == logLsn is false.
             * So if treeLsn < maxAnchorLsn, then it means we won't undo
             * treeLsn and thus logLsn is obsolete.
             */
            if (DbLsn.compareTo(treeLsn, maxAnchorLsn) < 0) {
                obsolete = true;
                completed = true;

                return true;
            }

            /*
             * Note: Even if we were to successfully lock treeLsn and confirm
             * that corresponding txn has committed, there is no guarantee that
             * all the logs corresponding to that txn are durable(because of
             * NO_SYNC and WRITE_NO_SYNC modes). So we can't definitively
             * say that we wont undo treeLsn and then restore logLsn.
             * So we can't afford to erase logLsn.
             */

            completed = true;
            return false;

        } finally {
            /*
             * Note: We are logging this before we queue up the entries in the
             * parent/calling method.
             */
            logFine(ERASE_LN, lnFromLog, logLsn, completed, obsolete);
        }
    }

    /*
     * All suspectObsoletes that have been gathered by eraser before the latest
     * checkpoint will now be added to TFS in global utilizationTracker. This
     * is because after a checkpoint we can say with certainty that they won't
     * be used by recovery. It can take upto 3 checkpoint for these to show up
     * in obsoleteOffsets for erasure.
     *
     * LogManager ensures that this flush is protected by logWriteMutex.
     */
    public void flushSuspectObsoleteData() {

        envImpl.getLogManager()
            .transferToUtilizationTracker(suspectObsoleteTracker);
    }

    /**
     * File reader that materializes only user-database LNs, and BINs and
     * BIN-deltas.
     */
    private class EraserReader extends FileReader {
        LogEntryHeader header;
        LogEntryType entryType;
        LogEntry logEntry;
        boolean isLN;
        boolean isIN;
        boolean isBIN;
        boolean isErased;
        boolean isBINDelta;

        EraserReader(final Long file) {
            super(DataEraser.this.envImpl, cleaner.readBufferSize,
                true /*forward*/,
                DbLsn.makeLsn(file, 0) /*startLsn*/,
                file /*singleFileNumber*/,
                DbLsn.NULL_LSN /*endOfFileLsn*/,
                DbLsn.NULL_LSN /*finishLsn*/);
        }

        @Override
        protected boolean processEntry(final ByteBuffer entryBuffer) {

            final int readOps = getAndResetNReads();
            reads.addAndGet(readOps);
            readBytes.addAndGet(readOps * cleaner.readBufferSize);

            cycleThrottle.throttle(currentEntryHeader.getEntrySize());
            checkContinue();

            header = currentEntryHeader;
            isLN = false;
            isIN = false;
            isBIN = false;
            isErased = false;
            isBINDelta = false;
            logEntry = null;
            entryType = LogEntryType.findType(header.getType());
            assert entryType != null;

            /*
             * NOTE :: No need to deal with dbtree root because it does not
             * contain user data.
             */
            if (entryType.isUserLNType()) {
                isLN = true;
            } else if (entryType.equals(LogEntryType.LOG_BIN_DELTA)) {
                isBINDelta = true;
            } else if (entryType.equals(LogEntryType.LOG_BIN)) {
                isBIN = true;
            } else if (entryType.equals(LogEntryType.LOG_IN)) {
                isIN = true;
            } else if (entryType.equals(LogEntryType.LOG_ERASED)) {
                isErased = true;
            } else {
                skipEntry(entryBuffer);
                return false;
            }

            logEntry = entryType.getNewLogEntry();
            logEntry.readEntry(envImpl, header, entryBuffer);
            return true;
        }

        public IN getIN(DatabaseImpl dbImpl) {
            return ((INLogEntry<?>) logEntry).getIN(dbImpl);
        }

        public BIN getBINDelta() {
            return ((BINDeltaLogEntry) logEntry).getMainItem();
        }

        /**
         * Get the last LN log entry seen by the reader.  Note that
         * LNLogEntry.postFetchInit must be called before calling certain
         * LNLogEntry methods.
         */
        public LNLogEntry<?> getLNLogEntry() {
            return (LNLogEntry<?>) logEntry;
        }
    }

    /**
     * Adds the offset/size of one entry to be erased. If the number of entries
     * has crossed the MAX_ERASURE_ENTRIES threshold, then it triggers
     * erasure of the file.
     */
    private int addEraseEntry(final CurrentFileProcessingInfo processingInfo,
                              final long offset,
                              final int size,
                              boolean latchHeld) throws IOException {
        int n = processingInfo.entriesToErase;

        if (n == eraseOffsets.length) {
            eraseEntries(processingInfo, latchHeld);
            n = processingInfo.entriesToErase;
        }

        eraseOffsets[n] = offset;
        eraseSizes[n] = size;
        return n + 1;
    }

    /**
     * Ensures that the file's lastModifiedTime is persistently updated,
     * before performing any modifications to the file. This allows
     * applications to detect erasure by checking for a change to
     * lastModifiedTime across erasure cycles, without the worry that a
     * crash during erasure may have prevented the file system from updating
     * the lastModifiedTime.
     */
    private void touchAndFsync(final RandomAccessFile file,
                               final long fileLength,
                               final boolean synchronizationHeld)
        throws IOException {
        checkContinue();

        file.seek(0);
        final byte b = file.readByte();
        file.seek(0);
        file.writeByte(b);
        writes.incrementAndGet();
        writeBytes.incrementAndGet();

        fsync(file);

        if (!synchronizationHeld) {
            cycleThrottle.throttle((fileLength * FSYNC1_WORK_PCT) / 100);
        }

        // TODO update FileManager stats?
    }

    /**
     * Changes the type of the entry on disk to the LOG_ERASED type.
     */
    private void writeErasedType(final RandomAccessFile file,
                                 final long headerOffset)
        throws IOException {

        file.seek(headerOffset + LogEntryHeader.ENTRYTYPE_OFFSET);
        checkContinue();
        file.writeByte(LogEntryType.LOG_ERASED.getTypeNum());

        writes.incrementAndGet();
        writeBytes.incrementAndGet();

        // TODO update FileManager stats?
    }

    /**
     * Writes zeros in the entries in the erase list and fsyncs to persist
     * the changes.
     */
    private void eraseEntries(
    		CurrentFileProcessingInfo fileProcessingInfo,
    		boolean latchHeld)
        throws IOException {

        final RandomAccessFile raf = fileProcessingInfo.raf;
        final long fileLength = fileProcessingInfo.currentFileOffset -
            fileProcessingInfo.prevFileOffset;
        final int entries = fileProcessingInfo.entriesToErase;

        if (entries == 0) {
            return;
        }

        final long fsync1Work = (fileLength * FSYNC1_WORK_PCT) / 100;
        final long fsync2Work = (fileLength * FSYNC2_WORK_PCT) / 100;
        final long fsync3Work = (fileLength * FSYNC3_WORK_PCT) / 100;
        final long writeWork = ((fileLength * WRITE_WORK_PCT) / 100) / entries;

        /* Don't neglect work lost due to int truncation. */
        final long extraWork = fileLength -
            ((writeWork * entries) + fsync1Work + fsync2Work + fsync3Work);

        /*
         * Make type changes persistent before writing zeros. This is the only
         * way to be sure that an entry's type is changed before it is zero
         * filled, since write ordering is not guaranteed.
         */
        checkContinue();
        fsync(raf);
        if (!latchHeld) {
        	cycleThrottle.throttle(fsync2Work);
        }

        for (int i = 0; i < entries; i += 1) {

            final long offset = eraseOffsets[i];
            final int size = eraseSizes[i];

            if (zeros.length < size) {
                zeros = new byte[size * 2];
            }

            raf.seek(offset);
            checkContinue();
            raf.write(zeros, 0, size);
            writes.incrementAndGet();
            writeBytes.addAndGet(size);
            if (!latchHeld) {
            	cycleThrottle.throttle(writeWork);
            }
        }

        /* Make complete erasure of this file persistent. */
        checkContinue();
        fsync(raf);
        if (!latchHeld) {
        	cycleThrottle.throttle(fsync3Work + extraWork);
        }

        fileProcessingInfo.totalEntriesErased += entries;
        fileProcessingInfo.entriesToErase = 0;
        fileProcessingInfo.prevFileOffset =
            fileProcessingInfo.currentFileOffset;

        for (int i = 0; i < entries; i += 1) {
            eraseOffsets[i] = 0;
            eraseSizes[i] = 0;
        }

        // TODO update FileManager stats?
    }

    /**
     * Returns whether the log entry at the given LSN has been erased.
     *
     * Is not particularly efficient (it opens and closes the file each time
     * it is called) and is meant for exceptional circumstances such as when a
     * checksum verification fails.
     */
    public boolean isEntryErased(final long lsn) {

        final long file = DbLsn.getFileNumber(lsn);
        final long offset = DbLsn.getFileOffset(lsn);

        /*
         * Can't use FileManager.getFileHandle here because we may be called
         * with the FileHandle already latched and latching is not reentrant.
         */
        RandomAccessFile fileHandle = null;
        try {
            fileHandle = new RandomAccessFile(
                fileManager.getFullFileName(file),
                FileManager.FileMode.READ_MODE.getModeValue());

            fileHandle.seek(offset + LogEntryHeader.ENTRYTYPE_OFFSET);

            return fileHandle.readByte() ==
                LogEntryType.LOG_ERASED.getTypeNum();

        } catch (FileNotFoundException|EOFException e) {
            return false;

        } catch (IOException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_WRITE,
                "Exception checking erasure file 0x" + Long.toHexString(file),
                e);
        } finally {
            if (fileHandle != null) {
                try {
                    fileHandle.close();
                } catch (IOException e) {
                    LoggerUtils.warning(
                        logger, envImpl,
                        "DataEraser.isEntryErased exception when closing " +
                            "file 0x" + Long.toHexString(file) + ": " + e);
                }
            }
        }
    }

    /**
     * Utility to help spread a given amount of totalWork overs a given
     * durationMs.
     */
    private class WorkThrottle {

        private final float msPerUnitOfWork;
        private final long startTime;
        private long workDone;

        WorkThrottle(final long totalWork,
                     final long durationMs) {

            msPerUnitOfWork = ((float) durationMs) / totalWork;
            startTime = TimeSupplier.currentTimeMillis();
            workDone = 0;
        }

        /**
         * Waits until the ratio of elapsed time to durationMs is GTE the
         * ratio of workDone to totalWork. Elapsed time is time since the
         * constructor was called.
         *
         * If the computed wait time is less than MIN_WORK_DELAY_MS, no wait
         * is performed. This is meant to prevent large numbers of waits for
         * relatively small amounts of work done. Note that this method is
         * called frequently to add very very small amounts of work.
         */
        void throttle(final long addWork) {

            workDone += addWork;

            final long workDoneMs = (long) (workDone * msPerUnitOfWork);

            final long delayMs =
                workDoneMs - (TimeSupplier.currentTimeMillis() - startTime);

            if (delayMs >= MIN_WORK_DELAY_MS) {

                final long checkMs = Math.min(delayMs, pollCheckMs);

                PollCondition.await(checkMs, delayMs, pollMutex,
                    () -> {
                        checkContinue();
                        return false;
                    });
            }
        }
    }

    private void checkShutdown() {
        if (shutdownRequested || !envImpl.isValid()) {
            throw new ShutdownRequestedException();
        }
    }

    private void checkContinue() {

        checkShutdown();

        if (!enabled) {
            throw new ErasureDisabledException();
        }

        if (endTime - startTime != cycleMs) {
            throw new PeriodChangedException();
        }

        synchronized (currentFileMutex) {
            if (abortCurrentFile) {
                final long file = currentFile;
                currentFile = null;
                abortCurrentFile = false;
                throw new AbortCurrentFileException(file);
            }
        }
    }

    private boolean checkForCycleEnd() {

        if (TimeSupplier.currentTimeMillis() >= endTime) {
            if (filesRemaining.isEmpty()) {
                logCycleComplete();
            } else {
                logCycleIncomplete();
            }

            /*
             * Future erasure cycles need an LSN corresponding to the start of
             * last erasure cycle to identify and remove obsolete, non-dirty
             * metadata like bin-deltas. We must update this at the cycle end
             * time.
             */
            oldestAllowedObsoleteLSN = currentCycleStartLSN;

            return true;
        }

        return false;
    }

    @SuppressWarnings("serial")
    private static class NoUnprotectedFilesException extends RuntimeException
        implements NotSerializable {

        final Level logLevel;

        NoUnprotectedFilesException(final String msg,
                                    final Level logLevel) {
            super(msg);
            this.logLevel = logLevel;
        }
    }

    @SuppressWarnings("serial")
    private static class ErasureDisabledException extends RuntimeException
        implements NotSerializable {
    }

    @SuppressWarnings("serial")
    private static class PeriodChangedException extends RuntimeException
        implements NotSerializable {
    }

    @SuppressWarnings("serial")
    private static class ShutdownRequestedException extends RuntimeException
        implements NotSerializable {
    }

    @SuppressWarnings("serial")
    private static class AbortCurrentFileException extends RuntimeException
        implements NotSerializable {
        final long file;

        AbortCurrentFileException(final long file) {
            this.file = file;
        }
    }

    public boolean isErasureEnabled() {
        return enabled;
    }

    /**
     * If enabled has been set to false, wait for it to be set to true again.
     *
     * Throws ShutdownRequestedException but no other internal exceptions.
     * Calling checkContinue is unnecessary because currentFile is null.
     */
    private void waitForEnabled() {
        while (true) {
            if (PollCondition.await(
                pollCheckMs, FOREVER_TIMEOUT_MS, pollMutex,
                () -> {
                    checkShutdown();
                    return enabled;
                })) {
                return;
            }
        }
    }

    /**
     * Wait for the current time to pass the end time.
     */
    private void waitForCycleEnd() {
        completionTime = TimeSupplier.currentTimeMillis();
        logCycleComplete();

        PollCondition.await(
            pollCheckMs, endTime - completionTime, pollMutex,
            () -> {
                checkContinue();
                return false;
            });
    }

    /**
     * Sleep for a time and dream about things that cause files to become
     * unprotected. When we wake up, our dreams may have come true.
     *
     * Throws ShutdownRequestedException but no other internal exceptions.
     * Calling checkContinue is unnecessary because currentFile is null.
     */
    private void waitForUnprotectedFiles(final NoUnprotectedFilesException e) {

        lastProtectedFilesMsg = e.getMessage();
        lastProtectedFilesMsgLevel = e.logLevel;

        PollCondition.await(
            pollCheckMs, NO_UNPROTECTED_FILES_DELAY_MS, pollMutex,
            () -> {
                checkShutdown();
                return false;
            });
    }

    private void storeState() {

        final TupleOutput out = new TupleOutput();

        /*
         * Store both filesRemaining and hardLinkedFiles as filesRemaining so
         * that in case of resumeCycle() we retry erasure of hardLinkedFiles.
         */
        NavigableSet<Long> allPendingfiles =
            Collections.synchronizedNavigableSet(new TreeSet<>());
        allPendingfiles.addAll(filesRemaining);
        allPendingfiles.addAll(hardLinkedFiles);

        out.writeLong(startTime);
        out.writeLong(endTime);
        out.writePackedLong(totalCycleWork);
        storeFileSet(out, filesCompleted);
        storeFileSet(out, allPendingfiles);
        out.writeInt(ERASER_METADATA_VERSION);
        out.writeLong(oldestAllowedObsoleteLSN);

        final DatabaseEntry data = new DatabaseEntry();
        TupleBase.outputToEntry(out, data);

        envImpl.getMetadataStore().put(MetadataStore.KEY_ERASER, data);
    }

    private boolean loadState() {

        final DatabaseEntry data = new DatabaseEntry();

        if (envImpl.getMetadataStore().get(
            MetadataStore.KEY_ERASER, data) == null) {
            return false;
        }

        final TupleInput in = TupleBase.entryToInput(data);

        startTime = in.readLong();
        endTime = in.readLong();
        totalCycleWork = in.readPackedLong();
        filesCompleted = loadFileSet(in);
        filesRemaining = loadFileSet(in);
        hardLinkedFiles =
            Collections.synchronizedNavigableSet(new TreeSet<>());

        if (in.available() > 0) {
            int metadataVersion = in.readInt();
            if (metadataVersion > 0) {
                oldestAllowedObsoleteLSN = in.readLong();
            }
        }

        return true;
    }

    private void storeFileSet(final TupleOutput out,
                              final NavigableSet<Long> set) {

        out.writePackedInt(set.size());
        long priorFile = 0;

        for (final long file : set) {
            out.writePackedLong(file - priorFile);
            priorFile = file;
        }
    }

    private NavigableSet<Long> loadFileSet(final TupleInput in) {

        final int size = in.readPackedInt();

        final NavigableSet<Long> set =
            Collections.synchronizedNavigableSet(new TreeSet<>());

        long file = 0;

        for (int i = 0; i < size; i += 1) {
            file += in.readPackedLong();
            set.add(file);
        }

        return set;
    }

    /** Info about a file pertaining to its ongoing erasure cycle */
    private static class CurrentFileProcessingInfo {
        long firstWriteTime;
        String fullFileName;
        int entriesToErase;
        RandomAccessFile raf;
        int totalEntriesErased;
        long currentFileOffset;
        long prevFileOffset;

        CurrentFileProcessingInfo() {
            this.firstWriteTime = 0;
            this.fullFileName = null;
            this.entriesToErase = 0;
            this.raf = null;
            this.totalEntriesErased = 0;
            this.currentFileOffset = 0;
            this.prevFileOffset = 0;
        }
    }

    /** Cached info about a file to be erased. */
    private static class FileInfo {

        final long creationTime;
        final long length;
        long lastVlsn = INVALID_VLSN;

        FileInfo(final long creationTime, final long length) {
            this.creationTime = creationTime;
            this.length = length;
        }
    }

    private void logCycleInit(final int filesToExamine) {

        LoggerUtils.info(logger, envImpl,
            "ERASER initializing new cycle. Total files: " + filesToExamine);

        callTestEventHook(TestEvent.Type.INIT);
    }

    private void logCycleStart() {

        LoggerUtils.info(logger, envImpl,
            "ERASER new cycle started. " + getCycleStatus());

        callTestEventHook(TestEvent.Type.START);
    }

    private void logCycleComplete() {

        LoggerUtils.info(logger, envImpl,
            "ERASER cycle completed. " + getCycleStatus());

        if (!hardLinkedFiles.isEmpty()) {
            LoggerUtils.warning(logger, envImpl,
                "Some files were not erased because they have " +
                    "hard-links to snapshots. Remove the snapshots to erase " +
                    "them in next cycle." + getHardLinkedFileStatus());
        }

        callTestEventHook(TestEvent.Type.COMPLETE);
    }

    private void logCycleIncomplete() {

        final String msg = "ERASER unable to erase files " +
            "within the erasure period. " +
            ((lastProtectedFilesMsg != null) ?
                lastProtectedFilesMsg :
                "File protection did not prevent erasure, so probably " +
                    "just ran out of time.") +
            " " + getFileProtectionMessage() +
            " " + getCycleStatus();

        LoggerUtils.logMsg(
            logger, envImpl,
            (lastProtectedFilesMsgLevel != null) ?
                lastProtectedFilesMsgLevel : Level.INFO,
            msg);

        callTestEventHook(TestEvent.Type.INCOMPLETE);
    }

    private void logCycleResume() {

        LoggerUtils.info(logger, envImpl,
            "ERASER previously incomplete cycle resumed at startup. " +
                getCycleStatus());

        callTestEventHook(TestEvent.Type.RESUME);
    }

    private void logCycleCannotResume() {

        LoggerUtils.warning(logger, envImpl,
            "ERASER previously incomplete cycle not resumed at startup" +
                " because end time has passed. " + getCycleStatus());

        callTestEventHook(TestEvent.Type.CANNOT_RESUME);
    }

    private void logCycleSuspend() {

        LoggerUtils.info(logger, envImpl,
            "ERASER incomplete cycle suspended at shutdown. " +
                getFileProtectionMessage() + " " + getCycleStatus());

        callTestEventHook(TestEvent.Type.SUSPEND);
    }

    private void logPeriodChanged() {

        LoggerUtils.info(logger, envImpl,
            "ERASER period param was changed, current cycle aborted. " +
                getCycleStatus());

        callTestEventHook(TestEvent.Type.PERIOD_CHANGED);
    }

    private String getCycleStatus() {

        return "Cycle start: " + formatTime(startTime) +
            ", end: " + formatTime(endTime) +
            ", filesCompleted: [" + FormatUtil.asHexString(filesCompleted) +
            "], filesRemaining: [" + FormatUtil.asHexString(
            filesRemaining) +
            "].";
    }

    private String getHardLinkedFileStatus() {
        return "filesRemaining [" + FormatUtil.asHexString(hardLinkedFiles) +
            "].";
    }

    /*
     * Get a permit from the task coordinator before fsyncing the file, then
     * release the permit.
     */
    private void fsync(RandomAccessFile file) throws IOException {
        Consumer<InterruptedException> handler = (e) -> {
            throw new EnvironmentFailureException(envImpl,
                EnvironmentFailureReason.THREAD_INTERRUPTED, e);
        };
        try (Permit permit = envImpl.getTaskCoordinator().acquirePermit(
            JE_DATA_ERASURE_TASK, PERMIT_WAIT_MS, PERMIT_TIME_TO_HOLD_MS,
            TimeUnit.MILLISECONDS, handler)) {
            file.getChannel().force(false);
        }
        fSyncs.incrementAndGet();
    }

    private static String formatTime(final long time) {

        return DATE_FORMAT.format(new Date(time));
    }

    private void callTestEventHook(final TestEvent.Type type) {

        if (testEventHook == null) {
            return;
        }

        testEventHook.doHook(new TestEvent(type, this));
    }

    static void setTestEventHook(final TestHook<TestEvent> hook) {
        testEventHook = hook;
    }

    static class TestEvent {

        enum Type {
            INIT,
            START,
            COMPLETE,
            INCOMPLETE,
            RESUME,
            CANNOT_RESUME,
            SUSPEND,
            PERIOD_CHANGED,
        }

        final Type type;
        final long startTime;
        final long endTime;
        final long completionTime;
        final NavigableSet<Long> filesCompleted;
        final NavigableSet<Long> filesRemaining;

        TestEvent(final Type type,
                  final DataEraser eraser) {

            this.type = type;
            startTime = eraser.startTime;
            endTime = eraser.endTime;
            completionTime = eraser.completionTime;
            filesCompleted = new TreeSet<>(eraser.filesCompleted);
            filesRemaining = new TreeSet<>(eraser.filesRemaining);
        }

        @Override
        public String toString() {
            return type.toString() +
                " startTime=" + formatTime(startTime) +
                " endTime=" + formatTime(endTime) +
                " completionTime=" + formatTime(completionTime) +
                " filesCompleted=[" + FormatUtil.asHexString(filesCompleted) +
                "] filesRemaining=[" + FormatUtil.asHexString(filesRemaining) +
                "]";
        }
    }
}
