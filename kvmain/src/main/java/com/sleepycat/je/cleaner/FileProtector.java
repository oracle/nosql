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

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.FormatUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The FileProtector is primarily responsible for protecting files from being
 * deleted due to log cleaning, when they are needed for other purposes. As
 * such it is a gatekeeper for reading files. In addition it maintains certain
 * metadata:
 *  - the size of each file, which is needed for calculating disk usage;
 *  - the first and last VLSNs in each reserved file, which are needed for
 *    maintaining the VLSNIndex;
 *  - the total size of active and reserved files, for disk usage statistics.
 *
 * All files are in three categories:
 * + Active: readable by all. The minimum set of files needed to function.
 * + Reserved: should be read only by feeders and low level utilities.
 *   They have been cleaned and will not become active again. They can be
 *   "condemned" and deleted if they are not being read.
 * + Condemned: not readable and effectively invisible. They will be deleted
 *   ASAP. They will not become active or reserved again. A file is typically
 *   in the Condemned category for a very short time, while being deleted.
 *
 * Reserved files can be temporarily protected, i.e., prevented from being
 * condemned and deleted. Only reserved files can be condemned and deleted. All
 * active files are implicitly protected, but are also protected explicitly by
 * consumers because they may become reserved while being consumed.
 *
 * Consumers of the File Protection Service
 * ----------------------------------------
 *
 * + DiskOrderedScanner (DiskOrderedCursor and Database.count)
 *   - Protects all currently active files. By currently active we mean active
 *     at the time they are protected. If they become reserved during the
 *     scan, they must continue to be protected.
 *   - Also protects any new files written during the scan.
 *
 * + DbBackup:
 *   - Protects-and-lists all currently active files, or a subset of the
 *     currently active files in the case of an incremental backup.
 *   - Provides API to remove files from protected set as they are copied, to
 *     allow file deletion.
 *
 * + NetworkRestore:
 *   - Protects-and-lists all currently active files using DbBackup.
 *   - Also protects the two newest reserved files at the time that the active
 *     files are protected.
 *   - Removes files from protected set as they are copied, to allow file
 *     deletion.
 *
 * + Syncup:
 *   - Protects all files (active and reserved) in an open ended range starting
 *     with the file of the VLSNIndex range start. Barren files (with no
 *     replicable entries) are not protected.
 *
 * + Feeder:
 *   - Protects all files (active and reserved) in an open ended range starting
 *     with the file of the current VLSN. Barren files (with no replicable
 *     entries) are not protected.
 *   - Advances lower bound of protected range as VLSN advances, to allow file
 *     deletion.
 *
 * + Cleaner:
 *   - Transforms active files into reserved files after cleaning them.
 *   - Condemns and deletes reserved files to honor disk limits. Truncates head
 *     of VLSNIndex when necessary to stay with disk thresholds.
 *
 * Syncup, Feeders and the VLSNIndex
 * ---------------------------------
 * During syncup, a ProtectedFileRange is used to protect files in the entire
 * range of the VLSNIndex. Syncup must also prevent head truncation of the
 * VLSNIndex itself because the file readers (used by both master and replica)
 * use the VLSNIndex to position in the file at various times.
 *
 * A feeder file reader also protects all files from the current VLSN onward
 * using a ProtectedFileRange. We rely on syncup to initialize the feeder's
 * ProtectedFileRange safely, while the syncup ProtectedFileRange is in effect.
 * The feeder reader will advance the lower bound of its ProtectedFileRange as
 * it reads forward to allow files to be deleted. It also uses the VLNSIndex to
 * skip over gaps in the file, although it is unclear whether this is really
 * necessary.
 *
 * Therefore the syncup and feeder ProtectedFileRanges are special in that
 * they also prevent head truncation of the VLSNIndex.
 *
 * The cleaner truncates the head of the VLSNIndex to allow deletion of files
 * when necessary to stay within disk usage limits. This truncation must not
 * be allowed when a syncup is in progress, and must not be allowed to remove
 * the portion of the VLSN range used by a feeder. This is enforced using a
 * special ProtectedFileRange (vlsnIndexRange) that protects the entire
 * VLSNIndex range. The vlsnIndexRange is advanced when necessary to delete
 * files that it protects, but only if those files are not protected by syncup
 * or feeders. See {@link #checkVLSNIndexTruncation}, {@code
 * com.sleepycat.je.rep.vlsn.VLSNTracker.tryTruncateFromHead} and {@code
 * com.sleepycat.je.rep.vlsn.VLSNTracker.protectRangeHead}.
 *
 * We takes pains to avoid synchronizing on FileProtector while truncating the
 * VLSNIndex head, which is a relatively expensive operation. (The
 * FileProtector is meant to be used by multiple threads without a lot of
 * blocking and should perform only a fairly small amount of work while
 * synchronized.) The following approach is used to truncate the VLSNIndex head
 * safely:
 *
 * -- To prevent disk usage limit violations, Cleaner.manageDiskUsage first
 *    tries to delete reserved files without truncating the VLSNIndex. If this
 *    is not sufficient, it then tries to truncate the VLSNIndex head. If the
 *    VLSNIndex head can be truncated, then it tries again to delete reserved
 *    files, since more files should then be unprotected.
 *
 * -- VLSNTracker synchronization is used to protect the VLSNIndex range. The
 *    vlsnIndexRange ProtectedFileRange is advanced only while synchronized on
 *    the VLSNTracker.
 *
 * -- VLSNTracker.tryTruncateFromHead (which is synchronized) calls
 *    FileProtector.checkVLSNIndexTruncation to determine where to truncate the
 *    index. Reserved files can be removed from the VLSNIndex range only if
 *    they are not protected by syncup and feeders.
 *
 * -- The VLSNTracker range is then truncated, and the vlsnIndexRange is
 *    advanced to allow file deletion, all while synchronized on the tracker.
 *
 * -- When a syncup starts, it adds a ProtectedFileRange with the same
 *    startFile as the vlsnIndexRange. This is done while synchronized on the
 *    VLSNTracker and it prevents the vlsnIndexRange from advancing during the
 *    syncup.
 *
 * -- When a syncup is successful, on the master the Feeder is initialized and
 *    it adds a ProtectedFileRange to protect the range of the VLSNIndex that
 *    it is reading. This is done BEFORE removing the ProtectedFileRange that
 *    was added at the start of the syncup. This guarantees that the files and
 *    VLSNIndex range used by the feeder will not be truncated/deleted.
 *
 * Note that the special vlsnIndexRange ProtectedFileRange is excluded from
 * LogSizeStats to avoid confusion and because this ProtectedFileRange does not
 * ultimately prevent VLSNIndex head truncation or file deletion.
 *
 * Barren Files
 * ------------
 * Files with no replicable entries do not need to be protected by feeders.
 * See {@link #protectFileRange(String, long, boolean, boolean)}. Barren files
 * may be created when cleaning is occurring but app writes are not, for
 * example, when recovering from a cache size configuration error. In this
 * situation it is important to delete the barren files to reclaim disk space.
 *
 * Such "barren" files are identified by having null begin/end VLSNs. The
 * begin/end VLSNs for a file are part of the cleaner metadata that is
 * collected when the cleaner processes a file. These VLSNs are for replicable
 * entries only, not migrated entries that happen to contain a VLSN.
 */
public class FileProtector {

    /* Prefixes for ProtectedFileSet names. */
    public static final String BACKUP_NAME = "Backup";
    public static final String FEEDER_NAME = "Feeder";
    public static final String SYNCUP_NAME = "Syncup";
    public static final String VLSN_INDEX_NAME = "VLSNIndex";
    public static final String NETWORK_RESTORE_NAME = "NetworkRestore";

    /* Suffix for ProtectedFileSet that is only momentarily in effect. */
    public static final String TEMP_SUFFIX = "-temp";

    private static class ReservedFileInfo {

        final long size;
        final long endVLSN;

        ReservedFileInfo(long size, long endVLSN) {
            this.size = size;
            this.endVLSN = endVLSN;
        }
    }

    private final EnvironmentImpl envImpl;

    /* Access active files only via getActiveFiles. */
    private final NavigableMap<Long, Long> activeFiles = new TreeMap<>();

    private final NavigableMap<Long, ReservedFileInfo> reservedFiles =
        new TreeMap<>();

    private volatile HashSet<Long> reservedFileCache = new HashSet<>();

    private final NavigableMap<Long, Long> condemnedFiles = new TreeMap<>();

    private final Map<String, ProtectedFileSet> protectedFileSets =
        new HashMap<>();

    /* Is null if the env is not replicated. */
    private ProtectedFileRange vlsnIndexRange;

    FileProtector(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    private void addFileProtection(ProtectedFileSet pfs) {

        if (protectedFileSets.putIfAbsent(pfs.getName(), pfs) != null) {

            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSets already present name=" + pfs.getName());
        }
    }

    /**
     * Removes protection by the given ProtectedFileSet to allow files to be
     * deleted.
     */
    public synchronized void removeFileProtection(ProtectedFileSet pfs) {

        final ProtectedFileSet oldPfs =
            protectedFileSets.remove(pfs.getName());

        if (oldPfs == null) {
            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSet not found name=" + pfs.getName());
        }

        if (oldPfs != pfs) {
            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSet mismatch name=" + pfs.getName());
        }
    }

    /**
     * Calls {@link #protectFileRange(String, long, boolean, boolean)} passing
     * false for protectVlsnIndex and true for protectBarrenFiles.
     */
    public synchronized ProtectedFileRange protectFileRange(
        final String name,
        final long rangeStart) {

        return protectFileRange(name, rangeStart, false, true);
    }

    /**
     * Returns a ProtectedFileRange that protects files with numbers GTE a
     * lower bound. The upper bound is open ended. The protectVlsnIndex param
     * should be true for feeder/syncup file protection only.
     *
     * @param rangeStart is the first file to be protected in the range.
     *
     * @param protectVlsnIndex is whether to prevent the VLSNIndex head from
     * advancing.
     *
     * @param protectBarrenFiles is whether barren files (having no replicable
     * entries) are protected.
     */
    public synchronized ProtectedFileRange protectFileRange(
        final String name,
        final long rangeStart,
        final boolean protectVlsnIndex,
        final boolean protectBarrenFiles) {

        final ProtectedFileRange fileRange = new ProtectedFileRange(
            name, rangeStart, protectVlsnIndex, protectBarrenFiles);

        addFileProtection(fileRange);
        return fileRange;
    }

    /**
     * Returns a ProtectedActiveFileSet that protects all files currently
     * active at the time of construction. These files are protected even if
     * they later become reserved. Note that this does not include the last
     * file at the time of construction. Additional files can also be
     * protected -- see params.
     *
     * @param nReservedFiles if greater than zero, this number of reserved
     * files are also protected. The most recent (highest numbered) reserved
     * files that contain VLSNs are selected. If there are less than
     * nReservedFiles of these, then the most recent files without VLSNs
     * (barren files) are added.
     *
     * @param protectNewFiles if true, the last file and any new files created
     * later are also protected.
     */
    public synchronized ProtectedActiveFileSet protectActiveFiles(
        final String name,
        final int nReservedFiles,
        final boolean protectNewFiles,
        long matchpointFile) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        /*
         * The matchpoint may be in the most recent file that is not part of
         * the backup set.
         */
        if (!activeFiles.isEmpty() && activeFiles.lastKey() < matchpointFile) {
            matchpointFile = DbLsn.NULL_LSN;
        }

        final NavigableSet<Long> protectedFiles =
            new TreeSet<>(activeFiles.keySet());
        final NavigableSet<Long> barrenFiles =
                new TreeSet<>();

        if (nReservedFiles > 0) {
            int n = nReservedFiles;
            for (Long file : reservedFiles.descendingKeySet()) {
                if (reservedFiles.get(file).endVLSN > 0) {
                    protectedFiles.add(file);
                    n -= 1;
                    if (n <= 0) {
                        break;
                    }
                } else if (barrenFiles.size() < nReservedFiles) {
                    barrenFiles.add(file);
                }
            }
            if (n > 0) {
                for (Long bFile : barrenFiles.descendingSet()) {
                    protectedFiles.add(bFile);
                    n -= 1;
                    if (n <= 0) {
                        break;
                    }
                }
            }
        }

        if (matchpointFile != DbLsn.NULL_LSN &&
            !protectedFiles.contains(matchpointFile)) {
            for (Long file : reservedFiles.descendingKeySet()) {
                protectedFiles.add(file);
                if (file <= matchpointFile) {
                    break;
                }
            }
        }
        final Long rangeStart = protectNewFiles ?
            (protectedFiles.isEmpty() ? 0 : (protectedFiles.last() + 1)) :
            null;

        final ProtectedActiveFileSet pfs =
            new ProtectedActiveFileSet(name, protectedFiles, rangeStart);

        addFileProtection(pfs);
        return pfs;
    }

    /**
     * Freshens and returns the active files. The last file is not included
     * because its length is still changing.
     *
     * Gets new file info lazily to prevent synchronization and work in the
     * CRUD code path when a new file is added.
     */
    private synchronized NavigableMap<Long, Long> getActiveFiles() {

        final FileManager fileManager = envImpl.getFileManager();

        /*
         * Add all existing files when the env is first opened (except for the
         * last file -- see below). This is a relatively expensive but one-time
         * initialization.
         */
        if (activeFiles.isEmpty()) {

            final Long[] files = fileManager.getAllFileNumbers();

            for (int i = 0; i < files.length - 1; i++) {
                final long file = files[i];

                final File fileObj =
                    new File(fileManager.getFullFileName(file));

                activeFiles.put(file, fileObj.length());
            }

            if (activeFiles.isEmpty()) {
                return activeFiles;
            }
        }

        /*
         * Add new files that have appeared. This is very quick, because no
         * synchronization is required to get the last file number. Do not
         * add the last file, since its length may still be changing.
         */
        final long lastFile = DbLsn.getFileNumber(fileManager.getNextLsn());
        final long firstNewFile = activeFiles.lastKey() + 1;

        for (long file = firstNewFile; file < lastFile; file += 1) {

            final File fileObj =
                new File(fileManager.getFullFileName(file));

            /* New files should be active before being reserved and deleted. */
            if (!fileObj.exists() && !envImpl.isMemOnly()) {
                throw EnvironmentFailureException.unexpectedState(
                    "File 0x" + Long.toHexString(file) +
                    " lastFile=" + Long.toHexString(lastFile));
            }

            activeFiles.put(file, fileObj.length());
        }

        return activeFiles;
    }

    /**
     * Moves a file from active status to reserved status.
     */
    synchronized void reserveFile(Long file, long endVLSN) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        final Long size = activeFiles.remove(file);

        if (size == null) {
            throw EnvironmentFailureException.unexpectedState(
                "Only active files (not the last file) may be" +
                    " cleaned/reserved file=0x" + Long.toHexString(file) +
                    " exists=" + envImpl.getFileManager().isFileValid(file) +
                    " reserved=" + reservedFiles.containsKey(file) +
                    " nextLsn=" + DbLsn.getNoFormatString(
                        envImpl.getFileManager().getNextLsn()));
        }

        final ReservedFileInfo info = new ReservedFileInfo(size, endVLSN);
        final ReservedFileInfo prevInfo = putReservedFile(file, info);
        assert prevInfo == null;
    }

    /**
     * Changes a file's state from reserved to active.
     */
    synchronized void reactivateReservedFile(Long file, long size) {
        removeReservedFile(file);
        getActiveFiles().put(file, size);
    }

    /**
     * Returns a set of all files except for the last file.
     */
    synchronized NavigableSet<Long> getAllCompletedFiles() {

        final NavigableSet<Long> set =
            new TreeSet<>(getActiveFiles().keySet());

        set.addAll(reservedFiles.keySet());
        set.addAll(condemnedFiles.keySet());

        return set;
    }

    /**
     * Returns the number of active files, including the last file.
     */
    synchronized int getNActiveFiles() {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        int count = activeFiles.size();

        if (activeFiles.isEmpty() ||
            activeFiles.lastKey() <
                envImpl.getFileManager().getCurrentFileNum()) {
            count += 1;
        }

        return count;
    }

    /**
     * Returns the number of reserved files.
     */
    synchronized int getNReservedFiles() {
        return reservedFiles.size();
    }

    /**
     * Only used in testing.
     */
    public synchronized long getProtectedSize() {
        return getLogSizeStats().protectedSize;
    }

    /**
     * Returns a copy of the reserved files along with the total size.
     */
    public synchronized Pair<Long, NavigableSet<Long>> getReservedFileInfo() {
        long size = 0;
        for (final ReservedFileInfo info : reservedFiles.values()) {
            size += info.size;
        }
        return new Pair<>(size, new TreeSet<>(reservedFiles.keySet()));
    }

    /**
     * Returns whether the given file is active, including the last file
     * whether or not it has been created on disk yet. If false is returned,
     * the file is reserved, condemned or deleted.
     */
    public synchronized boolean isActiveOrNewFile(Long file) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        return activeFiles.isEmpty() ||
            file > activeFiles.lastKey() ||
            activeFiles.containsKey(file);
    }

    /**
     * Returns whether the given file is in the reserved file set.
     * <p>
     * Optimized to use an unsynchronized HashMap lookup, providing fast
     * checking for invalid references to reserved files. The reservedFileCache
     * map is copied from reservedFiles each time that reservedFiles changes.
     * We expect changes to the reservedFiles set to be infrequent and error
     * checks to be very frequent, plus the error checks are in the CRUD path.
     */
    public boolean isReservedFile(Long file) {
        return reservedFileCache.contains(file);
    }

    /**
     * If the given file is a reserved file, returns its last VLSN which
     * indicates the file is barren when VLSN.isNull. Returns null if the
     * file is not reserved.
     */
    synchronized long getReservedFileLastVLSN(Long file) {
        final ReservedFileInfo info = reservedFiles.get(file);
        return (info == null) ? INVALID_VLSN : info.endVLSN;
    }

    /**
     * Bottleneck for reservedFiles.put to ensure that reservedFileCache is
     * updated.
     */
    private ReservedFileInfo putReservedFile(final Long file,
                                             final ReservedFileInfo info) {
        assert Thread.holdsLock(this);
        final ReservedFileInfo oldVal = reservedFiles.put(file, info);
        reservedFileCache = new HashSet<>(reservedFiles.keySet());
        return oldVal;
    }

    /**
     * Bottleneck for reservedFiles.remove to ensure that reservedFileCache is
     * updated.
     */
    private ReservedFileInfo removeReservedFile(final Long file) {
        assert Thread.holdsLock(this);
        final ReservedFileInfo oldVal = reservedFiles.remove(file);
        reservedFileCache = new HashSet<>(reservedFiles.keySet());
        return oldVal;
    }

    /**
     * Returns a previously condemned file or the oldest unprotected reserved
     * file. If non-null is returned the file is removed from the
     * FileProtector's data structures and is effectively condemned, so if it
     * cannot be deleted by the caller then {@link #putBackCondemnedFile}
     * should be called so the file deletion can be retried later.
     *
     * @param fromFile the lowest file number to return. Used to iterate over
     * reserved files that are protected.
     *
     * @return {file, size} pair or null if a condemned file is not available.
     */
    synchronized Pair<Long, Long> takeNextCondemnedFile(long fromFile) {

        if (!condemnedFiles.isEmpty()) {
            final Long file = condemnedFiles.firstKey();
            final Long size = condemnedFiles.remove(file);
            return new Pair<>(file, size);
        }

        if (reservedFiles.isEmpty()) {
            return null;
        }

        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.tailMap(fromFile).entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();

            if (isProtected(file, info)) {
                continue;
            }

            removeReservedFile(file);
            return new Pair<>(file, info.size);
        }

        return null;
    }

    /**
     * If the given file was previously condemned, or is reserved and
     * unprotected, this method removes it from the FileProtector's data
     * structures and returns its size. If non-null is returned the file is
     * effectively condemned, so if it cannot be deleted by the caller then
     * {@link #putBackCondemnedFile} should be called so the file deletion
     * can be retried later.
     *
     * @return the size, or null if the file is not condemned and is not
     * reserved and unprotected.
     */
    synchronized Long takeCondemnedFile(Long file) {

        final Long condemnedSize = condemnedFiles.remove(file);

        if (condemnedSize != null) {
            return condemnedSize;
        }

        final ReservedFileInfo info = reservedFiles.get(file);

        if (info != null && !isProtected(file, info)) {
            removeReservedFile(file);
            return info.size;
        }

        return null;
    }

    /**
     * Returns whether the given file is protected.
     */
    private synchronized boolean isProtected(final Long file,
                                             final ReservedFileInfo info) {

        for (final ProtectedFileSet pfs : protectedFileSets.values()) {
            if (pfs.isProtected(file, info)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Puts back a condemned file after a file returned by {@link
     * #takeNextCondemnedFile}, or passed to {@link #takeCondemnedFile}, could
     * not be deleted.
     */
    synchronized void putBackCondemnedFile(Long file, Long size) {
        final Long oldSize = condemnedFiles.put(file, size);
        assert oldSize == null;
    }

    /**
     * Returns the lowest-valued file number in the given set that is not
     * protected.
     */
    synchronized Long getFirstUnprotectedFile(NavigableSet<Long> files) {

        for (final Long file : files) {
            if (!isProtected(file, reservedFiles.get(file))) {
                return file;
            }
        }

        return null;
    }

    /**
     * Intended for testing.
     */
    public synchronized void addActiveFiles(Set<Long> set) {
        set.addAll(getActiveFiles().keySet());
    }

    /**
     * For the given files, returns map of file protector name to files
     * protected.
     */
    synchronized NavigableMap<String, String> getProtectedFileMap(
        final SortedSet<Long> files) {

        final Map<String, SortedSet<Long>> map = new HashMap<>();

        for (final Long file : files) {
            final ReservedFileInfo info = reservedFiles.get(file);

            for (final ProtectedFileSet pfs : protectedFileSets.values()) {

                if (!pfs.isProtected(file, info)) {
                    continue;
                }

                SortedSet<Long> set = map.get(pfs.getName());
                if (set == null) {
                    set = new TreeSet<>();
                }
                set.add(file);

                map.put(pfs.getName(), set);
            }
        }

        final NavigableMap<String, String> mapResult = new TreeMap<>();

        for (final Map.Entry<String, SortedSet<Long>> entry : map.entrySet()) {
            mapResult.put(
                entry.getKey(), FormatUtil.asHexString(entry.getValue()));
        }

        return mapResult;
    }

    static class LogSizeStats {
        final long activeSize;
        final long reservedSize;
        final long protectedSize;
        final Map<String, Long> protectedSizeMap;

        LogSizeStats(final long activeSize,
                     final long reservedSize,
                     final long protectedSize,
                     final Map<String, Long> protectedSizeMap) {
            this.activeSize = activeSize;
            this.reservedSize = reservedSize;
            this.protectedSize = protectedSize;
            this.protectedSizeMap = protectedSizeMap;
        }
    }

    /**
     * Returns sizes occupied by active, reserved and protected files.
     */
    synchronized LogSizeStats getLogSizeStats() {

        /* Calculate active size. */
        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        long activeSize = 0;

        for (final long size : activeFiles.values()) {
            activeSize += size;
        }

        /* Add size of last file, which is not included in activeFiles. */
        final long lastFileNum = activeFiles.isEmpty() ?
            0 : activeFiles.lastKey() + 1;

        final File lastFile = new File(
            envImpl.getFileManager().getFullFileName(lastFileNum));

        if (lastFile.exists()) {
            activeSize += lastFile.length();
        }

        /* Calculate reserved and protected sizes. */
        long reservedSize = 0;
        long protectedSize = 0;
        final Map<String, Long> protectedSizeMap = new HashMap<>();

        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();
            reservedSize += info.size;
            boolean isProtected = false;

            for (final ProtectedFileSet pfs : protectedFileSets.values()) {

                if (pfs == vlsnIndexRange || !pfs.isProtected(file, info)) {
                    continue;
                }

                isProtected = true;

                protectedSizeMap.compute(
                    pfs.getName(),
                    (k, v) -> ((v != null) ? v : 0) + info.size);
            }

            if (isProtected) {
                protectedSize += info.size;
            }
        }

        return new LogSizeStats(
            activeSize, reservedSize, protectedSize, protectedSizeMap);
    }

    /**
     * Sets the ProtectedFileRange that protects files in VLSNIndex range
     * from being deleted. The range start is changed during VLSNIndex
     * initialization and when the head of the index is truncated. It is
     * changed while synchronized on VLSNTracker so that changes to the
     * range and changes to the files it protects are made atomically. This
     * is important for
     * {@code com.sleepycat.je.rep.vlsn.VLSNTracker.protectRangeHead}.
     */
    public void setVLSNIndexProtectedFileRange(ProtectedFileRange pfs) {
        vlsnIndexRange = pfs;
    }

    /**
     * Determines whether the VLSNIndex ProtectedFileRange should be advanced
     * to reclaim bytesNeeded. This is possible if one or more reserved files
     * are not protected by syncup and feeders. The range of files to be
     * truncated must be at the head of the ordered set of reserved files, and
     * the highest numbered file must contain a VLSN so we know where to
     * truncate the VLSNIndex.
     *
     * @param bytesNeeded the number of bytes we need to free.
     *
     * @param preserveVLSN is the boundary above which the VLSN range may not
     * advance. The deleteEnd returned will be less than preserveVLSN.
     *
     * @return {deleteEnd, deleteFileNum} pair if the protected file range
     * should be advanced, or null if advancing is not currently possible.
     *  -- deleteEnd is the last VLSN to be truncated.
     *  -- deleteFileNum the file having deleteEnd as its last VLSN.
     */
    public synchronized Pair<Long, Long> checkVLSNIndexTruncation(
        final long bytesNeeded,
        final long preserveVLSN) {

        /*
         * Determine how many reserved files we need to delete, and find the
         * last file/VLSN in that set of files, which is the truncation point.
         */
        long truncateVLSN = NULL_VLSN;
        long truncateFile = -1;
        long deleteBytes = 0;

        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();

            if (isVLSNIndexProtected(file, info)) {
                break;
            }

            final long lastVlsn = info.endVLSN;

            if (!VLSN.isNull(lastVlsn)) {
                if (lastVlsn > preserveVLSN) {
                    break;
                }
                truncateVLSN = lastVlsn;
                truncateFile = file;
            }

            deleteBytes += info.size;

            if (deleteBytes >= bytesNeeded) {
                break;
            }
        }

        return VLSN.isNull(truncateVLSN) ? null :
            new Pair<>(truncateVLSN, truncateFile);
    }

    /**
     * Determines whether the VLSNIndex ProtectedFileRange should be advanced
     * to unprotect the given file. This is possible if the file is not
     * protected by syncup and feeders. The given file is assumed to contain
     * deleteEnd VLSN, i.e., it is not a barren file.
     *
     * @param file that should be unprotected.
     *
     * @return whether the protected file range should be advanced.
     */
    public boolean checkVLSNIndexTruncation(final Long file) {
        return !isVLSNIndexProtected(file, null);
    }

    /**
     * Returns whether the VLSNIndex is protected for the given file. Because
     * the VLSNIndex is always protected for all files in a range at the tail
     * of the log, returning false implies that the VLSNIndex is unprotected
     * for all files less than or equal to the given file.
     */
    private synchronized boolean isVLSNIndexProtected(
        final Long file,
        final ReservedFileInfo info) {

        for (final ProtectedFileSet pfs : protectedFileSets.values()) {

            if (pfs == vlsnIndexRange || !pfs.protectVlsnIndex) {
                continue;
            }

            if (pfs.isProtected(file, info)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the first file covered by the VLSNIndex, or DbLsn.MAX_FILE_NUM
     * in a non-replicated environment.
     */
    long getVLSNIndexStartFile() {
        return (vlsnIndexRange != null) ?
            vlsnIndexRange.getRangeStart() : DbLsn.MAX_FILE_NUM;
    }

    /**
     * A ProtectedFileSet is used to prevent a set of files from being deleted.
     * Implementations must meet two special requirements:
     *
     * 1. After a ProtectedFileSet is added using {@link #addFileProtection},
     * its set of protected files (the set for which {@link #isProtected}
     * returns true) may only be changed by shrinking the set. Files may not be
     * added to the set of protected files. (One exception is that newly
     * created files are effectively to a file set defined as an opened ended
     * range.)
     *
     * 2. Shrinking the protected set can be done without synchronization on
     * FileProtector. However, implementations should ensure that changes made
     * in one thread are visible to all threads.
     *
     * The intention is to allow protecting a set of files that are to be
     * processed in some way, and allow easily shrinking this set as the files
     * are processed, so that the processed files may be deleted. Changes to
     * the protected set should be visible to all threads so that periodic disk
     * space reclamation tasks can delete unprotected files ASAP. {@link
     * ProtectedFileRange} is a simple class that meets these requirements.
     */
    public static abstract class ProtectedFileSet {

        private final String name;
        private final boolean protectVlsnIndex;

        private ProtectedFileSet(final String name,
                                 final boolean protectVlsnIndex) {
            this.name = name;
            this.protectVlsnIndex = protectVlsnIndex;
        }

        /**
         * Identifies protecting entity, used in LogSizeStats. Must be unique
         * across all file sets added to the FileProtector.
         */
        private String getName() {
            return name;
        }

        /**
         * Whether the given file is protected.
         *
         * @param info is null for non-reserved files.
         */
        abstract boolean isProtected(Long file,
                                     @Nullable ReservedFileInfo info);

        @Override
        public String toString() {
            return "ProtectedFileSet:" + name;
        }
    }

    /**
     * A ProtectedFileSet created using {@link #protectFileRange}.
     *
     * Protection may be removed dynamically to allow file deletion using
     * {@link #advanceRange}. The current lower bound can be obtained using
     * {@link #getRangeStart()}.
     */
    public static class ProtectedFileRange extends ProtectedFileSet {

        private volatile long rangeStart;
        private final boolean protectBarrenFiles;

        ProtectedFileRange(
            final String name,
            final long rangeStart,
            final boolean protectVlsnIndex,
            final boolean protectBarrenFiles) {

            super(name, protectVlsnIndex);
            this.rangeStart = rangeStart;
            this.protectBarrenFiles = protectBarrenFiles;
        }

        @Override
        boolean isProtected(final Long file,
                            final ReservedFileInfo info) {

            return file >= rangeStart &&
                (protectBarrenFiles ||
                 info == null ||
                 !VLSN.isNull(info.endVLSN));
        }

        /**
         * Returns the current rangeStart. This method is not synchronized and
         * rangeStart is volatile to allow checking this value without
         * blocking.
         */
        public long getRangeStart() {
            return rangeStart;
        }

        /**
         * Moves the lower bound of the protected file range forward. Used to
         * allow file deletion as protected files are processed.
         */
        public synchronized void advanceRange(final long rangeStart) {

            if (rangeStart < this.rangeStart) {
                throw EnvironmentFailureException.unexpectedState(
                    "Attempted to advance to a new rangeStart=0x" +
                        Long.toHexString(rangeStart) +
                        " that precedes the old rangeStart=0x" +
                        Long.toHexString(this.rangeStart));
            }

            this.rangeStart = rangeStart;
        }
    }

    /**
     * A ProtectedFileSet created using {@link #protectActiveFiles}.
     *
     * Protection may be removed dynamically to allow file deletion using
     * {@link #truncateHead(long)}, {@link #truncateTail(long)} and
     * {@link #removeFile(Long)}. A copy of the currently protected files can
     * be obtained using {@link #getProtectedFiles()}.
     */
    public static class ProtectedActiveFileSet extends ProtectedFileSet {

        private NavigableSet<Long> protectedFiles;
        private Long rangeStart;

        ProtectedActiveFileSet(
            final String name,
            final NavigableSet<Long> protectedFiles,
            final Long rangeStart) {

            super(name, false /*protectVlsnIndex*/);
            this.protectedFiles = protectedFiles;
            this.rangeStart = rangeStart;
        }

        /**
         * Called by {@link DbBackup#startBackup} to add files created prior to
         * determining the last file of the backup. The last file of the backup
         * is passed to this method. Files from the last previously protected
         * file to the given file will be protected and will be part of the
         * backup set.
         * <p>
         * This is necessary to compensate for two conflicting factors when
         * starting a backup:
         *
         * 1. Active files must be protected (this object must be created)
         * prior to the file flip. The file completed by the file flip becomes
         * the last file in the backup. (If we were to wait until after the file
         * flip to protect active files, some files that were active may have
         * become reserved. This could happen if a checkpoint end occurs after
         * the file flip and before files are protected. See [KVSTORE-494].)
         *
         * 2. New files that were created after the initial file protection and
         * up to the file flip must also be protected and included in the
         * backup. This method is called to accomplish that.
         * <p>
         * WARNING: adding files to a ProtectedFileSet after it is constructed
         * is normally unsafe, because these files will not be protected for
         * the lifetime of the ProtectedFileSet. This exception is allowed
         * because startBackup protects all files while constructing this
         * object and calling this method. This method should not be called in
         * other cases.
         */
        public void addFinalBackupFiles(final long lastFile) {

            final long firstFile =
                protectedFiles.isEmpty() ? 0 : (protectedFiles.last() + 1);

            for (long file = firstFile; file <= lastFile; file++) {
                protectedFiles.add(file);
            }
        }

        @Override
        synchronized boolean isProtected(final Long file,
                                         final ReservedFileInfo info) {

            return (rangeStart != null && file >= rangeStart) ||
                protectedFiles.contains(file);
        }

        /**
         * Returns a copy of the currently protected files, not including any
         * new files.
         */
        public synchronized NavigableSet<Long> getProtectedFiles() {
            return new TreeSet<>(protectedFiles);
        }

        /**
         * Removes protection for files GT lastProtectedFile. Protection of
         * new files is not impacted.
         */
        public synchronized void truncateTail(long lastProtectedFile) {
            protectedFiles = protectedFiles.headSet(lastProtectedFile, true);
        }

        /**
         * Removes protection for files LT firstProtectedFile. Protection of
         * new files is not impacted.
         */
        public synchronized void truncateHead(long firstProtectedFile) {
            protectedFiles = protectedFiles.tailSet(firstProtectedFile, true);
        }

        /**
         * Removes protection for a given file.
         */
        public synchronized void removeFile(final Long file) {

            protectedFiles.remove(file);

            /*
             * This only works if protected files are removed in sequence, but
             * that's good enough -- new files will rarely need to be deleted.
             */
            if (file.equals(rangeStart)) {
                rangeStart += 1;
            }
        }
    }

    /**
     * For debugging.
     */
    @SuppressWarnings("unused")
    synchronized void verifyFileSizes() {
        final FileManager fm = envImpl.getFileManager();
        final Long[] numArray = fm.getAllFileNumbers();
        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        for (int i = 0; i < numArray.length - 1; i++) {
            final Long n = numArray[i];
            final long trueSize = new File(fm.getFullFileName(n)).length();
            if (activeFiles.containsKey(n)) {
                final long activeSize = activeFiles.get(n);
                if (activeSize != trueSize) {
                    System.out.format(
                        "active file %,d size %,d but true size %,d %n",
                        n, activeSize, trueSize);
                }
            } else if (reservedFiles.containsKey(n)) {
                final long reservedSize = reservedFiles.get(n).size;
                if (reservedSize != trueSize) {
                    System.out.format(
                        "reserved file %,d size %,d but true size %,d %n",
                        n, reservedSize, trueSize);
                }
            } else {
                System.out.format(
                    "true file %x size %,d missing in FileProtector%n",
                    n, trueSize);
            }
        }
    }
}
