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

package com.sleepycat.je.log;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;

/**
 * The DumpFileReader prints every log entry to stdout.
 */
public abstract class DumpFileReader extends FileReader {

    /* A set of the entry type numbers that this DumpFileReader should dump. */
    private final Set<Byte> targetEntryTypes;

    /* A set of the txn ids that this DumpFileReader should dump. */
    private final Set<Long> targetTxnIds;

    /* A set of the db ids that this DumpFileReader should dump. */
    private final Set<Long> targetDbIds;

    /* A set of the table ids that this DumpFileReader should dump. */
    protected final Set<Long> targetTableIds;

    /* A set of the region ids that this DumpFileReader should dump. */
    protected final Set<Long> targetRegionIds;

    protected final boolean printTableId;

    protected final boolean printRegionId;

    protected final Map<Long, String> idToNameMap;

    /* If true, dump the long version of the entry. */
    protected final boolean verbose;

    /* If true, only dump entries that have a VLSN */
    private final boolean repEntriesOnly;

    public static String NO_VALUE = "NO_VALUE";

    /**
     * Create this reader to start at a given LSN.
     */
    public DumpFileReader(EnvironmentImpl env,
                          int readBufferSize,
                          long startLsn,
                          long finishLsn,
                          long endOfFileLsn,
                          String entryTypes,
                          String dbIds,
                          String txnIds,
                          String tbIds,
                          String rgIds,
                          boolean verbose,
                          boolean repEntriesOnly,
                          boolean forwards,
                          boolean doChecksumOnRead)
        throws DatabaseException {

        super(env,
              readBufferSize,
              forwards, 
              startLsn,
              null, // single file number
              endOfFileLsn, // end of file lsn
              finishLsn, // finish lsn
              doChecksumOnRead); // doChecksumOnRead

        /* If entry types is not null, record the set of target entry types. */
        targetEntryTypes = new HashSet<>();
        if (entryTypes != null) {
            StringTokenizer tokenizer = new StringTokenizer(entryTypes, ",");
            while (tokenizer.hasMoreTokens()) {
                String typeString = tokenizer.nextToken();
                targetEntryTypes.add(Byte.valueOf(typeString.trim()));
            }
        }
        /* If db ids is not null, record the set of target db ids. */
        targetDbIds = new HashSet<>();
        if (dbIds != null) {
            StringTokenizer tokenizer = new StringTokenizer(dbIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String dbIdString = tokenizer.nextToken();
                targetDbIds.add(Long.valueOf(dbIdString.trim()));
            }
        }
        /* If txn ids is not null, record the set of target txn ids. */
        targetTxnIds = new HashSet<>();
        if (txnIds != null) {
            StringTokenizer tokenizer = new StringTokenizer(txnIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String txnIdString = tokenizer.nextToken();
                targetTxnIds.add(Long.valueOf(txnIdString.trim()));
            }
        }

        /* If table ids is not null, record the set of target table ids. */
        targetTableIds = new HashSet<>();
        if (tbIds != null && !tbIds.equals(DumpFileReader.NO_VALUE)) {
            StringTokenizer tokenizer = new StringTokenizer(tbIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String tableIdString = tokenizer.nextToken();
                targetTableIds.add(Long.valueOf(tableIdString.trim()));
            }
        }

        /* If region ids is not null, record the set of target region ids. */
        targetRegionIds = new HashSet<>();
        if (rgIds != null && !rgIds.equals(DumpFileReader.NO_VALUE)) {
            StringTokenizer tokenizer = new StringTokenizer(rgIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String regionIdString = tokenizer.nextToken();
                targetRegionIds.add(Long.valueOf(regionIdString.trim()));
            }
        }
        /*
         * printRegionId/printTableId is true as long as
         * rgIds/tbIds != null, even if rgIds/tbIds == DumpFileReader.NO_VALUE,
         * which means -rg/-tb is set thought no specific region/table
         * id is provided, we want to print log entries with tableId/regionId
         * attached
         */
        this.printRegionId = rgIds != null;
        this.printTableId = tbIds != null;
        this.verbose = verbose;
        this.repEntriesOnly = repEntriesOnly;
        if (printRegionId || printTableId) {
            Map<DatabaseId, String> map = env.getDbTree().getDbNamesAndIds();
            idToNameMap = map.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getId(),
                Map.Entry::getValue));
        } else {
            idToNameMap = null;
        }
    }

    protected boolean needMatchEntry() {
        return !targetTxnIds.isEmpty() || !targetDbIds.isEmpty();
    }

    protected boolean matchEntry(LogEntry entry) {
        if (!targetTxnIds.isEmpty()) {
            LogEntryType type = entry.getLogType();
            if (!type.isTransactional()) {
                /* If -tx spec'd and not a transactional entry, don't dump. */
                return false;
            }
            if (!targetTxnIds.contains(entry.getTransactionId())) {
                /* Not in the list of txn ids. */
                return false;
            }
        }
        if (!targetDbIds.isEmpty()) {
            DatabaseId dbId = entry.getDbId();
            if (dbId == null) {
                /* If -db spec'd and not a db entry, don't dump. */
                return false;
            }
            if (!targetDbIds.contains(dbId.getId())) {
                /* Not in the list of db ids. */
                return false;
            }
        }

        if (!targetTableIds.isEmpty()) {
            if (!(entry instanceof LNLogEntry)) {
                return false;
            }

            /*
             * This entry should belong to a user's database.
             */
            Long dbId = entry.getDbId().getId();
            if (!TbRgIdUtil.isUserDatabase(idToNameMap.get(dbId))) {
                return false;
            }

            /*
             * Entries with internal keyspace key are excluded
             */
            if (TbRgIdUtil.keySpaceIsInternal(((LNLogEntry<?>) entry).getKey())) {
                return false;
            }

            long tableId = getTableId(entry);

            /*
             * If this entry doesn't have a tableId,
             * or tableId is not a target one, return false.
             */
            if (tableId == -1 || !targetTableIds.contains(tableId)) {
                return false;
            }
        }

        if (!targetRegionIds.isEmpty()) {
            if (!(entry instanceof LNLogEntry)) {
                return false;
            }

            /*
             * This entry should belong to a user's database.
             */
            Long dbId = entry.getDbId().getId();
            if (!TbRgIdUtil.isUserDatabase(idToNameMap.get(dbId))) {
                return false;
            }

            /*
             * Entries with internal keyspace key are excluded
             */
            if (TbRgIdUtil.keySpaceIsInternal(((LNLogEntry<?>) entry).getKey())) {
                return false;
            }

            long regionId = getRegionId(entry);

            if (regionId == -1 || !targetRegionIds.contains(regionId)) {
                /* Not in the list of region ids. */
                return false;
            }
        }

        return true;
    }

    protected long getTableId(LogEntry entry) {
        byte[] key = ((LNLogEntry<?>)entry).getKey();
        return TbRgIdUtil.getTableId(key);
    }

    protected int getRegionId(LogEntry entry) {
        byte[] data = ((LNLogEntry<?>)entry).getData();
        return data == null ? -1 : TbRgIdUtil.getRegionId(data);
    }

    /**
     * @return true if this reader should process this entry, or just skip over
     * it.
     */
    @Override
    protected boolean isTargetEntry() {
        if (repEntriesOnly && !currentEntryHeader.getReplicated()) {

            /* 
             * Skip this entry; we only want replicated entries, and this
             * one is not replicated.
             */
            return false;
        }

        if (targetEntryTypes.size() == 0) {
            /* We want to dump all entry types. */
            return true;
        }
        return targetEntryTypes.contains
            (Byte.valueOf(currentEntryHeader.getType()));
    }

    /**
     * @param ignore  
     */
    public void summarize(boolean ignore /*csvFile*/) {
    }
}
