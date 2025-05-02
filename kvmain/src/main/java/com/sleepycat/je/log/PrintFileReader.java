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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.LN;

/**
 * The PrintFileReader prints out the target log entries.
 */
public class PrintFileReader extends DumpFileReader {

    private final boolean elideLN;
    private final boolean elideIN;
    private final long startVLSN;
    private final long endVLSN;
    private boolean vlsnInScope = false;
    /**
     * Create this reader to start at a given LSN.
     */
    public PrintFileReader(EnvironmentImpl env,
                           int readBufferSize,
                           long startLsn,
                           long finishLsn,
                           long startVLSN,
                           long endVLSN,
                           boolean elideLN,
                           boolean elideIN,
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
              startLsn,
              finishLsn,
              endOfFileLsn,
              entryTypes,
              dbIds,
              txnIds,
              tbIds,
              rgIds,
              verbose,
              repEntriesOnly,
              forwards,
              doChecksumOnRead);
        this.startVLSN = startVLSN;
        this.endVLSN = endVLSN;
        this.elideLN = elideLN;
        this.elideIN = elideIN;
        if (startVLSN == NULL_VLSN && endVLSN != NULL_VLSN) {
            this.vlsnInScope = true;
        }
    }

    /**
     * This reader prints the log entry item.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        /* Figure out what kind of log entry this is */
        LogEntryType type =
            LogEntryType.findType(currentEntryHeader.getType());

        /* Read the entry. */
        LogEntry entry = type.getSharedLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);

        if ((elideLN && type.isLNType()) || (elideIN && type.isINType())) {
            return true;
        }

        /* Match according to command line args. */
        if (!matchEntry(entry)) {
            return true;
        }

        /* Dump it. */
        StringBuilder sb = new StringBuilder();
        sb.append("<entry lsn=\"0x").append
            (Long.toHexString(window.currentFileNum()));
        sb.append("/0x").append(Long.toHexString(currentEntryOffset));
        sb.append("\" ");
        currentEntryHeader.dumpLogNoTag(sb, verbose);
        sb.append("\">");
        if (printRegionId) {
            if (!targetRegionIds.isEmpty()) {
                int regionId = getRegionId(entry);
                if (regionId != -1) {
                    sb.append("<regionId=").append(regionId).append(">");
                }
            } else if (entry instanceof LNLogEntry) {
                /*
                 * -rg is set but no rgId is specified
                 */
                Long dbId = entry.getDbId().getId();
                if (TbRgIdUtil.isUserDatabase(idToNameMap.get(dbId))) {
                    int regionId = getRegionId(entry);
                    if (regionId != -1) {
                        sb.append("<regionId=").append(regionId).append(">");
                    }
                }
            }
        }
        if (printTableId) {
            if (!targetTableIds.isEmpty()) {
                long tableId = getTableId(entry);
                if (tableId != -1) {
                    sb.append("<tableId=").append(tableId).append(">");
                }
            } else if (entry instanceof LNLogEntry) {
                /*
                 * -tb is set but no tbId is specified
                 */
                Long dbId = entry.getDbId().getId();
                if (TbRgIdUtil.isUserDatabase(idToNameMap.get(dbId))) {
                    long tableId = getTableId(entry);
                    if (tableId > 0) {
                        sb.append("<tableId=").append(tableId).append(">");
                    }
                }
            }
        }
        entry.dumpEntry(sb, verbose);

        /*
         * Format EXTINCT_SCAN_LN as individual fields rather than just a byte
         * array. This aids in debugging.
         *
         * TODO: Generalize this and do it for other LN log entry types that
         * use the LN class for serialization.
         */
        if (type.equals(LogEntryType.LOG_EXTINCT_SCAN_LN_TRANSACTIONAL) ||
            type.equals(LogEntryType.LOG_EXTINCT_SCAN_LN)) {
            final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            final LN ln = lnEntry.getLN();
            envImpl.getExtinctionScanner().dumpLog(
                lnEntry.getKey(), ln.getData(), sb, verbose);
        }

        sb.append("</entry>");
        System.out.println(sb.toString());

        return true;
    }

    /**
     * @return true if this reader should process this entry, or just skip over
     * it.
     */
    @Override
    protected boolean isTargetEntry() {

        if (!super.isTargetEntry()) {
            return false;
        }

        if (startVLSN != NULL_VLSN || endVLSN != NULL_VLSN) {
            /*
             * The following logic only applies to forwards scans. The upper
             * caller must make sure it is forwards scan when non-NULL VLSN
             * border is provided.
             */
            long currentVLSN = currentEntryHeader.getVLSN();
            if (currentVLSN < FIRST_VLSN) {
                if (vlsnInScope) {
                    return true;
                }
                return false;
            }

            if (startVLSN == NULL_VLSN) {
                /*
                 * Case1: startVLSN == NULL_VLSN, endVLSN != NULL_VLSN
                 */
                if (currentVLSN >= endVLSN) {
                    vlsnInScope = false;
                    if (currentVLSN > endVLSN) {
                        return false;
                    }
                }
            } else if (endVLSN == NULL_VLSN) {
                /*
                 * Case2: startVLSN != NULL_VLSN, endVLSN == NULL_VLSN
                 */
                if (currentVLSN < startVLSN) {
                    return false;
                }
                vlsnInScope = true;
            } else {
                /*
                 * Case3: startVLSN != NULL_VLSN, endVLSN != NULL_VLSN
                 */
                if (currentVLSN < startVLSN || currentVLSN > endVLSN) {
                    vlsnInScope = false;
                    return false;
                }

                if (currentVLSN == endVLSN) {
                    vlsnInScope = false;
                } else {
                    vlsnInScope = true;
                }
            }
        }

        return true;
    }
}
