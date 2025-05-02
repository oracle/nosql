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

package com.sleepycat.je.beforeimage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.ReservedDBMap;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.util.CmdLineParser;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;

/**
 * BeforeImageStatsReader scans beforeimage database to output 
 * obsolete and active before image data and it is single threaded.
 */

public class BeforeImageStatsReader {

    public static class StatsReader extends FileReader {

        private List<LogEntryType> targetTypes = new ArrayList<>();
        private LogEntry logEntry;
        private DatabaseId bImgId;

        private long obsoleteSize = 0l;
        private long activeSize = 0l;

        /**
         * Create this reader to start at a given LSN.
         */
        public StatsReader(EnvironmentImpl env, int readBufferSize,
                long startLsn, long finishLsn, DatabaseId bImgId)
            throws DatabaseException {

                super(env, readBufferSize, true, startLsn, null, DbLsn.NULL_LSN,
                        finishLsn);
                this.bImgId = bImgId;
                targetTypes.add(LogEntryType.LOG_INS_LN_TRANSACTIONAL);
                targetTypes.add(LogEntryType.LOG_INS_LN);
            }

        /**
         * @return true if this is a targeted entry.
         */
        @Override
            protected boolean isTargetEntry() {
                return targetTypes.stream()
                    .anyMatch(x -> x.equalsType(currentEntryHeader.getType()));
            }

        /**
         * This reader instantiate the first object of a given log entry.
         */
        protected boolean processEntry(ByteBuffer entryBuffer)
            throws DatabaseException {
                LogEntry entry = targetTypes.get(0).getSharedLogEntry();
                /**
                 * Deserialize as dbid not in header
                 */
                entry.readEntry(envImpl, currentEntryHeader, entryBuffer);
                DatabaseId dbId = entry.getDbId();
                if (!dbId.equals(bImgId)) {
                    return false;
                }
                final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
                if (TTL.isExpired(lnEntry.getExpirationTime())) {
                    obsoleteSize += lnEntry.getDataLength();
                } else {
                    activeSize += lnEntry.getDataLength();
                }
                return true;
            }

        public String printStatsData() {
            return "BeforeImage Active Log Size " + activeSize
                + " ObsoleteSize " + obsoleteSize;
        }

        /**
         * @return the last object read.
         */
        public Object getLastObject() {
            return logEntry.getMainItem();
        }
    }

    public static void main(String[] args) {
        CmdLineParser cmd = new CmdLineParser();
        cmd.addFlag("-verbose", "Enable verbose Mode");
        cmd.addOption("-h", true, "JE Envhome path");
        cmd.addOption("-s", false, "startLsn");
        cmd.addOption("-e", false, "endLsn");
        long startLsn = DbLsn.NULL_LSN;
        long endLsn = DbLsn.NULL_LSN;

        if (!cmd.parse(args)) {
            System.exit(1);
        }
        File envHome = new File(cmd.getOptionValue("-h"));
        EnvironmentImpl env = CmdUtil.makeUtilityEnvironment(envHome, true,
                false);

        int readBufferSize = env.getConfigManager()
            .getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        if (cmd.getOptionValue("-s") != null) {
            startLsn = CmdUtil.readLsn(cmd.getOptionValue("-s"));
        }
        if (cmd.getOptionValue("-e") != null) {
            endLsn = CmdUtil.readLsn(cmd.getOptionValue("-e"));
        }

        if (env.getBeforeImageIndex() == null) {
            System.out.println("No beforeImageIndex Exists. Exiting!!");
            return;
        }
        DatabaseId id = new DatabaseId(
                ReservedDBMap.getValue(DbType.BEFORE_IMAGE.getInternalName()));

        StatsReader sReader = new StatsReader(env, readBufferSize, startLsn,
                endLsn, id);
        while (sReader.readNextEntry()) {

        }
        System.out.println(sReader.printStatsData());
    }
}
