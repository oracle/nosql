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

package oracle.kv.impl.util.server;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.util.FileUtils;

import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.rep.SyncupProgress;
import com.sleepycat.je.rep.LogFileRewriteListener;

/**
 * Utility implementation of classes JE Logging and ProgressListener hooks.
 * These hooks are used by both admin and rep components in the system.
 */
public class JENotifyHooks {

    /**
     * A custom Handler for use with the JE environment so that JE logging
     * messages flow into the Oracle NoSQL DB monitoring system.
     */
    public static class RedirectHandler extends Handler {

        /* PREFIX is package-accessible to allow access by unit tests */
        static final String PREFIX = "JE: ";

        private final Logger logger;

        public RedirectHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void publish(LogRecord record) {
            logger.log(record.getLevel(), PREFIX + record.getMessage());
        }

        @Override
        public void flush() {
            /* Nothing to do */
        }

        @Override
        public void close() throws SecurityException {
            /* Nothing to do */
        }
    }

    /**
     * Monitor JE environment recovery progress through KVStore.
     */
    public static class RecoveryListener
        implements ProgressListener<RecoveryProgress> {

        /* PREFIX is package-accessible to allow access by unit tests */
        static final String PREFIX = "JE recovery: ";

        private final Logger logger;

        public RecoveryListener(Logger logger) {
            this.logger = logger;
        }

        @Override
        public boolean progress(RecoveryProgress phase, long n, long total) {
            if (n == -1) {
                logger.log(Level.INFO, PREFIX + "{0}", phase);
            } else {
                logger.log(Level.INFO, PREFIX + "{0} {1}/{2}",
                           new Object[] { phase, n, total });
            }

            return true;
        }
    }

    /**
     * Monitor JE environment syncup progress through KVStore
     */
    public static class SyncupListener
        implements ProgressListener<SyncupProgress> {

        private static final String PREFIX = "JE syncup: ";

        private final Logger logger;

        public SyncupListener(Logger logger) {
            this.logger = logger;
        }

        @Override
        public boolean progress(SyncupProgress phase, long n, long total) {
            if (n == 0) {
                logger.log(Level.INFO, PREFIX + "{0}", phase);
            }

            return true;
        }
    }

    /**
     * A callback to be notified when recovery on an active environment is
     * about to modify a log file.  This is a rare, if ever event, but can
     * happen.  In this case make a real copy of the target file name in all
     * snapshots it occurs (most likely only one).
     * <p>
     * The caller is responsible for determining the snapshot directory since
     * this method is going to be run in the context of the managed service and
     * not the SNA.
     * <p>
     * Note: if a file is part of multiple snapshots, this code will create a
     * new copy of it for each one.  (More ideal would be to create just one
     * copy, and then create new hard links to in in the other snapshots.  But
     * figuring out how to do that would be difficult, and at least as
     * platform-dependent as the code in
     # {@link oracle.kv.impl.sna.StorageNodeAgent#makeLinks}.)  Furthermore, if
     * a log file that is part of a snapshot is modified in two separate
     * rollback events (two separate master sync-ups at different times), then
     * the second time we will be making a (completely unnecessary) copy of the
     * copy.  However, both of these scenarios are exceedingly unlikely.
     */
    public static class LogRewriteListener implements LogFileRewriteListener {
        final private File snapshotDir;
        final private Logger logger;

        public LogRewriteListener(File dir, Logger logger) {
            snapshotDir = dir;
            this.logger = logger;
        }

        @Override
        public void rewriteLogFiles(Set<File> files) {

            /* If no snapshots exist, there is nothing to do */
            if (!snapshotDir.exists()) {
                return;
            }
            try {
                for (File file : files) {
                    logFileModifyTrigger(snapshotDir, file.getName());
                }
            } catch (IOException ie) {
                String msg =
                    "JE/HA sync-up roll-back needs to modify " +
                    "an existing log file that is part of a Snapshot, " +
                    "but copying that file failed";
                logger.severe(msg);

                /*
                 * Besides logging a message for the KV administrator, throw an
                 * exception to stop JE in its tracks, in order to prevent
                 * overwriting the log file (in case the Snapshot is
                 * precious).  An alternative here might have been to allow the
                 * overwriting to proceed, presumably spoiling the Snapshot; but
                 * it seems unlikely that JE would be able to get much farther
                 * anyway, if the disk/file system is in trouble.
                 */
                throw new RuntimeException(msg, ie);
            }
        }
        /**
         * Makes a real copy of a log file from a backup snapshot.  (Originally
         * all snapshots are formed simply by making hard links.)
         * <p>
         * The algorithm is recursive.  The snapshot directory tree is not deep.
         */
        private void logFileModifyTrigger(File snapshotDir1, String name)
            throws IOException {

            for (File file : snapshotDir1.listFiles()) {
                if (file.isDirectory()) {
                    logFileModifyTrigger(file, name);
                }
                if (file.getName().equals(name)) {
                    /**
                     * a hit -- copy the file.
                     */
                    File tempName = new File(snapshotDir1, name + ".temp");
                    FileUtils.copyFile(file, tempName);
                    tempName.renameTo(file);
                }
            }
        }
    }
}
