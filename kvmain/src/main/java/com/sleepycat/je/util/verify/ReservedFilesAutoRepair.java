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
package com.sleepycat.je.util.verify;

import static com.sleepycat.je.config.EnvironmentParams.VERIFY_BTREE_BATCH_DELAY;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_BTREE_BATCH_SIZE;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * See {@link EnvironmentParams#AUTO_RESERVED_FILE_REPAIR}.
 */
public class ReservedFilesAutoRepair extends StoppableThread {

    /* Is non-final so it can be set by tests. */
    public static int START_DELAY_S = 90;

    private final BtreeVerifier verifier;
    private boolean enabled;
    private boolean noThrottle;
    private boolean initialized;
    private volatile boolean running;
    private volatile boolean completed;

    public ReservedFilesAutoRepair(final EnvironmentImpl envImpl) {
        super(envImpl, "RepairReservedFiles-" + envImpl.getName());
        verifier = new BtreeVerifier(envImpl);
    }

    /**
     * Returns true if this thread will perform auto-repair, or false if the
     * background DataVerifier should be configured as usual.
     *
     * <p>If true is returned, the background DataVerifier will be configured
     * as usual by this thread after the auto-repair is complete.</p>
     *
     * <p>This method's implementation is fairly simple due to the
     * following:
     * <ul>
     *     <li>The caller, EnvironmentImpl.doSetMutableConfig, is
     *     synchronized.</li>
     *
     *     <li>The first call, when 'initialized' is false, is made before
     *     the Environment ctor returns.</li>
     *
     *     <li>The repair param is immutable.</li>
     * </ul>
     *
     * @throws IllegalArgumentException if the repair param is invalid.
     */
    public boolean startOrCheck(final DbConfigManager configMgr) {

        if (initialized) {
            /*
             * We were previously initialized and now we're being called via
             * Environment.setMutableConfig. If 'running' is false then either
             * repair was unnecessary or has been completed, and we should
             * return false to allow the background DataVerifier to run.
             */
            return running;
        }
        initialized = true;

        final DbTree dbTree = envImpl.getDbTree();
        parseConfig(configMgr);

        if (!enabled) {
            /*
             * When auto-repair is disabled, clear the repair-done flag if it
             * was previously set. This allows repeating repair after it was
             * previously completed.
             */
            if (dbTree.isAutoRepairReservedFilesDone()) {
                dbTree.clearAutoRepairReservedFilesDone();
                dirtyDbTree(dbTree);
            }
            return false;
        }

        /*
         * Auto-repair is enabled. Return false if auto-repair was previously
         * completed.
         */
        if (dbTree.isAutoRepairReservedFilesDone()) {
            return false;
        }

        /*
         * Auto-repair is enabled and should be run. Configure verify to
         * repair reserved files and start the thread.
         */
        final VerifyConfig verifyConfig = new VerifyConfig();
        verifyConfig.setRepairReservedFiles(true);
        verifyConfig.setBatchSize(configMgr.getInt(VERIFY_BTREE_BATCH_SIZE));

        if (noThrottle) {
            verifyConfig.setBatchDelay(0, TimeUnit.MILLISECONDS);
        } else {
            verifyConfig.setBatchDelay(
                configMgr.getDuration(VERIFY_BTREE_BATCH_DELAY),
                TimeUnit.MILLISECONDS);
        }

        verifier.setBtreeVerifyConfig(verifyConfig);
        running = true;
        start();
        return true;
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public Logger getLogger() {
        return envImpl.getLogger();
    }

    public void requestShutdown() {
        verifier.setStopVerifyFlag(true);
    }

    @Override
    protected int initiateSoftShutdown() {
        requestShutdown();
        return (int) TimeUnit.SECONDS.toMillis(5);
    }

    @Override
    public void run() {
        try {
            /*
             * Give the app time to initialize its metadata to avoid a
             * MAYBE_EXTINCT return from the extinction filter.
             */
            if (PollCondition.await(
                50, TimeUnit.SECONDS.toMillis(START_DELAY_S),
                verifier::getStopVerifyFlag)) {
                return;
            }

            /*
             * Call verifyAll. The repair is incomplete if an exception is
             * thrown or hasErrors returns true.
             *
             * If the repair is complete, log an info message and set the
             * repair-done flag, else log a warning.
             */
            try {
                final VerifySummary summary = verifier.verifyAll();
                if (!summary.hasErrors()) {
                    LoggerUtils.info(
                        getLogger(), envImpl, "Reserved file repair complete");

                    final DbTree dbTree = envImpl.getDbTree();
                    dbTree.setAutoRepairReservedFilesDone();
                    dirtyDbTree(dbTree);
                } else {
                    LoggerUtils.warning(
                        getLogger(), envImpl,
                        "Reserved file repair not complete." +
                            " recordErrors=" + summary.getRecordsWithErrors() +
                            " databaseErrors=" +
                                summary.getDatabasesWithErrors().size() +
                            " otherErrors=" + summary.getOtherErrors());
                }
            } catch (RuntimeException e) {
                /* Exception is not logged by the verifier, log it here. */
                LoggerUtils.warning(
                    getLogger(), envImpl,
                    "Reserved file repair not complete." +
                        " unexpectedException=" + e);
                LoggerUtils.warning(
                    getLogger(), envImpl, LoggerUtils.getStackTrace(e));
            }

            /* Resume normal background verifier operation. */
            envImpl.getDataVerifier().configVerifyTask(
                envImpl.getConfigManager());

            completed = true;
        } finally {
            running = false;
        }
    }

    /**
     * Dirtying either the ID or the Name DB will cause the next checkpoint to
     * log the DbRoot, which will make the repair-done flag durable.
     */
    private void dirtyDbTree(final DbTree dbTree) {
        dbTree.getIdDatabaseImpl().setDirty();
    }

    /**
     * Use repair param to set enabled and noThrottle fields.
     *
     * @throws IllegalArgumentException if the param is invalid.
     */
    private void parseConfig(final DbConfigManager configMgr) {

        final String paramName =
            EnvironmentParams.AUTO_RESERVED_FILE_REPAIR.getName();

        final String param = configMgr.get(
            EnvironmentParams.AUTO_RESERVED_FILE_REPAIR);

        if (param == null) {
            throw new IllegalArgumentException(paramName + " is null");
        }

        enabled = false;
        noThrottle = false;

        switch (param) {
        case "off":
            break;
        case "on":
            enabled = true;
            break;
        case "on.noThrottle":
            enabled = true;
            noThrottle = true;
            break;
        default:
            throw new IllegalArgumentException(
                paramName + " is not 'off', 'on' or 'on.noThrottle': " +
                    param);
        }
    }
}
