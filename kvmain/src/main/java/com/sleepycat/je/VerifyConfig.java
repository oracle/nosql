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

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.util.DbVerify;

/**
 * Specifies the attributes of a verification operation.
 *
 * @see Environment#verify(VerifyConfig)
 * @see Database#verify(VerifyConfig)
 */
public class VerifyConfig implements Cloneable {

    /*
     * For internal use, to allow null as a valid value for the config
     * parameter.
     */
    public static final VerifyConfig DEFAULT = new VerifyConfig();

    private boolean printInfo = false;
    private PrintStream showProgressStream = null;
    private int showProgressInterval = 0;
    private boolean verifySecondaries = true;
    private boolean verifyDataRecords = false;
    private boolean repairDataRecords = false;
    private boolean corruptSecondaryDB = false;
    private boolean verifyObsoleteRecords = false;
    private boolean repairReservedFiles = true;
    private int batchSize = 1000;
    private int batchDelayMs = 10;
    private Level errorLogLevel = Level.SEVERE;
    private VerifyListener listener;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public VerifyConfig() {
    }

    /**
     * Configures a listener that can be used to collect information about
     * verification and can cancel verification at any time.
     *
     * <p>By default this property is null.</p>
     */
    public VerifyConfig setListener(VerifyListener listener) {
        this.listener = listener;
        return this;
    }

    /**
     * Returns the configured listener or null if none has been configured.
     */
    public VerifyListener getListener() {
        return listener;
    }

    /**
     * Configures the level for logging of verification errors; if null, no
     * logging is performed.
     *
     * <p>By default this property is set to {@link Level#SEVERE}. Logging is
     * performed using a rate-limited logger that logs at most one
     * message per {@link VerifyError.Problem} per minute.</p>
     */
    public VerifyConfig setErrorLogLevel(final Level errorLogLevel) {
        this.errorLogLevel = errorLogLevel;
        return this;
    }

    /**
     * Returns the configured logging level or null if no logging is performed.
     */
    public Level getErrorLogLevel() {
        return errorLogLevel;
    }

    /**
     * Specifies whether the verifier repairs (reactivates) reserved files
     * when they contain actively referenced records.
     *
     * <p>By default this property is true.</p>
     *
     * <p>Used to correct corruption resulting from certain bugs that cause
     * incorrect cleaning. This type of repair applies only when there is a
     * known bug that prevented LN migration, and it has been fixed.</p>
     *
     * <p>The cost and impact on other operations is minimal. The record is
     * read locked for only a short duration and the more expensive checks
     * are performed without holding the record lock.</p>
     *
     * <p>An INFO-level "Reactivated reserved file" message is logged for
     * each reactivated file.</p>
     */
    public VerifyConfig setRepairReservedFiles(boolean repair) {
        repairReservedFiles = repair;
        return this;
    }

    /**
     * Returns whether the verifier repairs (reactivates) reserved files
     * when they contain actively referenced records.
     *
     * <p>By default this property is true.</p>
     */
    public boolean getRepairReservedFiles() {
        return repairReservedFiles;
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to print basic verification information.
     *
     * <p>Information is printed to the {@link #getShowProgressStream()} if it
     * is non-null, and otherwise to System.err.</p>
     *
     * <p>By default this is false. However, the default is true when
     * performing a verification using the {@link DbVerify} command line
     * utility, and the default can be overridden with the {@code -q}
     * argument.</p>
     *
     * @param printInfo If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to print basic
     * verification information.
     *
     * @return this
     */
    public VerifyConfig setPrintInfo(boolean printInfo) {
        this.printInfo = printInfo;
        return this;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information.
     */
    public boolean getPrintInfo() {
        return printInfo;
    }

    /**
     * Configures the verify operation to display progress to the PrintStream
     * argument.  The accumulated statistics will be displayed every N records,
     * where N is the value of showProgressInterval.
     *
     * <p>By default this is null, implying that {@code System.err} is used.</p>
     *
     * @return this
     */
    public VerifyConfig setShowProgressStream(PrintStream showProgressStream) {
        this.showProgressStream = showProgressStream;
        return this;
    }

    /**
     * Returns the PrintStream on which the progress messages will be displayed
     * during long running verify operations.
     */
    public PrintStream getShowProgressStream() {
        return showProgressStream;
    }

    /**
     * When the verify operation is configured to display progress the
     * showProgressInterval is the number of LNs between each progress report.
     *
     * <p>By default this is zero, implying that no progress info is shown.</p>
     *
     * @return this
     */
    public VerifyConfig setShowProgressInterval(int showProgressInterval) {
        this.showProgressInterval = showProgressInterval;
        return this;
    }

    /**
     * Returns the showProgressInterval value, if set.
     */
    public int getShowProgressInterval() {
        return showProgressInterval;
    }

    /**
     * Configures verification to verify secondary database integrity. This is
     * equivalent to verifying secondaries in the background Btree verifier,
     * when {@link EnvironmentConfig#VERIFY_SECONDARIES} is set to true.
     *
     * <p>By default this is true.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifySecondaries(boolean verifySecondaries) {
        this.verifySecondaries = verifySecondaries;
        return this;
    }

    /**
     * Returns the verifySecondaries value.
     */
    public boolean getVerifySecondaries() {
        return verifySecondaries;
    }

    /**
     * Configures verification to read and verify the leaf node (LN) of a
     * primary data record. This is equivalent to verifying data records in the
     * background Btree verifier, when
     * {@link EnvironmentConfig#VERIFY_DATA_RECORDS} is set to true.
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifyDataRecords(boolean verifyDataRecords) {
        this.verifyDataRecords = verifyDataRecords;
        return this;
    }

    /**
     * Returns the verifyDataRecords value.
     */
    public boolean getVerifyDataRecords() {
        return verifyDataRecords;
    }

    /**
     * Configures reparation of the leaf node (LN) of a primary data record. 
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setRepairDataRecords(boolean repairDataRecords) {
        this.repairDataRecords = repairDataRecords;
        return this;
    }

    /**
     * to check whether the secondary databases verification is working  
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setCorruptSecondaryDB(boolean corruptSecondaryDB) {
        this.corruptSecondaryDB = corruptSecondaryDB;
        return this;
    }

    /**
     * Returns the corruptSecondaryDB value.
     */
    public boolean getCorruptSecondaryDB() {
        return corruptSecondaryDB;
    }

    /**
     * Returns the repairDataRecords value.
     */
    public boolean getRepairDataRecords() {
        return repairDataRecords;
    }


    /**
     * Configures verification to verify the obsolete record metadata. This is
     * equivalent to verifying obsolete metadata in the background Btree
     * verifier, when {@link EnvironmentConfig#VERIFY_OBSOLETE_RECORDS} is set
     * to true.
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifyObsoleteRecords(
        boolean verifyObsoleteRecords) {
        this.verifyObsoleteRecords = verifyObsoleteRecords;
        return this;
    }

    /**
     * Returns the verifyObsoleteRecords value.
     */
    public boolean getVerifyObsoleteRecords() {
        return verifyObsoleteRecords;
    }

    /**
     * Configures the number of records verified per batch. In order to give
     * database remove/truncate the opportunity to execute, records are
     * verified in batches and there is a {@link #setBatchDelay delay}
     * between batches.
     *
     * <p>By default the batch size is 1000.</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the batch size is
     * {@link EnvironmentConfig#VERIFY_BTREE_BATCH_SIZE}.</p>
     *
     * @return this
     */
    public VerifyConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Returns the batchSize value.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Configures the delay between batches. In order to give database
     * remove/truncate the opportunity to execute, records are verified in
     * {@link #setBatchSize batches} and there is a delay between batches.
     *
     * <p>By default the batch delay is 10 ms. However, the default is zero (no
     * delay) when performing a verification using the {@link DbVerify} command
     * line utility, and the default can be overridden with the {@code -d}
     * argument..</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the batch delay is
     * {@link EnvironmentConfig#VERIFY_BTREE_BATCH_DELAY}.</p>
     *
     * @param delay the delay between batches.
     *
     * @param unit the {@code TimeUnit} of the delay value. May be
     * null only if delay is zero.
     *
     * @return this
     */
    public VerifyConfig setBatchDelay(long delay, TimeUnit unit) {
        batchDelayMs = PropUtil.durationToMillis(delay, unit);
        return this;
    }

    /**
     * Returns the batch delay.
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     */
    public long getBatchDelay(TimeUnit unit) {
        return PropUtil.millisToDuration(batchDelayMs, unit);
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public VerifyConfig clone() {
        try {
            return (VerifyConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "[VerifyConfig" +
            " printInfo=" + printInfo +
            " showProgressInterval=" + showProgressInterval +
            " verifySecondaries=" + verifySecondaries +
            " verifyDataRecords=" + verifyDataRecords +
            " verifyObsoleteRecords=" + verifyObsoleteRecords +
            " repairReservedFiles=" + repairReservedFiles +
            " batchSize=" + batchSize +
            " batchDelayMs=" + batchDelayMs +
            "]";
    }
}
