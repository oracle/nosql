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

package com.sleepycat.je.util;

import static com.sleepycat.je.utilint.JETaskCoordinator.JE_VERIFY_LOG_TASK;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

/**
 * Verifies the checksums in one or more log files.
 *
 * <p>This class may be instantiated and used programmatically, or used as a
 * command line utility as described below.</p>
 *
 * <pre>
 * usage: java { com.sleepycat.je.util.DbVerifyLog |
 *               -jar je-&lt;version&gt;.jar DbVerifyLog }
 *  [-h &lt;dir&gt;]      # environment home directory
 *  [-s &lt;file&gt;]     # starting (minimum) file number
 *  [-e &lt;file&gt;]     # ending (one past the maximum) file number
 *  [-d &lt;millis&gt;]   # delay in ms between reads (default is zero)
 *  [-V]                  # print JE version number"
 * </pre>
 *
 * <p>All arguments are optional.  The current directory is used if {@code -h}
 * is not specified.  File numbers may be specified in hex (preceded by {@code
 * 0x}) or decimal format.  For convenience when copy/pasting from other
 * output, LSN format (&lt;file&gt;/&lt;offset&gt;) is also allowed.</p>
 */
public class DbVerifyLog {

    /*
     * Get permit immediately, and do not have a timeout for the amount of time
     * the permit can be held.  Should only be changed for testing.
     */
    public static long PERMIT_WAIT_MS = 0;
    public static long PERMIT_TIME_TO_HOLD_MS = 0;

    private static final String USAGE =
        "usage: " + CmdUtil.getJavaCommand(DbVerifyLog.class) + "\n" +
        "   [-h <dir>]       # environment home directory\n" +
        "   [-s <file>]      # starting (minimum) file number\n" +
        "   [-e <file>]      # ending (one past the maximum) file number\n" +
        "   [-d <millis>]    # delay in ms between reads (default is zero)\n" +
        "   [-V]             # print JE version number";

    private static final Summary EMPTY_SUMMARY = new Summary();
    static {
        EMPTY_SUMMARY.canceled = true;
    }

    private final EnvironmentImpl envImpl;
    private final int readBufferSize;
    private final VerifyLogListener listener;
    private volatile boolean stopVerify = false;
    private long delayMs = 0;
    private boolean background = false;

    /**
     * Creates a utility object for verifying the checksums in log files.
     *
     * <p>The read buffer size is {@link
     * EnvironmentConfig#LOG_ITERATOR_READ_SIZE}.</p>
     *
     * @param env the {@code Environment} associated with the log.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public DbVerifyLog(final Environment env) {
        this(env, 0, null);
    }

    /**
     * Creates a utility object for verifying log files.
     *
     * @param env the {@code Environment} associated with the log.
     *
     * @param readBufferSize is the buffer size to use.  If a value less than
     * or equal to zero is specified, {@link
     * EnvironmentConfig#LOG_ITERATOR_READ_SIZE} is used.
     *
     * @param listener a VerifyLogListener implementation used to obtain
     * more control over the verification process, or null.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public DbVerifyLog(final Environment env,
                       final int readBufferSize,
                       final VerifyLogListener listener) {
        this(DbInternal.getNonNullEnvImpl(env), readBufferSize, listener);
    }

    /**
     * For internal use.
     * @hidden
     */
    public DbVerifyLog(final EnvironmentImpl envImpl,
                       final int readBufferSize,
                       final VerifyLogListener listener) {
        this.readBufferSize = (readBufferSize > 0) ?
            readBufferSize :
            envImpl.getConfigManager().getInt
                (EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        this.envImpl = envImpl;
        this.listener = listener;
    }

    /**
     * Verifies all log files in the environment.
     *
     * @return a summary of the errors that were detected. {@link
     * VerifyLogSummary#hasErrors} will return false if there were no errors.
     *
     * @throws IOException if an IOException occurs while reading a log file.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public VerifyLogSummary verifyAll()
        throws IOException {

        /* The same reason with BtreeVerifier.verifyAll. */
        if (stopVerify) {
            return EMPTY_SUMMARY;
        }

        LoggerUtils.envLogMsg(
            Level.INFO, envImpl, "Start verify of data files");

        final VerifyLogSummary summary = verify(0, Long.MAX_VALUE);

        if (summary.hasErrors()) {
            LoggerUtils.envLogMsg(
                Level.SEVERE, envImpl,
                "End verify of data files: errors detected. " + summary);
        } else {
            LoggerUtils.envLogMsg(
                Level.INFO, envImpl, "End verify of data files: no errors");
        }

        return summary;
    }

    /**
     * Verifies the given range of log files in the environment.
     *
     * @param startFile is the lowest numbered log file to be verified.
     *
     * @param endFile is one greater than the highest numbered log file to be
     * verified.
     *
     * @return a summary of the errors that were detected. {@link
     * VerifyLogSummary#hasErrors} will return false if there were no errors.
     *
     * @throws IOException if an IOException occurs while reading a log file.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public VerifyLogSummary verify(final long startFile, final long endFile)
        throws IOException {
        final SummaryListener summary = new SummaryListener(listener);
        /*
         * Retry if the process is interrupted by replay rollback.
         */
        while (!verifyInternal(startFile, endFile, summary)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(
                    envImpl, e);
            }
        }
        return summary;
    }

    public boolean verifyInternal(
        final long startFile, final long endFile, final SummaryListener summary)
        throws IOException {

        final FileManager fileManager = envImpl.getFileManager();
        final File homeDir = envImpl.getEnvironmentHome();
        final String[] fileNames =
            fileManager.listFileNames(startFile, endFile - 1);
        final ByteBuffer buf = ByteBuffer.allocateDirect(readBufferSize);

        if (!summary.onBegin(fileNames.length)) {
            return true;
        }

        outerLoop: for (final String fileName : fileNames) {
            /*
             * When env is closed, the current executing dataVerifier task
             * should be canceled asap. So when env is closed,
             * setStopVerifyFlag() is called in DataVerifier.shutdown().
             * Here stopVerify is checked to determine whether dataVerifier
             * task continues.
             */
            if (stopVerify) {
                summary.canceled = true;
                break;
            }

            final long fileNum = fileManager.getNumFromName(fileName);
            final File file = new File(homeDir, fileName);

            FileInputStream fis;
            try {
                fis = new FileInputStream(file);
            } catch (FileNotFoundException fne) {
                if (!summary.onFile(fileNum, Collections.emptyList(), true)) {
                    break;
                }
                continue;
            }

            final FileChannel fic = fis.getChannel();

            final LogVerificationReadableByteChannel vic =
                new LogVerificationReadableByteChannel(
                    envImpl, fic, fileName);

            IOException ioe = null;
            int bytesRead;
            try {
                Consumer<InterruptedException> handler = (e) -> {
                    throw new EnvironmentFailureException(envImpl,
                        EnvironmentFailureReason.THREAD_INTERRUPTED, e);
                };
                while (true) {
                    /*
                     * If we are in replay rollback then restart the verify
                     * process because the logs are about to change on disk.
                     */
                    if (envImpl.isReplayRollbackRunning()) {
                        return false;
                    }
                    try (Permit permit = (!background ? null
                        : envImpl.getTaskCoordinator().acquirePermit(
                            JE_VERIFY_LOG_TASK, PERMIT_WAIT_MS,
                            PERMIT_TIME_TO_HOLD_MS, TimeUnit.MILLISECONDS,
                            handler))) {
                        if ((bytesRead = vic.read(buf)) == -1) {
                            break;
                        }
                    }
                        
                    buf.clear();

                    if (!summary.onRead(fileNum, bytesRead)) {
                        break outerLoop;
                    }

                    /* Return as soon as possible if shutdown. */
                    if (stopVerify) {
                        summary.canceled = true;
                        break outerLoop;
                    }

                    if (delayMs > 0) {
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException e) {
                            throw new ThreadInterruptedException(
                                envImpl, e);
                        }
                    }
                }
                if (!summary.onFile(fileNum, Collections.emptyList(), false)) {
                    break;
                }

            } catch (LogVerificationException e) {
                final VerifyLogError error =
                    new VerifyLogError(e.toString(), e.getLsn());

                if (!summary.onFile(
                        fileNum, Collections.singletonList(error), false)) {
                    listener.onEnd(summary);
                }

            } catch (IOException e) {
                ioe = e;
                throw ioe;

            } finally {
                try {
                    /*
                     * vic.close aims to close associated channel fic, but
                     * it may be redundant because fis.close also closes
                     * fic.
                     */
                    fis.close();
                    vic.close();
                } catch (IOException e) {
                    if (ioe == null) {
                        throw e;
                    }
                }
            }
        }
        summary.onEnd(summary);
        return true;
    }

    private static class Summary implements VerifyLogSummary {
        long fileReads;
        long bytesRead;
        int totalFiles;
        int filesVerified;
        int filesWithErrors;
        int filesDeleted;
        boolean canceled;
        final Map<Long, List<VerifyLogError>> allErrors = new HashMap<>();

        @Override
        public boolean hasErrors() {
            return !allErrors.isEmpty();
        }

        @Override
        public boolean wasCanceled() {
            return canceled;
        }

        @Override
        public long getFileReads() {
            return fileReads;
        }

        @Override
        public long getBytesRead() {
            return bytesRead;
        }

        @Override
        public int getTotalFiles() {
            return totalFiles;
        }

        @Override
        public int getFilesVerified() {
            return filesVerified;
        }

        @Override
        public int getFilesWithErrors() {
            return filesWithErrors;
        }

        @Override
        public int getFilesDeleted() {
            return filesDeleted;
        }

        @Override
        public Map<Long, List<VerifyLogError>> getAllErrors() {
            return allErrors;
        }

        @Override
        public String toString() {
            return "[VerifyLogSummary" +
                " canceled=" + canceled +
                " fileReads=" + fileReads +
                " bytesRead=" + bytesRead +
                " filesVerified=" + filesVerified +
                " filesWithErrors=" + filesWithErrors +
                " filesDeleted=" + filesDeleted +
                " allErrors=" + allErrors +
                "]";
        }
    }

    /**
     * Records summary information, logs errors, calls the user-supplied
     * listener.
     */
    private static class SummaryListener extends Summary
        implements VerifyLogListener {

        private final VerifyLogListener listener;

        SummaryListener(final VerifyLogListener listener) {
            this.listener = (listener != null) ? listener : noopVerifyListener;
        }

        @Override
        public boolean onBegin(final int totalFiles) {
            this.totalFiles = totalFiles;
            final boolean keepGoing = listener.onBegin(totalFiles);
            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        @Override
        public void onEnd(final VerifyLogSummary summary) {
            listener.onEnd(summary);
        }

        @Override
        public boolean onRead(final long file, final int bytesRead) {
            ++fileReads;
            this.bytesRead += bytesRead;

            final boolean keepGoing = listener.onRead(file, bytesRead);
            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }

        @Override
        public boolean onFile(final long file,
                              final List<VerifyLogError> errors,
                              final boolean deleted) {
            ++filesVerified;
            if (!errors.isEmpty()) {
                ++filesWithErrors;
                allErrors.put(file, errors);
            }
            if (deleted) {
                ++filesDeleted;
            }

            final boolean keepGoing = listener.onFile(file, errors, deleted);
            if (!keepGoing) {
                canceled = true;
            }
            return keepGoing;
        }
    }

    /**
     * Listener that does nothing. Used to avoid checking listener variables
     * for null.
     */
    private static final VerifyLogListener noopVerifyListener =
        new VerifyLogListener() {

            @Override
            public boolean onBegin(final int totalFiles) {
                return true;
            }

            @Override
            public void onEnd(final VerifyLogSummary summary) {
            }

            @Override
            public boolean onRead(final long file, final int bytesRead) {
                return true;
            }

            @Override
            public boolean onFile(final long file,
                                  final List<VerifyLogError> errors,
                                  final boolean deleted) {
                return true;
            }
        };

    public static void main(String[] argv) {
        try {

            File envHome = new File(".");
            long startFile = 0;
            long endFile = Long.MAX_VALUE;
            long delayMs = 0;

            for (int whichArg = 0; whichArg < argv.length; whichArg += 1) {
                final String nextArg = argv[whichArg];
                if (nextArg.equals("-h")) {
                    whichArg++;
                    envHome = new File(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-s")) {
                    whichArg++;
                    String arg = CmdUtil.getArg(argv, whichArg);
                    final int slashOff = arg.indexOf("/");
                    if (slashOff >= 0) {
                        arg = arg.substring(0, slashOff);
                    }
                    startFile = CmdUtil.readLongNumber(arg);
                } else if (nextArg.equals("-e")) {
                    whichArg++;
                    String arg = CmdUtil.getArg(argv, whichArg);
                    final int slashOff = arg.indexOf("/");
                    if (slashOff >= 0) {
                        arg = arg.substring(0, slashOff);
                    }
                    endFile = CmdUtil.readLongNumber(arg);
                } else if (nextArg.equals("-d")) {
                    whichArg++;
                    delayMs =
                        CmdUtil.readLongNumber(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-V")) {
                    System.out.println(JEVersion.CURRENT_VERSION);
                    System.exit(0);
                } else {
                    printUsageAndExit("Unknown argument: " + nextArg);
                }
            }

            final EnvironmentImpl envImpl =
                CmdUtil.makeUtilityEnvironment(envHome, true /*readOnly*/, false);

            final DbVerifyLog verifier = new DbVerifyLog(envImpl, 0, null);
            verifier.setReadDelay(delayMs, TimeUnit.MILLISECONDS);

            final VerifyLogSummary summary =
                verifier.verify(startFile, endFile);

            if (summary.hasErrors()) {
                System.err.println("**** ERRORS WERE DETECTED ****");
                System.err.println(summary);
                System.exit(1);
            } else {
                System.out.println("SUCCESS");
                System.out.println(summary);
                System.exit(0);
            }
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            printUsageAndExit(e.toString());
        }
    }

    private static void printUsageAndExit(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(USAGE);
        System.exit(1);
    }

    /**
     * Configures the delay between file reads during verification. A delay
     * between reads is needed to allow other JE components, such as HA, to
     * make timely progress.
     *
     * <p>By default there is no read delay (it is zero).</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the delay between reads is
     * {@link EnvironmentConfig#VERIFY_LOG_READ_DELAY}.</p>
     *
     * @param delay the delay between reads or zero for no delay.
     *
     * @param unit the {@code TimeUnit} of the delay value. May be
     * null only if delay is zero.
     */
    public void setReadDelay(long delay, TimeUnit unit) {
        delayMs = PropUtil.durationToMillis(delay, unit);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public void setStopVerifyFlag(boolean val) {
        stopVerify = val;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public void setBackground(boolean background) {
        this.background = background;
    }
}
