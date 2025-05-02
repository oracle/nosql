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

package oracle.kv.impl.sna;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * The class used to create, manage and restart processes.
 * <p>
 * An instance of this object is created for each process to be managed.  The
 * caller is responsible for the command to be executed, setting a restart
 * count, and the initial creation of the process, for example:
 * <pre>
 *    List<String> command = new ArrayList<String>();
 *    command.add("/bin/sleep");
 *    command.add("1");
 *    ProcessMonitor monitor = new ProcessMonitor(command, -1);
 *    monitor.startProcess();
 * </pre>
 * Internal objects and threads are used to monitor and restart the process if
 * desired.  This will happen if the process terminates normally or abnormally.
 * A restart count of -1 means restart indefinitely.
 * <p>
 * Note: exit/restart of a managed process will not change the command line
 * arguments used for starting it.  Those are cached in this object.  The major
 * thing affected is JVMParams such as arguments to Java or logging
 * configuration.  This behavior could be changed but it's simple at this time.
 * <p>
 * The caller can explicitly stop the process which will not result in restart.
 * <p>
 * If an excessive number of process restarts are detected the managed service
 * will eventually be permanently stopped.  The algorithm, which may need
 * tuning over time is:
 * <blockquote>
 *   If there have been more than RESTART_MAX restarts in a period of
 *   RESTART_MILLIS milliseconds, stop restarting.
 * </blockquote>
 * The following flow chart describes the life cycle of a monitored service
 * process:
 * <pre>{@code
 *                                   +------------+
 *         +-----------------------> | NO_SERVICE | <-------------------+
 *         |                         +------------+                     |
 *         |                               V                            |
 *      destroy                            |                         destroy
 *         |                             create                         |
 *         |                               |                            |
 *         ^                               V                            ^
 * +---------------+ >----enable----> +----------+ <---enable----< +----------+
 * | TEMP_DISABLED |                  | RUNNABLE |                 | DISABLED |
 * +---------------+ <---no_auto----< +----------+ >---disable---> +----------+
 *         ^              start          ^    V                         ^
 *         |                             |    |                         |
 *         |                             |    |                         |
 *         |                             |  auto_start                  |
 *         |                             |    |                         |
 *         |                           exit   |                         |
 *         |                             |    |                         |
 *         |                             |    |                         |
 *         |                             ^    V                         |
 *         |                           +---------+                      |
 *         +-----------stop----------< | RUNNING | >------disable-------+
 *                                     +---------+
 * }</pre>
 * The TEMP_DISABLED state represents the fact that if the monitor decides to
 * not restart a process because the exit code request that it not be
 * restarted, or because the process failed too many times, that decision is
 * only maintained transiently by the current ProcessMonitor instance. If the
 * process running the process monitor -- typically the SNA -- is restarted,
 * then the monitored service will be reenabled. On the other hand, in the
 * DISABLED state, the fact that a service is disabled is stored persistently,
 * and the service will remain disabled even if the process monitor process is
 * restarted.
 */
public class ProcessMonitor {
    private static volatile boolean printStartupOK;
    private final ReentrantLock lock;
    private Logger logger;
    private List<String> command;
    private final Map<String, String> env;
    private ArrayList<Long> restarts;
    private String serviceName;
    private MonitorThread monitorThread;
    private IOThread ioThread;
    private int restartCount;
    private ProcessState state;
    private Process process;
    private int totalRestarts;
    private int exitCode;
    protected volatile StringBuilder startupBuffer;

    /**
     * Start with 5 restarts in under 60 seconds as a problem.  TODO: tune this
     * to maybe have both short- and long-term triggers.  A long-term trigger
     * could just log a warning that something's wrong (e.g. 1 restart/hour
     * isn't horrible but probably means something is up).
     */
    private static final int RESTART_RESET=30;
    private static final int RESTART_MAX=5;
    private static final long RESTART_MILLIS=60*1000;

    enum ProcessState {
        DOWN, RUNNING, STOPPING
            }

    public ProcessMonitor(List<String> command,
                          Map<String, String> env,
                          int restartCount,
                          String serviceName,
                          Logger logger) {
        this.lock = new ReentrantLock();
        this.restartCount = restartCount;
        this.logger = logger;
        this.command = command;
        this.env = env;
        this.serviceName = serviceName;
        this.process = null;
        this.monitorThread = null;
        this.ioThread = null;
        this.state = ProcessState.DOWN;
        this.totalRestarts = 0;
        this.restarts = new ArrayList<Long>();
        this.exitCode = 0;
    }

    /**
     * A convenience overloading of the above for cases where there is no
     * environment associated with the process.
     */
    public ProcessMonitor(List<String> command,
                          int restartCount,
                          String serviceName,
                          Logger logger) {
        this(command, null, restartCount, serviceName, logger);
    }

    public void reset(List<String> newCommand,
                      String newServiceName) {
        this.command = newCommand;
        this.serviceName = newServiceName;
    }

    public void dontRestart() {
        lock.lock();
        restartCount = 0;
        lock.unlock();
    }

    public boolean canRestart() {
        return (restartCount != 0);
    }

    public boolean isRunning() {
        return (state != ProcessState.DOWN);
    }

    public int getExitCode() {
        return exitCode;
    }

    public void resetLogger(Logger logger1) {
        this.logger = logger1;
    }

    private void logFine(String msg) {
        if (logger != null) {
            logger.fine(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    private void logInfo(String msg) {
        if (logger != null) {
            logger.info(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    private void logSevere(String msg) {
        if (logger != null) {
            logger.severe(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    protected void afterStart() {
        /* no-op */
    }

    protected void onRestart() {
        /* no-op */
    }

    protected void onExit() {
        /* no-op */
    }

    /** Create a thread for handling output from a managed Process. */
    protected IOThread createIOThread(String name) {
        return new IOThread(name);
    }

    /** Create a thread for monitoring and restarting a managed Process. */
    protected MonitorThread createMonitorThread(String name) {
        return new MonitorThread(name, true);
    }

    public void startProcess()
        throws IOException {

        lock.lock();
        try {
            if (state == ProcessState.DOWN) {
                ProcessBuilder builder = new ProcessBuilder(command);
                if (env != null) {
                    final Map<String, String> penv = builder.environment();
                    penv.putAll(env);
                }
                builder.redirectErrorStream(true);
                process = builder.start();
                state = ProcessState.RUNNING;
                logInfo("startProcess");
                ioThread = createIOThread("SNA.io." + serviceName);
                ioThread.start();
                monitorThread =
                    createMonitorThread("SNA.monitor." + serviceName);
                monitorThread.start();
            }
        } finally {
            lock.unlock();
        }
        afterStart();
    }

    /**
     * Stop a running process.  The process may still be running or it may have
     * exited; this method cleans up the object and threads in both cases.
     * This is complicated by the need to synchronize access from both the
     * MonitorThread and the owning thread.  If called by the owning thread the
     * process will not be restarted by the MonitorThread.  That case is
     * simpler.  Trickier races occur if the MonitorThread is in the middle of
     * stopping/restarting the process and the owning thread calls.  There is
     * one window where the lock allows the owning thread in.  If that occurs
     * the restartCount will have been set to 0 causing the MonitorThread to
     * simply exit if it is not interrupted.
     */
    public void stopProcess(boolean isMonitor)
        throws InterruptedException {

        if (!isMonitor) {
            logInfo("stopProcess");
        }
        lock.lock();

        /* Setting restartCount to 0 ensures that the process won't restart. */
        if (!isMonitor) {
            restartCount = 0;
        }

        /**
         * If the owning thread is stopping the process and the monitor calls,
         * just return.  The monitor will eventually exit.  If the monitor is
         * stopping the process and the owning thread calls, continue on so
         * that the process is not restarted and the monitor is killed.
         */
        if (state == ProcessState.DOWN ||
            (state != ProcessState.RUNNING && isMonitor)) {
            /* The process is already stopping or down. */
            lock.unlock();
            return;
        }

        state = ProcessState.STOPPING;
        if (process != null) {
            process.destroy();
            /* don't null the process object until after joins, below */
        }
        lock.unlock();
        /* The lock is unlocked, allowing threads to exit. */
        try {

            /**
             * No lock is held but the only threads that will set monitorThread
             * and ioThread to null are the object owner and the MonitorThread
             * itself, and the Thread.join() synchronizes that race.
             */
            if (monitorThread != null && !isMonitor) {
                monitorThread.join();
                monitorThread = null;
            }

            if (ioThread != null) {
                ioThread.join();
                ioThread = null;
            }
        } catch (InterruptedException e) {
            logInfo("Exception in stopProcess");
            if (Thread.interrupted() && isMonitor) {
                /* Rethrow if MonitorThread was interrupted. */
                throw e;
            }
        }
        lock.lock();
        state = ProcessState.DOWN;
        process = null;
        lock.unlock();
    }

    void restartProcess()
        throws IOException {

        logInfo("restartProcess called, totalRestarts is " + totalRestarts +
                ", restartCount is " + restartCount);

        lock.lock();
        try {
            /*
             * Although we make this check here, the caller should only call
             * this method if restartCount is non-0.
             */
            if (restartCount != 0) {
                /* This will lock recursively. */
                startProcess();
                restarts.add(System.currentTimeMillis());
                ++totalRestarts;
                if (restartCount > 0) {
                    --restartCount;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wait for the managed process to exit to synchronize shutdown.  This is
     * called by the ProcessServiceManager when a service is shut down cleanly.
     */
    public boolean waitProcess(long millis)
        throws InterruptedException {

        boolean retval = true;

        if (monitorThread != null) {
            logFine("waiting for MonitorThread");
            monitorThread.join(millis);
            if (monitorThread != null && monitorThread.isAlive()) {
                retval = false;
            }
            monitorThread = null;
        }

        if (ioThread != null && retval == true) {
            logFine("waiting for IOThread");
            ioThread.join(millis);
            if (ioThread != null && ioThread.isAlive()) {
                retval = false;
            }
            ioThread = null;
        }
        return retval;
    }

    /**
     * Forcibly terminate the process.  This should only be called if an
     * organized stop fails.
     */
    public void destroyProcess() {
        lock.lock();
        Process p = process;
        lock.unlock();
        if (p != null) {
            p.destroy();
        }
    }

    /**
     * Request that the process monitor write ManagedService.STARTUP_OK to
     * standard output when it encounters that line while monitoring the
     * started of managed services. Used to propagate the information to a
     * process monitor monitoring this process.
     */
    public static void setPrintStartupOK(boolean print) {
        printStartupOK = print;
    }

    /**
     * The class responsible for monitoring a Process and restarting it on
     * exit.  A new instance of this thread/object is created on each restart.
     */
    protected class MonitorThread extends Thread {
        private final boolean useExitCode;

        private MonitorThread(String name, boolean useExitCode) {
            super(name);
            this.useExitCode = useExitCode;
        }

        /**
         * Should the process be restarted? Default is yes.
         */
        private boolean okToRestart(int exitStatus) {
            if (useExitCode &&
                ! ProcessExitCode.needsRestart(exitStatus)) {
                logInfo("Process will not be restarted because restarts are" +
                        " not needed for exit code: " + exitStatus);
                return false;
            }

            logInfo("Process restart requested for exit code: " +
                    exitStatus +
                    ((exitStatus == ProcessExitCode.RESTART_OOME.getValue()) ?
                       " Process experienced an OOME." : ""));
            if (restartCount == 0) {
                logInfo("Process will not be restarted because restarts have" +
                        " been disabled");
                return false;
            }
            return checkExcessiveRestarts();
        }

        /**
         * Determine if there have been too many restarts of this process
         * based on algorithm vs number.
         *
         * Return true if it is OK to restart (not too many restarts).
         * Return false (bad) if there have been too many restarts.
         */
        private boolean checkExcessiveRestarts() {
            boolean ret = true;
            if (restarts.size() >= RESTART_MAX) {
                long last = restarts.get(restarts.size()-1);
                long first = restarts.get(restarts.size()-RESTART_MAX);
                if (last - first < RESTART_MILLIS) {
                    logSevere("Service will not restart and will be" +
                              " disabled because it restarted " +
                              restarts.size() + " times in " +
                              RESTART_MILLIS/1000 + " seconds");
                    dontRestart();
                    ret = false;
                }
            }
            if (restarts.size() >= RESTART_RESET) {
                /* Reset the list to avoid unbound growth */
                restarts = new ArrayList<Long>();
            }
            return ret;
        }

        @Override
        public void run() {
            try {
                assert (process != null);
                exitCode = process.waitFor();
                logInfo("exited, exit code: " + exitCode);

                /**
                 * Let the IOThread exit -- it may have useful things to say if
                 * the exit occurred during startup.
                 */
                if (ioThread != null) {
                    ioThread.join();
                    ioThread = null;
                }

                /**
                 * If the process was explicitly stopped the stop and restart
                 * will be no-ops because the process state will be DOWN and
                 * the restart count will have been zeroed.
                 */
                stopProcess(true);

                /**
                 * Restart or not?  Default is to restart.
                 * TODO: manufacture ERROR_* ServiceStatus for the service
                 */
                if (okToRestart(exitCode)) {
                    onRestart();
                    restartProcess();
                } else {
                    onExit();
                    logInfo("not restarting");
                }
            } catch (Exception e) {
                String msg = "Unexpected exception in MonitorThread: " +
                    e + CommonLoggerUtils.getStackTrace(e);
                logSevere(msg);
            }
        }
    }

    /**
     * The class responsible for handling output from a managed Process.  A new
     * instance of this thread/object is created on each restart.
     */
    protected class IOThread extends Thread {
        private IOThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                boolean startupOK = false;

                /**
                 * Small delay to start to give the process a chance to do
                 * something.
                 */
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {}

                /**
                 * Process may have already been stopped and nulled.
                 */
                startupBuffer = new StringBuilder(512);
                logFine("IOThread initializing startup buffer");
                BufferedReader reader = null;
                lock.lock();
                if (process != null) {
                    reader = new BufferedReader
                        (new InputStreamReader(process.getInputStream()));
                } else {
                    logInfo("IOthread: no process, exiting");
                    lock.unlock();
                    return;
                }
                lock.unlock();
                for (String line = reader.readLine();
                     line != null;
                     line = reader.readLine()) {

                    /**
                     * Logging output in the new process should only be
                     * generated by the SNA.  Once the real service takes over
                     * it will log to its own files.
                     */
                    logInfo(line);

                    if (line.contains(ManagedService.STARTUP_OK)) {
                        startupOK = true;
                        /*
                         * Print the STARTUP_OK message to standard output so
                         * that an outer ProcessMonitor would also see it.
                         */
                        if (printStartupOK) {
                            System.out.println(ManagedService.STARTUP_OK);
                        }
                        startupBuffer = null;
                        logFine("IOThread clearing startup buffer");
                    }
                    if (!startupOK) {
                        startupBuffer.append("\n" + line);
                    }
                }
            } catch (Exception e) {
                logInfo("IOThread exception: " +
                        e.getMessage());
            }
            logInfo("IOThread exiting");
        }

        void closeInput()
            throws IOException {

            /* Provoke an IO exception to cause the thread to exit. */
            process.getInputStream().close();
        }
    }
}
