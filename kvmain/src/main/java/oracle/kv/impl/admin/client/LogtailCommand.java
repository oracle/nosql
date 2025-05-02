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

package oracle.kv.impl.admin.client;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.LogRecord;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.contextlogger.LogFormatter;

class LogtailCommand extends ShellCommand {
    private static long POLLING_INTERVAL_MS = 2 * 1000;

    private RemoteException threadException = null;
    private boolean logTailThreadGo;
    CommandServiceAPI cs;
    CommandShell cmd;

    LogtailCommand() {
        super("logtail", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length != 1) {
            shell.badArgCount(this);
        }
        cmd = (CommandShell) shell;
        cs = cmd.getAdmin();
        try {
            shell.getOutput().println("(Press <enter> to interrupt.)");
            /*
             * Run the tail in a separate thread, so that the current thread
             * can terminate it when required.
             */
            Thread t = new Thread("logtail") {
                @Override
                public void run() {
                    try {
                        logtailWorker();
                    } catch (RemoteException e) {
                        threadException = e;
                    }
                }
            };

            threadException = null;
            logTailThreadGo = true;
            t.start();

            /* Wait for the user to type <enter>. */
            shell.getInput().readLine();
            synchronized (this) {
                logTailThreadGo = false;
                t.interrupt();
            }
            t.join();
            if (threadException != null) {
                return "Exception from logtail: " +
                    threadException.getMessage();
            }
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        } catch (IOException ioe) {
            return "Exception reading input during logtail";
        } catch (InterruptedException ignored) {
        }
        return "";
    }

    @Override
    public String getCommandDescription() {
        return "Monitors the store-wide log file until interrupted by an " +
               "\"enter\"" + eolt + "keypress.";
    }

    private void logtailWorker()
        throws RemoteException {

        long logSince = 0;
        LogFormatter lf = new LogFormatter(null);

        while (true) {
            Tracker.RetrievedEvents<LogRecord> logEventsContainer;
            List<LogRecord> logEvents;

            synchronized (this) {
                while (true) {
                    if (logTailThreadGo == false) {
                        return;
                    }

                    logEventsContainer = cs.getLogSince(logSince);
                    logEvents = logEventsContainer.getEvents();
                    if (logEvents.size() != 0) {
                        break;
                    }
                    /* Wait until time to poll again */
                    try {
                        this.wait(POLLING_INTERVAL_MS);
                    } catch (InterruptedException e) {
                    }
                }
                /*
                 * Remember the timestamp of the last record we retrieved;
                 * this will become the "since" argument in the next
                 * request.
                 */
                logSince = logEventsContainer.getLastSyntheticTimestamp();
            }

            /* Print the records we got. */
            for (LogRecord lr : logEvents) {
                cmd.getOutput().print(lf.format(lr));
            }
        }
    }
}
