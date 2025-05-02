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

package oracle.kv.util.internal;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;

import oracle.kv.impl.admin.EventStore;
import oracle.kv.impl.admin.EventStore.EventCursor;
import oracle.kv.impl.admin.GeneralStore;
import oracle.kv.impl.admin.PlanStore;
import oracle.kv.impl.admin.PlanStore.PlanCursor;
import oracle.kv.impl.admin.TopologyStore;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.util.FormatUtils;

/**
 * AdminDump is an unadvertised, in-progress utility which can read and display
 * the contents of the Admin database for debugging and diagnostic purposes.It
 * must be invoked on the node where the Admin environment is hosted.
 *
 * Note that due to changes in the Admin's persistent store, this utility
 * is not compatible with releases prior to R4.0.
 *
 * <pre>
 *   AdminDump -h &lt;directory where Admin database is located&gt;
 *             ... flags ... (see usage)
 * </pre>
 * Note that the Admin database is typically in
 * kvroot/store/snX/adminX/env. The je.jar must be in the classpath, as well as
 * the usual kvstore classes.
 *
 * Currently, the utility can
 * - display RN params
 * - the most recent numTopos deployed topologies are displayed.
 * - count all the records in the AdminDB
 * - dump, using a cursor, the stored events.
 * It would be a good idea to expand this to display params for SNs and Admins,
 * as well as other future metadata.
 */
public class AdminDump {
    private static int DEFAULT_TOPO_VERSIONS = 0;
    private final PrintStream out;
    private File envHome = null;
    private int showTopos = DEFAULT_TOPO_VERSIONS;
    private boolean showParams = false;
    private boolean showCounts = false;
    private boolean showEvents = false;
    private boolean showModel = false;
    private boolean showPlans = false;

    private AdminDump(PrintStream out) {
        this.out = out;
    }

    public static void main(String[] args) throws Exception {
        AdminDump dumper = new AdminDump(System.out);
        dumper.parseArgs(args);
        try {
            dumper.run();
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public void run() {
        out.println("For internal use only.");
        /*
         * Be sure to display which Admin this is, and what time the dump
         * is taken, in case the information is gathered by a user and
         * sent to us.
         */
        out.println("Information from admin database in " + envHome);
        out.println("Current time: " +
                     FormatUtils.formatDateTimeMillis(
                         System.currentTimeMillis()));

        displayAdminInfo();
    }

    /**
     * Display selected pieces of the admin database
     */
    private void displayAdminInfo() {
        final Logger logger = Logger.getLogger(AdminDump.class.getName());

        /*
         * Initialize an environment configuration, and create an environment.
         */
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        envConfig.setAllowCreate(false);
        final Environment env = new Environment(envHome, envConfig);

        /* Show the types of classes in the adminDB */
        if (showModel) {
            out.println("-----------------Stores in Admin DB");
            for (DB_TYPE dbType : DB_TYPE.values()) {
                out.println(dbType.getDBName());
            }
        }

        /* Count the records in the DB */
        if (showCounts) {
           countRecords(logger, env);
        }

        /* Show the events */
        if (showEvents) {
            try (final EventStore es =
                                    EventStore.getReadOnlyInstance(logger, env);
                 final EventCursor cursor = es.getEventCursor()) {
                int i = 0;
                CriticalEvent ev = cursor.first();
                while (ev != null) {
                    out.println(i++ + " " + ev);
                    ev = cursor.next();
                }
            }
        }

        /*
         * Show all RN params. In the future:
         *  - select RNS to show
         *  - show admin or sn params
         */
        if (showParams) {
            final Parameters params;
            try (final GeneralStore gs =
                                GeneralStore.getReadOnlyInstance(logger, env)) {
                params = gs.getParameters(null);
            }
            out.println(params.printRepNodeParams());
        }

        /* Show topologies */
        if (showTopos > 0) {
            try (final TopologyStore ts =
                               TopologyStore.getReadOnlyInstance(logger, env)) {
                final List<String> history = ts.displayHistory(false);
                final ListIterator<String> itr =
                            history.listIterator(history.size());
                int count = 0;
                while (itr.hasPrevious()) {
                    if (count >= showTopos) {
                        break;
                    }
                    out.println("--------- deployed topology --------");
                    out.println(itr.previous());
                }
            }
        }

        /* Show plans */
        if (showPlans) {
            try (final PlanStore ps = PlanStore.getReadOnlyInstance(logger,env);
                 final PlanCursor cursor = ps.getPlanCursor(null, 0)) {

                out.println("-----------------Plans in Admin DB");

                for (Plan p = cursor.first();
                     p != null;
                     p = cursor.next()) {

                    final StatusReport report =
                        new StatusReport(p,
                                         StatusReport.VERBOSE_BIT |
                                         StatusReport.SHOW_FINISHED_BIT);

                    out.println(report.display());
                }
            }
        }
    }

    /**
     * Parse the command line parameters.
     *
     * @param argv Input command line parameters.
     */
    public void parseArgs(String argv[]) {

        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-numTopos")) {
                showTopos = Integer.parseInt(argv[argc++]);
            } else if (thisArg.equals("-showCounts")) {
                showCounts = true;
            } else if (thisArg.equals("-showParams")) {
                showParams = true;
            } else if (thisArg.equals("-showEvents")) {
                showEvents = true;
            } else if (thisArg.equals("-showModel")) {
                showModel = true;
            } else if (thisArg.equals("-showPlans")) {
                showPlans = true;
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }
    }

    /**
     * Print the usage of this utility.
     *
     * @param message
     */
    private void printUsage(String msg) {
        if (msg != null) {
            out.println(msg);
        }

        out.println("Usage: " + AdminDump.class.getName());
        out.println(" -h <dir>        # admin service environment directory");
        out.println(" -numTopos <num> # number of historical topo versions");
        out.println("                 # to display");
        out.println(" -showParams     # dump RN params");
        out.println(" -showCounts     # show record counts by record type");
        out.println(" -showEvents     # show all events");
        out.println(" -showModel      # show all classes in the db");
        out.println(" -showPlans      # show all plans in the db");
        System.exit(-1);
    }

    /**
     * Get a count of all records in the AdminDB, by type. Actually this only
     * counts the record types which can have multiple instances. Currently
     * this is events, plans, and topologies.
     */
    private void countRecords(Logger logger, Environment env) {
        out.println("---------------- Counting records");
        long totalCount = 0;

        /* Events */
        try (final EventStore es =
                 EventStore.getReadOnlyInstance(logger, env);
             final EventCursor cursor = es.getEventCursor()) {
            int count = 0;
            CriticalEvent ev = cursor.first();
            while (ev != null) {
                count++;
                ev = cursor.next();
            }
            out.println("Events = " + count);
            totalCount += count;
        }

        /* Plans */
        try (final PlanStore ps = PlanStore.getReadOnlyInstance(logger,env);
             final PlanCursor cursor = ps.getPlanCursor(null, 0)) {
            int count = 0;
            Plan p = cursor.first();
            while (p != null) {
                count++;
                p = cursor.next();
            }
            out.println("Plans = " + count);
            totalCount += count;
        }

        /* Topologies */
        try (final TopologyStore ts =
                 TopologyStore.getReadOnlyInstance(logger, env)) {
            final int historyCount = ts.displayHistory(true).size();
            out.println("Topology history = " + historyCount);
            totalCount += historyCount;
            final int candidateCount = ts.getCandidateNames(null).size();
            out.println("Topology candidates = " + candidateCount);
            totalCount += candidateCount;
        }
        out.println("total records = " + totalCount);
    }
}
