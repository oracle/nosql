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

package com.sleepycat.je.utilint;

import java.util.HashMap;

import com.sleepycat.je.rep.util.DbEnableReplication;
import com.sleepycat.je.rep.util.DbGroupAdmin;
import com.sleepycat.je.rep.util.DbPing;
import com.sleepycat.je.rep.util.VLSNIndexDump;
import com.sleepycat.je.rep.util.ldiff.LDiff;
import com.sleepycat.je.rep.utilint.DbDumpGroup;
import com.sleepycat.je.rep.utilint.DbFeederPrintLog;
import com.sleepycat.je.rep.utilint.DbNullNode;
import com.sleepycat.je.rep.utilint.DbRepRunAction;
import com.sleepycat.je.rep.utilint.DbStreamVerify;
import com.sleepycat.je.rep.utilint.DbSync;
import com.sleepycat.je.util.DbCacheSize;
import com.sleepycat.je.util.DbDeleteReservedFiles;
import com.sleepycat.je.util.DbDump;
import com.sleepycat.je.util.DbFilterStats;
import com.sleepycat.je.util.DbLoad;
import com.sleepycat.je.util.DbPrintLog;
import com.sleepycat.je.util.DbRunAction;
import com.sleepycat.je.util.DbScavenger;
import com.sleepycat.je.util.DbSpace;
import com.sleepycat.je.util.DbStat;
import com.sleepycat.je.util.DbTruncateLog;
import com.sleepycat.je.util.DbVerify;
import com.sleepycat.je.util.DbVerifyLog;

/**
 * Used as the main class for the manifest of the je.jar file, and so it is
 * executed when running: java -jar je.jar.  The first argument must be the
 * final part of the class name of a utility in the com.sleepycat.je.util
 * package, e.g., DbDump.  All following parameters are passed to the main
 * method of the utility and are processed as usual.
 *
 * Apart from the package, this class is ambivalent about the name of the
 * utility specified; the only requirement is that it must be a public static
 * class and must contain a public static main method.
 */
public class JarMain {

    private static final String USAGE = "usage: java <utility> [options...]";

    static interface Worker {
        public void work(String[] args) throws Throwable;
    }

    static class Utility {
        private final String prefix;
        private final Worker worker;

        public Utility(String packageName, Worker worker) {
            super();
            this.prefix = packageName;
            this.worker = worker;
        }

        public String getPrefix() {
            return prefix;
        }

        public Worker getWorker() {
            return worker;
        }

    }

    /* Use a HashMap to allow the utilities to live in multiple packages. */
    private static HashMap<String, Utility> utilPrefixMap =
        new HashMap<String, Utility>();

    /* Map each utility name to its package. */
    static {
        /* The utilities in directory com/sleepycat/je/util. */
        String prefix = "com.sleepycat.je.util.";
        utilPrefixMap.put("DbCacheSize", new Utility(prefix, (args) -> {
            DbCacheSize.main(args);
        }));
        utilPrefixMap.put("DbDump", new Utility(prefix, (args) -> {
            DbDump.main(args);
        }));
        utilPrefixMap.put("DbDeleteReservedFiles",
                          new Utility(prefix, (args) -> {
                              DbDeleteReservedFiles.main(args);
                          }));
        utilPrefixMap.put("DbFilterStats", new Utility(prefix, (args) -> {
            DbFilterStats.main(args);
        }));
        utilPrefixMap.put("DbLoad", new Utility(prefix, (args) -> {
            DbLoad.main(args);
        }));
        utilPrefixMap.put("DbPrintLog", new Utility(prefix, (args) -> {
            DbPrintLog.main(args);
        }));
        utilPrefixMap.put("DbTruncateLog", new Utility(prefix, (args) -> {
            DbTruncateLog.main(args);
        }));
        utilPrefixMap.put("DbRunAction", new Utility(prefix, (args) -> {
            DbRunAction.main(args);
        }));
        utilPrefixMap.put("DbScavenger", new Utility(prefix, (args) -> {
            DbScavenger.main(args);
        }));
        utilPrefixMap.put("DbSpace", new Utility(prefix, (args) -> {
            DbSpace.main(args);
        }));
        utilPrefixMap.put("DbStat", new Utility(prefix, (args) -> {
            DbStat.main(args);
        }));
        utilPrefixMap.put("DbVerify", new Utility(prefix, (args) -> {
            DbVerify.main(args);
        }));
        utilPrefixMap.put("DbVerifyLog", new Utility(prefix, (args) -> {
            DbVerifyLog.main(args);
        }));

        /* The utilities in directory com/sleepycat/je/rep/util. */
        prefix = "com.sleepycat.je.rep.util.";
        utilPrefixMap.put("DbEnableReplication",
                          new Utility(prefix, (args) -> {
                              DbEnableReplication.main(args);
                          }));
        utilPrefixMap.put("DbGroupAdmin", new Utility(prefix, (args) -> {
            DbGroupAdmin.main(args);
        }));
        utilPrefixMap.put("DbPing", new Utility(prefix, (args) -> {
            DbPing.main(args);
        }));
        utilPrefixMap.put("VLSNIndexDump", new Utility(prefix, (args) -> {
            VLSNIndexDump.main(args);
        }));


        /* The utilities in directory com/sleepycat/je/rep/util/ldiff. */
        prefix = "com.sleepycat.je.rep.util.ldiff.";
        utilPrefixMap.put("LDiff", new Utility(prefix, (args) -> {
            LDiff.main(args);
        }));

        /* The utilities in directory com/sleepycat/je/rep/utilint. */
        prefix = "com.sleepycat.je.rep.utilint.";
        utilPrefixMap.put("DbDumpGroup", new Utility(prefix, (args) -> {
            DbDumpGroup.main(args);
        }));
        utilPrefixMap.put("DbFeederPrintLog",
                          new Utility(prefix, (args) -> {
                              DbFeederPrintLog.main(args);
                          }));
        utilPrefixMap.put("DbStreamVerify",
                          new Utility(prefix, (args) -> {
                              DbStreamVerify.main(args);
                          }));
        utilPrefixMap.put("DbSync", new Utility(prefix, (args) -> {
            DbSync.main(args);
        }));
        utilPrefixMap.put("DbRepRunAction",
                          new Utility(prefix, (args) -> {
                              DbRepRunAction.main(args);
                          }));
        utilPrefixMap.put("DbNullNode", new Utility(prefix, (args) -> {
            DbNullNode.main(args);
        }));
    }

    /* List all the available utilities. */
    private static String availableUtilities() {
        StringBuilder sbuf = new StringBuilder();
        for (String util : utilPrefixMap.keySet()) {
            sbuf.append(utilPrefixMap.get(util).getPrefix());
            sbuf.append(util);
            sbuf.append("\n");
        }

        return sbuf.toString();
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                usage("Missing utility name");
            }

            Utility utility = utilPrefixMap.get(args[0]);
            if (utility == null) {
                System.out.println("Available utilities are: ");
                System.out.println(availableUtilities());
                usage("No such utility");
                return;
            }

            String[] mainArgs = new String[args.length - 1];
            System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);

            utility.getWorker().work(mainArgs);
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            usage(e.toString());
        }
    }

    private static void usage(String msg) {
        System.err.println(msg);
        System.err.println(USAGE);
        System.exit(-1);
    }
}
