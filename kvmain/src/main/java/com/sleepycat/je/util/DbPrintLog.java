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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.File;
import java.lang.reflect.Constructor;

import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.DumpFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.PrintFileReader;
import com.sleepycat.je.log.StatsFileReader;
import com.sleepycat.je.log.VLSNDistributionReader;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Dumps the contents of the log in XML format to System.out.
 *
 * <p>To print an environment log:</p>
 *
 * <pre>
 *      DbPrintLog.main(argv);
 * </pre>
 */
public class DbPrintLog {

    /**
     * Dump a JE log into human readable form.
     */
    public void dump(File envHome,
                     String entryTypes,
                     String txnIds,
                     long startLsn,
                     long endLsn,
                     boolean verbose,
                     boolean stats,
                     boolean repEntriesOnly,
                     boolean csvFormat,
                     boolean forwards,
                     boolean vlsnDistribution,
                     String customDumpReaderClass,
                     boolean doChecksumOnRead)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException {

        dump(envHome, entryTypes, "" /* dbIds */, txnIds, "", "", startLsn, endLsn,
             NULL_VLSN, NULL_VLSN, false, false, verbose, stats,
             repEntriesOnly, csvFormat, forwards, vlsnDistribution,
             customDumpReaderClass, doChecksumOnRead);
    }

    private void dump(File envHome,
                     String entryTypes,
                     String dbIds,
                     String txnIds,
                     String tbIds,
                     String rgIds,
                     long startLsn,
                     long endLsn,
                     long startVLSN,
                     long endVLSN,
                     boolean elideLN,
                     boolean elideIN,
                     boolean verbose,
                     boolean stats,
                     boolean repEntriesOnly,
                     boolean csvFormat,
                     boolean forwards,
                     boolean vlsnDistribution,
                     String customDumpReaderClass,
                     boolean doChecksumOnRead) {
        EnvironmentImpl env = null;
        if (tbIds != null || rgIds != null) {
            env = CmdUtil.makeUtilityEnvironment(envHome, true, true);
        } else {
            env = CmdUtil.makeUtilityEnvironment(envHome, true, false);
        }

        /*
         * If either doChecksumOnRead is false or
         * env.getLogManager().getChecksumOnRead() is false,
         * checksum validation will be skipped during read operations.
         */
        doChecksumOnRead = env.getLogManager().getChecksumOnRead() &&
            doChecksumOnRead;

        FileManager fileManager = env.getFileManager();
        fileManager.setIncludeDeletedFiles(true);
        int readBufferSize =
            env.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /* Configure the startLsn and endOfFileLsn if reading backwards. */
        long endOfFileLsn = DbLsn.NULL_LSN;
        if (startLsn == DbLsn.NULL_LSN && endLsn == DbLsn.NULL_LSN &&
            !forwards) {
            LastFileReader fileReader =
                new LastFileReader(env, readBufferSize);
            while (fileReader.readNextEntry()) {
            }
            startLsn = fileReader.getLastValidLsn();
            endOfFileLsn = fileReader.getEndOfLog();
        }

        try {

            /* 
             * Make a reader. First see if a custom debug class is available,
             * else use the default versions. 
             */
            DumpFileReader reader = null;
            if (customDumpReaderClass != null) {

                reader = getDebugReader(
                    customDumpReaderClass, env, readBufferSize,
                    startLsn, endLsn, endOfFileLsn, entryTypes, txnIds,
                    verbose, repEntriesOnly, forwards, doChecksumOnRead);

            } else {
                if (stats) {

                    reader = new StatsFileReader(
                        env, readBufferSize, startLsn, endLsn, endOfFileLsn,
                        entryTypes, dbIds, txnIds, verbose, repEntriesOnly,
                        forwards, doChecksumOnRead);

                } else if (vlsnDistribution) {

                    reader = new VLSNDistributionReader(
                        env, readBufferSize, startLsn, endLsn, endOfFileLsn,
                        verbose, forwards, doChecksumOnRead);
                } else {

                    reader = new PrintFileReader(env, readBufferSize, startLsn,
                                                 endLsn, startVLSN, endVLSN,
                                                 elideLN, elideIN,
                                                 endOfFileLsn, entryTypes,
                                                 dbIds, txnIds, tbIds, rgIds,
                                                 verbose, repEntriesOnly,
                                                 forwards, doChecksumOnRead);
                }
            }

            /* Enclose the output in a tag to keep proper XML syntax. */
            if (!csvFormat) {
                System.out.println("<DbPrintLog>");
            }

            while (reader.readNextEntry()) {
            }

            reader.summarize(csvFormat);
            if (!csvFormat) {
                System.out.println("</DbPrintLog>");
            }
        } finally {
            env.close();
        }
    }

    /**
     * The main used by the DbPrintLog utility.
     *
     * @param argv An array of command line arguments to the DbPrintLog
     * utility.
     *
     * <pre>
     * usage: java { com.sleepycat.je.util.DbPrintLog | -jar
     * je-&lt;version&gt;.jar DbPrintLog }
     *  -h &lt;envHomeDir&gt;
     *  -s  &lt;start file number or LSN, in hex&gt;
     *  -e  &lt;end file number or LSN, in hex&gt;
     *  -k  &lt;binary|hex|text|obfuscate&gt; (format for dumping the key/data)
     *  -db &lt;targeted db ids, comma separated&gt;
     *  -tx &lt;targeted txn ids, comma separated&gt;
     *  -ty &lt;targeted entry types, comma separated&gt;
     *  -S  show summary of log entries
     *  -SC show summary of log entries in CSV format
     *  -r  only print replicated log entries
     *  -b  scan log backwards. The entire log must be scanned, cannot be used
     *      with -s or -e
     *  -q  if specified, concise version is printed,
     *      default is verbose version
     *  -c  &lt;name of custom dump reader class&gt; if specified, DbPrintLog
     *      will attempt to load a class of this name, which will be used to
     *      process log entries. Used to customize formatting and dumping when
     *      debugging files.
     *  -tb only print log entries that reside in a specific table, if no
     *      targeted ids are provided, then print log entry
     *      attached with table id if it has one
     *  -rg only print log entries that reside in a specific table, if no
     *      targeted ids are provided, then print log entry
     *      attached with region id if it has one
     *  -kv short for specifying -tb and -rg at the same time
     *  -dc disable checksum validation, print out logs even with a
     *      malformed checksum
     * </pre>
     *
     * <p>All arguments are optional.  The current directory is used if {@code
     * -h} is not specified.</p>
     */
    public static void main(String[] argv) {
        try {
            int whichArg = 0;
            String entryTypes = null;
            String dbIds = null;
            String txnIds = null;
            long startLsn = DbLsn.NULL_LSN;
            long endLsn = DbLsn.NULL_LSN;
            long startVLSN = NULL_VLSN;
            long endVLSN = NULL_VLSN;
            boolean elideLN = false;
            boolean elideIN = false;
            boolean verbose = true;
            boolean stats = false;
            boolean csvFormat = false;
            boolean repEntriesOnly = false;
            boolean forwards = true;
            String customDumpReaderClass = null;
            String tbIds = null;
            String rgIds = null;
            boolean vlsnDistribution = false;
            boolean doChecksumOnRead = true;

            /* Default to looking in current directory. */
            File envHome = new File(".");
            Key.DUMP_TYPE = DumpType.BINARY;

            while (whichArg < argv.length) {
                String nextArg = argv[whichArg];
                if (nextArg.equals("-h")) {
                    whichArg++;
                    envHome = new File(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-ty")) {
                    whichArg++;
                    entryTypes = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-db")) {
                    whichArg++;
                    dbIds = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-tx")) {
                    whichArg++;
                    txnIds = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-s")) {
                    whichArg++;
                    startLsn = CmdUtil.readLsn(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-e")) {
                    whichArg++;
                    endLsn = CmdUtil.readLsn(CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-vstart")) {
                    whichArg++;
                    startVLSN = CmdUtil.readLongNumber(
                        CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-vend")) {
                    whichArg++;
                    endVLSN = CmdUtil.readLongNumber(
                        CmdUtil.getArg(argv, whichArg));
                } else if (nextArg.equals("-k")) {
                    whichArg++;
                    String dumpType = CmdUtil.getArg(argv, whichArg);
                    if (dumpType.equalsIgnoreCase("text")) {
                        Key.DUMP_TYPE = DumpType.TEXT;
                    } else if (dumpType.equalsIgnoreCase("hex")) {
                        Key.DUMP_TYPE = DumpType.HEX;
                    } else if (dumpType.equalsIgnoreCase("binary")) {
                        Key.DUMP_TYPE = DumpType.BINARY;
                    } else if (dumpType.equalsIgnoreCase("obfuscate")) {
                        Key.DUMP_TYPE = DumpType.OBFUSCATE;
                    } else {
                        System.err.println
                            (dumpType +
                             " is not a supported dump format type.");
                    }
                } else if (nextArg.equals("-q")) {
                    verbose = false;
                } else if (nextArg.equals("-b")) {
                    forwards = false;
                } else if (nextArg.equals("-S")) {
                    stats = true;
                } else if (nextArg.equals("-SC")) {
                    stats = true;
                    csvFormat = true;
                } else if (nextArg.equals("-r")) {
                    repEntriesOnly = true;
                } else if (nextArg.equals("-elideLN")) {
                    elideLN = true;
                } else if (nextArg.equals("-elideIN")) {
                    elideIN = true;
                } else if (nextArg.equals("-c")) {
                    whichArg++;
                    customDumpReaderClass = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-vd")) {
                    /* 
                     * An unadvertised option which displays vlsn distribution
                     * in a log, for debugging.
                     */
                    vlsnDistribution = true;
                } else if (nextArg.equals("-tb")) {
                    whichArg++;
                    /*
                     * if -tb is the last flag then no table id is specified
                     */
                    if (whichArg == argv.length) {
                        whichArg --;
                        tbIds = DumpFileReader.NO_VALUE;
                    } else {
                        tbIds = CmdUtil.getArg(argv, whichArg);
                        /*
                         * no table id is specified, tbIds.charAt(0) == '-'
                         * means next potential flag is being reading in
                         */
                        if (tbIds.charAt(0) == '-'
                            && Character.isLetter(tbIds.charAt(1))) {
                            whichArg --;
                            tbIds = DumpFileReader.NO_VALUE;
                        }
                    }
                } else if(nextArg.equals("-rg")) {
                    whichArg++;
                    /*
                     * if -rg is the last flag then no rg id is specified
                     */
                    if (whichArg == argv.length) {
                        whichArg --;
                        rgIds = DumpFileReader.NO_VALUE;
                    } else {
                        rgIds = CmdUtil.getArg(argv, whichArg);
                        /*
                         * no table id is specified, tbIds.charAt(0) == '-'
                         * means next potential flag is being reading in
                         */
                        if (rgIds.charAt(0) == '-' &&
                            Character.isLetter(tbIds.charAt(1))) {
                            whichArg--;
                            rgIds = DumpFileReader.NO_VALUE;
                        }
                    }
                } else if (nextArg.equals("-kv")) {
                    rgIds = rgIds == null ? DumpFileReader.NO_VALUE : rgIds;
                    tbIds = tbIds == null ? DumpFileReader.NO_VALUE : tbIds;
                } else if (nextArg.equals("-dc")) {
                    doChecksumOnRead = false;
                } else {
                    System.err.println
                        (nextArg + " is not a supported option.");
                    usage();
                    System.exit(-1);
                }
                whichArg++;
            }

            /* Don't support scan backwards when -s or -e is enabled. */
            if ((startLsn != DbLsn.NULL_LSN || endLsn != DbLsn.NULL_LSN ||
                 startVLSN != NULL_VLSN || endVLSN != NULL_VLSN) &&
                !forwards) {
                throw new UnsupportedOperationException
                    ("Backwards scans are not supported when -s or -e or " +
                     "-vstart or -vend are used. They can only be used " +
                     "against the entire log.");
            }

            if ((startVLSN != NULL_VLSN || endVLSN != NULL_VLSN) &&
                customDumpReaderClass != null) {
                throw new UnsupportedOperationException
                ("VLSN range can not be specified for custom reader class.");
            }

            DbPrintLog printer = new DbPrintLog();
            printer.dump(envHome, entryTypes, dbIds, txnIds, tbIds, rgIds, startLsn, endLsn,
                         startVLSN, endVLSN, elideLN, elideIN, verbose, stats,
                         repEntriesOnly, csvFormat, forwards, vlsnDistribution,
                         customDumpReaderClass, doChecksumOnRead);

        } catch (Throwable e) {
            System.out.println(e.getMessage());
            usage();
            System.exit(1);
        }
    }

    private static void usage() {
        System.out.println("Usage: " +
                           CmdUtil.getJavaCommand(DbPrintLog.class));
        System.out.println(" -h  <envHomeDir>");
        System.out.println(" -s  <start file number or LSN, in hex>");
        System.out.println(" -e  <end file number or LSN, in hex>");
        System.out.println(" -vstart  <start VLSN, in hex>");
        System.out.println(" -vend  <end VLSN, in hex>");
        System.out.println(" -k  <binary|text|hex|obfuscate> " +
                           "(format for dumping the key and data)");
        System.out.println(" -db <targeted db ids, comma separated>");
        System.out.println(" -tx <targeted txn ids, comma separated>");
        System.out.println(" -ty <targeted entry types, comma separated>");
        System.out.println(" -S  show Summary of log entries");
        System.out.println(" -SC show Summary of log entries in CSV format");
        System.out.println(" -r  only print replicated log entries");
        System.out.println(" -elideLN  elide the LN data");
        System.out.println(" -elideIN  elide the IN slots");
        System.out.println(" -b  scan all the log files backwards, don't ");
        System.out.println("     support scan between two log files");
        System.out.println(" -q  if specified, concise version is printed");
        System.out.println("     Default is verbose version.)");
        System.out.println(" -c  <custom dump reader class> if specified, ");
        System.out.println("     attempt to load this class to use for the ");
        System.out.println("     formatting of dumped log entries");
        System.out.println(" -tb <targeted table ids, comma separated, " +
            "if no targeted ids are provided, then print log entry attached " +
            "with table id if it has one>");
        System.out.println(" -rg <targeted region ids, comma separated, " +
            "if no targeted ids are provided, then print log entry attached " +
            "with region id if it has one>");
        System.out.println(" -kv short for specifying -tb and -rg at the same" +
            "time with no targeted ids provided");
        System.out.println(" -dc disable checksum validation, print out logs" +
            " even with a malformed checksum");
        System.out.println("All arguments are optional");
    }

    /**
     * If a custom dump reader class is specified, we'll use that for 
     * DbPrintLog instead of the regular DumpFileReader. The custom reader must
     * have DumpFileReader as a superclass ancestor. Its constructor must have
     * this signature:
     *
     *  public class FooReader extends DumpFileReader {
     *
     *      public FooReader(EnvironmentImpl env,
     *                       Integer readBufferSize, 
     *                       Long startLsn,
     *                       Long finishLsn,
     *                       Long endOfFileLsn,
     *                       String entryTypes,
     *                       String txnIds,
     *                       Boolean verbose,
     *                       Boolean repEntriesOnly,
     *                       Boolean forwards) 
     *          super(env, readBufferSize, startLsn, finishLsn, endOfFileLsn,
     *                entryTypes, txnIds, verbose, repEntriesOnly, forwards);
     *
     * See com.sleepycat.je.util.TestDumper, on the test side, for an example.
     */
    private DumpFileReader getDebugReader(String customDumpReaderClass,
                                          EnvironmentImpl env,
                                          int readBufferSize,
                                          long startLsn,
                                          long finishLsn,
                                          long endOfFileLsn,
                                          String entryTypes,
                                          String txnIds,
                                          boolean verbose,
                                          boolean repEntriesOnly,
                                          boolean forwards,
                                          boolean doChecksumOnRead) {
        Class<?> debugClass = null;
        try {
            debugClass = Class.forName(customDumpReaderClass);
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("-c was specified, but couldn't load " +
                 customDumpReaderClass + " ", e);
        }

        Class<?> args[] = { EnvironmentImpl.class,
                            Integer.class,     // readBufferSize
                            Long.class,        // startLsn
                            Long.class,        // finishLsn
                            Long.class,        // endOfFileLsn
                            String.class,      // entryTypes
                            String.class,      // txnIds
                            Boolean.class,     // verbose
                            Boolean.class,     // repEntriesOnly
                            Boolean.class,     // forwards
                            Boolean.class };   // doChecksumOnRead

        DumpFileReader debugReader = null;
        try {
            Constructor<?> con = 
                debugClass.getConstructor(args);
            debugReader = (DumpFileReader) con.newInstance(env,
                                                           readBufferSize,
                                                           startLsn,
                                                           finishLsn,
                                                           endOfFileLsn,
                                                           entryTypes,
                                                           txnIds,
                                                           verbose,
                                                           repEntriesOnly,
                                                           forwards,
                                                           doChecksumOnRead);
        } catch (Exception e) {
            throw new IllegalStateException
                ("-c was specified, but couldn't instantiate " + 
                 customDumpReaderClass + " ", e);
        }

        return debugReader;
    }
}
