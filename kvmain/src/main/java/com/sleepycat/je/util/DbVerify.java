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

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ExtinctionFilter;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.util.verify.BtreeVerifier;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.util.ClassResolver;

/**
 * Verifies the internal structures of a database.
 *
 * <p>When using this utility as a command line program, and the
 * application uses custom key comparators, be sure to add the jars or
 * classes to the classpath that contain the application's comparator
 * classes. When the {@code -kv} option is specified the {@code
 * oracle.kv.impl.util.DbVerifyExtinctionFilter} class is also needed.</p>
 *
 * <p>To verify a database and write the errors to stream:</p>
 *
 * <pre>
 *    DbVerify verifier = new DbVerify(env, dbName, quiet);
 *    verifier.verify();
 * </pre>
 *
 * <p>WARNING: This program opens the JE environment read-only, runs recovery
 * as usual, and uses a Cursor to scan the Btree of the specified Database(s).
 * If the environment is also open in a normal application, i.e., a read-write
 * process, running this program may impact the application:
 * <ul>
 * <li>While this program is running, no cleaned files may be deleted by the
 *     application process, i.e, no disk space can be reclaimed.</li>
 * <li>Whatever memory is used by this process, it will not be available to the
 *     application process. Therefore, a small heap size for this program
 *     should normally be specified. However, for this program to run in a
 *     reasonable amount of time, a large enough heap should be given to hold
 *     at least a portion of the upper INs of the Btree.</li>
 * </ul>
 *
 * <p>DbVerify currently has several deficiencies when used with an environment
 * created by KVStore. The following issues can occur whether or not the
 * KVStore RN is running concurrently, e.g., when verifying backups:
 * <ul>
 * <li>[KVSTORE-655] False LSN error or ERASED_ENTRY error after a drop
 *     table, because DbVerify doesn't have table metadata.</li>
 * <li>[KVSTORE-657] ClassCastException is thrown rather than an LSN error for
 *     a dangling referenced to a MapLN.</li>
 * </ul>
 *
 * <p>In addition, when running DbVerify the following errors can occur when
 * the KVStore RN is running concurrently. These can be avoided by using the
 * KVStore verify-data plan instead.
 * <ul>
 * <li>[KVSTORE-666] False ENTRY_ERASED error when erasure is enabled, since
 *     erasure is not blocked by DbVerify.</li>
 * <li>[KVSTORE-667] Secondary indexes cannot be verified because they must be
 *     open by the application.</li>
 * </ul>
 */
public class DbVerify {

    private static final String usageString =
        "usage: " + CmdUtil.getJavaCommand(DbVerify.class) + "\n" +
        "      -h <dir>             # environment home directory\n" +
        "      [-q ]                # quiet, exit with success or failure\n" +
        "      [-cor ]              # corrupt secondary DB integrity \n" +
        "      [-s <databaseName>]  # database to verify\n" +
        "      [-v <interval>]      # progress notification interval\n" +
        "      [-bs <size>]         # how many records to check each batch\n" +
        "      [-d <millis>]        # delay in ms between batches\n" +
        "      [-kv]                # enable features for verifying a KV env\n" +
        "      [-vdr]               # verify data records (read LNs)\n" +
        "      [-vor]               # verify obsolete records (cleaner metadata)\n" +
        "      [-vos]               # verify offline secondary databases\n" +
        "      [-rdr]               # repair User data records (User Tree LNs)\n" +
        "      [-V]                 # print JE version number";

    private static final String KV_EXTINCTION_FILTER =
        "oracle.kv.impl.util.DbVerifyExtinctionFilter";

    private static final String KV_SECONDARY_INDEXES =
        "oracle.kv.impl.util.DbVerifySecondaryIndexes";

    File envHome = null;
    Environment env;
    String dbName = null;

    private boolean useKVFilter = false;
    private boolean offlineSecondaryVerification = false;

    private VerifyConfig verifyConfig = new VerifyConfig();
    private DatabaseConfig dbConfig = new DatabaseConfig();
    private Map<String, Database> allDBs = new HashMap<>();
    private Map<String, SecondaryConfig> keyCreatorLookup = new HashMap<>();

    /**
     * The main used by the DbVerify utility.
     *
     * @param argv The arguments accepted by the DbVerify utility.
     *
     * <pre>
     * usage: java { com.sleepycat.je.util.DbVerify | -jar
     * je-&lt;version&gt;.jar DbVerify }
     *             [-q] [-V] -s database -h dbEnvHome [-v intervalLNs]
     *             [-bs batchSize] [-d delayMs] [-vdr] [-vor] [-cor] [-vos]
     * </pre>
     *
     * <p>
     * -V   - show the version of the JE library.<br>
     * -s   - name of the database to verify; if omitted, verify all DBs<br>
     * -h   - the environment directory path name; required<br>
     * -q   - don't display database info or errors; default: false (display it)<br>
     * -v   - report intermediate statistics every specified LNs (Leaf Nodes);
     *        default: do not report stats<br>
     * -bs  - number of records to check each batch; default 1000<br>
     * -cor - corrupting the secondary integrity; default false<br>
     * -d   - the delay in ms between batches; default: no delay<br>
     * -kv  - enable features needed to correctly verify an offline environment
     *        created by KV<br>
     * -vdr - verify data records (fetch LNs if not cached); default: do not fetch<br>
     * -vor - verify obsolete records (cleaner metadata); default: do not verify<br>
     * </p>
     *
     * <p>Note that the DbVerify command line cannot be used to verify the
     * integrity of secondary databases, because this feature requires the
     * secondary databases to have been opened by the application. To verify
     * secondary database integrity, use {@link Environment#verify} or
     * {@link com.sleepycat.je.Database#verify} instead, from within the
     * application.</p>
     *
     * <p>When running DbVerify, trace logging of individual problems is not
     * enabled because the JE environment is opened read-only. To cause output
     * to System.err, the java.util.logging.config.file JVM property may be set
     * to the name of a file containing:
     *   <pre>com.sleepycat.je.util.ConsoleHandler.level=ALL</pre>
     * Note that rate-limited logging is not used by the verifier when it is
     * run via DbVerify. This ensures that all problems are logged for
     * debugging purposes, but may produce a large amount of output.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public static void main(String argv[])
        throws DatabaseException {

        final DbVerify verifier = new DbVerify();
        final VerifySummary summary = verifier.runVerify(argv, false);

        /*
         * Show the status, only omit if the user asked for a quiet run and
         * didn't specify a progress interval, in which case we can assume
         * that they really don't want any status output.
         *
         * If the user runs this from the command line, presumably they'd
         * like to see the status.
         */
        if (verifier.verifyConfig.getPrintInfo() ||
            (verifier.verifyConfig.getShowProgressInterval() > 0)) {
            System.err.println("Exit status = " + summary);
        }

        System.exit((summary != null && !summary.hasErrors()) ? 0 : -1);
    }

    /** Only publicly used in testing. */
    public VerifySummary runVerify(String argv[], boolean quiet) {
        parseArgs(argv);

        VerifySummary summary = null;
        try {
            openEnv();
            summary = verify(System.err);
            closeDbs();
            closeEnv();
            if (!quiet) {
                System.err.println(summary);
            }
        } catch (Throwable T) {
            T.printStackTrace(System.err);
        }
        return summary;
    }

    /** Only publicly used in testing. */
    public DbVerify() {
    }

    protected String getUsageString() {
        return usageString;
    }

    void printUsage(String msg) {
        System.err.println(msg);
        System.err.println(getUsageString());
        System.exit(-1);
    }

    void parseArgs(String argv[]) {
        verifyConfig.setPrintInfo(true);
        verifyConfig.setBatchDelay(0, TimeUnit.MILLISECONDS);

        int argc = 0;
        int nArgs = argv.length;
        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-q")) {
                verifyConfig.setPrintInfo(false);
            } else if (thisArg.equals("-V")) {
                System.out.println(JEVersion.CURRENT_VERSION);
                System.exit(0);
            } else if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-s")) {
                if (argc < nArgs) {
                    dbName = argv[argc++];
                } else {
                    printUsage("-s requires an argument");
                }
            } else if (thisArg.equals("-v")) {
                if (argc < nArgs) {
                    int progressInterval = Integer.parseInt(argv[argc++]);
                    if (progressInterval <= 0) {
                        printUsage("-v requires a positive argument");
                    }
                    verifyConfig.setShowProgressInterval(progressInterval);
                } else {
                    printUsage("-v requires an argument");
                }
            } else if (thisArg.equals("-bs")) {
                if (argc < nArgs) {
                    int batchSize = Integer.parseInt(argv[argc++]);
                    if (batchSize <= 0) {
                        printUsage("-bs requires a positive argument");
                    }
                    verifyConfig.setBatchSize(batchSize);
                } else {
                    printUsage("-bs requires an argument");
                }
            } else if (thisArg.equals("-d")) {
                if (argc < nArgs) {
                    long delayMs = Long.parseLong(argv[argc++]);
                    if (delayMs < 0) {
                        printUsage("-d requires a positive argument");
                    }
                    verifyConfig.setBatchDelay(delayMs, TimeUnit.MILLISECONDS);
                } else {
                    printUsage("-d requires an argument");
                }
            } else if (thisArg.equals("-cor")) {
                verifyConfig.setCorruptSecondaryDB(true);
            } else if (thisArg.equals("-vdr")) {
                verifyConfig.setVerifyDataRecords(true);
            } else if (thisArg.equals("-rdr")) {
                verifyConfig.setRepairDataRecords(true);
            } else if (thisArg.equals("-vor")) {
                verifyConfig.setVerifyObsoleteRecords(true);
            } else if (thisArg.equals("-c")) {
                System.err.println("WARNING: -c is no longer supported, use -vor");
            } else if (thisArg.equals("-vos")) {
                offlineSecondaryVerification = true;
            } else if (thisArg.equals("-kv")) {
                useKVFilter = true;
            } else {
                printUsage("Unknown arg: " + thisArg);
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }
    }

    void openEnv() throws ClassNotFoundException {
        if (env == null) {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setReadOnly(true);
            env = new Environment(envHome, envConfig);
            if (useKVFilter) {
                env = loadKV(envConfig);
            }
            if (offlineSecondaryVerification) {
                setSecAssoc(env);
            }
        }
    }
    
    void closeDbs() {
        try {
            for (Database db : allDBs.values()) {
                db.close();
            }
        } finally {
        }
    }

    void closeEnv() {
        try {
            if (env != null) {
                env.close();
            }
        } finally {
            env = null;
        }
    }

    private VerifySummary verify(PrintStream out) {

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final BtreeVerifier verifier;
        if (!offlineSecondaryVerification) {
            verifier = new BtreeVerifier(envImpl,
                                         true /*disableRateLimitedLogging*/);
        } else {
            verifier = new BtreeVerifier(envImpl, true,
                                         dbConfig.getSecondaryAssociation(),
                                         keyCreatorLookup,
                                         verifyConfig.getCorruptSecondaryDB(),
                                         offlineSecondaryVerification);
        }
        verifyConfig.setShowProgressStream(out);
        verifier.setBtreeVerifyConfig(verifyConfig);

        if (dbName == null) {
            return verifier.verifyAll();
        } else {
            /* Get DB ID from name. */
            BasicLocker locker =
                BasicLocker.createBasicLocker(envImpl, false /*noWait*/);
            final DbTree dbTree = envImpl.getDbTree();
            DatabaseImpl dbImpl = null;
            DatabaseId dbId;

            try {
                dbImpl = dbTree.getDb(locker, dbName, null, false);
                if (dbImpl == null) {
                    return null;
                }
                dbId = dbImpl.getId();
            } finally {
                dbTree.releaseDb(dbImpl);
                locker.operationEnd();
            }

            return verifier.verifyDatabase(dbId);
        }
    }

    /*
     * Creating the DbVerifyExtinctionFilter for KV requires an open
     * environment, but configuring the ExtinctionFilter has to be done before
     * opening the Environment to avoid concurrency issues.  The only way
     * around this is to open the environment, create the
     * DbVerifyExtinctionFilter, close the environment, configure the
     * ExtinctionFilter, then reopen the environment.
     */
    private Environment loadKV(EnvironmentConfig envConfig) {
        ExtinctionFilter filter = null;
        final Class<?>[] args = new Class[] { Environment.class };
        try {
            Class<?> filterClass =
                ClassResolver.resolveClass(KV_EXTINCTION_FILTER, null);
            filter = (ExtinctionFilter)
                filterClass.getConstructor(args).newInstance(env);

        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            boolean throwException = true;
            /*
             * The TableMetadata database does not exist.  This can be valid
             * if this is a brand new environment.
             */
            if (cause != null && cause instanceof DatabaseNotFoundException) {
                EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
                if (envImpl.getFileManager().getCurrentFileNum() == 0) {
                    useKVFilter = false;
                    offlineSecondaryVerification = false;
                    LoggerUtils.warning(envImpl.getLogger(), envImpl,
                        "Skipping loading KV extinction filter, " +
                        "TableMetadata database has not been created yet.");
                    throwException = false;
                }
            }
            if (throwException) {
                throw new IllegalArgumentException("An error was encountered" +
                    " while loading DbVerifyExtictionFilter.  Cause: " +
                    e.getCause());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Unable to load the DbVerifyExtinctionFilter class, check " +
                "that the kvstore.jar is in the classpath.  Exception: " + e);
        }
        closeEnv();
        envConfig.setExtinctionFilter(filter);
        return new Environment(envHome, envConfig);
    }

    private void setSecAssoc(Environment env) {
        SecondaryAssociation secAssoc;
        try {
            final Class<?>[] secArgs = new Class[] { Environment.class,
                                                     Map.class, Map.class };
            Class<?> secAssocClass =
                ClassResolver.resolveClass(KV_SECONDARY_INDEXES, null);
            String pattern = "p(\\d{1,5})$";
            Pattern r = Pattern.compile(pattern);
            dbConfig.setReadOnly(true);
            dbConfig.setCacheMode(CacheMode.EVICT_LN);
            for (String dbName : env.getDatabaseNames()) {
                if (r.matcher(dbName).find()) {
                    allDBs.put(dbName, env.openDatabase(null, dbName,
                                                        dbConfig));
                }
            }
            secAssoc = (SecondaryAssociation)
                secAssocClass.getConstructor(secArgs).newInstance(env, allDBs,
                                                            keyCreatorLookup);
            dbConfig.setSecondaryAssociation(secAssoc);
            for (String dbName : env.getDatabaseNames()) {
                if (dbName.split("\\.").length >=2) {
                    final SecondaryConfig secConfig = new SecondaryConfig();
                    secConfig.setSecondaryAssociation(
                        dbConfig.getSecondaryAssociation()).
                        setReadOnly(true).setSortedDuplicates(true);
                    allDBs.put(dbName, env.openSecondaryDatabase(null, dbName,
                                null, secConfig));
                }
            }
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("An error was encountered" +
                    " while loading DbVerifySecondaryIndexes.  Cause: " +
                    e.getCause());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Unable to load the DbVerifySecondaryIndexes class, check "
                    + "that the kvstore.jar is in the classpath.  Exception: " 
                    + e);
        }
    }
}
