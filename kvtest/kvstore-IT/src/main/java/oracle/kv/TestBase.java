/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.util.TestUtils.NULL_PRINTSTREAM;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.impl.api.table.TableSysTableUtil;
import oracle.kv.impl.security.util.FileSysUtils;
import oracle.kv.impl.systables.TableMetadataDesc;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.xregion.XRegionTestBase;
import oracle.kv.impl.xregion.XRegionTestBase.ReqRespThread;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestTimedOutException;

/**
 * The base class for all KV unit tests.
 *
 * When writing or debugging unit tests which use tables there are a few
 * things to be aware of.
 *
 * System Tables
 *
 * There have been many problems with the unit tests due to changes in store
 * startup. Many of the failures are due to a test expecting that a system
 * table exist, but doesn't. If a test depends on one or more system tables
 * it should first wait for the table(s) to be created. There is a utility
 * method to wait for a named table to exist.
 *
 * For example, a test that creates a table with an identity columns should
 * wait for the supporting system table:
 *
 *      waitForTable(tableAPI, SGAttributesTableDesc.TABLE_NAME);
 *
 * Table Metadata System Table
 *
 * In addition to the Admin's own database, the table metadata is in a
 * system table (MD table). Currently, this only affects the client library
 * (TableAPI), and only after the MD table is initialized. To explicitly test
 * the client's use of the MD table the following should be added to the
 * beginning of the test:
 *
 *      tableAPI.setEnableTableMDSysTable(true);
 *      waitForTableMDSysTable(tableAPI);
 *
 * This will make sure that 1) the MD Table is created and initialized and
 * 2) that the client library will use it. Note that the MD table is used for
 * the various getTable() methods, as well as getRegionMapper() and other
 * metadata access methods. It is not used for data operations.
 *
 * Conversely to test a client using the old scheme (getting MD from
 * directly from an RN) disable the client's use of the MD table by calling:
 *
 *      tableAPI.setEnableTableMDSysTable(false);
 *
 * Note that the utility method to wait for the MD table is not the same the
 * generic wait-for-table. This is because it must wait for the MD table to
 * be created AND initialized.
 *
 * Warning! There is a hazard when calling waitForTableMDSysTable() which may
 * result in the test timing out.
 *
 * Tables Only Mode
 *
 * By default, the Admin will not write to system tables unless user table
 * metadata is present. This is to avoid key-space collisions when the store
 * is used solely for Key/Value operations.
 *
 * This means that if a test starts a store and then immediately calls
 * waitForTableMDSysTable() it will time out. As mentioned above, this call
 * will wait for the Admin to initialize the MD table, which it will not do
 * unless there is user table MD.
 *
 * Two options: The test could create a table, namespace, or region
 * and then call waitForTableMDSysTable().
 *
 * Another option is to use the Admin.isStoreReady() method passing in true.
 * This will enable system table writes. The waitForStoreReady() method uses
 * this API to enable system table writes and waits for the system tables to
 * be created and initialized.
 */
public abstract class TestBase {

    /**
     * Enable the security test mode if the property value if true.
     */
    public static final boolean SECURITY_ENABLE =
        Boolean.getBoolean("test.security.enable");

    /**
     * Contains explicit parameter values to be used when running parameterized
     * tests as a way of narrowing the set of tests run.  If the property is
     * specified with a non-empty value, the value is treated as a comma
     * separated list of the parameter values to use tests.  Values "true" or
     * "false" are converted to type Boolean.  All other values are left as
     * Strings.  If null, no override was specified.
     */
    public static List<Object[]> PARAMS_OVERRIDE =
        getParamsOverride(System.getProperty("testcase.params"));

    /**
     * The default number of milliseconds after which a test class should
     * timeout, used for classes that do not have {@link
     * TestClassTimeoutMillis} annotations.
     */
    public static final long DEFAULT_CLASS_TIMEOUT_MILLIS =
        20 * 60 * 1000; /* 20 minutes */

    /**
     * A multiplier that can be specified to expand or contract the test class
     * timeout requirements for all tests. If you are running unit tests on a
     * slow system, you can increase all timeouts proportionally by setting the
     * value of this system property to a value greater than 1.0. If you always
     * want to run tests this way, you can add the setting to the value of
     * {@code ANT_OPTS} in the {@code $HOME/.antrc} file.
     */
    public static final double TEST_CLASS_TIMEOUT_MULTIPLIER =
        Double.parseDouble(
            System.getProperty("test.class.timeout.multiplier", "1.0"));

    /**
     * A boolean that tells the test watcher to indicate when a test
     * case starts and ends, and how long it took.
     */
    protected static boolean TEST_TRACE = Boolean.getBoolean("test.trace");

    /**
     * Whether to copy environment files after a successful test, for
     * debugging
     */
    private static final boolean COPY_ON_SUCCESS =
        Boolean.getBoolean("test.copy.on.success");

    /**
     * A string containing the fully qualified names of classes and methods,
     * separated by commas or whitespace, that are considered undependable.
     */
    private static final String undependableTests =
        System.getProperty("test.undependable", "");

    /**
     * Whether to enable the system standard output and
     * error output produced by tests.
     */
    private static final boolean SYS_OUTPUT_ENABLE =
       Boolean.getBoolean("test.sys.output.enable");

    /**
     * A set of the fully qualified names of classes and methods that are
     * considered undependable.
     */
    private static final Set<String> undependableSet =
        Stream.of(undependableTests.split("[, \\s]+"))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());

    /** Whether to skip undependable tests */
    private static final boolean skipUndependable =
        Boolean.getBoolean("test.undependable.skip");

    /**
     * If set to more than 1, repeat the test that number of times.
     */
    private static final int REPEAT = Integer.getInteger("testcase.repeat",
                                                        1);
    static {
        if (REPEAT > 1) {
            System.out.println("Test repeat count: " + REPEAT);
        }
    }

    /** Perform test class timeout checks. */
    @ClassRule
    public static TestRule globalTimeout = (base, desc) -> {
        try {
            long timeout = getClassTimeoutMillis(desc);
            if (REPEAT > 1) {
                timeout *= REPEAT;
            }
            TimeUnit unit = MILLISECONDS;

            // Convert to eye friendly time unit
            if (timeout % (1000 * 60) == 0) {
                timeout = MINUTES.convert(timeout, unit);
                unit = MINUTES;
            } else if (timeout % 1000 == 0) {
                timeout = SECONDS.convert(timeout, unit);
                unit = SECONDS;
            }
            return Timeout.builder()
                .withTimeout(timeout, unit)
                .withLookingForStuckThread(false)
                .build().apply(base, desc);
        } catch (final Exception e) {
            return new Statement() {
                @Override
                public void evaluate() {
                    throw new RuntimeException(
                        "Invalid parameters for Timeout", e);
                }
            };
        }
    };

    /**
     * The class level watcher.
     * This rule is primarily to copy the testing environment to
     * another place for future investigation. This is targeting the
     * {@code globalTimeOut} Class Rule, it only handles TestTimedOutException
     */
    @ClassRule(order = Integer.MIN_VALUE)
    public static final TestRule classWatcher = new TestWatcher() {
        @Override
        protected void failed(Throwable t, Description description) {
            super.failed(t, description);
            if (t instanceof TestTimedOutException) {
                String dirName = description.getClassName();
                try {
                    copyEnvironments(dirName);
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Can't copy env dir to " + dirName
                                + " after failure", e);
                }
            }
        }
    };

    /**
     * Method level watcher rule.
     * The rule we use to control every test case, this rule is primarily to
     * copy the testing environment, files, sub directories to another place
     * for future investigation, if any of test failed. But we do have a limit
     * to control how many times we copy because of disk space. So once the
     * failure counter exceed limit, it won't copy the environment any more.
     */
    public final TestRule watchman = new TestWatcher() {

        long startTimeMs;

        /* Copy Environments when the test failed. */
        @Override
        protected void failed(Throwable t, Description desc) {
            if (TEST_TRACE) {
                final long endTimeMs = System.currentTimeMillis();
                final double testTimeSec = (endTimeMs - startTimeMs) / 1000.0;
                System.out.format(
                    FormatUtils.formatDateTimeMillis(endTimeMs) +
                    ": Test failed: %.3f sec\n", testTimeSec);
            }
            String dirName = makeFileName(desc);
            try {
                copyEnvironments(dirName);
            } catch (Exception e) {
                throw new RuntimeException(
                   "Can't copy env dir to " + dirName  + " after failure", e);
            }
        }

        @Override
        protected void succeeded(Description desc) {
            if (TEST_TRACE) {
                final long endTimeMs = System.currentTimeMillis();
                final double testTimeSec = (endTimeMs - startTimeMs) / 1000.0;
                System.out.format(
                    FormatUtils.formatDateTimeMillis(endTimeMs) +
                    ": Test passed: %.3f sec\n", testTimeSec);
            }
            if (COPY_ON_SUCCESS) {
                String dirName = makeFileName(desc);
                try {
                    copyEnvironments(dirName);
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Can't copy env dir to " + dirName  + " after success",
                        e);
                }
            }
        }

        @Override
        protected void skipped(AssumptionViolatedException e,
                               Description desc) {
            if (TEST_TRACE) {
                final long endTimeMs = System.currentTimeMillis();
                System.out.println(
                    FormatUtils.formatDateTimeMillis(endTimeMs) +
                    ": Test skipped");
            }
        }

        @Override
        protected void starting(Description description) {
            if (TEST_TRACE) {
                startTimeMs = System.currentTimeMillis();
                System.out.println(
                    FormatUtils.formatDateTimeMillis(startTimeMs) +
                    ": Test starts: " + description.getMethodName());
            }
        }
    };

    /** Provides the name of the current test. */
    public final TestName testName = new TestName();

    /**
     * The default name for a KVStore used in a test, or null when not in a
     * test method.
     */
    protected volatile String kvstoreName;

    protected static final String LOCAL_REGION = "TARGET";

    public static final String REMOTE_REGION = "SOURCE";

    public static final String MR_TABLE_MODE_PROP = "test.mrtable";

    /**
     * If true, requests that tests be run using tables created in multi-region
     * mode, so that standard table tests can provide some test coverage for
     * multi-region tables.
     */
    public static final boolean DEFAULT_MR_TABLE_MODE =
        Boolean.parseBoolean(System.getProperty(MR_TABLE_MODE_PROP));

    /** Whether tables should run in multi-region table mode. */
    public static boolean mrTableMode = DEFAULT_MR_TABLE_MODE;

    /* mock stream manager for multi-region table mode. */
    protected static ReqRespThread reqRespThread;

    /* Used to cache system standard output and error output */
    private static PrintStream stdOut;
    private static PrintStream stdErr;

    /**
     * Set to true if the current test method is considered undependable.
     */
    protected boolean undependable;

    /**
     * For any logging that the test wants to do in instance methods. Note that
     * some tests may require static loggers and should create them as needed.
     */
    protected final Logger logger = getLogger();

    /**
     * Operations that will be called by tearDown
     */
    protected final List<Runnable> tearDowns = new ArrayList<>();

    /**
     * Check if the test is undependable and skip it if requested. If running
     * an undependable test, wrap exceptions in ones that mark them as
     * undependable.
     */
    public final TestRule checkUndependable = (base, desc) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            final String testMethodName =
                testClassName + "." + desc.getMethodName();
            undependable = undependableSet.contains(testMethodName) ||
                undependableSet.contains(testClassName);
            final boolean skip = undependable && skipUndependable;
            assumeFalse("Undependable test", skip);
            if (!undependable) {
                base.evaluate();
            } else {
                try {
                    base.evaluate();
                } catch (AssumptionViolatedException e) {
                    throw e;    /* No need to mark assumptions */
                } catch (AssertionError e) {
                    throw new AssertionError(
                        "Undependable test failed: " + e, e);
                } catch (Throwable e) {
                    throw new RuntimeException(
                        "Undependable test caused error: " + e, e);
                }
            }
        }
    };

    /**
     * Define a class rule to skip the entire class if it is marked as
     * undependable and undependable tests are being skipped. The class level
     * check is important because it prevents performing static test setup when
     * the test is being skipped.
     */
    @ClassRule
    public static TestRule skipUndependableClass = (base, desc) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            final boolean skip = skipUndependable &&
                undependableSet.contains(desc.getClassName());
            if (skip) {
                /*
                 * Print a message, since assumeFalse seems to not
                 * include one for a class-level assumption failure.
                 */
                System.out.println("SKIPPED: Undependable test: " +
                    desc.getClassName());
            }
            assumeFalse("Undependable test", skip);
            base.evaluate();
        }
    };

    /**
     * The name of the test class in case it is needed for static
     * initialization elsewhere.
     */
    protected volatile static String testClassName;

    /** Initialize testClassName */
    @ClassRule
    public static TestRule initTestClassName = (base, desc) -> {
        testClassName = desc.getClassName();
        return base;
    };

    /** Repeat the test if requested. */
    public final TestRule repeatRule = (base, desc) -> {
        if (REPEAT <= 1) {
            return base;
        }
        return new Statement() {
            @Override
            public void evaluate() {
                for (int i = 0; i < REPEAT; i++) {
                    try {
                        base.evaluate();
                    } catch (Throwable t) {
                        throw new RuntimeException(
                            "Test failure at repeat count " + i + ": " + t,
                            t);
                    }
                }
            }
        };
    };

    /**
     * Mark that a test is active.
     */
    @BeforeClass
    public static void setTestStatusActive() {
        TestStatus.setActive(true);
    }

    @AfterClass
    public static void stopMRTableService() {
        if (reqRespThread != null) {
            reqRespThread.stopResResp();
        }
    }

    /**
     * Control the order of test rules. Use the same order as the rules appear
     * in the file, for clarity.
     */
    @Rule
    public final TestRule chain =
        /*
         * Watchman needs to be the outermost rule because it prints
         * information about success and failure as well as total times
         */
        RuleChain.outerRule(watchman)
        /* Making this next means that other rules can refer to it */
        .around(testName)
        /* Filter out undependable tests before repeating them */
        .around(checkUndependable)
        .around(repeatRule);

    @Before
    public void setUp()
        throws Exception {

        FileSysUtils.USE_TEST_WIN_FILE_OPERATIONS = true;
        clearTestDirectory();
        kvstoreName =
            "kvtest-" + getClass().getName() + "-" + testName.getMethodName()
            /* Filter out illegal characters */
            .replaceAll("[^-_.a-zA-Z0-9]", "-");
    }

    /** Returns the logger to store in the logger field. */
    protected Logger getLogger() {
        return ClientLoggerUtils.getLogger(getClass(), "test");
    }

    /**
     * Clear the test directory.  This is a separate method to allow subclasses
     * to override its behavior without removing other required setUp
     * operations.
     */
    protected void clearTestDirectory() {
        TestUtils.clearTestDirectory();
    }

    @After
    public void tearDown()
        throws Exception {

        tearDowns.forEach(Runnable::run);
        tearDowns.clear();
        kvstoreName = null;
    }

    /**
     * Waits for the specified table to be created. Times out after 20 seconds.
     * If a table is found, it is returned.
     */
    public static Table waitForTable(TableAPI tableAPI, String tableName) {
        final Table table[] = new Table[]{null};
        assertTrue("Waiting for table: " + tableName,
                new PollCondition(500, 40000) {
                    @Override
                    protected boolean condition() {
                        table[0] = tableAPI.getTable(tableName);
                        return table[0] != null;
                    }
                }.await());
        return table[0];
    }

    /**
     * Waits for the table metadata system table to be created and
     * initialized. The table is created async during store startup
     * and is delayed until the store is deployed. So the delay can
     * be significant.
     *
     * Care must be taken when calling this method. If writes to system
     * tables are not enabled then this call will time out. See comments
     * at top of file.
     */
    public static void waitForTableMDSysTable(TableAPI tableAPI) {
        assertTrue("Waiting for table: " + TableMetadataDesc.TABLE_NAME,
                new PollCondition(1000, 60000) {
                    @Override
                    protected boolean condition() {
                        return TableSysTableUtil.getMDTable(tableAPI) != null;
                    }
                }.await());
    }

    /**
     * Add regions to "create table" DDL.
     */
    protected static String addRegionsForMRTable(String ddl) {
        String[] ddlArr = ddl.split("\\s+");
        String ddlCheck = String.join(" ", ddlArr);
        if (mrTableMode &&
            ddlCheck.toLowerCase().contains("create table")) {
            //TODO: Remove the restrictions after MRTables support identity
            //column and TTL.
            if (!ddlCheck.toLowerCase().contains("as identity") &&
                !ddlCheck.toLowerCase().contains("ttl")) {
                /* add a remote region when create tables. */
                ddl += " IN REGIONS " + REMOTE_REGION;
            }
        }
        return ddl;
    }

    /**
     * Set up for multi-region table mode.
     */
    public static void mrTableSetUp(KVStore storeHandle) {
        if (mrTableMode) {
            if (reqRespThread == null) {
                /*
                 * start a mock service so "create table" DDLs for MRTables
                 * can finish
                 */
                reqRespThread = mrTableRegionSetUp(storeHandle);

            }
        }
    }

    /**
     * Create regions for multi-region table mode and return a mock
     * service.
     */
    protected static ReqRespThread mrTableRegionSetUp(KVStore storeHandle) {
        ReqRespThread manThread = new ReqRespThread(storeHandle, null,
                                                    false);
        manThread.start();
        XRegionTestBase.setLocalRegionName(storeHandle,
                                           LOCAL_REGION,
                                           null, false);
        /* create regions */
        XRegionTestBase.createRegion(storeHandle,
                                     REMOTE_REGION,
                                     null, false);
        return manThread;
    }

    public static void mrTableTearDown() {
        if (reqRespThread != null) {
            reqRespThread.stopResResp();
            reqRespThread = null;
        }
    }

    /**
     * Suppress system standard output produced by test.
     * Must call resetSystemOut after use.
     */
    public static void suppressSystemOut() {
        if (!SYS_OUTPUT_ENABLE) {
            stdOut = System.out;
            System.setOut(NULL_PRINTSTREAM);
        }
    }

    /**
     * Suppress system standard error output produced by test.
     * Must call resetSystemOut after use.
     */
    public static void suppressSystemError() {
        if (!SYS_OUTPUT_ENABLE) {
            stdErr = System.err;
            System.setErr(NULL_PRINTSTREAM);
        }
    }

    /**
     * Reset system standard output if it has been suppressed and cached.
     */
    public static void resetSystemOut() {
        if (stdOut != null) {
            System.setOut(stdOut);
        }
    }

    /**
     * Reset system standard error output if it has been suppressed and cached.
     */
    public static void resetSystemError() {
        if (stdErr != null) {
            System.setErr(stdErr);
        }
    }

    /**
     * Copy the testing directory to other place.
     */
    private static void copyEnvironments(String path) throws Exception{
        File failureDir = TestUtils.getFailureCopyDir();

        if (failureDir == null || failureDir.list() == null)
            return;

        if (failureDir.list().length < TestUtils.getCopyLimit()) {
            TestUtils.copyDir(TestUtils.getTestDir(),
                              new File(failureDir, path));
        }
    }

    /**
     * Get failure copy directory name.
     */
    private String makeFileName(Description desc) {
        return desc.getClassName() + "-" + desc.getMethodName();
    }

    /**
     * Returns the override parameters value for the specified string, or null.
     */
    private static List<Object[]> getParamsOverride(String propValue) {
        if ((propValue == null) || "".equals(propValue)) {
            return null;
        }
        final String[] stringValues = propValue.split(",");
        final Object[] element = new Object[stringValues.length];
        int i = 0;
        for (String s : stringValues) {
            element[i++] = "true".equals(s) ? true :
                "false".equals(s) ? false :
                s;
        }
        System.out.println("Using testcase parameters: " +
                           Arrays.toString(element));
        return Collections.singletonList(element);
    }

    /** Returns the KVStore name. */
    public String getKVStoreName() {
        return kvstoreName;
    }

    /**
     * Returns the number of milliseconds after which the test class should
     * timeout. Returns the value specified by the {@link
     * TestClassTimeoutMillis} annotation associated with the test class
     * description, if one was specified, or else the value of {@link
     * #DEFAULT_CLASS_TIMEOUT_MILLIS}. Scales the value by the timeout
     * multiplier.
     */
    private static long getClassTimeoutMillis(Description desc) {
        final TestClassTimeoutMillis annotation =
            desc.getAnnotation(TestClassTimeoutMillis.class);
        final long rawTimeout = (annotation != null) ?
            annotation.value() :
            DEFAULT_CLASS_TIMEOUT_MILLIS;
        return Math.round(rawTimeout * TEST_CLASS_TIMEOUT_MULTIPLIER);
    }

    /**
     * Tests if the testcase is running alone.
     */
    protected boolean isRunningAlone(String methodName) {
        return methodName.equals(parseTestMethod());
    }

    /**
     * Parse the mvn command and obtains testcase name. We assume that the test
     * case is specified with -Dit.test=TestSuite#testMethodName.
     */
    private String parseTestMethod() {
        final String testMethodString = System.getProperty("it.test", null);
        if (testMethodString == null) {
            return null;
        }
        final int pos = testMethodString.indexOf("#", 0);
        if (pos < 0) {
            return null;
        }
        return testMethodString.substring(pos + 1);
    }
}
