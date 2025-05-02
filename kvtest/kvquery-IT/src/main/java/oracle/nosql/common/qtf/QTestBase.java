/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 * Parent class for the QTest class, which is the "root" class for QTF,
 * when QTF is run via JUnit.
 */
public  class QTestBase {

    public static QTFactory factory;

    public static QTSuite previousSuite = null;

    public static QTRun previousRun = null;

    public static QTOptions parseCommandLine() throws IOException {

        String baseDirProp = System.getProperty("qtf.cases");

        if (baseDirProp == null) {

            String testdestdir = System.getProperty("testdestdir");

            if (testdestdir != null) {

                File tdd = new File(testdestdir);
                File casesDir = new File(tdd.getParentFile().getParentFile(),
                                         "test/query/cases");

                if (!casesDir.exists() || !casesDir.isDirectory()) {
                    casesDir = new File(tdd.getParentFile().getParentFile(),
                                        "test/oracle/nosql/query/cases");
                }

                baseDirProp = casesDir.getAbsolutePath();
            }
        }

        if (baseDirProp == null)
            throw new IllegalArgumentException(
                "The directory for QTF cases has not been specified. " +
                "Either the qtf.case or the testdestdir property must be set.");

        File baseDir = new File(baseDirProp).getCanonicalFile();

        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new IllegalArgumentException(
                "The QTF base directory does not exist: " +
                baseDir.getAbsolutePath());
        }

        String filter = System.getProperty("test.filter");
        String filterOut = System.getProperty("test.filterOut");

        if (filterOut == null && !"idc_".equals(filter)) {
            filterOut = "idc_";
        }

        boolean verbose = System.getProperty("test.qt.verbose") != null;

        boolean updateQueryPlans = System.getProperty("test.updateQueryPlans") != null;
        boolean genActualFiles = System.getProperty("test.actual") != null;

        byte traceLevel = 0;
        String traceLevelStr = System.getProperty("test.traceLevel");
        if (traceLevelStr != null) {
            traceLevel = Byte.parseByte(traceLevelStr);
        }

        int batchSize = 5;
        String batchSizeStr = System.getProperty("test.batchSize");
        if (batchSizeStr != null) {
            batchSize = Integer.parseInt(batchSizeStr);
        }

        int readKBLimit = 0;
        String readKBLimitStr = System.getProperty("test.readKBLimit");
        if (readKBLimitStr != null) {
            readKBLimit = Integer.parseInt(readKBLimitStr);
        }

        boolean useAsync = false;
        String useAsynStr = System.getProperty("test.async");
        if (useAsynStr != null &&
            (useAsynStr.equals("true") || useAsynStr.equals("on"))) {
            useAsync = true;
        }

        boolean onprem = false;
        String onpremStr = System.getProperty("onprem");
        if (onpremStr != null && onpremStr.equals("true")) {
            onprem = true;
        }

        boolean progress = Boolean.getBoolean("qtf.progress");

        QTOptions opts = new QTOptions();

        opts.setBaseDir(baseDir);
        opts.setFilter(filter);
        opts.setFilterOut(filterOut);
        opts.setVerbose(verbose);
        opts.setTraceLevel(traceLevel);
        opts.setUpdateQueryPlans(updateQueryPlans);
        opts.setGenActualFiles(genActualFiles);
        opts.setBatchSize(batchSize);
        opts.setReadKBLimit(readKBLimit);
        opts.setUseAsync(useAsync);
        opts.setOnPrem(onprem);
        opts.setProgress(progress);

        opts.progress("\nQuery Test Framework\n");

        return opts;
    }

    public static List<QTSuite> createQTSuites(QTOptions opts)
        throws IOException {

        // baseDir = <kvstore>/test/query/cases
        File baseDir = opts.getBaseDir();
        opts.verbose("Init basedir: " + baseDir.getCanonicalPath());

        List<QTSuite> suites = new ArrayList<QTSuite>();

        File[] subdirs = baseDir.listFiles();

        if (subdirs != null) {

            for (File dir : subdirs) {

                if (dir.isDirectory()) {

                    String relativePath = opts.relativize(dir);

                    opts.verbose("Scan: " + relativePath);

                    File configFile = new File(dir, "test.config");

                    if (configFile.exists() && configFile.isFile()) {

                        /* To make the results of the rowprops suite stable,
                         * this suite must be the 1st one to run, so that it
                         * starts with an empty store */
                        QTSuite suite = factory.createQTSuite(opts, configFile);
                        if (configFile.getPath().contains("rowprops")) {
                            suites.add(0, suite);
                        } else {
                            suites.add(suite);
                        }
                    }
                }
            }
        }

        return suites;
    }

    synchronized protected static void checkDeps(QTCase c) {

        if (previousRun == null) {
            /*
             * If suite.before() failed, it implies the preparation
             * work for current test is not completed, either because
             * of creating table/index failed or data population failed.
             *
             * In this case, the current test should be failed, did
             * some cleanup works to make sure next test can continue
             * to run with a clean store.
             */
            try {
                c.getRun().suite.before();
            } catch (RuntimeException re) {
                try {
                    c.getRun().suite.after();
                } catch (RuntimeException ignored) {
                    /* do nothing if clean up tables failed. */
                }
                throw re;
            }

            previousSuite = c.getRun().suite;
            previousRun = c.getRun();

        } else {

            if (previousSuite != c.getRun().suite) {

                previousSuite.after();

                try {
                    c.getRun().suite.before();
                    previousSuite = c.getRun().suite;

                } catch (RuntimeException re) {
                    /*
                     * If suite.before() failed, it implies the preparation
                     * work for current test is not completed, either because
                     * of creating table/index failed or data population failed.
                     *
                     * In this case, the current test should be failed, did
                     * some cleanup works to make sure next test can continue
                     * to run with a clean store.
                     */
                    try {
                        c.getRun().suite.after();
                    } catch (RuntimeException ignored) {
                        /* do nothing if clean up tables failed. */
                    }

                    previousRun = null;
                    throw re;
                }
            }

            previousRun = c.getRun();
        }
    }
}
