/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.xregion.XRegionTestBase.ReqRespThread;

import oracle.nosql.common.qtf.QTCase;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.common.qtf.QTSuite;
import oracle.nosql.common.qtf.QTestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The "root" class for QTF, when QTF is run via JUnit. The class given to
 * JUnit to run the QTF tests (see QTest.main() method). It uses JUnit's
 * parametrized testing.
 */
@RunWith(Parameterized.class)
public class QTest extends QTestBase {

    static final String QTF_HOSTPORT = System.getProperty("test.qtf.hostport");

    static final String QTF_STORE = System.getProperty("test.qtf.store");

    public static KVStore store;

    static ReqRespThread reqRespThread;

    public QTCaseKV testCase;

    public static void main(String args[]) {
      org.junit.runner.JUnitCore.main(QTest.class.getName());
    }

    @BeforeClass
    public static void beforeAll() throws Exception {

        /* Use external store */
        if (QTF_HOSTPORT != null && QTF_STORE != null) {
            return;
        }

        TestStatus.setActive(true);
        TableTestBase.staticSetUp();
    }

    @AfterClass
    public static void afterAll() throws Exception {

        if (previousRun != null) {
            previousSuite.after();
            previousRun = null;
            previousSuite = null;
        }

        TableTestBase.staticTearDown();
        if (reqRespThread != null) {
            reqRespThread.stopResResp();
        }
        TestStatus.setActive(false);
    }

    /*
     * It is invoked once by JUnit to construct and return the list of test cases
     * to run. Each element of the list is an array of 2 Objects: the 1st object
     * is a QTCase instance and the 2nd is always null. For each element in the
     * list, JUnit will construct a QTest instance, passing the 2 objects in the
     * element as arguments to the QTest constructor. For each such QTest
     * instance, JUnit will invoke its methods tagged with @Test
     */
    @Parameterized.Parameters(name = "{index}: case({0})")
    public static Collection<Object[]> data()
        throws Exception {

        factory = new QTFactoryKV();

        QTOptions opts = parseCommandLine();

        if (TestBase.mrTableMode) {
            String filterOut = System.getProperty("test.filterMRTable");
            if (filterOut != null) {
                opts.setFilterOut(filterOut);
            }
        }

        // Create QTSuites and QTRuns per QTSuite
        List<QTSuite> suites = createQTSuites(opts);

        List<QTCase> cases = new ArrayList<QTCase>();
        for(QTSuite s : suites) {
            s.addCases(cases);
        }

        List<Object[]> params = new ArrayList<Object[]>();
        for (QTCase c : cases) {
            params.add(new Object[]{c, null});
        }

        return params;
    }

    static KVStore getStore() {

        if (store == null) {
            /* return external store's handle */
            if (QTF_HOSTPORT != null && QTF_STORE != null) {
                final KVStoreConfig config =
                    new KVStoreConfig(QTF_STORE, QTF_HOSTPORT);
                store = KVStoreFactory.getStore(config);

                System.out.println("Using external store: " + QTF_STORE +
                                   " in " + QTF_HOSTPORT);
                return store;
            }
            store = TableTestBase.getStore();
        }
        return store;
    }

    @SuppressWarnings("unused")
    public QTest(Object param1, Object param2) {

        testCase = (QTCaseKV)param1;

        if (testCase != null) {
            testCase.setStore((KVStoreImpl)getStore());
        }
    }

    @Before
    public void before() {
    }

    @After
    public void after() {
    }

    @Test
    public void test()
        throws IOException {

        checkDeps(testCase);
        testCase.run();
    }
}
