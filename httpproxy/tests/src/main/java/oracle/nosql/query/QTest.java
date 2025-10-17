/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import oracle.kv.impl.test.TestStatus;
import oracle.nosql.common.qtf.QTCase;
import oracle.nosql.common.qtf.QTestBase;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.common.qtf.QTSuite;
import oracle.nosql.driver.NoSQLHandle;

import oracle.nosql.proxy.Proxy;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.fasterxml.jackson.core.StreamReadConstraints;

/**
 * Use JUnit's parametrized testing
 */
@RunWith(Parameterized.class)
public class QTest extends QTestBase {

    private static final String QTF_PROXY_HOST =
        System.getProperty("test.qtf.proxy.host");

    private static final String QTF_PROXY_PORT =
        System.getProperty("test.qtf.proxy.port");

    static {
        /*
         * Since Jackson 2.15, the constraints StreamReadConstraints was added
         * to guard against malicious input by preventing processing of
         * "too big" input constructs.
         *
         * The StreamReadConstraints.DEFAULT_MAX_NUM_LEN(default 1000) was
         * added since jackson 2.16 to limit the maximum number length.
         * See details in https://docshoster.org/p/com.fasterxml.jackson/jackson-core/2.18.2/com/fasterxml/jackson/core/StreamReadConstraints.html
         *
         * The qtf test "gb2" uses the number values with max length of 3087,
         * increase the maxNumberLength to be able to read those number values
         * when parsing from JSON string.
         */
        int maxNumberLength = 4000;
        StreamReadConstraints constraints = StreamReadConstraints.builder()
                .maxNumberLength(maxNumberLength)
                .build();
        StreamReadConstraints.overrideDefaultStreamReadConstraints(constraints);
    }

    private static Proxy proxy;

    private static NoSQLHandle handle;

    public QTCaseCloud testCase;

    public static void main(String args[]) {
      org.junit.runner.JUnitCore.main(QTest.class.getName());
    }

    @BeforeClass
    public static void beforeAll()
        throws Exception {

        /* Use external proxy */
        if (QTF_PROXY_HOST != null && QTF_PROXY_PORT != null) {
            String endpoint = QTF_PROXY_HOST + ":" + QTF_PROXY_PORT;
            handle = ProxyOperation.configHandleStatic(endpoint,
                         Boolean.getBoolean("onprem"));
            return;
        }

        TestStatus.setActive(true);
        ProxyOperation.staticSetUp();

        proxy = ProxyOperation.getProxy();
    }

    @AfterClass
    public static void afterAll()
        throws Exception {

        if (previousRun != null) {
            previousSuite.after();
            previousRun = null;
            previousSuite = null;
        }

        ProxyOperation.staticTearDown();
    }

    @Parameterized.Parameters(name = "{index}: case({0})")
    public static Collection<Object[]> data()
        throws Exception {

        factory = new QTFactoryCloud();

        QTOptions opts = parseCommandLine();
        opts.setVerbose(Boolean.getBoolean("test.verbose"));

        String filter = opts.getFilter();
        String filterOut = System.getProperty("test.filterOutList");
        if ( !"idc_".equals(filter) && filterOut == null ) {
            filterOut = "idc_";
        }
        opts.setFilterOut(filterOut);

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

    public static NoSQLHandle getHandle() {

        if (handle == null) {
            handle = ProxyOperation.getNosqlHandle();
        }
        return handle;
    }

    public QTest(Object param1, Object param2) {

        testCase = (QTCaseCloud)param1;

        if (testCase != null) {
            testCase.setHandle(getHandle());
            if (proxy != null && proxy.isOnPrem()) {
                testCase.setIsOnPrem();
            }
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

        try {
            checkDeps(testCase);
        } catch (Exception e) {
            /*
             * This was added to catch unexpected failures in checkDeps.
             * Without this, such exceptions are silent and cause QTF to
             * misbehave (see checkDeps -- it won't properly change
             * previous suite and handle before/after calls).
             */
            fail("Exception from checkDeps: " + e);
        }
        testCase.run();
    }
}
