/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.common.qtf.QTCase;
import oracle.nosql.common.qtf.QTestBase;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.common.qtf.QTSuite;

/**
 * Class extends oracle.nosql.common.qtf.RunQueryTests
 */
public class RunQueryTests extends oracle.nosql.common.qtf.RunQueryTests {

    public QTOptions opt = new QTOptions();

    public List<QTSuite> suites = new ArrayList<QTSuite>();

    public static void main(String[] args)
        throws Exception {

        RunQueryTests rqt = new RunQueryTests();

        int initRes = rqt.init(args);
        if (initRes < 0) {
            return;
        }

        rqt.opt.setVerbose(Boolean.getBoolean("test.verbose"));

        try {
            ProxyOperation.staticSetUp();

            rqt.opt.verbose("\nQuery Test Framework\n");

            rqt.suites = QTestBase.createQTSuites(rqt.opt);

            rqt.opt.verbose("\nFind test cases\n");
            List<QTCase> cases = rqt.getCases();

            rqt.opt.verbose("\nRun\n");

            // Run only the tests that were filtered and only their run and
            // suite before/after code.
            QTest qTest = new QTest(null, null);
            for (QTCase qtCase : cases) {
                qTest.testCase = (QTCaseCloud)qtCase;
                qTest.testCase.setHandle(QTest.getHandle());
                qTest.before();
                qTest.test();
                qTest.after();
            }

        } finally {
            ProxyOperation.staticTearDown();
        }
    }
}
