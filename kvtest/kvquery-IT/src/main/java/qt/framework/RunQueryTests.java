/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

import java.util.List;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.nosql.common.qtf.QTestBase;
import oracle.nosql.common.qtf.QTCase;

public class RunQueryTests extends oracle.nosql.common.qtf.RunQueryTests {

    public static void main(String[] args)
        throws Exception {

        RunQueryTests rqt = new RunQueryTests();

        int initRes = rqt.init(args);
        if (initRes < 0) {
            return;
        }

        try {
            TableTestBase.staticSetUp();

            rqt.opt.verbose("\nQuery Test Framework\n");

            rqt.suites = QTestBase.createQTSuites(rqt.opt);

            rqt.opt.verbose("\nFind test cases\n");
            List<QTCase> cases = rqt.getCases();

            rqt.opt.verbose("\nRun\n");

            // Run only the tests that were filtered and only their run and
            // suite before/after code.
            QTest qTest = new QTest(null, null);
            for (QTCase qtCase : cases) {
                qTest.testCase = (QTCaseKV)qtCase;
                qTest.testCase.setStore((KVStoreImpl)TableTestBase.getStore());
                qTest.before();
                qTest.test();
                qTest.after();
            }

        } finally {
            TableTestBase.staticTearDown();
        }
    }
}
