/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

import oracle.kv.StatementResult;

import org.junit.Assert;

import qt.framework.QTest;

public class PrimIndexSetup3 extends PrimIndexSetup2 {

    public PrimIndexSetup3() {
        num2 = 15;
    }

    @Override
    public void before() {

        super.before();

        String indexStatement;
        StatementResult res;

        indexStatement = "CREATE INDEX idx1 on Foo (age, firstName)";  
        res = QTest.store.executeSync(indexStatement);
        Assert.assertTrue(res.isSuccessful());

        indexStatement = "CREATE INDEX idx2 on Foo (age)";  
        res = QTest.store.executeSync(indexStatement);
        Assert.assertTrue(res.isSuccessful());
    }
}
