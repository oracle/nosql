/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import java.util.ArrayList;
import java.util.List;
/**
 * Setup implementation for cases that depend on //data1.
 */
public class PrimIndexSetup3 extends PrimIndexSetup2 {

    public PrimIndexSetup3() {
        num2 = 15;
    }

    @Override
    public void before() {

        super.before();

        String indexStatement =
            "CREATE INDEX idx on Foo (age)";  

        List<String> stmts = new ArrayList<String>();
        stmts.add(indexStatement);
        executeStatements(stmts);
    }
}
