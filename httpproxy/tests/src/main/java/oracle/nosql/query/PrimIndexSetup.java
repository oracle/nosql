/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.values.MapValue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

/**
 * Setup implementation for cases that depend on //data1.
 */
public class PrimIndexSetup extends QTDefaultImpl {

    String tableStatement =
        "CREATE TABLE Foo(             \n" +
        "    id1 INTEGER,                \n" +
        "    id2 DOUBLE,                 \n" +
        "    id3 ENUM(tok0, tok1, tok2), \n" +
        "    firstName STRING,           \n" +
        "    lastName STRING,            \n" +
        "    age INTEGER,                \n" +
        "    id4 STRING,                 \n" +
        "primary key (id1, id2, id3, id4))";

    @Override
    public void before() {
        opts.verbose("Run Before: PrimIndexSetup");

        List<String> stmts = new ArrayList<String>();
        stmts.add(tableStatement);
        executeStatements(stmts);

        NoSQLHandleImpl handle = (NoSQLHandleImpl)QTest.getHandle();

        for (int i = 1; i < 6; i++) {

            for (int j = 0; j < 3; ++j) {
                MapValue mv = new MapValue().put("id1", i)
                    .put("id2", i * 10.0 + j)
                    .put("id3", "tok" + (i % 3))
                    .put("id4", ("id4-" + i))
                    .put("firstName", ("first" + i))
                    .put("lastName", ("last" + i))
                    .put("age", i+10);
                PutRequest req = new PutRequest().setValue(mv)
                            .setTableName("Foo");
                PutResult res = handle.put(req);
                Assert.assertNotNull(res);
            }
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: PrimIndexSetup");

        List<String> stmts = new ArrayList<String>();
        stmts.add("DROP TABLE Foo");
        executeStatements(stmts);
    }
}
