/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.values.MapValue;

import org.junit.Assert;

/**
 * Setup implementation for cases that depend on //data1.
 */
public class PrimIndexSetup2 extends QTDefaultImpl {

    String tableStatement =
        "CREATE TABLE Foo(               \n" +
        "    id1 INTEGER,                \n" +
        "    id2 INTEGER,                \n" +
        "    id3 INTEGER,                \n" +
        "    firstName STRING,           \n" +
        "    lastName STRING,            \n" +
        "    age INTEGER,                \n" +
        "    id4 STRING,                 \n" +
        "primary key (shard(id1, id2), id3, id4))";

    Random rand = new Random(1);

    int num1 = 20;
    int num2 = 5;
    int num3 = 3;

    public PrimIndexSetup2() {
    }

    @Override
    public void before() {
        opts.verbose("Run Before: PrimIndexSetup2");

        List<String> stmts = new ArrayList<String>();
        stmts.add(tableStatement);
        executeStatements(stmts);

        NoSQLHandleImpl handle = (NoSQLHandleImpl)QTest.getHandle();

        for (int i = 0; i < num1; i++) {

            for (int j = 0; j < num2; ++j) {

                for (int k = 0; k < num3; ++k) {

                    MapValue mv = new MapValue().put("id1", rand.nextInt(20))
                        .put("id2", rand.nextInt(5))
                        .put("id3", rand.nextInt(5))
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
    }

    @Override
    public void after() {
        opts.verbose("Run After: PrimIndexSetup2");

        List<String> stmts = new ArrayList<String>();
        stmts.add("DROP TABLE Foo");
        executeStatements(stmts);
    }
}
