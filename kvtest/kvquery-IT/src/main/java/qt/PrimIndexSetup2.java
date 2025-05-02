/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

import java.util.Random;

import oracle.kv.StatementResult;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import org.junit.Assert;
import qt.framework.QTDefaultImpl;
import qt.framework.QTest;

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

        StatementResult res = QTest.store.executeSync(tableStatement);
        Assert.assertTrue(res.isSuccessful());

        TableAPI tapi = QTest.store.getTableAPI();
        Table table = tapi.getTable("Foo");

        for (int i = 0; i < num1; i++) {

            for (int j = 0; j < num2; ++j) {

                for (int k = 0; k < num3; ++k) {

                    Row row = table.createRow();
                    row.put("id1", rand.nextInt(20));
                    row.put("id2", rand.nextInt(5));
                    row.put("id3", rand.nextInt(5));
                    row.put("id4", ("id4-" + i));
                    row.put("firstName", ("first" + i));
                    row.put("lastName", ("last" + i));
                    row.put("age", i+10);

                    tapi.putIfAbsent(row, null, null);
                }
            }
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: PrimIndexSetup2");

        StatementResult res = QTest.store.executeSync("DROP TABLE Foo");
        Assert.assertTrue(res.isSuccessful());
    }
}
