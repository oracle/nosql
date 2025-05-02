/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

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

        StatementResult res = QTest.store.executeSync(tableStatement);
        Assert.assertTrue(res.isSuccessful());

        TableAPI tapi = QTest.store.getTableAPI();
        Table table = tapi.getTable("Foo");

        for (int i = 1; i < 6; i++) {

            for (int j = 0; j < 3; ++j) {

                Row row = table.createRow();
                row.put("id1", i);
                row.put("id2", i * 10.0 + j);
                row.putEnum("id3", ("tok" + (i % 3)));
                row.put("id4", ("id4-" + i));
                row.put("firstName", ("first" + i));
                row.put("lastName", ("last" + i));
                row.put("age", i+10);

                tapi.put(row, null, null);
            }
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: PrimIndexSetup");

        StatementResult res = QTest.store.executeSync("DROP TABLE Foo");
        Assert.assertTrue(res.isSuccessful());
    }
}
