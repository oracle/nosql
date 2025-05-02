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
public class Data1Setup extends QTDefaultImpl {

    String userTableStatement =
        "CREATE TABLE Data1Users" +
            "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
            "primary key (id))";

    @Override
    public void before() {
        opts.verbose("Run Before:  Data1");

        StatementResult res = QTest.store.executeSync(userTableStatement);
        Assert.assertTrue(res.isSuccessful());

        TableAPI tableImpl = QTest.store.getTableAPI();
        Table table = tableImpl.getTable("Data1Users");

        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            tableImpl.put(row, null, null);
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: Data1");

        StatementResult res = QTest.store.executeSync("DROP TABLE Data1Users");
        Assert.assertTrue(res.isSuccessful());
    }
}
