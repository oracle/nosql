/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

import oracle.kv.StatementResult;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import org.junit.Assert;
import qt.framework.QTDefaultImpl;
import qt.framework.QTest;

/**
 * Setup implementation for //simpleUserTable cases.
 */
public class UserTable extends QTDefaultImpl {

    String userTableStatement =
        "CREATE TABLE Users" +
            "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
            "  address RECORD(   \n" +
            "          city STRING,\n" +
            "          state STRING,\n" +
            "          phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),\n" +
            "          ptr STRING), \n" +
            "  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),\n" +
            "primary key (id))";

    @Override
    public void before() {
        opts.verbose("Run Before:  UsersTable");

        StatementResult res = QTest.store.executeSync(userTableStatement);
        Assert.assertTrue(res.isSuccessful());

        TableAPI tableImpl = QTest.store.getTableAPI();
        Table table = tableImpl.getTable("Users");
        FieldDef addressDef = table.getField("address");
        FieldDef childrenDef = table.getField("children");

        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i + 10);
            row.put("address", FieldValueFactory.createValueFromJson
                (addressDef, "{\"city\":\"Boston\", \"state\":\"MA\", " +
                    "\"phones\":[{\"work\" : 111,\"home\" : 222}], \"ptr\" :" +
                    " null }"));
            row.put("children", FieldValueFactory.createValueFromJson
                (childrenDef,
                    "{\"john\": {\"age\" : 3, \"friends\":[\"f1\"]}, " +
                     "\"cory\": {\"age\" : 4, \"friends\":[\"f2\"]} }"));
            tableImpl.put(row, null, null);
        }

        //res = opts.getStore().executeSync("DESCRIBE AS JSON TABLE Users");
        //System.out.println("Describe Users: " + res.getResult());
    }

    @Override
    public void after() {
        opts.verbose("Run After: UsersTable");

        StatementResult res = QTest.store.executeSync("DROP TABLE Users");
        Assert.assertTrue(res.isSuccessful());
    }
}
