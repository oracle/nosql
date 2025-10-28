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
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

/**
 * Setup implementation for //simpleUserTable cases.
 */
public class UserTable extends QTDefaultImpl {

    String userTableStatement =
        "CREATE TABLE Users" +
            "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
            " address RECORD(   \n" +
            "         city STRING,\n" +
            "         state STRING,\n" +
            "         phones ARRAY( RECORD( work INTEGER, home INTEGER ) ),\n" +
            "         ptr STRING), \n" +
            " children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),\n" +
            "primary key (id))";

    @Override
    public void before() {
        opts.verbose("Run Before:  UsersTable");

        List<String> stmts = new ArrayList<String>();
        stmts.add(userTableStatement);
        executeStatements(stmts);

        NoSQLHandleImpl handle = (NoSQLHandleImpl)QTest.getHandle();
        JsonOptions options = new JsonOptions();
        for (int i = 0; i < 10; i++) {
            MapValue mv = new MapValue().put("id", i)
                .put("firstName", ("first" + i))
                .put("lastName", ("last" + i))
                .put("age", i + 10)
                .putFromJson("address", 
                    "{\"city\":\"Boston\", \"state\":\"MA\", " +
                    "\"phones\":[{\"work\" : 111,\"home\" : 222}], \"ptr\" :" +
                    " null }", options)
                .putFromJson("children", 
                    "{\"john\": {\"age\" : 3, \"friends\":[\"f1\"]}, " +
                    "\"cory\": {\"age\" : 4, \"friends\":[\"f2\"]} }",
                    options);
            PutRequest req = new PutRequest().setValue(mv)
                                .setTableName("Users");
            PutResult res = handle.put(req);
            Assert.assertNotNull(res);
        }
    }

    @Override
    public void after() {
        opts.verbose("Run After: UsersTable");

        List<String> stmts = new ArrayList<String>();
        stmts.add("DROP TABLE Users");
        executeStatements(stmts);
    }
}
