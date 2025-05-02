/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.util.List;

import oracle.kv.StatementResult;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests select and update of the selected results
 */
public class SelectPlusUpdateTest
    extends DmlTest {

    @Test
    public void testSelectPlusUpdate() {
        executeDdl(userTableStatement);
        addUsers(10);

        String query = "select * from Users where (age > 13 and age < 17)";

        List<RecordValue> results = executeDML(query, 0);
        if (debugging) {
            for (RecordValue record : results) {
                System.out.println(" -> " + record);
            }
        }
        assertEquals("Unexpected number of results", 3, results.size());

        Table table = tableImpl.getTable(getNamespace(), "Users");

        for (RecordValue record : results) {
            Row row = table.createRow();
            row.put("id", record.get("id"));
            row.put("age", record.get("age").asInteger().get() + 100);
            tableImpl.put(row, null, null);
        }

        results = executeDML("select * from Users", 1);
        assertEquals("Unexpected number of results", 10, results.size());

        for (RecordValue record : results) {
            if (debugging ) {
                System.out.println(" --> " + record);
            }

            if (record.get("id").asInteger().get() > 3 &&
                record.get("id").asInteger().get() < 7) {
                assertEquals("Unexpected value",
                    record.get("id").asInteger().get() + 10 + 100,
                    record.get("age").asInteger().get());
                assertTrue(record.get("firstName").isNull());
                assertTrue(record.get("lastName").isNull());
            } else {
                assertEquals("Unexpected value",
                    record.get("id").asInteger().get() + 10,
                    record.get("age").asInteger().get());
                assertFalse(record.get("firstName").isNull());
                assertFalse(record.get("lastName").isNull());
            }
        }

        executeDdl("drop table Users");
    }

    @Test
    public void testDmlApiSample1() {

        executeDdl(userTableStatement);
        addUsers(10);

        // compile and execute the statement
        StatementResult result = executeDml("SELECT firstName, age FROM users");

        // get the results
        for (RecordValue record : result) {

            if (debugging) {
                System.out.println(
                    "  nameFirst: " + record.get("firstName").asString().get());
                System.out.println(
                    "  age:  " + record.get("age").asInteger().get());
            }
        }

        executeDdl("drop table Users");
    }

    //@Test
    public void testDmlApiSample2() {

        executeDdl(userTableStatement);
        addUsers(10);


        // compile the statement
        PreparedStatement pStmt = prepare(
                "var int $minAge;" +
                "var int $maxAge;" +
                "SELECT id, firstName FROM users WHERE age >= $minAge and " +
                    "age < $maxAge");


        for (int age = 0; age <= 100; age = age + 10) {

            int maxAge = age + ( age < 100 ? 10 : 1000 );

            if (debugging) {
                System.out.println("Persons with ages between " +
                                   age + " and " + maxAge + ".");
            }
            // bind variables, reuse the same pStmt
            BoundStatement bStmt = pStmt.createBoundStatement();
            bStmt.setVariable("minAge", age);
            bStmt.setVariable("maxAge", maxAge);

            // execute
            StatementResult result = executeStatement(bStmt);

            // get the results
            for (RecordValue record : result) {

                if (debugging) {
                    System.out.println("  id:        " +
                                       record.get("id").asInteger().get() );
                    System.out.println("  firstName: " +
                                       record.get("firstName")
                                       .asString().get() );
                }
            }
        }

        executeDdl("drop table Users");
    }

    //@Test
    public void testDmlApiSample3() {

        executeDdl(userTableStatement);
        addUsers(10);

        TableAPI tableAPI = store.getTableAPI();
        Table table = tableAPI.getTable(getNamespace(), "Users");

        StatementResult result = executeDml(
            "select * from Users where (age > 13 and age < 17)");

        for (RecordValue record : result) {

            // case 1: update a field reusing the record
            record.put("age", record.get("age").asInteger().get() + 1 );
            tableAPI.put(record.asRow(), null, null);

            // case 2: create a new row
            Row row = table.createRow();
            row.put("id", record.get("id"));
            row.put("age", record.get("age").asInteger().get() + 100);
            // any other fields will be null

            tableAPI.put(row, null, null);
        }

        executeDdl("drop table Users");
    }

    @Test
    public void testDmlApiSample4() {
        executeDdl(userTableStatement);
        addUsers(10);

        // access metadata on PreparedStatement or BoundStatement
        PreparedStatement pStmt = prepare(
            "SELECT id, firstName FROM users WHERE age >= 1 and age < 100 ");

        RecordDef recodDef = pStmt.getResultDef();
        int noOfFields = recodDef.getFieldNames().size();
        String fieldName = recodDef.getFieldName(1);
        FieldDef fieldType = recodDef.getFieldDef(1);

        Assert.assertTrue( noOfFields>0);
        Assert.assertNotNull(fieldName);
        Assert.assertNotNull(fieldType);

        // access metadata on StatementResult
        StatementResult result = executeDml(
            "select * from Users where (age > 13 and age < 17)");

        recodDef = result.getResultDef();

        executeDdl("drop table Users");
    }
}
