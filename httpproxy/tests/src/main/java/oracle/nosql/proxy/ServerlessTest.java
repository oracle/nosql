/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.values.MapValue;

/**
 * An extension of ServerlessTestBase that uses the V3 binary protocol
 */
public class ServerlessTest extends ServerlessTestBase {

    @Test
    public void testServerless() {
        String tableName = "foo";
        String statement = "create table if not exists " + tableName +
            "(id integer, primary key(id), name string)";
        TableResult res = tableOp(statement, tableLimits, null);
        assertTrue(res.getTableState() == TableResult.State.ACTIVE);

        /* CRUD */
        MapValue value = new MapValue().put("id", 10).put("name", "jane");
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult pres = (PutResult) doRequest(putRequest);
        assertNotNull(pres.getVersion());
        value.put("id", 5).put("name", "joe");
        pres = (PutResult) doRequest(putRequest);
        assertNotNull(pres.getVersion());

        MapValue key = new MapValue().put("id", 5);
        GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);

        GetResult gres = (GetResult) doRequest(getRequest);
        assertEquals(value, gres.getValue());

        QueryRequest queryReq = new QueryRequest()
            .setStatement("select * from " + tableName);
        /* Query */
        QueryResult qres = (QueryResult) doRequest(queryReq);
        assertEquals(2, qres.getResults().size());

        /* test error mapping */

        /*
         * test a table not found
         */
        try {
            getRequest.setTableName("abc");
            gres = (GetResult) doRequest(getRequest);
            fail("Should have thrown");
        } catch (TableNotFoundException tnfe) {
            // success
        }

        /*
         * test a bad DDL statement
         */
        try {
            res = tableOp("creat table(xxx)", tableLimits, null);
            fail("Should have thrown");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    TableResult tableOp(String statement,
                        TableLimits limits,
                        String tableName) {
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(tableLimits)
            .setTableName(tableName);
        TableResult res = (TableResult) doRequest(tableRequest);
        return waitForCompletion(res.getTableName(), res.getOperationId());
    }

    TableResult waitForCompletion(String tableName, String opId) {

        TableResult res = getTable(tableName, opId);
        TableResult.State state = res.getTableState();
        while (!isTerminal(state)) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {} // ignore
            res = getTable(tableName, opId);
            state = res.getTableState();
        }
        return res;
    }

    TableResult getTable(String tableName, String opId) {

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(opId);
        return (TableResult) doRequest(getTable);
    }

    static boolean isTerminal(TableResult.State state) {
        return state == State.ACTIVE || state == State.DROPPED;
    }

    private Result doRequest(Request request) {
        return doV3Request(request);
    }
}
