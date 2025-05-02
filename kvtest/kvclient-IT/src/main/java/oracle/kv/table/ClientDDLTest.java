/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.ExecutionFuture;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.api.ops.ClientTestBase;

import org.junit.Test;

/**
 * Test basic DDL operations using only the kvclient jar.
 */
public class ClientDDLTest extends ClientTestBase {

    @Override
    public void setUp() throws Exception {

        checkRMIRegistryLookup();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Test
    public void testBasicAPI()
        throws Exception {

        String statement =
            "CREATE TABLE users (id INTEGER, name STRING, age INTEGER, " +
            " PRIMARY KEY (id))";
        statement = TestBase.addRegionsForMRTable(statement);
        oracle.kv.ExecutionFuture f = store.execute(statement);
        StatementResult result = f.get();
        isSuccessfulExecution(statement, result, f);

        /* Check status again */
        result = f.updateStatus();
        isSuccessfulExecution(statement, result, f);

        /* Create an index */
        statement = "CREATE INDEX age on users(age)";
        result = store.executeSync(statement);
        isSuccessfulExecution(statement, result, null);

        /* Should fail */
        try {
            store.execute("CREATE FOO");
            fail("bad statement");
        } catch (IllegalArgumentException expected) {
        }

        try {
            store.executeSync("CREATE INDEX foo on users(foo)");
            fail("bad statement");
        } catch (IllegalArgumentException expected) {
        }
    }

    private void isSuccessfulExecution(String statement,
                                       StatementResult result,
                                       ExecutionFuture f) {
        assertTrue(result.isSuccessful());
        assertTrue(result.getPlanId() != 0);
        assertTrue(result.getInfo() != null);
        assertTrue(result.getInfoAsJson() != null);
        assertTrue(result.getErrorMessage() == null);

        /* Future can be null for sync execution */
        if (f != null) {
            assertTrue(f.isDone());
            assertFalse(f.isCancelled());
            assertEquals(statement, f.getStatement());
        }
    }
}
