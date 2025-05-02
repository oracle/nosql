/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.table.RecordValue;

public class DDLTestUtils {

    /**
     * Assert that all fields in the future and result are correct for a
     * successful execution.
     * @param future may be null.
     */
    public static void checkSuccess(ExecutionFuture future,
        StatementResult result) {
        if (future != null) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        }

        assertTrue(result.toString(), result.isSuccessful());
        assertNull(result.toString(), result.getErrorMessage());
        assertTrue(result.isDone());
        assertFalse(result.isCancelled());
        assertNotNull(result.toString(), result.getInfo());
    }

    public static void execStatement(KVStore store, String statement)
        throws Exception {

        ExecutionFuture future = store.execute(statement);
        StatementResult result = future.get();
        checkSuccess(future, result);
    }

    public static void execQueryStatement(KVStore store, String statement,
        int expectedResults)
        throws Exception {

        ExecutionFuture future = store.execute(statement);
        StatementResult result = future.get();
        checkQuerySuccess(future, result, expectedResults);
    }

    /**
     * Assert that all fields in the future and result are correct for a
     * successful execution.
     * @param future may be null.
     */
    public static void checkQuerySuccess(ExecutionFuture future,
        StatementResult result, int expectedResults) {
        if (future != null) {
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        }

        assertEquals(StatementResult.Kind.QUERY, result.getKind());
        assertTrue(result.toString(), result.isSuccessful());
        assertNull(result.toString(), result.getErrorMessage());
        assertFalse(result.isCancelled());
        assertNull(result.getInfo());

        if (expectedResults < 0)
            return;

        int results = 0;
        for (RecordValue r : result) {
            assertNotNull(r);
            results++;
        }
        assertEquals(expectedResults, results);
    }
}
