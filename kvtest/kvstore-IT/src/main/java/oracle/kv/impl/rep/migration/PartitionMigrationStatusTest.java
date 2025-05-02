/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;

import org.junit.Test;

/**
 */
public class PartitionMigrationStatusTest extends TestBase {

    @Test
    public void testTargetStatus() {

        final PartitionMigrationState state = PartitionMigrationState.PENDING;
        final int partition = 1;
        final int targetShard = 2;
        final int sourceShard = 3;
        final long requestTime = System.currentTimeMillis();
        final long startTime = requestTime + 10000;
        final long endTime = startTime + 10000;
        final long operations = 7L;
        final int attempts = 8;
        final int busyResponses = 9;
        final int errors = 10;

        final PartitionMigrationStatus status =
                        new PartitionMigrationStatus(state,
                                                     partition,
                                                     targetShard,
                                                     sourceShard,
                                                     operations,
                                                     requestTime,
                                                     startTime,
                                                     endTime,
                                                     attempts,
                                                     busyResponses,
                                                     errors);
        assertTrue(status.forTarget());
        assertEquals(status.getState(), state);
        assertEquals(status.getPartition(), partition);
        assertEquals(status.getTargetShard(), targetShard);
        assertEquals(status.getSourceShard(), sourceShard);
        assertEquals(status.getOperations(), operations);
        assertEquals(status.getRequestTime(), requestTime);
        assertEquals(status.getStartTime(), startTime);
        assertEquals(status.getEndTime(), endTime);
        assertEquals(status.getAttempts(), attempts);
        assertEquals(status.getBusyResponses(), busyResponses);
        assertEquals(status.getErrors(), errors);

        /* Create a map of the data */
        final Map<String, String> map = status.toMap();
        assertEquals(map.size(), 10);

        /* Make a copy from the map and make sure its the same */
        check(status, PartitionMigrationStatus.parseTargetStatus(map));
    }

    @Test
    public void testSourceStatus() {

        final int partition = 1;
        final int targetShard = 2;
        final int sourceShard = 3;
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 10000;
        final long operations = 6L;
        final long recordsSent = 7L;
        final long clientOpsSent = 8L;
        final PartitionMigrationStatus status =
                        new PartitionMigrationStatus(partition,
                                                     targetShard,
                                                     sourceShard,
                                                     operations,
                                                     startTime,
                                                     endTime,
                                                     recordsSent,
                                                     clientOpsSent);
        assertTrue(status.forSource());
        assertNull(status.getState());
        assertEquals(status.getPartition(), partition);
        assertEquals(status.getTargetShard(), targetShard);
        assertEquals(status.getSourceShard(), sourceShard);
        assertEquals(status.getOperations(), operations);
        assertEquals(status.getStartTime(), startTime);
        assertEquals(status.getEndTime(), endTime);
        assertEquals(status.getRecordsSent(), recordsSent);
        assertEquals(status.getClientOpsSent(), clientOpsSent);

        final Map<String, String> map = status.toMap();
        assertEquals(map.size(), 7);

        check(status, PartitionMigrationStatus.parseSourceStatus(map));
    }

    private void check(PartitionMigrationStatus status,
                       PartitionMigrationStatus copy) {
        assertEquals(status.forTarget(), copy.forTarget());
        assertEquals(status.forSource(), copy.forSource());
        assertEquals(status.getState(), copy.getState());
        assertEquals(status.getPartition(), copy.getPartition());
        assertEquals(status.getSourceShard(), copy.getSourceShard());
        assertEquals(status.getTargetShard(), copy.getTargetShard());
        assertEquals(status.getRequestTime(), copy.getRequestTime());
        assertEquals(status.getStartTime(), copy.getStartTime());
        assertEquals(status.getEndTime(), copy.getEndTime());
        assertEquals(status.getAttempts(), copy.getAttempts());
        assertEquals(status.getBusyResponses(), copy.getBusyResponses());
        assertEquals(status.getErrors(), copy.getErrors());
        assertEquals(status.getRecordsSent(), copy.getRecordsSent());
        assertEquals(status.getClientOpsSent(), copy.getClientOpsSent());
    }   
}
