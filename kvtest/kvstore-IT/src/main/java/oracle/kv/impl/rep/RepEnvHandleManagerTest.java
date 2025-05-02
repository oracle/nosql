/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static oracle.kv.impl.param.ParameterState.JVM_OVERHEAD_PERCENT_DEFAULT;
import static oracle.kv.impl.param.ParameterState.SN_RN_HEAP_PERCENT_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests for the RepEnvHandleManager class.
 */
public class RepEnvHandleManagerTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testCheckMemoryAllocation() {
        final RecordingHandler recordingHandler = new RecordingHandler();
        tearDowns.add(() -> logger.removeHandler(recordingHandler));
        logger.addHandler(recordingHandler);

        /* Defaults */
        assertTrue(RepEnvHandleManager.
                   checkMemoryAllocation(
                       Integer.parseInt(SN_RN_HEAP_PERCENT_DEFAULT),
                       Integer.parseInt(JVM_OVERHEAD_PERCENT_DEFAULT),
                       32 * 1024, /* snMemorySizeMB */
                       1, /* capacity */
                       32 * 1024, /* rnMaxHeapMB */
                       logger));

        assertEquals(1, recordingHandler.records.size());
        LogRecord record = recordingHandler.records.remove(0);
        assertEquals(Level.INFO, record.getLevel());
        String message = record.getMessage();
        assertTrue(message,
                   message.startsWith("SN memory allocation parameters:"));

        /*
         * All Java memory percent too large. Note that rnHeapPercent is
         * restricted to a maximum of 95%, so we can't get beyond 100% overall
         * with just rnHeapPercent.
         */
        assertFalse(RepEnvHandleManager.
                    checkMemoryAllocation(
                        80, /* rnHeapPercent */
                        50, /* jvmOverheadPercent */
                        32 * 1024, /* snMemorySizeMB */
                        1, /* capacity */
                        32 * 1024, /* rnMaxHeapMB */
                        logger));
        assertEquals(1, recordingHandler.records.size());
        record = recordingHandler.records.remove(0);
        assertEquals(Level.WARNING, record.getLevel());
        message = record.getMessage();
        assertTrue(message,
                   message.startsWith("Problem with SN memory allocation"));
        assertTrue(message, message.contains("Java all memory"));
        assertFalse(message, message.contains("All RNs max heap"));
    }

    static class RecordingHandler extends StreamHandler {
        final List<LogRecord> records = new ArrayList<>();
        @Override
        public synchronized void publish(LogRecord record) {
            if (record != null) {
                records.add(record);
            }
        }
    }
}
