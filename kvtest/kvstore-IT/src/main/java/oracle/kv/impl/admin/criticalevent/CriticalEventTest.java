/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.criticalevent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventKey;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventType;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import org.junit.Test;

/**
 * Exercises the CriticalEvent class and subclasses.
 */
public class CriticalEventTest extends TestBase {

    LogRecord lr;
    PerfEvent pe;
    ServiceChange sc;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        lr = new LogRecord(Level.SEVERE, "A fake log message");

        /* Construct a fake PerfEvent. */
        final LatencyInfo li = new LatencyInfo
            (PerfStatType.PUT_IF_ABSENT_INT, System.currentTimeMillis(),
             System.currentTimeMillis() + 100,
             new LatencyResult(20, 20, 1, 100, 10, 95, 99, 0));

        pe = new PerfEvent(new RepNodeId(77, 3), li, li,
                           0, 0, li, li);

        /* And a fake ServiceChange. */
        final ServiceStatusChange ssc =
            new ServiceStatusChange(ServiceStatus.ERROR_NO_RESTART);
        sc = new ServiceChange(new RepNodeId(55, 2), ssc);
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * Exercise encoding and decoding of event objects and keys, using real
     * timestamps.
     */
    @Test
    public void testBasic() {

        final long lrTs = System.currentTimeMillis();
        final CriticalEvent celr = new CriticalEvent(lrTs, lr);
        final long peTs = System.currentTimeMillis();
        final CriticalEvent cepe = new CriticalEvent(peTs, pe);
        final long scTs = System.currentTimeMillis();
        final CriticalEvent cesc = new CriticalEvent(scTs, sc);

        assertEquals(celr.getEventType(), EventType.LOG);
        assertEquals(cepe.getEventType(), EventType.PERF);
        assertEquals(cesc.getEventType(), EventType.STAT);

        assertEquals(lrTs, celr.getSyntheticTimestamp());
        assertEquals(peTs, cepe.getSyntheticTimestamp());
        assertEquals(scTs, cesc.getSyntheticTimestamp());

        /*
         * Exercise toString() for each type.
         */
        final String celrStr = celr.toString();
        assertTrue(celrStr.contains(
                       lr.getLevel().getLocalizedName() + " " +
                       lr.getMessage()));
        assertTrue(celrStr.contains(" LOG "));

        final String cepeStr = cepe.toString();
        assertTrue(cepeStr.contains(pe.getResourceId().toString()));
        assertTrue(cepeStr.contains(" PERF "));

        final String cescStr = cesc.toString();
        assertTrue(cescStr.contains(sc.getTarget().toString()));
        assertTrue(cescStr.contains(" STAT "));

        /*
         * Exercise getDetailString() for each type.
         */
        final String celrDet = celr.getDetailString();
        assertTrue(celrDet.contains(
                       lr.getLevel().getLocalizedName() + " " +
                       lr.getMessage()));
        assertTrue(celrDet.contains(" LOG "));

        final String cepeDet = cepe.getDetailString();
        assertTrue(cepeDet.contains(pe.getResourceId().toString()));
        assertTrue(cepeDet.contains(" PERF "));

        final String cescDet = cesc.getDetailString();
        assertTrue(cescDet.contains(sc.getTarget().toString()));
        assertTrue(cescDet.contains(" STAT "));

        final EventKey keylr = celr.getKey();
        final String keystrlr = keylr.toString();
        final EventKey keylr2 = EventKey.fromString(keystrlr);
        assertEquals(keylr.getSyntheticTimestamp(),
                     keylr2.getSyntheticTimestamp());
        assertEquals(keylr.getCategory(), keylr2.getCategory());

        final EventKey keype = cepe.getKey();
        final String keystrpe = keype.toString();
        final EventKey keype2 = EventKey.fromString(keystrpe);
        assertEquals(keype.getSyntheticTimestamp(),
                     keype2.getSyntheticTimestamp());
        assertEquals(keype.getCategory(), keype2.getCategory());

        final EventKey keysc = cesc.getKey();
        final String keystrsc = keysc.toString();
        final EventKey keysc2 = EventKey.fromString(keystrsc);
        assertEquals(keysc.getSyntheticTimestamp(),
                     keysc2.getSyntheticTimestamp());
        assertEquals(keysc.getCategory(), keysc2.getCategory());

        assertEquals(lr.getMessage(), celr.getLogEvent().getMessage());
        assertEquals(pe.getResourceId(), cepe.getPerfEvent().getResourceId());
        assertEquals(sc.getTarget(), cesc.getStatusEvent().getTarget());

        /*
         * Verify exception cases when wrong event type is selected
         */
        try {
            celr.getPerfEvent();
            fail("expected LogRecord event to fail for getPerfEvent()");
        } catch (NonfatalAssertionException nae) {
            assertEquals(nae.getClass(), NonfatalAssertionException.class);
        }

        try {
            cepe.getStatusEvent();
            fail("expected Perf event to fail for getStatusEvent()");
        } catch (NonfatalAssertionException nae) {
            assertEquals(nae.getClass(), NonfatalAssertionException.class);
        }

        try {
            cesc.getLogEvent();
            fail("expected Status event to fail for getLogEvent()");
        } catch (NonfatalAssertionException nae) {
            assertEquals(nae.getClass(), NonfatalAssertionException.class);
        }
    }

    private void verifyKeyStringEncoding(long ts) {

        final CriticalEvent celr = new CriticalEvent(ts, lr);
        final EventKey keylr = celr.getKey();
        final String keystrlr = keylr.toString();
        final EventKey keylr2 = EventKey.fromString(keystrlr);
        assertEquals(keylr.getSyntheticTimestamp(),
                     keylr2.getSyntheticTimestamp());
        assertEquals(keylr.getCategory(), keylr2.getCategory());
    }

    /**
     * Try a broad range of synthetic timestamp values.
     */
    @Test
    public void testKeyStringEncoding() {

        long i = 1;
        for (int e = 1; e < 64; e++) {

            verifyKeyStringEncoding(i);
            i *= 2; /* i = 2^(e+1) */
        }

        /* Also verify timestamp value 0 */
        verifyKeyStringEncoding(0L);
    }
}




