/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.Tracker.EventHolder;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.views.LogTracker;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.PerfTracker;
import oracle.kv.impl.monitor.views.PlanStateChangeTracker;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.monitor.views.ServiceStatusTracker;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/** Test event delivery and pruning in Tracker subclasses. */
public class TrackerTest extends TestBase {
    private static final Logger staticLogger =
        LoggerUtils.getLogger(TrackerTest.class, "test");
    private static final ResourceId rId = new RepNodeId(1, 2);
    private static final LatencyInfo EMPTY_SINGLE_CUM =
        new LatencyInfo(PerfStatType.USER_SINGLE_OP_CUM, 0, 0,
                        new LatencyResult());
    private static final LatencyInfo EMPTY_MULTI_INT =
        new LatencyInfo(PerfStatType.USER_MULTI_OP_INT, 0, 0,
                        new LatencyResult());
    private static final LatencyInfo EMPTY_MULTI_CUM =
        new LatencyInfo(PerfStatType.USER_MULTI_OP_CUM, 0, 0,
                        new LatencyResult());

    /* Tests */

    @Test
    public void testServiceStatusTrackerEvents() {
        final MyServiceStatusTracker tracker = new MyServiceStatusTracker();

        /* No items initially */
        RetrievedEvents<ServiceChange> events = tracker.retrieveNewEvents(0);
        assertEquals(0, events.size());

        /* First item */
        newInfo(tracker, 11);
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getChangeTime());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getChangeTime());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(11);
        assertEquals(0, events.size());

        /* Next two items */
        newInfo(tracker, 12);
        newInfo(tracker, 13);

        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getChangeTime());
        assertEquals(13, events.getEvents().get(1).getChangeTime());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getChangeTime());
        assertEquals(13, events.getEvents().get(1).getChangeTime());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(13);
        assertEquals(0, events.size());
    }

    @Test
    public void testServiceStatusTrackerPruning() {
        final MyServiceStatusTracker tracker =
            new MyServiceStatusTracker(10000);

        /* This test depends on the pruning frequency */
        assertEquals(40, ServiceStatusTracker.PRUNE_FREQUENCY);

        /* Events 1 - 20 */
        for (long i = 1; i <= 20; i++) {
            newInfo(tracker, i);
        }
        RetrievedEvents<ServiceChange> events = tracker.retrieveNewEvents(0);
        assertEquals(20, tracker.getQueue().size());
        assertEquals(20, events.size());

        /* Events 21 - 40 */
        for (long i = 21; i <= 40; i++) {
            newInfo(tracker, i);
        }
        events = tracker.retrieveNewEvents(20);
        assertEquals(40, tracker.getQueue().size());
        assertEquals(20, events.size());

        /*
         * Event 41, which causing pruning. Note that pruning is also called
         * for the 1st entry, but that does nothing.
         */
        newInfo(tracker, 41);

        /* The first 20 events were pruned because they were acknowledged */
        assertEquals(21, tracker.getQueue().size());

        events = tracker.retrieveNewEvents(40);
        assertEquals(1, events.size());

        /* Queue QUEUE_MAX events and make sure they are pruned */
        assertEquals(5000, Tracker.QUEUE_MAX);
        long start = 42;
        long end = start + Tracker.QUEUE_MAX;
        for (long i = start; i <= end; i++) {
            newInfo(tracker, i);
        }
        if (tracker.getQueue().size() > (Tracker.QUEUE_MAX + 40)) {
            fail("Tracker queue was not pruned: " +
                 tracker.getQueue().size());
        }

        /* Advance time so that unacknowledged events are pruned */
        tracker.currentTimeMillis += Tracker.QUEUE_UNACKED_MAX_AGE_MS;

        /* Force another prune using events that will not be expired */
        start = Tracker.QUEUE_UNACKED_MAX_AGE_MS - 100;
        end = start + 40;
        for (long i = start; i <= end; i++) {
            newInfo(tracker, i);
        }
        assertEquals(41, tracker.getQueue().size());
    }

    @Test
    public void testPerfTrackerEvents() throws Exception {
        final PerfTracker tracker = new PerfTracker(logger);

        /* No items initially */
        RetrievedEvents<PerfEvent> events = tracker.retrieveNewEvents(0);
        assertEquals(0, events.size());

        /* First item */
        tracker.newInfo(rId, perfEvent(11));
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getSingleInt().getEnd());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getSingleInt().getEnd());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(11);
        assertEquals(0, events.size());

        /* Next two items */
        tracker.newInfo(rId, perfEvent(12));
        tracker.newInfo(rId, perfEvent(13));
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getSingleInt().getEnd());
        assertEquals(13, events.getEvents().get(1).getSingleInt().getEnd());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getSingleInt().getEnd());
        assertEquals(13, events.getEvents().get(1).getSingleInt().getEnd());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(13);
        assertEquals(0, events.size());
    }

    @Test
    public void testPerfTrackerPruning() {
        final MyPerfTracker tracker = new MyPerfTracker(10000);

        /* This test depends on the pruning frequency */
        assertEquals(15, PerfTracker.CHUNK_SIZE);

        /* Events 1 - 10 */
        for (long i = 1; i <= 10; i++) {
            tracker.newInfo(rId, perfEvent(i));
        }
        RetrievedEvents<PerfEvent> events = tracker.retrieveNewEvents(0);
        assertEquals(10, tracker.getQueue().size());
        assertEquals(10, events.size());

        /* Events 11 - 15 */
        for (long i = 11; i <= 15; i++) {
            tracker.newInfo(rId, perfEvent(i));
        }
        events = tracker.retrieveNewEvents(10);
        assertEquals(15, tracker.getQueue().size());
        assertEquals(5, events.size());

        /*
         * Event 16, which causing pruning. Note that pruning is also called
         * for the 1st entry, but that does nothing.
         */
        tracker.newInfo(rId, perfEvent(16));

        /* The first 10 events were pruned because they were acknowledged */
        assertEquals(6, tracker.getQueue().size());

        events = tracker.retrieveNewEvents(15);
        assertEquals(1, events.size());

        /* Queue QUEUE_MAX events and make sure they are pruned */
        assertEquals(5000, Tracker.QUEUE_MAX);
        long start = 15;
        long end = start + Tracker.QUEUE_MAX;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, perfEvent(i));
        }
        if (tracker.getQueue().size() > (Tracker.QUEUE_MAX + 15)) {
            fail("Tracker queue was not pruned: " +
                 tracker.getQueue().size());
        }

        /* Advance time so that unacknowledged events are pruned */
        tracker.currentTimeMillis += Tracker.QUEUE_UNACKED_MAX_AGE_MS;

        /* Force another prune using events that will not be expired */
        start = Tracker.QUEUE_UNACKED_MAX_AGE_MS - 100;
        end = start + 15;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, perfEvent(i));
        }
        assertEquals(16, tracker.getQueue().size());
    }

    @Test
    public void testPlanStateChangeTrackerEvents() {
        final PlanStateChangeTracker tracker = new PlanStateChangeTracker();

        /* No items initially */
        RetrievedEvents<PlanStateChange> events = tracker.retrieveNewEvents(0);
        assertEquals(0, events.size());

        /* First item */
        tracker.newInfo(rId, planStateChange(11));
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getEnd());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getEnd());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(11);
        assertEquals(0, events.size());

        /* Next two items */
        tracker.newInfo(rId, planStateChange(12));
        tracker.newInfo(rId, planStateChange(13));
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getEnd());
        assertEquals(13, events.getEvents().get(1).getEnd());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getEnd());
        assertEquals(13, events.getEvents().get(1).getEnd());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(13);
        assertEquals(0, events.size());
    }

    @Test
    public void testPlanStateChangeTrackerPruning() {
        final MyPlanStateChangeTracker tracker =
            new MyPlanStateChangeTracker(10000);

        /* This test depends on the pruning frequency */
        assertEquals(40, PlanStateChangeTracker.PRUNE_FREQUENCY);

        /* Events 1 - 20 */
        for (long i = 1; i <= 20; i++) {
            tracker.newInfo(rId, planStateChange(i));
        }
        RetrievedEvents<PlanStateChange> events =
            tracker.retrieveNewEvents(0);
        assertEquals(20, tracker.getQueue().size());
        assertEquals(20, events.size());

        /* Events 21 - 40 */
        for (long i = 21; i <= 40; i++) {
            tracker.newInfo(rId, planStateChange(i));
        }
        events = tracker.retrieveNewEvents(20);
        assertEquals(40, tracker.getQueue().size());
        assertEquals(20, events.size());

        /*
         * Event 41, which causing pruning. Note that pruning is also called
         * for the 1st entry, but that does nothing.
         */
        tracker.newInfo(rId, planStateChange(41));

        /* The first 20 events were pruned because they were acknowledged */
        assertEquals(21, tracker.getQueue().size());

        events = tracker.retrieveNewEvents(40);
        assertEquals(1, events.size());

        /* Queue QUEUE_MAX events and make sure they are pruned */
        assertEquals(5000, Tracker.QUEUE_MAX);
        long start = 42;
        long end = start + Tracker.QUEUE_MAX;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, planStateChange(i));
        }
        if (tracker.getQueue().size() > (Tracker.QUEUE_MAX + 40)) {
            fail("Tracker queue was not pruned: " +
                 tracker.getQueue().size());
        }

        /* Advance time so that unacknowledged events are pruned */
        tracker.currentTimeMillis += Tracker.QUEUE_UNACKED_MAX_AGE_MS;

        /* Force another prune using events that will not be expired */
        start = Tracker.QUEUE_UNACKED_MAX_AGE_MS - 100;
        end = start + 40;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, planStateChange(i));
        }
        assertEquals(41, tracker.getQueue().size());
    }

    @Test
    public void testLogTrackerEvents() {
        final LogTracker tracker = new LogTracker(logger);

        /* No items initially */
        RetrievedEvents<LogRecord> events = tracker.retrieveNewEvents(0);
        assertEquals(0, events.size());

        /* First item */
        tracker.newInfo(rId, logRecord(11));
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getMillis());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(0);
        assertEquals(1, events.size());
        assertEquals(11, events.getEvents().get(0).getMillis());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(11);
        assertEquals(0, events.size());

        /* Next two items */
        tracker.newInfo(rId, logRecord(12));
        tracker.newInfo(rId, logRecord(13));
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getMillis());
        assertEquals(13, events.getEvents().get(1).getMillis());

        /* Same result if we ask again */
        events = tracker.retrieveNewEvents(11);
        assertEquals(2, events.size());
        assertEquals(12, events.getEvents().get(0).getMillis());
        assertEquals(13, events.getEvents().get(1).getMillis());

        /* No more items if we supply the latest retrieved time */
        events = tracker.retrieveNewEvents(13);
        assertEquals(0, events.size());
    }

    @Test
    public void testLogTrackerPruning() {
        final MyLogTracker tracker = new MyLogTracker(10000);

        /* This test depends on the pruning frequency */
        assertEquals(40, PlanStateChangeTracker.PRUNE_FREQUENCY);

        /* Events 1 - 20 */
        for (long i = 1; i <= 20; i++) {
            tracker.newInfo(rId, logRecord(i));
        }
        RetrievedEvents<LogRecord> events = tracker.retrieveNewEvents(0);
        assertEquals(20, tracker.getQueue().size());
        assertEquals(20, events.size());

        /* Events 21 - 40 */
        for (long i = 21; i <= 40; i++) {
            tracker.newInfo(rId, logRecord(i));
        }
        events = tracker.retrieveNewEvents(20);
        assertEquals(40, tracker.getQueue().size());
        assertEquals(20, events.size());

        /*
         * Event 41, which causes pruning, but LogTracker always retains at
         * least 39 events when pruning, which happens before the new event is
         * added. Note that pruning is also called for the 1st entry, but that
         * does nothing.
         */
        tracker.newInfo(rId, logRecord(41));
        assertEquals(40, tracker.getQueue().size());

        events = tracker.retrieveNewEvents(40);
        assertEquals(1, events.size());

        /* Events 42 - 59 */
        for (long i = 42; i <= 59; i++) {
            tracker.newInfo(rId, logRecord(i));
        }
        events = tracker.retrieveNewEvents(41);

        assertEquals(58, tracker.getQueue().size());
        assertEquals(18, events.size());

        /* Events 60 - 80 */
        for (long i = 60; i <= 80; i++) {
            tracker.newInfo(rId, logRecord(i));
        }
        events = tracker.retrieveNewEvents(59);
        assertEquals(79, tracker.getQueue().size());
        assertEquals(21, events.size());

        /*
         * Event 81 causes pruning, retaining 40 items, even if some are
         * acknowledged.
         */
        tracker.newInfo(rId, logRecord(81));
        assertEquals(40, tracker.getQueue().size());

        /* Queue QUEUE_MAX events and make sure they are pruned */
        assertEquals(5000, Tracker.QUEUE_MAX);
        long start = 82;
        long end = start + Tracker.QUEUE_MAX;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, logRecord(i));
        }
        if (tracker.getQueue().size() > (Tracker.QUEUE_MAX + 40)) {
            fail("Tracker queue was not pruned: " +
                 tracker.getQueue().size());
        }

        /* Advance time so that unacknowledged events are pruned */
        tracker.currentTimeMillis += Tracker.QUEUE_UNACKED_MAX_AGE_MS;

        /*
         * Force two prunes using events that will not be expired so that all
         * of the earlier events are removed
         */
        start = Tracker.QUEUE_UNACKED_MAX_AGE_MS - 100;
        end = start + 80;
        for (long i = start; i <= end; i++) {
            tracker.newInfo(rId, logRecord(i));
        }

        assertEquals(81, tracker.getQueue().size());
    }

    /* Methods and classes */

    static class MyServiceStatusTracker extends ServiceStatusTracker {
        long currentTimeMillis;
        MyServiceStatusTracker() {
            this(-1);
        }
        MyServiceStatusTracker(long currentTimeMillis) {
            super(null);
            this.currentTimeMillis = currentTimeMillis;
        }
        List<EventHolder<ServiceChange>> getQueue() {
            return queue;
        }
        @Override
        protected long getCurrentTimeMillis() {
            return (currentTimeMillis >= 0) ?
                currentTimeMillis :
                super.getCurrentTimeMillis();
        }
    }

    void newInfo(MyServiceStatusTracker tracker, long time) {

        /*
         * Alternate between RUNNING and STOPPED status values so that all
         * changes get recorded
         */
        final List<EventHolder<ServiceChange>> queue = tracker.getQueue();
        final ServiceStatus lastStatus = queue.isEmpty() ?
            ServiceStatus.STOPPED :
            queue.get(queue.size() - 1).getEvent().getStatus();
        tracker.newInfo(rId,
                        new ServiceStatusChange(
                            (lastStatus == ServiceStatus.STOPPED ?
                             ServiceStatus.RUNNING :
                             ServiceStatus.STOPPED),
                            time));
    }

    static class MyPerfTracker extends PerfTracker {
        long currentTimeMillis;
        MyPerfTracker(long currentTimeMillis) {
            super(staticLogger);
            this.currentTimeMillis = currentTimeMillis;
        }
        List<EventHolder<PerfEvent>> getQueue() {
            return queue;
        }
        @Override
        protected long getCurrentTimeMillis() {
            return currentTimeMillis;
        }
    }

    PerfEvent perfEvent(long time) {
        return new PerfEvent(
            rId,
            new LatencyInfo(
                PerfStatType.USER_SINGLE_OP_INT, 0, time,
                new LatencyResult(1, 1, 1, 1, 1, 1, 1, 1)),
            EMPTY_SINGLE_CUM, 0, 0, EMPTY_MULTI_INT, EMPTY_MULTI_CUM);
    }

    static class MyPlanStateChangeTracker extends PlanStateChangeTracker {
        long currentTimeMillis;
        MyPlanStateChangeTracker(long currentTimeMillis) {
            this.currentTimeMillis = currentTimeMillis;
        }
        List<EventHolder<PlanStateChange>> getQueue() {
            return queue;
        }
        @Override
        protected long getCurrentTimeMillis() {
            return currentTimeMillis;
        }
    }

    PlanStateChange planStateChange(long time) {
        return new PlanStateChange(1, "plan1", Plan.State.PENDING, 1, "msg",
                                   time);
    }

    static class MyLogTracker extends LogTracker {
        long currentTimeMillis;
        MyLogTracker(long currentTimeMillis) {
            super(staticLogger);
            this.currentTimeMillis = currentTimeMillis;
        }
        List<EventHolder<LogRecord>> getQueue() {
            return queue;
        }
        @Override
        protected long getCurrentTimeMillis() {
            return currentTimeMillis;
        }
    }

    private LogRecord logRecord(long time) {
        final LogRecord record = new LogRecord(Level.SEVERE, "msg");
        setRecordMillis(record, time);
        return record;
    }

    private void setRecordMillis(LogRecord record, long time) {
        /*
         * Use reflection to set the time to avoid a warning for setMillis
         * being deprecated in Java 11 but not in Java 8. Switch to using
         * setInstant when we switch to Java 9 compatibility.
         */
        try {
            LogRecord.class.getMethod("setMillis", Long.TYPE)
                .invoke(record, time);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
    }
}
