/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.async;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.DialogContextImpl;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.utilint.CommonLoggerUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the buffer slices allocated from IO buffer pool.
 *
 * <p>
 * Allocated slices may be forked and passed around for read and write. The
 * slices maintain reference counts and freed to the pool when the reference
 * count is zero (see {@link IOBufSliceImpl}). Incorrect reference counting or
 * missed free operation will result in a leak on the IO buffer pool. This
 * class is used to instrument and track the slice events in order to solve the
 * leak problem.
 *
 * <p>
 * A previous instrumentation attempt encounters two issues: (1) A heavy-weight
 * mechanism which must be explicitly enabled and hence is only useful for
 * obvious issues, but cannot help when the leak issue is hard to reproduce,
 * e.g., only triggered in stress tests or production environment; (2) Only the
 * purpose of the allocated slices are recorded but not internally events which
 * I find it very hard to debug: the problem of tracking down a leak issue is
 * to locate what did not happen when we expect it to happen; therefore, we can
 * only insert enough critical event records to deduce the ones that are
 * missing.
 *
 * <p>
 * This implementation solves the above two problem. First, we employ a
 * mechanism that can be always enabled. We ensure that the instrumentation is
 * lightweight enough to be always online. A sampling strategy is used which
 * can ensure that the CPU resources used for tracking is trivial comparing to
 * a normal load. Note that the IOBufSliceImpl class implements the reference
 * counting which is always in place. The sampling is to collect event
 * information and to detect leaks.
 *
 * <p>
 * TODO: It is worth adding a mechanism that keeps track of allocated and
 * records their latest use time. The purpose of this mechanism is to always
 * detect leak as opposed to the sampling mechanism implemented here. The
 * IOBufSliceImpl.PooledBufSlice class previously implements the finalize
 * method that can always detect leak, but that approach resulted in a
 * performance regression; another approach is needed [KVSTORE-966].
 *
 * <p>
 * Furthermore, we bound of the number of records to ensure a limited memory
 * and IO resource used to hold and log the records. To solve the second issue,
 * we extensively instrument the code for buffer slice events including channel
 * read/write, dialog read/write, etc. We also add channel and dialog info
 * suppliers to the records, so that we can obtain the latest states on the
 * related objects during logging.
 */
public class IOBufPoolTrackers {

    public static final String JSON_LINE_LOGGING_NAME = "IOBufPoolTrackers";
    public static final String JSON_LINE_ERROR_LOGGING_NAME =
        "IOBufPoolTrackersErrors";

    /*
     * Controlling the rate control from system property. One buffer/slice is
     * sampled out of the number specified. The value needs to be a positive
     * number. A negative or zero value is not allowed. The sampling is tested
     * with mod. Testing with logic AND to a pow of 2 sample rate is faster but
     * since both latencies are on the magnitude of ns, it seems not worth the
     * trouble.
     */
    public static final long DEFAULT_SAMPLE_RATE = 1024;
    public static final String SAMPLE_RATE_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.samplerate";
    /*
     * Controlling the max number of trackers from system property. Each
     * tracker tracks one buffer/slice created from the pool. When the number
     * of trackers not removed reaches, the sampling is stopped. If there is a
     * leak, some trackers will stay forever which will stop the tracking for
     * new buffers/slices.
     */
    public static final int DEFAULT_MAX_NUM_TRACKERS = 2;
    public static final
        String MAX_NUM_TRACKERS_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.max.num.trackers";
    /*
     * Controlling the max number of slice entries per tracker to output from
     * the system property. We do a depth-first-search from the root slice to
     * its children to iterate throught the entries of each tracker so that the
     * output can show a direct cause of why the parent is not released
     * correctly. The slice fork tree has a small depth (around 2 to 3), i.e.,
     * the async code has only 2 to 3 layers that a pool slice is passed
     * through and forked. Therefore, we should not need a large number of
     * tracked slices to figure out the problem.
     */
    public static final int DEFAULT_MAX_NUM_SLICES = 8;
    public static final String MAX_NUM_SLICES_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.max.num.slices";
    /*
     * Controlling the max number of events to output per slice entry from the
     * system property. Events are important for figuring out what happens to a
     * slice. Many slices usually have 3 to 4 events though.
     */
    public static final int DEFAULT_MAX_NUM_EVENTS = 16;
    public static final String MAX_NUM_EVENTS_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.max.num.events";
    /*
     * Controlling the logging interval from the system property.
     */
    public static final long DEFAULT_LOGGING_INTERVAL_MILLIS = 10 * 60 * 1000;
    public static final String LOGGING_INTERVAL_MILLIS_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.logging.interval.millis";
    /*
     * Controlling the logging delay time for a tracker from the system
     * property. Trackers held may not indicate a leak but simply the slices
     * are in use. This parameter controls the time the trackers are held
     * before we suspect a leak and being logged.
     */
    public static final long DEFAULT_TRACKER_LOGGING_DELAY_MILLIS = 10 * 1000;
    public static final String TRACKER_LOGGING_DELAY_MILLIS_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.tracker.logging.delay.millis";
    /*
     * Controlling the maximum number of errors to record from the system
     * property. If there is some invarients that are violated, we put the
     * errors in a global list of the IOBufPoolTrackers class. We limit the
     * number of entries in the list to ensure a bounded resource consumption.
     */
    public static final int DEFAULT_MAX_NUM_ERRORS = 8;
    public static final String MAX_NUM_ERRORS_PROPERTY =
        "oracle.kv.async.iobufpool.trackers.max.num.errors";

    /**
     * Test hook for assigning slice or byte buffer identity.
     */
    public static volatile @Nullable
        Function<Object, Integer> getIdentityHook = null;

    /**
     * A singleton null tracker that does nothing for instrumentations that are
     * not sampled. This tracker is used when the {@link EventTrackersManager}
     * is not available and thus it does not have a logger.
     */
    private static final NullTracker NO_LOGGER_NULL_TRACKER =
        new NullTracker();
    /*
     * A null tracker but contains the rate limiting logger created from the
     * logger of the event trackers manager.
     */
    private final NullTracker nullTracker;

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitingLogger;
    private final ScheduledExecutorService executor;
    /*
     * Global reference time for computing trackers creation times. Millis time
     * for print. Nanos time for sorting the trackers.
     */
    private final long referenceTimeMillis = System.currentTimeMillis();
    private final long referenceTimeNanos = System.nanoTime();
    /* Configurations */
    private static final String SAMPLE_RATE = "Sample rate";
    private volatile long sampleRate =
        ensureLargerThanZero(
            Long.getLong(SAMPLE_RATE_PROPERTY, DEFAULT_SAMPLE_RATE),
            SAMPLE_RATE);
    private static final String MAX_NUM_TRACKERS = "Max number of trackers";
    private volatile int maxNumTrackers =
        ensureLargerThanZero(
            Integer.getInteger(MAX_NUM_TRACKERS_PROPERTY,
                               DEFAULT_MAX_NUM_TRACKERS),
            MAX_NUM_TRACKERS);
    private static final String MAX_NUM_SLICES = "Max number of slices";
    private volatile int maxNumSlices =
        ensureLargerThanZero(
            Integer.getInteger(MAX_NUM_SLICES_PROPERTY,
                               DEFAULT_MAX_NUM_SLICES),
            MAX_NUM_SLICES);
    private static final String MAX_NUM_EVENTS = "Max number of events";
    private volatile int maxNumEvents =
        ensureLargerThanZero(
            Integer.getInteger(MAX_NUM_EVENTS_PROPERTY,
                               DEFAULT_MAX_NUM_EVENTS),
            MAX_NUM_EVENTS);
    private static final String LOGGING_INTERVAL = "Logging interval";
    private volatile long loggingIntervalMillis =
        ensureLargerThanZero(
            Long.getLong(LOGGING_INTERVAL_MILLIS_PROPERTY,
                         DEFAULT_LOGGING_INTERVAL_MILLIS),
            LOGGING_INTERVAL);
    private static final String LOGGING_DELAY = "Logging delay";
    private volatile long trackerLoggingDelayMillis =
        ensureLargerThanZero(
            Long.getLong(TRACKER_LOGGING_DELAY_MILLIS_PROPERTY,
                         DEFAULT_TRACKER_LOGGING_DELAY_MILLIS),
            LOGGING_DELAY);
    private static final String MAX_NUM_ERRORS = "Max number of error";
    private volatile long maxNumErrors =
        ensureLargerThanZero(
            Long.getLong(MAX_NUM_ERRORS_PROPERTY,
                         DEFAULT_MAX_NUM_ERRORS),
            MAX_NUM_ERRORS);

    /*
     * The future of the logging task, must synchronize on this object when
     * accessing this field.
     */
    private @Nullable Future<?> loggingTaskFuture = null;
    private volatile long lastLoggingTimeMillis = System.currentTimeMillis();

    /*
     * Maps the tracker type to the count of encountered createTracker calls on
     * the type. Used for sampling. The map is not modified outside the
     * constructor and thus synchronization is not necessary.
     */
    private final HashMap<TrackerType, AtomicLong> createTrackerCount =
        new HashMap<>();
    /* Trackers */
    private final ConcurrentHashMap<TrackerType, Set<TrackerImpl>>
        trackers = new ConcurrentHashMap<>();
    /* Records unexpected errors for our instrumentation. */
    private final ConcurrentLinkedQueue<Throwable> errors =
        new ConcurrentLinkedQueue<>();

    /* Null infos */
    private static final JsonNode NULL_ENDPOINT_HANDLER_INFO =
        JsonUtils.createJsonNode("null endpoint handler");
    private static final JsonNode NULL_DIALOG_INFO =
        JsonUtils.createJsonNode("null dialog");

    public IOBufPoolTrackers(Logger logger,
                             ScheduledExecutorService executor) {
        this.logger = logger;
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            10 * 1000 /* 10 seconds */,
            3 /* types of the channel pool */,
            logger);
        this.nullTracker = new NullTracker(rateLimitingLogger);
        this.executor = executor;
        Stream.of(TrackerType.values()).forEach(
            (t) -> createTrackerCount.put(t, new AtomicLong(0)));
        Stream.of(TrackerType.values()).forEach(
            (t) -> trackers.put(t, new ConcurrentSkipListSet<>()));
        logger.log(Level.FINE,
                   () ->
                   String.format(
                       "IOBufPoolTrackers, " +
                       "sampleRate=%s, maxNumTrackers=%s, " +
                       "maxNumSlices=%s, maxNumEvents=%s, " +
                       "loggingInterval=%s, loggingDelay=%s, " +
                       "maxNumErrors=%s",
                       sampleRate, maxNumTrackers,
                       maxNumSlices, maxNumEvents,
                       loggingIntervalMillis, trackerLoggingDelayMillis,
                       maxNumErrors));
    }

    private long ensureLargerThanZero(long val, String description) {
        if (val <= 0) {
            throw new IllegalArgumentException(String.format(
                "%s must be larger than zero", description));
        }
        return val;
    }

    private int ensureLargerThanZero(int val, String description) {
        if (val <= 0) {
            throw new IllegalArgumentException(String.format(
                "%s must be larger than zero", description));
        }
        return val;
    }

    /** Schedules the logging task. Made public for testing. */
    public synchronized void scheduleLoggingTask() {
        if (loggingTaskFuture != null) {
            return;
        }
        loggingTaskFuture = executor.scheduleWithFixedDelay(
            this::logTrackers, loggingIntervalMillis, loggingIntervalMillis,
            TimeUnit.MILLISECONDS);
    }

    /** Cancels the logging task, for testing. */
    public synchronized void cancelLoggingTask() {
        if (loggingTaskFuture != null) {
            loggingTaskFuture.cancel(false);
            loggingTaskFuture = null;
        }
    }

    /**
     * Logs the trackers. Removes the tracker once it is logged so that we do
     * not flood the log by repeatedly logging the same tracker.
     *
     * TODO: we probably should log to the separate *.stat file
     * (LoggerUtils.getStatFileLogger) so that these entries are more likely to
     * survive log rotation.
     */
    public void logTrackers() {
        logTrackers(true);
    }

    /**
     * Logs the trackers.
     */
    public void logTrackers(boolean clear) {
        final long currentTimeMillis = System.currentTimeMillis();
        final ArrayNode leakJson =
            getLeakTrackersJson(currentTimeMillis, clear);
        if (!leakJson.isEmpty()) {
            logger.log(
                Level.INFO,
                () ->
                FormatUtils.toJsonLine(
                    JSON_LINE_LOGGING_NAME,
                    lastLoggingTimeMillis, currentTimeMillis,
                    leakJson));
        }
        final ArrayNode errorJson = getErrorJson(clear);
        if (!errorJson.isEmpty()) {
            logger.log(
                Level.INFO,
                () ->
                FormatUtils.toJsonLine(
                    JSON_LINE_ERROR_LOGGING_NAME,
                    lastLoggingTimeMillis, currentTimeMillis,
                    errorJson));
        }
        lastLoggingTimeMillis = currentTimeMillis;
    }

    /**
     * Returns the json array describing the leaking trackers.
     */
    public ArrayNode getLeakTrackersJson(long currentTimeMillis,
                                         boolean clear) {
        final ArrayNode result = JsonUtils.createArrayNode();
        for (Set<TrackerImpl> trackerSet : trackers.values()) {
            if (trackerSet.isEmpty()) {
                continue;
            }
            final Iterator<TrackerImpl> iter = trackerSet.iterator();
            while (iter.hasNext()) {
                final TrackerImpl tracker = iter.next();
                if (tracker.getCreationTimeMillis()
                    >= currentTimeMillis - trackerLoggingDelayMillis) {
                    break;
                }
                result.add(tracker.toJson());
                if (clear) {
                    iter.remove();
                }
            }
        }
        return result;
    }

    /**
     * Returns the json node describing the errors.
     */
    public ArrayNode getErrorJson(boolean clear) {
        final ArrayNode result = JsonUtils.createArrayNode();
        if (!errors.isEmpty()) {
            final Iterator<Throwable> iter = errors.iterator();
            while (iter.hasNext()) {
                final Throwable t = iter.next();
                result.add(
                    JsonUtils.createJsonNode(
                        CommonLoggerUtils.getStackTrace(t)));
                if (clear) {
                    iter.remove();
                }
            }
        }
        return result;
    }

    Tracker createTracker(TrackerType trackerType, ByteBuffer buf) {
        if (shouldTrack(trackerType)) {
            return new TrackerImpl(trackerType, buf);
        }
        return nullTracker;
    }

    Tracker getNullTracker() {
        return nullTracker;
    }

    private boolean shouldTrack(TrackerType trackerType) {
        final long cnt = createTrackerCount.get(trackerType).incrementAndGet();
        if ((cnt % sampleRate == 0) &&
            (trackers.get(trackerType).size() < maxNumTrackers)) {
            return true;
        }
        return false;
    }

    void add(TrackerImpl tracker) {
        trackers.computeIfAbsent(tracker.type,
                                 (k) -> new ConcurrentSkipListSet<>())
            .add(tracker);
    }

    void remove(TrackerImpl tracker) {
        trackers.computeIfAbsent(tracker.type,
                                 (k) -> new ConcurrentSkipListSet<>())
            .remove(tracker);
    }

    private void addError(Throwable t) {
        if (errors.size() < maxNumErrors) {
            errors.add(t);
        }
    }

    /**
     * Tracker types.
     */
    public enum TrackerType {
        CHANNEL_INPUT,
        CHANNEL_OUTPUT,
        MESSAGE_OUTPUT,
    }

    /**
     * Event types.
     */
    public enum EventType {
        NEW,
        FORK,
        RELEASE,
        FREE,
        INFO,
    }

    /**
     * The common tracker class passing around for code instrumentation.
     *
     * The class is implemented by the NullTracker that does nothing and the
     * real Tracker.
     */
    public interface Tracker {

        void markNew(IOBufSliceImpl slice, int refcnt);
        void markFork(IOBufSliceImpl slice, int refcnt, IOBufSliceImpl child);
        void markRelease(IOBufSliceImpl slice,
                         int refcnt,
                         IOBufSliceImpl child);
        void markFree(IOBufSliceImpl slice, int refcnt);
        void addInfo(IOBufSliceImpl slice, Supplier<JsonNode> supplier);
        boolean isNull();
        JsonNode toJson();
        @Nullable RateLimitingLogger<String> getLogger();
    }

    /**
     * The null tracker that does nothing.
     */
    private static class NullTracker implements Tracker {

        private final @Nullable RateLimitingLogger<String> logger;

        private NullTracker() {
            this(null);
        }

        private NullTracker(@Nullable RateLimitingLogger<String> logger) {
            this.logger = logger;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public void markNew(IOBufSliceImpl obj, int refcnt) {
        }

        @Override
        public void markFork(IOBufSliceImpl obj,
                             int refcnt,
                             IOBufSliceImpl child) {
        }

        @Override
        public void markRelease(IOBufSliceImpl obj,
                                int refcnt,
                                IOBufSliceImpl child) {
        }

        @Override
        public void markFree(IOBufSliceImpl obj, int refcnt) {
        }

        @Override
        public void addInfo(IOBufSliceImpl obj,
                            Supplier<JsonNode> supplier) {
        }

        @Override
        public JsonNode toJson() {
            return JsonUtils.createJsonNull();
        }

        @Override
        public @Nullable RateLimitingLogger<String> getLogger() {
            return logger;
        }
    }

    /**
     * The real tracker that stores the events.
     *
     * The tracker maintains the events for each slice. It also maintains the
     * tree structure of the parents and the forked children slices so that
     * when we can go through the slices in an orderly manner. Trackers are
     * ordered based on the creation time so that trackers are logged in a
     * orderly manner.
     */
    public class TrackerImpl implements Tracker, Comparable<TrackerImpl> {

        private final long creationTimeNanos = System.nanoTime();
        private final TrackerType type;
        private final ByteBuffer key;
        private @Nullable IOBufSliceImpl root = null;
        private final Map<IOBufSliceImpl, List<Event>> events =
            new HashMap<>();
        private final Map<IOBufSliceImpl, IOBufSliceImpl> parentMap =
            new HashMap<>();
        private final Map<IOBufSliceImpl, Set<IOBufSliceImpl>> childrenMap =
            new HashMap<>();

        public TrackerImpl(TrackerType trackerType,
                           ByteBuffer trackerKey) {
            this.type = trackerType;
            this.key = trackerKey;
            add(this);
        }

        public long getCreationTimeMillis() {
            return referenceTimeMillis + TimeUnit.NANOSECONDS
                .toMillis(creationTimeNanos - referenceTimeNanos);
        }

        public ByteBuffer getKey() {
            return key;
        }

        public @Nullable IOBufSliceImpl getRoot() {
            return root;
        }

        public void setRoot(IOBufSliceImpl slice) {
            verifyHoldsLock();
            if (root == null) {
                root = slice;
            }
        }

        private void verifyHoldsLock() {
            if (!Thread.holdsLock(this)) {
                throw new IllegalStateException(
                    "Must hold tracker lock");
            }
        }

        public void addToForkMap(IOBufSliceImpl parent,
                                 IOBufSliceImpl child) {
            verifyHoldsLock();
            parentMap.put(child, parent);
            childrenMap
                .computeIfAbsent(
                    parent, (k) -> new HashSet<IOBufSliceImpl>())
                .add(child);
        }

        public void removeSlice(IOBufSliceImpl slice) {
            verifyHoldsLock();
            events.remove(slice);
            childrenMap.remove(slice);
            if (slice == root) {
                return;
            }
            final IOBufSliceImpl p = parentMap.remove(slice);
            if (p != null) {
                final Set<IOBufSliceImpl> children = childrenMap.get(p);
                if (children != null) {
                    children.remove(slice);
                }
            } else {
                addError(new RuntimeException(String.format(
                    "Tracker %s with root %s has removed the parent " +
                    "before the child %s is removed, %s",
                    getDisplayIdString(key),
                    getDisplayIdString(root),
                    getDisplayIdString(slice),
                    toJson())));
            }
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public void markNew(IOBufSliceImpl slice, int refcnt) {
            addEvent(new NewEvent(slice, refcnt));
        }

        @Override
        public void markFork(IOBufSliceImpl slice,
                             int refcnt,
                             IOBufSliceImpl child) {
            addEvent(new ForkEvent(slice, refcnt, child));
        }

        @Override
        public void markRelease(IOBufSliceImpl slice,
                                int refcnt,
                                IOBufSliceImpl child) {
            addEvent(new ReleaseEvent(slice, refcnt, child));
            cleanup(slice, refcnt);
        }

        @Override
        public void markFree(IOBufSliceImpl slice, int refcnt) {
            addEvent(new FreeEvent(slice, refcnt));
            cleanup(slice, refcnt);
        }

        /**
         * Removes the tracker if root has a refcnt of zero.
         */
        private void cleanup(IOBufSliceImpl slice, int refcnt) {
            if (refcnt != 0) {
                return;
            }
            if (root == null) {
                addError(new RuntimeException(String.format(
                    "Tracker %s with root %s marked %s free " +
                    "before the root is properly marked new",
                    getDisplayIdString(key),
                    getDisplayIdString(root),
                    getDisplayIdString(slice))));
            }
            if (slice != root) {
                return;
            }
            if ((childrenMap.get(root) != null)
                && !childrenMap.get(root).isEmpty()) {
                addError(new RuntimeException(String.format(
                    "Tracker %s with root %s removed " +
                    "before itself is properly freed, %s",
                    getDisplayIdString(key),
                    getDisplayIdString(root),
                    toJson())));
            }
            root = null;
            remove(this);
        }

        @Override
        public void addInfo(IOBufSliceImpl slice, Supplier<JsonNode> supplier) {
            addEvent(new InfoEvent(slice, supplier));
        }

        private synchronized void addEvent(Event e) {
            events.computeIfAbsent(e.getSlice(), (k) -> new ArrayList<>())
                .add(e);
            e.addToTracker(this);
        }

        @Override
        public ObjectNode toJson() {
            final long t = getCreationTimeMillis();
            final ObjectNode result = JsonUtils.createObjectNode();
            result.put("id", getDisplayIdString(key));
            result.put("root", getDisplayIdString(root));
            result.put("trackerType", type.toString());
            result.put("creationTimeMillis", t);
            result.put(
                "creationTimeHuman", FormatUtils.formatDateTime(t));
            final JsonNode slices = dfsSlices();
            result.put("slices", slices);
            return result;
        }

        private JsonNode dfsSlices() {
            if (root == null) {
                return JsonUtils.createJsonNull();
            }
            final ArrayNode result = JsonUtils.createArrayNode();
            final Stack<IOBufSliceImpl> stack = new Stack<>();
            stack.push(root);
            int cnt = maxNumSlices;
            while (!stack.isEmpty()) {
                final IOBufSliceImpl curr = stack.pop();
                addEventsToDfsSlices(curr, result);
                if (cnt-- == 0) {
                    break;
                }
                final Set<IOBufSliceImpl> children = childrenMap.get(curr);
                if (children == null) {
                    continue;
                }
                children.stream()
                    /*
                     * Sort the the children with the identity function so that
                     * the print out can have a deterministic order.
                     */
                    .sorted(Comparator.comparingInt((o) -> getDisplayId(o)))
                    .forEach((c) -> stack.push(c));
            }
            return result;
        }

        private void addEventsToDfsSlices(IOBufSliceImpl curr, ArrayNode result) {
            final ObjectNode entry = JsonUtils.createObjectNode();
            entry.put("id", getDisplayIdString(curr));
            final List<Event> evts = events.get(curr);
            final int cnt = maxNumEvents;
            final ArrayNode evtsEntry = JsonUtils.createArrayNode();
            if (evts != null) {
                if (evts.size() <= cnt) {
                    evts.forEach((e) -> evtsEntry.add(e.toJson()));
                } else {
                    IntStream.range(0, cnt / 2)
                        .forEach((i) -> evtsEntry.add(evts.get(i).toJson()));
                    evtsEntry.add(JsonUtils.createJsonNode("..."));
                    IntStream.range(evts.size() - cnt / 2 + 1,
                                    evts.size())
                        .forEach((i) -> evtsEntry.add(evts.get(i).toJson()));
                }
            }
            entry.put("events", evtsEntry);
            result.add(entry);
        }

        @Override
        public @Nullable RateLimitingLogger<String> getLogger() {
            return rateLimitingLogger;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            if (!(other instanceof TrackerImpl)) {
                return false;
            }
            final TrackerImpl that = (TrackerImpl) other;
            return (this.creationTimeNanos == that.creationTimeNanos)
                && (key == that.key);
        }

        @Override
        public int hashCode() {
            return (int) creationTimeNanos * 37 + key.hashCode();
        }

        @Override
        public int compareTo(@Nullable TrackerImpl that) {
            if (that == null) {
                return -1;
            }
            if (this == that) {
                return 0;
            }
            if (creationTimeNanos != that.creationTimeNanos) {
                return (int) (creationTimeNanos - that.creationTimeNanos);
            } else if (System.identityHashCode(this)
                       != System.identityHashCode(that)) {
                return System.identityHashCode(this)
                    - System.identityHashCode(that);
            } else {
                return -1;
            }
        }

        @Override
        public String toString() {
            return toJson().toString();
        }
    }

    /**
     * Represents an event adding to the tracker.
     */
    public abstract class Event {

        protected final IOBufSliceImpl slice;

        public Event(IOBufSliceImpl slice) {
            this.slice = slice;
        }

        public IOBufSliceImpl getSlice() {
            return slice;
        }

        public abstract EventType getType();

        public abstract void addToTracker(TrackerImpl tracker);

        public ObjectNode toJson() {
            final ObjectNode result = JsonUtils.createObjectNode();
            result.put("type", getType().toString());
            return result;
        }
    }

    /**
     * An event with a reference count.
     */
    public abstract class ReferenceCountedEvent extends Event {

        protected final int refcnt;

        public ReferenceCountedEvent(IOBufSliceImpl slice, int refcnt) {
            super(slice);
            this.refcnt = refcnt;
        }

        @Override
        public void addToTracker(TrackerImpl tracker) {
            verifyReferenceCount(tracker);
        }

        protected void verifyReferenceCount(TrackerImpl tracker) {
            if (refcnt < 0) {
                addError(new RuntimeException(String.format(
                    "Tracker %s with root %s " +
                    "sees a slice %s with refcnt %s, %s",
                    getDisplayIdString(tracker.getKey()),
                    getDisplayIdString(tracker.getRoot()),
                    getDisplayIdString(slice),
                    refcnt,
                    tracker.toJson())));
            }
        }

        @Override
        public ObjectNode toJson() {
            final ObjectNode result = super.toJson();
            result.put("refcnt", refcnt);
            return result;
        }
    }

    /**
     * The event that a new slice is created.
     */
    private class NewEvent extends ReferenceCountedEvent {

        public NewEvent(IOBufSliceImpl slice, int refcnt) {
            super(slice, refcnt);
        }

        @Override
        public EventType getType() {
            return EventType.NEW;
        }

        @Override
        public void addToTracker(TrackerImpl tracker) {
            super.addToTracker(tracker);
            tracker.setRoot(slice);
        }
    }

    /**
     * The event that a child slice is forked from the parent.
     */
    private class ForkEvent extends ReferenceCountedEvent {

        private final IOBufSliceImpl child;

        public ForkEvent(IOBufSliceImpl slice,
                         int refcnt,
                         IOBufSliceImpl child) {
            super(slice, refcnt);
            this.child = child;
        }

        @Override
        public EventType getType() {
            return EventType.FORK;
        }

        @Override
        public void addToTracker(TrackerImpl tracker) {
            super.addToTracker(tracker);
            tracker.addToForkMap(slice, child);
        }

        @Override
        public ObjectNode toJson() {
            final ObjectNode result = super.toJson();
            result.put("child", getDisplayIdString(child));
            return result;
        }
    }

    /**
     * The event that does clean up. We need to remove slices from the tree when
     * the reference count is zero.
     */
    private abstract class CleanupEvent extends ReferenceCountedEvent {

        CleanupEvent(IOBufSliceImpl slice, int refcnt) {
            super(slice, refcnt);
        }

        @Override
        public void addToTracker(TrackerImpl tracker) {
            super.addToTracker(tracker);
            if (refcnt == 0) {
                tracker.removeSlice(slice);
            }
        }
    }

    /**
     * The event that the IOBufSliceImpl.markFree(child) has been called. That
     * is, a child slice has been released as a result of its reference count
     * is decremented to zero. Releasing the child causes the reference count
     * of the parent being decremented.
     */
    private class ReleaseEvent extends CleanupEvent {

        private final Object child;

        public ReleaseEvent(IOBufSliceImpl slice,
                            int refcnt,
                            Object child) {
            super(slice, refcnt);
            this.child = child;
        }

        @Override
        public EventType getType() {
            return EventType.RELEASE;
        }

        @Override
        public ObjectNode toJson() {
            final ObjectNode result = super.toJson();
            result.put("child", getDisplayIdString(child));
            return result;
        }
    }

    /**
     * The event that the IOBufSliceImpl.markFree() has been called. That is,
     * the slice has been marked to free by itself and as a result of its
     * reference count will be decremented.
     */
    private class FreeEvent extends CleanupEvent {

        public FreeEvent(IOBufSliceImpl slice, int refcnt) {
            super(slice, refcnt);
        }

        @Override
        public EventType getType() {
            return EventType.FREE;
        }
    }

    /**
     * An event to provide some information.
     */
    private class InfoEvent extends Event {

        private final Supplier<JsonNode> infoSupplier;

        public InfoEvent(IOBufSliceImpl slice,
                         Supplier<JsonNode> infoSupplier) {
            super(slice);
            this.infoSupplier = infoSupplier;
        }

        @Override
        public EventType getType() {
            return EventType.INFO;
        }

        @Override
        public void addToTracker(TrackerImpl tracker) {
        }

        @Override
        public ObjectNode toJson() {
            final ObjectNode result = super.toJson();
            result.put("info", infoSupplier.get());
            return result;
        }
    }

    /**
     * Creates a tracker that will be used by the slices that are created from
     * the IO buffer pool.
     */
    public static Tracker createFromPool(
        @Nullable EventTrackersManager trackersManager,
        TrackerType trackerType,
        ByteBuffer buf)
    {
        if (trackersManager == null) {
            return NO_LOGGER_NULL_TRACKER;
        }
        return trackersManager
            .getIOBufPoolTrackers().createTracker(trackerType, buf);
    }

    /**
     * Creates a null tracker that will be used by the slices that are created
     * from heap.
     */
    public static Tracker createFromHeap(
        @Nullable EventTrackersManager trackersManager)
    {
        if (trackersManager == null) {
            return NO_LOGGER_NULL_TRACKER;
        }
        return trackersManager
            .getIOBufPoolTrackers().getNullTracker();
    }

    /**
     * Returns an identity of a slice or a byte buffer for print out.  The
     * identity integer is only used for printing out, not for identity. Hence
     * it is OK that there are value conflicts.
     */
    public static int getDisplayId(Object obj) {
        if (getIdentityHook != null) {
            return getIdentityHook.apply(obj);
        }
        return System.identityHashCode(obj);
    }

    /**
     * Adds the endpoint handler info to the slice tracker.
     *
     * Note that the info events use a supplier so that we will be able to
     * obtain the handler status at the time of the logging. However, this also
     * means we keep a reference to the handler and therefore if the buffer is
     * not freed correctly, the handler will not be gc'ed. We could use a weak
     * reference, but that would make the info lost after it is gc'ed. We think
     * it is fine. The assumption is that we will not have a leak problem when
     * we release and even if there was a problem, since we the number of
     * trackers are bounded, we will not have a lot of handlers lingering
     * around.
     */
    public static void addEndpointHandlerInfo(
        IOBufSliceImpl slice,
        @Nullable AbstractDialogEndpointHandler handler)
    {
        slice.getTracker().addInfo(
            slice,
            () -> {
                if (handler == null) {
                    return NULL_ENDPOINT_HANDLER_INFO;
                }
                return handler.toJson();
            });
    }

    /**
     * Adds the dialog info to the slice tracker.
     */
    public static void addDialogInfo(
        IOBufSliceImpl slice,
        @Nullable DialogContextImpl dialog)
    {
        slice.getTracker().addInfo(
            slice,
            () -> {
                if (dialog == null) {
                    return NULL_DIALOG_INFO;
                }
                return dialog.toJson();
            });
    }

    /**
     * Adds a string info.
     */
    public static void addStringInfo(IOBufSliceImpl slice, String info) {
        slice.getTracker().addInfo(
            slice, () -> JsonUtils.createJsonNode(info));
    }


    /* Configuration setters and getters for testing. */

    public void setSampleRate(long val) {
        sampleRate = ensureLargerThanZero(val, SAMPLE_RATE);
    }

    public void setMaxNumTrackers(int val) {
        maxNumTrackers = ensureLargerThanZero(val, MAX_NUM_TRACKERS);
    }

    public void setMaxNumSlices(int val) {
        maxNumSlices = ensureLargerThanZero(val, MAX_NUM_SLICES);
    }

    public void setMaxNumEvents(int val) {
        maxNumEvents = ensureLargerThanZero(val, MAX_NUM_EVENTS);
    }

    public void setLoggingIntervalMillis(long val) {
        loggingIntervalMillis = ensureLargerThanZero(val, LOGGING_INTERVAL);
    }

    public void setTrackerLoggingDelayMillis(long val) {
        trackerLoggingDelayMillis = ensureLargerThanZero(val, LOGGING_DELAY);
    }

    public void setMaxNumErrors(int val) {
        maxNumErrors = ensureLargerThanZero(val, MAX_NUM_ERRORS);
    }

    /**
     * Returns an identity string for a slice or a byte buffer for print out.
     */
    public static @Nullable String getDisplayIdString(
        @Nullable Object obj)
    {
        if (obj == null) {
            return null;
        }
        return Integer.toString(getDisplayId(obj), 16);
    }

    /**
     * Returns the trackers for testing.
     */
    public Set<TrackerImpl> getTrackers(TrackerType type) {
        return trackers.get(type);
    }
}
