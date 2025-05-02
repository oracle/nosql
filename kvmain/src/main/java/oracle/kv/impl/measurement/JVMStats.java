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
package oracle.kv.impl.measurement;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import oracle.kv.impl.util.WatcherNames;

import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.json.JsonUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.UnixOperatingSystemMXBean;

/**
 * Dump of Java Virtual Machine information.
 */
public class JVMStats implements ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Class for tracking JVM statistics over time, so that we can supply
     * counts and times since the last report, even though GC beans report
     * cumulative values.  Use an instance of this class to maintain the
     * history for returned stats.
     */
    public static class Tracker {

        /**
         * Map from GC name to information about the last GC count and time.
         */
        private final Map<String, CollectorTotal> collectorTotals =
            Collections.synchronizedMap(new HashMap<String, CollectorTotal>());

        /**
         * The listener that has been registered for GC notifications, or null
         * if not currently registered.
         */
        private volatile NotificationListener gcListener;

        /** Map from GC name to information used to quantify GC performance. */
        private final Map<String, LatencyElement> gcLatencyElements =
            Collections.synchronizedMap(new HashMap<>());

        /** Check for JVM pauses. */
        private final PauseChecker pauseChecker = new PauseChecker();

        /** Creates a JVMStats instance for the specified time period. */
        public JVMStats createStats(long start, long end) {
            maybeRegisterForNotifications();
            return new JVMStats(start, end, collectorTotals, gcLatencyElements,
                                pauseChecker.latencyElement);
        }

        /**
         * Shutdown the tracker, including shutting down the pause checker
         * thread and unregistering for JMX GC notifications.
         */
        public void shutdown() {
            pauseChecker.shutdown();
            unregisterForNotifications();
        }

        /** Register for JMX GC notifications if needed and supported. */
        private synchronized void maybeRegisterForNotifications() {
            if (gcListener != null) {
                return;
            }
            gcListener = new GCListener();
            for (GarbageCollectorMXBean gcbean :
                     ManagementFactory.getGarbageCollectorMXBeans()) {
                if (gcbean instanceof NotificationEmitter) {
                    ((NotificationEmitter) gcbean).addNotificationListener(
                        gcListener, null, null);
                }
            }
        }

        /** Unregister for JMX GC notifications */
        private synchronized void unregisterForNotifications() {
            if (gcListener == null) {
                return;
            }
            for (GarbageCollectorMXBean gcbean :
                     ManagementFactory.getGarbageCollectorMXBeans()) {
                if (gcbean instanceof NotificationEmitter) {
                    final NotificationEmitter notifier =
                        (NotificationEmitter) gcbean;
                    try {
                        notifier.removeNotificationListener(gcListener);
                    } catch (ListenerNotFoundException e) {
                    }
                }
            }
            gcListener = null;
        }

        /** Processes JMX GC notifications. */
        private class GCListener implements NotificationListener {
            @Override
            public void handleNotification(Notification notification,
                                           Object handback) {
                if (!notification.getType().equals(
                        GARBAGE_COLLECTION_NOTIFICATION)) {
                    return;
                }

                final String gcName;
                final long gcDuration;

                /* Catch exceptions for malformed JMX notifications */
                try {
                    final CompositeData compositeData =
                        (CompositeData) notification.getUserData();
                    gcName = getGcName(compositeData);
                    gcDuration = getGcDurationNanos(compositeData);
                } catch (Exception e) {
                    return;
                }
                gcLatencyElements.computeIfAbsent(
                    gcName, (k) -> new LatencyElement()).
                    observe(gcDuration);
            }
        }
    }

    /**
     * Returns the GC name specified by the composite data for an internal GC
     * notification.
     */
    private static String getGcName(CompositeData compositeData) {
        final GarbageCollectionNotificationInfo info =
            GarbageCollectionNotificationInfo.from(compositeData);
        return info.getGcName();
    }

    /**
     * Returns the GC duration in nanoseconds specified by the composite data
     * for an internal GC notification.
     */
    private static long getGcDurationNanos(CompositeData compositeData) {
        final GarbageCollectionNotificationInfo info =
            GarbageCollectionNotificationInfo.from(compositeData);
        return MILLISECONDS.toNanos(info.getGcInfo().getDuration());
    }

    /** Tallies information about total GC counts and times. */
    private static class CollectorTotal {
        private long count;
        private long time;

        /** Updates the count and returns the change since the last value. */
        synchronized long updateCount(long newCount) {
            final long change = newCount - count;
            count = newCount;
            return change;
        }

        /** Updates the time and returns the change since the last value. */
        synchronized long updateTime(long newTime) {
            final long change = newTime - time;
            time = newTime;
            return change;
        }
    }

    /**
     * Check for JVM pauses by sleeping a millisecond and reporting if the
     * sleep lasts longer than that.  Pauses are typically caused by GCs, but
     * could be caused by other things, say by pauses due to OS virtualization.
     */
    private static class PauseChecker implements Runnable {
        private static final AtomicInteger index = new AtomicInteger();

        private final LatencyElement latencyElement = new LatencyElement();
        private volatile boolean shutdown;

        PauseChecker() {
            final Thread t = new Thread(
                this, "KV Pause Checker " + index.incrementAndGet());
            t.setDaemon(true);
            t.start();
        }

        void shutdown() {
            shutdown = true;
        }

        @Override
        public void run() {
            while (!shutdown) {
                final long start = System.nanoTime();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    continue;
                }

                /*
                 * Only report pauses if we are sure there was at least a full
                 * millisecond delay, to avoid problems with platforms that
                 * don't have access to fine grain clocks.  Since we don't know
                 * where the pause started in the millisecond we were asleep,
                 * count half of that time as part of the millisecond.  We also
                 * need to add a half millisecond as part of the conversion
                 * from nanoseconds to milliseconds so that we get rounding
                 * rather than truncation.  As a result, just report the entire
                 * pause time if it is greater than 1.
                 */
                final long elapsed = System.nanoTime() - start;
                if (NANOSECONDS.toMillis(elapsed) > 1) {
                    latencyElement.observe(elapsed);
                }
            }
        }
    }

    /**
     * Container class for garbage collector information.
     */
    public static class CollectorInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The name of the garbage collector this object represents.
         */
        private final String name;

        /**
         * The total number of collections that have occurred. This field is
         * set to the value returned by {@link
         * java.lang.management.GarbageCollectorMXBean#getCollectionCount
         * GarbageCollectorMXBean.getCollectionCount} at the time this object
         * is constructed.
         *
         * @see java.lang.management.GarbageCollectorMXBean#getCollectionCount
         * GarbageCollectorMXBean.getCollectionCount
         */
        private final long count;

        /**
         * The approximate accumulated collection elapsed time in nanoseconds.
         *
         * This field is set to the value returned by {@link
         * java.lang.management.GarbageCollectorMXBean#getCollectionTime
         * GarbageCollectorMXBean.getCollectionTime} at the time this object is
         * constructed. The returned value is in milliseconds, which is
         * converted to nanoseconds.
         *
         * @see java.lang.management.GarbageCollectorMXBean#getCollectionTime
         * GarbageCollectorMXBean.getCollectionTime
         */
        private final long timeNanos;

        /**
         * The 95th percentile of times for individual GCs, or -1 if not known.
         * The value can be zero when part of the store is in a version before
         * 20.2.
         */
        private final long percent95Nanos;

        /**
         * The 99th percentile of times for individual GCs, or -1 if not known.
         * The value can be zero when part of the store is in a version before
         * 20.2.
         */
        private final long percent99Nanos;

        /**
         * The max time for individual GCs, or -1 if not known.
         * The value can be zero when part of the store is in a version before
         * 20.2.
         */
        private final long maxNanos;

        private CollectorInfo(GarbageCollectorMXBean gc,
                              Map<String, CollectorTotal> collectorTotals,
                              Map<String, LatencyElement> gcLatencyElements) {
            name = gc.getName();
            CollectorTotal total;
            synchronized (collectorTotals) {
                total = collectorTotals.computeIfAbsent(
                    name, (k) -> new CollectorTotal());
            }
            count = total.updateCount(gc.getCollectionCount());
            timeNanos = total.updateTime(
                MILLISECONDS.toNanos(gc.getCollectionTime()));

            final LatencyElement element = gcLatencyElements.get(gc.getName());
            if (element != null) {
                final LatencyElement.Result result =
                    element.obtain(
                        /* Use an ad hoc watcher name */
                        WatcherNames.getWatcherName(
                            CollectorInfo.class, "constructor"));
                percent95Nanos = result.getPercent95();
                percent99Nanos = result.getPercent99();
                maxNanos = result.getMax();
            } else {
                percent95Nanos = -1;
                percent99Nanos = -1;
                maxNanos = -1;
            }
        }

        /**
         * Returns the name of the GC.
         *
         * @return the GC name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the number of times the GC was called in this period.
         *
         * @return the GC count
         */
        public long getCount() {
            return count;
        }

        /**
         * Returns the total amount of time in nanoseconds spent by the GC in
         * this period.
         *
         * @return the total GC time
         */
        public long getTimeNanos() {
            return timeNanos;
        }

        /**
         * Returns the 95th percentile of times, in nanoseconds, for
         * individual GCs in this period, or -1 if not known.
         *
         * @return the 95th percentile GC time or -1
         */
        public long get95thNanos() {
            return percent95Nanos;
        }

        /**
         * Returns the 99th percentile of times, in nanoseconds, for
         * individual GCs in this period, or -1 if not known.
         *
         * @return the 99th percentile GC time or -1
         */
        public long get99thNanos() {
            return percent99Nanos;
        }

        /**
         * Returns the maximum time in nanoseconds of individual GCs in this
         * period, or -1 if not known.
         *
         * @return the maximum GC time or -1
         */
        public long getMaxNanos() {
            return maxNanos;
        }

        void getFormattedStats(StringBuilder sb) {
            if (count > 0) {
                sb.append("\n");
                sb.append("GC ");
                sb.append(name);
                sb.append("\n\tcount=");
                sb.append(count);
                sb.append("\n\ttime=");
                sb.append(timeNanos);
                if (percent95Nanos >= 0) {
                    sb.append("\n\t95th=");
                    sb.append(
                        NANOSECONDS.toMillis(percent95Nanos));
                }
                if (percent99Nanos >= 0) {
                    sb.append("\n\t99th=");
                    sb.append(
                        NANOSECONDS.toMillis(percent99Nanos));
                }
                if (maxNanos >= 0) {
                    sb.append("\n\tmax=");
                    sb.append(NANOSECONDS.toMillis(maxNanos));
                }
            }
        }

        ObjectNode toJson() {
            final ObjectNode result = JsonUtils.createObjectNode();
            result.put("Name", name);
            result.put("Count", count);
            /* Report time in millis for compatibility */
            result.put(
                "Time", NANOSECONDS.toMillis(timeNanos));
            if (percent95Nanos >= 0) {
                result.put(
                    "95th", NANOSECONDS.toMillis(percent95Nanos));
            }
            if (percent99Nanos >= 0) {
                result.put(
                    "99th", NANOSECONDS.toMillis(percent99Nanos));
            }
            if (maxNanos >= 0) {
                result.put(
                    "Max", NANOSECONDS.toMillis(maxNanos));
            }
            return result;
        }
    }

    private final long start;
    private final long end;

    /**
     * The amount of free memory in the Java Virtual Machine. This field is set
     * to the value returned by
     * {@link java.lang.Runtime#freeMemory() Runtime.freeMemory} at the time
     * this object is constructed.
     *
     * @see java.lang.Runtime#freeMemory Runtime.freeMemory
     */
    private final long freeMemory;

    /**
     * The maximum amount of memory that the Java virtual machine will attempt
     * to use. This field is set to the value returned by
     * {@link java.lang.Runtime#maxMemory() Runtime.maxMemory} (or for Zing,
     * a JVM-specific method) at the time this object is constructed.
     *
     * @see java.lang.Runtime#maxMemory() Runtime.maxMemory
     */
    private final long maxMemory;

    /**
     * The total amount of memory in the Java virtual machine. This field is set
     * to the value returned by
     * {@link java.lang.Runtime#totalMemory() Runtime.totalMemory} at the time
     * this object is constructed.
     *
     * @see java.lang.Runtime#totalMemory() Runtime.totalMemory
     */
    private final long totalMemory;

    /** The number of JVM pauses detected. */
    private final long pauseCount;

    /** The 95th percentile of JVM pause times, in nanoseconds. */
    private final long pause95Nanos;

    /** The 99th percentile JVM pause times, in nanoseconds. */
    private final long pause99Nanos;

    /** The maximum JVM pause time, in nanoseconds. */
    private final long pauseMaxNanos;

    /**
     * Garbage collectors operating in the Java virtual machine.
     */
    private final List<CollectorInfo> collectors;

    /**
     * Percentage of the total physical memory on the machine currently in use
     * as a value from 0 to 100 inclusive, or 0 if not known.
     */
    private final int inUsePhysicalMemoryPercent;

    /** Total amount of physical memory on the machine in bytes. */
    private final long totalPhysicalMemorySize;

    /**
     * Recent CPU usage for the JVM process as a percentage from 0 to 100
     * inclusive, or 0 if not known.
     */
    private final int processCpuLoadPercent;

    /**
     * Recent CPU usage for the whole system as a percentage from 0 to 100
     * inclusive, or 0 if not known.
     */
    private final int systemCpuLoadPercent;

    /**
     * Percentage of the maximum file descriptors currently in use as a value
     * from 0 to 100 inclusive, or 0 if not known.
     */
    private final int inUseFileDescriptorPercent;

    /** Maximum number of file descriptors. */
    private final long maxFileDescriptorCount;

    /** Peak count of live threads. */
    private final long peakThreadCount;

    /**
     * Constructor. The JVM information contained in this object is collected
     * at construction time.
     */
    private JVMStats(long start,
                     long end,
                     Map<String, CollectorTotal> collectorTotals,
                     Map<String, LatencyElement> gcLatencyElements,
                     LatencyElement pauseLatencyElement) {
    	this.start = start;
    	this.end = end;
        Runtime rt = Runtime.getRuntime();
        this.freeMemory = rt.freeMemory();
        this.maxMemory = JVMSystemUtils.getRuntimeMaxMemory();
        this.totalMemory = rt.totalMemory();
        final LatencyElement.Result pauseResult =
            pauseLatencyElement.obtain(
                WatcherNames.getWatcherName(JVMStats.class, "ctor"));
        this.pauseCount = pauseResult.getOperationCount();
        this.pause95Nanos = pauseResult.getPercent95();
        this.pause99Nanos = pauseResult.getPercent99();
        this.pauseMaxNanos = pauseResult.getMax();
        final List<GarbageCollectorMXBean> gcBeans =
            ManagementFactory.getGarbageCollectorMXBeans();
        collectors = new ArrayList<CollectorInfo>(gcBeans.size());
        for (GarbageCollectorMXBean gc : gcBeans) {
            collectors.add(
                new CollectorInfo(gc, collectorTotals, gcLatencyElements));
        }

        final Object genericOsBean =
            ManagementFactory.getOperatingSystemMXBean();
        if (genericOsBean instanceof OperatingSystemMXBean) {
            final OperatingSystemMXBean osBean =
                (OperatingSystemMXBean) genericOsBean;
            totalPhysicalMemorySize = getTotalMemorySize(osBean);
            final long freePhysicalMemorySize = getFreeMemorySize(osBean);
            final long inUseSize =
                totalPhysicalMemorySize - freePhysicalMemorySize;
            inUsePhysicalMemoryPercent = (int) Math.round(
                (((double) inUseSize) / totalPhysicalMemorySize) * 100);
            final double processCpuLoad = osBean.getProcessCpuLoad();
            processCpuLoadPercent = (int) Math.round(processCpuLoad * 100);
            final double systemCpuLoad = getCpuLoad(osBean);
            systemCpuLoadPercent = (int) Math.round(systemCpuLoad * 100);
        } else {
            totalPhysicalMemorySize = 0;
            inUsePhysicalMemoryPercent = 0;
            processCpuLoadPercent = 0;
            systemCpuLoadPercent = 0;
        }
        if (genericOsBean instanceof UnixOperatingSystemMXBean) {
            final UnixOperatingSystemMXBean unixBean =
                (UnixOperatingSystemMXBean) genericOsBean;
            maxFileDescriptorCount = unixBean.getMaxFileDescriptorCount();
            final long openFileDescriptorCount =
                unixBean.getOpenFileDescriptorCount();
            inUseFileDescriptorPercent = (int) Math.round(
                ((double) openFileDescriptorCount / maxFileDescriptorCount)
                * 100);
        } else {
            maxFileDescriptorCount = 0;
            inUseFileDescriptorPercent = 0;
        }
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        peakThreadCount = threadBean.getPeakThreadCount();
        threadBean.resetPeakThreadCount();
    }

    /**
     * TODO: OperatingSystemMXBean.getTotalPhysicalMemorySize is deprecated as
     * of Java 14. Use OperatingSystemMXBean.getTotalMemorySize when we move to
     * Java 14 or later.
     */
    @Deprecated
    private static long getTotalMemorySize(OperatingSystemMXBean osBean) {
        return osBean.getTotalPhysicalMemorySize();
    }

    /**
     * TODO: OperatingSystemMXBean.getFreePhysicalMemorySize is deprecated as
     * of Java 14. Use OperatingSystemMXBean.getFreeMemorySize when we move to
     * Java 14 or later.
     */
    @Deprecated
    private static long getFreeMemorySize(OperatingSystemMXBean osBean) {
        return osBean.getFreePhysicalMemorySize();
    }

    /**
     * TODO: OperatingSystemMXBean.getSystemCpuLoad is deprecated as of Java
     * 14. Use OperatingSystemMXBean.getCpuLoad when we move to Java 14 or
     * later.
     */
    @Deprecated
    private static double getCpuLoad(OperatingSystemMXBean osBean) {
        return osBean.getSystemCpuLoad();
    }

    /**
     * Returns the amount of free memory in the Java Virtual Machine.
     *
     * @return the amount of free memory
     */
    public long getFreeMemory() {
        return freeMemory;
    }

    /**
     * Returns the maximum amount of memory the Java Virtual Machine will
     * attempt to use.
     *
     * @return the maximum amount of memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Returns the total amount of memory in the Java Virtual Machine.
     *
     * @return the total amount of memory
     */
    public long getTotalMemory() {
        return totalMemory;
    }

    /**
     * Returns the number of JVM pauses detected in this period.
     *
     * @return the number of pauses
     */
    public long getPauseCount() {
        return pauseCount;
    }

    /**
     * Returns the 95th percentile of JVM pause times, in nanoseconds.
     *
     * @return the 95th percentile pause time
     */
    public long getPause95thNanos() {
        return pause95Nanos;
    }

    /**
     * Returns the 99th percentile of JVM pause times, in nanoseconds.
     *
     * @return the 99th percentile pause time
     */
    public long getPause99thNanos() {
        return pause99Nanos;
    }

    /**
     * Returns the maximum of JVM pause times, in nanoseconds.
     *
     * @return the maximum pause time
     */
    public long getPauseMaxNanos() {
        return pauseMaxNanos;
    }

    /**
     * Returns information about garbage collectors operating in the Java
     * Virtual Machine.
     *
     * @return information about garbage collectors
     */
    public List<CollectorInfo> getCollectors() {
        return collectors;
    }

    /**
     * Returns the percentage of the total physical memory on the machine
     * currently in use as a value from 0 to 100 inclusive, or 0 if not known.
     *
     * @return the percentage of physical memory in use or 0
     * @since 21.2
     */
    public int getInUsePhysicalMemoryPercent() {
        return inUsePhysicalMemoryPercent;
    }

    /**
     * Returns the total amount of physical memory on the machine in bytes, or
     * 0 if not known.
     *
     * @return the total amount of physical memory on the machine in bytes or 0
     * @since 21.2
     */
    public long getTotalPhysicalMemorySize() {
        return totalPhysicalMemorySize;
    }

    /**
     * Returns the recent CPU usage for the JVM process as a percentage from 0
     * to 100 inclusive, or 0 if not known.
     *
     * @return the recent CPU usage percentage for the JVM process or 0
     * @since 21.2
     */
    public int getProcessCpuLoadPercent() {
        return processCpuLoadPercent;
    }

    /**
     * Returns the recent CPU usage for the whole system as a percentage from 0
     * to 100 inclusive, or 0 if not known.
     *
     * @return the recent CPU usage percentage for the whole system or 0
     * @since 21.2
     */
    public int getSystemCpuLoadPercent() {
        return systemCpuLoadPercent;
    }

    /**
     * Returns the percentage of the maximum file descriptors currently in use
     * as a value from 0 to 100 inclusive, or 0 if not known.
     *
     * @return the percentage of file descriptors in use or 0
     * @since 21.2
     */
    public int getInUseFileDescriptorPercent() {
        return inUsePhysicalMemoryPercent;
    }

    /**
     * Returns the maximum number of file descriptors, or 0 if not known.
     *
     * @return the maximum number of file descriptors or 0
     * @since 21.2
     */
    public long getMaxFileDescriptorCount() {
        return maxFileDescriptorCount;
    }

    /**
     * Returns the peak count of live threads, or 0 if not known.
     *
     * @return the peak count of live threads or 0
     * @since 21.2
     */
    public long getPeakThreadCount() {
        return peakThreadCount;
    }

    /* -- From ConciseStats -- */

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public String getFormattedStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("Memory");
        sb.append("\n\tfreeMemory=");
        sb.append(freeMemory);
        sb.append("\n\tmaxMemory=");
        sb.append(maxMemory);
        sb.append("\n\ttotalMemory=");
        sb.append(totalMemory);
        if (inUsePhysicalMemoryPercent > 0) {
            sb.append("\n\tinUsePhysicalMemoryPercent=");
            sb.append(inUsePhysicalMemoryPercent);
            sb.append("\n\ttotalPhysicalMemorySize=");
            sb.append(totalPhysicalMemorySize);
        }
        if (systemCpuLoadPercent > 0) {
            sb.append("\nCPU");
            sb.append("\n\tprocessCpuLoadPercent=");
            sb.append(processCpuLoadPercent);
            sb.append("\n\tsystemCpuLoadPercent=");
            sb.append(systemCpuLoadPercent);
        }
        if (maxFileDescriptorCount > 0) {
            sb.append("\nFileDescriptors");
            sb.append("\n\tinUseFileDescriptorPercent=");
            sb.append(inUseFileDescriptorPercent);
            sb.append("\n\tmaxFileDescriptorCount=");
            sb.append(maxFileDescriptorCount);
        }
        if (peakThreadCount > 0) {
            sb.append("\nThreads");
            sb.append("\n\tpeakThreadCount=");
            sb.append(peakThreadCount);
        }
        if (pauseCount > 0) {
            sb.append("\nPause");
            sb.append("\n\tpauseCount=");
            sb.append(pauseCount);
            if (pause95Nanos > 0) {
                sb.append("\n\tpause95th=");
                sb.append(NANOSECONDS.toMillis(pause95Nanos));
            }
            if (pause99Nanos > 0) {
                sb.append("\n\tpause99th=");
                sb.append(NANOSECONDS.toMillis(pause99Nanos));
            }
            if (pauseMaxNanos > 0) {
                sb.append("\n\tpauseMax=");
                sb.append(NANOSECONDS.toMillis(pauseMaxNanos));
            }
        }
        for (CollectorInfo gc : collectors) {
            gc.getFormattedStats(sb);
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return "JVMStats[" + getFormattedStats() + "]";
    }

    public ObjectNode toJson() {
        final ObjectNode result = JsonUtils.createObjectNode();
        /* TODO: Move memory-related items to a separate Memory section */
        result.put("Free_Memory", freeMemory);
        result.put("Max_Memory", maxMemory);
        result.put("Total_Memory", totalMemory);
        if (totalPhysicalMemorySize > 0) {
            result.put("In_Use_Physical_Memory_Percent",
                               inUsePhysicalMemoryPercent);
            result.put("Total_Physical_Memory_Size",
                               totalPhysicalMemorySize);
        }
        if (systemCpuLoadPercent > 0) {
            final ObjectNode cpu = JsonUtils.createObjectNode();
            cpu.put("Process_Load_Percent", processCpuLoadPercent);
            cpu.put("System_Load_Percent", systemCpuLoadPercent);
            result.put("CPU", cpu);
        }
        if (maxFileDescriptorCount > 0) {
            final ObjectNode fds = JsonUtils.createObjectNode();
            fds.put("In_Use_Percent", inUseFileDescriptorPercent);
            fds.put("Max_Count", maxFileDescriptorCount);
            result.put("File_Descriptors", fds);
        }
        if (peakThreadCount > 0) {
            final ObjectNode threads = JsonUtils.createObjectNode();
            threads.put("Peak_Count", peakThreadCount);
            result.put("Threads", threads);
        }
        /* TODO: Move pause items to separate Pause section */
        if (pauseCount > 0) {
            result.put("Pause_Count", pauseCount);
        }
        /* Report time in millis for compatibility */
        if (pause95Nanos > 0) {
            result.put(
                "Pause_95th", NANOSECONDS.toMillis(pause95Nanos));
        }
        if (pause99Nanos > 0) {
            result.put(
                "Pause_99th", NANOSECONDS.toMillis(pause99Nanos));
        }
        if (pauseMaxNanos > 0) {
            result.put(
                "Pause_Max", NANOSECONDS.toMillis(pauseMaxNanos));
        }
        result.put("GC_Stats",
                   collectors.stream()
                   .map((c) -> c.toJson())
                   .collect(JsonSerializationUtils.getArrayCollector()));
        return result;
    }
}
