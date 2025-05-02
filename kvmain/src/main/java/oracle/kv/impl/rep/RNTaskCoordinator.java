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


package oracle.kv.impl.rep;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.logging.Logger;

import oracle.kv.impl.api.AggregateThroughputTracker.RWKB;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;

import com.sleepycat.je.utilint.JETaskCoordinator;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Defines the task coordinator used by the RepNodeService. It supplies the
 * application behavior needed by the task coordinator to control the permits
 * available for use by housekeeping tasks. To implement this behavior it
 * samples the application load on a periodic basis and invokes
 * {@link #setAppPermitPercent(int)} to keep the coordinator informed about any
 * changes in its permit requirements.
 *
 * The RN's (R+W)KB/sec is the basis for determining the RN's application load
 * and thus the application permit percentage.
 *
 * The class is abstract mainly to facilitate testing.
 *
 */
public abstract class RNTaskCoordinator extends JETaskCoordinator {

    /* The set of cooperating RN tasks in the service. */
    /*
     * TODO: Note that eventually (after JE becomes coordinator-aware),
     * the list of tasks will include JE tasks.
     */
    public static final Task KV_STORAGE_STATS_TASK =
        new Task("KVStorageStats", 1);
    public static final Task KV_INDEX_CREATION_TASK =
        new Task("KVIndexCreation", 1);

    /* The set of cooperating tasks associated with the app */
    private static final HashSet<Task> tasks =
        new HashSet<>(Arrays.asList(KV_STORAGE_STATS_TASK,
                                    KV_INDEX_CREATION_TASK));

    /**
     * The trailing sample size. The size must be a power of two so that the
     * index into list below can be rapidly computed as an and operation. The
     * size must be relatively small so that the number of permits can be
     * changed rapidly, but large enough to provide some hysteresis when load
     * diminishes.
     */
    private static final int SAMPLE_SIZE = 8;

    static {
        /*
         * Check for power of two. Change the "mod" code in sampleThroughput()
         * and remove the assertion if the SAMPLE_SIZE is changed
         */
        assert(Integer.bitCount(SAMPLE_SIZE) == 1);
    }

    /**
     * A circular buffer whose elements are KB/sec for each of the samples. The
     * max of these sample is used to determine the app permit percent.
     */
    private final int samplesKBPerSec[] = new int[SAMPLE_SIZE];

    /**
     * Index into the above sample, its range is 0 <= sampleIndex < SAMPLE_SIZE.
     * The sample at the index (samplesKBPerSec[sampleIndex]) represents the
     * latest sample value.
     */
    private volatile int sampleIndex = 0;

    /**
     * The max of the values in above sample. It's used as the basis for
     * calculating the app permit percentage.
     */
    private volatile int maxKBPerSec;

    /**
     * The period used to update the samples above.
     */
    private static final int SAMPLE_PERIOD_MS = 1000 /* Period of a second. */;

    /**
     * A snapshot of RWKB at the start of the sample being computed. It will
     * form the basis of the KB/sec throughput calculations.
     */
    private volatile RWKB sampleStartRWKB;

    /**
     * The above activity number converted into an app  permit percentage.
     */
    private volatile int permitPercent = 0;

    /**
     * An array that captures the rough time percentile distribution of app
     * permits. They represent stats describing the operational behavior of the
     * coordinator that could be used to tune the policy curve represented by
     * the throughputPercentMap.
     */
    private final AtomicIntegerArray permitPercentDistribution =
        new AtomicIntegerArray(11); /* Allow for the closed interval 0 to 100 */

    /**
     * The permit policy in use.
     */
    private final ThroughputPercent[] throughputPercentMap;

    /**
     * Assume a conservative 100 MB/sec
     */
    private static final ThroughputPercent[] SSD_PERMIT_POLICY = {
        new ThroughputPercent(0, 0),
        new ThroughputPercent(15 * 1024, 15),
        new ThroughputPercent(25 * 1024, 25),
        new ThroughputPercent(50 * 1024, 50),
        new ThroughputPercent(75 * 1024, 100) };
    /**
     * Assume a conservative 150 MB/sec or 1.5X SSD throughput
     */
    private static final ThroughputPercent[] NVME_PERMIT_POLICY = {
        new ThroughputPercent(0, 0),
        new ThroughputPercent(22 * 1024, 15),
        new ThroughputPercent(37 * 1024, 25),
        new ThroughputPercent(75 * 1024, 50),
        new ThroughputPercent(112 * 1024, 100) };

    /**
     * Assume conservative 40 MB/sec HD.
     *
     * We are more aggressive claiming permits since read random IOs are so
     * expensive and we don't currently distinguish them in the throughput
     * numbers.
     */
    private static final ThroughputPercent[] HD_PERMIT_POLICY = {
        new ThroughputPercent(0, 0),
        new ThroughputPercent(5 * 1024, 30),
        new ThroughputPercent(10 * 1024, 70),
        new ThroughputPercent(30 * 1024, 100) };

    /**
     * Creates an RN Task coordinator
     *
     * @param logger the logger to be used.
     *
     * @param storageType the type associated with the storage being used by the
     * RN. It's used to determine a permit policy if one is not explicitly
     * provided via the throughputPercentMap parameter.
     *
     * @param throughputPercentMap the policy map used to map throughput to
     * app permit percentages. A null value results in use of a default policy
     * suitable based upon the supplied storageType parameter.
     */
    RNTaskCoordinator(Logger logger,
                      @Nullable StorageType storageType,
                      @Nullable ThroughputPercent[] throughputPercentMap) {
        super(logger, null /* envImpl */, tasks, true /* active */);

        Objects.requireNonNull(logger, "logger argument must be non null");
        sampleStartRWKB = getRWKB();
        if (throughputPercentMap == null) {
            /* Use the storage type to determine the default policy map. */
            if (storageType != null) {
                switch (storageType) {
                    case HD:
                        this.throughputPercentMap = HD_PERMIT_POLICY;
                        break;
                    case NVME:
                        this.throughputPercentMap = NVME_PERMIT_POLICY;
                        break;
                    case SSD:
                        this.throughputPercentMap = SSD_PERMIT_POLICY;
                        break;
                    case UNKNOWN:
                        this.throughputPercentMap = HD_PERMIT_POLICY;
                        break;
                    default:
                        this.throughputPercentMap = HD_PERMIT_POLICY;
                }
            } else {
                this.throughputPercentMap = HD_PERMIT_POLICY;
            }
        } else {
            this.throughputPercentMap = throughputPercentMap;
        }

        final TimerTask samplingTask =
            new TimerTask() {

                @Override
                public void run() {
                    /* Take a new application load sample. */
                    if (sampleThroughput()) {
                        /* Load has changed, recalculate permits. */
                        permitPercent = permitPercent();
                    }
                    permitPercentDistribution.incrementAndGet(permitPercent/10);

                    if (setAppPermitPercent(permitPercent)) {
                        /* Change in app permits. */
                        final String msg = String.format("Task coordinator." +
                            " Max trailing throughput: %,d RWKB/sec. ",
                            maxKBPerSec);
                        /*
                         * In case of bursty workloads the count of app permits
                         * would oscillate between two values, consuming lot of
                         * space in log files. So the following log message was
                         * changed to fine level. If required to tune the permit
                         * policies, reenable this logging.
                         */
                        logger.fine(() -> msg + permitSummary());
                    }
                }
            };

        timer.schedule(samplingTask, 0, /* No start  delay */ SAMPLE_PERIOD_MS);
    }

    /**
     * Returns the total RWKB processed by the RN.
     */
    abstract RWKB getRWKB();

    /**
     * Returns the throughput map currently in use. A debugging API.
     */
    ThroughputPercent[] getThroughputPercentMap() {
        return throughputPercentMap;
    }

    int getMaxKBPerSec() {
        return maxKBPerSec;
    }

    @Override
    public void close() {
        super.close();
        int totalSamples = 0;

        for (int i=0; i < permitPercentDistribution.length(); i++) {
            totalSamples += permitPercentDistribution.get(i);
        }

        String dmsg = "App permit percentage frequency: ";

        for (int i=0; i < permitPercentDistribution.length(); i++) {
            int count = permitPercentDistribution.get(i);
            if (count == 0) {
                continue;
            }
            int percent = (count * 100) / totalSamples;

            dmsg +=  (i == 10) ?
                (" 100% = " + percent) :
                String.format(" %d-%d%% = %d", i * 10, (i * 10 + 10), percent);
        }
        logger.info(dmsg);
    }

    /**
     * Invoked to calculate the percentage of permits reserved for the
     * application. Maps max throughput to a permit percentage based on the
     * supplied policy map.
     */
    private int permitPercent() {
        for (int i = throughputPercentMap.length - 1; i >= 0; i--) {
            if (maxKBPerSec >= throughputPercentMap[i].KBPerSec ) {
                return throughputPercentMap[i].percent;
            }
        }
        throw new IllegalStateException("Could not determine limit for:" +
                                        maxKBPerSec);
    }

    /**
     * Adds a new sample to the trailing list of samples, obsoleting the oldest
     * sample.
     *
     * @return true if the trailing max changed (up or down) from its current
     * value
     */
    private boolean sampleThroughput() {
        final RWKB currRWKB = getRWKB();
        final int currKBPerSec = currRWKB.getKBPerSec(sampleStartRWKB);
        sampleStartRWKB = currRWKB;

        final int slot = ++sampleIndex & (SAMPLE_SIZE - 1);
        final long obsoleteKBPerSec = samplesKBPerSec[slot];
        samplesKBPerSec[slot] = currKBPerSec;

        if (currKBPerSec == maxKBPerSec) {
            /* No change in max. */
            return false;
        } else if (currKBPerSec > maxKBPerSec) {
            maxKBPerSec = currKBPerSec;
            return true;
        } else if (obsoleteKBPerSec == maxKBPerSec) {
            /* Recompute, potentially lower trailing max. */
            int newMax = 0;
            for (int element : samplesKBPerSec) {
                if (element > newMax) {
                    newMax = element;
                }
            }
            maxKBPerSec = newMax;
            return obsoleteKBPerSec != newMax;
        } else {
            /* No change in max. */
            return false;
        }
    }

    /**
     * A convenience class for expressing breakpoints on the throughput versus
     * app permit percent policy curve.
     */
    public static class ThroughputPercent {
        final int KBPerSec;
        final int percent;

        public ThroughputPercent(int KBPerSec, int percent) {
            super();
            this.KBPerSec = KBPerSec;
            this.percent = percent;
        }
    }
}
