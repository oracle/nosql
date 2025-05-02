/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.utilint;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * A struct holding the min, max, avg, 95th, and 99th percentile measurements
 * for the collection of values held in a LatencyStat.
 */
public class Latency implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private int maxTrackedLatencyMillis;
    private int min;
    private int max;
    private float avg;
    private int totalOps;
    /* totalOps with new type long */
    private long totalOpsLong;
    private int percent95;
    private int percent99;

    /*
     * This field should be called requestsOverflow, but is left opsOverflow
     * for serialization compatibility with JE 5.0.69 and earlier.
     */
    private int opsOverflow;

    /*
     * The totalRequests field was added in JE 5.0.70.  When an object
     * serialized by JE 5.0.69 or earler is deserialized here, this field is
     * initialized here to 0 by Java and then set equal to totalOps by
     * readObject.  Setting totalRequests to totalOps is accurate for
     * single-op-per-request stats.  It is inaccurate for
     * multiple-op-per-request stats, but the best we can do with the
     * information we have available.
     */
    private int totalRequests;
    /* totalRequests with new type long */
    private long totalRequestsLong;

    /**
     * Creates a Latency with a maxTrackedLatencyMillis and all fields with
     * zero values.
     */
    public Latency(int maxTrackedLatencyMillis) {
        this.maxTrackedLatencyMillis = maxTrackedLatencyMillis;
    }

    public Latency(int maxTrackedLatencyMillis,
                   int minMillis,
                   int maxMillis,
                   float avg,
                   long totalOps,
                   long totalRequests,
                   int percent95,
                   int percent99,
                   int requestsOverflow) {
        this.maxTrackedLatencyMillis = maxTrackedLatencyMillis;
        this.min = minMillis;
        this.max = maxMillis;
        this.avg = avg;
        this.totalOpsLong = totalOps;
        this.totalRequestsLong = totalRequests;
        this.percent95 = percent95;
        this.percent99 = percent99;
        this.opsOverflow = requestsOverflow;
    }

    /* Support for Java serialization compatibility */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        if (totalOpsLong == 0) {
            totalOpsLong = totalOps;
        }

        if (totalRequests == 0) {
            totalRequests = totalOps;
        }

        if (totalRequestsLong == 0) {
            totalRequestsLong = totalRequests;
        }

    }

    /* Support for Java serialization compatibility */
    private void writeObject(ObjectOutputStream out)
        throws IOException {

        totalOps = (int)totalOpsLong;
        totalRequests = (int)totalRequestsLong;

        out.defaultWriteObject();
    }

    @Override
    public Latency clone() {
        try {
            return (Latency) super.clone();
        } catch (CloneNotSupportedException e) {
            /* Should never happen. */
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        if (totalOpsLong == 0) {
            return "No operations";
        }

        final DecimalFormat fmt = FormatUtil.decimalScale2();
        
        return "maxTrackedLatencyMillis=" + 
               fmt.format(maxTrackedLatencyMillis) +
               " totalOps=" + fmt.format(totalOpsLong) +
               " totalReq=" + fmt.format(totalRequestsLong) +
               " reqOverflow=" + fmt.format(opsOverflow) +
               " min=" + fmt.format(min) +
               " max=" + fmt.format(max) +
               " avg=" + fmt.format(avg) +
               " 95%=" + fmt.format(percent95) +
               " 99%=" + fmt.format(percent99);
    }

    /**
     * @return the number of operations recorded by this stat.
     */
    public long getTotalOps() {
        return totalOpsLong;
    }

    public int getTotalOpsInt() {
        return totalOps;
    }

    /**
     * @return the number of requests recorded by this stat.
     */
    public long getTotalRequests() {
        return totalRequestsLong;
    }

    public int getTotalRequestsInt() {
        return totalRequests;
    }

    /**
     * @return the number of requests which exceed the max expected latency
     */
    public int getRequestsOverflow() {
        return opsOverflow;
    }

    /**
     * @return the max expected latency for this kind of operation
     */
    public int getMaxTrackedLatencyMillis() {
        return maxTrackedLatencyMillis;
    }

    /**
     * @return the fastest latency tracked
     */
    public int getMin() {
        return min;
    }

    /**
     * @return the slowest latency tracked
     */
    public int getMax() {
        return max;
    }

    /**
     * @return the average latency tracked
     */
    public float getAvg() {
        return avg;
    }

    /**
     * @return the 95th percentile latency tracked by the histogram
     */
    public int get95thPercent() {
        return percent95;
    }

    /**
     * @return the 99th percentile latency tracked by the histogram
     */
    public int get99thPercent() {
        return percent99;
    }

    /** 
     * Add the measurements from "other" and recalculate the min, max, and
     * average values. The 95th and 99th percentile are not recalculated, 
     * because the histogram from LatencyStatis not available, and those values
     * can't be generated.
     */
    public void rollup(Latency other) {
        if (other == null || other.totalOpsLong == 0 ||
            other.totalRequestsLong == 0) {
            throw new IllegalStateException
                ("Can't rollup a Latency that doesn't have any data");
        }

        if (maxTrackedLatencyMillis != other.maxTrackedLatencyMillis) {
            throw new IllegalStateException
                ("Can't rollup a Latency whose maxTrackedLatencyMillis is " +
                 "different");
        }

        if (min > other.min) {
            min = other.min;
        }

        if (max < other.max) {
            max = other.max;
        }

        avg = ((totalRequestsLong * avg) +
               (other.totalRequestsLong * other.avg)) /
              (totalRequestsLong + other.totalRequestsLong);

        /* Clear out 95th and 99th. They have become invalid. */
        percent95 = 0;
        percent99 = 0;

        totalOpsLong += other.totalOpsLong;
        totalRequestsLong += other.totalRequestsLong;
        opsOverflow += other.opsOverflow;
    }
}
