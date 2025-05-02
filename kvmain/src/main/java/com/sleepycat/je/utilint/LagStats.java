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

package com.sleepycat.je.utilint;

import static com.sleepycat.je.rep.impl.node.ReplicaStatDefinition.REPLICA_TXN_END_TIME_LAG;

import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.json_simple.JsonObject;

/**
 * The lag stats.
 */
public class LagStats extends Stat<JsonObject> {

    private static final long serialVersionUID = 1L;

    private final AtomicReference<JsonObject> statsObject;

    public LagStats(StatGroup group,
                     AtomicReference<JsonObject> obj) {
        super(group, REPLICA_TXN_END_TIME_LAG);
        this.statsObject = obj;
    }

    public LagStats(AtomicReference<JsonObject> obj) {
        super(REPLICA_TXN_END_TIME_LAG);
        this.statsObject = obj;
    }

    @Override
    public void add(Stat<JsonObject> other) {
        throw new IllegalStateException(
            "LagStats does not support externally adding the value");
    }

    @Override
    public JsonObject get() {
        return statsObject.get();
    }

    @Override
    public boolean isNotSet() {
        return statsObject.get() == null;
    }

    @Override
    public void set(JsonObject stats) {
        statsObject.set(stats);
    }

    @Override
    public void clear() {
    }

    @Override
    public LagStats copy() {
        return new LagStats(statsObject);
    }

    @Override
    public LagStats computeInterval(Stat<JsonObject> base) {
        return copy();
    }

    @Override
    public void negate() { }

    @Override
    protected String getFormattedValue() {
        final JsonObject obj = statsObject.get();
        if (obj == null) {
            return null;
        }
        /*
         * Replace "," with ";". The stats are written to CSV files which
         * uses "," as separator. This collide with the json format. We
         * follow the same convention as LongArrayStat to separate items
         * with ";".
         *
         * Note that this method seems to be actually not used. The same
         * replacement is added in StatCapture as well.
         */
        return obj.toString().replace(",", ";");
    }
}

