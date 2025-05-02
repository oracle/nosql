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
package com.sleepycat.je.log.entry;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Properties;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.Timestamp;

/**
 * This log entry is used to indicate that the environment's log files are not
 * recoverable and that some sort of curative action should happen first. It's
 * a general purpose mechanism that can be used for many types of errors.
 */
public class RestoreRequired implements Loggable {

    /* The failure type is used to decide on the course of action. */
    public enum FailureType {

        /**
         * Used to prevent opening an env after a partial/aborted network
         * restore.
         */
        NETWORK_RESTORE,

        /**
         * Only used when {@link EnvironmentParams#LOG_CHECKSUM_FATAL} is true
         * (not the default), and this option may be removed in the future.
         */
        LOG_CHECKSUM,

        /**
         * Is no longer used, but we may attempt to open an old env where it
         * was written to the marker file.
         */
        BTREE_CORRUPTION
    };

    private FailureType failureType;

    /* For debugging, information */
    private Timestamp time;

    /*
     * PropVals is a general purpose, serialized property list, to hold
     * whatever each failure type needs, in order to fix the environment.
     */
    private String propVals;

    public RestoreRequired(FailureType failureType,
                           Properties props) throws IOException {
        this.failureType = failureType;
        time = new Timestamp(TimeSupplier.currentTimeMillis());
        StringWriter sw = new StringWriter();
        props.store(sw, null);
        propVals = sw.toString();
    }

    public RestoreRequired() {
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public Properties getProperties() {
        Properties p = new Properties();
        StringReader reader = new StringReader(propVals);
        try {
            p.load(reader);
        } catch (IOException e) {
            /* This should never occur since there is no real IO. */
            throw EnvironmentFailureException.unexpectedException(e);
        }
        return p;
    }

    @Override
    public int getLogSize() {
        return LogUtils.getStringLogSize(failureType.name()) +
            LogUtils.getTimestampLogSize(time) +
            LogUtils.getStringLogSize(propVals);
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeString(logBuffer, failureType.name());
        LogUtils.writeTimestamp(logBuffer, time);
        LogUtils.writeString(logBuffer, propVals);
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer itemBuffer,
                            int entryVersion) {
        String typeName = LogUtils.readString(itemBuffer, entryVersion);
        failureType = FailureType.valueOf(FailureType.class, typeName);
        time = LogUtils.readTimestamp(itemBuffer);
        propVals = LogUtils.readString(itemBuffer, entryVersion);
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<RestoreRequired failureType=\"").append(failureType);
        sb.append("\" time=\"").append(time);
        sb.append("\" properties=\"").append(propVals);
        sb.append("\"/>");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof RestoreRequired)) {
            return false;
        }

        RestoreRequired otherEntry = (RestoreRequired) other;
        if (!time.equals(otherEntry.time)) {
            return false;
        }

        if (!propVals.equals(otherEntry.propVals)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        dumpLog(sb, true);
        return sb.toString();
    }
}
