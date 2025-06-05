/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Contains utilization stats for a given table for the given period of
 * time.
 *
 * TOOD: how should the time period be expressed? Is a string date
 * preferable to a start time which is a long? Should duration be
 * something else?
 *
 * This class encapsulates the REST response to the path:
 *   /tables/{tablename}/usage
 */
public class TableUsage {
    /*
     * - version 2, added maxPartitionUsage
     */
    private static final int CURRENT_VERSION = 2;
    private int version;

    private long startTimeMillis;
    private int secondsInPeriod;
    private int readUnits;
    private int writeUnits;
    private int storageGB;
    private int maxPartitionUsage;
    private int readThrottleCount;
    private int writeThrottleCount;
    private int storageThrottleCount;

    public TableUsage() {}

    public TableUsage(long startTimeMillis, int secondsInPeriod,
                      int readUnits, int writeUnits, int storageGB,
                      int maxPartitionUsage, int readThrottleCount,
                      int writeThrottleCount, int storageThrottleCount) {
        this.version = CURRENT_VERSION;
        this.startTimeMillis = startTimeMillis;
        this.secondsInPeriod = secondsInPeriod;
        this.readUnits = readUnits;
        this.writeUnits = writeUnits;
        this.storageGB = storageGB;
        this.maxPartitionUsage = maxPartitionUsage;
        this.readThrottleCount = readThrottleCount;
        this.writeThrottleCount = writeThrottleCount;
        this.storageThrottleCount = storageThrottleCount;
    }

    public int getVersion() {
        return version;
    }

    public int getReadUnits() {
        return readUnits;
    }

    public int getWriteUnits() {
        return writeUnits;
    }

    public int getStorageGB() {
        return storageGB;
    }

    public int getMaxPartitionUsage() {
        return maxPartitionUsage;
    }

    public int getReadThrottleCount() {
        return readThrottleCount;
    }

    public int getWriteThrottleCount() {
        return writeThrottleCount;
    }

    public int getStorageThrottleCount() {
        return storageThrottleCount;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public int getSecondsInPeriod() {
        return secondsInPeriod;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        toBuilder(builder);
        return builder.toString();
    }

    public void toBuilder(StringBuilder builder) {
        builder.append("TableUsage [startTimeMillis=");
        builder.append(startTimeMillis);
        builder.append(", secondsInPeriod=");
        builder.append(secondsInPeriod);
        builder.append(", readUnits=");
        builder.append(readUnits);
        builder.append(", writeUnits=");
        builder.append(writeUnits);
        builder.append(", storageGB=");
        builder.append(storageGB);
        builder.append(", maxPartitionUsage=");
        builder.append(maxPartitionUsage);
        builder.append(", readThrottleCount=");
        builder.append(readThrottleCount);
        builder.append(", writeThrottleCount=");
        builder.append(writeThrottleCount);
        builder.append(", storageThrottleCount=");
        builder.append(storageThrottleCount);
        builder.append("]");
    }
}
