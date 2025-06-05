/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Contains peak usage stats for a given table for the given period of
 * time.
 *
 * This class encapsulates the REST response to the path:
 *   /tables/{tablename}/peakusage
 */
public class PeakUsage {
    private static final int CURRENT_VERSION = 1;
    private int version;

    private long startTimeMillis;
    private int readUnits;
    private int writeUnits;

    public PeakUsage() {}

    public PeakUsage(long startTimeMillis, int readUnits, int writeUnits) {
        this.version = CURRENT_VERSION;
        this.startTimeMillis = startTimeMillis;
        this.readUnits = readUnits;
        this.writeUnits = writeUnits;
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

    public long getStartTimeMillis() {
        return startTimeMillis;
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
        builder.append(", readUnits=");
        builder.append(readUnits);
        builder.append(", writeUnits=");
        builder.append(writeUnits);
        builder.append("]");
    }
}
