/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import oracle.nosql.common.json.JsonUtils;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 */
public class TableLimits {

    /**
     * Table limits option
     */
    private enum LimitsMode {
        PROVISIONED,
        AUTO_SCALING;
    }

    private static final TableLimits NO_LIMITS =
        new TableLimits(Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        Integer.MAX_VALUE);

    private int readUnits;
    private int writeUnits;
    private int tableSize;
    private String mode;

    public static TableLimits getNoLimits() {
        return NO_LIMITS;
    }

    /* Needed for serialization */
    public TableLimits() {
    }

    /**
     * Limits constructor for provisioned table.
     */
    public TableLimits(int readUnits,
                       int writeUnits,
                       int tableSize) {
        this(readUnits, writeUnits, tableSize, LimitsMode.PROVISIONED.name());
    }

    /**
     * Limits constructor for auto scaling table.
     */
    public TableLimits(int tableSize) {
        this(-1, -1, tableSize, LimitsMode.AUTO_SCALING.name());
    }

    /**
     * Limits constructor used when retrieving limits info from the metadata
     * store.
     */
    public TableLimits(int readUnits,
                       int writeUnits,
                       int tableSize,
                       String mode) {
        this.readUnits = readUnits;
        this.writeUnits = writeUnits;
        this.tableSize = tableSize;
        this.mode = mode;
    }

    /**
     * @return the readUnits
     */
    public int getReadUnits() {
        return readUnits;
    }

    /**
     * Set readUnits. Used by jersey to deserialize json
     */
    public void setReadUnits(int readUnits) {
        this.readUnits = readUnits;
    }

    /**
     * @return the writeUnits
     */
    public int getWriteUnits() {
        return writeUnits;
    }

    /**
     * Set writeUnits. Used by jersey to deserialize json
     */
    public void setWriteUnits(int writeUnits) {
        this.writeUnits = writeUnits;
    }

    /**
     * @return the storageMaxGB
     */
    public int getTableSize() {
        return tableSize;
    }

    /**
     * Set storage size, in gb. Used by jersey to deserialize json
     */
    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    /**
     * @return mode
     * To be compatible with old TableLimits, return PROVISIONED if mode is
     * null.
     */
    public String getMode() {
        return mode == null ? LimitsMode.PROVISIONED.name() : mode;
    }

    /**
     * Sets the mode.
     */
    public void setMode(String mode) {
        this.mode = mode;
    }

    /**
     * @return true if table limits mode is auto scaling.
     */
    public boolean modeIsAutoScaling() {
        return modeIsAutoScaling(getMode());
    }

    /**
     * @return true if table limits mode is provisioned.
     */
    public boolean modeIsProvisioned() {
        return modeIsProvisioned(getMode());
    }

    /**
     * Returns true if this object is less than the other, where less than
     * means that any one of the 3 fields is less than the other. It doesn't
     * matter if one if greater; any field being less counts.
     */
    public boolean isLessThan(TableLimits other) {
        if (modeIsAutoScaling()) {
            return tableSize < other.tableSize;
        }
        if (modeIsProvisioned()) {
            return tableSize < other.tableSize ||
                readUnits < other.readUnits ||
                writeUnits < other.writeUnits;
        }
        return false;
    }

    public static boolean modeIsProvisioned(String mode) {
        return mode.equals(LimitsMode.PROVISIONED.name());
    }

    public static boolean modeIsAutoScaling(String mode) {
        return mode.equals(LimitsMode.AUTO_SCALING.name());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof TableLimits)) {
            return false;
        }
        TableLimits otherLimits = (TableLimits) other;
        return readUnits == otherLimits.readUnits &&
               writeUnits == otherLimits.writeUnits &&
               tableSize == otherLimits.tableSize &&
               getMode().equals(otherLimits.getMode());
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash * 31 + readUnits;
        hash = hash * 31 + writeUnits;
        hash = hash * 31 + tableSize;
        hash = hash * 31 + getMode().hashCode();
        return hash;
    }

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
