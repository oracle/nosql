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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A range of ports that the listener can choose from.
 */
public class ListenerPortRange {

    /**
     * A port range that for any port to listen on.
     */
    public static final ListenerPortRange ANY_PORT_RANGE =
        new ListenerPortRange();

    /** The first port in the range, or 0 if the range is unlimited. */
    private final int portStart;

    /** The last port in the range, or 0 if the range is unlimited. */
    private final int portEnd;

    /**
     * Constructor to listen on a range of ports.
     *
     * @param portStart the starting port (inclusive), must be positive
     * @param portEnd the ending port (inclusive), must be greater than
     * portStart and less than 65536
     * @throws IllegalArgumentException if the ports do not satisfy the
     * requirements
     */
    public ListenerPortRange(int portStart, int portEnd) {
        if (portStart <= 0) {
            throw new IllegalArgumentException(
                "Starting port should be greater than 0");
        }
        if (portEnd < portStart) {
            throw new IllegalArgumentException(
                    "Starting port is larger than ending port");
        }
        if (portEnd >= 1 << 16) {
            throw new IllegalArgumentException(
                    "Port cannot be larger than 16 bits");
        }
        this.portStart = portStart;
        this.portEnd = portEnd;
    }

    /**
     * Constructor to listen on any port.
     */
    public ListenerPortRange() {
        portStart = 0;
        portEnd = 0;
    }

    /**
     * Returns the starting port (inclusive) or 0 if the range is unlimited.
     *
     * @return the starting port or 0
     */
    public int getPortStart() {
        return portStart;
    }

    /**
     * Returns the ending port (inclusive) or 0 if the range is unlimited.
     *
     * @return the ending port or 0
     */
    public int getPortEnd() {
        return portEnd;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if ((obj == null) || !(obj instanceof ListenerPortRange)) {
            return false;
        }
        ListenerPortRange that = ((ListenerPortRange) obj);
        return ((this.portStart == that.portStart) &&
                (this.portEnd == that.portEnd));
    }

    @Override
    public int hashCode() {
        int hash = portStart;
        hash = hash * 31 + portEnd;
        return hash;
    }

    @Override
    public String toString() {
         return String.format("%s-%s", portStart, portEnd);
    }
}
