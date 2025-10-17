/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.util;

/**
 * A utility to find available ports for the RMI registry, Admin HTTP port,
 * and a range of ports for use by RepNodes.
 */
public class PortFinder {

    private int registryPort;
    private String hostname;
    private String haRange;
    private int haFirstPort;
    private int haNextPort;
    private final int haRangeSize;
    private final FreePortLocator locator;
    private int mgmtTrapPort;
    private int mgmtPollPort;
    private static final int RANGE = 100;

    public PortFinder(int startingPort, int haRangeSize) {
        this(startingPort, haRangeSize, "localhost");
    }

    public PortFinder(int startingPort, int haRangeSize, String hostname) {
        this.haRangeSize = haRangeSize;
        haFirstPort = 0;
        registryPort = 0;
        haRange = null;
        this.hostname = hostname;

        locator =
            new FreePortLocator(hostname, startingPort, startingPort + RANGE);
        findPorts();
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public String getHostname() {
        return hostname;
    }

    public String getHaRange() {
        return haRange;
    }

    public int getHaRangeSize() {
        return haRangeSize;
    }

    public int getHaFirstPort() {
        return haFirstPort;
    }

    public int getMgmtPollPort() {
        return mgmtPollPort;
    }

    public int getMgmtTrapPort() {
        return mgmtTrapPort;
    }

    /**
     * Supply a sequence of port numbers, starting with HaFirstPort.
     */
    public int getHaNextPort() {
        int nextPort;
        nextPort = haNextPort++;
        assert nextPort < haFirstPort + haRangeSize;
        return nextPort;
    }

    private void findPorts() {
        /*
         * When adding new ports to this class, allocate them at the end of
         * this method, so that the sequence is preserved.  This matters for
         * cross-release testing.
         */

        registryPort = locator.next();

        /**
         * Need haRangeSize contiguous available ports...
         */
        int startRange = locator.next();
        int previous = startRange;
        int num = 0;
        while (num <= haRangeSize) {
            int current = locator.next();
            if (current != previous + 1) {
                /* start over with current */
                num = 0;
                startRange = previous = current;
            } else {
                previous = current;
                ++num;
            }
        }
        haRange = (startRange + "," + (startRange + haRangeSize - 1));
        haFirstPort = startRange;
        haNextPort = haFirstPort;

        mgmtPollPort = locator.next();
        mgmtTrapPort = locator.next();
    }
}
