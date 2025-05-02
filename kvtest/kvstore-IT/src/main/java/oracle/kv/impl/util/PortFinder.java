/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import oracle.kv.impl.topo.util.FreePortLocator;

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
    
    /**
     * Class to parse the PortFinder command line.
     */
    final static class PortFinderParser extends CommandParser {
        final String COMMAND_NAME = "oracle.kv.util.PortFinder";
        final String HA_RANGE_FLAG = "-haRangeSize";
        final String SHOWOPT_FLAG = "-show";
        final String COMMAND_DESC =
            "attempts to find free ports for a storage node";
        final String PORT_FINDER_USAGE =
            "-haRangeSize HA_RANGE_SIZE";
        final String COMMAND_ARGS =
            CommandParser.getHostUsage() + " " +
            CommandParser.getPortUsage() + " " +
            PORT_FINDER_USAGE + getShowOptUsage();

        private int haRangeSize;

        enum ShowOpt { all, registryport, harange }
        private ShowOpt showOpt = ShowOpt.all;

        PortFinderParser(String[] args1) {
            super(args1);
        }

        int getHaRangeSize() {
            return haRangeSize;
        }

        ShowOpt getShowOpt() {
            return showOpt;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                               "\n\t" + COMMAND_ARGS);
            System.exit(-1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(HA_RANGE_FLAG)) {
                haRangeSize = nextIntArg(arg);
                return true;
            }
            if (arg.equals(SHOWOPT_FLAG)) {
                showOpt = ShowOpt.valueOf(nextArg(arg));
                return true;
            }
            return false;
        }

        @Override
        protected void verifyArgs() {
            if (getHostname() == null) {
                missingArg(HOST_FLAG);
            }
            if (getRegistryPort() == 0) {
                missingArg(PORT_FLAG);
            }
            if (haRangeSize == 0) {
                usage("Flag " + HA_RANGE_FLAG + " is required");
            }
        }

        private static String getShowOptUsage() {
            String r = "\n\t[ -show ";
            int n = 0;
            for (ShowOpt s : ShowOpt.values()) {
                if (n++ > 0) {
                    r += " | ";
                }
                r += s.toString();
            }
            return r + " ]";
        }
    }
    
    /**
     * Use hostName;registryHost;haFirstPort as stdout format.
     */
    public void genHostInfo() {
        System.out.println(getHostname() + ";" + getRegistryPort() + ";" +
                           getHaFirstPort());
    }
    
    public static void main(String args[]) {
        
        PortFinderParser parser = new PortFinderParser(args);
        parser.parseArgs();
        String hostName = parser.getHostname();
        int startPort = parser.getRegistryPort();
        int haRangeSize = parser.getHaRangeSize();
        
        PortFinder pf = new PortFinder(startPort, haRangeSize, hostName);
        switch(parser.getShowOpt()) {
        case all:
            pf.genHostInfo();
            break;
        case registryport:
            System.out.println(pf.getRegistryPort());
            break;
        case harange:
            System.out.println(pf.getHaRange());
            break;
        }
    }
}
