/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.cloudsim;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.as.AggregationService;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.Proxy;
import oracle.nosql.proxy.ProxyMain;

/*
 * A standalone Oracle NoSQL Cloud instance running on a single KVLite-based
 * store. An in-process KVLite instance is started against the specified
 * root directory and a cloud HTTP proxy instance is started on that store.
 * If the root directory already contains a store it is used. If not, the
 * store is created.
 *
 * Users of the cloud driver connect with the proxy using the port provided.
 *
 * Usage:
 *  CloudSim -root <path-to-kvroot>
 *    [-host <hostname>]       (defaults to localhost)
 *    [-storePort <port>       (defaults to 5000)
 *    [-httpPort <http-port>]  (defaults to 8080)
 *    [-throttle true|false]   (defaults to false)
 *    [-noLimits true|false]   (defaults to false)
 *
 * The store name is not an argument. The name "nosqlstore" is used
 * unconditionally. The store name is not exposed to users. Applications
 * only need the host and port of the proxy which they use in the form
 * of a URL.
 */

public class CloudSim {
    /*
     * CloudSim doesn't need or use peak throughput information. Ideally there
     * would be a way to disable it.
     */
    private static final int PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC =
        Integer.MAX_VALUE;
    private static final int PEAK_THROUGHPUT_DEFAULT_TTL_DAY = 1;

    static final String storeName = "nosqlstore";
    static final int proxyThreads = 5;
    static final String defaultHost = "localhost";
    static final int defaultStorePort = 5000;
    static final int defaultHttpPort = 8080;
    static final int defaultNumShards = 1;
    static final int defaultNumProxies = 1;

    KVLite kvlite;
    Proxy[] proxies;
    AggregationService as;
    boolean throttle;
    boolean async;
    boolean verbose;

    public CloudSim(String rootPath) {
        this(rootPath, defaultHost, defaultStorePort, defaultHttpPort,
             false /* throttle */, false /* verbose */);
    }

    public CloudSim(String rootPath,
                    String host,
                    int storePort,
                    int httpPort,
                    boolean throttle,
                    boolean verbose) {
        this(rootPath, host,
             0, 0, // default num shards and num proxies
             storePort,
             httpPort, throttle,
             false /* async */,
             false, false, // use default cloudsim mode and limits
             false, // don't force V3
             verbose);
    }

    public CloudSim(String rootPath,
                    String host,
                    int storePort,
                    int httpPort,
                    boolean throttle,
                    boolean onPremMode,
                    boolean noLimits,
                    boolean verbose) {
        this(rootPath, host,
             0, 0, // default num shards and num proxies
             storePort,
             httpPort, throttle,
             false /* async */,
             onPremMode, noLimits,
             false, /* forceV3 */
             verbose);
    }

    public CloudSim(String rootPath,
                    String host,
                    int numShards,
                    int numProxies,
                    int storePort,
                    int httpPort,
                    boolean throttle,
                    boolean async,
                    boolean onPremMode,
                    boolean noLimits,
                    boolean forceV3,
                    boolean verbose) {

        try {
            this.throttle = throttle;
            this.verbose = verbose;
            this.async = async;

            if (host == null) {
                host = defaultHost;
            }
            if (storePort == 0) {
                storePort = defaultStorePort;
            }
            if (httpPort == 0) {
                httpPort = defaultHttpPort;
            }
            if (numShards == 0) {
                numShards = defaultNumShards;
            }
            if (numProxies == 0) {
                numProxies = defaultNumProxies;
            }

            verbose("Starting NoSQL database instance using host, storePort, " +
                    "httpPort: " + host + ", " + storePort + ", " + httpPort +
                    ", throttle: " + throttle);

            final int numStorageNodes = numShards;
            final int capacity = (numStorageNodes > 2) ? 3 : 1;
            final int repfactor = (numStorageNodes > 2) ? 3 : 1;
            final int partitions = 10 * numStorageNodes * capacity;

            final String portstr = getPortStr(storePort, numStorageNodes);
            final String rangestr = getRangeStr(storePort, numStorageNodes);

            if (numShards > 1) {
                verbose("  SNs=" + numStorageNodes);
                verbose("  capacity=" + capacity);
                verbose("  repfactor=" + repfactor);
                verbose("  partitions=" + partitions);
                verbose("  portstr=\"" + portstr + "\"");
                verbose("  rangestr=\"" + rangestr + "\"");
            }

            if (throttle) {
                /* throttle is not valid with either onPremMode or noLimits */
                if (onPremMode || noLimits) {
                    throw new IllegalArgumentException(
                        "On prem mode and elimination of limits cannot be " +
                        "used with throttling");
                }
            }
            if (onPremMode && noLimits) {
                throw new IllegalArgumentException(
                    "On prem mode and no limits cannot be used together");
            }

            kvlite = new KVLite(rootPath,
                                storeName,
                                portstr,
                                true,   // runBootAdmin
                                host,   // hostName
                                rangestr,
                                null,   // servicePortRange
                                partitions,
                                null,   // mountPoint
                                true,   // useThreads
                                false,  // isSecure
                                null,   // no backup to restore
                                0,      // adminWebPort
                                numStorageNodes,
                                repfactor,
                                capacity);

            kvlite.setVerbose(verbose);
            kvlite.setTableOnly(true);

            /* start and wait for services */
            kvlite.start(true);

            if (kvlite.getSNA() == null) {
                /* KVLite doesn't always throw on startup problems */
                System.err.println("Unable to start CloudSim");
                return;
            }

            verbose("NoSQL database instance started");

            /*
             * This will quiet things down on shutdown
             */
            TestStatus.setActive(true);

            /*
             * Start prox(ies)
             */
            String ptype = (onPremMode ?
                            Config.ProxyType.KVPROXY.name() :
                            Config.ProxyType.CLOUDSIM.name());
            String hHosts = host + ":" + storePort;
            Properties commandLine = new Properties();
            commandLine.setProperty(Config.PROXY_TYPE.paramName,
                                    ptype);
            commandLine.setProperty(Config.HTTP_PORT.paramName,
                                    Integer.toString(httpPort));
            commandLine.setProperty(Config.NUM_REQUEST_THREADS.paramName,
                                    Integer.toString(proxyThreads));
            commandLine.setProperty(Config.NUM_ACCEPT_THREADS.paramName,"2");
            commandLine.setProperty(Config.STORE_NAME.paramName, storeName);
            commandLine.setProperty(Config.HELPER_HOSTS.paramName, hHosts);
            commandLine.setProperty(Config.VERBOSE.paramName,
                                    Boolean.toString(verbose));
            commandLine.setProperty(Config.ASYNC.paramName,
                                    Boolean.toString(async));
            commandLine.setProperty(Config.PULL_RULES_INTERVAL_SEC.paramName,
                                    "0");
            if (noLimits) {
                commandLine.setProperty(Config.NO_LIMITS.paramName,
                                        Boolean.toString(noLimits));
            }
            if (forceV3) {
                commandLine.setProperty(Config.FORCE_V3.paramName,
                                        Boolean.toString(forceV3));
            }

            proxies = new Proxy[numProxies];
            for (int x=0; x<numProxies; x++) {
                commandLine.setProperty(Config.HTTP_PORT.paramName,
                                Integer.toString(httpPort + (x * 10)));
                if (x > 0) {
                    TimeUnit.MILLISECONDS.sleep(300);
                }
                verbose("Starting proxy on port " + (httpPort + (x * 10)));
                proxies[x] = ProxyMain.startProxy(commandLine);
                assert proxies[x] != null;
            }

            if (throttle) {
                int numRetries = 0;
                final int maxRetries = 5;
                final int delay = 1000;
                verbose("Starting AggregationService to enable throttling");
                String[]  helperHosts = new String[]{(host + ":" + storePort)};
                Exception failEx = null;
                while (numRetries < maxRetries) {
                    try {
                        as = AggregationService.createAggregationService(
                            storeName,
                            helperHosts,
                            180, // throughputHistorySecs
                            5, 5, // poll periods 5 seconds
                            PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC,
                            PEAK_THROUGHPUT_DEFAULT_TTL_DAY,
                            5);   // max threads
                        break;
                    } catch (IllegalStateException ise) {
                        failEx = ise;
                    } catch (Exception e) {
                        /* maybe don't retry these; that's why it's separate */
                        failEx = e;
                    }
                    ++numRetries;

                    try { Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                    }
                }
                if (as == null) {
                    /*
                     * Don't treat this is fatal. The AS is not started but the
                     * system will work.
                     */
                    System.err.println("Could not start AggregationService: " +
                               (failEx != null ? failEx.getMessage() : ""));
                    System.err.println("Continuing...");
                }
            }
            for (Proxy p : proxies) {
                p.getTenantManager().waitForStoreInit(20); // 20s wait
            }

            /* unconditional output */
            System.out.println("Oracle NoSQL Cloud Simulator is ready");
        } catch (Exception e) {
            System.err.println("Unable to start CloudSim: " + e.getMessage());
            stop();
        }
    }

    public void stop() {
        try {
            if (as != null) {
                as.stop();
                as = null;
            }
            for (int x=0; x<proxies.length; x++) {
                if (proxies[x] != null) {
                    proxies[x].shutdown(3, TimeUnit.SECONDS);
                    proxies[x] = null;
                }
            }
            if (kvlite != null) {
                verbose("Stopping NoSQL database instance");
                kvlite.stop(true);
                kvlite = null;
            }
        } catch (Exception e) {
            verbose("Exception on CloudSim shutdown: " + e.getMessage());
        }
    }

    private void verbose(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    private String getPortStr(int port, int numStorageNodes) {
        StringBuilder sb = new StringBuilder();
        for (int x=0; x<numStorageNodes; x++) {
            if (x>0) sb.append(KVLite.DEFAULT_SPLIT_STR);
            sb.append(port);
            port += 5;
        }
        return sb.toString();
    }

    private String getRangeStr(int port, int numStorageNodes) {
        final int incr = (numStorageNodes * 5);
        port += (5 * numStorageNodes) + incr;
        StringBuilder sb = new StringBuilder();
        for (int x=0; x<numStorageNodes; x++) {
            if (x>0) sb.append(KVLite.DEFAULT_SPLIT_STR);
            sb.append(port);
            sb.append(",");
            port += incr;
            sb.append(port);
            port += 1;
        }
        return sb.toString();
    }

    private static class CloudSimParser extends CommandParser {
        private static final String STORE_PORT = "-storePort";
        private static final String PROXY_PORT = "-httpPort";
        private static final String THROTTLE = "-throttle";
        private static final String ASYNC = "-async";
        private static final String SHARDS = "-numShards";
        private static final String PROXIES = "-numProxies";
        private static final String ONPREM = "-onPrem";
        private static final String NO_LIMITS = "-noLimits";
        private static final String FORCE_V3 = "-forceV3";

        private int storePort;
        private int httpPort;
        private boolean throttle;
        private boolean async;
        private int numShards;
        private int numProxies;
        private boolean onPremMode;
        private boolean noLimits;
        private boolean forceV3;

        public CloudSimParser(String[] args) {
            super(args);
        }

        private int getStorePort() {
            return storePort;
        }

        private int getHttpPort() {
            return httpPort;
        }

        private boolean getThrottle() {
            return throttle;
        }

        private boolean getAsync() {
            return async;
        }

        private int getNumShards() {
            return numShards;
        }

        private int getNumProxies() {
            return numProxies;
        }

        private boolean getOnPremMode() {
            return onPremMode;
        }

        private boolean getForceV3() {
            return forceV3;
        }

        private boolean getNoLimits() {
            return noLimits;
        }

        @Override
        protected void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            }
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(STORE_PORT)) {
                storePort = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(PROXY_PORT)) {
                httpPort = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(THROTTLE)) {
                throttle = Boolean.parseBoolean(nextArg(arg));
                return true;
            }

            /* hidden */
            if (arg.equals(SHARDS)) {
                numShards = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(PROXIES)) {
                numProxies = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(ASYNC)) {
                async = Boolean.parseBoolean(nextArg(arg));
                return true;
            }

            if (arg.equals(ONPREM)) {
                onPremMode = Boolean.parseBoolean(nextArg(arg));
                return true;
            }

            if (arg.equals(FORCE_V3)) {
                forceV3 = Boolean.parseBoolean(nextArg(arg));
                return true;
            }

            if (arg.equals(NO_LIMITS)) {
                noLimits = Boolean.parseBoolean(nextArg(arg));
                return true;
            }

            if (arg.equals("-?") || arg.equals("-help")) {
                usage(null);
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println("CloudSim " +
                               getRootUsage() + "\n\t" +
                               optional(getHostUsage()) +
                               optional("-storePort <store port>") +
                               optional("-httpPort <http port>") +
                               optional("-throttle <true|false>") +
                               optional("-noLimits <true|false>") +
                               optional("-verbose"));
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        CloudSimParser clp = new CloudSimParser(args);
        clp.parseArgs();

        /*
         * The constructor starts the instance which is stopped
         * by killing the process. If started in-process the returned
         * instance can be used to call the stop() method.
         */
        @SuppressWarnings("unused")
        CloudSim cl = new CloudSim(clp.getRootDir(),
                                   clp.getHostname(),
                                   clp.getNumShards(),
                                   clp.getNumProxies(),
                                   clp.getStorePort(),
                                   clp.getHttpPort(),
                                   clp.getThrottle(),
                                   clp.getAsync(),
                                   clp.getOnPremMode(),
                                   clp.getNoLimits(),
                                   clp.getForceV3(),
                                   clp.getVerbose());
    }
}
