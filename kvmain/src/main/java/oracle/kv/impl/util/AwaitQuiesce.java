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

package oracle.kv.impl.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Utility class used to wait for a KVS to quiesce. A KVS is considered to be
 * quiescent over a specified period of time if:
 * <ol>
 * <li>
 * There is no write activity associated with the store. The write activity can
 * be the result of explicit user requests or partition migrations.</li>
 * <li>
 * The replicas have caught up with the master and are at the same VLSN.</li>
 * </ol>
 * This class supports secure KVStore access through the oracle.kv.login
 * property only.
 */
public class AwaitQuiesce {

    private static final Logger mainLogger =
        ClientLoggerUtils.getLogger(AwaitQuiesce.class, "main");

    /**
     * The topology used as the basis for quiescing the store.
     */
    private final Topology topology;

    /**
     * Convenience reg utils handle based upon the above Topology for accessing
     * the RNs.
     */
    private final RegistryUtils regUtils;

    /**
     * The thread pool used to manage the threads used to check the state of
     * RGs in parallel.
     */
    private final ThreadPoolExecutor threadPool;

    /**
     * The sum of the VLSNs at each RG master in the store. It's used to
     * whether there are any writes during the quiesce period. It also provides
     * a rough approximation of the VLSN change rate while the store is being
     * written.
     */
    private final AtomicLong storewideVLSNsum = new AtomicLong(0);

    /**
     * Map used capture state associated with RNs that have not caught up, or
     * in the case of network partitioning or failovers, any multiple masters
     * as well.
     */
    private final Map<RepGroupId, List<QuiesceStatus>> awaitRGs;

    private AwaitQuiesce(Topology topology, Logger logger) {
        this.topology = topology;

        /* LoginManager is not required for access to RepNodeAdmin.Ping() */
        regUtils = new RegistryUtils(topology, (LoginManager) null, logger);

        KVThreadFactory factory = new KVThreadFactory("awaitQuiesce", null) {
            @Override
            public Thread.UncaughtExceptionHandler
                makeUncaughtExceptionHandler() {

                return new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        System.err.println("Exiting thread:" + t);
                        e.printStackTrace(System.err);
                    }
                };
            }
        };

        threadPool = new ThreadPoolExecutor
            (100, /* core size */
             100, /* Max pool size: max 100 RGs at a time */
             100000, TimeUnit.MILLISECONDS,
             new LinkedBlockingQueue<Runnable>(),
             factory);

        awaitRGs = new ConcurrentHashMap<RepGroupId, List<QuiesceStatus>>();
    }

    /**
     * @see AwaitQuiesce#await(Topology, long, long)
     */
    private long await(final long quiescePeriodMs, final long timeoutMs)
        throws InterruptedException, TimeoutException {

        final long quiesceStartMs =  System.currentTimeMillis();
        final long limitMs = quiesceStartMs + timeoutMs;

        long prevTimeMs = quiesceStartMs;

        for (long prevStoreCumVLSN = 0;
             System.currentTimeMillis() < limitMs;
             prevStoreCumVLSN = storewideVLSNsum.getAndSet(0)) {

            /**
             * The latch used by RG checking threads to denote thread exit.
             */
            final CountDownLatch threadExitLatch =
                new CountDownLatch(topology.getRepGroupIds().size());

            awaitRGs.clear();

            for (RepGroup rg : topology.getRepGroupMap().getAll()) {
                threadPool.execute(new CheckRepGroup(rg, threadExitLatch));
            }

            final long currTimeMs = System.currentTimeMillis();
            final long latchWaitMs = limitMs - currTimeMs;
            if ((latchWaitMs <= 0) ||
                !threadExitLatch.await(latchWaitMs, TimeUnit.MILLISECONDS)) {
                break;
            }

            final long vlsnDelta = storewideVLSNsum.get() - prevStoreCumVLSN;
            if ((vlsnDelta == 0) && /* no store writes. */
                (awaitRGs.size() == 0)) {
                return (System.currentTimeMillis() - quiesceStartMs);
            }

            if (prevStoreCumVLSN == 0) {
                /* First measurement. */
                final String fmt = "Cumulative store VLSN: %,d on %s. \n";
                System.err.printf(fmt, storewideVLSNsum.get(), currUTC());
            } else if ((prevStoreCumVLSN > 0) && (vlsnDelta > 0)) {

                /*
                 * Writes in progress. vlsnDelta may be -ve if all masters were
                 * not available.
                 */
                final long deltaMs = (System.currentTimeMillis() - prevTimeMs);
                final String fmt = "Store is being actively updated on %s. " +
                    "Cumulative store VLSN: %,d " +
                    "Storewide VLSN delta: %,d, rate: ~%,d VLSNs/sec\n";
                System.err.printf(fmt,
                                  currUTC(),
                                  storewideVLSNsum.get(),
                                  vlsnDelta,
                                  (vlsnDelta * 1000) / deltaMs);
            } else {
                printLaggingRG();
            }
            prevTimeMs = currTimeMs;

            Thread.sleep(quiescePeriodMs);
        }

        final String msg =
            String.format("Could not quiesce in %,d ms", timeoutMs);

       throw new java.util.concurrent.TimeoutException(msg);
    }

    /**
     * Utility method to return current UTC time string.
     */
    private static String currUTC() {
        return FormatUtils.formatDateTime(System.currentTimeMillis());
    }

    /**
     * Prints status information about lagging RNs in an RG, or for RGs that
     * otherwise inconsistent due to network partitioning or master failovers.
     */
    private void printLaggingRG() {
        final String fmt = "Cumulative store VLSN: %,d on %s. \n";

        System.err.printf(fmt, storewideVLSNsum.get(), currUTC());

        /* Print out the pending RGs and the reasons */
        for (Entry<RepGroupId, List<QuiesceStatus>> rge :
            awaitRGs.entrySet()) {

            System.err.printf("Rep Group: %s in flux\n",
                              rge.getKey().toString());

            for (QuiesceStatus rnqw : rge.getValue()) {
                System.err.println(" RN:" + rnqw.rnId +
                                   " Reason: " + rnqw.getReason());
            }
        }
    }

    /**
     * Base class used to capture reasons, one per rn, why this RG has not
     * quiesced.
     */
    private static abstract class QuiesceStatus {
        /* The rn responsible for the group not being quiescent. */
        final RepNodeId rnId;
        /* The reason for this state. */
        final String message;

        QuiesceStatus(RepNodeId rnId, String message) {
            super();
            this.rnId = rnId;
            this.message = message;
        }

        String getReason() {
            return message;
        }
    }

    /**
     * Identifies a lagging replica, or multiple masters responsible for the
     * the group not being quiescent.
     */
    private static class QuiesceLag extends QuiesceStatus {

        final RepNodeStatus rnStatus;

        QuiesceLag(RepNodeId rnId,
            String message,
            RepNodeStatus rnStatus) {
            super(rnId, message);
            this.rnStatus = rnStatus;
        }

        @Override
        String getReason() {
            return message + " Status:" + rnStatus;
        }
    }

    /**
     * Could not reach the RN to get lag status
     */
    private static class QuiesceUnavailable extends QuiesceStatus {

        @SuppressWarnings("unused")
        final Exception exception;

        QuiesceUnavailable(RepNodeId rnId,
                           Exception exception) {
            super(rnId, exception.getMessage());
            this.exception = exception;
        }
    }

    /**
     * The thread that is used to examine the status of the RNs in the RG
     */
    private class CheckRepGroup implements Runnable {
        final RepGroup rg;
        final CountDownLatch latch;

        final HashMap<RepNodeId, RepNodeStatus> groupStatus =
            new HashMap<RepNodeId, RepNodeStatus>();
        final List<QuiesceStatus> quiesceStatus =
            new LinkedList<QuiesceStatus>();

        private CheckRepGroup(RepGroup rg,
            CountDownLatch latch) {
            this.rg = rg;
            this.latch = latch;
        }

        @Override
        public void run() {

            try {
                runInternal();

                if (quiesceStatus.size() != 0) {
                    awaitRGs.put(rg.getResourceId(), quiesceStatus);
                }
            } finally {
                latch.countDown();
            }
        }

        /**
         * Locates the master and calculates the replica lag relative to the
         * master.
         */
        public void runInternal() {

            final RepNodeStatus mRNStatus = locateMaster();

            if (mRNStatus == null) {
                /*
                 * No master, or multiple masters need to retry later. Capture
                 * replica status as rationale.
                 */
                for (Entry<RepNodeId, RepNodeStatus> rne :
                    groupStatus.entrySet()) {

                    quiesceStatus.add(new QuiesceLag(rne.getKey(),
                                           "master unavailable",
                                           rne.getValue()));
                }
                return;
            }

            final long masterVLSN = mRNStatus.getVlsn();
            storewideVLSNsum.getAndAdd(masterVLSN);

            /* Add replicas to quiesceStatus if they are lagging. */
            for (Entry<RepNodeId, RepNodeStatus> rne :
                groupStatus.entrySet()) {

                final RepNodeStatus status = rne.getValue();
                if (masterVLSN == status.getVlsn()) {
                    continue;
                }

                final String message =
                    String.format(" Replica lagging. VLSN delta: %,d." +
                        " Master at VLSN: %,d replica at VLSN:%,d.",
                        (mRNStatus.getVlsn() - status.getVlsn()),
                        mRNStatus.getVlsn(),
                        status.getVlsn());
                quiesceStatus.add(new QuiesceLag(rne.getKey(), message,
                                                 rne.getValue()));
            }
        }

        /**
         * Locates the RN that's serving as the master and populates the
         * groupStatus associated with the RNs in the RG or the quiesceStatus
         * if the RN was not reachable.
         *
         * @return the master RN if there was an unambiguous master. Null if
         * one could not be found or here were duplicates
         */
        private RepNodeStatus locateMaster() {
            RepNodeStatus mRNStatus = null;

            for (RepNode rn : rg.getRepNodes()) {
                try {
                    RepNodeAdminAPI rna =
                        regUtils.getRepNodeAdmin(rn.getResourceId());
                    final RepNodeStatus status = rna.ping();

                    if (status.getReplicationState().isMaster()) {
                        if (mRNStatus != null) {
                            /* Network partition or master switch. */
                            return null;
                        }
                        mRNStatus = status;
                    }
                    groupStatus.put(rn.getResourceId(), status);
                } catch (Exception re) {
                    quiesceStatus.add(new QuiesceUnavailable(rn.getResourceId(),
                                                             re));
                }
            }
            return mRNStatus;
        }
    }

    /**
     * Waits for the store to become quiescent. The store is quiescent if it
     * does not process write requests, or have any replica replay activity for
     * <code>quiescePeriodMs</code>. The method checks every quiescePeriodMs
     * for the absence of such activity during the <code>timeoutMs</code>
     * interval.
     *
     * @param topology the Topology used as the basis for quiesce operation
     *
     * @param quiescePeriodMs The amount of time that the store should not
     * experience write activity and the replicas need to remain consistent
     *
     * @param timeoutMs The maximum amount of time to wait for the store to
     * become quiescent
     *
     * @param logger logger for debugging
     *
     * @return the approx amount of time in ms that it took for the system to
     * become quiescent.
     *
     * @throws TimeoutException if the store did not quiesce within
     * <code>timeoutMs</code>
     */
    public static long await(Topology topology,
                             final long quiescePeriodMs,
                             final long timeoutMs,
                             Logger logger)
        throws InterruptedException, TimeoutException {

        return new AwaitQuiesce(topology, logger)
            .await(quiescePeriodMs, timeoutMs);
    }

    public static void main(String[] args)
        throws KVStoreException, InterruptedException, TimeoutException {

        if (args.length < 3 || args.length > 7) {
            System.err.println(
                "Usage: java " +
                AwaitQuiesce.class.getName() +
                " <topoHost:port> <quiescePeriodSec>" +
                " <quiesceTimeoutSec> [userName] [securityFile]" +
                " [registryOpenTimeoutMs] [registryReadTimeoutMs]");
            System.exit(1);
        }

        String user = null;
        String securityFile = null;
        int regOpenTimeout = KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT;
        int regReadTimeout = KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
        try {
            user = args[3];
            securityFile = args[4];
            regOpenTimeout = Integer.parseInt(args[5]);
            regReadTimeout = Integer.parseInt(args[6]);
        } catch (IndexOutOfBoundsException ioobe) /* CHECKSTYLE:OFF */ {
            /*
             * The IOOBE indicates that either the user or the security
             * parameter is not specified. We just ignore it in this case.
             */
        } /* CHECKSTYLE:ON */

        final KVStoreLogin storeLogin = new KVStoreLogin(user, securityFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF(regOpenTimeout, regReadTimeout);

        final String regHostPort = args[0];

        RepNodeLoginManager loginMgr = null;

        /* Needs authentication */
        if (storeLogin.foundTransportSettings()) {
            try {
                final LoginCredentials creds =
                    storeLogin.makeShellLoginCredentials();
                loginMgr = KVStoreLogin.getRepNodeLoginMgr(
                    new String[] { regHostPort }, creds,
                    null /* expectedStoreName */);
            } catch (AuthenticationFailureException afe) {
                System.err.println("Login failed: " + afe.getMessage());
                return;
            } catch (IOException ioe) {
                System.err.println("Failed to get login credentials: " +
                                   ioe.getMessage());
                return;
            }
        }

        final Topology topology =
            TopologyLocator.get(new String[] { regHostPort }, 0, loginMgr,
                                null /* expectedStoreName */,
                                null /* clientId */);
        final long quiescePeriodMs =
            TimeUnit.SECONDS.toMillis(Integer.parseInt(args[1]));
        final long timeoutMs =
            TimeUnit.SECONDS.toMillis(Integer.parseInt(args[2]));

        final String headerFmt =
            "Waiting for the KVS:%s to be quiescent for %,d sec. " +
            "Timeout: %,d sec\n";
        System.err.printf(headerFmt,
                          topology.getKVStoreName(),
                          TimeUnit.MILLISECONDS.toSeconds(quiescePeriodMs),
                          TimeUnit.MILLISECONDS.toSeconds(timeoutMs));

        final long quiesceMs = new AwaitQuiesce(topology, mainLogger)
            .await(quiescePeriodMs, timeoutMs);

        System.err.printf("%s took %,d sec to become quiescent. " +
                          "It has been quiescent for the preceding %,d sec " +
                          "on %s\n",
                          topology.getKVStoreName(),
                          TimeUnit.MILLISECONDS.toSeconds(quiesceMs),
                          TimeUnit.MILLISECONDS.toSeconds(quiescePeriodMs),
                          currUTC());
    }
}
