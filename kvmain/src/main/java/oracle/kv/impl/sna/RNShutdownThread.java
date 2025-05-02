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

package oracle.kv.impl.sna;

import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;

/**
 * Thread used to shutdown all RNs (or arbiters), as part of shutting down the
 * SNA, in parallel while ensuring they finish their checkpoints. It attempts a
 * clean shutdown by invoking the RN's shutdown method and if that fails, or
 * the process does not exit in the configured time period, it kills the RN
 * process.
 */
class RNShutdownThread implements Runnable {

    private final StorageNodeAgent sna;
    private final Logger logger;
    private final int serviceWaitMs;
    private final ServiceManager mgr;
    private final boolean stopService;
    private final boolean force;
    private final String reason;

    RNShutdownThread(StorageNodeAgent sna,
                     ServiceManager mgr,
                     int serviceWaitMs,
                     boolean stopService,
                     boolean force,
                     String reason) {
        super();
        this.sna = sna;
        this.serviceWaitMs = serviceWaitMs;
        logger = sna.getLogger();

        this.mgr = mgr;
        this.stopService = stopService;
        this.force = force;
        this.reason = reason;
    }

    public ServiceManager getMgr() {
        return mgr;
    }

    @Override
    public void run() {

        try {
            /**
             * Make sure the service won't automatically restart.
             */
            mgr.dontRestart();

            if (mgr.forceOK(force)) {
                mgr.stop();
                return;
            }

            /**
             * Don't try to shut down if it's known to be down already.
             */
            if (!stopService) {
                return;
            }

            if (mgr.isRunning()) {
                if (mgr.getService() instanceof ManagedRepNode) {
                    stopRN();
                } else {
                    stopAN();
                }
            }

            /*
             * TODO: replace with a polling or heartbeat mechanism someday for
             * a more explicit sign of forward progress towards a shutdown.
             */
            mgr.waitFor(serviceWaitMs);

        } catch (Exception e) {

            /**
             * Eat the exception but log it and make sure that the service
             * is really stopped.
             */
            logger.log(Level.WARNING, mgr.getService().getServiceName() +
                        ": Exception stopping Node", e);
            mgr.stop();
        } finally {
            sna.unbindService(sna.makeRepNodeBindingName
                              (mgr.getService().getServiceName()));
        }
    }

    private void stopRN() throws RemoteException {
        final ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
        /**
         * Get the RN's RMI handle
         *
         * NOTE: this timeout is helpful but not critical
         * so it need not be tuneable.
         */
        final RepNodeAdminAPI rna = mrn.waitForRepNodeAdmin(sna, 5);

        /**
         * Try clean shutdown first. If that fails for any reason kill
         * the process to be sure the service is gone. Give the RN some
         * time in case it's still running. Stopping it at a random
         * time can cause problems.
         */
        if (rna != null) {
            try {
                rna.shutdown(force, false /* checkStream */, reason);
            } catch (RemoteException e) {
                final Throwable ce = e.getCause();

                /*
                 * Make special provisions for a request timeout, the
                 * RN could just be in the midst of a long checkpoint:
                 * Don't kill the process right away, but wait for the
                 * configured period.
                 */
                if (! (ce instanceof SocketTimeoutException)) {
                    /* Kill the process in the handler. */
                    throw e;
                }

                logger.warning(String.format(
                        "Socket timed out waiting for %s." +
                                " Message:%s. Wait %,d ms for process exit.",
                                mgr.getService().getServiceName(),
                                ce.getMessage(), serviceWaitMs));
            }
        }
    }

    private void stopAN() throws RemoteException {
        final ManagedArbNode man = (ManagedArbNode) mgr.getService();
        final ArbNodeAdminAPI ana = man.waitForArbNodeAdmin(sna, 5);

        if (ana != null) {
            ana.shutdown(force);
        }
    }
}
