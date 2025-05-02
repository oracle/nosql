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

package oracle.kv.impl.mgmt;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * This factory produces an implementation of MgmtAgent based on the
 * configuration in the BootstrapParameters.
 */
public class MgmtAgentFactory {

    public static MgmtAgent getAgent(StorageNodeAgent sna,
                                     StorageNodeParams snp,
                                     ServiceStatusTracker tracker) {

        final Logger logger = sna.getLogger();
        assert(logger != null);
        final BootstrapParams bp = sna.getBootstrapParams();
        assert(bp != null);

        /*
         * If StorageNodeParams are provided, use them.  Otherwise, use
         * BootstratParams.
         */
        String mgmtClass;
        int pollingPort;
        String trapHost;
        int trapPort;

        if (snp != null) {
            mgmtClass = snp.getMgmtClass();
            pollingPort = snp.getMgmtPollingPort();
            trapHost = snp.getMgmtTrapHost();
            trapPort = snp.getMgmtTrapPort();
        } else {
            mgmtClass = bp.getMgmtClass();
            pollingPort = bp.getMgmtPollingPort();
            trapHost = bp.getMgmtTrapHost();
            trapPort = bp.getMgmtTrapPort();
        }
        assert(mgmtClass != null);

        if (MgmtUtil.verifyImplClassName(mgmtClass) != true) {
            throw new IllegalStateException
                ("The class name " + mgmtClass +
                 ", present in the config file, is not allowed.");
        }

        /*
         * Check to see if the current configuration matches the requested
         * configuration.  If it does, then we'll do nothing.
         */
        MgmtAgent currentImpl = sna.getMgmtAgent();
        if (currentImpl != null) {
            if (currentImpl.getClass().getName().equals(mgmtClass) &&
                currentImpl.checkParametersEqual(pollingPort, trapHost, trapPort)) {
                /*
                 * The requested configuration is already in force.  But there
                 * might be a new ServiceStatusTracker, so set that.
                 */
                currentImpl.setSnaStatusTracker(tracker);
                currentImpl.setSnaLoggingStatsTracker(
                    sna.getSnaLoggingStatsTracker());
                return currentImpl;
            }
            currentImpl.shutdown();
        }

        try {
            final Class<?> c = Class.forName(mgmtClass);
            final Constructor<?> ctor =
                c.getConstructor(StorageNodeAgent.class,
                                 Integer.TYPE,
                                 String.class, Integer.TYPE,
                                 ServiceStatusTracker.class);
            logger.info("Using mgmt class " + mgmtClass);
            return (MgmtAgent) ctor.newInstance(sna, pollingPort, trapHost,
                                                trapPort, tracker);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Can't instantiate MgmtAgent class " + mgmtClass, e);
        }
        /* Don't prevent the SNA from starting. */
        logger.info("Falling back to mgmt agent NoOpAgent.");
        return new NoOpAgent(sna, pollingPort, trapHost, trapPort, tracker);
    }
}
