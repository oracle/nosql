/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.cloud.ProxyHealthSource;
import oracle.nosql.proxy.sc.SCTenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.util.HostPort;
import oracle.nosql.util.ServiceDirectory;
import oracle.nosql.util.ph.HealthStatus;

public class ProxyHealthSourceTest extends ProxyTestBase {

    private static final SkLogger logger = new SkLogger(
        ProxyHealthSourceTest.class.getName(),
        "proxy", "proxytest.log");

    @Test
    public void scConnectivityTest() throws Exception {
        /*
         * 1. Set up a test Proxy with Wrong SC IP address.
         */
        SCTenantManager testTM = 
        	new SCTenantManager("V0",
                                 0, 0, true /* isChildTableEnabled */, 0, 
                                 new TestServiceDirectory());
        AccessChecker checker =
            AccessCheckerFactory.createInsecureAccessChecker();
        HostPort hp = new HostPort("errorhost", 8888);
        testTM.establishURLBase(hp.toUrl(false), true /* reset */);
        Properties commandLine = new Properties();
        commandLine.setProperty(Config.PROXY_TYPE.paramName,
                Config.ProxyType.CLOUDTEST.name());

        commandLine.setProperty(Config.HTTP_PORT.paramName,
                Integer.toString(9095));

        commandLine.setProperty(Config.HTTPS_PORT.paramName,
                Integer.toString(9096));
        commandLine.setProperty(Config.NUM_REQUEST_THREADS.paramName,
                Integer.toString(1));
        /* Disable pulling rules thread in FilterHandler */
        commandLine.setProperty(Config.PULL_RULES_INTERVAL_SEC.paramName,
                Integer.toString(0));

        Proxy testProxy = ProxyMain.startProxy(commandLine, testTM,
                                               checker, audit);

        /*
         * 2. Check Proxy HealthStatus is RED as SC can't be connected.
         */
        ProxyHealthSource healthSource = testProxy.getHealthSource();
        List<String> errors = new ArrayList<>();
        HealthStatus status  = healthSource.getStatus("Proxy",
                                                      "Proxy0",
                                                      "localhost",
                                                      logger,
                                                      errors);
        assertEquals(HealthStatus.YELLOW, status);
        assertEquals(1, errors.size());
        /*
         * Minicloud test only
         */
        if (useMiniCloud) {
            /*
             * 3. Set TM to the real SC IP address.
             */
            hp = new HostPort(scHost, scPort);
            testTM.establishURLBase(hp.toUrl(false), true /* reset */);
            /*
             * Wait more than 1 minute for last failed SC request expired.
             */
            try {
                Thread.sleep(61_000);
            } catch (InterruptedException e) {
            }
            /*
             * 4. Check Proxy HealthStatus is GREEN now.
             */
            healthSource = testProxy.getHealthSource();
            errors = new ArrayList<>();
            status  = healthSource.getStatus("Proxy", "Proxy0", "localhost",
                                             logger, errors);
            assertEquals(errors.toString(), HealthStatus.GREEN, status);
            assertEquals(errors.toString(), 0, errors.size());
        }
    }
    
    class TestServiceDirectory implements ServiceDirectory {

        /**
         * Returns a positive value as the service (region) identifier
         * A positive value is required to indicate that this proxy
         * is in a "cloud" environment - whether it's cloudsim or a unittest.
         */
        @Override
        public int getLocalServiceInteger() {
            return 1;
        }

        @Override
        public String getLocalServiceName() {
            return "localPP";
        }

        @Override
        public String translateToRegionName(String serviceName) {
            return serviceName + "-region";
        }

        @Override
        public String validateRemoteReplica(String targetRegionName) {
            return targetRegionName + "-servicename";
        }
    }
}
