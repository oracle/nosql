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

package oracle.nosql.proxy;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Properties;

import oracle.nosql.proxy.audit.ProxyAuditManager;
import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.SCTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.util.ServiceDirectory;

/**
 * Start the Proxy's HTTP Server.
 */
public final class ProxyMain {

    /**
     * TBW usage
     */

    public static void main(String[] argv) {
        String errMsg = "Failed to start proxy: ";
        Config config = null;

        try {
            /* Read in and validate arguments */
            config = new Config(argv);
        } catch (Exception e) {
            System.err.println(errMsg + e.getMessage());
            System.exit(1);
        }

        try {
            startProxy(config, null, null, null);
        } catch (Exception e) {
            /*
             * Add more detail on why the proxy failed. The
             * stacktrace is often very helpful if the failure
             * is complex
             */
            System.err.println(errMsg + getStackTrace(e));
            System.exit(1);
        }
    }

    private static String getStackTrace(Throwable t) {
        if (t == null) {
            return "";
        }

        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        stackTrace = stackTrace.replaceAll("&lt", "<");
        stackTrace = stackTrace.replaceAll("&gt", ">");

        return stackTrace;
    }

    /**
     * This method is for tests, so they can use the same paths that
     * are used to start the proxy, but are easier to use programmatically.
     * To use, create the equivalent of the command line via
     * the properties object.
     */
    public static Proxy startProxy(Properties commandLine) {
        return startProxy(commandLine, null, null, null);
    }

    /**
     * This method is for tests that want to create mock TMs and ACs
     */
    public static Proxy startProxy(Properties commandLine,
                                   TenantManager tm,
                                   AccessChecker ac,
                                   ProxyAuditManager audit) {
        Config config = new Config(commandLine);
        return startProxy(config, tm, ac, audit);
    }

    /**
     * Based on the proxy type, check for required params, set up a tenant
     * manager and access manager, and create a proxy.
     */
    private static Proxy startProxy(Config config,
                                    TenantManager providedTM,
                                    AccessChecker providedAC,
                                    ProxyAuditManager providedAudit) {
        /* Set up a tenant manager and access checker for the proxy */
        TenantManager tm = null;
        AccessChecker ac = null;
        ProxyAuditManager audit = null;
        switch (config.getProxyType()) {
            case KVPROXY:
                /* Check for kv proxy required arguments */
                if (config.getStoreName() == null) {
                    throw new IllegalArgumentException
                        (Config.STORE_NAME + " must be specified");
                }

                String[] helperHosts = config.getHelperHosts();
                if ((helperHosts == null) || (helperHosts.length == 0)) {
                    throw new IllegalArgumentException
                        (Config.HELPER_HOSTS + " must be specified");
                }

                if (config.getSaltWallet() != null) {
                    throw new IllegalArgumentException
                        (Config.SALT_WALLET +
                        " isn't supported for kvproxy");
                }

                /* Only a TM is needed, no access checking is done */
                tm = KVTenantManager.createTenantManager(config);

                break;

            case CLOUD:
                /* Check cloud proxy requirements */
                if (config.getStoreSecurityFile() == null) {
                    throw new IllegalArgumentException
                        ("Cloud proxy must set " + Config.STORE_SECURITY_FILE);
                }

                File saltWallet = config.getSaltWallet();
                if (saltWallet == null) {
                    throw new IllegalArgumentException
                        ("Cloud proxy must set " + Config.SALT_WALLET);
                }
                if (!saltWallet.exists()) {
                    throw new IllegalArgumentException
                        (saltWallet + " does not exist");
                }

                /*
                 * Make a service directory for the cloud by reflection. The
                 * intent is to keep knowledge about how the service
                 * directory is initialized in a real cloud system out of the
                 * so that the httpproxy code base.
                 */
                Class<?> sdClass;
                try {
                    sdClass =
                    Class.forName("oracle.nosql.util.DeployedServiceDirectory");
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalArgumentException(
                        "Unable to find class", cnfe);
                }

                /**
                 * Instantiate the service directory with arguments for
                 * logging.
                 */
                ServiceDirectory serviceDirectory = null;
                try {
                    Method method = sdClass.getDeclaredMethod
                            ("create", String.class, String.class, String.class);
                    serviceDirectory = (ServiceDirectory) method.invoke(null,
                            "ServiceDirectory", "proxy", "proxy_worker.log" );
                } catch (Throwable t) {
                    throw new IllegalArgumentException(
                        "Unable to create service directory", t);
                }

                /* Set up TM and AC */
                tm = new SCTenantManager
                    ("V0",
                     config.getScRequestConnectTimeoutSec() * 1000,
                     config.getScRequestReadTimeoutSec() * 1000,
                     config.isChildTableEnabled(),
                     config.getScLatencyInfoThresholdMs(),
                     serviceDirectory);

                /*
                 * Use IAM for proxy authorization.  If an iamConfigFile is
                 * provided, customize the iam properties.
                 *
                 * Note that mock IAM is implemented by by using certain iam
                 * properties
                 *
                 * Warning:
                 * Cloud access checker must be initialized as early
                 * as possible, it does all the required configuration
                 * to run Proxy in BCFIPS approved only mode.
                 */
                ac = AccessCheckerFactory.createIAMAccessChecker(
                        tm,
                        config.getIAMConfigFile().getAbsolutePath());
                audit = ProxyAuditManager.createProxyOCIAuditManager();
                break;

            case CLOUDSIM:
                tm = LocalTenantManager.createTenantManager(config);
                ac = AccessCheckerFactory.createInsecureAccessChecker();
                break;
            case CLOUDTEST:
                tm = providedTM;
                ac = providedAC;
                audit = providedAudit;
                break;
            default:
                throw new IllegalStateException("Don't know how to configure " +
                                                " proxy type " +
                                                config.getProxyType());
            }

        /* Create the proxy*/
        return Proxy.initialize(config, tm, ac, audit);
    }

    // TODO: provide a way to stop the service when it is started by main().
}
