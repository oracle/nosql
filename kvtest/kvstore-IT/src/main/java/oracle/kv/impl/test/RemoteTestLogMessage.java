/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A main program to use RemoteTestInterface.logMessage to log a message in a
 * service.
 */
public class RemoteTestLogMessage {
    private static final Logger logger =
        ClientLoggerUtils.getLogger(RemoteTestLogMessage.class, "main");

    /** Usage. */
    public static final String USAGE =
        "Usage: java " + RemoteTestLogMessage.class.getName() + " [args]\n" +
        "  -host <hostname> [default: localhost]\n" +
        "  -port <port> [default: 5000]\n" +
        "  -store <storename> [default: kvstore]\n" +
        "  -security <login-file> [default: none]\n" +
        "  -serviceId <service-id> [default: admin1]\n" +
        "  -level <level> [default: WARNING]\n" +
        "  -useJeLogger <boolean> [default: false]\n" +
        "  -message <message> [default: 'Log message']\n";

    /**
     * Logs a message in a service.
     *
     * @param args arguments as specified by {@link #USAGE}
     */
    public static void main(String[] args)
        throws Exception
    {
        String storeName = "kvstore";
        String host = "localhost";
        int port = 5000;
        String security = null;
        String serviceId = "admin1";
        Level level = Level.WARNING;
        boolean useJeLogger = false;
        String message = "Log message";

        for (int offset = 0; offset < args.length; offset++) {
            final String arg = args[offset];
            if ("-host".equals(arg)) {
                host = args[++offset];
            } else if ("-port".equals(arg)) {
                port = Integer.parseInt(args[++offset]);
            } else if ("-store".equals(arg)) {
                storeName = args[++offset];
            } else if ("-security".equals(arg)) {
                security = args[++offset];
            } else if ("-serviceId".equals(arg)) {
                serviceId = args[++offset];
            } else if ("-level".equals(arg)) {
                level = LoggerUtils.parseLevel(args[++offset]);
            } else if ("-useJeLogger".equals(arg)) {
                useJeLogger = Boolean.parseBoolean(args[++offset]);
            } else if ("-message".equals(arg)) {
                message = args[++offset];
            } else if ("-help".equals(arg)) {
                System.err.println(USAGE);
                System.exit(1);
            } else if (arg.startsWith("-")) {
                System.err.println("Unknown flag: " + arg);
                System.err.println(USAGE);
                System.exit(1);
            } else {
                System.err.println("Unexpected argument: " + arg);
                System.err.println(USAGE);
                System.exit(1);
            }
        }

        final RegistryUtils registryUtils = getRegistryUtils(storeName, host,
                                                             port, security);
        final RemoteTestAPI testAPI = getRemoteTestAPI(serviceId,
                                                       registryUtils);
        testAPI.logMessage(level, message, useJeLogger);
        System.out.format("Logged message '%s' at level %s to%s service %s\n",
                          message,
                          level,
                          useJeLogger ? " the JE logger of" : "",
                          serviceId);
    }

    private static RegistryUtils getRegistryUtils(String storeName,
                                                  String host,
                                                  int port,
                                                  String security)
        throws Exception
    {
        final KVStoreLogin storeLogin = new KVStoreLogin(null /* user */,
                                                         security);
        storeLogin.loadSecurityProperties();
        final String[] helpers = { host + ":" + port };

        RepNodeLoginManager loginMgr = null;

        /* Needs authentication */
        if (storeLogin.foundTransportSettings()) {
            final LoginCredentials creds =
                storeLogin.makeShellLoginCredentials();
            loginMgr = KVStoreLogin.getRepNodeLoginMgr(helpers, creds,
                                                       storeName);
        }

        final Topology topology = TopologyLocator.get(helpers, 0 /* maxRNs */,
                                                      loginMgr, storeName,
                                                      null /* clientId */);
        return new RegistryUtils(topology, loginMgr, logger);
    }

    private static
        RemoteTestAPI getRemoteTestAPI(String serviceId,
                                       RegistryUtils registryUtils)
        throws RemoteException, NotBoundException {

        try {
            return registryUtils.getRepNodeTest(RepNodeId.parse(serviceId));
        } catch (IllegalArgumentException ignored) {
        }

        try {
            return registryUtils.getStorageNodeAgentTest(
                StorageNodeId.parse(serviceId));
        } catch (IllegalArgumentException ignored) {
        }

        try {
            return registryUtils.getArbNodeTest(ArbNodeId.parse(serviceId));
        } catch (IllegalArgumentException ignored) {
        }

        final AdminId adminId;
        try {
            adminId = AdminId.parse(serviceId);
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("Invalid serviceId: " +
                                               serviceId);
        }

        /* Find an admin */
        final Topology topo = registryUtils.getTopology();
        CommandServiceAPI admin = null;
        for (final StorageNodeId snId : topo.getStorageNodeIds()) {
            try {
                admin = registryUtils.getAdmin(snId);
                break;
            } catch (RemoteException|NotBoundException e) {
            }
        }
        if (admin == null) {
            throw new IllegalStateException("Couldn't find any admins");
        }

        /* Find the requested admin */
        for (final ParameterMap map : admin.getAdmins()) {
            final AdminParams params = new AdminParams(map);
            final AdminId id = params.getAdminId();
            if (id.equals(adminId)) {
                return registryUtils.getAdminTest(params.getStorageNodeId());
            }
        }
        throw new IllegalStateException("Service not found: " + adminId);
    }
}
