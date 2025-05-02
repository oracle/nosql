/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static oracle.kv.impl.admin.param.GlobalParams.COMMAND_SERVICE_NAME;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.StoreUtils.RecordType;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.TableAPI;

/**
 * Verifier is a class that verifies the expected state of a store or service
 * based on one or more of topology, ping, or parameter state.  Topology and
 * parameter state requires a running admin on the specified host.  Ping only
 * requires access to the topology.  Topology requires the admin in order to
 * retrieve the status map containing the current known status of various
 * services.  Ping can only provide up/down and limited status information.
 * Ping is useful for testing situations where the admin may not be available
 * or on a known host.
 *
 * <p>An instance of this class is used to verify based on strings of the format:
 * <br><code>verify_type args</code>
 *
 * <p>verify_type is [topology | ping | parameter]
 * <br>args will typically be a service name, and is a valid string
 * representing a service name, e.g. "rg1-rn1" "sn1", "admin1", etc.  A service
 * name of policy means looking at policy parameters.
 * <br> topology and ping have an expected_state argument which is a legitimate
 * ServiceStatus value for the service with the addition of the keyword "ABSENT"
 * which indicates that the named service does not exist.
 * In addition, the leyword "ANY" can be used to match any existing state.
 * <br> parameter is more complex, see examples.
 * <br>Here are some examples:
 * <ul>
 * <li>topology sn1 ABSENT (not present at all)</li>
 * <li>topology sn1 RUNNING</li>
 * <li>topology rg1-rn2 STOPPED</li>
 * <li>ping admin1 RUNNING</li>
 * <li>ping sn1 UNREACHABLE</li>
 * <li>parameter sn1 x equal y</li>
 * <li>parameter admin1 x notequal y</li>
 * <li>parameter rg1-sn1 z empty</li>
 * <li>parameter policy z empty</li>
 * <li>schema enabled foo</li>
 * <li>schema disabled foo</li>
 * <li>schema absent foo</li>
 * <li>plan planId ABSENT</li>
 * <li>plan planId PENDING</li>
 * <li>pool poolName exists</li>
 * <li>pool poolName absent</li>
 * <li>pool poolName contains sn1</li>
 * <li>pool poolName contains sn1 sn2 sn3</li>
 * <li>pool poolName notcontains sn4</li>
 * <li>pool poolName notcontains sn1 sn2 sn3</li>
 * <li>dir exists dirName</li>
 * <li>dir absent dirName</li>
 * <li>dir empty dirName</li>
 * <li>dir notempty dirName</li>
 * <li>key exists keyName</li>
 * <li>key absent keyName</li>
 * <li>store equal store:host:port:INT</li>
 *  <li>store notequal store:host:port:UUID</li>
 * </ul>
 *
 * TODO:
 *  syntax for parameter validation based on range, inequality, others?
 */
public class Verifier {
    private static final Logger logger =
        ClientLoggerUtils.getLogger(Verifier.class, "test");
    private boolean verbose = false;
    private String hostname;
    private int registryPort;
    private PrintStream output;
    private boolean isSecured = false;
    private LoginCredentials loginCreds = null;
    private Properties securityProp = null;

    /**
     * The types of verification supported.  TOPOLOGY and PARAMETER require a
     * running admin service on the specified host. PING only requres a way to
     * get the topology from the host.
     */
    public enum VerifierType {
        TOPOLOGY, PING, PARAMETER, POOL, DIR, KEY, STORE, PLAN, TABLE;
    }

    /**
     * Used by the main() command line.
     */
    public Verifier() {
    }

    /**
     * Construct an instance that that will verify based on the specified host
     * and registry port.
     */
    public Verifier(String hostname, int registryPort) {
        init(hostname, registryPort, null);
    }

    /**
     * Construct an instance that that will verify based on the specified host,
     * registry port and securityFile.
     */
    public Verifier(String hostname, int registryPort, String securityFile) {
        init(hostname, registryPort, securityFile);
    }

    private void init(String hostname1,
                      int registryPort1,
                      String securityFile) {
        this.hostname = hostname1;
        this.registryPort = registryPort1;

        final KVStoreLogin storeLogin =
            new KVStoreLogin(null /* username */, securityFile);
        storeLogin.loadSecurityProperties();
        /* Needs authentication */
        isSecured = storeLogin.foundTransportSettings();
        storeLogin.prepareRegistryCSF();
        if (isSecured) {
            try {
                loginCreds = storeLogin.makeShellLoginCredentials();
                securityProp = storeLogin.getSecurityProperties();
            } catch (IOException ioe) {
                output.println("Failed to get login credentials: " +
                                   ioe.getMessage());
                return;
            }
        }
    }

    private LoginManager getLoginManager(ResourceType rtype) {
        LoginManager loginMgr = null;
        if (isSecured) {
            switch (rtype) {
            case ADMIN:
                loginMgr = KVStoreLogin.getAdminLoginMgr(
                    hostname, registryPort, loginCreds, logger);
                break;
            case REP_NODE:
                loginMgr = KVStoreLogin.getRepNodeLoginMgr(hostname,
                    registryPort, loginCreds, null /* storeName */);
                break;
            default:
                throw new IllegalArgumentException(rtype + " not supported");
            }
        }
        return loginMgr;
    }

    /**
     * Set verbose output
     */
    public void setVerbose(boolean value) {
        verbose = value;
    }

    /**
     * Set PrintStream
     */
    public void setOutputStream(PrintStream output) {
        this.output = output;
    }

    public boolean run(String verifyString)
        throws RemoteException {

        List<String> args = parseString(verifyString);

        StateVerifier verifier = findVerifier(args.get(0), verifyString);
        return verifier.verify(args);
    }

    /**
     * Parse an input string of the form:
     *  type service value
     * e.g.
     * topology sn1 RUNNING
     * topology sn2 ABSENT
     * parameter sn1 hostname equals blah
     * parameter sn1 hostname notequals baz
     * parameter sn1 jvmMisc empty
     * ping sn1 WAITING_FOR_DEPLOY
     */
    public static List<String> parseString(String verifyString) {
        String regex = "[^\\s\"']+|\"([^\"]*)\"|'([^']*)'";
        List<String> matchList = new ArrayList<String>();
        Pattern pattern = Pattern.compile(regex);
        Matcher regexMatcher = pattern.matcher(verifyString);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group());
            }
        }
        return matchList;
    }

    /**
     * A factory method to return the correct StateVerifier instance to handle
     * the line.  This could be more/better abstracted with factory classes but
     * this suffices for now.
     */
    private StateVerifier findVerifier(String type, String line) {
        VerifierType vtype = null;
        try {
            vtype = VerifierType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException
                ("Cannot find verifier for line: " + line);
        }
        switch (vtype) {
        case TOPOLOGY:
            return new TopologyVerifier();
        case PARAMETER:
            return new ParameterVerifier();
        case PING:
            return new PingVerifier();
        case POOL:
            return new PoolVerifier();
        case DIR:
            return new DirVerifier();
        case KEY:
            return new KeyVerifier();
        case STORE:
            return new StoreVerifier();
        case PLAN:
            return new PlanVerifier();
        case TABLE:
            return new TableVerifier();
        default:
            break;
        }
        throw new IllegalArgumentException
            ("Cannot find verifier for line: " + line);
    }

    /**
     * Give a storage node in the kvstore, find a copy of the topology from
     * one of its resident repNodes or an admin.
     */
    private Topology getTopology()
        throws RemoteException, AccessException {

        try {
            Topology bestRnTopo = null;
            for (String svcName :
                     RegistryUtils.getServiceNames(
                         null /* storeName */, hostname, registryPort,
                         Protocols.getDefault(), null /* clientId */,
                         logger)) {

                /**
                 * If SNA's name is still the global name it is not registered
                 */
                if (GlobalParams.SNA_SERVICE_NAME.equals(svcName)) {
                    throw new IllegalStateException
                        ("SNA at host:port " + hostname + ":" + registryPort +
                         " is not registered, no topology available");
                }

                /*
                 * Skip to avoid the improper access to SNA trusted login
                 * interface from clients.
                 */
                if (RegistryUtils.isStorageNodeAgentLogin(svcName)) {
                    continue;
                }

                try {
                    if (COMMAND_SERVICE_NAME.equals(svcName)) {
                        final CommandServiceAPI admin =
                            RegistryUtils.getAdmin(
                                hostname, registryPort,
                                getLoginManager(ResourceType.ADMIN), logger);
                        /* admin has the up-to-date topology -- return it */
                        return admin.getTopology();
                    }
                    if (RegistryUtils.isRepNodeAdmin(svcName)) {
                        final RepNodeAdminAPI admin =
                            RegistryUtils.getRepNodeAdmin(
                                null /* storeName */, hostname, registryPort,
                                svcName,
                                getLoginManager(ResourceType.REP_NODE),
                                logger);
                        final Topology topo = admin.getTopology();
                        if (topo == null) {
                            continue;
                        }
                        /* Save RN topo with highest sequence number */
                        if ((bestRnTopo != null) &&
                            (topo.getSequenceNumber() <=
                             bestRnTopo.getSequenceNumber())) {
                            continue;
                        }
                        bestRnTopo = topo;
                    }
                } catch (Exception e) {
                    /*
                     * ignore failure for now, continue to look
                     */
                }
            }
            if (bestRnTopo != null) {
                return bestRnTopo;
            }
            throw new IllegalStateException
                ("SNA at host:port " + hostname + ":" + registryPort +
                 " has no admins or RepNodes, no topology available");
        } catch (ConnectException ce) {
            throw new IllegalStateException
                ("SNA at host:port " + hostname + ":" + registryPort +
                 ": cannot connect: " + ce);
        }
    }

    /**
     * Verify that the service name instance is valid.  It needs to be one of:
     * RepNode
     * Storage Node
     * Datacenter
     */
    static protected ResourceId verifyServiceName(String serviceName) {
        try {
            ResourceId rid = AdminId.parse(serviceName);
            return rid;
        } catch (IllegalArgumentException e) {
            /* Fall through */
        }
        try {
            ResourceId rid = RepNodeId.parse(serviceName);
            return rid;
        } catch (IllegalArgumentException e) {
            /* Fall through */
        }
        try {
            ResourceId rid = StorageNodeId.parse(serviceName);
            return rid;
        } catch (IllegalArgumentException e) {
            /* Fall through */
        }
        try {
            ResourceId rid = ArbNodeId.parse(serviceName);
            return rid;
        } catch (IllegalArgumentException e) {
            /* Fall through */
        }
        throw new IllegalArgumentException("Service name " + serviceName +
                                           " is not a recognized service");
    }

    /**
     * Translate the expectedState string into ServiceStatus.
     * The special string, "ABSENT" indicates that it's not in the topology at
     * all.
     * "ANY" can be used to ignore a state and just test if the plan exists.
     */
    static protected ServiceStatus
        verifyServiceStatus(String expectedState) {

        if (expectedState.equals("ABSENT")) {
            return null;
        }
        if (expectedState.equals("ANY")) {
            return null;
        }
        try {
            return ServiceStatus.valueOf(expectedState);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Malformed service state: "
                                               + expectedState);
        }
    }

    /**
     * Translate the expectedState string into Plan state.
     * The special string, "ABSENT" indicates that this plan does not exist.
     * "ANY" can be used to ignore a state and just test if the plan exists.
     */
    static protected State
        verifyPlanState(String expectedState) {

        if (expectedState.equals("ABSENT")) {
            return null;
        }
        if (expectedState.equals("ANY")) {
            return null;
        }

        try {
            return State.valueOf(expectedState);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Malformed plan state: "
                                                + expectedState);
        }

    }

    private CommandServiceAPI getAdmin() {
        try {
            LoginManager loginMgr = getLoginManager(ResourceType.ADMIN);
            CommandServiceAPI cs =
                RegistryUtils.getAdmin(hostname, registryPort,
                                       loginMgr, logger);

            /* Redirect to Admin master */
            if (cs.getAdminStatus().getReplicationState() ==
                com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA) {
                URI rmiaddr = cs.getMasterRmiAddress();
                hostname = rmiaddr.getHost();
                registryPort = rmiaddr.getPort();
                cs = RegistryUtils.getAdmin(hostname, registryPort,
                                            loginMgr, logger);
            }
            return cs;
        } catch (Exception e) {
            throw new IllegalStateException
                ("Cannot get admin at host:port " + hostname + ":" +
                 registryPort);
        }
    }

    private KVStore getStore() {
        try {
            Topology topology = getTopology();
            KVStoreConfig kvconfig = new KVStoreConfig(
                topology.getKVStoreName(), hostname + ":" + registryPort);
            kvconfig.setSecurityProperties(securityProp);
            KVStore store = KVStoreFactory.getStore(kvconfig, null, null);
            return store;
        } catch (Exception ae) {
            throw new IllegalStateException(
                "Cannot get store at host:port " + hostname + ":" +
                registryPort);
        }
    }

    /**
     * This method requires a running admin on the specified host.
     */
    private ServiceStatus getStatus(ResourceId rid) {

        try {
            CommandServiceAPI cs = getAdmin();
            Map<ResourceId, ServiceChange> statusMap = cs.getStatusMap();
            ServiceChange snev = statusMap.get(rid);
            return snev.getStatus();
        } catch (Exception e) {
            /* ignored */
        }
        return null;
    }

    private void verbose(String msg) {
        if (verbose) {
            if (output == null) {
                output = System.out;
            }
            output.println(msg);
        }
    }

    /**
      * This function is to get the expected values of a verify string, as
      * well as the operation between them. For example, if the verify
      * string is "parameter rg1-rn1 rnMountPoint equal dir1 | dir2 | ...",
      * the input argument should be a list contains "dir1 | dir2 | ..."
      * the return should be a list as "dir1 dir2 ... |".
      * The method will return the expected value list only if input
      * argument is legal, e.g.
      *      `             dir1 & dir2 & ...
      *                    dir1 | dir2 | ...
      * However if the input argument is illegal as follows, the method
      * will throw exception.
      *                    | dir1 | dir2 | ...
      *                    dir1 & & dir2 ...
      *                    dir1 | dir2 & dir3 ...
      *                    dir1 | dir2 dir3 | ...
      *                    dir1 & ... & dirn &
      */
    private List<String> getExpectedValues
        (List<String> args, final String USAGE) {
        List<String> expectedVals = new ArrayList<String>();
        boolean isVal = false;
        String lastLogic = "";

        for (int i = 0 ; i < args.size() ; i++) {
            String expectedVal = args.get(i);
            if (expectedVal.equals("|") || expectedVal.equals("&")) {

                /**
                   * Check the unsupported format, the example of
                   * these unacceptable format as follows
                   *        | dir1 | dir2 | ...
                   *        dir1 & & dir2 ...
                   *        dir1 | dir2 & dir3 ...
                   */
                if (i == 0 || !isVal ||
                     !lastLogic.equals("") &&
                     !expectedVal.equals(lastLogic)) {
                    throw new IllegalArgumentException
                        ("Incorrect input of expected value, usage: \n" +
                        USAGE);
                 }
                 isVal = false;
                 lastLogic = expectedVal;
                 continue;
            }

            /**
              * Check the unsupported format, the example of these
              * unacceptable format as follows
              *         dir exists dir1 | dir2 dir3 | ...
              */
            if (isVal) {
                throw new IllegalArgumentException
                ("Incorrect input of expected value, usage: \n" +
                 USAGE);
            }
            isVal = true;
            expectedVals.add(expectedVal);
        }

        /**
          * Check the unexpected input, example as follows
          *     dir notempty dir1 & ... & dirn &
          */
        if (!isVal) {
             throw new IllegalArgumentException
                ("Incorrect input of expected value, usage: \n" +
                 USAGE);
        }
        expectedVals.add(lastLogic);
        return expectedVals;
    }

    /**
     * Interface to a verifier of a particular type
     */
    public interface StateVerifier {

        public boolean verify(List<String> args) throws RemoteException;
    }

    public class ParameterVerifier implements StateVerifier {

        /**
         * Pattern: parameter service|policy|security
         * param <equal|notequal|empty> value
         * We currently support multiple values with "and" and "or" logic
         * between them.
         * e.g. parameter rg1-rn1 storageNodeId equal 1
         *      parameter rg1-rn1 rnMountPoint equal dir1 | dir2 | ...
         *      parameter rg1-rn1 rnMountPoint equal dir1 & dir2 & ...
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {
            boolean isPolicy = false;
            boolean isSecurity = false;
            final String USAGE = "parameter service|policy|security param " +
                           "<equal|notequal|empty> [value1 | value2 | ...] \n" +
                           "parameter service|policy|security param " +
                           "<equal|notequal|empty> [value1 & value2 & ...]";
            if (args.size() < 4) {
                throw new IllegalArgumentException
                    ("Not enough arguments for parameter verifier, usage: \n" +
                     USAGE);
            }
            String serviceName = args.get(1);
            ResourceId rid = null;
            if (serviceName.equals("policy")) {
                isPolicy = true;
            } else if(serviceName.equals("security")) {
                isSecurity = true;
            }
            else {
                rid = verifyServiceName(serviceName);
            }
            String param = args.get(2);
            String operation = args.get(3);

            /**
              * Add judgement for operation so that this program can only
              * accept three legal operations including "equal", "notequal"
              * and "empty".
              */
            if (!operation.equals("equal") &&
                !operation.equals("empty") &&
                !operation.equals("notequal")) {
                throw new IllegalArgumentException
                    ("Only three operations <equal|notequal|empty> are " +
                     "accepted, usage: \n" + USAGE);
            }

            CommandServiceAPI cs = getAdmin();
            Parameters params = cs.getParameters();
            ParameterMap map = null;
            if (isPolicy) {
                map = params.getPolicies();
            } else if (isSecurity) {
                map = params.getGlobalParams().getGlobalSecurityPolicies();
            } else {
                if (rid instanceof StorageNodeId) {
                    map = params.get((StorageNodeId)rid).getMap();
                } else if (rid instanceof RepNodeId) {
                    map = params.get((RepNodeId)rid).getMap();
                } else if (rid instanceof AdminId) {
                    map = params.get((AdminId)rid).getMap();
                } else if (rid instanceof ArbNodeId) {
                    map = params.get((ArbNodeId)rid).getMap();
                }
            }

            /**
             * No final else -- an unsupported ResourceId would have been
             * found above in verifyServiceName()
             */
            verbose("Checking parameter for " +
                    (isPolicy ? "policy parameters" : "service " +
                     serviceName) + ", parameter " + param + ", operation " +
                    operation + ", value " +
                    (args.size() > 4 ?
                     args.subList(4, args.size()) :
                     "null"));
            if (map != null) {

                /**
                 * Null or empty parameters are specified is specified using
                 * operation "empty"
                 */
                boolean empty = operation.equals("empty");

                Parameter p = map.get(param);
                if (p != null) {
                    if (empty) {

                        /**
                          * If expected values appears after operation
                          * "empty", show usage.
                          */
                        if (args.size() > 4) {
                            throw new IllegalArgumentException
                                ("Expected value should not appear after " +
                                 "operation \"empty\", usage: \n" + USAGE);
                        }

                        /**
                         * empty passes if the parameter exists and has an
                         * empty string as its value.
                         */
                        args.add("");
                        operation = "equal";
                    }

                    /**
                      * If the operation is "equal" or "notequal", and there
                      * is no expected values, set "null" as an expected value.
                      */
                    if (args.size() == 4) {
                        args.add(null);
                    }

                    List<String> expectedVals =
                        getExpectedValues(args.subList(4, args.size()), USAGE);

                    /**
                      * Using a list to save more than one expected parameter
                      * values, so that users can input one or more expected
                      * values with "or" logic between each one.
                      */
                    List<Parameter> expecteds = new ArrayList<Parameter>();
                    for (int i = 0 ; i < expectedVals.size() - 1 ; i++) {
                        expecteds.add
                            (Parameter.createParameter
                                (p.getName(), expectedVals.get(i)));
                    }

                    /**
                      * To deal with the "&" and "|" logic.
                      */
                    String logic = expectedVals.get(expectedVals.size() - 1);
                    boolean val;
                    if (logic.equals("") || logic.equals("&")) {
                        val = true;
                        for (int i = 0 ; i < expecteds.size() ; i++) {
                            val = val && p.equals(expecteds.get(i));
                            if (!val) {
                                break;
                            }
                        }
                    } else {
                        val = false;
                        for (int i = 0 ; i < expecteds.size() ; i++) {
                            val = val || p.equals(expecteds.get(i));
                            if(val) {
                                break;
                            }
                        }
                    }
                    if (!val) {
                        verbose("Verifier expected " +
                            (expecteds.size() > 1 ?
                            expectedVals.subList(0, expectedVals.size() - 1) +
                            " with " + logic + " logic between them" :
                            expectedVals.get(0)) + ", actual: " + p);
                    }
                    return (operation.equals("equal") ? val : !val);
                } else if (empty) {
                    return true;
                }
                verbose("Verifier could not find parameter: " + param +
                        " for " + (isPolicy ? "policy parameters" :
                                   "resource " + rid));
                return false;
            }
            verbose("Verifier could not find parameter(s) for " +
                    (isPolicy ? "policy parameters" : "resource " + rid));
            return false;
        }
    }

    public class TopologyVerifier implements StateVerifier {

        /**
         * Pattern: topology service ServiceStatus
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {

            String serviceName = args.get(1);
            ResourceId rid = verifyServiceName(serviceName);
            String expectedState = args.get(2);
            verbose("Checking service " + serviceName + " against state " +
                    expectedState + " in the Topology");
            ServiceStatus expectedStatus = verifyServiceStatus(expectedState);
            Topology topology = getTopology();
            Topology.Component<?> comp = topology.get(rid);

            if (comp == null && expectedStatus == null) {
                verbose("Matched ABSENT status");
                return true;
            }
            ServiceStatus status = getStatus(rid);
            boolean ret = false;
            if (expectedStatus == ServiceStatus.UNREACHABLE) {
                if (status == ServiceStatus.STOPPED ||
                    status == ServiceStatus.STOPPING ||
                    status == ServiceStatus.UNREACHABLE) {
                    ret = true;
                }
            } else {
                ret = (expectedStatus == status);
            }
            verbose("Returning value " + ret);
            if (!ret) {
                verbose("Current status: " + status);
            }
            return ret;
        }
    }

    public class PingVerifier implements StateVerifier {

        /**
         * Pattern: ping service ServiceStatus
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {

            String serviceName = args.get(1);
            String expectedState = args.get(2);
            verbose("Checking service " + serviceName + " against state " +
                    expectedState + " via Ping");
            ResourceId rid = verifyServiceName(serviceName);
            ServiceStatus expectedStatus = verifyServiceStatus(expectedState);
            ServiceStatus status = ServiceStatus.UNREACHABLE;

            if (rid instanceof AdminId) {
                CommandServiceAPI cs = getAdmin();
                Parameters params = cs.getParameters();
                AdminParams adminParams = params.get((AdminId)rid);
                ParameterMap map = null;

                if (adminParams != null) {
                    map = adminParams.getMap();

                    if (map != null) {
                        try {
                            StorageNodeId hostSN =
                                adminParams.getStorageNodeId();
                            StorageNodeParams snp = params.get(hostSN);

                            /* ping service doesn't need to login,
                             * so set login manager as null here */
                            cs = RegistryUtils.getAdmin(snp.getHostname(),
                                                        snp.getRegistryPort(),
                                                        null/* loginManager */,
                                                        logger);
                            status = cs.ping();
                        } catch (Exception e) {
                            /* ignored */
                        }
                    }
                }
            } else {
                Topology topology = getTopology();
                status = ServiceUtils.ping(rid, topology, logger);
            }
            boolean ret = false;
            if (expectedStatus == ServiceStatus.UNREACHABLE) {
                if (status == ServiceStatus.STOPPED ||
                    status == ServiceStatus.STOPPING ||
                    status == ServiceStatus.UNREACHABLE) {
                    ret = true;
                }
            } else {
                ret = (expectedStatus == status);
            }
            verbose("Returning value " + ret);
            if (!ret) {
                verbose("Current status: " + status.toString());
            }
            return ret;
        }
    }

    public class PoolVerifier implements StateVerifier {

        /**
         * Pattern : pool pool_name exists | absent
         *           pool pool_name contains | notcontains snx
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {
            if (args.size() < 3) {
                throw new IllegalArgumentException
                ("Not enough arguments for pool verifier, usage: \n" +
                        "pool <pool_name> exists|absent\n" +
                        "pool <pool_name> contains|notcontains <sn_id>");
            }
            String poolName;
            String expectedState;
            CommandServiceAPI cs = getAdmin();
            boolean retval = true;

            if (args.size() == 3) {
                boolean exists = true;

                /* Verify pool exists|absent pool_name */
                poolName = args.get(1);
                expectedState = args.get(2).toLowerCase();

                if (expectedState.equals("exists")) {
                    exists = true;
                } else if (expectedState.equals("absent")) {
                    exists = false;
                } else {
                    throw new IllegalArgumentException
                    ("Unrecognized state: " + expectedState);
                }

                verbose("Checking pool " + poolName + " against state " +
                        expectedState);

                /* Get all pools */
                Set<String> pools =
                    cs.getParameters().getStorageNodePoolNames();
                retval = pools.contains(poolName) == exists;
                verbose("Returning value " + retval);
                return retval;
            }
            boolean contains = true;
            boolean val = true;

            /* Verify pool pool_name contains|absent snX */
            poolName = args.get(1);
            expectedState = args.get(2).toLowerCase();
            if (expectedState.equals("contains")) {
                contains = true;
            } else if (expectedState.equals("notcontains")) {
                contains = false;
            } else {
                throw new IllegalArgumentException
                ("Unrecognized state: " + expectedState);
            }

            List<StorageNodeId> snIds = cs.getStorageNodePoolIds(poolName);

            if (snIds.isEmpty()) {
                verbose(poolName + " is empty!");
                return (contains ? false : true);
            }

            for (int i = 3; i < args.size(); i++) {
                String arg = args.get(i);
                StorageNodeId snId = null;
                try {
                    snId = StorageNodeId.parse(arg);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException
                        (arg + " is not a valid StorageNodeId");
                }
                verbose("Checking " + poolName +
                    (contains ? " contains " : " not contains ") + snId);
                val = snIds.contains(snId) == contains;
                verbose("Checking value " + val);
                retval = retval && val;
            }

            verbose("Returning value " + retval);
            return retval;
        }
    }

    public class DirVerifier implements StateVerifier {

        /**
         * Pattern : dir exists|absent <dir1 | dir2 | ...>
         *           dir exists|absent <dir1 & dir2 & ...>
         *           dir empty|notempty <dir1 | dir2 | ...>
         *           dir empty|notempty <dir1 & dir2 & ...>
         *
         * If use "dir empty|notempty dir1" to check a nonexistent directory
         * dir1, this verifier will return false for both of them and verbose
         * output this dir does not exists.
         *
         * Support checking state of multiple directories. When "|" is used
         * between each directory, and if any directories state subject to
         * conditions, the whole verification return true, a simple example
         * of its usage would be
         *
         *          dir exists dir1 | dir2 | ...
         *
         * When "&" is used between each directory, once one directory state
         * violate conditions, the whole verification return false, usage is
         *
         *          dir absent dir1 & dir2 & ...
         *
         * Now we have not support mixed operation of "|" and "&", and
         * bracket expressions, the unacceptable format as follows
         *
         *          dir empty dir1 | dir2 & dir3 | ...
         *          dir notempty dir1 & ( dir2 | dir3 ) & ...
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {
            final String USAGE = "dir exists|absent <dir1 | dir2 | ...>\n" +
                                 "dir exists|absent <dir1 & dir2 & ...>\n" +
                                 "dir empty|notempty <dir1 | dir2 | ...>\n" +
                                 "dir empty|notempty <dir1 & dir2 & ...>";
            if (args.size() < 3) {
                throw new IllegalArgumentException
                ("Not enough arguments for dir verifier, usage: \n" +
                 USAGE);
            }
            String expectedState = args.get(1).toLowerCase();
            File dir;
            boolean empty = false;
            boolean checkVal = false;
            boolean retval;

            List<String> expectedVals =
                getExpectedValues(args.subList(2, args.size()), USAGE);
            String logic = expectedVals.get(expectedVals.size() - 1);
            if (logic.equals("") || logic.equals("&")) {
                retval = true;
            } else {
                retval = false;
            }
            for (int i = 0; i < expectedVals.size() - 1; i++) {
                dir = new File(expectedVals.get(i));
                verbose("Checking directory " + dir + " against state " +
                        expectedState);
                boolean exists = dir.isDirectory();

                if (!exists) {
                    verbose("Directory " + dir + " not exists");

                    if (dir.exists()) {
                        verbose("Given directory " + dir + " is a file");
                    }
                    empty = false;
                } else {
                    empty = (dir.listFiles().length == 0) ? true : false;
                }

                if (expectedState.equals("exists")) {
                    checkVal = exists == true;
                } else if (expectedState.equals("absent")) {
                    checkVal = exists == false;
                } else if (expectedState.equals("empty")) {
                    checkVal = empty == true;
                } else if (expectedState.equals("notempty")) {

                    if (exists) {
                        checkVal = empty == false;
                    } else {
                        checkVal = false;
                    }
                } else {
                    verbose("Unrecognized state: " + expectedState);
                }
                verbose("Checking value " + checkVal);

                /**
                  * To deal with the "&" and "|" logic.
                  */
                if (logic.equals("") || logic.equals("&")) {
                    retval = retval && checkVal;
                    if (!retval) {
                        break;
                    }
                } else {
                    retval = retval || checkVal;
                    if (retval) {
                        break;
                    }
                }
            }
            verbose("Returning value " + retval);
            return retval;
        }
    }

    public class KeyVerifier implements StateVerifier {

        /**
         * Pattern : key exists|absent <key_name>
         * Key name only accept formal URI format, begin with a leading slash.
         * Example:
         * /MajorPathPart1/MajorPathPart2/-/MinorPathPart1/MinorPathPart2
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {

            if (args.size() < 3) {
                throw new IllegalArgumentException
                ("Not enough arguments for key verifier, usage: \n" +
                 "key exists|absent <key_name>");
            }
            boolean retval = true;
            boolean exists = args.get(1).toLowerCase().equals("exists");
            Key key;
            try {
                key = Key.fromString(args.get(2));
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException
                    ("Malformed key format: " + args.get(2));
            }

            verbose("Checking directory " + key + " against state " +
                    (exists ? "exists" : "absent"));
            KVStore store = getStore();
            try {
                if (store.get(key) == null) {
                    retval = false;
                }
                return exists == retval;
            } catch (Exception e) {
                throw new IllegalStateException("Key verification failed" + e);
            } finally {
                store.close();
            }
        }
    }

    public class StoreVerifier implements StateVerifier {

        /**
         * Pattern : store equal|notequal <other store>
         * Compare records between current and other given store by using
         * StoreUtils compare function, therefore recordType only support INT
         * and UUID, the store should be populated by StoreUtils.load().
         * <other_store> parameter should follow this pattern:
         * storeName:hostName:portNumber:recordType
         * Example mystore:localhost:13231:INT
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {

            if (args.size() < 3) {
                throw new IllegalArgumentException
                ("Not enough arguments for store, usage: \n" +
                 "store equal | notequal <store_name>");
            }
            boolean equal = false;
            boolean retval = true;

            if (args.get(1).toLowerCase().equals("equal")) {
                equal = true;
            }
            String[] config = args.get(2).split(":");

            if (config.length < 3) {
                throw new IllegalArgumentException
                ("Not enough arguments for store config, usage: \n" +
                 "storeName:hostName:portNumber:recordType");
            }
            RecordType recordType;
            try {
                recordType = RecordType.valueOf(config[3].toUpperCase());
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException
                    ("Error record type" + config[3] +
                     "only support INT|UUID");
            }

            Topology topology = getTopology();
            StoreUtils current = new StoreUtils(topology.getKVStoreName(),
                hostname, registryPort, recordType);
            StoreUtils target = new StoreUtils(config[0], config[1],
                Integer.parseInt(config[2]),recordType);
            try {
                verbose("Compare records of current store with " +
                        args.get(2));
                retval = current.compare(target) == equal;
            } catch (Exception e) {
                throw new IllegalStateException
                    ("Store verification failed" + e);
            } finally {
                current.close();
                target.close();
            }
            return retval;
        }
    }

    public class PlanVerifier implements StateVerifier {

        /**
         * Pattern: plan planID|LAST|"plan name" planStatus [numMatches]
         */
        @Override
        public boolean verify(List<String> args) throws RemoteException {
            if (args.size() != 3 && args.size() != 4) {
                throw new IllegalArgumentException
                    ("Not enough arguments for plan verifier, usage: \n" +
                     "plan planID|LAST|\"plan name\" planStatus [numMatches]");
            }

            CommandServiceAPI cs = getAdmin();
            int planId = -1;
            String planName = null;
            int numMatches = 0;
            String val = args.get(1);
            if (val.toUpperCase().equals("LAST")) {
                planId = getLastPlanId(cs);
            } else if (val.matches("[0-9]+")) {
                planId = Integer.parseInt(val);
            } else {
                // plan name to match
                planName = val;
                if (args.size() == 4 && args.get(3).matches("[0-9]+")) {
                    numMatches = Integer.parseInt(args.get(3));
                } else {
                    numMatches = 1;
                }
                planId = 1000;
            }
            Map<Integer, Plan> plans = getAllPlans(cs);
            State expectedState = verifyPlanState(args.get(2));

            if (planName != null) {
                verbose("Checking " + numMatches + " \"" + planName +
                    "\" plan(s) for state " + expectedState);
                // TODO: allow range of matching plans
                // walk all plans, see if numMatches plans of type planName are
                // in expectedState
                int matches = 0;
                for (Plan plan: plans.values()) {
                    if (expectedState != null && plan.getState() != expectedState) {
                        continue;
                    }
                    if (! plan.getDefaultName().equalsIgnoreCase(planName)) {
                        continue;
                    }
                    matches++;
                }
                verbose("Found " + matches + " plan(s) matching \"" + planName +
                    "\" and state " + expectedState);
                if (matches == numMatches) {
                    return true;
                }
                verbose("Current plans:");
                for (Plan plan: plans.values()) {
                    verbose("   Plan " + plan.getId() + " (\"" + plan.getDefaultName() + "\") " + plan.getState());
                }
                return false;
            }

            verbose("Checking plan " + planId + " against state " +
                expectedState);
            if (!plans.containsKey(planId)) {
                verbose("Plan " + planId + " does not exist");
                if (expectedState == null) {
                    return true;
                }
                verbose("Current plans:");
                for (Plan plan: plans.values()) {
                    verbose("   Plan " + plan.getId() + " (\"" + plan.getDefaultName() + "\") " + plan.getState());
                }
                return false;
            }
            State state = plans.get(planId).getState();
            verbose("Plan is actually in the state of " + state);
            boolean ret = (state == expectedState);
            if (ret == false) {
                verbose("Current plans:");
                for (Plan plan: plans.values()) {
                    verbose("   Plan " + plan.getId() + " (\"" + plan.getDefaultName() + "\") " + plan.getState());
                }
            }
            verbose("Returning value " + ret);
            return ret;
        }

        private int getLastPlanId(CommandServiceAPI cs)
            throws RemoteException {

            int range[] =
                cs.getPlanIdRange(0L, (new Date()).getTime(), 1);
            return range[0];
        }

        private int maxPlanId(Map<Integer, Plan> plans) {
            if (plans == null || plans.size() == 0) {
                return 0;
            }
            int maxId = 0;
            for (int id: plans.keySet()) {
                if (maxId < id) {
                    maxId = id;
                }
            }
            return maxId;
        }

        private Map<Integer, Plan> getAllPlans(CommandServiceAPI cs)
            throws RemoteException {
            // because kv/impl/admin/PlanStore.java implements a MAX_PLANS of 20,
            // we may not get all the plans back. Loop until we get all plans.
            Map<Integer, Plan> allPlans = null;
            int startId = 1;
            while (true) {
                Map<Integer, Plan> plans = cs.getPlanRange(startId, startId + 100);
                if (plans == null || plans.size() == 0) break;
                if (allPlans == null) {
                    allPlans = plans;
                }
                else {
                    allPlans.putAll(plans);
                }
                startId = maxPlanId(plans) + 1;
            }
            return allPlans;
        }

    }

    public class TableVerifier implements StateVerifier {
        final static int CHK_TYPE_TABLE = 0;
        final static int CHK_TYPE_FIELD = 1;
        final static int CHK_TYPE_INDEX = 2;
        final static int CHK_TYPE_KEY = 3;

        /**
         * Pattern:
         *  table absent|exists table-full-path
         *  table field-absent|field-exists table-full-path field1 & field2
         *  table index-absent|index-exists table-full-path index1 & index2
         *  table key-absent|key-exists table-full-path
         *      key-json-string1 & key-json-string2
         */
        @SuppressWarnings("null")
        @Override
        public boolean verify(List<String> args) throws RemoteException {
            final String USAGE =
                "table absent|exists table-full-path\n" +
                "table field-absent|field-exists " +
                    "table-full-path field1 & field2\n" +
                "table index-absent|index-exists " +
                    "table-full-path index1 & index2\n" +
                "table key-absent|key-exists " +
                    "table-full-path key-json-string1 & key-json-string2\n";

            if (args.size() < 3) {
                throw new IllegalArgumentException
                    ("Not enough arguments for table verifier, usage:\n" +
                     USAGE);
            }

            String expectedState = args.get(1).toLowerCase();
            String tableName = args.get(2);

            boolean exists = true;
            boolean objExists = true;
            int checkType = CHK_TYPE_TABLE;
            List<String> expectedVals = null;
            if (expectedState.equals("absent")) {
                exists = false;
            } else if (!expectedState.equals("exists")) {
                if (args.size() == 3) {
                    throw new IllegalArgumentException
                     ("Not enough arguments for table verifier, usage:\n" +
                      USAGE);
                }
                if (expectedState.startsWith("field")) {
                    checkType = CHK_TYPE_FIELD;
                } else if (expectedState.startsWith("index")) {
                    checkType = CHK_TYPE_INDEX;
                } else if (expectedState.startsWith("key")) {
                    checkType = CHK_TYPE_KEY;
                } else {
                    throw new IllegalArgumentException
                        ("Unrecognized state: " + expectedState);
                }
                if (expectedState.endsWith("absent")) {
                    objExists = false;
                }
                expectedVals =
                    getExpectedValues(args.subList(3, args.size()), USAGE);
            }

            verbose("Checking table " +
                    NameUtils.makeQualifiedName(null, null, tableName) +
                    " against state " + expectedState);

            boolean result = false;
            CommandServiceAPI cs = getAdmin();
            TableMetadata meta = cs.getMetadata(TableMetadata.class,
                                                MetadataType.TABLE);
            if (meta == null) {
                result = !exists;
                verbose("Returning value " + result);
                return result;
            }

            /* Check table existence */
            TableImpl table = meta.getTable(null, tableName, false);
            result = ((table != null) == exists);
            if (checkType == CHK_TYPE_TABLE || !result) {
                verbose("Returning value " + result);
                return result;
            }

            /* Check the existence of field and index further. */
            KVStore store = null;
            TableAPI tableImpl = null;
            if (checkType == CHK_TYPE_KEY) {
                store = getStore();
                tableImpl = store.getTableAPI();
            }
            try {
                for (int i = 0; i < expectedVals.size() - 1; i++) {
                    String expVal = expectedVals.get(i);
                    boolean ret = verifyObjectExists(table, checkType, expVal,
                                                     tableImpl, objExists);
                    if (!ret) {
                        verbose(expVal + ", Returning value false");
                        return false;
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Table key verification failed" + e);
            } finally {
                if (store != null) {
                    store.close();
                }
            }
            verbose("Returning value true");
            return true;
        }

        private boolean verifyObjectExists(TableImpl table, int type,
                                           String name, TableAPI tableImpl,
                                           boolean expected) {
            boolean result = false;
            switch (type) {
            case CHK_TYPE_FIELD:
                result = (table.getField(name) != null);
                break;
            case CHK_TYPE_INDEX:
                result = (table.getIndex(name) != null);
                break;
            case CHK_TYPE_KEY:
                PrimaryKey key = table.createPrimaryKeyFromJson(name, true);
                if (tableImpl.get(key, null) != null) {
                    result = true;
                }
                break;
            default:
                break;
            }
            return (result == expected);
        }
    }

    /**
     * Class to parse the command line.
     */
    final class VerifyParser extends CommandParser {
        final String COMMAND_NAME = "verify";
        final String COMMAND_DESC =
            "attempts to contact a store to get status of running services";
        final String TEST_FLAG = "-test";
        final String VERIFY_USAGE =
            "-test \"topology|parameter|ping SERVICE EXPECTED_STATE ...'\"";
        final String COMMAND_ARGS =
            CommandParser.getHostUsage() + " " +
            CommandParser.getPortUsage() + " " + VERIFY_USAGE;
        private String testString;

        VerifyParser(String[] args1) {
            super(args1);
        }

        String getTestString() {
            return testString;
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
            if (arg.equals(TEST_FLAG)) {
                testString = nextArg(arg);
                while (getNRemainingArgs() > 0) {
                    testString = testString + " " + nextArg(arg);
                }
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
            if (testString == null) {
                missingArg(TEST_FLAG);
            }
            if (getVerbose()) {
                verbose = true;
            }
        }
    }

    private String parseArgs(String[] args) {
        VerifyParser parser = new VerifyParser(args);
        parser.parseArgs();
        init(parser.getHostname(), parser.getRegistryPort(),
             parser.getSecurityFile());
        verbose = parser.getVerbose();
        return parser.getTestString();
    }

    public static void main(String[] args) {

        try {
            Verifier vf =  new Verifier();
            String testString = vf.parseArgs(args);
            boolean result = vf.run(testString);
            System.out.println("Result: " + result);
            if (!result) {
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Verifier exception: " + e);
            System.exit(1);
        }
        System.exit(0);
    }
}
