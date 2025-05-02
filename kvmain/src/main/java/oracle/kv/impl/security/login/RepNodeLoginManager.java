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

package oracle.kv.impl.security.login;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.rmi.AccessException;
import java.rmi.ConnectIOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.security.ClientProxyCredentials;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.kerberos.KerberosLoginHelper;
import oracle.kv.impl.security.login.KerberosClientCreds.KrbServicePrincipals;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.TopologyLocator.ClientRNAdminCallback;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * This is an implementation of a UserLoginManager that is appropriate to use
 * when communicating with a RepNode, which is the primary client use case.
 *
 * TODO: consider handling the case that we are unable to fully logout due
 * to no ability to write for persistent tokens.
 */
public class RepNodeLoginManager extends UserLoginManager {

    private final ClientId clientId;

    private final Protocols protocols;

    private final Logger logger;

    private TopologyManager topoManager;

    private KerberosPrincipals krbPrincipalsInfo;

    /**
     * Creates a basic instance.  Prior to using the login manager it must
     * be bootstrapped, using a call to the bootstrap() method, and typically
     * followed by a call to setTopology().
     *
     * @param username the user name for logins
     * @param autoRenew whether renew tokens automatically
     * @param clientId the client ID or null for server contexts
     * @param protocols what network protocols to use for the user login
     * service
     * @param logger the logger to use for debug output
     */
    public RepNodeLoginManager(String username,
                               boolean autoRenew,
                               ClientId clientId,
                               Protocols protocols,
                               Logger logger) {
        super(username, autoRenew);
        this.clientId = clientId;
        this.protocols = protocols;
        this.logger = checkNull("logger", logger);
        this.krbPrincipalsInfo = null;
    }

    /**
     * Creates a basic instance -- for utilities or testing. Prior to using the
     * login manager it must be bootstrapped, using a call to the bootstrap()
     * method, and typically followed by a call to setTopology(). This
     * constructor should only be used when the caller can be sure it is only
     * accessing a single store. Uses the default setting for async and a
     * default logger.
     *
     * @param username the user name for logins
     * @param autoRenew whether renew tokens automatically
     */
    public RepNodeLoginManager(String username,
                               boolean autoRenew) {
        this(username, autoRenew, null /* clientId */, Protocols.getDefault(),
             Logger.getLogger(RepNodeLoginManager.class.getName()));
    }

    /**
     * The bootstap method must be called to perform the initial login
     * operation before we have Topology information.  This will set up a
     * login token that can be used to get the Topology information.  Once
     * Topology acquisition is done, the owner should call the
     * setTopology method on this class.
     *
     * @param expectedStoreName if not null, specifies the name of the kvstore
     *   instance that we are looking for
     * @throws KVStoreException if no registry host could be found that
     *   supports the login or if an error occured while attempting the
     *   login.
     * @throws AuthenticationFailureException if the login credentials
     *   are not accepted
     */
    public void bootstrap(String[] registryHostPorts,
                          LoginCredentials loginCreds,
                          String expectedStoreName)
        throws KVStoreException, AuthenticationFailureException  {

        /*
         * The structure of this method was lifted from the
         * TopologyLocator.getInitialTopology() method. It would be nice
         * to share code, but there are differences that make factoring
         * tricky.
         */

        Exception cause = null;

        /*
         * If we run across a kvStore of the wrong name, we refuse to talk
         * to it.  Make a note if we do see one that is wrong.
         */
        String wrongStoreName = null;

        final HostPort[] hostPorts = HostPort.parse(registryHostPorts);

        for (final HostPort hostPort : hostPorts) {

            final String registryHostname = hostPort.hostname();
            final int registryPort = hostPort.port();

            try {
                for (final String serviceName :
                         RegistryUtils.getServiceNames(
                             expectedStoreName, registryHostname, registryPort,
                             protocols, clientId, logger)) {
                    try {

                        /*
                         * Skip things that don't look like RepNodes (this is
                         * for the client).
                         */
                        final String svcStoreName =
                            RegistryUtils.isRepNodeLogin(serviceName);
                        if (svcStoreName == null) {
                            continue;
                        }

                        if ((expectedStoreName != null) &&
                            !expectedStoreName.equals(svcStoreName)) {
                            wrongStoreName = svcStoreName;
                            continue;
                        }

                        final UserLoginAPI loginAPI =
                            RegistryUtils.getRepNodeLogin(
                                expectedStoreName, registryHostname,
                                registryPort, serviceName, null /* loginMgr */,
                                clientId, protocols,
                                true /* ignoreWrongType */, logger);
                        final HostPort loginTarget =
                            new HostPort(registryHostname, registryPort);
                        if (bootstrapLogin(loginAPI, loginCreds,
                                           loginTarget)) {
                            break;
                        }
                    } catch (SessionAccessException e) {
                        cause = e;
                    } catch (AccessException e) {
                        cause = e;
                    } catch (ConnectIOException e) {
                        cause = e;
                    } catch (NotBoundException e) {
                        /*
                         * Should not happen since we are iterating over a
                         * bound list.
                         */
                        cause = e;
                    } catch (InternalFaultException e) {
                        /*
                         * Be robust even in the presence of internal faults.
                         * Keep trying with other functioning nodes.
                         */
                        if (cause == null) {
                            /*
                             * Preserve non-fault exception as the reason
                             * if one's already present.
                             */
                            cause = e;
                        }
                    } catch (RNUnavailableException rnue) {
                        /*
                         * Be robust in the presence of RN unavailable cases.
                         * Keep trying with other functioning nodes.
                         */
                        if (cause == null) {
                            /*
                             * Preserve non-fault exception as the reason if
                             * one's already present.
                             */
                            cause = rnue;
                        }
                    }
                }

                /*
                 * Once we have an initial login handle, we can stop
                 * visiting the helper hosts.
                 */
                if (getLoginHandle() != null) {
                    break;
                }

            } catch (RemoteException e) {
                cause = e;
            }
        }

        /*
         * Hopefully we were able to get a login handle.  If not, it could be
         * because there were no RepNodes active at any of the hosts, or
         * because none of the RepNodes is running a LoginManager.  If the
         * latter is true, it's probably the case that the SN is not secure,
         * and the credentials are pointless.  We might want to indicate that
         * somehow, but we'll need to signal that later.  If the former is
         * true, or if we are somehow in a broken state where no LoginManager
         * is running yet and security is enabled, an
         * AuthenticationRequiredException will be signaled later.
         */

        if (getLoginHandle() == null) {
            if (wrongStoreName != null) {
                throw new KVStoreException(
                    "Could not establish an initial login from: " +
                    Arrays.toString(registryHostPorts) +
                    " - ignored non-matching store name " + wrongStoreName,
                    cause);
            }
            throw new KVStoreException(
                "Could not establish an initial login from: " +
                Arrays.toString(registryHostPorts), cause);
        }
    }

    /**
     * Register the TopologyManager to allow proper availability to
     * provided.
     */
    public void setTopology(TopologyManager topoMgr) {
        this.topoManager = topoMgr;
        final LoginHandle currentHandle = getLoginHandle();
        if (currentHandle == null) {
            /* Presumably there will be a login attempt to follow */
            return;
        }

        /* Replace the current login handle with a new one */
        final LoginHandle liveLoginHandle =
            new LiveRNLoginHandle(currentHandle.getLoginToken());
        init(liveLoginHandle);
    }

    /**
     * Re-login using the supplied credentials.
     * A topology must be in effect.
     * @throws AuthenticationRequiredException if no node could be contacted
     *    that was capable of processing the login request
     * @throws AuthenticationFailureException if the credentials are invalid
     */
    public synchronized void login(LoginCredentials creds)
        throws AuthenticationRequiredException,
               AuthenticationFailureException {

        if (topoManager == null) {
            throw new IllegalStateException("Not properly initialized");
        }

        final Topology topo = topoManager.getLocalTopology();
        try {
            login(creds, topo);
        } catch (KVStoreException e) {
            /* Cannot occur as topoManager is not null */
        }
    }

    /**
     * Re-login using the supplied credentials and topology. This method can be
     * used when TopologyManager has not been initialised, viz. for bootstrap
     * relogin in KVStoreFactory.getStoreInternal, when we have admin login only
     * and RequestDispatcher is not yet instantiated.
     * @throws KVStoreException if no node could be contacted that was capable
     *    of processing the login request and topoManager is not initialised
     * @throws AuthenticationRequiredException if no node could be contacted
     *    that was capable of processing the login request
     * @throws AuthenticationFailureException if the credentials are invalid
     */
    public synchronized void login(LoginCredentials creds, Topology topo)
        throws KVStoreException,
               AuthenticationRequiredException,
               AuthenticationFailureException {

        Exception cause = null;

        final RegistryUtils ru = new RegistryUtils(topo, (LoginManager) null,
                                                   logger);
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {

                try {
                    final UserLoginAPI rnLogin =
                        ru.getRepNodeLogin(rn.getResourceId(), protocols);
                    final LoginResult result;

                    if (creds instanceof KerberosClientCreds) {
                        result = kerberosLogin(rnLogin,
                                               (KerberosClientCreds) creds,
                                               topo.get(rn.getStorageNodeId()));
                    } else {
                        result = rnLogin.login(creds);
                    }
                    if (result.getLoginToken() != null) {
                        final LoginHandle loginHandle =
                            new LiveRNLoginHandle(result.getLoginToken());
                        init(loginHandle);
                        return;
                    }
                } catch (AuthenticationFailureException e) {
                    /* Only one chance on this */
                    throw e;
                } catch (AccessException e) {
                    cause = e;
                } catch (NotBoundException e) {
                    /*
                     * Should not happen since we are iterating over a
                     * bound list.
                     */
                    cause = e;
                } catch (InternalFaultException e) {
                    /*
                     * Be robust even in the presence of internal faults.
                     * Keep trying with other functioning nodes.
                     */
                    if (cause == null) {
                        /*
                         * Preserve non-fault exception as the reason
                         * if one's already present.
                         */
                        cause = e;
                    }
                } catch (RNUnavailableException rnue) {
                    /*
                     * Be robust in the presence of RN unavailable cases.
                     * Keep trying with other functioning nodes.
                     */
                    if (cause == null) {
                        /*
                         * Preserve non-fault exception as the reason if
                         * one's already present.
                         */
                        cause = rnue;
                    }
                } catch (RemoteException re) {
                    if (cause == null) {
                        /*
                         * Preserve non-fault exception as the reason
                         * if one's already present.
                         */
                        cause = re;
                    }
                }
            }
        }
        if (topoManager == null) {
            /* This use case is bootstrap relogin */
            throw new KVStoreException(
                "Could not establish an initial login from any RNs.", cause);
        }
        throw new AuthenticationRequiredException(cause,
                                                  false /* isReturnSignal */);
    }

    /**
     * Locates NoSQL database service principal information based upon the
     * existing topology This method is intended to be called after the
     * re-authentication with Kerberos credentials.
     *
     * @throws KVStoreException if no registry host could be found that
     * supports getting Kerberos principal information or if errors happened
     * while attempting to get principal information.
     */
    public void locateKrbPrincipals()
        throws KVStoreException{

        final Topology topo = topoManager.getLocalTopology();
        final List<String> hostPorts = new ArrayList<String>();

        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            hostPorts.add(sn.getHostname() + ":" + sn.getRegistryPort());
        }
        locateKrbPrincipals(hostPorts.toArray(new String[hostPorts.size()]),
                            topo.getKVStoreName());
    }

    /**
     * Locates NoSQL database service principal information based upon the
     * supplied SNs. This method is intended to be called after the bootstrap
     * login with Kerberos credentials.
     *
     * @throws KVStoreException if no registry host could be found that
     * supports getting Kerberos principal information or if errors happened
     * while attempting to get principal information.
     */
    public void locateKrbPrincipals(final String[] registryHostPorts,
                                    final String expectedStoreName)
        throws KVStoreException {

        /* The exception cause collector */
        final AtomicReference<Throwable> cause =
            new AtomicReference<Throwable>();

        /* The RN admin */
        final AtomicReference<ClientRepNodeAdminAPI> currentAdmin =
            new AtomicReference<>();

        TopologyLocator.applyToRNs(
            registryHostPorts,
            expectedStoreName,
            clientId,
            "Kerberos principals locator",
            this,
            cause,
            new ClientRNAdminCallback() {

                @Override
                public void callback(ClientRepNodeAdminAPI rnAdmin)
                    throws RemoteException {

                    currentAdmin.set(rnAdmin);
                }
            },
            protocols,
            logger);

        if (currentAdmin.get() == null) {

            /* If there was already a FaultException, throw that */
            if (cause.get() instanceof FaultException) {
                throw (FaultException)cause.get();
            }
            throw new KVStoreException("Could not contact any RepNode at: " +
                 Arrays.toString(registryHostPorts), cause.get());
        }

        try {
            krbPrincipalsInfo = currentAdmin.get().getKerberosPrincipals();
        } catch (RemoteException e) {
            throw new KVStoreException(
                "Could not find Kerberos principal map from: " +
                 Arrays.toString(registryHostPorts), cause.get());
        } catch (InternalFaultException e) {
            /* Clients expect FaultException */
            throw new FaultException(e, false);
        }
    }

    /**
     * Attempts to log in to a RepNode.
     *
     * @param ulLogin the user login API
     * @param loginCreds proxy login credentials containing the user
     * identity to log in as.
     * @param loginTarget The SNA trusted login identity to which we are
     * attempting the login.
     * @return the login login result
     * @throws RemoteException
     * @throws AuthenticationFailureException
     */
    private boolean bootstrapLogin(UserLoginAPI ulAPI,
                                   LoginCredentials loginCreds,
                                   HostPort loginTarget)
        throws RemoteException, AuthenticationFailureException,
               SessionAccessException {

        final LoginResult loginResult;

        if (loginCreds instanceof ClientProxyCredentials) {

            /*
             * A KVStore internal component is logging into the store on
             * behalf of a previously authenticated user. Get a login on
             * their behalf using the proxyLogin method.
             */
            loginResult =
                proxyBootstrapLogin(ulAPI.getProxy(),
                                    (ClientProxyCredentials) loginCreds,
                                    loginTarget);
        } else if (loginCreds instanceof KerberosClientCreds) {
            loginResult = KerberosLoginHelper.kerberosLogin(
                ulAPI, (KerberosClientCreds)loginCreds, loginTarget.hostname());
        } else {
            loginResult = ulAPI.login(loginCreds);
        }

        if (loginResult.getLoginToken() != null) {
            final LoginHandle loginHandle =
                new BSRNLoginHandle(loginResult.getLoginToken(), ulAPI);
            init(loginHandle);
            return true;
        }

        return false;
    }

    /**
     * Attempts a proxyLogin to the specified login interface.
     *
     * @param userLogin an RMI stub interface to a UserLogin
     * @param loginCreds proxy login credentials containing the user
     * identity to log in as and a LoginManager that authenticates us
     * as a KVStore internal entity.
     * @param loginTarget The SNA trusted login identity to which we are
     * attempting the login.
     * @return the login login result
     * @throws RemoteException
     */
    private LoginResult proxyBootstrapLogin(UserLogin userLogin,
                                            ClientProxyCredentials loginCreds,
                                            HostPort loginTarget)
        throws RemoteException, SessionAccessException {

        final UserLoginAPI localAPI =
            UserLoginAPI.wrap(userLogin,
                              loginCreds.getInternalManager().
                              getHandle(loginTarget, ResourceType.REP_NODE));

        return localAPI.proxyLogin(
            new ProxyCredentials(loginCreds.getUser()));
    }

    /**
     * Attempts to login with Kerberos credentials.
     *
     * Before perform actual Kerberos login, check if credentials contains the
     * information of service principal of specified SN. If not, try to find
     * the service principal information from cached principal information.
     */
    private LoginResult kerberosLogin(UserLoginAPI userLogin,
                                      KerberosClientCreds krbCreds,
                                      StorageNode sn) {
        final KrbServicePrincipals princs = krbCreds.getKrbServicePrincipals();
        final String hostName = sn.getHostname();

        if (princs.getPrincipal(hostName) == null) {
            /*
             * It is possible that instance name is empty, the principal info
             * won't have the entries for SNs that have empty instance name.
             */
            String instanceName = krbPrincipalsInfo.getInstanceName(sn);
            krbCreds.addServicePrincipal(hostName, instanceName);
        }
        return KerberosLoginHelper.kerberosLogin(userLogin, krbCreds, hostName);
    }

    /**
     * Implementation of LoginHandle for RepNode login after bootstrap has
     * been completed and we have a topology.
     */
    class LiveRNLoginHandle extends AbstractUserLoginHandle {

        public LiveRNLoginHandle(LoginToken loginToken) {
            super(loginToken);
        }

        /**
         * Get a UserLoginAPI appropriate for the current LoginToken.
         */
        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            final LoginToken token = getLoginToken();
            if (token == null) {
                return null;
            }
            final SessionId sessId = token.getSessionId();
            final String storename =
                topoManager.getTopology().getKVStoreName();
            final Topology topo = topoManager.getTopology();

            if (sessId.getIdValueScope() == SessionId.IdScope.PERSISTENT) {
                final List<RepNode> repNodes = topo.getSortedRepNodes();

                /*
                 * Very simple logic for now. Later, think about datacenter
                 * choice, shards, retries, etc.
                 */
                RemoteException toThrow = null;
                for (RepNode rn : repNodes) {
                    final StorageNodeId snid = rn.getStorageNodeId();
                    final StorageNode sn = topo.get(snid);
                    try {
                        return
                            RegistryUtils.getRepNodeLogin(
                                storename, sn.getHostname(),
                                sn.getRegistryPort(), rn.getResourceId(),
                                (LoginManager) null, clientId, protocols,
                                logger);
                    } catch (RemoteException re) {
                        if (toThrow == null) {
                            toThrow = re;
                        }
                    } catch (NotBoundException nbe) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }

                if (toThrow != null) {
                    throw toThrow;
                }

                throw new RemoteException("No RepNode available");
            }

            /* Non-persistent case */
            final ResourceId rid = sessId.getAllocator();
            if (!(rid instanceof RepNodeId)) {
                throw new IllegalStateException("Expected a RepNodeId");
            }

            final RepNodeId rnid = (RepNodeId) rid;
            final RepGroup rg = topo.get(new RepGroupId(rnid.getGroupId()));
            final RepNode rn = rg.get(rnid);
            if (rn == null) {
                throw new IllegalStateException(
                    "Missing RepNode with id " + rnid + " in topology");
            }
            final StorageNodeId snid = rn.getStorageNodeId();
            final StorageNode sn = topo.get(snid);
            try {
                return
                    RegistryUtils.getRepNodeLogin(
                        storename, sn.getHostname(),
                        sn.getRegistryPort(), rn.getResourceId(),
                        (LoginManager) null, clientId, protocols, logger);
            } catch (NotBoundException nbe) {
                throw new RemoteException(
                    "login interface not bound", nbe);
            }
        }

        /**
         * Report whether this login handle supports authentication to the
         * specified type of resource.
         */
        @Override
        public boolean isUsable(ResourceType rtype) {
            return (rtype.equals(ResourceType.REP_NODE) ||
                    rtype.equals(ResourceType.ADMIN));
        }
    }

    /**
     * The RepNodeLoginManager "Bootstrap" login handle. We use this upon the
     * initial login, prior to having Topology available.  It isn't very
     * durable vis. reconnections, but isn't expected to be long-lived.
     */
    static class BSRNLoginHandle extends AbstractUserLoginHandle {
        private UserLoginAPI loginAPI;

        public BSRNLoginHandle(LoginToken loginToken, UserLoginAPI loginAPI) {
            super(loginToken);
            this.loginAPI = loginAPI;
        }

        /**
         * Get a UserLoginAPI appropriate for the current LoginToken.
         */
        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            return loginAPI;
        }

        /**
         * Report whether this login handle supports authentication to the
         * specified type of resource.
         */
        @Override
        public boolean isUsable(ResourceType rtype) {
            return (rtype.equals(ResourceType.REP_NODE) ||
                    rtype.equals(ResourceType.ADMIN));
        }
    }
}
