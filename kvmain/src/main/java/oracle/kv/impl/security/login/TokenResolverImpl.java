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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.security.login.TopologyResolver.SNInfo;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * The standard implementation of the TokenResolver interface.
 */
public class TokenResolverImpl implements TokenResolver {

    /**
     * Maximum number of RepNodes to request from the topology resolver
     * for the purpose of a proxied persistent resolve.
     */
    private static final int MAX_RESOLVE_RNS = 10;

    /**
     * Maximum number of completed resolve failures before we trust that a
     * token is invalid.
     */
    private static final int RESOLVE_FAIL_LIMIT = 2;

    /**
     * The hostname associated with this SN.
     */
    private final String hostname;

    /**
     * The registry port associated with this SN.
     */
    private final int registryPort;

    /**
     * The KVStore store name associated with this SN, if configured.
     */
    private volatile String storeName;

    /**
     * A resolver for topology information.
     */
    private final TopologyResolver topoResolver;

    /**
     * A TokenResolver that should be used to resolve persistent tokens.
     */
    private volatile TokenResolver persistentResolver;

    /**
     * The internal login manager to allow token validation
     */
    private final LoginManager loginMgr;

    private volatile Logger logger;

    /**
     * Construct a TokenResolver.
     *
     * @param hostname the hostname at which our registry is located
     * @param registryPort the port at which our registry is located
     * @param storeName the KVStore store name - null allowable
     * @param topoResolver a topology resolver to allow the token resolver
     * to locate a host that has allocated a token - null allowable
     * @param logger a Logger instance
     */
    public TokenResolverImpl(String hostname,
                             int registryPort,
                             String storeName,
                             TopologyResolver topoResolver,
                             LoginManager internalLoginMgr,
                             Logger logger) {
        this.hostname = hostname;
        this.registryPort = registryPort;
        this.storeName = storeName;
        this.topoResolver = topoResolver;
        this.loginMgr = internalLoginMgr;
        this.persistentResolver = null;
        this.logger = logger;
    }

    /**
     * Allows changing logger during the lifecycle of a process.
     */
    public void setLogger(Logger newLogger) {
        this.logger = newLogger;
    }

    /**
     * Allows changing storeName during the lifecycle of a process.
     */
    public void setStoreName(String newStoreName) {
        this.storeName = newStoreName;
    }

    public void setPersistentResolver(TokenResolver persResolver) {
        persistentResolver = persResolver;
    }

    /*
     * @throw SessionAccessException if an operational failure prevents
     *   access to the session referenced by the token.
     */
    @Override
    public Subject resolve(LoginToken token)
        throws SessionAccessException {

        logger.fine("TokenResolver: attempt to resolve " + token);

        final SessionId sid = token.getSessionId();
        try {
            switch (sid.getIdValueScope()) {
            case PERSISTENT:
                final Subject persSubject = resolvePersistentToken(token);
                if (persSubject != null) {
                    logger.fine("TokenResolver: token is valid : " +
                                token.hashId());
                } else {
                    logger.fine("TokenResolver: token is not valid: " +
                                token.hashId());
                }
                return persSubject;

            case LOCAL:
            case STORE:
                final Subject compSubject = resolveComponentToken(token);
                if (compSubject != null) {
                    logger.fine("TokenResolver: token is valid: " +
                                token.hashId());
                } else {
                    logger.fine("TokenResolver: token is not valid: " +
                                token.hashId());
                }
                return compSubject;

            default:
                throw new UnsupportedOperationException("Unknown id scope");
            }
        } catch (RemoteException re) {
            logger.info("Unable to resolve token due to RemoteException: " +
                        re);
            throw new SessionAccessException(re, true /* isReturnSignal */);
        } catch (NotBoundException nbe) {
            logger.info("Unable to resolve token due to " +
                        "NotBoundException: " + nbe);
            throw new SessionAccessException(nbe, true /* isReturnSignal */);
        }
    }

    /**
     * Resolve a LoginToken that is known to have PERSISTENT scope.
     * @param token a PERSISTENT scope token
     * @return a Subject representing the identity and capabilities of the
     *  token, if resolvable, and null otherwise.
     * @throw SessionAccessException if an operational failure prevents
     *   access to the session referenced by the token.
     */
    private Subject resolvePersistentToken(LoginToken token)
        throws SessionAccessException {

        if (persistentResolver != null) {
            return persistentResolver.resolve(token);
        }

        if (topoResolver != null) {
            return proxyResolveComponentToken(token);
        }

        logger.info("TokenResolver: unable to resolve persistent " +
                    "token without a persistent resolver or a " +
                    "TopologyResolver");

        throw new SessionAccessException(
            "Unable to resolve a persistent session without a " +
            "persistent resolver");
    }

    /**
     * Remotely resolve a LoginToken that is known to have PERSISTENT scope.
     * The caller must verify that there is a TopologyResolver available.
     * @param token a PERSISTENT scope token
     * @return a Subject representing the identity and capabilities of the
     *  token, if resolvable, and null otherwise.
     * @throws SessionAccessException if an operational failure prevents access
     *  to security information.
     */
    private Subject proxyResolveComponentToken(LoginToken token)
        throws SessionAccessException {

        /*
         * Get a list of repNodeIds that we can use for token resolution.
         */
        final List<RepNodeId> rnList =
            topoResolver.listRepNodeIds(MAX_RESOLVE_RNS);
        if (rnList == null || rnList.isEmpty()) {
            logger.info("TokenResolver: topology resolver is unable to " +
                        "provide any RepNodeIds for token resolution.");
            throw new SessionAccessException("no RepNodeIds found");
        }

        Exception cause = null;

        int resolveFailCount = 0;

        for (RepNodeId rnId : rnList) {
            final SNInfo rnSN = topoResolver.getStorageNode(rnId);
            if (rnSN == null) {
                logger.info("TokenResolver: unable to resolve RepNodeId " +
                            rnId + " to a SN");
                continue;
            }

            /**
             * We have location information for the RepNode.  Attempt to use
             * the RepNode's UserLogin interface to resolve the token.
             */
            try {
                final Subject subj =
                    proxyResolvePersistentToken(token, rnId, rnSN);
                if (subj != null) {
                    return subj;
                }
                cause = null;
                if (++resolveFailCount >= RESOLVE_FAIL_LIMIT) {
                    break;
                }
            } catch (SessionAccessException sae) {
                logger.info("TokenResolver: remote error occurred while " +
                            " resolving token: " + sae);
                 cause = sae;
            } catch (NotBoundException nbe) {
                logger.info("TokenResolver: unable to contact remote RN " +
                            rnId + " for token resolve.");
                cause = nbe;
            } catch (RemoteException re) {
                logger.info("TokenResolver: Error on remote RN " + rnId +
                            " during token resolve.");
                cause = re;
            }
        }

        if (resolveFailCount > 0) {
            return null;
        }

        throw new SessionAccessException(cause, true /* isReturnSignal */);
    }

    /**
     * Remotely resolve a LoginToken that is known to have PERSISTENT scope
     * @param token a PERSISTENT scope token
     * @param rnId the id of the RepNode to contact
     * @param rnSN the SN on which the RepNode is located
     * @return a Subject representing the identity and capabilities of the
     *  token, if resolvable, and null otherwise.
     * @throw NotBoundException if the UserLogin interface is not bound
     *  at the SN registry
     * @throw RemoteException if an RMI error occurs during UserLogin access
     * @throw SessionAccessException if an a remote error occurred
     */
    private Subject proxyResolvePersistentToken(LoginToken token,
                                                RepNodeId rnId,
                                                SNInfo rnSN)
        throws NotBoundException, RemoteException, SessionAccessException {

        final String allocatorHost = rnSN.getHostname();
        final int allocatorPort = rnSN.getRegistryPort();

        final UserLoginAPI ulapi =
            RegistryUtils.getRepNodeLogin(storeName, allocatorHost,
                                          allocatorPort, rnId, loginMgr,
                                          logger);

        /* Let caller perform logging operations */
        return ulapi.validateLoginToken(token);
    }

    /**
     * Resolve a LoginToken that is known to be LOCAL or STORE in scope.
     */
    private Subject resolveComponentToken(LoginToken token)
        throws NotBoundException, RemoteException, SessionAccessException {

        /* Assert documents checks made by calling code */
        assert (token.getSessionId().getIdValueScope() == IdScope.LOCAL ||
                token.getSessionId().getIdValueScope() == IdScope.STORE);

        final ResourceId allocator = token.getSessionId().getAllocator();
        if (allocator == null) {
            /* Should never happen */
            logger.info("resolveComponentToken - allocator is null");

            return null;
        }

        switch (allocator.getType()) {
        case ADMIN:
            return resolveAdminToken(token);

        case REP_NODE:
            return resolveRepNodeToken(token);

        case STORAGE_NODE:
            return resolveSNAToken(token);

        default:
            logger.info("unsupported resource component: " +
                        allocator.getType());
            return null;
        }
    }

    /*
     * Admin tokens are provided by Admins to admin clients and are only
     * expected to be passed to the same, or another admin.
     */
    private Subject resolveAdminToken(LoginToken token)
        throws NotBoundException, RemoteException, SessionAccessException {

        final ResourceId allocator = token.getSessionId().getAllocator();

        /* Assert documents checks made by calling code */
        assert (allocator.getType() == ResourceId.ResourceType.ADMIN);

        final AdminId adminId = (AdminId) allocator;

        /* The host/port where the admin lives */
        String allocatorHost = null;
        int allocatorPort = 0;

        if (token.getSessionId().getIdValueScope() == IdScope.LOCAL) {
            allocatorHost = hostname;
            allocatorPort = registryPort;
        } else if (token.getSessionId().getIdValueScope() == IdScope.STORE) {
            if (topoResolver == null) {
                logger.info("Unable to resolve non-local admin token " +
                            "because parameters are not available.");
                throw new SessionAccessException("parameters not available");
            }
            final SNInfo sn = topoResolver.getStorageNode(adminId);

            if (sn == null) {
                logger.info("Unable to resolve non-local admin token " +
                            "because admin id " + adminId);
                throw new SessionAccessException("unknown allocator id");
            }

            allocatorHost = sn.getHostname();
            allocatorPort = sn.getRegistryPort();
        } else {
            logger.info("Invalid session scope for admin token: " +
                        token.getSessionId().getIdValueScope());
            return null;
        }

        final UserLoginAPI ulapi =
            RegistryUtils.getAdminLogin(allocatorHost, allocatorPort, loginMgr,
                                        logger);

        final Subject subj = ulapi.validateLoginToken(token);
        if (subj == null) {
            logger.fine("resolveAdminToken: token not valid");
        } else {
            logger.fine("resolveAdminToken: token is valid");
        }
        return subj;
    }

    /*
     * RepNode tokens are provided to KV clients and to Admins and RNs through
     * a proxy login.  They are expected to be STORE only.  PERSISTENT tokens
     * are handled through a separate code path.
     */
    private Subject resolveRepNodeToken(LoginToken token)
        throws NotBoundException, RemoteException, SessionAccessException {

        final ResourceId allocator = token.getSessionId().getAllocator();

        /* Assert documents checks made by calling code */
        assert (allocator.getType() == ResourceId.ResourceType.REP_NODE);
        final RepNodeId rnid = (RepNodeId) allocator;

        if (topoResolver == null) {
            logger.info("Unable to resolve RepNode-allocated " +
                        "token - no topology resolver available.");
            return null;
        }

        if (token.getSessionId().getIdValueScope() != IdScope.STORE) {
            logger.info("Unsupported session id scope for RepNode: " +
                        token.getSessionId().getIdValueScope());
            return null;
        }

        final SNInfo sn = topoResolver.getStorageNode(rnid);
        if (sn == null) {
            logger.info("Unable to resolve RepNode-allocated " +
                        "token - RepNode with id " + rnid);
            return null;
        }
        final String allocatorHost = sn.getHostname();
        final int allocatorPort = sn.getRegistryPort();

        final UserLoginAPI ulapi =
            RegistryUtils.getRepNodeLogin(storeName, allocatorHost,
                                          allocatorPort, rnid, loginMgr,
                                          logger);

        final Subject subj = ulapi.validateLoginToken(token);
        if (subj == null) {
            logger.fine("resolveRNToken: token not valid");
        } else {
            logger.fine("resolveRNToken: token is valid");
        }
        return subj;
    }

    /**
     * SNA tokens may be LOCAL or STORE, depending on whether the SN was
     * configured at the time.  We normally only present SNA tokens to the
     * SN that issued them, but in some cases we may be asked to resolve
     * non-local SNA tokens.
     */
    private Subject resolveSNAToken(LoginToken token)
        throws NotBoundException, RemoteException, SessionAccessException {

        String resolveHost = hostname;
        int resolvePort = registryPort;

        final ResourceId allocator = token.getSessionId().getAllocator();

        /* Assert documents checks made by calling code */
        assert (allocator.getType() == ResourceId.ResourceType.STORAGE_NODE);
        final StorageNodeId snid = (StorageNodeId) allocator;

        /*
         * Although we don't normally receive tokens that are not allocated
         * by our SNA, this can occur in the case of a forwarded SN KVStore
         * request, so attempt to resolve if possible.
         */
        if (topoResolver != null &&
            token.getSessionId().getIdValueScope() == IdScope.STORE) {

            final SNInfo sn = topoResolver.getStorageNode(snid);
            if (sn == null) {
                logger.info("resolveSNAToken: unable to resolve snid " + snid +
                            ". Will try as local.");
            } else {
                final String allocatorHost = sn.getHostname();
                final int allocatorPort = sn.getRegistryPort();

                if (!hostname.equals(allocatorHost) ||
                    registryPort != allocatorPort) {

                    resolveHost = allocatorHost;
                    resolvePort = allocatorPort;

                    logger.fine("resolveSNAToken: using allocatorHost = " +
                                allocatorHost + ", allocatorPort = " +
                                allocatorPort);
                }
            }
        }

        final TrustedLoginAPI tlapi =
            RegistryUtils.getStorageNodeAgentLogin(resolveHost, resolvePort,
                                                   logger);

        final Subject subj = tlapi.validateLoginToken(token);
        if (subj == null) {
            logger.fine("resolveSNAToken: token not valid");
        } else {
            logger.fine("resolveSNAToken: token is valid");
        }
        return subj;
    }
}
