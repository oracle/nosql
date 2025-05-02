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

package oracle.kv.impl.admin;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.AuthenticatorManager;
import oracle.kv.impl.security.AuthenticatorManager.SystemAuthMethod;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.login.AdminInternalLoginManager;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.ParamTopoResolver;
import oracle.kv.impl.security.login.ParamTopoResolver.ParamsHandle;
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMDChange;
import oracle.kv.impl.security.metadata.SecurityMDUpdater.RoleChangeUpdater;
import oracle.kv.impl.security.metadata.SecurityMDUpdater.UserChangeUpdater;
import oracle.kv.impl.security.oauth.IDCSOAuthAuthenticator;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * This is the security management portion of the Admin. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class AdminSecurity implements GlobalParamsUpdater,
                                      ServiceParamsUpdater,
                                      UserChangeUpdater,
                                      RoleChangeUpdater {

    private final AdminService adminService;
    private final AccessCheckerImpl accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final AdminParamsHandle paramsHandle;
    private final ParamTopoResolver topoResolver;
    private final TokenVerifier tokenVerifier;
    private Logger logger;
    /* not final because it can change when configured */
    private InternalLoginManager loginMgr;
    private final AdminRoleResolver roleResolver;
    private final Map<String, Authenticator> authenticators;

    /*
     * On admin, since the heap size is small, we set a small cache size and
     * small timeout value so that the cache will not occupy too much space.
     */
    private static final int CHECKER_SUBJECT_CACHE_SIZE = 50;
    private static final long CHECKER_SUBJECT_CACHE_TIMEOUT =
        MINUTES.toMillis(30);

    /**
     * Constructor
     */
    public AdminSecurity(AdminService adminService, Logger logger) {

        this.logger = logger;
        this.adminService = adminService;
        final AdminServiceParams params = adminService.getParams();
        final SecurityParams secParams = params.getSecurityParams();
        final String storeName = params.getGlobalParams().getKVStoreName();

        if (secParams.isSecure()) {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final String hostname = snParams.getHostname();
            final int registryPort = snParams.getRegistryPort();

            this.paramsHandle = new AdminParamsHandle();
            this.topoResolver = new ParamTopoResolver(paramsHandle, logger);
            this.loginMgr = new AdminInternalLoginManager(topoResolver, logger);
            this.tokenResolver = new TokenResolverImpl(hostname, registryPort,
                                                       storeName, topoResolver,
                                                       loginMgr, logger);
            /* TODO: To be configured via parameters */
            final int roleCacheSize = 100;
            final long roleCacheEntryLifetime = MINUTES.toMillis(5);

            final CacheConfig roleCacheConfig =
                new CacheConfig().setCapacity(roleCacheSize).
                                  setLifetime(roleCacheEntryLifetime);
            this.roleResolver =
                new AdminRoleResolver(adminService, roleCacheConfig);

            final AdminParams ap = params.getAdminParams();
            final int tokenCacheCapacity = ap.getLoginCacheSize();

            final GlobalParams gp = params.getGlobalParams();
            final long tokenCacheEntryLifetime =
                gp.getLoginCacheTimeoutUnit().toMillis(
                    gp.getLoginCacheTimeout());
            final CacheConfig tokenCacheConfig =
                new CacheConfig().setCapacity(tokenCacheCapacity).
                                  setLifetime(tokenCacheEntryLifetime);
            this.tokenVerifier =
                new TokenVerifier(tokenCacheConfig, tokenResolver);
            final CacheConfig subjectCacheConfig =
                new CacheConfig().setCapacity(CHECKER_SUBJECT_CACHE_SIZE).
                                  setLifetime(CHECKER_SUBJECT_CACHE_TIMEOUT);
            this.accessChecker =
                new AccessCheckerImpl(tokenVerifier, roleResolver,
                                      subjectCacheConfig, logger);
            this.authenticators = new HashMap<>();

            /* Initialize all supported system authenticators */
            for (final SystemAuthMethod sysAuth : SystemAuthMethod.values()) {
                final Authenticator authenticator =
                    createAuthenticator(sysAuth, secParams, gp);

                if (authenticator != null) {
                    logger.info("AdminSecurity: " + sysAuth +
                                " authenticator is initialized");
                    authenticators.put(sysAuth.name(), authenticator);
                }
            }
        } else {
            paramsHandle = null;
            topoResolver = null;
            tokenResolver = null;
            accessChecker = null;
            loginMgr = null;
            tokenVerifier = null;
            roleResolver = null;
            authenticators = null;
        }
    }

    /**
     * For access by AdminService when a configure() operation is performed
     */
    void configure(String storeName) {
        if (loginMgr == null) {
            return;
        }
        loginMgr.logout();
        logger = adminService.getLogger();
        loginMgr = new InternalLoginManager(topoResolver, logger);
        topoResolver.setLogger(logger);
        tokenResolver.setLogger(logger);
        tokenResolver.setStoreName(storeName);
        accessChecker.setLogger(logger);
    }

    public AccessChecker getAccessChecker() {
        return accessChecker;
    }

    public InternalLoginManager getLoginManager() {
        return loginMgr;
    }

    /** Stop any threads associated with this instance. */
    public void stop() {
        if (accessChecker != null) {
            accessChecker.stop();
        }
        if (tokenVerifier != null) {
            tokenVerifier.stop();
        }
        if (roleResolver != null) {
            roleResolver.stop();
        }
    }

    RoleResolver getRoleResolver() {
        return roleResolver;
    }

    Map<String, Authenticator> getAuthenticators() {
        return authenticators;
    }

    IDCSOAuthAuthenticator getIDCSOAuthAuthenticator() {
        if (authenticators == null) {
            return null;
        }
        return (IDCSOAuthAuthenticator)
            authenticators.get(SystemAuthMethod.IDCSOAUTH.name());
    }

    private class AdminParamsHandle implements ParamsHandle {
        @Override
        public Parameters getParameters() {

            Admin admin = adminService.getAdmin();
            if (admin == null) {
                return null;
            }

            return admin.getCurrentParameters();
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final AdminParams ap = new AdminParams(map);
        final int newCapacity = ap.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "AdminSecurity: loginCacheSize has been updated to %d",
                newCapacity));
        }
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }

        final GlobalParams gp = new GlobalParams(map);
        final long newLifeTime =
            gp.getLoginCacheTimeoutUnit().toMillis(gp.getLoginCacheTimeout());

        /* Update the loginCacheTimeout if a new value is specified */
        if (tokenVerifier.updateLoginCacheTimeout(newLifeTime)) {
            logger.info(String.format(
                "AdminSecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }

        if (authenticators == null) {
            return;
        }
        final String[] enabledAuthMethods = gp.getUserExternalAuthMethods();

        for (Map.Entry<String, Authenticator> entry :
                authenticators.entrySet()) {
            final String authenName = entry.getKey();
            boolean enabled = false;
            for (String authMethod : enabledAuthMethods) {
                if (authenName.equals(authMethod)) {
                    enabled = true;
                    break;
                }
            }
            if (!enabled) {
                final Authenticator authen = entry.getValue();
                logger.info("AdminSecurity: disable authenticator " +
                            authenName);
                authen.resetAuthenticator();
            }
        }
    }

    @Override
    public void newRoleDefinition(SecurityMDChange mdChange) {
        if (!(mdChange.getElement() instanceof RoleInstance)) {
            throw new AssertionError();
        }
        final RoleInstance role = (RoleInstance) mdChange.getElement();

        if (roleResolver == null) {
            return;
        }

        if (roleResolver.updateRoleCache(role)) {
            logger.fine(String.format("AdminSecurity: update role %s " +
                "instance in role cache", role.name()));
        }
        if (accessChecker.updateRoleDefinition(role)) {
            logger.fine(String.format("AdminSecurity: update role %s " +
                "definition in access checker privilege cache", role.name()));
        }
    }

    @Override
    public void newUserDefinition(SecurityMDChange mdChange) {
        /* A guard */
        if (!(mdChange.getElement() instanceof KVStoreUser)) {
            throw new AssertionError();
        }
        final KVStoreUser user = (KVStoreUser)mdChange.getElement();

        if (tokenVerifier == null) {
            return;
        }

        if (tokenVerifier.updateLoginCacheSessions(user)) {
            logger.info(String.format(
                "AdminSecurity: update sessions in login cache with " +
                "metadata %d", mdChange.getSeqNum()));
        }

        if (accessChecker.updateUserDefinition(user)) {
            logger.fine(String.format("AdminSecurity: update user %s " +
                "definition in access checker privilege cache",
                user.getName()));
        }
    }

    private Authenticator createAuthenticator(SystemAuthMethod authMethod,
                                              SecurityParams secParams,
                                              GlobalParams globalParams) {
        try {
            return AuthenticatorManager.
                getAuthenticator(authMethod.name(), secParams, globalParams);
        } catch (ClassNotFoundException cfe) {
            logger.info("AdminSecurity: authenticator " + authMethod +
                " is not initialized, no implementation found");
            return null;
        } catch (Exception e) {
            logger.info("AdminSecurity: authenticator " + authMethod +
                " is not initialized, " + e.getMessage());
            return null;
        }
    }
}
