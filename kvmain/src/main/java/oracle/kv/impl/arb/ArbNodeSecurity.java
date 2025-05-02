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

package oracle.kv.impl.arb;

import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.KVBuiltInRoleResolver;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.TopologyResolver.SNInfo;
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * This is the security management portion of the ArbNode. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class ArbNodeSecurity implements GlobalParamsUpdater,
                                        ServiceParamsUpdater {

    private final AccessCheckerImpl accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final TopologyResolver topoResolver;
    private final TokenVerifier tokenVerifier;
    private final Logger logger;
    private final InternalLoginManager loginMgr;

    /**
     * Constructor
     */
    public ArbNodeSecurity(ArbNodeService arbService, Logger logger) {

        this.logger = logger;
        final ArbNodeService.Params params = arbService.getParams();
        final SecurityParams secParams = params.getSecurityParams();
        final String storeName = params.getGlobalParams().getKVStoreName();

        if (secParams.isSecure()) {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final String hostname = snParams.getHostname();
            final int registryPort = snParams.getRegistryPort();
            final StorageNodeId snid = snParams.getStorageNodeId();

            SNInfo sni =
                new TopologyResolver.SNInfo(hostname, registryPort, snid);
            this.topoResolver = new ArbTopoResolver(sni);
            this.loginMgr = new InternalLoginManager(topoResolver, logger);
            this.tokenResolver = new TokenResolverImpl(hostname, registryPort,
                                                       storeName, topoResolver,
                                                       loginMgr, logger);

            final ArbNodeParams ap = params.getArbNodeParams();
            final int tokenCacheCapacity = ap.getLoginCacheSize();

            final GlobalParams gp = params.getGlobalParams();
            final long tokenCacheEntryLifetime =
                gp.getLoginCacheTimeoutUnit().toMillis(
                    gp.getLoginCacheTimeout());

            final CacheConfig tokenCacheConfig =
                new CacheConfig().setCapacity(tokenCacheCapacity).
                setLifetime(tokenCacheEntryLifetime);
            tokenVerifier =
                new TokenVerifier(tokenCacheConfig, tokenResolver);

            this.accessChecker =
                new AccessCheckerImpl(tokenVerifier,
                                      new KVBuiltInRoleResolver(),
                                      null,
                                      logger);
        } else {
            topoResolver = null;
            tokenResolver = null;
            accessChecker = null;
            loginMgr = null;
            tokenVerifier = null;
        }
    }

    public AccessChecker getAccessChecker() {
        return accessChecker;
    }

    public LoginManager getLoginManager() {
        return loginMgr;
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final ArbNodeParams ap = new ArbNodeParams(map);
        final int newCapacity = ap.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "ArbNodeSecurity: loginCacheSize has been updated to %d",
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
                "ArbNodeSecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }
    }

    private final class ArbTopoResolver implements TopologyResolver {

        final SNInfo localSNInfo;

        private ArbTopoResolver(SNInfo snInfo) {
            this.localSNInfo = snInfo;
        }

        @Override
        public SNInfo getStorageNode(ResourceId rid) {
            if (rid instanceof StorageNodeId &&
                ((StorageNodeId) rid).getStorageNodeId() ==
                localSNInfo.getStorageNodeId().getStorageNodeId()) {

                return localSNInfo;
            }

            return null;
        }

        @Override
        public List<RepNodeId> listRepNodeIds(int maxRNs) {
            return null;
        }
    }
}
