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

import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
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
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * This is the security management portion of the SNA. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class SNASecurity implements GlobalParamsUpdater,
                                    ServiceParamsUpdater {

    @SuppressWarnings("unused")
    private final StorageNodeAgent sna;
    private final AccessChecker accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final InternalLoginManager loginMgr;
    private final TokenVerifier tokenVerifier;
    private final Logger logger;

    /**
     * Creates the security repository for the SNA.
     */
    public SNASecurity(StorageNodeAgent sna,
                       BootstrapParams bp,
                       SecurityParams sp,
                       GlobalParams gp,
                       StorageNodeParams snp,
                       Logger logger) {

        this.sna = sna;
        this.logger = logger;

        if (sp.isSecure()) {
            final String hostname = bp.getHostname();
            final int registryPort = bp.getRegistryPort();

            final String storeName = bp.getStoreName();
            final StorageNodeId snid =
                (storeName == null) ? null : new StorageNodeId(bp.getId());

            final TopologyResolver topoResolver =
                (storeName == null) ? null :
                new SNATopoResolver(
                    new TopologyResolver.SNInfo(hostname, registryPort, snid));

            this.loginMgr = new InternalLoginManager(null, logger);
            this.tokenResolver =
                new TokenResolverImpl(hostname, registryPort, storeName,
                                      topoResolver, loginMgr, logger);

            final StorageNodeParams newSNp =
                (snp == null) ?
                new StorageNodeParams(sna.getHostname(), sna.getRegistryPort(),
                                      null) :
                snp;
            final int tokenCacheCapacity = newSNp.getLoginCacheSize();

            final GlobalParams newGp =
                (gp == null) ? new GlobalParams(sna.getStoreName()) : gp;
            final long tokenCacheEntryLifetime =
                newGp.getLoginCacheTimeoutUnit().toMillis(
                    newGp.getLoginCacheTimeout());
            final CacheConfig tokenCacheConfig =
                    new CacheConfig().setCapacity(tokenCacheCapacity).
                                      setLifetime(tokenCacheEntryLifetime);

            tokenVerifier = new TokenVerifier(tokenCacheConfig, tokenResolver);

            this.accessChecker =
                new AccessCheckerImpl(tokenVerifier,
                                      new KVBuiltInRoleResolver(),
                                      null /* cache disabled */,
                                      logger);
        } else {
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

    /** Stop any threads associated with this instance. */
    public void stop() {
        if (tokenVerifier != null) {
            tokenVerifier.stop();
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final StorageNodeParams snp = new StorageNodeParams(map);
        final int newCapacity = snp.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "SNASecurity: loginCacheSize has been updated to %d",
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
                "SNASecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }
    }

    private final class SNATopoResolver implements TopologyResolver {

        final SNInfo localSNInfo;

        private SNATopoResolver(SNInfo snInfo) {
            this.localSNInfo = snInfo;
        }

        private SNATopoResolver() {
            localSNInfo = null;
        }

        @Override
        public SNInfo getStorageNode(ResourceId rid) {
            if (localSNInfo != null &&
                rid instanceof StorageNodeId &&
                ((StorageNodeId) rid).getStorageNodeId() ==
                localSNInfo.getStorageNodeId().getStorageNodeId()) {

                return localSNInfo;
            }

            return  null;
        }

        @Override
        public List<RepNodeId> listRepNodeIds(int maxRNs) {
            return null;
        }
    }
}
