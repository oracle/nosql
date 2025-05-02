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

package oracle.kv.impl.rep.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.security.login.UserLoginHandler.SESSION_ID_RANDOM_BYTES;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeSecurity;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.AsyncUserLoginResponder;
import oracle.kv.impl.security.login.LoginTable;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginHandler;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.security.metadata.SecurityMDChange;
import oracle.kv.impl.security.metadata.SecurityMDListener;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * RepNodeLoginService provides the user login service for the RepNode.
 */
public class RepNodeLoginService implements GlobalParamsUpdater,
                                            ServiceParamsUpdater,
                                            SecurityMDListener {

    /**
     *  The repNode being served
     */
    private final RepNodeService repNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final ServiceFaultHandler faultHandler;

    /**
     * The login module for the RepNode
     */
    private final UserLogin userLogin;

    /**
     * The login module for the RepNode wrapped by the SecureProxy
     */
    private UserLogin exportableUL;

    private final Logger logger;

    private final UserLoginHandler loginHandler;

    private LoginTable tableSessionMgr;

    /**
     * Creates a RepNodeLoginService to provide login capabilities for
     * the RepNode.
     */
    public RepNodeLoginService(RepNodeService repNodeService) {

        this.repNodeService = repNodeService;
        logger = LoggerUtils.getLogger(this.getClass(),
                                       repNodeService.getParams());

        if (repNodeService.getParams().getSecurityParams().isSecure()) {
            faultHandler =
                new ServiceFaultHandler(repNodeService,
                                        logger,
                                        ProcessExitCode.RESTART);
            loginHandler =
                makeLoginHandler(repNodeService.getRepNodeSecurity().
                                 getKVSessionManager());
            userLogin = new UserLoginImpl(faultHandler, loginHandler, logger);
        } else {
            faultHandler = null;
            userLogin = null;
            loginHandler = null;
        }
    }

    public UserLogin getUserLogin() {
        return userLogin;
    }

    private UserLoginHandler makeLoginHandler(KVSessionManager kvSessMgr) {
        final UserVerifier verifier =
            repNodeService.getRepNodeSecurity().getUserVerifier();
        final RepNodeId rnId = repNodeService.getRepNodeId();

        /* Populated loginConfig from GlobalParameters */
        final GlobalParams gp = repNodeService.getParams().getGlobalParams();
        final LoginConfig loginConfig = LoginConfig.buildLoginConfig(gp);

        /* For use when no writable shard can be found for login */
        final RepNodeParams rp = repNodeService.getRepNodeParams();
        tableSessionMgr = new LoginTable(rp.getSessionLimit(),
                                         FailoverSessionManager.MEMORY_PREFIX,
                                         SESSION_ID_RANDOM_BYTES);

        final FailoverSessionManager failSessMgr =
            new FailoverSessionManager(kvSessMgr, tableSessionMgr, logger);

        /* RN does not support password renewal */
        final UserLoginHandler ulh =
            new UserLoginHandler(rnId, false, verifier,
                                 null /* password renewer */,
                                 failSessMgr, loginConfig, logger);

        return ulh;
    }

    /**
     * Starts up the login service. The UserLogin is bound in the registry.
     */
    public void startup()
        throws RemoteException {

        if (userLogin == null) {
            logger.info("No RN Login configured. ");
            return;
        }

        final RepNodeParams rnp = repNodeService.getRepNodeParams();
        final String kvsName =
                repNodeService.getParams().getGlobalParams().getKVStoreName();

        final String csfName =
            ClientSocketFactory.factoryName(
                kvsName, RepNodeId.getPrefix(), LOGIN.interfaceName());

        final StorageNodeParams snp =
            repNodeService.getParams().getStorageNodeParams();

        final RMISocketPolicy rmiPolicy = repNodeService.getParams().
            getSecurityParams().getRMISocketPolicy();
        final SocketFactoryPair sfp =
            rnp.getLoginSFP(rmiPolicy, snp.getServicePortRange(), csfName);

        initExportableUL();

        logger.info("Starting RN Login. " +
                    " Server socket factory:" + sfp.getServerFactory() +
                    " Client socket connect factory: " +
                    sfp.getClientFactory());

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        repNodeService.rebind(exportableUL, LOGIN,
                              sfp.getClientFactory(),
                              sfp.getServerFactory(),
                              USER_LOGIN_TYPE_FAMILY,
                              AsyncUserLoginResponder::new);

        logger.info("Starting UserLogin");
    }

    /**
     * Unbind the login service in the registry.
     */
    public void stop() {
        if (userLogin != null) {
            try {
                repNodeService.unbind(exportableUL, LOGIN);
            } catch (RemoteException e) {
                /* Ignore */
            }
        }
    }

    private void initExportableUL() {
        final RepNodeSecurity rnSecurity = repNodeService.getRepNodeSecurity();
        try {
            exportableUL =
                SecureProxy.create(userLogin,
                                   rnSecurity.getAccessChecker(),
                                   faultHandler);
            logger.info(
                "Successfully created secure proxy for the user login");
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tableSessionMgr == null) {
            return;
        }
        final int newLimit =
            map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
        if (tableSessionMgr.updateSessionLimit(newLimit)) {
            logger.info(
                "SessionLimit for LoginTable has been updated with " +
                newLimit);
        }
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        if (loginHandler == null) {
            return;
        }
        final GlobalParams gp = new GlobalParams(map);
        final LoginConfig config = LoginConfig.buildLoginConfig(gp);
        loginHandler.updateConfig(config);
        logger.info(
            "Config for UserLoginHandler has been updated with GlobalParams:" +
            gp.getMap()
        );
    }

    @Override
    public void notifyMetadataChange(SecurityMDChange mdChange) {
        if (loginHandler == null) {
            return;
        }

        /*
         * When SecurityMetadataManager.update calls this method, the call
         * occurs inside of a JE transaction commit hook, and must return
         * quickly.  SecurityMetadataManager.update only calls this method on
         * replicas, since that is how replicas learn about persistent meta
         * data changes made on the master.  The call to updateSessionUpdate
         * below is what makes the persistent meta data change, and so should
         * only be made on the master.  Not calling updateSessionUpdate on
         * replicas means this method won't fail or block due to a lack of
         * quorum, which could otherwise cause a deadlock in the JE transaction
         * commit.
         */
        final ReplicatedEnvironment repEnv =
            repNodeService.getRepNode().getEnv(1);
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return;
        }
        logger.info("Attempting to update sessions with metadata change: " +
                     mdChange.getSeqNum());
        loginHandler.updateSessionSubject(mdChange);
        logger.info("Sessions has been updated with metadata change: " +
                mdChange.getSeqNum());
    }
}
