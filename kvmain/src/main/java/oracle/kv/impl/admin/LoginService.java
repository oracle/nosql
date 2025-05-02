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

import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.security.metadata.SecurityMDChange;
import oracle.kv.impl.security.metadata.SecurityMDListener;
import oracle.kv.impl.topo.AdminId;

/**
 * Implements the login service for the Admin. The LoginService uses the admin
 * database as its source for security metadata.
 */
public class LoginService implements GlobalParamsUpdater,
                                     ServiceParamsUpdater,
                                     SecurityMDListener {
    private final AdminService aservice;

    private final UserLogin userLogin;
    private final AdminLoginHandler loginHandler;

    /**
     * Creates a LoginService instance to operate on behalf of the specified
     * AdminService.
     */
    public LoginService(AdminService aservice) {
        this.aservice = aservice;

        loginHandler = AdminLoginHandler.create(aservice);

        userLogin = new UserLoginImpl(aservice.getFaultHandler(),
                                      loginHandler,
                                      aservice.getLogger());
    }

    public UserLogin getUserLogin() {
        return userLogin;
    }

    /**
     * Reset logger for LoginHandler and UserLogin.
     */
    public void resetLogger(Logger newLogger) {
        if (loginHandler == null) {
            return;
        }
        loginHandler.resetLogger(newLogger);
        ((UserLoginImpl) userLogin).resetLogger(newLogger);
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (loginHandler == null) {
            return;
        }
        final Logger logger = aservice.getLogger();
        final int newLimit =
            map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
        if (loginHandler.updateSessionLimit(newLimit)) {
            logger.info(
                "SessionLimit for AdminLoginHandler has been updated " +
                "with " + newLimit);
        }

        /* Update the ownerId of loginHandler once the admin is deployed */
        final AdminId adminId = new AdminParams(map).getAdminId();
        if (!loginHandler.getOwnerId().equals(adminId)) {
            loginHandler.updateOwner(adminId);
            logger.info("Owner of login handler has been updated with " +
                        adminId);
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
        final Logger logger = aservice.getLogger();
        logger.info( "Config for AdminLoginHandler has been updated with " +
                     "GlobalParams:" + gp.getMap());
    }

    @Override
    public void notifyMetadataChange(SecurityMDChange mdChange) {
        if (loginHandler == null) {
            return;
        }
        final Logger logger = aservice.getLogger();
        loginHandler.updateSessionSubject(mdChange);
        logger.info("Sessions of AdminLoginHandler has been updated with " +
            "metadata change " + mdChange.getSeqNum());
    }
}
