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

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

/**
 * An RMI interface that provides login capabilities for infrastructure
 * components.  KVTrustedLogin is implemented and exported by RepNode, Admin
 * and SNA components in a storage node with an InterfaceType of TRUSTED_LOGIN.
 * This is provided only over an SSL interface that requires client
 * authentication or that includes some other connection-level authentication
 * phase.
 */
public class TrustedLoginImpl
    extends VersionedRemoteImpl
    implements TrustedLogin, ServiceParamsUpdater, GlobalParamsUpdater {

    private final TrustedLoginHandler trustedHandler;
    private final ProcessFaultHandler faultHandler;
    private final Logger logger;

    public TrustedLoginImpl(ProcessFaultHandler faultHandler,
                            TrustedLoginHandler trustedHandler,
                            Logger logger) {
        this.faultHandler = faultHandler;
        this.trustedHandler = trustedHandler;
        this.logger = logger;
    }

    /**
     * Obtain a login token that identifies the caller as an infrastructure
     * component when accessing the RMI interfaces of this component.
     *
     * @return a login result
     */
    @Override
    public LoginResult loginInternal(final short serialVersion) {

        final LoginResult result = faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {
                    return trustedHandler.loginInternal(getClientHost());
                }
            });
        return result;
    }

    /**
     * Check an existing LoginToken for validity.  This is intended for use
     * with locally generated tokens.
     *
     * @return a Subject describing the user, or null if not valid
     */
    @Override
    public Subject validateLoginToken(final LoginToken loginToken,
                                      final short serialVersion) {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<Subject>() {

            @Override
            public Subject execute() {
                return trustedHandler.validateLoginToken(loginToken, logger);
            }
        });

    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure interfaces.
     * @throws AuthenticationRequiredException if the login token is not valid,
     *         or is already logged out.
     */
    @Override
    public void logout(final LoginToken loginToken,
                       final short serialVersion)
        throws AuthenticationRequiredException, SessionAccessException {

        faultHandler.execute(
            new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                try {
                    trustedHandler.logout(loginToken);
                } catch (KVSecurityException kvse) {
                    throw new ClientAccessException(kvse);
                }
            }
        });

    }

    private String getClientHost() {
        final String clientHost =
            AsyncVersionedRemoteDialogResponder.getClientHost();
        if (clientHost == null) {
            logger.severe("getClientHost failed");
        }
        return clientHost;
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (trustedHandler == null) {
            return;
        }
        final int newLimit =
            map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
        if (trustedHandler.updateSessionLimit(newLimit)) {
            logger.log(
                Level.INFO,
                "SessionLimit for TrustedLoginHandler has been updated: {0}",
                newLimit);
        }
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        if (trustedHandler == null) {
            return;
        }
        final GlobalParams gp = new GlobalParams(map);
        final long newLifetime =
            gp.getSessionTimeoutUnit().toMillis(gp.getSessionTimeout());
        if (trustedHandler.updateSessionLifetime(newLifetime)) {
            logger.log(
                Level.INFO,
                "SessionLifetime for TrustedLoginHandler has been updated: " +
                "{0} ms", newLifetime);
        }
    }
}
