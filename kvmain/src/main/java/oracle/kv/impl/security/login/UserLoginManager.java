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

import java.rmi.RemoteException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;

/**
 * UserLoginManager provides an abstract implementation of a LoginToken Manager
 * where the logins being managed are for a KVStore user. It assumes a model
 * of a single login handle being sufficient to support all requests. Derived
 * classes must take care of handle initialization.
 */
public abstract class UserLoginManager implements LoginManager {

    /** Test hook to disable auto-renewal */
    public static volatile boolean testDisableAutoRenew = false;

    private static final AtomicInteger nextTimerId = new AtomicInteger();
    private static final AtomicInteger activeCount = new AtomicInteger();

    private final String username;
    private final boolean autoRenew;
    private long acquireTime;
    private long renewTime;
    private Timer timer;
    private volatile LoginHandle loginHandle;

    /**
     * Creates a login manager with no login handle.
     */
    public UserLoginManager(String username, boolean autoRenew) {
        this.loginHandle = null;
        this.username = username;
        this.autoRenew = autoRenew && !testDisableAutoRenew;
        activeCount.getAndIncrement();
    }

    /**
     * Returns the associated username.
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * Get a login appropriate for the specified target.  We assume that
     * we either have a non-local login or that all accesses will be local.
     */
    @Override
    public LoginHandle getHandle(ResourceId target, boolean cachedOnly) {

        if (loginHandle != null) {
            return loginHandle;
        }
        if (cachedOnly) {
            return null;
        }

        /*
         * The LoginHandle is not yet initialized. Allow the derived class to
         * initialize it dynamically.
         */
        initializeLoginHandle();

        if (loginHandle == null) {
            throw new IllegalStateException(
                "LoginManager is not initialized");
        }

        if (!loginHandle.isUsable(target.getType())) {
            throw new UnsupportedOperationException(
                "Unable to use the login handle for accessing a resource" +
                " of type " + target.getType());
        }

        return loginHandle;
    }

    /**
     * Get a login appropriate for the specified target.  We assume that
     * we either have a non-local login or that all accesses will be local.
     */
    @Override
    public LoginHandle getHandle(HostPort target,
                                 ResourceType rtype,
                                 boolean cachedOnly) {

        if (loginHandle != null) {
            return loginHandle;
        }

        if (cachedOnly) {
            return null;
        }

        /*
         * The LoginHandle is not yet initialized. Allow the derived class to
         * initialize it dynamically.
         */
        initializeLoginHandle();

        if (loginHandle == null) {
            throw new IllegalStateException(
                "LoginManager is not initialized");
        }

        if (!loginHandle.isUsable(rtype)) {
            throw new UnsupportedOperationException(
                "Unable to use the login handle for accessing a resource " +
                " of type " + rtype);
        }

        return loginHandle;
    }

    @Override
    public void logout()
        throws SessionAccessException {
        if (timer != null) {
            timer.cancel();
        }
        if (loginHandle != null) {
            loginHandle.logoutToken();
        }
        activeCount.getAndDecrement();
    }

    /**
     * Return the number of active UserLoginManager instances: ones that have
     * been created and that have not been logged out. For testing.
     */
    public static int getActiveCount() {
        return activeCount.get();
    }

    /**
     * Attempts to initialize the LoginHandle for this manager.  This is
     * called when a call to getHandle() is made, but no login handle has
     * been set.  A derived class could have sufficient knowledge to allow
     * a handle to be generated.  If so, it should call the init() method
     * with a login handle.
     */
    protected void initializeLoginHandle() {
        /* No-op implementation */
    }

    /*
     * public to be used be cloud login service
     */
    public LoginHandle getLoginHandle() {
        return loginHandle;
    }

    protected void init(LoginHandle loginHndl) {
        this.loginHandle = loginHndl;
        if (autoRenew) {
            autoRenewToken();
        }
    }

    /**
     * Common base class for UserLoginHandle.  Provides renewToken() and
     * logout() implementations.
     */
    abstract static class AbstractUserLoginHandle extends LoginHandle {
        private boolean extendFailed = false;

        AbstractUserLoginHandle(LoginToken loginToken) {
            super(loginToken);
        }

        @Override
        public LoginToken renewToken(LoginToken prevToken)
            throws SessionAccessException {

            final LoginToken currToken = getLoginToken();
            if (currToken != prevToken) {
                /* Someone else slipped in before us */
                return currToken;
            }

            try {
                final UserLoginAPI ulAPI = getLoginAPI();
                if (ulAPI == null) {
                    return null;
                }

                synchronized (this) {
                    if (currToken == getLoginToken() && !extendFailed) {
                        final LoginToken newToken =
                            ulAPI.requestSessionExtension(currToken);
                        if (newToken == null) {
                            /* apparently session extension is not supported */
                            extendFailed = true;
                        } else {
                            updateLoginToken(currToken, newToken);
                        }
                    }
                }

                return getLoginToken();

            } catch (RemoteException re) {
                throw new SessionAccessException(re,
                                                 false /* isReturnSignal */);
            }
        }

        @Override
        public void logoutToken()
            throws SessionAccessException {

            final LoginToken currToken = getLoginToken();
            if (currToken == null) {
                return;
            }

            try {
                final UserLoginAPI ulAPI = getLoginAPI();
                ulAPI.logout(currToken);
                updateLoginToken(currToken, null);
            } catch (RemoteException re) {

                /*
                 * If a RemoteException occurs, the logout probably did not
                 * actually take effect.  We currently simply ignore this since
                 * there's little a user could do about it, even if they were
                 * concerned.  At some point we may wish to allow this to be
                 * a user policy decision.
                 */
                throw new SessionAccessException(re,
                                                 false /* isReturnSignal */);
            } catch (AuthenticationRequiredException are) {
                  updateLoginToken(currToken, null);
            }
        }

        /**
         * Get a UserLoginAPI appropriate for the current LoginToken.
         */
        protected abstract UserLoginAPI getLoginAPI()
            throws RemoteException;
    }

    /**
     * Called when a new token is assigned and token renewal is enabled.
     */
    private void autoRenewToken() {
        /* Cancel any outstanding timer */
        if (timer != null) {
            timer.cancel();
            timer = null;
        }

        acquireTime = System.currentTimeMillis();
        final LoginToken token = loginHandle.getLoginToken();
        if (token == null) {
            return;
        }
        final long expireTime = token.getExpireTime();
        if (expireTime > acquireTime) {
            renewTime = acquireTime + (expireTime - acquireTime) / 2;

            timer = new Timer(getClass().getSimpleName() + "-Timer-" +
                              nextTimerId.getAndIncrement(),
                              true /* isDaemon */);

            /* Attempt a renew at the token half-life */
            timer.schedule(new RenewTask(), (renewTime - acquireTime));
        } else {
            renewTime = 0L;
        }
    }

    private class RenewTask extends TimerTask {
        @Override
        public void run() {
            try {
                final LoginToken lt = loginHandle.getLoginToken();
                loginHandle.renewToken(lt);
                autoRenewToken();
            } catch (SessionAccessException sae) {
                /* Try again in a minute */
                final long delay = 60 * 1000L;
                timer.schedule(this, delay);
                autoRenewToken();
            } catch (AuthenticationRequiredException are) {
                /* no longer feasible */
                timer.cancel();
                timer = null;
            }
        }
    }

}
