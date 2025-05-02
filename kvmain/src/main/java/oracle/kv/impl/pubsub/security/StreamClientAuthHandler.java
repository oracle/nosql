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

package oracle.kv.impl.pubsub.security;

import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.subscription.SubscriptionAuthHandler;

/**
 * Object represents a re-authentication handler created by subscriber and
 * used to re-authenticate the subscriber in subscription. The handler will
 * keep track of the expiration time of current token and renew it before it
 * expires. If existing token is no longer renewable, reauthentication is
 * needed to obtain a new token.
 */
public class StreamClientAuthHandler implements SubscriptionAuthHandler {

    /*
     * percentage of token life time to renew token
     *
     * Existing facilities are renewing at the 50% mark, as shown in
     * UserLoginManager.RenewTask.autoRenewToken(). We follow the same rule
     * here.
     *
     * Note: current implementation in UserLoginManager does not parametrize
     * the percentage, there could be a maintenance issue for us if it changes
     * and we still want to keep consistent with it.
     */
    private static final int RENEW_TOKEN_LIFE_TIME_PERCENT = 50;

    /* parent rep stream consumer */
    private final NameIdPair consumerId;
    /* private logger */
    private final Logger logger;
    /* unit test only*/
    private final long reAuthIntervalMs;
    /* parent publishing unit */
    private final PublishingUnit pu;
    /* id of rep group this handler deal with */
    private final RepGroupId gid;

    /* time to renew the current login token */
    private volatile long timeToRenew;
    /* # of times token renewed */
    private final AtomicLong numTokenRenewed;
    /* # of times token re-authenticated */
    private final AtomicLong numTokenReauth;
    /* cached current token */
    private volatile LoginToken cachedToken;
    /* login handler from kvstore */
    private volatile LoginHandle loginHandle;

    /* if security check fails, record the cause */
    private volatile Throwable causeOfFailure;

    private StreamClientAuthHandler(NameIdPair consumerId,
                                    PublishingUnit pu,
                                    long reAuthIntervalMs,
                                    RepGroupId gid,
                                    Logger logger) {

        if (consumerId == null) {
            throw new IllegalArgumentException("Null consumer id name pair");
        }

        if (pu.getSecurityCred() == null) {
            throw new IllegalArgumentException("Null security credential");
        }

        this.consumerId = consumerId;
        this.pu = pu;
        this.reAuthIntervalMs = reAuthIntervalMs;
        this.gid = gid;
        this.logger = logger;

        causeOfFailure = null;
        numTokenRenewed = new AtomicLong();
        numTokenReauth = new AtomicLong();
        loginHandle = pu.getLoginHandle(gid);

        if (loginHandle.getLoginToken() == null) {
            cachedToken = null;
            /* no token, never renew */
            timeToRenew = Long.MAX_VALUE;
        } else {
            cachedToken = copyToken(loginHandle.getLoginToken());
            timeToRenew = computeTimeToRenew(cachedToken, reAuthIntervalMs);
        }

        logger.info(lm("StreamClientAuthHandler created for consumer=" +
                       consumerId.getName() + ", next time to refresh the " +
                       "token=" +
                       FormatUtils.formatDateTimeMillis(timeToRenew)));
    }

    /**
     * Gets an instance of client side authentication handler
     *
     * @param consumerId        client name id pair
     * @param pu                parent publishing unit
     * @param reAuthIntervalMs  re-authentication interval in ms
     * @param gid               replication group id
     * @param logger            private logger
     *
     * @return an instance of client side authentication handler
     *
     * @throws IllegalArgumentException if required parameter is null
     */
    public static StreamClientAuthHandler getAuthHandler(NameIdPair consumerId,
                                                         PublishingUnit pu,
                                                         long reAuthIntervalMs,
                                                         RepGroupId gid,
                                                         Logger logger)
        throws IllegalArgumentException {


        return new StreamClientAuthHandler(consumerId, pu, reAuthIntervalMs,
                                           gid, logger);
    }

    /**
     * Returns if there is a new token. Client can get the new token by
     * calling {@link SubscriptionAuthHandler#getToken()}.
     *
     * @return true if there is a new token, false otherwise.
     *
     * @throws ReplicationSecurityException if token cannot be renewed or
     * re-authenticated.
     */
    @Override
    public boolean hasNewToken() throws ReplicationSecurityException {

        if (System.currentTimeMillis() <= timeToRenew) {
            return false;
        }

        try {
            /* first try to renew the token */
            final LoginToken token = loginHandle.renewToken(cachedToken);
            if (token != null && !token.equals(cachedToken)) {
                /* successfully renew a token */
                cachedToken = copyToken(token);
                numTokenRenewed.incrementAndGet();
                timeToRenew = computeTimeToRenew(cachedToken, reAuthIntervalMs);
                /*
                 * Make it INFO level to dump token renew trace which can be
                 * useful to diagnosis other issues. It shall not flood trace
                 * files.
                 */
                logger.info(lm("Token renewed successfully, total # renewals=" +
                               numTokenRenewed.get() +
                               ", next time to renew at=" +
                               FormatUtils.formatDateTimeMillis(timeToRenew)));

                return true;
            }

            /* token not renewed, try re-authenticate */
            final KVStoreImpl kvs = pu.getKVStore();
            final LoginManager lm = KVStoreImpl.getLoginManager(kvs);

            /* ask reauth handler to reauthenticate */
            if (kvs.tryReauthenticate(lm)) {
                loginHandle = pu.getLoginHandle(gid);
                cachedToken = copyToken(loginHandle.getLoginToken());
                numTokenReauth.incrementAndGet();
                timeToRenew = computeTimeToRenew(cachedToken, reAuthIntervalMs);
                /*
                 * Make it INFO level to dump token re-authenticate trace which
                 * can be useful to diagnosis other issues. It shall not
                 * flood trace files.
                 */
                logger.info(lm("Token re-authenticated successfully, total # " +
                               "re-authenticates=" + numTokenReauth.get() +
                               ", next time to renew at=" +
                               FormatUtils.formatDateTimeMillis(timeToRenew)));
                return true;
            }

            /* unable to reauthenticate */
            throw new AuthenticationRequiredException(
                "Unable to reauthenticate with store=" +
                kvs.getTopology().getKVStoreName(), true);
        } catch (Exception cause) {
            final String err = "Error in renew or re-authenticate token with " +
                               "user=" + pu.getSecurityCred().getUserName() +
                               "(cred type=" +
                               pu.getSecurityCred().getCredentialType() + ")" +
                               " for consumer=" + consumerId +
                               ", error=" + exceptionString(cause);
            causeOfFailure = cause;
            logger.log(Level.WARNING, lm(err), cause);
            throw new ReplicationSecurityException(err, consumerId.getName(),
                                                   cause);
        }
    }

    /**
     * Gets the login token in bytes.
     *
     * @return login token in bytes, null if no login token is available.
     */
    @Override
    public byte[] getToken() {
        return cachedToken.toByteArray();
    }

    /**
     * Unit test only, sets the token
     *
     * @param token login token
     */
    public void setToken(LoginToken token) {
        cachedToken = copyToken(token);
    }

    /**
     * Gets number of times token refreshed, including # of renewal and
     * re-authenticated.
     *
     * @return number of times token refreshed
     */
    public long getNumTokenRefreshed() {
        return numTokenRenewed.get() + numTokenReauth.get();
    }

    public Throwable getCause() {
        return causeOfFailure;
    }

    private String lm(String msg) {
        return "[StreamClientAuth-" + consumerId + "] "  + msg;
    }

    /* computes a time stamp in future to renew current token */
    private static long computeTimeToRenew(LoginToken token,
                                           long reAutInvMs) {
        final long currMs = System.currentTimeMillis();
        final long expMs = token.getExpireTime();
        final long tokenLifeMs = Math.max(expMs - currMs, 0);

        /* compute time to renew from token lifetime */
        if (reAutInvMs == 0) {
            return currMs + tokenLifeMs * RENEW_TOKEN_LIFE_TIME_PERCENT / 100;
        }

        /* compute time to renew from auth interval set by user */
        return currMs + reAutInvMs;
    }

    private static LoginToken copyToken(LoginToken token) {
        return new LoginToken(token.getSessionId(), token.getExpireTime());
    }
}
