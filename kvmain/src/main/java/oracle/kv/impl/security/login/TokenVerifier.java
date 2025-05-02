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

import java.util.List;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.metadata.KVStoreUser;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * Provides a common mechanism for looking up user capabilities based on tokens.
 * This classes combines the use of TokenResolver and TokenCache in order to
 * provide low-overhead verification of tokens, on average.
 */

public class TokenVerifier {

    private final TokenResolver tokenResolver;
    private volatile TokenCache tokenCache;

    /**
     * Creates a new TokenVerifier.
     * @param cacheConfig if null, no cache is to be used.  Otherwise, provides
     *    configuration information for the token cache.
     * @param tokenResolver the resolver instance to use to determine a login
     *    identity base on a login token.
     */
    public TokenVerifier(CacheConfig cacheConfig, TokenResolver tokenResolver) {
        this.tokenResolver = tokenResolver;
        this.tokenCache = (cacheConfig == null) ?
            null :
            new TokenCache(cacheConfig, tokenResolver);
    }

    /**
     * Determine the user capabilities associated with the input LoginToken.
     * @param token a LoginToken acquired from the system
     * @return a Subject that reflects the user capabilities if the token is
     *    valid or null if not valid.
     * @throws SessionAccessException if an operational failure prevents
     *    reliable verification
     */
    public Subject verifyToken(LoginToken token)
        throws SessionAccessException {

        Subject subject =
            (null == tokenCache) ? null : tokenCache.lookup(token);

        if (subject != null) {
            return subject;
        }

        subject = tokenResolver.resolve(token);

        if (subject != null && tokenCache != null) {
            tokenCache.add(token, subject);
        }

        return subject;
    }

    /**
     * Update the tokenCache with a new size.
     *
     * @param newSize
     * @return true if the size is updated, or false if it remains the same
     */
    public boolean updateLoginCacheSize(final int newSize) {
        if (newSize == tokenCache.getCacheSize()) {
            return false;
        }
        final long entryLifeTime = tokenCache.getEntryLifeTime();
        final TokenCache oldCache = tokenCache;
        tokenCache = new TokenCache(
            new CacheConfig().setCapacity(newSize).setLifetime(entryLifeTime),
            tokenResolver);

        /* shut down background processing on the old cache */
        if (oldCache != null) {
            oldCache.stop(false);
        }
        return true;
    }

    /**
     * Update the tokenCache entry lifetime.
     *
     * @param newTimeoutInMillis
     * @return true if the lifetime is updated, or false if it remains the same
     */
    public boolean updateLoginCacheTimeout(final long newTimeoutInMillis) {
        if (newTimeoutInMillis == tokenCache.getEntryLifeTime()) {
            return false;
        }
        final TokenCache oldCache = tokenCache;
        tokenCache = new TokenCache(
            new CacheConfig().setCapacity(oldCache.getCacheSize())
            .setLifetime(newTimeoutInMillis),
            tokenResolver);

        /* shut down background processing on the old cache but don't wait */
        if (oldCache != null) {
            oldCache.stop(false);
        }
        return true;
    }

    /**
     * Update sessions in login cache with updated user.
     *
     * @param user updated KVStoreUser instance
     * @return true if sessions are updated, or false if they are not.
     */
    public boolean updateLoginCacheSessions(final KVStoreUser user) {
        if (tokenCache == null) {
            return false;
        }

        final Subject newSubject = user.makeKVSubject();
        final List<SessionId> sessIds =
            tokenCache.lookupSessionByUser(user.getName());

        if (sessIds.size() == 0) {
            return false;
        }

        for (SessionId id : sessIds) {
            tokenCache.updateSessionSubject(id, newSubject);
        }
        return true;
    }

    /** Stop any threads associated with this instance. */
    public void stop() {
        if (tokenCache != null) {
            tokenCache.stop(true);
        }
    }
}
