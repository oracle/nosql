/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.TestBase;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

import org.junit.Test;

/**
 * Test the TokenCache class.
 */
public class TokenCacheTest extends TestBase {

    private final Map<LoginToken, Subject> tokenResolutions =
        new HashMap<LoginToken, Subject>();

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testBasic() {

        /* Number of entries to allow the cache to contain */
        final int capacity = 3;

        /*
         * Number of ms that an entry should be allowed in the
         * cache before being evicted.
         */
        final long lifetime = 1000L;
        final CacheConfig config =
            new CacheConfig().setCapacity(capacity).setLifetime(lifetime);
        final TokenCache cache =
            new TokenCache(config, new TestResolver());

        final LoginToken[] tokens = new LoginToken[capacity + 1];
        final Subject[] subjects = new Subject[capacity + 1];

        /* Manufacture login tokens that should have a much longer lifetime */
        final long et = System.currentTimeMillis() + 3600 * 1000;

        for (int i = 0; i <= capacity; i++) {
            final byte[] sid = new byte[1];
            sid[0] = (byte) i;
            tokens[i] = new LoginToken(new SessionId(sid), et);
            subjects[i] = new Subject();
            tokenResolutions.put(tokens[i], subjects[i]);
        }

        /* Make sure the cache holds nothing yet */
        for (int i = 0; i <= capacity; i++) {
            assertNull(cache.lookup(tokens[i]));
        }

        final long cacheStart = System.currentTimeMillis();

        /* Make sure the cache holds objects when initially inserted */
        for (int i = 0; i <= capacity; i++) {
            cache.add(tokens[i], subjects[i]);
            assertSame(subjects[i], cache.lookup(tokens[i]));
        }

        /*
         * Expect that all but the first are still cached if we iterate in the
         * reverse direction.
         */
        for (int i = capacity; i > 0; i--) {
            assertSame(subjects[i], cache.lookup(tokens[i]));
        }

        assertNull(cache.lookup(tokens[0]));

        /*
         * Check that we have completed the first pass quickly enought that
         * the refresh checks are valid.
         */
        final long cachePass1 = System.currentTimeMillis();
        assertTrue(cachePass1 - cacheStart < lifetime / 2);

        /*
         * Iterate over some members of the cache repeatedly in order to
         * exercise the refresh capability.
         */
        while (true) {
            for (int i = 1; i <= capacity; i += 2) {
                assertSame(subjects[i], cache.lookup(tokens[i]));
            }
            final long now = System.currentTimeMillis();
            if ((now - cacheStart) > 2 * lifetime) {
                break;
            }
        }

        final long cachePass2 = System.currentTimeMillis();
        assertTrue(cachePass2 - cacheStart > lifetime);

        /*
         * Verify that the odd-numbered entries are still present, but not
         * the even-numbered ones
         */

        for (int i = 0; i <= capacity; i += 2) {
            assertNull(cache.lookup(tokens[i]));
        }

        for (int i = 1; i <= capacity; i += 2) {
            assertSame("i = " + i, subjects[i], cache.lookup(tokens[i]));
        }

        /* Check the stats */
        final TokenCache.EntryRefreshStats refreshStats =
            cache.getRefreshStats();
        assertTrue(refreshStats.getRefreshAttempts() > 0);

        /* Sleep for a bit to allow the cache lifetime to expire */

        try {
            Thread.sleep(lifetime + 1000);
        } catch (InterruptedException ie) {
            assertTrue(true); /* ignore */
        }

        /* Now all should be gone */
        for (int i = 0; i <= capacity; i++) {
            assertNull(cache.lookup(tokens[i]));
        }

        cache.stop(true);
    }

    @Test
    public void testUpdateSessionentry() {
        /* Number of entries to allow the cache to contain */
        final int capacity = 30;

        /*
         * Number of ms that an entry should be allowed in the
         * cache before being evicted.
         */
        final long lifetime = 1000L;
        final CacheConfig config =
            new CacheConfig().setCapacity(capacity).setLifetime(lifetime);
        final TokenCache cache =
            new TokenCache(config, new TestResolver());

        final LoginToken[] tokens = new LoginToken[capacity + 1];
        final Subject[] subjects = new Subject[capacity + 1];

        /* Manufacture login tokens that should have a much longer lifetime */
        final long et = System.currentTimeMillis() + 3600 * 1000;
        final Subject subj = makeSubject("auser", "1", "readonly");

        for (int i = 1; i <= capacity; i++) {
            final byte[] sid = new byte[1];
            sid[0] = (byte) i;
            tokens[i] = new LoginToken(new SessionId(sid), et);
            subjects[i] = subj;
            tokenResolutions.put(tokens[i], subjects[i]);
        }

        /* Add first 15 session entries */
        for (int i = 1; i <= 15; i++) {
            cache.add(tokens[i], subjects[i]);
            assertSame(subjects[i], cache.lookup(tokens[i]));
        }

        final Subject updateSubj =
            makeSubject("auser", "1", "readonly", "public");

        for (int i = 1; i <= capacity; i++) {
            tokenResolutions.put(tokens[i], updateSubj);
        }

        /* Wait first 15 session entries expire */
        try {
            Thread.sleep(lifetime);
        } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        for (int i = 16; i <= 30; i++) {
            cache.add(tokens[i], subjects[i]);
            assertSame(subjects[i], cache.lookup(tokens[i]));
        }

        for (int i = 1; i <= capacity; i++) {
            cache.updateSessionSubject(tokens[i].getSessionId(),
                                       updateSubj);
        }

        final long cacheStart = System.currentTimeMillis();
        while (true) {
            for (int i = 1; i <= capacity; i++){
                cache.lookup(tokens[i]);
            }
            final long now = System.currentTimeMillis();
            if ((now - cacheStart) > 2 * lifetime) {
                break;
            }
        }

        /* The first 15 session should be removed */
        for (int i = 1; i <= 15; i++) {
            assertNull(cache.lookup(tokens[i]));
        }
        /* The rest of sessions have been updated with new roles */
        for (int i = 16; i <= 30; i++) {
            assertEquals(updateSubj, cache.lookup(tokens[i]));
        }
    }

    private Subject makeSubject(final String userName,
                                final String userId,
                                final String... roles) {
        final Set<Principal> userPrincipals = new HashSet<Principal>();
        for (String role : roles) {
            userPrincipals.add(KVStoreRolePrincipal.get(role));
        }
        userPrincipals.add(new KVStoreUserPrincipal(userName, userId));

        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        return new Subject(true /* readOnly */,
                           userPrincipals, publicCreds, privateCreds);
    }

    class TestResolver implements TokenResolver {

        @Override
        public Subject resolve(LoginToken token) {
            return tokenResolutions.get(token);
        }
    }
}
