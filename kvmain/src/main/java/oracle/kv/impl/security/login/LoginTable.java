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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreUserPrincipal;

/**
 * Provides a common mechanism for tracking login sessions in memory.
 * It is configured with a maximum capacity, and will maintain the number
 * of entries below that capacity.  If a new session is to be created but
 * the table is full, the least recently referenced session is dropped from
 * the table.
 * TODO: consider adding support for a logout listener interface to allow
 * notifications of session logouts.  Unless it's implemented to broadcast
 * network-wide, it's of limited value, but may be worth doing to lessen
 * the window of opportunity for usage of logged-out sessions.
 */

public class LoginTable implements SessionManager {

    private static final float LOAD_FACTOR = 0.6F;

    /* Random number generator for session id creation */
    private final SecureRandom random;

    /* A prefix to add to generated ids */
    private final byte[] idPrefix;

    /* The size of the generated portion of a session id value */
    private final int nSIDRandomBytes;

    /* The LRU login map */
    private final LinkedHashMap<LoginSession.Id, LoginSession> loginMap;

    /* The capacity of the table */
    private volatile int capacity;

    /**
     * Create a new LoginTable holding at most the specified capacity
     * of logins.
     *
     * @param capacity the limit on the number of entries that the table should
     * allow
     * @param idPrefix a prefix to add to all generated id values
     * @param nSIDRandomBytes the number of bytes of random identifier to
     * generate for ids.
     */
    @SuppressWarnings("serial")
    public LoginTable(int capacity, byte[] idPrefix, int nSIDRandomBytes) {
        this.capacity = capacity;
        random = new SecureRandom();
        loginMap = new LinkedHashMap<LoginSession.Id, LoginSession> (
            capacity, LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<LoginSession.Id, LoginSession> entry) {
                return size() > LoginTable.this.capacity;
            }
        };
        this.idPrefix = Arrays.copyOf(idPrefix, idPrefix.length);
        this.nSIDRandomBytes = nSIDRandomBytes;
    }

    /* SessionManager methods */

    /**
     * Create a new Session.
     */
    @Override
    public LoginSession createSession(
        final Subject subject, String clientHost, long expireTime) {

        final LoginSession sess = newLogin(subject, clientHost);
        sess.setExpireTime(expireTime);
        return sess;
    }

    /**
     * Look up a Session by SessionId.
     * @return the login session if found, or else null
     */
    @Override
    public LoginSession lookupSession(LoginSession.Id sessionId) {
        synchronized (loginMap) {
            return loginMap.get(sessionId);
        }
    }

    /**
     * Update session expiration time.
     * @return the updated session if the update was successful, or else null
     */
    @Override
    public LoginSession updateSessionExpiration(LoginSession.Id sessionId,
                                                long expireTime) {
        return updateExpiration(sessionId, expireTime);
    }

    /**
     * Log out the specified session.
     */
    @Override
    public void logoutSession(LoginSession.Id sessionId) {
        synchronized (loginMap) {
            loginMap.remove(sessionId);
        }
    }

    /**
     * Look up session ids of given user.
     */
    @Override
    public List<LoginSession.Id> lookupSessionByUser(String userName) {

        final List<LoginSession.Id> ids = new ArrayList<LoginSession.Id>();
        synchronized (loginMap) {
            for (LoginSession entry : loginMap.values()) {
                final Subject subject = entry.getSubject();
                final KVStoreUserPrincipal sessUserPrinc =
                    ExecutionContext.getSubjectUserPrincipal(subject);

                if (sessUserPrinc != null &&
                    sessUserPrinc.getName().equals(userName)) {
                    ids.add(entry.getId());
                }
            }
        }
        return ids;
    }

    /**
     * Update session with given new subject.
     */
    @Override
    public void updateSessionSubject(LoginSession.Id sessionId,
                                     Subject newSubject) {

        final LoginSession session;
        synchronized (loginMap) {
            session = loginMap.get(sessionId);
        }
        if (session != null && !session.getSubject().equals(newSubject)) {
            session.setSubject(newSubject);
        }
    }

    /* Non-interface methods */

    /**
     * Update the limit on number of managed sessions.
     * @param newLimit the maximum number of sessions that this manager
     * should support.
     * @return true if the limit was changed
     */
    public boolean updateSessionLimit(int newLimit) {
        return resize(newLimit);
    }

    private LoginSession newLogin(Subject subject, String clientHost) {
        while (true) {
            final byte[] randomBytes = new byte[nSIDRandomBytes];
            final byte[] idBytes = new byte[nSIDRandomBytes + idPrefix.length];
            random.nextBytes(randomBytes);
            System.arraycopy(idPrefix, 0, idBytes, 0, idPrefix.length);
            System.arraycopy(randomBytes, 0, idBytes, idPrefix.length,
                             nSIDRandomBytes);

            final LoginSession.Id id = new LoginSession.Id(idBytes);
            final LoginSession session =
                new LoginSession(id, subject, clientHost, false);

            synchronized (loginMap) {
                loginMap.put(session.getId(), session);
            }
            return session;
        }
    }

    /**
     * Updates the login table to replace an existing session with the new
     * session. The table must already contain a session with the same id.
     * @return an updated login session if successful
     */
    private LoginSession updateExpiration(LoginSession.Id sessionId,
                                          long newExpire) {
        final LoginSession session;
        synchronized (loginMap) {
            session = loginMap.get(sessionId);
        }
        if (session != null) {
            session.setExpireTime(newExpire);
        }
        return session;
    }

    /**
     * Resize the capacity of the login table
     *
     * @param newCapacity
     * @return true if updated with a new capacity
     */
    private boolean resize(final int newCapacity) {
        synchronized (loginMap) {
            if (newCapacity <= 0 || newCapacity == this.capacity) {
                return false;
            }
            if (newCapacity < loginMap.size()) {
                /*
                 * Remove the oldest entries.  LinkedHashMap guarantees that
                 * the iteration is in order of least to most recently accessed.
                 */
                final int remove = loginMap.size() - newCapacity;
                final Iterator<LoginSession.Id> iter =
                    loginMap.keySet().iterator();
                int count = 0;
                while (iter.hasNext() && (count++ < remove)) {
                    iter.next();
                    iter.remove();
                }
            }
            this.capacity = newCapacity;
            return true;
        }
    }

    /* For test purpose */
    int size() {
        return loginMap.size();
    }
}
