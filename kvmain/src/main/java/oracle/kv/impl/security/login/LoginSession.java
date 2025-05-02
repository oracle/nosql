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

import java.util.Arrays;

import javax.security.auth.Subject;

/**
 * LoginSession captures the critical information about a login session.
 */
public class LoginSession {

    /* The key for this login session */
    private final Id id;

    /* The Subject that describes the logged-in entity */
    private volatile Subject subject;

    /* The client host from which the login originated */
    private final String clientHost;

    /* Indicates whether this is a persistent session */
    private final boolean isPersistentSession;

    /*
     * The time at which the login session will time out, expressed in the
     * same units and time reference as System.currentTimeMillis(). If the
     * value is 0L, the session will not expire.
     */
    private volatile long expireTime;

    public static final class Id {
        private final byte[] idValue;

        public Id(byte[] value) {
            idValue = value;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(idValue);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (other.getClass() != getClass()) {
                return false;
            }
            final Id otherId = (Id) other;
            return Arrays.equals(idValue, otherId.idValue);
        }

        public byte[] getValue() {
            return Arrays.copyOf(idValue, idValue.length);
        }

        public boolean beginsWith(byte[] prefix) {
            if (idValue.length < prefix.length) {
                return false;
            }

            for (int i = 0; i < prefix.length; i++) {
                if (idValue[i] != prefix[i]) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Computes a securely hashed identifier for the session id. The hash
         * values for two distinct ids are not guaranteed to be unique.
         */
        public int hashId() {
            return SessionId.hashId(idValue);
        }

    }

    /**
     * Create a login session object.
     */
    public LoginSession(Id id,
                        Subject subject,
                        String clientHost,
                        boolean persistent) {
        if (id == null) {
            throw new IllegalArgumentException("id may mnot be null");
        }
        this.id = id;
        this.subject = subject;
        this.clientHost = clientHost;
        this.expireTime = 0;
        this.isPersistentSession = persistent;
    }

    public Id getId() {
        return id;
    }

    public Subject getSubject() {
        return subject;
    }

    public String getClientHost() {
        return clientHost;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public boolean isExpired() {
        return expireTime != 0 && System.currentTimeMillis() > expireTime;
    }

    public boolean isPersistent() {
        return isPersistentSession;
    }

    @Override
    public LoginSession clone() {
        final LoginSession session =
            new LoginSession(getId(), getSubject(), getClientHost(),
                             isPersistentSession);
        session.setExpireTime(getExpireTime());
        return session;
    }
}
