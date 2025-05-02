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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.impl.security.KVStorePrincipal;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.LoginSession;
import oracle.kv.impl.security.util.SubjectUtils;

/**
 * KVSession captures the state of a login session as stored in the persistent
 * security section of the store.
 */
public class KVSession {

    /*
     * The data format of this object.  We provide for the ability to create
     * read, update and delete of version 2 objects and to read update and
     * delete (no create) version 3.  If a version 3 of this format is created,
     * it must only add fields at the end.  If an object of version 3 is read,
     * we de-serialize only the V2 portion and retain the remainder as a
     * byte array, and add it to the end of the object when re-writing.
     *
     * Version history:
     * 1: initial version
     * 2: change type of roles field from KVStoreRole to String
     */
    static final int VERSION = 2;
    static final int NEXT_VERSION = 3;

    /**
     * The source version of this object.
     */
    private byte version;

    /**
     * The key of this session
     */
    private byte[] sessionId;

    /**
     * The internal user id associated with the session.
     */
    private String userId;

    /**
     * The user name associated with the session
     */
    private String userName;

    /**
     * The names of KVStore roles associated with the session
     */
    private String[] roles;

    /**
     * The address of the host from which the user login originated
     */
    private String clientHost;

    /**
     * The expiration  time of the session
     */
    private long sessionExpire;

    /**
     * A possibly null byte array that should be appended to the output when
     * writing.
     */
    byte[] remainder;

    private KVSession(byte version) {
        this.version = version;
    }

    /**
     * Creates a KVSession object from a LoginSession.
     * @throws IllegalArgumentException if the session is null or does not
     * have an associated KVStoreUserPrincipal
     */
    public KVSession(LoginSession session)
        throws IllegalArgumentException {

        if (session == null) {
            throw new IllegalArgumentException("session may not be null");
        }

        this.version = VERSION;
        final byte[] idValue = session.getId().getValue();
        this.sessionId = Arrays.copyOf(idValue, idValue.length);
        final Subject subj = session.getSubject();

        final KVStoreUserPrincipal userPrinc =
            KVStoreUserPrincipal.getSubjectUser(subj);
        if (userPrinc == null) {
            throw new IllegalArgumentException(
                "No user principal associated with the login session");
        }
        this.userId = userPrinc.getUserId();
        this.userName = userPrinc.getName();
        this.roles = KVStoreRolePrincipal.getSubjectRoleNames(subj);
        if (this.roles == null) {
            this.roles = new String[0];
        }
        this.clientHost = session.getClientHost();
        this.sessionExpire = session.getExpireTime();
    }

    public byte[] toByteArray()
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        dos.write(version);

        dos.write(sessionId.length);
        dos.write(sessionId, 0, sessionId.length);

        if (userId == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(userId);
        }
        if (userName == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(userName);
        }
        if (clientHost == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(clientHost);
        }
        dos.write(roles.length);
        for (int i = 0; i < roles.length; i++) {
            /*
             * The serialized result may be read by an old version R3 node
             * during upgrade, so we write the the R3 compatible name into
             * stream so that the old node can recognize it successfully. 
             */
            dos.writeUTF(RoleInstance.getR3CompatName(roles[i]));
        }
        dos.writeLong(sessionExpire);

        /*
         * If there is any remainder (because we read a version 2 object), put
         * the remainder on the output stream.
         */
        if (remainder != null) {
            dos.write(remainder, 0, remainder.length);
        }
        dos.close();

        return baos.toByteArray();
    }

    public static KVSession fromByteArray(byte[] data)
        throws IOException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        final DataInputStream dis = new DataInputStream(bais);

        final int version = dis.readByte();

        if (version < 1 /* initial version */ ||
            version > NEXT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        final KVSession session = new KVSession((byte) version);

        final int sidLen = dis.readByte();
        session.sessionId = new byte[sidLen];
        dis.readFully(session.sessionId);

        final boolean haveUserId = dis.readBoolean();
        session.userId = haveUserId ? dis.readUTF() : null;

        final boolean haveUserName = dis.readBoolean();
        session.userName = haveUserName ? dis.readUTF() : null;

        final boolean haveClientHost = dis.readBoolean();
        session.clientHost = haveClientHost ? dis.readUTF() : null;

        final int nRoles = dis.readByte();

        session.roles = new String[nRoles];
        for (int i = 0; i < nRoles; i++) {
            session.roles[i] = RoleInstance.getNormalizedName(dis.readUTF());
        }
        session.sessionExpire = dis.readLong();

        if (version == NEXT_VERSION) {
            /*
             * If we are reading a session written in the next version of the
             * format, it may differ only in the presence of additional fields.
             * Capture the remaining data so that it can be included if this 
             * object is re-written.
             */
            final byte[] buffer = new byte[data.length];
            final int bytesRead = dis.read(buffer);
            if (bytesRead > 0) {
                session.remainder = Arrays.copyOfRange(buffer, 0, bytesRead);
            }
        } else {
            int read = dis.read(new byte[1]);
            if (read > 0) {
                throw new IOException("Encountered unexpected data");
            }
        }

        dis.close();

        return session;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public String getUserName() {
        return userName;
    }

    public long getSessionExpire() {
        return sessionExpire;
    }

    public String[] getUserRoles() {
        return roles;
    }

    public void setSessionExpire(long expireTime) {
        sessionExpire = expireTime;
    }

    public void setUserRoles(String[] roles) {
        this.roles = roles;
    }

    public LoginSession makeLoginSession() {

        final LoginSession sess =
            new LoginSession(new LoginSession.Id(sessionId),
                             makeSubject(), clientHost, true);
        sess.setExpireTime(sessionExpire);
        return sess;
    }

    /* For testing */
    void setVersion(byte version) {
        if (version < VERSION) {
            throw new IllegalArgumentException(
                "Version earlier than " + VERSION + " is deprecated");
        }
        this.version = version;
    }

    private Subject makeSubject() {

        final Set<KVStorePrincipal> princs = new HashSet<>();

        princs.add(new KVStoreUserPrincipal(userName, userId));

        for (final String role : roles) {
            final KVStoreRolePrincipal princ =
                KVStoreRolePrincipal.get(role);
            princs.add(princ);
        }

        return SubjectUtils.createSubject(princs);
    }
}
