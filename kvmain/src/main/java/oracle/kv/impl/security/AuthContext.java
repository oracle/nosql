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
package oracle.kv.impl.security;

import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * AuthContext captures security information for an RMI method call.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class AuthContext implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    /** Context includes a login token. */
    public static final int HAS_TOKEN = 0x1;

    /** Context includes a forwarder login token. */
    public static final int HAS_FORWARDER_TOKEN = 0x2;

    /** Context includes a client host. */
    public static final int HAS_CLIENT_HOST = 0x4;

    public static final AuthContext NULL_CTX = null;

    /* The primary login token */
    private LoginToken loginToken;

    /* The login token for the forwarding entity */
    private LoginToken forwarderToken;

    /* The client host for the loginToken, as reported by the forwarder */
    private String clientHost;

    /**
     * Create a AuthContext for an operation being initiated from the
     * original client.
     */
    public AuthContext(LoginToken loginToken) {
        this.loginToken = loginToken;
        this.forwarderToken = null;
        this.clientHost = null;
    }

    /**
     * Create a AuthContext for an operation being forwarded by an SN
     * component to another component on behalf of the original client.
     */
    public AuthContext(LoginToken loginToken,
                       LoginToken forwarderToken,
                       String clientHost) {
        this.loginToken = loginToken;
        this.forwarderToken = forwarderToken;
        this.clientHost = clientHost;
    }

    /* for FastExternalizable */
    public AuthContext(DataInput in, short serialVersion)
        throws IOException {

        final int flags = in.readByte();
        if ((flags & HAS_TOKEN) != 0) {
            loginToken = new LoginToken(in, serialVersion);
        } else {
            loginToken = null;
        }

        if ((flags & HAS_FORWARDER_TOKEN) != 0) {
            forwarderToken = new LoginToken(in, serialVersion);
        } else {
            forwarderToken = null;
        }

        if ((flags & HAS_CLIENT_HOST) == 0) {
            clientHost = null;
        } else {
            clientHost = readNonNullString(in, serialVersion);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>flags</i> // bit set with values {@link
     *      #HAS_TOKEN}=1, {@link #HAS_FORWARDER_TOKEN}=2, and {@link
     *      #HAS_CLIENT_HOST}=4
     * <li> <i>[Optional]</i> ({@link LoginToken}) {@link #getLoginToken
     *      loginToken} // if login token present
     * <li> <i>[Optional]</i> ({@link LoginToken}) {@link
     *      #getForwarderLoginToken forwarderToken} // if forwarder login token
     *      present
     * <li> <i>[Optional]</i> ({@link SerializationUtil#writeNonNullString
     *      non-null String}) {@link #getClientHost clientHost} // if client
     *      host present
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        int flags = 0;
        if (loginToken != null) {
            flags |= HAS_TOKEN;
        }
        if (forwarderToken != null) {
            flags |= HAS_FORWARDER_TOKEN;
        }
        if (clientHost != null) {
            flags |= HAS_CLIENT_HOST;
        }

        out.writeByte((byte) flags);

        if (loginToken != null) {
            loginToken.writeFastExternal(out, serialVersion);
        }
        if (forwarderToken != null) {
            forwarderToken.writeFastExternal(out, serialVersion);
        }
        if (clientHost != null) {
            writeNonNullString(out, serialVersion, clientHost);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AuthContext)) {
            return false;
        }
        final AuthContext other = (AuthContext) obj;
        return Objects.equals(loginToken, other.loginToken) &&
            Objects.equals(forwarderToken, other.forwarderToken) &&
            Objects.equals(clientHost, other.clientHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loginToken, forwarderToken, clientHost);
    }

    /**
     * Get the login token for the originating requester.
     */
    public LoginToken getLoginToken() {
        return loginToken;
    }

    /*
     * The ForwarderLoginToken is populated for forwarded requests.
     * It is needed to allow the clientHost to be specified.
     */
    public LoginToken getForwarderLoginToken() {
        return forwarderToken;
    }

    /**
     * The host that originated a forwarded request, for audit logging.
     */
    public String getClientHost() {
        return clientHost;
    }
}
