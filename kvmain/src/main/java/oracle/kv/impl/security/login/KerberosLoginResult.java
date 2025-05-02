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

import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;

/**
 * KerberosLoginResult is the result of a Kerberos login. It extends LoginResult
 * adding the mutual authentication token in addition to the LoginToken itself.
 *
 * This class, which includes the mutual authentication token bytes, was
 * introduced in release 3.5.
 *
 * @since 3.5
 * @see #writeFastExternal FastExternalizable format
 */
public class KerberosLoginResult extends LoginResult {

    private static final long serialVersionUID = 1L;

    /*
     * Contains information that the client can use to authenticate the
     * server, when performing mutual authentication, otherwise null.
     */
    private final byte[] mutualAuthenToken;

    /**
     * Creates an instance the supplies the mutual authentication token but no
     * login token.
     */
    public KerberosLoginResult(byte[] token) {
        super(null);
        this.mutualAuthenToken = token;
    }

    public KerberosLoginResult(LoginToken loginToken, byte[] authenToken) {
        super(loginToken);
        mutualAuthenToken = authenToken;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public KerberosLoginResult(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);

        mutualAuthenToken = readNonNullByteArray(in);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link LoginResult}) {@code super}
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getMutualAuthToken mutualAuthenToken}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        writeNonNullByteArray(out, mutualAuthenToken);
    }

    @Override
    protected LoginResultType getResultType() {
        return LoginResultType.KERBEROS_LOGIN_RESULT;
    }

    public byte[] getMutualAuthToken() {
        return mutualAuthenToken;
    }
}
