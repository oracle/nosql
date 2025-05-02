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
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.LoginCredentials;

/**
 * Kerberos credential constructed internally to send over to the NoSQL store
 * server for authentication. It represents the Kerberos context initiator that
 * contains the desired flags and AP-REQ message.
 */
public class KerberosInternalCredentials
    implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    private final String username;

    private final byte [] initContextToken;

    public KerberosInternalCredentials(String username, byte [] token)
        throws IllegalArgumentException {

        /* User name and token must not be null */
        if (username == null) {
            throw new IllegalArgumentException("username must not be null");
        }
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }

        this.username = username;
        this.initContextToken = token;
    }

    public KerberosInternalCredentials(DataInput in, short serialVersion)
        throws IOException
    {
        username = readNonNullString(in, serialVersion);
        initContextToken = readNonNullByteArray(in);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writeNonNullString(out, serialVersion, username);
        writeNonNullByteArray(out, initContextToken);
    }

    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.KERBEROS_INTERNAL;
    }

    /* For testing */
    public KerberosInternalCredentials() {
        this.username = null;
        this.initContextToken = null;
    }

    @Override
    public String getUsername() {
        return username;
    }

    public byte [] getInitToken() {
        return initContextToken;
    }
}
