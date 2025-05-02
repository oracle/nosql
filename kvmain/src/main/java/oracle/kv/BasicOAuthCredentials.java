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

package oracle.kv;

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.util.SerializationUtil;

/**
 * Basic OAuth Credentials.<p>
 *
 * This class represents the basic credentials used for OAuth authentication.
 * According to OAuth specification, the credentials client used to access
 * resource servers are access token. However, the definition and format of
 * access token are different in various implementations. This class only
 * contains access token as the only field, the subclasses need to extend
 * this class in order to be compatible with various OAuth2 implementations.
 *
 * @hidden For internal use only
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class BasicOAuthCredentials implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    protected String accessToken;

    public BasicOAuthCredentials(String accessToken) {
        this.accessToken = accessToken;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public BasicOAuthCredentials(DataInput in, short serialVersion)
        throws IOException {

        accessToken = readString(in, serialVersion);
    }

    /**
     * Writes the fields of this instance to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString} String) {@link
     *      #getAccessToken accessToken}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeString(out, serialVersion, accessToken);
    }

    /**
     * @hidden For internal use only
     */
    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.BASIC_OATH;
    }

    /**
     * Return the OAuth access token.
     *
     * @return string of access token.
     */
    public String getAccessToken() {
        return accessToken;
    }

    /**
     * Identifies the user owning the access token. The subclasses need to
     * override this method to parse access token to get user name. In some
     * implementations, this method returns client name to represent the
     * identity owning this access token.
     *
     * @return user name who own this access token
     */
    @Override
    public String getUsername() {
        return null;
    }
}
