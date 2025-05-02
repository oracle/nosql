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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.security.oauth.IDCSOAuthUtils;

/**
 * Login Credentials for IDCS OAuth authentication.<p>
 *
 * This class is only used for an application to authenticate as a particular
 * client defined in IDCS to NoSQL Database. It has to wrap the access token
 * acquired from IDCS OAuth service.<p>
 *
 * IDCS is the Oracle Identity Cloud Service that provides the OAuth
 * authorization service. The access token IDCS issued is encoded as a
 * JSON Web Token (JWT) signed by IDCS private key. NoSQL database verifies
 * various fields IDCS defined in access token for authentication, such as
 * scope, exp and aud. Besides, NoSQL database also verify the signature of
 * IDCS access token.<p>
 *
 * IDCS client-only access token does not contains user name as a field, the
 * user name for this credentials is the value of client_name field. It also
 * provides an additional method {@link #getUserDisplayName()} to return
 * the user name, the user_displayname field of IDCS access token.</p>
 *
 * @hidden For internal use only
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 * @since 4.2
 */
public class IDCSOAuthCredentials extends BasicOAuthCredentials {

    private static final long serialVersionUID = 1L;

    /*
     * OAuth2 Access Tokens have user-present and client-only, client-only
     * access token does not contain value of user name and id. Use client
     * name as the user name of this credential to cover these two types of
     * access tokens.
     */
    private String oauthClientName;

    /**
     * Creates IDCS OAuth Credentials with encoded access token string acquired
     * from IDCS OAuth service in JSON Web Token format.
     * @param accessToken encoded JWT string of IDCS access token
     */
    public IDCSOAuthCredentials(String accessToken) {
        super(accessToken);
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public IDCSOAuthCredentials(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    /**
     * Writes the fields of this instance to the output stream.  Format:
     * <ol>
     * <li> ({@link BasicOAuthCredentials}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    /**
     * @hidden For internal use only
     */
    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.IDCS_OAUTH;
    }

    /**
     * Returns user_displayname field in IDCS OAuth2 Access Token, may be null
     * if specified client-only access token.
     * @return the value of the user_displayname field or null
     * @throws IllegalArgumentException if parsing or decoding access token
     * encounter failures.
     */
    public String getUserDisplayName() {
        return IDCSOAuthUtils.getUserDisplayName(accessToken);
    }

    /**
     * Returns client_name field in IDCS OAuth2 Access Token.
     * @return client_name field in access token
     * @throws IllegalArgumentException if parsing or decoding access token
     * encounter failures.
     */
    @Override
    public String getUsername() {
        this.oauthClientName = IDCSOAuthUtils.getClientName(accessToken);
        return oauthClientName;
    }
}
