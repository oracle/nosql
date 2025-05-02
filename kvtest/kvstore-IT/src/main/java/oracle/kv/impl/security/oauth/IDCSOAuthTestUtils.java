/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.oauth;

import java.io.File;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.interfaces.RSAPrivateKey;
import java.util.Date;
import java.util.List;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.util.SecurityUtils;

public final class IDCSOAuthTestUtils {

    public static final String OAUTH_DIR_PROP = "testoauthdir";

    /* constants for trust store having IDCS tenant public key and key alias */
    public static final String IDCS_TRUST_STORE = "idcs.trust";
    public static final String IDCS_PUBLIC_KEY = "tenantKey";
    public static final String IDCS_AUDIENCE =
        "nosql://mystore.oraclecloud.com";

    public static final String OAUTH_PW_NAME = "oauth.passwd";
    public static final String OAUTH_VFY_ALG = "RS256";
    public static final String PW_DEFAULT = "unittest";

    /*
     * An access token acquired from IDCS having below schema and value.
     *
     * {
     *  "sub": "dd6ede56a96e45e2a2aff01b951a5c02",
     *  "user.tenant.name": "TENANT1",
     *  "sub_mappingattr": "userName",
     *  "iss": "https://identity.oraclecloud.com/",
     *  "tok_type": "AT",
     *  "client_id": "dd6ede56a96e45e2a2aff01b951a5c02",
     *  "aud": "nosql://mystore.oraclecloud.com",
     *  "scope": "/tables.ddl /read",
     *  "client_tenantname": "TENANT1",
     *  "exp": 3616000869,
     *  "iat": 1468517222,
     *  "client_name": "NMCS-client",
     *  "tenant": "TENANT1",
     *   "jti": "daf0271e-786b-47bf-a610-6bd996494c2c"
     * }
     * Note that expiration time for this is long enough as 2084.
     */
    public static final String IDCS_ACCESS_TOKEN =
        "eyJraWQiOiJTSUdOSU5HX0tFWSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJkZDZlZ" +
        "GU1NmE5NmU0NWUyYTJhZmYwMWI5NTFhNWMwMiIsInVzZXIudGVuYW50Lm5hbWUiOi" +
        "JURU5BTlQxIiwic3ViX21hcHBpbmdhdHRyIjoidXNlck5hbWUiLCJpc3MiOiJodHR" +
        "wczpcL1wvaWRlbnRpdHkub3JhY2xlY2xvdWQuY29tXC8iLCJ0b2tfdHlwZSI6IkFU" +
        "IiwiY2xpZW50X2lkIjoiZGQ2ZWRlNTZhOTZlNDVlMmEyYWZmMDFiOTUxYTVjMDIiL" +
        "CJhdWQiOiJub3NxbDpcL1wvbXlzdG9yZS5vcmFjbGVjbG91ZC5jb20iLCJzY29wZS" +
        "I6IlwvdGFibGVzLmRkbCBcL3JlYWQiLCJjbGllbnRfdGVuYW50bmFtZSI6IlRFTkF" +
        "OVDEiLCJleHAiOjM2MTYwMDA4NjksImlhdCI6MTQ2ODUxNzIyMiwiY2xpZW50X25h" +
        "bWUiOiJOTUNTLWNsaWVudCIsInRlbmFudCI6IlRFTkFOVDEiLCJqdGkiOiJkYWYwM" +
        "jcxZS03ODZiLTQ3YmYtYTYxMC02YmQ5OTY0OTRjMmMifQ.H-RxN2MnrLGBnMgzfDt" +
        "FK9NYFHGOzJ77tr3DYIGbeiW0JHvwnKZVYTjWVFt0vADCiWBxDo-xqvKf4DUzbl_J" +
        "6-l82QzRZtLvTX1k0OLtIEjQwOiNtFQLUWJEKXtmgJpV5xHnYFHrfcMeIr6jsNw0j" +
        "ZHeqXQlv8C8jZOOPuM6_eU";

    /*
     * constants for key store, trust store having key pair used for signing
     * verifying self-generated access token
     */
    public static final String OAUTH_TRUST_STORE = "oauth.trust";
    public static final String OAUTH_KEY_STORE = "oauth.keys";
    public static final String OAUTH_KS_ALIAS_DEF = "signingkey";
    public static final String OAUTH_TS_ALIAS_DEF = "signingkey";
    public static final String OAUTH_KS_TYPE = "JKS";

    /*
     * constants for a wrong public key in trust store that is not the right one
     * to verify access token signature.
     */
    public static final String WRONG_TS_ALIAS_DEF = "wrongKey";

    /* not instantiable */
    private IDCSOAuthTestUtils() {
    }

    public static File getTestOAuthDir() {
        final String dir = System.getProperty(OAUTH_DIR_PROP);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test oauth directory: " +
                  OAUTH_DIR_PROP);
        }

        return new File(dir);
    }

    /*
     * Build an Access Token using IDCS schema and signed by given signing key.
     *
     * The claims of access token only include what we need to verify :
     * audience, scope, expireTime.
     */
    public static String buildAccessToken(List<String> audience,
                                          String scopes,
                                          Date expireTime,
                                          String signingKey,
                                          String keystorePath,
                                          char[] ksPwd)
        throws Exception {

        RSAPrivateKey privateKey =
            getSigningKey(signingKey, keystorePath, ksPwd);
        if (privateKey == null) {
            return null;
        }
        JWSSigner signer = new RSASSASigner(privateKey);
        JWTClaimsSet claimsSet;
        if (scopes == null) {
            claimsSet = new JWTClaimsSet.Builder().
                audience(audience).
                expirationTime(expireTime).
                claim("client_id", "u01").
                claim("client_name", "unittest").
                build();
        } else {
            claimsSet = new JWTClaimsSet.Builder().
                audience(audience).
                expirationTime(expireTime).
                claim("scope", scopes).
                claim("client_id", "u01").
                claim("client_name", "unittest").
                build();
        }

        SignedJWT signedJWT =
            new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsSet);
        signedJWT.sign(signer);

        return signedJWT.serialize();
    }

    private static RSAPrivateKey getSigningKey(String signingKey,
                                               String keystorePath,
                                               char[] ksPwd)
        throws Exception {

        KeyStore keyStore = SecurityUtils.loadKeyStore(
            keystorePath, ksPwd, "keystore", OAUTH_KS_TYPE);
        PasswordProtection pwdParam = new PasswordProtection(ksPwd);

        final PrivateKeyEntry pkEntry =
            (PrivateKeyEntry) keyStore.getEntry(signingKey,
                                                pwdParam);
        if (pkEntry == null) {
            return null;
        }
        return (RSAPrivateKey)pkEntry.getPrivateKey();
    }

    public static SecurityParams makeSecurityParams(File secDir,
                                                    String truststore,
                                                    String pwdStore) {
        SecurityParams secParams = new SecurityParams();
        secParams.setConfigDir(secDir);
        secParams.setTruststoreFile(truststore);
        secParams.setPasswordFile(pwdStore);
        return secParams;
    }

    public static GlobalParams makeGlobalParams(String audience,
                                                String publicKey,
                                                String vfyAlg) {
        GlobalParams globalParams = new GlobalParams("teststore");
        globalParams.setIDCSOAuthAudienceValue(audience);
        globalParams.setIDCSOAuthPublicKeyAlias(publicKey);
        globalParams.setIDCSOAuthSignatureVerifyAlg(vfyAlg);
        return globalParams;
    }
}
