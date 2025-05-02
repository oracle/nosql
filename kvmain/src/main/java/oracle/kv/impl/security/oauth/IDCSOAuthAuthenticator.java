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

package oracle.kv.impl.security.oauth;

import java.io.File;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import javax.security.auth.Subject;

import oracle.kv.IDCSOAuthCredentials;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.AuthenticatorManager.SystemAuthMethod;
import oracle.kv.impl.security.KVStorePrincipal;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.LoggingCallback;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.UserSessionCallback;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.UserSessionInfo;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Authenticator implemented with OAuth2 authentication mechanism based on
 * IDCS OAuth2 Service. The implementation has dependencies on IDCS OAuth2
 * design.<p>
 *
 * The IDCS Access Token is encoded JSON Web Token string in JSON Web Signature
 * structure with some customized fields, the fields we need to resolve for
 * authentication are:<p>
 *
 * scope: space-delimited string containing scopes<p>
 * exp: Time when the JWT expires (seconds)<p>
 * aud: Contains URI string expected by the Resource Server to match
 * target URL resource prefixes.<p>
 * client_id: OAuth2 client ID. This is the GUID of the OAuth2 Client making
 * the request.<p>
 * client_name: OAuth2 client Name. This is the name of the OAuth2 Client
 * making the request.<p>
 *
 * The IDCS access token is signed by Tenant Private Key, it is required to
 * verify signature locally using Tenant Public Key, which is supposed to be
 * stored in server trust store.
 */
public class IDCSOAuthAuthenticator implements Authenticator,
                                               GlobalParamsUpdater {

    /* Allowed number of audience IDCS access token has */
    private static int ALLOWED_AUDIENCE_NUM = 1;

    private final static String LOAD_PUBLIC_KEY_ERROR_MSG =
        "IDCSOAuthAuthenticator: Failed to load IDCS OAuth public key. ";
    private final static String INVALID_ACCESS_TOKEN_ERROR_MSG =
        "IDCSOAuthAuthenticator: Not valid OAuth credentials, ";

    /**
     * Allowed scopes and their mapping roles.
     */
    private static final Map<String, KVStoreRolePrincipal> scopeRoleMapping =
        new HashMap<>();

    static {
        scopeRoleMapping.put("/read", KVStoreRolePrincipal.READONLY);
        scopeRoleMapping.put("/readwrite", KVStoreRolePrincipal.READWRITE);
        scopeRoleMapping.put("/tables.ddl", KVStoreRolePrincipal.DBADMIN);
    }
    private volatile String audience;
    private volatile String sigVerifyAlg;
    private volatile String publicKeyAlias;

    private volatile PublicKey publicKey;
    private final SecurityParams sp;

    IDCSOAuthAuthenticator(SecurityParams sp, GlobalParams gp) {
        this.audience = gp.getIDCSOAuthAudienceValue();
        this.sigVerifyAlg = gp.getIDCSOAuthSignatureVerifyAlg();
        this.publicKeyAlias = gp.getIDCSOAuthPublicKeyAlias();
        this.sp = sp;
        this.publicKey = null;
    }

    /*
     * Load public key. The public key is stored in the server trust store with
     * configured alias name.
     */
    private PublicKey loadPublicKey(UserLoginCallbackHandler handler) {
        final KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        final LoggingCallback logging = new LoggingCallback();

        if (publicKeyAlias == null) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                LOAD_PUBLIC_KEY_ERROR_MSG +
                "Public key alias is not specified"));
        }
        if (pwdSrc == null) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                LOAD_PUBLIC_KEY_ERROR_MSG +
                "Unable to create keystore password source"));
            return null;
        }
        final String certStoreName =
            sp.getConfigDir() + File.separator + sp.getTruststoreFile();
        char[] ksPwd = null;
        try {
            ksPwd = pwdSrc.getPassword();
            final KeyStore certStore = SecurityUtils.loadKeyStore(
                certStoreName, ksPwd, "truststore", sp.getTruststoreType());
            final Certificate cert = certStore.getCertificate(publicKeyAlias);
            if (cert == null) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    LOAD_PUBLIC_KEY_ERROR_MSG +
                    "Could not find certificate with alias of " +
                    publicKeyAlias));
                return null;
            }
            return cert.getPublicKey();
        } catch (IllegalArgumentException iae) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                LOAD_PUBLIC_KEY_ERROR_MSG + iae.getMessage()));
                return null;
        } catch (IllegalStateException ise) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                LOAD_PUBLIC_KEY_ERROR_MSG + ise.getMessage()));
                return null;
        } catch (KeyStoreException ke) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                LOAD_PUBLIC_KEY_ERROR_MSG +
                "Certificate store is not loaded or initialized"));
            return null;
        } finally {
            SecurityUtils.clearPassword(ksPwd);
        }
    }

    private Subject resolveSubject(String clientName,
                                   String clientId,
                                   String[] scopes) {
        final Set<KVStorePrincipal> userPrincipals = new HashSet<>();
        for (String scope : scopes) {
            final KVStoreRolePrincipal princ = scopeRoleMapping.get(scope);
            if (princ != null) {
                userPrincipals.add(princ);
            } else {
                /*
                 * If found any invalid scope, stop the resolution, because
                 * the IDCS access token only can have scopes from one
                 * resource server.
                 */
                userPrincipals.clear();
                break;
            }
        }
        if (userPrincipals.size() == 0) {
            return null;
        }
        userPrincipals.add(KVStoreRolePrincipal.PUBLIC);
        userPrincipals.add(new KVStoreUserPrincipal(
            clientName, SecurityUtils.IDCS_OAUTH_USER_ID_PREFIX + clientId));
        return SubjectUtils.createSubject(userPrincipals);
    }

    @Override
    public boolean authenticate(LoginCredentials loginCreds,
                                UserLoginCallbackHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("IDCS OAuth authentication " +
                "requires callback handler to be specified");
        }
        final LoggingCallback logging = new LoggingCallback();

        if (!(loginCreds instanceof IDCSOAuthCredentials)) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                "IDCSOAuthAuthenticator: Not OAuth credentials, type is " +
                 loginCreds.getClass().getName()));
            return false;
        }

        if (audience == null || audience.isEmpty()) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                "IDCSOAuthAuthenticator is not initialized or is disabled"));
            return false;
        }

        if (!IDCSOAuthUtils.idcsSupportedAlgorithm(sigVerifyAlg)) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                "IDCSOAuthAuthenticator: " + sigVerifyAlg +
                " is not the supported algorithm for verifying signature"));
            return false;
        }

        /* Load public key that is used for signature verification */
        publicKey = loadPublicKey(handler);
        if (publicKey == null) {
            return false;
        }

        /* Check if credentials contain access token */
        final String accessToken =
            ((IDCSOAuthCredentials) loginCreds).getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                INVALID_ACCESS_TOKEN_ERROR_MSG + "no access token"));
            return false;
        }

        try {
            final IDCSAccessToken idcsAT = IDCSAccessToken.parse(accessToken);

            /* Audience verification */
            final List<String> audiences = idcsAT.getAudience();
            if (audiences.size() != ALLOWED_AUDIENCE_NUM) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG + "invalid number " +
                    audiences.size() + " of audiences specified"));
                return false;
            }

            if (!audiences.contains(audience)) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "failed to find match audience " + audience));
                return false;
            }

            /* Access token signature verification */
            final String signedAlg = idcsAT.getSigningAlg();
            if (!IDCSOAuthUtils.idcsSupportedAlgorithm(signedAlg)) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "token is signed using an unsupported algorithm: " +
                    signedAlg));
                return false;
            }
            final String algName =
                IDCSOAuthUtils.getInternalAlgName(sigVerifyAlg);
            if (algName == null) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "cannot recognize the signing algorithm " + algName));
                return false;
            }
            final Signature signature = Signature.getInstance(algName);
            signature.initVerify(publicKey);
            signature.update(idcsAT.getSigningContent().getBytes());

            if (!signature.verify(idcsAT.getSignature())) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "signature verification failed"));
                return false;
            }

            /* Expiration verification */
            final long expireTime = idcsAT.getExpirationTime();
            if (expireTime < System.currentTimeMillis()) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG+ "access token is expired"));
                return false;
            }

            /* Scopes validation and build user subject */
            final String[] scopes = idcsAT.getScopes();
            if (scopes == null || scopes.length == 0) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "no scope claim in access token"));
                return false;
            }
            final Subject subject = resolveSubject(
                idcsAT.getClientName(), idcsAT.getClientId(), scopes);
            if (subject == null) {
                handler.handle(logging.setLevel(Level.INFO).setMessage(
                    INVALID_ACCESS_TOKEN_ERROR_MSG +
                    "failed to find valid scope"));
                return false;
            }

            /*
             * Retrieve information from access token to build the user session
             */
            handler.handle(new UserSessionCallback() {

                @Override
                public UserSessionInfo getSessionInfo() {
                    return new UserSessionInfo(subject, expireTime);
                }
            });
        } catch (IllegalArgumentException iae) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                INVALID_ACCESS_TOKEN_ERROR_MSG +
                "problem while parsing and decoding access token: " +
                LoggerUtils.getStackTrace(iae)));
                return false;
        }  catch (SignatureException se) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                INVALID_ACCESS_TOKEN_ERROR_MSG +
                "problem while attempting to verify signature: " +
                 LoggerUtils.getStackTrace(se)));
            return false;
        } catch (NoSuchAlgorithmException nsae) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                INVALID_ACCESS_TOKEN_ERROR_MSG +
                "failed to initialize signature instance : " +
                LoggerUtils.getStackTrace(nsae)));
            return false;
        } catch (InvalidKeyException ike) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                INVALID_ACCESS_TOKEN_ERROR_MSG +
                "signature verify key is invalid: " +
                LoggerUtils.getStackTrace(ike)));
            return false;
        }
        return true;
    }

    @Override
    public void resetAuthenticator() {
        this.audience = null;
        this.publicKey = null;
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        final GlobalParams gp = new GlobalParams(map);
        for (String authMethod : gp.getUserExternalAuthMethods()) {
            if (SystemAuthMethod.IDCSOAUTH.name().equals(authMethod)) {
                this.audience = gp.getIDCSOAuthAudienceValue();
                this.sigVerifyAlg = gp.getIDCSOAuthSignatureVerifyAlg();
                this.publicKeyAlias = gp.getIDCSOAuthPublicKeyAlias();
            }
        }
    }
}
