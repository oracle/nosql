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

import static oracle.kv.impl.util.SerializationUtil.readCharArray;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeCharArray;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.async.AsyncInitiatorProxy.MethodCallClass;
import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * An async interface that provides user login capabilities for the KVStore
 * database. KVUserLogin is implemented and exported by RepNode components in a
 * storage node with an InterfaceType of LOGIN.
 *
 * @since 21.2
 */
public interface AsyncUserLogin extends AsyncVersionedRemote {

    /** The IDs for methods in this interface. */
    enum ServiceMethodOp implements MethodOp {
        GET_SERIAL_VERSION(0, GetSerialVersionCall::new),
        LOGIN(1, LoginCall::new),
        PROXY_LOGIN(2, ProxyLoginCall::new),
        RENEW_PASSWORD_LOGIN(3, RenewPasswordLoginCall::new),
        REQUEST_SESSION_EXTENSION(4, RequestSessionExtensionCall::new),
        VALIDATE_LOGIN_TOKEN(5, ValidateLoginTokenCall::new),
        LOGOUT(6, LogoutCall::new);

        private static final ServiceMethodOp[] VALUES = values();

        private final ReadFastExternal<MethodCall<?>> reader;

        ServiceMethodOp(final int ordinal,
                        final ReadFastExternal<MethodCall<?>> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Returns the ServiceMethodOp with the specified ordinal.
         *
         * @param ordinal the ordinal
         * @return the ServiceMethodOp
         * @throws IllegalArgumentException if there is no associated value
         */
        static ServiceMethodOp valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for ServiceMethodOp: " + ordinal, e);
            }
        }

        @Override
        public int getValue() {
            return ordinal();
        }

        @Override
        public MethodCall<?> readRequest(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }

        @Override
        public String toString() {
            return name() + '(' + ordinal() + ')';
        }
    }

    /* Remote methods */

    @MethodCallClass(GetSerialVersionCall.class)
    @Override
    CompletableFuture<Short> getSerialVersion(short serialVersion,
                                              long timeoutMillis);

    class GetSerialVersionCall extends AbstractGetSerialVersionCall
            implements ServiceMethodCall<Short> {
        public GetSerialVersionCall() { }
        @SuppressWarnings("unused")
        GetSerialVersionCall(DataInput in, short serialVersion) { }
        @Override
        public CompletableFuture<Short>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.getSerialVersion(serialVersion, timeoutMillis);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_SERIAL_VERSION;
        }
        @Override
        public String describeCall() {
            return "AsyncUserLogin.GetSerialVersionCall";
        }
    }

    /**
     * Log a user into the database.  A login may be persistent, in which
     * case it is can be used on any rep node, and does not depend upon the
     * node that served the token being up for others to make use of the token,
     * or it can be non-persistent, in which case it remains valid only while
     * the serving component is up.  For the non-persistent case, it can be
     * either local or non-local.  In the case of local tokens, it is valid
     * only for use on the SN that performed the login. Non-local tokens may
     * be used on SNs other than the one that originated it.  Note that a
     * non-persistent token is quicker to process and requires fewer resources,
     * but is less resilient in the case of a component restart and less
     * efficient and reliable for logging out if the login token is used with
     * multiple components.  If the login is authenticated but the
     * login cannot be made persistent, the login is treated as successful
     * and the returned login token is noted as non-persistent.
     *
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @return a future with a LoginResult, or {@link
     *         AuthenticationFailureException} if the LoginCredentials are not
     *         valid
     */
    @MethodCallClass(LoginCall.class)
    CompletableFuture<LoginResult> login(short serialVersion,
                                         LoginCredentials creds,
                                         long timeoutMillis);

    class LoginCall extends AbstractCall<LoginResult> {
        final LoginCredentials creds;
        public LoginCall(final LoginCredentials creds) {
            this.creds = creds;
        }
        LoginCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(
                     in, serialVersion,
                     LoginCredentials::readLoginCredentials));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(
                out, sv, creds, LoginCredentials::writeLoginCredentials);
        }
        @Override
        public LoginResult readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          LoginResult::readLoginResult);
        }
        @Override
        public void writeResponse(LoginResult response,
                                  DataOutput out,
                                  short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, response,
                                    LoginResult::writeLoginResult);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.LOGIN;
        }
        @Override
        public CompletableFuture<LoginResult>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.login(serialVersion, creds, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("creds=").append(creds);
        }
    }

    /**
     * Attempt a login on behalf of a user into the database.  If a
     * user logs in to an admin but subsequently issues a command that requires
     * access to a RepNode, the admin uses this interface to establish a usable
     * LoginToken.  The caller must provide a security context of internal.
     * This is not supported by the SNA.
     *
     * @return a future with a LoginResult, provided that the login did not
     *         fail, {@link AuthenticationFailureException} if the credentials
     *         are not valid, or {@link AuthenticationRequiredException} if the
     *         component does not support proxy login
     */
    @MethodCallClass(ProxyLoginCall.class)
    CompletableFuture<LoginResult> proxyLogin(short serialVersion,
                                              ProxyCredentials creds,
                                              AuthContext authContext,
                                              long timeoutMillis);

    class ProxyLoginCall extends AbstractCall<LoginResult> {
        final ProxyCredentials creds;
        final AuthContext authContext;
        public ProxyLoginCall(final ProxyCredentials creds,
                              final AuthContext authContext) {
            this.creds = creds;
            this.authContext = authContext;
        }
        ProxyLoginCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion,
                                        ProxyCredentials::new),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, creds);
            writeFastExternalOrNull(out, sv, authContext);
        }
        @Override
        public LoginResult readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          LoginResult::readLoginResult);
        }
        @Override
        public void writeResponse(LoginResult response,
                                  DataOutput out,
                                  short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, response,
                                    LoginResult::writeLoginResult);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.PROXY_LOGIN;
        }
        @Override
        public CompletableFuture<LoginResult>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.proxyLogin(serialVersion, creds, authContext,
                                      timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("creds=").append(creds);
        }
    }

    /**
     * Request that an old password be replaced with new one passed in, and a
     * login will be established by using the new password.  In any other case,
     * e.g., the users' password is not expired yet, the login will fail.
     * <p>
     * The old password should be the one user currently use in the system,
     * but expires. At the present, only login interface on admin nodes
     * supports this.
     *
     * @param oldCreds the password credentials are expired
     * @param newPassword new password
     *
     * @return a future with a LoginResult, or {@link
     *         AuthenticationFailureException} if old password credentials are
     *         not valid or password renewal operation is failed.
     */
    @MethodCallClass(RenewPasswordLoginCall.class)
    CompletableFuture<LoginResult>
        renewPasswordLogin(short serialVersion,
                           PasswordCredentials oldCreds,
                           char[] newPassword,
                           long timeoutMillis);

    class RenewPasswordLoginCall extends AbstractCall<LoginResult> {
        final PasswordCredentials creds;
        final char[] newPassword;
        public RenewPasswordLoginCall(final PasswordCredentials creds,
                                      final char[] newPassword) {
            this.creds = creds;
            this.newPassword = newPassword;
        }
        RenewPasswordLoginCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion,
                                        PasswordCredentials::new),
                 readCharArray(in));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, creds);
            writeCharArray(out, newPassword);
        }
        @Override
        public LoginResult readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          LoginResult::readLoginResult);
        }
        @Override
        public void writeResponse(LoginResult response,
                                  DataOutput out,
                                  short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, response,
                                    LoginResult::writeLoginResult);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.RENEW_PASSWORD_LOGIN;
        }
        @Override
        public CompletableFuture<LoginResult>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.renewPasswordLogin(serialVersion, creds,
                                              newPassword, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("creds=").append(creds);
        }
    }

    /**
     * Request that a login token be replaced with a new token that has a later
     * expiration than that of the original token.  Depending on system policy,
     * this might not be allowed.  This is controlled by the
     * sessionTokenExpiration security configuration parameter.
     * TODO: link to discussion of system policy
     *
     * @return a future with null if the LoginToken is not valid or if session
     *         extension is not allowed - otherwise, return a new LoginToken.
     *         The returned LoginToken will use the same session id as the
     *         original token, but the client should use the new LoginToken for
     *         subsequent requests. Returns with {@link SessionAccessException}
     *         if unable to access the session associated with the token due to
     *         an infrastructure malfunction.
     */
    @MethodCallClass(RequestSessionExtensionCall.class)
    CompletableFuture<LoginToken>
        requestSessionExtension(short serialVersion,
                                LoginToken loginToken,
                                long timeoutMillis);

    class RequestSessionExtensionCall extends AbstractCall<LoginToken> {
        final LoginToken loginToken;
        public RequestSessionExtensionCall(final LoginToken loginToken) {
            this.loginToken = loginToken;
        }
        RequestSessionExtensionCall(final DataInput in, final short sv)
            throws IOException
        {
            this(readFastExternalOrNull(in, sv, LoginToken::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, loginToken);
        }
        @Override
        public LoginToken readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion, LoginToken::new);
        }
        @Override
        public void writeResponse(LoginToken response,
                                  DataOutput out,
                                  short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, response);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.REQUEST_SESSION_EXTENSION;
        }
        @Override
        public CompletableFuture<LoginToken>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.requestSessionExtension(serialVersion, loginToken,
                                                   timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("loginToken=").append(loginToken);
        }
    }

    /**
     * Check an existing LoginToken for validity.
     *
     * @return a future with a Subject describing the user, or null, if the
     *         token is not valid. Returns with {@link SessionAccessException}
     *         if unable to resolve the token due to an infrastructure
     *         malfunction.
     */
    @MethodCallClass(ValidateLoginTokenCall.class)
    CompletableFuture<Subject> validateLoginToken(short serialVersion,
                                                  LoginToken loginToken,
                                                  AuthContext authCtx,
                                                  long timeoutMillis);

    class ValidateLoginTokenCall extends AbstractCall<Subject> {
        final LoginToken loginToken;
        final AuthContext authCtx;
        public ValidateLoginTokenCall(final LoginToken loginToken,
                                      final AuthContext authCtx) {
            this.loginToken = loginToken;
            this.authCtx = authCtx;
        }
        ValidateLoginTokenCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion, LoginToken::new),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, loginToken);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Subject readResponse(DataInput in, short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          SubjectUtils::readSubject);
        }
        @Override
        public void writeResponse(Subject response,
                                  DataOutput out,
                                  short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, response,
                                    SubjectUtils::writeSubject);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.VALIDATE_LOGIN_TOKEN;
        }
        @Override
        public CompletableFuture<Subject>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.validateLoginToken(serialVersion, loginToken,
                                              authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("loginToken=").append(loginToken);
        }
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure object interfaces.  If the session is already logged
     * out, this is treated as a a successful operation.  If the LoginToken
     * is not recognized, this may be because it was logged out earlier and
     * flushed from memory, and so this case will also be treated as successful.
     *
     * @return a future with {@link AuthenticationRequiredException} if the
     *         loginToken does not identified a logged-in session, or
     *         SessionAccessException if unable to access the underlying
     *         session due to an infrastructure malfunction
     */
    @MethodCallClass(LogoutCall.class)
    CompletableFuture<Void> logout(short serialVersion,
                                   LoginToken loginToken,
                                   long timeoutMillis);

    class LogoutCall extends AbstractCall<Void> {
        final LoginToken loginToken;
        public LogoutCall(final LoginToken loginToken) {
            this.loginToken = loginToken;
        }
        LogoutCall(final DataInput in, final short sv) throws IOException {
            this(readFastExternalOrNull(in, sv, LoginToken::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, loginToken);
        }
        @Override
        public Void readResponse(DataInput in, short serialVersion) {
            return null;
        }
        @Override
        public void writeResponse(Void response,
                                  DataOutput out,
                                  short serialVersion) {
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.LOGOUT;
        }
        @Override
        public CompletableFuture<Void>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncUserLogin service) {
            return service.logout(serialVersion, loginToken, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("loginToken=").append(loginToken);
        }
    }

    /* Other classes */

    /**
     * A method call that provides the callService method to perform the call
     * on the specified service.
     */
    interface ServiceMethodCall<R> extends MethodCall<R> {
        CompletableFuture<R> callService(short serialVersion,
                                         long timeoutMillis,
                                         AsyncUserLogin service);
    }

    /** A MethodCall that simplifies implementing the describeCall method. */
    abstract class AbstractCall<R> implements ServiceMethodCall<R> {
        @Override
        public String describeCall() {
            final StringBuilder sb = new StringBuilder();
            sb.append("AsyncUserLogin.")
                .append(getMethodOp())
                .append("[");
            describeParams(sb);
            sb.append("]");
            return sb.toString();
        }
        abstract void describeParams(StringBuilder sb);
    }
}
