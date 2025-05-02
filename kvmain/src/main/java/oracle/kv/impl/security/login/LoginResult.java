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

import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerializationUtil;

/**
 * LoginResult is the result of a login operation.  It currently contains only
 * a single field, but it is expected that later versions will expand on this.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class LoginResult implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    private LoginToken loginToken;

    /**
     * The types of login results.
     */
    public enum LoginResultType implements FastExternalizable {
        LOGIN_RESULT(0, LoginResult::new),
        KERBEROS_LOGIN_RESULT(1, KerberosLoginResult::new);

        private static final LoginResultType[] VALUES = values();
        private final ReadFastExternal<LoginResult> reader;

        LoginResultType(final int ordinal,
                        final ReadFastExternal<LoginResult> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /** Reads a login result type. */
        static
        LoginResultType readFastExternal(DataInput in,
                                         @SuppressWarnings("unused")
                                         short serialVersion)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown LoginResultType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        /** Reads the associated login result. */
        LoginResult readLoginResult(DataInput in,
                                           short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    /**
     * Constructor.
     */
    public LoginResult() {
        this.loginToken = null;
    }

    public LoginResult(LoginToken loginToken) {
        this.loginToken = loginToken;
    }

    public LoginResult setLoginToken(LoginToken token) {
        this.loginToken = token;
        return this;
    }

    /* for FastExternalizable */
    public LoginResult(DataInput in, short serialVersion)
        throws IOException {

        final boolean hasToken = in.readBoolean();
        if (hasToken) {
            loginToken = new LoginToken(in, serialVersion);
        } else {
            loginToken = null;
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeFastExternalOrNull LoginToken or
     *      null}) {@link #getLoginToken loginToken}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeFastExternalOrNull(out, serialVersion, loginToken);
    }

    /** Returns the login result type of this object. */
    protected LoginResultType getResultType() {
        return LoginResultType.LOGIN_RESULT;
    }

    public LoginToken getLoginToken() {
        return loginToken;
    }

    /** Writes an instance, possibly a subclass, of LoginResult. */
    void writeLoginResult(DataOutput out, short serialVersion)
        throws IOException
    {
        getResultType().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    /** Reads an instance, possibly a subclass, of LoginResult. */
    static LoginResult readLoginResult(DataInput in, short sv)
        throws IOException
    {
        return LoginResultType.readFastExternal(in, sv)
            .readLoginResult(in, sv);
    }
}
