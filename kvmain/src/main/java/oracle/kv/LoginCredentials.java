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

import oracle.kv.impl.security.ClientProxyCredentials;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.KerberosInternalCredentials;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * The common interface of KVStore credential class implementations.
 * Applications can supply objects that implement this interface in order to
 * authenticate a user when accessing a KVStore instance as an alternative to
 * specifying authentication information in login properties.
 *
 * @since 3.0
 */
public interface LoginCredentials extends FastExternalizable {

    /**
     * Identifies all classes that implement LoginCredentials. Use an interface
     * separate from the enumeration of standard types so tests can add new
     * types.
     *
     * @hidden For internal use
     */
    interface LoginCredentialsType {

        /** Returns the integer value associated with the type. */
        int getIntValue();

        /**
         * Reads the login credentials associated with this type from the
         * stream.
         */
        LoginCredentials readLoginCredentials(DataInput in,
                                              short serialVersion)
            throws IOException;

        /** Reads the integer value of a login credentials type. */
        static int readIntValue(DataInput in,
                                @SuppressWarnings("unused") short sv)
            throws IOException
        {
            return in.readByte();
        }

        /** Writes the integer value of this login credentials type. */
        default void writeIntValue(DataOutput out,
                                   @SuppressWarnings("unused") short sv)
            throws IOException
        {
            out.writeByte(getIntValue());
        }
    }

    /**
     * Finds a LoginCredentialsType from the associated integer value.
     *
     * @hidden For internal use
     */
    interface LoginCredentialsTypeFinder {

        /**
         * Returns the LoginCredentialsType associated with the specified
         * value, or null if none is found.
         */
        LoginCredentialsType getLoginCredentialsType(int intValue);
    }

    /**
     * Identifies all the standard login credential types.
     *
     * @hidden For internal use
     */
    enum StdLoginCredentialsType implements LoginCredentialsType {
        BASIC_OATH(0, BasicOAuthCredentials::new),
        CLIENT_PROXY(1, ClientProxyCredentials::new),
        IDCS_OAUTH(2, IDCSOAuthCredentials::new),
        KERBEROS(3, KerberosCredentials::new),
        KERBEROS_CLIENT(4, KerberosClientCreds::new),
        KERBEROS_INTERNAL(5, KerberosInternalCredentials::new),
        PASSWORD(6, PasswordCredentials::new),
        PROXY(7, ProxyCredentials::new);

        private static final StdLoginCredentialsType[] VALUES = values();

        private final ReadFastExternal<LoginCredentials> reader;

        StdLoginCredentialsType(final int ordinal,
                                final ReadFastExternal<LoginCredentials>
                                reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Returns the StdLoginCredentialsType with the specified ordinal, or
         * null if not found.
         */
        static StdLoginCredentialsType valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                return null;
            }
        }

        @Override
        public int getIntValue() {
            return ordinal();
        }

        @Override
        public LoginCredentials readLoginCredentials(final DataInput in,
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

    /**
     * Identifies the user owning the credentials.
     *
     * @return the name of the user for which the credentials belong.
     */
    String getUsername();

    /**
     * Returns the credentials type of these credentials.
     *
     * @hidden For internal use
     */
    LoginCredentialsType getLoginCredentialsType();

    /**
     * Reads a LoginCredentials from the input stream.
     *
     * @hidden For internal use
     */
    static LoginCredentials readLoginCredentials(DataInput in,
                                                 short serialVersion)
        throws IOException
    {
        final LoginCredentialsType credentialsType =
            LoginCredentialsTypeFinders.findLoginCredentialsType(
                LoginCredentialsType.readIntValue(in, serialVersion));
        return credentialsType.readLoginCredentials(in, serialVersion);
    }

    /**
     * Writes this object as a LoginCredentials to the output stream.
     *
     * @hidden For internal use
     */
    default void writeLoginCredentials(DataOutput out, short serialVersion)
        throws IOException
    {
        getLoginCredentialsType().writeIntValue(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }
}
