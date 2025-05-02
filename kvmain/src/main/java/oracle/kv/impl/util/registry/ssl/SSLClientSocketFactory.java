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

package oracle.kv.impl.util.registry.ssl;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.registry.ClientSocketFactory;

/**
 * An implementation of RMIClientSocketFactory that permits configuration of
 * the following Socket timeouts:
 * <ol>
 * <li>Connection timeout</li>
 * <li>Read timeout</li>
 * </ol>
 * These are set to allow clients to become aware of possible network problems
 * in a timely manner.
 * <p>
 * CSFs with the appropriate timeouts for a registry are specified on the
 * client side.
 * <p>
 * CSFs for service requests (unrelated to the registry) have default values
 * provided by the server that can be overridden by the client as below:
 * <ol>
 * <li>Server side timeout parameters are set via the KVS admin as policy
 * parameters</li>
 * <li>Client side timeout parameters are set via {@link KVStoreConfig}. When
 * present, they override the parameters set at the server level.</li>
 * </ol>
 * <p>
 * This class leverages the underlying ClientSocketFactory class, which
 * performs all of the heavy lifting for socket timeout handling.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class SSLClientSocketFactory
    extends ClientSocketFactory {

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    public enum Use implements FastExternalizable {
        USER(0),
        TRUSTED(1);

        private static final Use[] VALUES = values();

        private Use(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        private static Use readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for Use: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #USER}=0,
         *      {@link #TRUSTED}=1
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    private static final long serialVersionUID = 1L;

    /* SSLControl for use when clientUse = TRUSTED */
    private static volatile SSLControl trustedSSLControl;

    /**
     * SSLControl for use when clientUse = USER and either there is no known
     * store context or client ID, or there is no SSLControl associated with
     * the store context.
     */
    private static volatile SSLControl defaultUserSSLControl;

    /** Map from store name and client ID to SSLControl */
    private static final Map<Object, SSLControl> userSSLControlMap =
        new ConcurrentHashMap<>();

    /* The KVStore instance with which this is associated */
    private final String kvStoreName;

    /* The intended use for this factory */
    private final Use clientUse;

    /**
     * Creates the client socket factory for use by a Client.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param kvStoreName the name of the KVStore instance
     * @param clientId the client ID or null in a server environment
     */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs,
                                  String kvStoreName,
                                  ClientId clientId) {
        this(name, connectTimeoutMs, readTimeoutMs, kvStoreName, clientId,
             Use.USER);
    }

    /**
     * Creates a client socket factory for client access.  This is a
     * convenience version that supplies the use as USER.  Currently used only
     * for testing.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs) {
        this(name, connectTimeoutMs, readTimeoutMs, Use.USER);
    }

    /**
     * Creates the client socket factory within the server.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param clientUse The intended use for the client socekt factory
     */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs,
                                  Use clientUse) {
        this(name, connectTimeoutMs, readTimeoutMs, null /* kvStoreName */,
             null /* clientId */, clientUse);
    }

    /** Specify csfId, for testing */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs,
                                  String kvStoreName,
                                  ClientId clientId,
                                  Use clientUse,
                                  long csfId) {
        super(name, connectTimeoutMs, readTimeoutMs, clientId, csfId);
        this.kvStoreName = kvStoreName;
        this.clientUse = clientUse;
        checkValidFields();
    }

    private SSLClientSocketFactory(String name,
                                   int connectTimeoutMs,
                                   int readTimeoutMs,
                                   String kvStoreName,
                                   ClientId clientId,
                                   Use clientUse) {
        super(name, connectTimeoutMs, readTimeoutMs, clientId);
        this.kvStoreName = kvStoreName;
        this.clientUse = clientUse;
        checkValidFields();
    }

    private void checkValidFields() {
        checkNull("clientUse", clientUse);
    }

    /**
     * Creates an using data from an input stream.
     */
    public SSLClientSocketFactory(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        kvStoreName = readString(in, serialVersion);
        clientUse = Use.readFastExternal(in, serialVersion);
    }

    /**
     * Writes the object to the output stream.  Format:
     * <ol>
     * <li> ({@link ClientSocketFactory}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) <i>kvStoreName</i>
     * <li> ({@link Use}) <i>clientUse</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, kvStoreName);
        clientUse.writeFastExternal(out, serialVersion);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
            ((kvStoreName == null) ? 0 : kvStoreName.hashCode()) +
            clientUse.hashCode();
        return result;
    }

    @Override
    protected void getToStringBody(StringBuilder sb) {
        super.getToStringBody(sb);
        sb.append(" kvStoreName=").append(kvStoreName);
        sb.append(" clientUse=").append(clientUse);
        sb.append(" control=");
        try {
            sb.append(getSSLControl());
        } catch (IOException e) {
            sb.append(e);
        }
    }

    @Override
    public boolean equals(Object obj) {

        if (!super.equals(obj)) {
            return false;
        }

        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SSLClientSocketFactory)) {
            return false;
        }

        final SSLClientSocketFactory other = (SSLClientSocketFactory) obj;

        if (!clientUse.equals(other.clientUse)) {
            return false;
        }

        if (kvStoreName == null) {
            if (other.kvStoreName != null) {
                return false;
            }
        } else if (!kvStoreName.equals(other.kvStoreName)) {
            return false;
        }
        return true;
    }

    /**
     * @see java.rmi.server.RMIClientSocketFactory#createSocket
     */
    @Override
    public Socket createSocket(String host, int port)
        throws IOException {

        final Socket sock = createTimeoutSocket(host, port);

        boolean ok = false;
        try {
            final SSLControl control = getSSLControl();
            final SSLContext context = (control != null) ?
                control.sslContext() :
                SSLContext.getDefault();

            final SSLSocket sslSock = (SSLSocket)
                context.getSocketFactory().createSocket(
                    sock, host, port, true); /* autoclose */

            if (control != null) {
                control.applySSLParameters(sslSock);
            }

            /* Start the handshake.  This is completed synchronously */
            sslSock.startHandshake();
            if (control != null && control.hostVerifier() != null) {
                if (!control.hostVerifier().
                    verify(host, sslSock.getSession())) {
                    throw new IOException(
                        "SSL connection to host " + host + " is not valid.");
                }
            }
            ok = true;
            return sslSock;
        } catch (NoSuchAlgorithmException nsae) {
            /*
             * This is very unlikely to occur in a reasonably configured system
             */
            throw new IOException("Unknown algorithm", nsae);
        } finally {
            if (!ok) {
                try {
                    sock.close();
                } catch (IOException ioe) {
                }
            }
        }
    }

    /**
     * Returns the SSLControl object for this factory, which may be null if
     * client use equals Use.USER.
     *
     * @return the SSLControl object or null
     * @throws IOException if clientUse is Use.TRUSTED and no SSLControl object
     * is available
     */
    private SSLControl getSSLControl()
        throws IOException {

        SSLControl control = null;
        if (Use.TRUSTED.equals(clientUse)) {
            control = trustedSSLControl;
            if (control == null) {
                throw new IOException(
                    "Cannot create TRUSTED client SSLSocket with empty " +
                    "trustedSSLControl");
            }
        } else {
            /* First look for a store- and client-ID-specific SSLControl */
            if (clientId != null) {
                control = userSSLControlMap.get(clientId);
            }
            if ((control == null) && (kvStoreName != null)) {
                control = userSSLControlMap.get(kvStoreName);
            }
            /*
             * If no store-specific SSLControl exists consider a default
             * SSLControl.
             */
            if (control == null) {
                control = defaultUserSSLControl;
            }
        }
        return control;
    }

    /*
     * Used by unit test
     */
    public static void clearUserSSLControlMap() {
        userSSLControlMap.clear();
    }

    /**
     * Returns the size of userSSLControlMap. For testing.
     */
    public static int getUserSSLControlMapSize() {
        return userSSLControlMap.size();
    }

    /**
     * Remove any entry in userSSLControlMap for a client ID associated with a
     * KVStoreImpl that is being shutdown.
     */
    static void clearUserSSLControlMap(ClientId clientId) {
        userSSLControlMap.remove(clientId);
    }

    /**
     * Set the Trusted SSLControl
     */
    static void setTrustedControl(SSLControl trustedControl) {
        trustedSSLControl = trustedControl;
    }

    /**
     * Set the User SSLControl
     */
    static void setUserControl(SSLControl userControl,
                               String storeContext,
                               ClientId clientId) {
        if (storeContext != null) {
            userSSLControlMap.put(storeContext, userControl);
            userSSLControlMap.put(clientId, userControl);
        }
        /* Always set the default */
        defaultUserSSLControl = userControl;
    }

    private void readObject(ObjectInputStream in)
        throws ClassNotFoundException, IOException {

        in.defaultReadObject();
        try {
            checkValidFields();
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid fields: " + e.getMessage(), e);
        }
    }

    @Override
    public EndpointConfigBuilder getEndpointConfigBuilder()
        throws IOException {

        SSLControl sslControl = getSSLControl();
        if (sslControl == null) {
            /* Use a default SSLControl since that is how we know to use SSL */
            final SSLContext defaultContext;
            try {
                defaultContext = SSLContext.getDefault();
            } catch (NoSuchAlgorithmException e) {
                throw new IOException("Problem creating SSLControl: " + e, e);
            }
            sslControl =
                new SSLControl(defaultContext.getDefaultSSLParameters(),
                               defaultContext,
                               null /* sslHostVerifier */,
                               null /* sslAuthenticator */);
        }
        return super.getEndpointConfigBuilder()
            .sslControl(sslControl);
    }
}

