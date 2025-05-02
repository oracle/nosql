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

package oracle.kv.impl.async.registry;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A service endpoint that includes the information needed to establish dialogs
 * with a remote server that implements an asynchronous interface.
 *
 * @see ServiceRegistryAPI
 * @see #writeFastExternal FastExternalizable format
 */
public class ServiceEndpoint implements FastExternalizable {

    /**
     * Identifies the type of socket factory.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum SocketFactoryType implements FastExternalizable {
        CLEAR(0),
        SSL(1);

        private static final SocketFactoryType[] VALUES = values();

        SocketFactoryType(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        public static SocketFactoryType readFastExternal(
            DataInput in,
            @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException(
                    "Wrong value for SocketFactoryType: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>value</i> // {@link #CLEAR}=0, {@link #SSL}=1
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    private final NetworkAddress address;
    private final DialogType dialogType;
    private final ClientSocketFactory clientSocketFactory;

    /**
     * Creates a service endpoint.
     *
     * @param address the network address
     * @param dialogType the dialogType
     * @param clientSocketFactory the client socket factory
     */
    public ServiceEndpoint(NetworkAddress address,
                           DialogType dialogType,
                           ClientSocketFactory clientSocketFactory) {
        this.address = checkNull("address", address);
        this.dialogType = checkNull("dialogType", dialogType);
        this.clientSocketFactory =
            checkNull("clientSocketFactory", clientSocketFactory);
    }

    /**
     * Creates a service endpoint from an input stream.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialized form
     */
    public ServiceEndpoint(DataInput in, short serialVersion)
        throws IOException {

        address = NetworkAddress.readNetworkAddress(in, serialVersion);
        dialogType = DialogType.readFastExternal(in, serialVersion);
        final SocketFactoryType socketFactoryType =
            SocketFactoryType.readFastExternal(in, serialVersion);
        switch (socketFactoryType) {
        case CLEAR:
            clientSocketFactory =
                new ClearClientSocketFactory(in, serialVersion);
            break;
        case SSL:
            clientSocketFactory =
                new SSLClientSocketFactory(in, serialVersion);
            break;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link NetworkAddress#writeNetworkAddress})
     *      {@link #getNetworkAddress address}
     * <li> ({@link DialogType}) {@link #getDialogType dialogType}
     * <li> ({@link SocketFactoryType}) <i>socket factory type</i>
     * <li> ({@link ClientSocketFactory}) {@link #getClientSocketFactory
     *      clientSocketFactory}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        address.writeNetworkAddress(out, serialVersion);
        dialogType.writeFastExternal(out, serialVersion);
        getSocketFactoryType().writeFastExternal(out, serialVersion);
        clientSocketFactory.writeFastExternal(out, serialVersion);
    }

    /**
     * Returns type of socket factory supported by the endpoint.
     *
     * @return the socket factory type
     */
    private SocketFactoryType getSocketFactoryType() {
        return (clientSocketFactory instanceof SSLClientSocketFactory) ?
            SocketFactoryType.SSL :
            SocketFactoryType.CLEAR;
    }

    /**
     * Returns the network address of the endpoint.
     *
     * @return the network address
     */
    public NetworkAddress getNetworkAddress() {
        return address;
    }

    /**
     * Returns the dialog type of the endpoint.
     *
     * @return the dialog type
     */
    public DialogType getDialogType() {
        return dialogType;
    }

    /**
     * Returns the client socket factory to use for connecting to the endpoint.
     *
     * @return the client socket factory
     */
    public ClientSocketFactory getClientSocketFactory() {
        return clientSocketFactory;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if (object instanceof ServiceEndpoint) {
            final ServiceEndpoint endpoint = (ServiceEndpoint) object;
            return address.equals(endpoint.address) &&
                dialogType.equals(endpoint.dialogType) &&
                clientSocketFactory.equals(endpoint.clientSocketFactory);
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 7;
        int value = 11;
        value = (value * prime) + address.hashCode();
        value = (value * prime) + dialogType.hashCode();
        value = (value * prime) + clientSocketFactory.hashCode();
        return value;
    }

    @Override
    public String toString() {
        return "ServiceEndpoint[address=" + address +
            " dialogType=" + dialogType +
            " clientSocketFactory=" + clientSocketFactory + "]";
    }
}
