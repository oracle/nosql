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

package oracle.kv.impl.async;

import static oracle.kv.impl.async.FutureUtils.checkedVoid;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.nio.channels.ServerSocketChannel;

import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * A wrapper class to replace SocketAddress that supports different protocols
 * and provides flexibility with address resolution of TCP addresses.
 *
 * @see #writeNetworkAddress FastExternalizable format
 */
public abstract class NetworkAddress extends SocketAddress
        implements FastExternalizable {

    /**
     * The prefix that should be added to a hostname so that the rest of the
     * hostname will be treated as the initial part of a pathname for a Unix
     * domain socket. A dash and the port will be appended to the path
     * specified in the hostname to create the full pathname. If the port is 0,
     * calling {@link #bind} will try random ports until a non-existent file is
     * found.
     */
    public static final String UNIX_DOMAIN_PREFIX = "unix_domain:";

    private static final int UNIX_DOMAIN_PREFIX_LENGTH =
        UNIX_DOMAIN_PREFIX.length();

    private static final long serialVersionUID = 1;
    static final int MAX_PORT = 65535;

    /** The type of a network address. */
    public enum NetworkAddressType implements FastExternalizable {
        INET(0, InetNetworkAddress::new),
        UNIX_DOMAIN(1, UnixDomainNetworkAddress::new);

        private static final NetworkAddressType[] VALUES = values();
        private final ReadFastExternal<NetworkAddress> reader;

        private NetworkAddressType(int ordinal,
                                   ReadFastExternal<NetworkAddress> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        public static NetworkAddressType
            readFastExternal(DataInput in,
                             @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown NetworkAddressType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        NetworkAddress readNetworkAddress(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    NetworkAddress() { }

    static void checkPort(int port) {
        if ((port < 0) || (port > MAX_PORT)) {
            throw new IllegalArgumentException("Illegal port: " + port);
        }
    }

    /**
     * Returns true if the host name specifies a TCP address, which is true
     * unless it starts with {@link #UNIX_DOMAIN_PREFIX}, ignoring case. If
     * true, creating a {@link NetworkAddress} with this host name will create
     * a {@link InetNetworkAddress}.
     *
     * @param host the host name
     * @return whether the host name specifies a TCP address
     */
    public static boolean isInetHostname(String host) {
        return !isUnixDomainHostname(host);
    }

    /**
     * Returns true if the host name specifies a Unix domain socket address,
     * which is true if it starts with {@link #UNIX_DOMAIN_PREFIX}, ignoring
     * case. If true, creating a {@link NetworkAddress} with this host name
     * will create a {@link UnixDomainNetworkAddress}.
     */
    public static boolean isUnixDomainHostname(String host) {
        checkNull("host", host);
        return host.regionMatches(true /* ignoreCase */,
                                  0 /* thisOffset */,
                                  UNIX_DOMAIN_PREFIX /* other */,
                                  0 /* otherOffset */,
                                  UNIX_DOMAIN_PREFIX_LENGTH /* otherLength */);
    }

    /**
     * Creates a network address with the specified host and port. If host
     * starts with {@link #UNIX_DOMAIN_PREFIX}, then a {@link
     * UnixDomainNetworkAddress} will be created with the specified host, with
     * the prefix removed, and port. Otherwise an {@link InetNetworkAddress}
     * will be created.
     */
    public static NetworkAddress createNetworkAddress(String host, int port) {
        return isUnixDomainHostname(host) ?
            new UnixDomainNetworkAddress(
                host.substring(UNIX_DOMAIN_PREFIX_LENGTH), port) :
            new InetNetworkAddress(host, port);
    }

    /**
     * Convert a SocketAddress to a NetworkAddress asynchronously. Completes to
     * an InetNetworkAddress for an InetSocketAddress, and a
     * UnixDomainNetworkAddress for a UnixDomainSocketAddress.
     *
     * <p>Throws the following exceptions, possibly wrapped in the {@code
     * CompletionException}, in the completable future.
     * <ul>
     * <li>Throws IOException if an I/O error occurs
     * <li>throws IllegalArgumentException if the socket address is not an
     * InetSocketAddress or a UnixDomainSocketAddress, or if the resolved
     * hostname does not match the socket type.
     * </ul>
     *
     * @param address the socket address
     * @return the network address
     */
    public static CompletableFuture<NetworkAddress>
        convert(SocketAddress address)
    {
        checkNull("address", address);
        if (address instanceof InetSocketAddress) {
            return InetNetworkAddress.create((InetSocketAddress) address);
        }
        try {
            return CompletableFuture.completedFuture(
                UnixDomainNetworkAddress.convertUnix(address));
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    /**
     * Converts a SocketAddress to a NetworkAddress without any blocking
     * operation such as attempting to query DNS to resolve an InetSocketAddress
     * to a host name. Returns an InetNetworkAddress for an InetSocketAddress,
     * and a UnixDomainNetworkAddress for a UnixDomainSocketAddress.
     *
     * The InetSocketAddress#getHostString does not do a reverse lookup, instead
     * a string form of the IP address is returned if the host name is not
     * available.
     *
     * @param address the socket address
     * @return the network address
     * @throws IllegalArgumentException if the socket address is not an
     *             InetSocketAddress or a UnixDomainSocketAddress, or if the
     *             address does not match the socket type.
     */
    public static NetworkAddress convertNow(SocketAddress address)
    {
        checkNull("address", address);
        if (address instanceof InetSocketAddress) {
            final InetSocketAddress addr = (InetSocketAddress) address;
            return new InetNetworkAddress(
                addr.getHostString(), addr.getPort());
        }
        return UnixDomainNetworkAddress.convertUnix(address);
    }

    /**
     * Resolves this address to the associated socket address asynchronously.
     *
     * <p>Throws the following exceptions, possibly wrapped in the {@code
     * CompletionException}, in the completable future.
     * <ul>
     * <li> Throws IOException if an I/O error occurs
     * </ul>
     *
     * @return the socket address
     */
    public abstract CompletableFuture<SocketAddress> resolveSocketAddress();

    /**
     * Opens and returns a socket channel using the appropriate protocol family
     * for this address. Use this method rather than {@link SocketChannel#open}
     * so that the channel is opened using the correct protocol family.
     *
     * @return the socket channel
     * @throws IOException if an I/O error occurs
     */
    public abstract SocketChannel openSocketChannel() throws IOException;

    /**
     * Opens and returns a server socket channel using the appropriate protocol
     * family for this address. Use this method rather than {@link
     * ServerSocketChannel#open} so that the channel is opened using the
     * correct protocol family.
     *
     * @return the socket channel
     * @throws IOException if an I/O error occurs
     */
    public abstract ServerSocketChannel openServerSocketChannel()
        throws IOException;

    /**
     * Close the specified server socket channel, removing the associated file
     * if the channel is Unix domain socket channel and is bound. Use this
     * method rather than {@link ServerSocketChannel#close} so that the file is
     * removed for Unix domain server socket channels.
     *
     * @param channel the channel to close
     * @throws IOException if an I/O error occurs
     */
    public static void closeServerSocketChannel(ServerSocketChannel channel)
        throws IOException
    {
        checkNull("channel", channel);
        if (!channel.isOpen()) {
            return;
        }
        final SocketAddress address;
        try {
            address = channel.getLocalAddress();
        } catch (ClosedChannelException e) {
            return;
        }
        if ((address != null) && !(address instanceof InetSocketAddress)) {
            UnixDomainNetworkAddress.removeServerSocketFile(address);
        }
        channel.close();
    }

    /**
     * Binds the specified server socket channel to this address as its local
     * address asynchronously, using the specified backlog. Use this method
     * rather than {@link ServerSocketChannel#bind} because the Unix domain
     * socket overloading is written to support anonymous ports using a
     * pathname in the standard socket directory, and to avoid blocking.
     *
     * <p>Throws the following exceptions, possibly wrapped in the {@code
     * CompletionException}, in the completable future.
     * <ul>
     * <li> Throws IllegalArgumentException if the channel does not support this
     * address's protocol family
     * <li> Throws IOException if an I/O error occurs
     * </ul>
     *
     * @param channel the channel to bind this address
     * @param backlog the maximum number of pending connections; uses the
     * default if less than 1
     */
    public CompletableFuture<Void> bind(ServerSocketChannel channel,
                                        int backlog)
    {
        return resolveSocketAddress()
            .thenAccept(
                checkedVoid(
                    (socket) -> channel.bind(socket, backlog)));
    }

    /**
     * Gets the host name. To support local network checks, this value
     * represents the network hostname, which is "localhost" for a Unix domain
     * socket factory. As a result, this method should not be used to create
     * error messages that describe a network address since the host name will
     * not identify a Unix socket domain address.
     *
     * @return the host name
     */
    public abstract String getHostName();

    /**
     * Gets the port.
     * @return the port
     */
    public abstract int getPort();

    /**
     * Returns whether this address represents a TCP address.
     *
     * @return whether this address represents a TCP address
     */
    public boolean isInetAddress() {
        return getType() == NetworkAddressType.INET;
    }

    /**
     * Returns whether this address represents a Unix domain socket address
     *
     * @return whether this address represents a Unix domain socket address
     */
    public boolean isUnixDomainAddress() {
        return getType() == NetworkAddressType.UNIX_DOMAIN;
    }

    /** Returns the type of this network address. */
    abstract NetworkAddressType getType();

    /**
     * Writes this network address to the output stream. Format:
     * <ol>
     * <li> ({@link NetworkAddressType}) {@link #getType type} // for {@code
     *      serialVersion}
     * <li> ({@link NetworkAddress}) {@link NetworkAddress this}
     * </ol>
     */
    public void writeNetworkAddress(DataOutput out, short serialVersion)
        throws IOException
    {
        getType().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    /**
     * Reads a network address from the input stream.
     */
    public static NetworkAddress readNetworkAddress(DataInput in, short sv)
        throws IOException
    {
        return NetworkAddressType.readFastExternal(in, sv)
            .readNetworkAddress(in, sv);
    }
}
