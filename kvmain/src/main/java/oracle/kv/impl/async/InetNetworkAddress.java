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

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.nio.channels.ServerSocketChannel;

import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper class to replace the InetSocketAddress for dealing with the
 * IP-address-change problem in the cloud.
 *
 * The problem is that InetAddress resolves a hostname to IP address upon
 * creation. After that, when we use the InetAddress to connect, it will use
 * the resolved IP address. When in the cloud, the IP address of a hostname is
 * susceptible to change and thus the already-resolved
 * InetAddress/InetSocketAddress will be invalid after the change.
 *
 * The class simply constructs a new InetAddress/InetSocketAddress when one is
 * needed and this will do a new address resolution at the point it is
 * required. The resolution part of the InetAddress uses a cache, so this
 * should not have too much performance impact.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class InetNetworkAddress extends NetworkAddress {

    private static final long serialVersionUID = 1;

    /**
     * An InetSocketAddress with a wildcard address and port 0. Useful as a
     * default value when a non-null network address is needed.
     */
    public static final InetSocketAddress ANY_LOCAL =
        new InetSocketAddress(0);

    /**
     * A network address with a wildcard address and port 0. Useful as a
     * default value when a non-null network address is needed.
     */
    public static final InetNetworkAddress ANY_LOCAL_ADDRESS =
        new InetNetworkAddress(ANY_LOCAL.getHostString(), 0);

    private final String hostname;
    private final int port;

    /**
     * Creates a network address from a host name and a port.
     *
     * @param hostname the host name
     * @param port the port
     * @throws IllegalArgumentException if hostname represents a Unix domain
     * address
     */
    public InetNetworkAddress(String hostname, int port) {
        checkNull("hostname", hostname);
        if (isUnixDomainHostname(hostname)) {
            throw new IllegalArgumentException(
                "Unexpected Unix domain hostname: " + hostname);
        }
        this.hostname = hostname;
        this.port = port;
        checkPort(port);
    }

    /**
     * Creates a network address asynchronously from an IP address and a port.
     *
     * <p>Throws the following exceptions, possibly wrapped in the {@code
     * CompletionException}, in the completable future:
     * <ul>
     * <li> Throws IllegalArgumentException if hostname represents a Unix
     * domain address
     * </ul>
     *
     * @param address the IP address
     * @param port the port
     */
    static CompletableFuture<NetworkAddress>
        create(InetAddress address, int port)
    {
        checkNull("address", address);
        checkPort(port);
        /*
         * Use an async call and schedule it in the backup service since the
         * InetSocketAddress.getHostName can block on reverse DNS resolution.
         */
        return CompletableFuture.supplyAsync(
            () -> {
                final String hostname = address.getHostName();
                if (isUnixDomainHostname(hostname)) {
                    throw new IllegalArgumentException(
                        "Unix domain hostname: " + hostname);
                }
                return new InetNetworkAddress(hostname, port);
            },
            AsyncRegistryUtils.getEndpointGroup().getBackupExecService());
    }

    static CompletableFuture<NetworkAddress>
        create(InetSocketAddress address)
    {
        checkNull("address", address);
        /*
         * Use an async call and schedule it in the backup service since the
         * InetSocketAddress.getHostName can block on reverse DNS resolution.
         */
        return CompletableFuture.supplyAsync(
            () -> {
                final String hostname = address.getHostName();
                if (isUnixDomainHostname(hostname)) {
                    throw new IllegalArgumentException(
                        "Unix domain hostname: " + hostname);
                }
                return new InetNetworkAddress(hostname, address.getPort());
            },
            AsyncRegistryUtils.getEndpointGroup().getBackupExecService());
    }

    /**
     * Initializes an instance from an input stream.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialized form
     */
    InetNetworkAddress(DataInput in, short serialVersion)
        throws IOException {

        hostname = readNonNullString(in, serialVersion);
        if (isUnixDomainHostname(hostname)) {
            throw new IOException("Unexpected Unix domain hostname: " +
                                  hostname);
        }
        port = in.readUnsignedShort();
        try {
            checkPort(port);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid field: " + e.getMessage(), e);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString String}) {@link
     *      #getHostName hostname}
     * <li> ({@link DataOutput#writeShort short}) {@link #getPort port}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullString(out, serialVersion, hostname);
        out.writeShort(port);
    }

    /**
     * Gets the IP address.
     *
     * The method will do a hostname-IP resolution every time the method is
     * called.
     *
     * @return the IP address
     */
    public InetAddress getInetAddress() throws UnknownHostException {
        return InetAddress.getByName(hostname);
    }

    /**
     * Resolves to a socket address asynchronously.
     *
     * <p>The method will do a hostname-IP resolution every time the method is
     * called.
     *
     * @return the socket address
     */
    @Override
    public CompletableFuture<SocketAddress> resolveSocketAddress() {
        /*
         * Use an async call and schedule it in the backup service since the
         * InetSocketAddress hostname constructor can block on DNS resolution.
         */
        return CompletableFuture.supplyAsync(
            () -> new InetSocketAddress(hostname, port),
            AsyncRegistryUtils.getEndpointGroup().getBackupExecService());
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        return SocketChannel.open();
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }

    /**
     * Gets the host name.
     *
     * @return the host name
     */
    @Override
    public String getHostName() {
         return hostname;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    @Override
    public int getPort() {
        return port;
    }

    @Override
    NetworkAddressType getType() {
        return NetworkAddressType.INET;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if ((obj == null) || !(obj instanceof InetNetworkAddress)) {
            return false;
        }
        final InetNetworkAddress addr = ((InetNetworkAddress) obj);
        if (!addr.hostname.equals(hostname)) {
            return false;
        }
        return addr.port == port;
    }

    @Override
    public int hashCode() {
        return hostname.hashCode() *31 + port;
    }

    @Override
    public String toString() {
         return hostname + ":" + port;
    }
}
