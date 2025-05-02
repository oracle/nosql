/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;
import static oracle.kv.util.TestUtils.checkAll;
import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutionException;

import oracle.kv.TestBase;

import org.junit.Test;

/** Test the {@link NetworkAddress} class. */
public class NetworkAddressTest extends TestBase {

    @SuppressWarnings("null")
    @Test
    public void testConvert() throws Exception {
        checkException(() -> NetworkAddress.convert(null).get(),
                       IllegalArgumentException.class,
                       "address must not be null");
        assertEquals(new InetNetworkAddress("host", 42),
                     NetworkAddress.convert(
                         new InetSocketAddress("host", 42)).get());
    }

    @Test
    public void testConvertUnix() throws Exception {
        assumeTrue("Requires Java 16 or later, found " + getJavaMajorVersion(),
                   getJavaMajorVersion() >= 16);

        final Class<?> cl = Class.forName("java.net.UnixDomainSocketAddress");
        final Method method = cl.getMethod("of", String.class);

        {
            SocketAddress address =
                (SocketAddress) method.invoke(null, "/a/b/c-42");
            assertEquals(new UnixDomainNetworkAddress("/a/b/c", 42),
                         NetworkAddress.convert(address).get());
        }

        {
            SocketAddress address = (SocketAddress) method.invoke(null, "");
            assertEquals(new UnixDomainNetworkAddress("", 0),
                         NetworkAddress.convert(address).get());
        }

        {
            SocketAddress address =
                (SocketAddress) method.invoke(null, "/a/b/c-abc");
            checkCause(
                checkException(() -> NetworkAddress.convert(address).get(),
                               ExecutionException.class),
                IllegalArgumentException.class,
                null);
        }

        {
            SocketAddress address =
                (SocketAddress) method.invoke(null, "/a/b/c777");
            checkCause(
                checkException(() -> NetworkAddress.convert(address).get(),
                               ExecutionException.class),
                IllegalArgumentException.class,
                "wrong format");
        }
    }

    @SuppressWarnings("null")
    @Test
    public void testCloseServerSocketChannel() throws Exception {
        checkException(() -> NetworkAddress.closeServerSocketChannel(null),
                       IllegalArgumentException.class,
                       "channel must not be null");

        ServerSocketChannel channel = ServerSocketChannel.open();
        NetworkAddress.closeServerSocketChannel(channel);
        assertFalse(channel.isOpen());
        NetworkAddress.closeServerSocketChannel(channel);

        channel = ServerSocketChannel.open();
        channel.bind(null);
        NetworkAddress.closeServerSocketChannel(channel);
        assertFalse(channel.isOpen());
        NetworkAddress.closeServerSocketChannel(channel);
    }

    @SuppressWarnings("null")
    @Test
    public void testCreateNetworkAddress() {
        checkException(() -> NetworkAddress.createNetworkAddress(null, 42),
                       IllegalArgumentException.class,
                       "host must not be null");
        assertEquals(new InetNetworkAddress("host", 42),
                     NetworkAddress.createNetworkAddress("host", 42));
        assertEquals(new UnixDomainNetworkAddress("/a/b/c", 42),
                     NetworkAddress.createNetworkAddress(
                         "unix_domain:/a/b/c", 42));
    }

    @SuppressWarnings("null")
    @Test
    public void testIsUnixDomainHostname() {
        checkException(() -> NetworkAddress.isUnixDomainHostname(null),
                       IllegalArgumentException.class,
                       "host must not be null");
        assertTrue(NetworkAddress.isUnixDomainHostname("unix_domain:/a/b/c"));
        assertTrue(NetworkAddress.isUnixDomainHostname("Unix_Domain:/a/b/c"));
        assertTrue(NetworkAddress.isUnixDomainHostname("unix_domain:"));
        assertFalse(NetworkAddress.isUnixDomainHostname("something:/a/b/c"));
        assertFalse(NetworkAddress.isUnixDomainHostname("localhost"));
    }

    @Test
    public void testSerialVersion() {
        checkAll(serialVersionChecker(
                     NetworkAddress.createNetworkAddress("localhost", 5000),
                     0x1d4639340ecae605L));
    }
}
