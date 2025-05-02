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
import static oracle.kv.util.TestUtils.tryHandleFutureGetException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import oracle.kv.TestBase;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/** Test the {@link UnixDomainNetworkAddress} class. */
public class UnixDomainNetworkAddressTest extends TestBase {

    @SuppressWarnings("null")
    @Test
    public void testConstructor() {
        checkException(() -> new UnixDomainNetworkAddress(null, 42),
                       IllegalArgumentException.class,
                       "pathname must not be null");
        checkException(() -> new UnixDomainNetworkAddress("/a/b/c", -1),
                       IllegalArgumentException.class,
                       "Illegal port");
        checkException(() -> new UnixDomainNetworkAddress("/a/b/c", 65536),
                       IllegalArgumentException.class,
                       "Illegal port");
        checkException(() -> new UnixDomainNetworkAddress("", 42),
                       IllegalArgumentException.class,
                       "must not be empty");

        UnixDomainNetworkAddress address =
            new UnixDomainNetworkAddress("/a/b/c", 42);
        assertEquals("/a/b/c", address.getPathname());
        assertEquals(42, address.getPort());

        address = new UnixDomainNetworkAddress("/a/b/c", 0);
        assertEquals("/a/b/c", address.getPathname());
        assertEquals(0, address.getPort());

        address = new UnixDomainNetworkAddress("", 0);
        assertEquals("", address.getPathname());
        assertEquals(0, address.getPort());
    }

    @Test
    public void testConvertUnix() throws Exception {
        if (getJavaMajorVersion() < 16) {
            final SocketAddress address =
                new InetSocketAddress("localhost", 1);
            checkException(() -> UnixDomainNetworkAddress.convertUnix(address),
                           IllegalStateException.class,
                           "requires Java 16");
            return;
        }

        final Class<?> cl = Class.forName("java.net.UnixDomainSocketAddress");
        final Method method = cl.getMethod("of", String.class);

        {
            SocketAddress address =
                (SocketAddress) method.invoke(null, "/a/b/c");
            checkException(() -> UnixDomainNetworkAddress.convertUnix(address),
                           IllegalArgumentException.class,
                           "wrong format");
        }

        {
            SocketAddress address =
                (SocketAddress) method.invoke(null, "/a/b/c-abc");
            checkException(() -> UnixDomainNetworkAddress.convertUnix(address),
                           IllegalArgumentException.class);
        }

        {
            SocketAddress address = (SocketAddress) method.invoke(null, "-42");
            checkException(() -> UnixDomainNetworkAddress.convertUnix(address),
                           IllegalArgumentException.class,
                           "wrong format");
        }

        SocketAddress address = (SocketAddress) method.invoke(null, "");
        UnixDomainNetworkAddress networkAddress =
            UnixDomainNetworkAddress.convertUnix(address);
        assertEquals("", networkAddress.getPathname());
        assertEquals(0, networkAddress.getPort());

        address = (SocketAddress) method.invoke(null, "/a/b/c-42");
        networkAddress = UnixDomainNetworkAddress.convertUnix(address);
        assertEquals("/a/b/c", networkAddress.getPathname());
        assertEquals(42, networkAddress.getPort());
    }

    @Test
    public void testResolveSocketAddress() throws Exception {
        if (getJavaMajorVersion() < 16) {
            UnixDomainNetworkAddress networkAddress =
                new UnixDomainNetworkAddress("/a/b/c", 1234);
            checkException(
                tryHandleFutureGetException(
                    () -> networkAddress.resolveSocketAddress().get()),
                IllegalStateException.class,
                "requires Java 16");
            return;
        }

        final Class<?> cl = Class.forName("java.net.UnixDomainSocketAddress");
        final Method getPath = cl.getMethod("getPath");

        {
            UnixDomainNetworkAddress networkAddress =
                new UnixDomainNetworkAddress("/a/b/c", 1234);
            SocketAddress socketAddress =
                networkAddress.resolveSocketAddress().get();
            assertTrue("Checking class of " + socketAddress,
                       cl.isInstance(socketAddress));
            assertEquals("/a/b/c-1234",
                         getPath.invoke(socketAddress).toString());
        }

        {
            UnixDomainNetworkAddress networkAddress =
                new UnixDomainNetworkAddress("/a/b/c", 0);
            SocketAddress socketAddress =
                networkAddress.resolveSocketAddress().get();
            assertTrue("Checking class of " + socketAddress,
                       cl.isInstance(socketAddress));
            assertEquals("/a/b/c-0", getPath.invoke(socketAddress).toString());
        }

        {
            UnixDomainNetworkAddress networkAddress =
                new UnixDomainNetworkAddress("", 0);
            SocketAddress socketAddress =
                networkAddress.resolveSocketAddress().get();
            assertTrue("Checking class of " + socketAddress,
                       cl.isInstance(socketAddress));
            assertEquals("", getPath.invoke(socketAddress).toString());
        }
    }

    @Test
    public void testOpenSocketChannel() throws Exception {
        final UnixDomainNetworkAddress address =
            new UnixDomainNetworkAddress("a", 42);

        if (getJavaMajorVersion() < 16) {
            checkException(() -> address.openSocketChannel(),
                           IllegalStateException.class,
                           "requires Java 16");
            return;
        }

        address.openSocketChannel().close();
    }

    @Test
    public void testOpenServerSocketChannel() throws Exception {
        final UnixDomainNetworkAddress address =
            new UnixDomainNetworkAddress("a", 42);

        if (getJavaMajorVersion() < 16) {
            checkException(() -> address.openServerSocketChannel(),
                           IllegalStateException.class,
                           "requires Java 16");
            return;
        }

        address.openServerSocketChannel().close();
    }

    @Test
    public void testBind() throws Exception {

        final File dir = Files.createTempDirectory("tstbnd").toFile();
        tearDowns.add(() -> FileUtils.deleteDirectory(dir));
        final UnixDomainNetworkAddress address =
            new UnixDomainNetworkAddress(dir + "/a", 42);

        final ServerSocketChannel inetChannel = ServerSocketChannel.open();

        if (getJavaMajorVersion() < 16) {
            checkException(
                tryHandleFutureGetException(
                    () -> address.bind(inetChannel, 0).get()),
                IllegalStateException.class,
                "requires Java 16");
            return;
        }

        checkException(
            tryHandleFutureGetException(
                () -> address.bind(inetChannel, 0).get()),
            UnsupportedAddressTypeException.class);

        {
            ServerSocketChannel channel = address.openServerSocketChannel();
            address.bind(channel, 0);
            channel.close();
        }

        {
            ServerSocketChannel channel = address.openServerSocketChannel();
            checkException(
                tryHandleFutureGetException(
                    () -> address.bind(channel, 0).get()),
                BindException.class);
        }

        {
            ServerSocketChannel channel = address.openServerSocketChannel();
            InetNetworkAddress inetAddress =
                new InetNetworkAddress("localhost", 0);
            checkException(
                tryHandleFutureGetException(
                    () -> inetAddress.bind(channel, 0).get()),
                UnsupportedAddressTypeException.class);
        }

        final File file = new File(dir, "a-42");
        assertTrue(file.delete());

        final UnixDomainNetworkAddress unnamedAddress =
            new UnixDomainNetworkAddress("", 0);
        ServerSocketChannel channel = unnamedAddress.openServerSocketChannel();
        address.bind(channel, 0);
        channel.close();

        final UnixDomainNetworkAddress addressZero =
            new UnixDomainNetworkAddress(dir + "/a", 0);
        channel = addressZero.openServerSocketChannel();
        addressZero.bind(channel, 0);
        SocketAddress socketAddress = channel.getLocalAddress();
        UnixDomainNetworkAddress resultAddress =
            UnixDomainNetworkAddress.convertUnix(socketAddress);
        assertEquals(dir + "/a", resultAddress.getPathname());
        int port = resultAddress.getPort();
        assertTrue("Port should not be zero, found: " + resultAddress.getPort(),
                   resultAddress.getPort() != 0);

        channel = addressZero.openServerSocketChannel();
        addressZero.bind(channel, 0);
        socketAddress = channel.getLocalAddress();
        resultAddress = UnixDomainNetworkAddress.convertUnix(socketAddress);
        assertTrue("Port should be different",
                   port != resultAddress.getPort());

        FileUtils.deleteDirectory(dir);
    }

    /**
     * Test that a failure to bind a Unix domain socket because the path is too
     * long gets reported properly. [KVSTORE-1478]
     */
    @Test
    public void testBindPathTooLong() throws Exception {
        if (getJavaMajorVersion() < 16) {
            System.out.println("Skipped Unix domain sockets with Java " +
                               getJavaMajorVersion());
            return;
        }

        /*
         * Unix domain sockets seem to only support about 100 character
         * pathnames: use something much bigger to be sure.
         */
        final char[] chars = new char[500];
        Arrays.fill(chars, 'a');
        final String longPath =
            TestUtils.getTestDir() + "testBindPathTooLong/" +
            new String(chars);
        {
            final UnixDomainNetworkAddress longAddress =
                new UnixDomainNetworkAddress(longPath, 42);
            final ServerSocketChannel channel =
                longAddress.openServerSocketChannel();
            checkCause(
                checkException(() -> longAddress.bind(channel, 0).get(),
                               ExecutionException.class),
                IOException.class, "path too long");
        }
        {
            final UnixDomainNetworkAddress longAddress =
                new UnixDomainNetworkAddress(longPath, 0);
            final ServerSocketChannel channel =
                longAddress.openServerSocketChannel();
            checkCause(
                checkException(() -> longAddress.bind(channel, 0).get(),
                               ExecutionException.class),
                IOException.class,
                "path too long");
        }
    }

    /* So we can test equals with an unlikely value */
    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals() {
        final List<Supplier<UnixDomainNetworkAddress>> addressSuppliers =
            Arrays.asList(
                () -> new UnixDomainNetworkAddress("a", 42),
                () -> new UnixDomainNetworkAddress("b", 42),
                () -> new UnixDomainNetworkAddress("a", 70),
                () -> new UnixDomainNetworkAddress("a", 0),
                () -> new UnixDomainNetworkAddress("", 0));
        for (final Supplier<UnixDomainNetworkAddress> supplier :
                 addressSuppliers) {
            final UnixDomainNetworkAddress address = supplier.get();
            assertFalse("Address should not equal null: " + address,
                        address.equals(null));
            assertFalse("Address should not equal String: " + address,
                        address.equals("a-42"));
            for (final Supplier<UnixDomainNetworkAddress> supplier2 :
                     addressSuppliers) {
                final UnixDomainNetworkAddress address2 = supplier2.get();
                if (supplier == supplier2) {
                    assertEquals(address, address2);
                    assertEquals(address.hashCode(), address2.hashCode());
                } else {
                    assertFalse("Should not be equal: " + address +
                                " " + address2,
                                address.equals(address2));
                }
            }
        }
    }

    @Test
    public void testSerialVersion() throws Exception {
        checkAll(serialVersionChecker(new UnixDomainNetworkAddress("a", 42),
                                      SerialVersion.MINIMUM,
                                      0x30c2015ad8c5e9eeL),
                 serialVersionChecker(new UnixDomainNetworkAddress("b", 0),
                                      SerialVersion.MINIMUM,
                                      0x888f8503ffcfdd31L),
                 serialVersionChecker(new UnixDomainNetworkAddress("", 0),
                                      SerialVersion.MINIMUM,
                                      0xa10909c2cdcaf5adL));
    }
}
