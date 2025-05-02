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

import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.Random;

import oracle.kv.impl.util.SerializationUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A network address for a Unix domain socket. Named addresses have a non-empty
 * pathname and a port value between 0 and 65535, and the associated Unix
 * domain socket has path <i>pathname</i>-<i>port</i>. Named addresses with
 * port 0 can be used to bind server socket channels to an anonymous port,
 * selecting a non-zero port between 32768 and 65535 at listen time. Unnamed
 * addresses have an empty pathname and port 0.
 */
public class UnixDomainNetworkAddress extends NetworkAddress {

    /**
     * Unix domain sockets require Java 16. TODO: Remove this field when we
     * require Java 16 or later.
     */
    public static final int UNIX_DOMAIN_SOCKETS_JAVA_VERSION = 16;
    static final int FIRST_RANDOM_PORT = 32768;
    private static final long serialVersionUID = 1;
    private static final UnixDomainNetworkAddress UNNAMED_ADDRESS =
        new UnixDomainNetworkAddress("", 0);
    private static final Random random = new Random();
    private static final Set<File> serverSocketFiles = new HashSet<>();
    private static boolean addedShutdownHook;

    private final String pathname;
    private final int port;

    /**
     * Creates an address with the specified pathname and port. For a named
     * address, the pathname must not be empty and the port should be between 0
     * and 65535 inclusive. For the unnamed address, the pathname should be
     * empty and the port should be 0.
     *
     * @param pathname the pathname
     * @param port the port
     * @throws IllegalArgumentException if the port is less than 0 or greater
     * than 65535, or if the pathname is empty and the port is not 0
     */
    public UnixDomainNetworkAddress(String pathname, int port) {
        this.pathname = checkNull("pathname", pathname);
        this.port = port;
        checkPort(port);
        if (pathname.isEmpty() && (port != 0)) {
            throw new IllegalArgumentException(
                "Address must not be empty when port is not 0");
        }
    }

    /**
     * Returns a network address for the specified socket address.
     *
     * @param address the socket address
     * @return the network address
     * @throws IllegalArgumentException if the socket address is not an
     * instance of UnixDomainSocketAddress, if the pathname of the address not
     * empty and either does not end with a '-' and a port number in decimal
     * format between 0 and 65535, or does not have any characters prior to the
     * '-'
     * @throws IllegalStateException if running a version of Java less than
     * Java 16
     */
    public static UnixDomainNetworkAddress convertUnix(SocketAddress address) {
        final String addressPath =
            Java16Wrapper.getUnixDomainSocketAddressPath(address).toString();
        if (addressPath.isEmpty()) {
            return UNNAMED_ADDRESS;
        }
        final int dash = addressPath.lastIndexOf('-');
        if (dash < 1) {
            throw new IllegalArgumentException(
                "Unix domain socket address pathname has the wrong" +
                " format: '" + addressPath + "'");
        }
        return new UnixDomainNetworkAddress(
            addressPath.substring(0, dash),
            Integer.parseInt(addressPath.substring(dash + 1)));
    }

    /**
     * Initializes an instance from an input stream.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialized form
     */
    public UnixDomainNetworkAddress(DataInput in, short serialVersion)
        throws IOException {

        pathname = readNonNullString(in, serialVersion);
        port = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString String}) {@link
     *      #getPathname pathname}
     * <li> ({@code int}) {@link #getPort port}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullString(out, serialVersion, pathname);
        out.writeInt(port);
    }

    /**
     * Returns the pathname specified for this address.
     *
     * @return the pathname
     */
    public String getPathname() {
        return pathname;
    }

    @Override
    NetworkAddressType getType() {
        return NetworkAddressType.UNIX_DOMAIN;
    }

    @Override
    public int getPort() {
        return port;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if running a version of Java less than
     * Java 16
     */
    @Override
    public CompletableFuture<SocketAddress> resolveSocketAddress() {
        try {
            final SocketAddress addr =
                Java16Wrapper.unixDomainSocketAddressOf(getFullPathname());
            return CompletableFuture.completedFuture(addr);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        return Java16Wrapper.openSocketChannel();
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        return Java16Wrapper.openServerSocketChannel();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Throws IllegalStateException if running a version of Java less than
     * Java 16
     */
    @Override
    public CompletableFuture<Void> bind(ServerSocketChannel channel,
                                        int backlog)
    {
        try {
            /* Create the shutdown hook first */
            assureShutdownHook();

            String fullPathname = null;

            /*
             * Named sockets with explicit ports, and unnamed sockets, can be
             * handled directly
             */
            if ((port != 0) || pathname.isEmpty()) {
                fullPathname = getFullPathname();
                /*
                 * Directly call get on resolveSocketAddress since this is a
                 * non-blocking call with UnixDomainSocketAddress.
                 */
                channel.bind(resolveSocketAddress().get(), backlog);
            } else {

                /* Otherwise, find a free port */
                Exception lastException = null;
                for (int i = FIRST_RANDOM_PORT; i <= MAX_PORT; i++) {
                    final int s = FIRST_RANDOM_PORT +
                        random.nextInt(MAX_PORT + 1 - FIRST_RANDOM_PORT);
                    final UnixDomainNetworkAddress updatedAddress =
                        new UnixDomainNetworkAddress(pathname, s);
                    try {
                        channel.bind(
                            updatedAddress.resolveSocketAddress().get(),
                            backlog);
                        fullPathname = updatedAddress.getFullPathname();
                        break;
                    } catch (IOException|AlreadyBoundException e) {
                        lastException = e;
                    }
                }
                if (fullPathname == null) {
                    throw new IOException(
                        "No free Unix domain socket address found for " +
                        pathname +
                        ((lastException != null) ?
                         ": " + lastException.getMessage() :
                         ""),
                        lastException);
                }
            }

            registerServerSocketFile(new File(fullPathname));
            return CompletableFuture.completedFuture(null);
        } catch (ExecutionException e) {
            /*
             * Need to unwraps the exception due to that thrown from
             * resolveSocketAddress.
             */
            return failedFuture(e.getCause());

        } catch (Throwable t) {
            /*
             * Need to unwraps the exception due to that thrown from
             * resolveSocketAddress.
             */
            return failedFuture(t);
        }
    }

    /**
     * This overloading always returns "localhost" since Unix domain sockets
     * are always only accessible on the local computer.
     */
    @Override
    public String getHostName() {
         return "localhost";
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if ((obj == null) || !(obj instanceof UnixDomainNetworkAddress)) {
            return false;
        }
        final UnixDomainNetworkAddress other = (UnixDomainNetworkAddress) obj;
        return pathname.equals(other.pathname) && (port == other.port);
    }

    @Override
    public int hashCode() {
        int hash = 71;
        hash = (hash * 73) + pathname.hashCode();
        hash = (hash * 73) + port;
        return hash;
    }

    @Override
    public String toString() {
         return "UnixDomainNetworkAddress[" + pathname + ":" + port + "]";
    }

    /**
     * Returns the full pathname associated with this address, including both
     * the pathname and the port. Returns an empty String for the unnamed
     * address.
     *
     * @return the full pathname
     */
    String getFullPathname() {
        return pathname.isEmpty() ? "" : pathname + '-' + port;
    }

    /** Register a shutdown hook, if needed, to remove server socket files */
    private static void assureShutdownHook() {
        synchronized (serverSocketFiles) {
            if (!addedShutdownHook) {
                Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            removeAllServerSocketFiles();
                        }
                    });
                addedShutdownHook = true;
            }
        }
    }

    /** Register a server socket file for removal */
    private static void registerServerSocketFile(File file) {
        synchronized (serverSocketFiles) {
            serverSocketFiles.add(file);
        }
    }

    /**
     * Remove the server socket file associated with the specified address,
     * which should be a UnixDomainSocketAddress.
     */
    static void removeServerSocketFile(SocketAddress address) {
        final File file =
            Java16Wrapper.getUnixDomainSocketAddressPath(address).toFile();
        synchronized (serverSocketFiles) {
            serverSocketFiles.remove(file);
            file.delete();
        }
    }

    /** Remove all server socket files. */
    private static void removeAllServerSocketFiles() {
        synchronized (serverSocketFiles) {
            serverSocketFiles.forEach(File::delete);
        }
    }

    /**
     * Use reflection to access methods that require Java 16 so that we don't
     * need to compile classes with different target versions. Replace this
     * reflective code with direct calls when we switch to a Java 16 or later
     * compilation target.
     */
    private static class Java16Wrapper {
        private static final String SOCKET_ADDRESS_CLASS_NAME =
            "java.net.UnixDomainSocketAddress";
        private static final String PROTOCOL_NAME = "UNIX";
        private static volatile @Nullable Class<?> addressClass;
        private static volatile @Nullable ProtocolFamily protocolFamily;
        private static volatile @Nullable Method getAddressPath;
        private static volatile @Nullable Method addressOf;
        private static volatile @Nullable Method openSocketChannel;
        private static volatile @Nullable Method openServerSocketChannel;

        /** Call UnixDomainSocketAddress.getPath */
        static Path getUnixDomainSocketAddressPath(SocketAddress address) {
            checkSupported();
            if (!getAddressClass().isInstance(address)) {
                throw new IllegalArgumentException(
                    "The address must be a UnixDomainSocketAddress," +
                    " got: " + address);
            }
            final Method method = getGetAddressPath();
            try {
                return (Path) method.invoke(address);
            } catch (InvocationTargetException e) {
                throw handleException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                    "Unexpected exception: " + e, e);
            }
        }

        /**
         * Checks if Unix domain sockets are supported by the current
         * environment.
         *
         * @throws IllegalStateException if Unix domain sockets are not
         * supported
         */
        private static void checkSupported() {
            if (getJavaMajorVersion() < UNIX_DOMAIN_SOCKETS_JAVA_VERSION) {
                throw new IllegalStateException(
                    "Attempt to use a Unix domain socket address, which" +
                    " requires Java 16 or later, but running Java version " +
                    System.getProperty("java.version"));
            }
        }

        /** Return UnixDomainSocketAddress.getPath */
        private static Method getGetAddressPath() {
            final @Nullable Method method = getAddressPath;
            if (method != null) {
                return method;
            }
            try {
                final Method nonNullMethod =
                    getAddressClass().getMethod("getPath");
                getAddressPath = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Call UnixDomainSocketAddress.of(String) */
        static SocketAddress unixDomainSocketAddressOf(String pathname) {
            checkSupported();
            final Method method = getAddressOf();
            try {
                return (SocketAddress) method.invoke(null, pathname);
            } catch (InvocationTargetException e) {
                throw handleException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Return UnixDomainSocketAddress.of */
        private static Method getAddressOf() {
            final @Nullable Method method = addressOf;
            if (method != null) {
                return method;
            }
            try {
                final Method nonNullMethod =
                    getAddressClass().getMethod("of", String.class);
                addressOf = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Call SocketChannel.open(ProtocolFamily) */
        static SocketChannel openSocketChannel()
            throws IOException
        {
            checkSupported();
            final ProtocolFamily family = getUnixDomainSocketProtocolFamily();
            final Method method = getOpenSocketChannel();
            try {
                return (SocketChannel) method.invoke(null, family);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw handleException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Return StandardProtocolFamily.UNIX */
        private static ProtocolFamily getUnixDomainSocketProtocolFamily() {
            final @Nullable ProtocolFamily result = protocolFamily;
            if (result != null) {
                return result;
            }
            try {
                final ProtocolFamily nonNullResult = (ProtocolFamily)
                    StandardProtocolFamily.class.getField(PROTOCOL_NAME)
                    .get(null);
                protocolFamily = nonNullResult;
                return nonNullResult;
            } catch (IllegalAccessException|NoSuchFieldException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Return 'SocketChannel.open(ProtocolFamily) throws IOException' */
        private static Method getOpenSocketChannel() {
            final @Nullable Method method = openSocketChannel;
            if (method != null) {
                return method;
            }
            try {
                final Method nonNullMethod =
                    SocketChannel.class.getMethod(
                        "open", ProtocolFamily.class);
                openSocketChannel = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Call ServerSocketChannel.open(ProtocolFamily) */
        static ServerSocketChannel openServerSocketChannel()
            throws IOException
        {
            checkSupported();
            final ProtocolFamily family = getUnixDomainSocketProtocolFamily();
            final Method method = getOpenServerSocketChannel();
            try {
                return (ServerSocketChannel) method.invoke(null, family);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw handleException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /**
         * Return 'ServerSocketChannel.open(ProtocolFamily) throws IOException'
         */
        private static Method getOpenServerSocketChannel() {
            final @Nullable Method method = openServerSocketChannel;
            if (method != null) {
                return method;
            }
            try {
                final Method nonNullMethod =
                    ServerSocketChannel.class.getMethod(
                        "open", ProtocolFamily.class);
                openServerSocketChannel = nonNullMethod;
                return nonNullMethod;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Return UnixDomainSocketAddress.class */
        private static Class<?> getAddressClass() {
            final @Nullable Class<?> cl = addressClass;
            if (cl != null) {
                return cl;
            }
            try {
                final Class<?> nonNullClass =
                    Class.forName(SOCKET_ADDRESS_CLASS_NAME);
                addressClass = nonNullClass;
                return nonNullClass;
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        }

        /** Translate InvocationTargetExceptions */
        private static
            RuntimeException handleException(InvocationTargetException e) {

            final Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause instanceof RuntimeException) {
                return (RuntimeException) cause;
            } else {
                throw new IllegalStateException(
                    "Unexpected exception: " + cause, e);
            }
        }
    }
}
