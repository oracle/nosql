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

import java.io.File;

import oracle.kv.impl.async.NetworkAddress;

/**
 * Represents the configuration parameters used to create a {@link KVLocal}
 * object.
 *
 * <p>To build a {@link KVLocalConfig} object, the root directory parameter is
 * required. All other parameters are optional.
 *
 * <p>Use the {@link InetBuilder} class to construct a {@code KVLocalConfig}
 * object using TCP sockets. For example:
 * <pre>
 * // Create a configuration with root directory specified as "rootDir".
 * // Other parameters are set implicitly to default values.
 * // storeName: kvstore
 * // hostName: localhost
 * // port: 5000
 * // isSecure: true
 * // storageGB: 10
 * // memoryMB: 8192
 * KVLocalConfig config = new KVLocalConfig.InetBuilder("rootDir"))
 *                                         .build();
 *
 * // Create a configuration with root directory specified as "rootDir".
 * // Set port number to 6000 and memoryMB to 100 MB.
 * // Other parameters are set implicitly to default values.
 * // storeName: kvstore
 * // hostName: localhost
 * // isSecure: true
 * // storageGB: 10
 * KVLocalConfig config = new KVLocalConfig.InetBuilder("rootDir"))
 *                                         .setPort(6000)
 *                                         .setMemoryMB(100)
 *                                         .build();
 * </pre>
 *
 * Use the {@link UnixDomainBuilder} class to construct a {@code KVLocalConfig}
 * object using Unix domain sockets. Note that the resulting configuration can
 * only be used when running Java 16 or latest releases since Unix domain
 * sockets require at least Java 16.
 *
 * @since 22.1
 */
public class KVLocalConfig {

    /**
     * The default store name is "kvstore".
     */
    public static final String DEFAULT_STORENAME = "kvstore";

    /**
     * The default storage size in gigabyte is 10.
     */
    public static final int DEFAULT_STORAGE_SIZE_GB = 10;

    /**
     * The default memory size in megabytes is 8192, or 8 GB.
     */
    public static final int DEFAULT_MEMORY_SIZE_MB = 8192;

    /**
     * The default port number is 5000.
     */
    public static final int DEFAULT_PORT = 5000;

    private final String storeName;
    private final String hostName;
    private final String kvroot;
    private final boolean isSecure;
    private final int port;
    private final int memoryMB;
    private final int storageGB;
    private final boolean isUnixDomain;

    /* Creates an instance from a builder */
    private KVLocalConfig(Builder builder) {
        storeName = builder.storeName;
        hostName = builder.getHostName();
        kvroot = builder.kvroot;
        isSecure = builder.isSecure();
        port = builder.getPort();
        memoryMB = builder.memoryMB;
        storageGB = builder.storageGB;
        isUnixDomain = builder.isUnixDomain();
    }

    @Override
    public String toString() {
        /*
         * Leave out the host name, port, and isSecure values in the Unix
         * domain socket case because they are not interesting or settable
         * there
         */
        return "<KVLocalConfig" +
               " kvroot=" + kvroot +
               " storeName=" + storeName +
               (!isUnixDomain ? " hostName=" + hostName : "") +
               " memoryMB=" + memoryMB +
               " storageGB=" + storageGB +
               (!isUnixDomain ? " port=" + port : "") +
               (!isUnixDomain ? " isSecure=" + isSecure : "") +
               " isUnixDomain=" + isUnixDomain +
               ">";
    }

    /**
     * Returns the directory where NoSQL Database data is placed.
     *
     * @return the directory where NoSQL Database data is placed
     */
    public String getRootDirectory() {
        return kvroot;
    }

    /**
     * Returns the store name.
     *
     * @return the store name
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Returns the host name.
     *
     * @return the host name
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Returns the port number.
     *
     * @return the port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns whether security is enabled.
     *
     * @return whether security is enabled
     */
    public boolean isSecure() {
        return isSecure;
    }

    /**
     * Returns the memory size in MB.
     *
     * @return the memory size in MB
     */
    public int getMemoryMB() {
        return memoryMB;
    }

    /**
     * Returns the storage directory size in GB.
     *
     * @return the storage directory size in GB
     */
    public int getStorageGB() {
        return storageGB;
    }

    /**
     * Returns whether the configuration uses Unix domain sockets.
     *
     * @return whether the configuration uses Unix domain sockets
     */
    public boolean isUnixDomain() {
        return isUnixDomain;
    }

    /**
     * InetBuilder to help construct a KVLocalConfig instance using TCP
     * sockets.
     */
    public static class InetBuilder extends Builder {

        /**
         * The default host name is "localhost".
         */
        public static final String DEFAULT_HOSTNAME = "localhost";

        /**
         * The default is secure.
         */
        public static final boolean DEFAULT_IS_SECURE = true;

        private String hostName = DEFAULT_HOSTNAME;
        private int port = DEFAULT_PORT;
        private boolean isSecure = DEFAULT_IS_SECURE;

        /**
         * Makes an InetBuilder for KVLocalConfig with the specified root
         * directory.
         *
         * @param rootDir the root directory where NoSQL Database data is
         * placed
         *
         * @throws IllegalArgumentException if specified rootDir is null or
         * empty
         */
        public InetBuilder(String rootDir) {
            super(rootDir);
        }

        @Override
        public KVLocalConfig build() {
            return new KVLocalConfig(this);
        }

        @Override
        public Builder setHostName(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException
                    ("Host name cannot be null or empty");
            }
            hostName = name;
            return this;
        }

        @Override
        String getHostName() {
            return hostName;
        }

        @Override
        public InetBuilder setPort(int portNum) {
            if ((portNum < 0) || (portNum > 65535)) {
                throw new IllegalArgumentException("Illegal port: " + port);
            }
            port = portNum;
            return this;
        }

        @Override
        int getPort() {
            return port;
        }

        /**
         * Specifies whether the store is secure.
         *
         * @param secure whether the store is secure
         *
         * @return this instance
         */
        @Override
        public InetBuilder isSecure(boolean secure) {
            isSecure = secure;
            return this;
        }

        @Override
        boolean isSecure() {
            return isSecure;
        }

        @Override
        boolean isUnixDomain() {
            return false;
        }
    }

    /**
     * Builder class to help construct a {@link KVLocalConfig} instance using
     * Unix domain sockets. Configurations for Unix domain sockets always use
     * the default port number of {@value #DEFAULT_PORT}, do not use security
     * because their security is based on the file system protections for the
     * socket file, and use a fixed host name associated with the Unix domain
     * socket file. Note that the associated methods for setting these values,
     * {@link #setPort setPort}, {@link #setHostName setHostName}, and {@link
     * #isSecure(boolean) isSecure}, all throw {@link
     * UnsupportedOperationException} if called.
     */
    public static class UnixDomainBuilder extends Builder {

        private static final String SOCKETS_DIR = "sockets";
        private static final String SOCKETS_FILE = "sock";

        /**
         * Creates a builder that using the specified root directory
         *
         * @param rootDir the root directory where NoSQL Database data is
         * placed
         *
         * @throws IllegalArgumentException if specified rootDir is null or
         * empty
         */
        public UnixDomainBuilder(String rootDir) {
            super(rootDir);
        }

        /**
         * {@inheritDoc}
         *
         * This implementation always throws {@link
         * UnsupportedOperationException}.
         */
        @Override
        public Builder setHostName(String name) {
            throw new UnsupportedOperationException(
                "UnixDomainBuilder does not support setHostName");
        }

        @Override
        String getHostName() {
            return NetworkAddress.UNIX_DOMAIN_PREFIX + kvroot +
                File.separator + SOCKETS_DIR + File.separator + SOCKETS_FILE;
        }

        /**
         * {@inheritDoc}
         *
         * This implementation always throws {@link
         * UnsupportedOperationException}.
         */
        @Override
        public Builder setPort(int portNum) {
            throw new UnsupportedOperationException(
                "UnixDomainBuilder does not support setPort");
        }

        @Override
        int getPort() {
            return DEFAULT_PORT;
        }

        /**
         * {@inheritDoc}
         *
         * This implementation always throws {@link
         * UnsupportedOperationException}.
         */
        @Override
        public Builder isSecure(boolean secure) {
            throw new UnsupportedOperationException(
                "UnixDomainBuilder does not support isSecure");
        }

        @Override
        boolean isSecure() {
            return false;
        }

        @Override
        boolean isUnixDomain() {
            return true;
        }

        @Override
        public KVLocalConfig build() {
            return new KVLocalConfig(this);
        }
    }

    /**
     * Base builder class used to set configuration parameters common to both
     * TCP sockets and Unix domain sockets.
     */
    public static abstract class Builder {
        /* Required parameter */
        String kvroot;

        /* Optional parameters */
        String storeName = DEFAULT_STORENAME;
        int memoryMB = DEFAULT_MEMORY_SIZE_MB;
        int storageGB = DEFAULT_STORAGE_SIZE_GB;

        /**
         * Makes a builder for KVLocalConfig with the specified root
         * directory.
         *
         * @param rootDir the root directory where NoSQL Database data is
         * placed
         *
         * @throws IllegalArgumentException if specified rootDir is null or
         * empty
         */
        public Builder(String rootDir) {
            if (rootDir == null || rootDir.isEmpty()) {
                throw new IllegalArgumentException
                    ("The root directory cannot be null or empty");
            }
            kvroot = rootDir;
        }

        /**
         * Sets store name.
         *
         * @param name the name of the store
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if store name is null or empty
         */
        public Builder setStoreName(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException
                    ("Store name cannot be null or empty");
            }
            storeName = name;
            return this;
        }

        /**
         * Sets the host name.
         *
         * @param name the name of the host
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if host name is null or empty
         * @throws UnsupportedOperationException if this builder does not
         * support setting the host name
         */
        public abstract Builder setHostName(String name);

        /** Returns the host name. */
        abstract String getHostName();

        /**
         * Sets the memory size in megabytes. Specifies the size of the Java
         * heap used to run the embedded store.
         *
         * @param mb the memory size in megabytes
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if the memory size is less than 1
         */
        public Builder setMemoryMB(int mb) {
            if (mb < 1) {
                throw new IllegalArgumentException(
                    "Memory size must not be less than 1, found: " + mb);
            }
            memoryMB = mb;
            return this;
        }

        /**
         * Sets the storage directory size in gigabytes.
         *
         * <p>
         * The storage directory size is the disk space limit for the
         * Storage Node. If a Storage Node exceeds its disk usage threshold
         * value (storage directory size - 5 GB), then the store suspends
         * all write activities on that node, until sufficient data is removed
         * to satisfy the threshold requirement. If the storage directory size
         * is set to 0, the store opportunistically uses all available space,
         * less 5 GB free disk space.
         *
         * @param gb the storage directory size in gigabytes
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if the storage directory size is
         * negative
         */
        public Builder setStorageGB(int gb) {
            if (gb < 0) {
                throw new IllegalArgumentException
                    ("Illegal storage directory size: " + gb);
            }
            storageGB = gb;
            return this;
        }

        /**
         * Builds a {@link KVLocalConfig} instance using the values specified
         * in this builder.
         *
         * @return a {@code KVLocalConfig} instance for this builder
         */
        public abstract KVLocalConfig build();

        /**
         * Sets port number.
         *
         * @param portNum the port number for registry
         *
         * @return this instance
         * @throws IllegalArgumentException if the port number is not in the
         * range 0 to 65535
         * @throws UnsupportedOperationException if this builder does not
         * support setting the port number
         */
        public abstract Builder setPort(int portNum);

        /** Returns the port number. */
        abstract int getPort();

        /**
         * Specifies whether the store is secure.
         *
         * @param secure whether the store is secure
         *
         * @return this instance
         * @throws UnsupportedOperationException if this builder does not
         * support enabling or disabling security
         */
        public abstract Builder isSecure(boolean secure);

        /** Returns whether the store is secure. */
        abstract boolean isSecure();

        /** Returns whether the store uses Unix domain sockets. */
        abstract boolean isUnixDomain();
    }
}
