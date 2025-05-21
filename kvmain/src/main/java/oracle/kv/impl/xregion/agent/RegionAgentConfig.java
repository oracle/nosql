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

package oracle.kv.impl.xregion.agent;

import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_CKPT_INTERVAL_OPS;
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_CKPT_INTERVAL_SECS;
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_MAX_CONCURRENT_STREAM_OPS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriberId;

/**
 * Object that represents the configuration information to create a data
 * replication link between two distinct regions.
 */
public class RegionAgentConfig {

    /* type of region agent */
    public enum Type {

        /* subscriber for Point-In-Time-Recover */
        PITR,

        /* subscriber for Multi-Region Table*/
        MRT
    }

    /* mode to start the stream in agent */
    public enum StartMode {

        /* create a stream from source subscribing given tables */
        STREAM,

        /* initialize the tables from source region */
        INIT_TABLE,

        /* create agent with either stream nor table initialization */
        IDLE
    }

    /* region agent type */
    private final Type type;

    /* agent start mode */
    private final StartMode startMode;

    /* subscriber id, caller may create a subscriber group  */
    private final NoSQLSubscriberId sid;

    /* source region to stream data from */
    private final RegionInfo source;

    /* target region to write data to, null for pitr  */
    private final RegionInfo target;

    /* host region of the agent */
    private final RegionInfo host;

    /* ckpt table interval in seconds */
    private final long ckptIntvSecs;

    /* ckpt table interval in streamed ops */
    private final long ckptIntvOps;

    /* # concurrent stream ops */
    private final int numConcurrentStreamOps;

    /* table to stream at the beginning */
    private final Set<String> tables;

    /* local and writable path for agent */
    private final String localPath;

    /* path to security config file, null if non-secure store */
    private final String securityConfig;

    /* true if stream source local writes, false to stream all writes */
    private final boolean localWritesOnly;

    /* unit test only */
    private final boolean enableCkptTable;

    /* unit test only */
    private volatile TestHook<NoSQLStreamMode> testHook = null;

    /**
     * Creates agent configuration instance from builder
     *
     * @param builder builder instance
     */
    private RegionAgentConfig(Builder builder) {

        this.sid = builder.sid;
        this.type = builder.type;
        this.source = builder.source;
        this.target = builder.target;
        this.host = builder.host;
        this.ckptIntvSecs = builder.ckptIntvSecs;
        this.ckptIntvOps = builder.ckptIntvOps;
        this.numConcurrentStreamOps = builder.numConcurrentStreamOps;
        this.localPath = builder.localPath;
        this.tables = builder.tables;
        this.localWritesOnly = builder.localWritesOnly;
        this.startMode = builder.startMode;
        this.enableCkptTable = builder.enableCkptTable;
        this.securityConfig = builder.securityConfig;
    }

    /**
     * Gets the agent type
     *
     * @return the agent type
     */
    Type getType() {
        return type;
    }

    /**
     * Gets the start mode of region agent
     *
     * @return the start mode of region agent
     */
    StartMode getStartMode() {
        return startMode;
    }

    /**
     * Gets the subscriber id associated with agent
     *
     * @return the subscriber id associated with agent
     */
    public NoSQLSubscriberId getSubscriberId(){
        return sid;
    }

    /**
     * Gets the local writable path for agent
     *
     * @return the local writable path for agent
     */
    String getLocalPath() {
        return localPath;
    }

    /**
     * Gets the source region this agent pulls data from
     *
     * @return the source region
     */
    public RegionInfo getSource() {
        return source;
    }

    /**
     * Gets the host region
     *
     * @return the host region
     */
    public RegionInfo getHost() {
        return host;
    }

    /**
     * Gets the target region this agent pushes data to
     *
     * @return the target region
     */
    RegionInfo getTarget() {
        return target;
    }

    /**
     * Gets the set of tables that the agent needs to subscribe at the
     * beginning
     *
     * @return the set of subscribed tables
     */
    String[] getTables() {
        return tables.toArray(new String[0]);
    }

    /**
     * Gets the checkpoint interval in seconds
     *
     * @return the checkpoint interval in seconds
     */
    long getCkptIntvSecs() {
        return ckptIntvSecs;
    }

    /**
     * Gets the checkpoint interval in number of streamed operations
     *
     * @return the checkpoint interval in number of streamed operations
     */
    long getCkptIntvNumOps() {
        return ckptIntvOps;
    }

    /**
     * Gets # of concurrent stream ops
     *
     * @return # of concurrent stream ops
     */
    int getNumConcurrentStreamOps() {
        return numConcurrentStreamOps;
    }

    /**
     * Returns security config file or null if not secure.
     *
     * @return security config file or null
     */
    String getSecurityConfig() {
        return securityConfig;
    }

    /**
     * Returns true if the agent only streams locally originated writes from
     * the source, false if the agent streams all writes regardless of their
     * originated region
     *
     * @return true if the agent only streams locally originated writes from
     * the source, or false.
     */
    boolean isLocalWritesOnly() {
        return localWritesOnly;
    }

    /**
     * Returns true if the region is a secure store, false otherwise
     *
     * @return true if the region is a secure store, false otherwise
     */
    boolean isSecureStore() {
        return securityConfig != null;
    }

    /**
     * @hidden
     *
     * Unit test only
     *
     * Returns true if checkpoint is enabled, false otherwise
     */
    boolean isEnableCkptTable() {
        return enableCkptTable;
    }

    /**
     * @hidden
     *
     * Set test hook
     */
    public void setTestHook(TestHook<NoSQLStreamMode> hook) {
        testHook = hook;
    }

    /**
     * @hidden
     *
     * @return ILE test hooker
     */
    public TestHook<NoSQLStreamMode> getTestHook() {
        return testHook;
    }

    @Override
    public String toString() {
        return "type=" + type +
               ", source=" + source.getName() +
               ", target=" + target.getName() +
               ", ckpt enabled=" + enableCkptTable +
               ", ckpt interval ops=" + ckptIntvOps +
               ", ckpt interval secs=" + ckptIntvSecs +
               ", local write only=" + localWritesOnly;
    }

    /**
     * Builder to construct a RegionAgentConfig instance
     */
    public static class Builder {

        /* required parameters */

        /* subscriber id */
        private final NoSQLSubscriberId sid;

        /* agent type */
        private final Type type;

        /* start mode */
        private final StartMode startMode;

        /* source region to stream data from */
        private final RegionInfo source;

        /* target region to write data to */
        private final RegionInfo target;

        /* host region of the agent */
        private final RegionInfo host;

        /* local path */
        private final String localPath;


        /* optional parameters */

        /* checkpoint interval in seconds */
        private long ckptIntvSecs;

        /* ckpt table interval in streamed ops */
        private long ckptIntvOps;

        /* # concurrent stream ops */
        private final int numConcurrentStreamOps;

        /* table to subscribe */
        private final Set<String> tables;

        private boolean localWritesOnly;

        /* path to security config file, null if non-secure store */
        private String securityConfig;

        /**
         * Unit test only,
         *
         * true if checkpoint is enabled, false otherwise.
         */
        private boolean enableCkptTable;

        /**
         * Makes a builder for RegionAgentConfig with required
         * parameters.
         *
         * @param sid        subscriber id
         * @param type       type of agent
         * @param startMode  start mode
         * @param host       host region
         * @param source     source region
         * @param target     target region
         * @param config     json config
         *
         * @throws IllegalArgumentException if any required parameter is
         * invalid
         * @throws IOException if a problem occurs attempting to create the
         * agent root directory
         */
        public Builder(NoSQLSubscriberId sid,
                       Type type,
                       StartMode startMode,
                       RegionInfo host,
                       RegionInfo source,
                       RegionInfo target,
                       JsonConfig config) throws IOException  {
            this(sid, type, startMode, host, source, target,
                 config.getAgentRoot(),
                 config.getCheckpointIntvSecs(),
                 config.getCheckpointIntvOps(),
                 config.getNumConcurrentStreamOps());
        }

        /**
         * Unit test only
         */
        public Builder(NoSQLSubscriberId sid,
                       Type type,
                       StartMode startMode,
                       RegionInfo host,
                       RegionInfo source,
                       RegionInfo target,
                       String localPath) {
            this(sid, type, startMode, host, source, target, localPath,
                 DEFAULT_CKPT_INTERVAL_SECS, DEFAULT_CKPT_INTERVAL_OPS,
                 DEFAULT_MAX_CONCURRENT_STREAM_OPS);
        }

        private Builder(NoSQLSubscriberId sid,
                        Type type,
                        StartMode startMode,
                        RegionInfo host,
                        RegionInfo source,
                        RegionInfo target,
                        String localPath,
                        long ckptIntvSecs,
                        long ckptIntvOps,
                        int numConcurrentStreamOps) {

            if (sid == null) {
                throw new IllegalArgumentException(
                    "subscriber id cannot be empty");
            }
            if (type == null) {
                throw new IllegalArgumentException(
                    "agent type cannot be null");
            }
            if (startMode == null) {
                throw new IllegalArgumentException(
                    "agent start mode cannot be null");
            }
            if (host == null || host.getHelpers().length == 0) {
                throw new IllegalArgumentException(
                    "invalid host region");
            }
            if (source == null || source.getHelpers().length == 0) {
                throw new IllegalArgumentException(
                    "invalid source region");
            }
            if (type.equals(Type.MRT) &&
                (target == null || target.getHelpers().length == 0)) {
                throw new IllegalArgumentException(
                    "invalid target region for MRT");
            }
            if (localPath == null) {
                throw new IllegalArgumentException("invalid local path");
            }

            this.type = type;
            this.host = host;
            this.source = source;
            this.target = target;
            this.sid = sid;
            this.startMode = startMode;
            this.localPath = localPath;
            this.ckptIntvSecs = ckptIntvSecs;
            this.ckptIntvOps = ckptIntvOps;
            this.numConcurrentStreamOps = numConcurrentStreamOps;
            tables = new HashSet<>();
            localWritesOnly = true;
            enableCkptTable = true;
        }

        /**
         * Sets the checkpoint interval in seconds
         *
         * @param ckptIntvSecs checkpoint interval in seconds.
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if the checkpoint interval is non
         * positive
         */
        public Builder setCkptIntvSecs(int ckptIntvSecs) {
            if (ckptIntvSecs <= 0) {
                throw new IllegalArgumentException(
                    "Checkpoint interval in seconds must be positive");
            }
            this.ckptIntvSecs = ckptIntvSecs;
            return this;
        }

        /**
         * Sets the checkpoint interval in number of streamed operations
         *
         * @param ckptIntvOps checkpoint interval in ops.
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if the checkpoint interval is non
         * positive
         */
        Builder setCkptIntvOps(long ckptIntvOps) {
            if (ckptIntvOps <= 0) {
                throw new IllegalArgumentException(
                    "Checkpoint interval in ops must be positive");
            }
            this.ckptIntvOps = ckptIntvOps;
            return this;
        }

        /**
         * Sets the subscribed tables
         *
         * @param tbs  set of subscribed table names
         *
         * @return this instance
         */
        public Builder setTables(String... tbs) {
            tables.addAll(Arrays.asList(tbs));
            return this;
        }

        /**
         * Sets the subscribed tables
         *
         * @param tbs  set of subscribed table names
         *
         * @return this instance
         */
        public Builder setTables(Set<String> tbs) {
            tables.addAll(tbs);
            return this;
        }

        /**
         * Sets if only local writes can be streamed from the source region
         *
         * @param localWritesOnly true if only local writes can be streamed
         *                        from the source region, false otherwise
         *
         * @return this instance
         */
        Builder setLocalWritesOnly(boolean localWritesOnly) {
            this.localWritesOnly = localWritesOnly;
            return this;
        }

        /**
         * Sets the security config file
         *
         * @param securityConfig full path to security config file, or null
         *                       for non-secure store
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if invalid security config file
         */
        public Builder setSecurityConfig(String securityConfig) {
            if (securityConfig != null && securityConfig.isEmpty()) {
                throw new IllegalArgumentException("Invalid security file");
            }
            this.securityConfig = securityConfig;
            return this;
        }

        /**
         * @hidden
         *
         * Unit test only
         *
         * Disables checkpoint table
         */
        public Builder setEnableCheckpoint(boolean enableCkptTable) {
            this.enableCkptTable = enableCkptTable;
            return this;
        }

        /**
         * Builds a RegionAgentConfig instance from builder
         *
         * @return a RegionAgentConfig instance
         */
        public RegionAgentConfig build() {
            return new RegionAgentConfig(this);
        }
    }
}
