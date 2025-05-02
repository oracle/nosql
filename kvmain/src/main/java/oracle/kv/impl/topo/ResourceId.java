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

package oracle.kv.impl.topo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.util.FastExternalizable;

/**
 * Uniquely identifies a component in the KV Store. There is a subclass of
 * ResourceId corresponding to each different resource type in the kv store.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class ResourceId implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    /**
     * An enumeration identifying the different resource types in the KV Store.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static enum ResourceType implements FastExternalizable {
        DATACENTER(0) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new DatacenterId(in, serialVersion);
            }

            @Override
            public boolean isDatacenter() {
                return true;
            }
        },
        STORAGE_NODE(1) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new StorageNodeId(in, serialVersion);
            }

            @Override
            public boolean isStorageNode() {
                return true;
            }
        },
        REP_GROUP(2) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new RepGroupId(in, serialVersion);
            }

            @Override
            public boolean isRepGroup() {
                return true;
            }
        },
        REP_NODE(3) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new RepNodeId(in, serialVersion);
            }

            @Override
            public boolean isRepNode() {
                return true;
            }
        },
        PARTITION(4) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new PartitionId(in, serialVersion);
            }

            @Override
            public boolean isPartition() {
                return true;
            }
        },
        ADMIN(5) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new AdminId(in, serialVersion);
            }

            @Override
            public boolean isAdmin() {
                return true;
            }
        },
        CLIENT(6) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new ClientId(in, serialVersion);
            }

            @Override
            public boolean isClient() {
                return true;
            }
        },
        ARB_NODE(7) {
            @Override
            ResourceId readResourceId(DataInput in, short serialVersion)
                throws IOException {

                return new ArbNodeId(in, serialVersion);
            }

            @Override
            public boolean isArbNode() {
                return true;
            }
        };

        private static final ResourceType[] VALUES = values();

        private ResourceType(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        abstract ResourceId readResourceId(DataInput in, short serialVersion)
            throws IOException;

        static ResourceType readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown resource type: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #DATACENTER}=0, {@link
         *      #STORAGE_NODE}=1, {@link #REP_GROUP}=2, {@link #REP_NODE}=3,
         *      {@link #PARTITION}=4, {@link #ADMIN}=5, {@link #CLIENT}=6,
         *      {@link #ARB_NODE}=7
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        /**
         * Default predicates overridden by the appropriate enumerator.
         */
        public boolean isDatacenter() {
            return false;
        }

        public boolean isStorageNode() {
            return false;
        }

        public boolean isRepGroup() {
            return false;
        }

        public boolean isRepNode() {
            return false;
        }

        public boolean isPartition() {
            return false;
        }

        public boolean isAdmin() {
            return false;
        }

        public boolean isArbNode() {
            return false;
        }

        public boolean isClient() {
            return false;
        }
    }

    /**
     * Utility method for ResourceIds to parse string representations.
     * Supports both X, prefixX (i.e. 10, sn10)
     */
    protected static int parseForInt(String idPrefix, String val) {
        try {
            int id = Integer.parseInt(val);
            return id;
        } catch (NumberFormatException e) {

            /*
             * The argument isn't an integer. See if it's prefixed with
             * the resource id's prefix.
             */
            String arg = val.toLowerCase();
            if (arg.startsWith(idPrefix)) {
                try {
                    int id = Integer.parseInt(val.substring(idPrefix.length()));

                    /*
                     * Guard against a negative value, which could happen if
                     * the string had a hyphen, i.e prefix-X
                     */
                    if (id > 0) {
                        return id;
                    }
                } catch (NumberFormatException e2) {
                    /* Fall through and throw an exception. */
                }
            }
            throw new IllegalArgumentException
               (val + " is not a valid id. It must follow the format " +
                idPrefix + "X");
        }
    }

    /**
     * Utility method for ResourceIds to parse string representations.
     * Supports both X, and prefixX for multiple prefixes (i.e. 10, dc10, zn10)
     */
    protected static int parseForInt(final String[] idPrefixes,
                                     final String val) {
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
        }

        /* The argument isn't an integer -- see if it has a prefix */
        final String arg = val.toLowerCase();
        for (final String idPrefix : idPrefixes) {
            if (arg.startsWith(idPrefix)) {
                try {
                    final int id =
                        Integer.parseInt(val.substring(idPrefix.length()));

                    /*
                     * Guard against a negative value, which could happen if
                     * the string had a hyphen, i.e prefix-X
                     */
                    if (id > 0) {
                        return id;
                    }
                } catch (NumberFormatException e2) {
                    break;
                }
            }
        }
        throw new IllegalArgumentException(
            val + " is not a valid id." +
            " It must have the form <prefix>X where <prefix> is one of: " +
            Arrays.toString(idPrefixes));
    }

    protected ResourceId() {
    }

    /**
     * FastExternalizable constructor.  Subclasses must call this constructor
     * before reading additional elements.
     *
     * The ResourceType was read by readFastExternal.
     */
    public ResourceId(@SuppressWarnings("unused") DataInput in,
                      @SuppressWarnings("unused") short serialVersion) {
    }

    /**
     * FastExternalizable factory for all ResourceId subclasses.
     */
    public static ResourceId readFastExternal(DataInput in,
                                              short serialVersion)
        throws IOException {

        final ResourceType type =
            ResourceType.readFastExternal(in, serialVersion);
        return type.readResourceId(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Subclasses must call this method before
     * writing additional elements.
     *
     * <p>Format:
     * <ol>
     * <li> ({@link ResourceType}) {@link #getType resourceType}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        getType().writeFastExternal(out, serialVersion);
    }

    /**
     * Returns the type associated with the resource id.
     */
    public abstract ResourceType getType();

    /**
     * Resolve the resource id to a component in the Topology.
     *
     * @param topology the topology to be used for the resolution
     *
     * @return the topology component
     */
    public abstract Topology.Component<?> getComponent(Topology topology);

    /**
     * Reads the associated component from the input stream.
     *
     * @param topology the containing topology or null
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @throws IOException if there is a problem reading from the input stream
     */
    protected abstract Topology.Component<?> readComponent(Topology topology,
                                                           DataInput in,
                                                           short serialVersion)
        throws IOException;

    /*
     * Resource ids must provide their own non-default equals and hash code
     * methods.
     */

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    @Override
    public abstract ResourceId clone();

   /**
     * Returns a string representation that uniquely identifies this component,
     * suitable for requesting RMI resources.
     * @return the fully qualified name of the component.
     */
    public String getFullName() {
        throw new UnsupportedOperationException
            ("Not supported for " + getType());
    }
}
