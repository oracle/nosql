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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Used to provide consistency guarantees for read operations.
 * <p>
 * In general, read operations may be serviced either at a Master or Replica
 * node.  When serviced at the Master node, consistency is always absolute.  If
 * absolute consistency is required, {@link #ABSOLUTE} may be specified to
 * force the operation to be serviced at the Master.  For other types of
 * consistency, when the operation is serviced at a Replica node, the
 * transaction will not begin until the consistency policy is satisfied.
 * </p>
 * <p>
 * The Consistency is specified as an argument to all read operations, for
 * example, {@link KVStore#get get}.
 * </p>
 *
 * @hidden.see For {@link FastExternalizable} format, see: {@link NoneRequired},
 * {@link Absolute}, {@link NoneRequiredNoMaster}, {@link Version}, {@link
 * Time}
 */
/*
 * Suppress warnings about references from public (but hidden) javadoc comments
 * to non-public items.
 */
@SuppressWarnings("javadoc")
public abstract class Consistency implements FastExternalizable, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * A consistency policy that lets a transaction on a replica using this
     * policy proceed regardless of the state of the Replica relative to the
     * Master.
     */
    public static final Consistency NONE_REQUIRED = new NoneRequired();

    /**
     * A consistency policy that requires that a transaction be serviced on the
     * Master so that consistency is absolute.
     */
    public static final Consistency ABSOLUTE = new Absolute();

    /**
     * A consistency policy that requires that a read operation be serviced on
     * a replica; never the Master. When this consistency policy is used, the
     * read operation will not be performed if the only node available is the
     * Master.
     * <p>
     * For read-heavy applications (ex. analytics), it may be desirable to
     * reduce the load on the master by restricting the read requests to only
     * the replicas in the store. It is important to note however, that the
     * secondary zones feature is preferred over this consistency policy as the
     * mechanism for achieving this sort of read isolation. But for cases where
     * the use of secondary zones is either impractical or not desired, this
     * consistency policy can be used to achieve a similar effect; without
     * employing the additional resources that secondary zones may require.
     *
     * @deprecated as of 4.3, use {@link #NONE_REQUIRED} or read zones
     * instead
     */
    @Deprecated
    public static final Consistency NONE_REQUIRED_NO_MASTER =
        new NoneRequiredNoMaster();

    /**
     * WARNING: Do not make this enum public, because the hidden tag causes
     * javadoc to crash with a NPE.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    enum SerialType implements FastExternalizable {
        NONE_REQUIRED_TYPE(0) {
            @Override
            Consistency readConsistency(DataInput in, short serialVersion)
                throws IOException {

                return Consistency.NONE_REQUIRED;
            }
        },

        ABSOLUTE_TYPE(1) {
            @Override
            Consistency readConsistency(DataInput in, short serialVersion)
                throws IOException {

                return Consistency.ABSOLUTE;
            }
        },

        VERSION_TYPE(2) {
            @Override
            Consistency readConsistency(DataInput in, short serialVersion)
                throws IOException {

                return new Version(in, serialVersion);
            }
        },

        TIME_TYPE(3) {
            @Override
            Consistency readConsistency(DataInput in, short serialVersion)
                throws IOException {

                return new Time(in, serialVersion);
            }
        },

        NONE_REQUIRED_NO_MASTER_TYPE(4) {
            @Override
            Consistency readConsistency(DataInput in, short serialVersion)
                throws IOException {

                return Consistency.NONE_REQUIRED_NO_MASTER;
            }
        };

        private static final SerialType[] VALUES = values();

        private SerialType(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        static SerialType readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown SerialType: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #NONE_REQUIRED_TYPE}=0,
         *      {@link #ABSOLUTE_TYPE}=1, {@link #VERSION_TYPE}=2,
         *      {@link #TIME_TYPE}=3, {@link #NONE_REQUIRED_NO_MASTER_TYPE}=4
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        abstract Consistency readConsistency(DataInput in, short serialVersion)
            throws IOException;
    }

    /** For subclasses. */
    Consistency() {
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable factory for all Consistency subclasses.
     */
    public static Consistency readFastExternal(DataInput in,
                                               short serialVersion)
        throws IOException {

        final SerialType type = SerialType.readFastExternal(in, serialVersion);
        return type.readConsistency(in, serialVersion);
    }

    /**
     * Returns this Consistency as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Consistency.  Values
     * returned by calls to this method can be used with current and newer
     * releases, but are not guaranteed to be compatible with earlier releases.
     */
    public byte[] toByteArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeShort(SerialVersion.CURRENT);
            writeFastExternal(oos, SerialVersion.CURRENT);

            oos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Consistency.  Values created
     * with either the current or earlier releases can be used with this
     * method, but values created by later releases are not guaranteed to be
     * compatible.
     */
    public static Consistency fromByteArray(byte[] keyBytes) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes);
        try {
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short serialVersion = ois.readShort();

            return readFastExternal(ois, serialVersion);

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Returns the name used to identify the policy.
     */
    public abstract String getName();

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    public static class NoneRequired extends Consistency {

        private static final long serialVersionUID = 1L;
        private static final String NAME = "Consistency.NoneRequired";

        NoneRequired() {
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerialType}) {@link SerialType#NONE_REQUIRED_TYPE}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {

            SerialType.NONE_REQUIRED_TYPE.writeFastExternal(
                out, serialVersion);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int hashCode() {
            return 31;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof NoneRequired)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    static class Absolute extends Consistency {

        private static final long serialVersionUID = 1L;
        private static final String NAME = "Consistency.Absolute";

        Absolute() {
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerialType}) {@link SerialType#ABSOLUTE_TYPE}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {

            SerialType.ABSOLUTE_TYPE.writeFastExternal(out, serialVersion);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int hashCode() {
            return 37;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Absolute)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    /**
     * @see #writeFastExternal FastExternalizable format
     */
    static class NoneRequiredNoMaster extends Consistency {

        private static final long serialVersionUID = 1L;
        private static final String NAME = "Consistency.NoneRequiredNoMaster";

        NoneRequiredNoMaster() {
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerialType}) {@link
         *      SerialType#NONE_REQUIRED_NO_MASTER_TYPE}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {

            SerialType.NONE_REQUIRED_NO_MASTER_TYPE.writeFastExternal(
                out, serialVersion);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int hashCode() {
            return 43;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof NoneRequiredNoMaster)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    /**
     * A consistency policy which ensures that the environment on a Replica
     * node is at least as current as denoted by the specified Key-Value pair
     * {@link Version}.
     * <p>
     * The version of a Key-Value pair represents a point in the serialized
     * transaction schedule created by the master. In other words, the version
     * is like a bookmark, representing a particular transaction commit in the
     * replication stream.  The Replica ensures that the commit identified by
     * the {@link Version} has been executed before allowing the transaction on
     * the Replica to proceed.
     * <p>
     * For example, suppose the application is a web application.  Each request
     * to the web server consists of an update operation followed by read
     * operations (say from the same client). The read operations naturally
     * expect to see the data from the updates executed by the same request.
     * However, the read operations might have been routed to a Replica node
     * that did not execute the update.
     * <p>
     * In such a case, the update request would generate a {@link Version},
     * which would be resubmitted by the browser, and then passed via a
     * {@link Consistency.Version} object with subsequent read requests to the
     * KV Store.  The read request may be directed by the KV Store's load
     * balancer to any one of the available Replicas.  If the Replica servicing
     * the request is already current (with respect to the version token), it
     * will immediately execute the transaction and satisfy the request. If
     * not, the transaction will stall until the Replica replay has caught up
     * and the change is available at that node.
     *
     * <h2>Consistency Timeout</h2>
     *
     * <p>This class has a {@code timeout} attribute that controls how
     * long a Replica may wait for the desired consistency to be
     * achieved before giving up.</p>
     *
     * <p>All KVStore read operations support a Consistency
     * specification, as well as a separate operation timeout.  The
     * KVStore client driver implements a read operation by choosing a
     * node (usually a Replica) from the proper replication group, and
     * sending it a request.  If the Replica cannot guarantee the
     * desired Consistency within the Consistency timeout, it replies
     * to the request with a failure indication.  If there is still
     * time remaining within the operation timeout, the client driver
     * picks another node and tries the request again (transparent to
     * the application).</p>
     *
     * <p>It makes sense to think of the operation timeout as the
     * maximum amount of time the application is willing to wait for
     * the operation to complete.  The Consistency timeout is like a
     * performance hint to the implementation, suggesting that it can
     * generally expect that a healthy Replica usually should be able
     * to become consistent within the given amount of time, and that
     * if it doesn't, then it is probably more likely worth the
     * overhead of abandoning the request attempt and retrying with a
     * different replica.  Note that for the Consistency timeout to be
     * meaningful it must be smaller than the operation timeout.</p>
     *
     * <p>Choosing a value for the operation timeout depends on the
     * needs of the application.  Finding a good Consistency timeout
     * value is more likely to depend on observations made of real
     * system performance.</p>
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public static class Version extends Consistency {

        private static final long serialVersionUID = 1L;

        private static final String NAME = "Consistency.Version";

        /*
         * Identifies the commit of interest in a serialized transaction
         * schedule.
         */
        private final oracle.kv.Version version;

        /*
         * Amount of time (in milliseconds) to wait for consistency to be
         * reached.
         */
        private final int timeout;

        /**
         * Defines how current a Replica needs to be in terms of a specific
         * write operation that was previously completed. An operation on a
         * Replica that uses this consistency policy is allowed to start only
         * after the transaction identified by the <code>version</code> has
         * been committed on the Replica. The transaction will wait for at most
         * <code>timeout</code> for the Replica to catch up. If the Replica has
         * not caught up in this period, the client method will throw a {@link
         * ConsistencyException}.
         *
         * @param version the token identifying the transaction.
         *
         * @param timeout the maximum amount of time that the transaction start
         * will wait to allow the Replica to catch up.
         *
         * @param timeoutUnit the {@code TimeUnit} for the timeout parameter.
         */
        public Version(oracle.kv.Version version,
                       long timeout,
                       TimeUnit timeoutUnit) {
            if (version == null) {
                throw new IllegalArgumentException("version must not be null");
            }
            this.version = version;
            this.timeout = PropUtil.durationToMillis(timeout, timeoutUnit);
        }

        /**
         * For internal use only.
         * @hidden
         *
         * FastExternalizable constructor.
         *
         * The SerialType was read by readFastExternal.
         */
        Version(DataInput in, short serialVersion)
            throws IOException {

            version = oracle.kv.Version.createVersion(in, serialVersion);
            timeout = in.readInt();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerialType}) {@link SerialType#VERSION_TYPE}
         * <li> ({@link oracle.kv.Version}) {@link #getVersion version}
         * <li> ({@link DataOutput#writeInt int}) {@link #getTimeout timeout}
         * // in milliseconds
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            SerialType.VERSION_TYPE.writeFastExternal(out, serialVersion);
            version.writeFastExternal(out, serialVersion);
            out.writeInt(timeout);
        }

        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Return the <code>Version</code> used to create this consistency
         * policy.
         */
        public oracle.kv.Version getVersion() {
            return version;
        }

        /**
         * Return the timeout specified when creating this consistency policy.
         *
         * @param unit the {@code TimeUnit} of the returned value.
         *
         * @return the timeout specified when creating this consistency policy
         */
        public long getTimeout(TimeUnit unit) {
            return PropUtil.millisToDuration(timeout, unit);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result +
                     ((version == null) ? 0 : version.hashCode());
            result = prime * result + timeout;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Version)) {
                return false;
            }
            final Version other = (Version) obj;
            if (version == null) {
                if (other.version != null) {
                    return false;
                }
            } else if (!version.equals(other.version)) {
                return false;
            }
            if (timeout != other.timeout) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getName() + " version=" + version;
        }
    }

    /**
     * A consistency policy which describes the amount of time the Replica is
     * allowed to lag the Master. The application can use this policy to ensure
     * that the Replica node sees all transactions that were committed on the
     * Master before the lag interval.
     * <p>
     * Effective use of this policy requires that the clocks on the Master and
     * Replica are synchronized by using a protocol like NTP.
     *
     * <h2>Consistency Timeout</h2>
     *
     * <p>Besides the lag time, this class has a {@code timeout}
     * attribute.  The timeout controls how long a Replica may wait
     * for the desired consistency to be achieved before giving
     * up.</p>
     *
     * <p>All KVStore read operations support a Consistency
     * specification, as well as a separate operation timeout.  The
     * KVStore client driver implements a read operation by choosing a
     * node (usually a Replica) from the proper replication group, and
     * sending it a request.  If the Replica cannot guarantee the
     * desired Consistency within the Consistency timeout, it replies
     * to the request with a failure indication.  If there is still
     * time remaining within the operation timeout, the client driver
     * picks another node and tries the request again (transparent to
     * the application).</p>
     *
     * <p>It makes sense to think of the operation timeout as the
     * maximum amount of time the application is willing to wait for
     * the operation to complete.  The Consistency timeout is like a
     * performance hint to the implementation, suggesting that it can
     * generally expect that a healthy Replica usually should be able
     * to become consistent within the given amount of time, and that
     * if it doesn't, then it is probably more likely worth the
     * overhead of abandoning the request attempt and retrying with a
     * different replica.  Note that for the Consistency timeout to be
     * meaningful it must be smaller than the operation timeout.</p>
     *
     * <p>Choosing a value for the operation timeout depends on the
     * needs of the application.  Finding a good Consistency timeout
     * value is more likely to depend on observations made of real
     * system performance.</p>
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public static class Time extends Consistency {

        private static final long serialVersionUID = 1L;

        private static final String NAME = "Consistency.Time";

        private final int permissibleLag;

        /* Amount of time to wait (in ms) for the consistency to be reached. */
        private final int timeout;

        /**
         * Specifies the amount of time by which the Replica is allowed to lag
         * the master when initiating a transaction. The Replica ensures that
         * all transactions that were committed on the Master before this lag
         * interval are available at the Replica before allowing a transaction
         * to proceed.
         * <p>
         * Effective use of this policy requires that the clocks on the Master
         * and Replica are synchronized by using a protocol like NTP.
         *
         * @param permissibleLag the time interval by which the Replica may be
         * out of date with respect to the Master when a transaction is
         * initiated on the Replica.
         *
         * @param permissibleLagUnit the {@code TimeUnit} for the
         * permissibleLag parameter.
         *
         * @param timeout the amount of time to wait for the consistency to be
         * reached.
         *
         * @param timeoutUnit the {@code TimeUnit} for the timeout parameter.
         */
        public Time(long permissibleLag,
                    TimeUnit permissibleLagUnit,
                    long timeout,
                    TimeUnit timeoutUnit) {
            this.permissibleLag = PropUtil.durationToMillis(permissibleLag,
                                                            permissibleLagUnit);
            this.timeout = PropUtil.durationToMillis(timeout, timeoutUnit);
        }

        /**
         * For internal use only.
         * @hidden
         *
         * FastExternalizable constructor.
         *
         * The SerialType was read by readFastExternal.
         */
        Time(DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            permissibleLag = in.readInt();
            timeout = in.readInt();
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link SerialType}) {@link SerialType#TIME_TYPE}
         * <li> ({@link DataOutput#writeInt int}) {@link #getPermissibleLag
         *      permissibleLag} // in milliseconds
         * <li> ({@link DataOutput#writeInt int}) {@link #getTimeout timeout}
         *      // in milliseconds
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out,
                                      short serialVersion)
            throws IOException {

            SerialType.TIME_TYPE.writeFastExternal(out, serialVersion);
            out.writeInt(permissibleLag);
            out.writeInt(timeout);
        }

        @Override
        public String getName() {
            return NAME;
        }

        /**
         * Returns the allowed time lag associated with this policy.
         *
         * @param unit the {@code TimeUnit} of the returned value.
         *
         * @return the permissible lag time in the specified unit.
         */
        public long getPermissibleLag(TimeUnit unit) {
            return PropUtil.millisToDuration(permissibleLag, unit);
        }

        /**
         * Returns the consistency timeout associated with this policy.
         *
         * @param unit the {@code TimeUnit} of the returned value.
         *
         * @return the consistency timeout in the specified unit.
         */
        public long getTimeout(TimeUnit unit) {
            return PropUtil.millisToDuration(timeout, unit);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + permissibleLag;
            result = prime * result + timeout;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Time other = (Time) obj;
            if (permissibleLag != other.permissibleLag) {
                return false;
            }
            if (timeout != other.timeout) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getName() + " permissibleLag=" + permissibleLag;
        }
    }
}
