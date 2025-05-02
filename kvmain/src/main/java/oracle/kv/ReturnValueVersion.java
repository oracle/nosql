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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.util.FastExternalizable;

/**
 * Used with put and delete operations to return the previous value and
 * version.
 * <p>
 * A ReturnValueVersion instance may be created and passed as the {@code
 * prevValue} parameter to methods such as {@link KVStore#put(Key, Value,
 * ReturnValueVersion, Durability, long, TimeUnit)}.
 * </p>
 * <p>
 * For best performance, it is important to choose only the properties that are
 * required.  The KV Store is optimized to avoid I/O when the requested
 * properties are in cache.
 * </p>
 * <p>Note that because both properties are optional, the version property,
 * value property, or both properties may be null.</p>
 */
public class ReturnValueVersion extends ValueVersion {

    /**
     * Specifies whether to return the value, version, both or neither.
     * <p>
     * For best performance, it is important to choose only the properties that
     * are required.  The KV Store is optimized to avoid I/O when the requested
     * properties are in cache.  </p>
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public enum Choice implements FastExternalizable {

        /*
         * WARNING: To avoid breaking serialization compatibility, the order of
         * the values must not be changed and new values must be added at the
         * end.
         */

        /**
         * Return the value only.
         */
        VALUE(0, true, false),

        /**
         * Return the version only.
         */
        VERSION(1, false, true),

        /**
         * Return both the value and the version.
         */
        ALL(2, true, true),

        /**
         * Do not return the value or the version.
         */
        NONE(3, false, false);

        private static final Choice[] VALUES = values();

        private boolean needValue;
        private boolean needVersion;

        private Choice(int ordinal, boolean needValue, boolean needVersion) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.needValue = needValue;
            this.needVersion = needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValue() {
            return needValue;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needVersion() {
            return needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValueOrVersion() {
            return needValue || needVersion;
        }

        /**
         * Reads a Choice from the input stream.
         *
         * @hidden For internal use only
         */
        public static Choice readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown Choice: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #VALUE}=0, {@link
         *      #VERSION}=1, {@link #ALL}=2, {@link #NONE}=3
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    private final Choice returnChoice;

    /**
     * Creates an object for returning the value, version or both.
     *
     * @param returnChoice determines whether the value, version, both or
     * none are returned.
     */
    public ReturnValueVersion(Choice returnChoice) {
        this.returnChoice = returnChoice;
    }

    /**
     * Returns the ReturnValueVersion.Choice used to create this object.
     */
    public Choice getReturnChoice() {
        return returnChoice;
    }
}
