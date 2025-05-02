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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.table.TimeToLive;

/**
 * Inserts a key/data pair.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class PutIfVersion extends Put {

    private final Version matchVersion;

    /**
     * Constructs a put-if-version operation.
     */
    public PutIfVersion(byte[] keyBytes,
                        Value value,
                        ReturnValueVersion.Choice prevValChoice,
                        Version matchVersion) {
        this(keyBytes, value, prevValChoice, matchVersion, 0);
    }

    /**
     * Constructs a put-if-version operation with a table id.
     */
    public PutIfVersion(byte[] keyBytes,
                        Value value,
                        ReturnValueVersion.Choice prevValChoice,
                        Version matchVersion,
                        long tableId) {
        this(keyBytes, value, prevValChoice, matchVersion, tableId,
             null, false /* isSQLUpdate */);
    }

    public PutIfVersion(byte[] keyBytes,
            Value value,
            ReturnValueVersion.Choice prevValChoice,
            Version matchVersion,
            long tableId,
            TimeToLive ttl,
            boolean updateTTL) {
        super(OpCode.PUT_IF_VERSION, keyBytes, value, prevValChoice, tableId,
              ttl, updateTTL,
              false, /* isSQLUpdate */
              null, /*allIndexes*/
              null, /*allIndexIds*/
              null /*indexesToUpdate*/);
        checkNull("matchVersion", matchVersion);
        this.matchVersion = matchVersion;
    }

    /** Constructor to implement deserializedForm */
    private PutIfVersion(PutIfVersion other, short serialVersion) {
        super(other, serialVersion);
        matchVersion = other.matchVersion;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    PutIfVersion(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.PUT_IF_VERSION, in, serialVersion);
        matchVersion = Version.createVersion(in, serialVersion);
    }

    Version getMatchVersion() {
        return matchVersion;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Put}) {@code super}
     * <li> ({@link Version}) {@link #getMatchVersion matchVersion}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        matchVersion.writeFastExternal(out, serialVersion);
    }

    @Override
    public boolean performsRead() {
        /* Override the conditional return in Put */
        return true;
    }

    @Override
    public String toString() {
        return super.toString() + " MatchVersion: " + matchVersion;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof PutIfVersion)) {
            return false;
        }
        final PutIfVersion other = (PutIfVersion) obj;
        return matchVersion.equals(other.matchVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), matchVersion);
    }

    @Override
    public PutIfVersion deserializedForm(short serialVersion) {
        return new PutIfVersion(this, serialVersion);
    }
}
