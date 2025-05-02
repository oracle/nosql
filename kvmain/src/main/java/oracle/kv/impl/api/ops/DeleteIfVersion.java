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

import oracle.kv.ReturnValueVersion;
import oracle.kv.Version;

/**
 * Inserts a key/data pair.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class DeleteIfVersion extends Delete {

    private final Version matchVersion;

    /**
     * Constructs a delete-if-version operation.
     */
    public DeleteIfVersion(byte[] keyBytes,
                           ReturnValueVersion.Choice prevValChoice,
                           Version matchVersion) {
        this(keyBytes, prevValChoice, matchVersion, 0 /* tableId */,
             false /* doTombstone */);
    }

    /**
     * Constructs a delete-if-version operation with a table id and
     * doTombstone.
     */
    public DeleteIfVersion(byte[] keyBytes,
                           ReturnValueVersion.Choice prevValChoice,
                           Version matchVersion,
                           long tableId,
                           boolean doTombstone) {
        super(OpCode.DELETE_IF_VERSION, keyBytes, prevValChoice, tableId,
              doTombstone);
        checkNull("matchVersion", matchVersion);
        this.matchVersion = matchVersion;
    }

    /** Constructor to implement deserializedForm */
    private DeleteIfVersion(DeleteIfVersion other, short serialVersion) {
        super(other, serialVersion);
        this.matchVersion = other.matchVersion;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    DeleteIfVersion(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.DELETE_IF_VERSION, in, serialVersion);
        matchVersion = Version.createVersion(in, serialVersion);
    }

    Version getMatchVersion() {
        return matchVersion;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Delete}) {@code super}
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
    public String toString() {
        return super.toString() + " MatchVersion: " + matchVersion;
    }

    @Override
    public DeleteIfVersion deserializedForm(short serialVersion) {
        return new DeleteIfVersion(this, serialVersion);
    }
}
