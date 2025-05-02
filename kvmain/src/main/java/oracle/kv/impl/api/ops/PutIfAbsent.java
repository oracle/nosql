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

import java.io.DataInput;
import java.io.IOException;

import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.table.TimeToLive;

/**
 * Inserts a key/data pair.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class PutIfAbsent extends Put {

    /**
     * Constructs a put-if-absent operation.
     */
    public PutIfAbsent(byte[] keyBytes,
                       Value value,
                       ReturnValueVersion.Choice prevValChoice) {
        this(keyBytes, value, prevValChoice, 0);
    }

    /**
     * Constructs a put-if-absent operation with a table id.
     */
    public PutIfAbsent(byte[] keyBytes,
                       Value value,
                       ReturnValueVersion.Choice prevValChoice,
                       long tableId) {
        super(OpCode.PUT_IF_ABSENT, keyBytes, value, prevValChoice, tableId);
    }

    /** Constructor to implement deserializedForm */
    private PutIfAbsent(PutIfAbsent other, short serialVersion) {
        super(other, serialVersion);
    }

    /**
     * Constructs a put-if-absent operation with a table id.
     */
    public PutIfAbsent(byte[] keyBytes,
                       Value value,
                       ReturnValueVersion.Choice prevValChoice,
                       long tableId,
                       TimeToLive ttl,
                       boolean updateTTL) {
        super(OpCode.PUT_IF_ABSENT, keyBytes, value, prevValChoice, tableId,
              ttl, updateTTL,
              false, /* isSQLUpdate */
              null,  /*allIndexes*/
              null,  /*allIndexIds*/
              null   /*indexesToUpdate*/);
    }


    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    PutIfAbsent(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.PUT_IF_ABSENT, in, serialVersion);
    }

    @Override
    public boolean performsRead() {
        /* Override the conditional return in Put */
        return true;
    }

    @Override
    public PutIfAbsent deserializedForm(short serialVersion) {
        return new PutIfAbsent(this, serialVersion);
    }
}
