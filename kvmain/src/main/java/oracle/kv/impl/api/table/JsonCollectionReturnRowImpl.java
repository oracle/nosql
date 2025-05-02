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

package oracle.kv.impl.api.table;

import oracle.kv.ReturnValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.table.RecordDef;
import oracle.kv.table.ReturnRow;

/*
 * Note in ReturnRow.  The values of ReturnRow.Choice are identical to
 * those in ReturnValueVersion.Choice.  ReturnRow does not extend
 * ReturnValueVersion because it does not need, or want the underlying
 * ValueVersion object.
 */
public class JsonCollectionReturnRowImpl
    extends JsonCollectionRowImpl implements ReturnRow {

    private static final long serialVersionUID = 1L;
    private final Choice returnChoice;

    JsonCollectionReturnRowImpl(RecordDef field, TableImpl table,
                                Choice returnChoice) {
        super(field, table);
        this.returnChoice = returnChoice;
    }

    private JsonCollectionReturnRowImpl(JsonCollectionReturnRowImpl other) {
        super(other);
        returnChoice = other.returnChoice;
    }

    @Override
    public Choice getReturnChoice() {
        return returnChoice;
    }

    @Override
    public JsonCollectionReturnRowImpl clone() {
        return new JsonCollectionReturnRowImpl(this);
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            if (other instanceof JsonCollectionReturnRowImpl) {
                JsonCollectionReturnRowImpl otherImpl =
                    (JsonCollectionReturnRowImpl) other;
                return returnChoice == otherImpl.returnChoice;
            }
        }
        return false;
    }

    /**
     * Initialize this object from a ReturnValueVersion returned
     * from a get, put, or delete operation.
     *
     * This code is simpler than the similar method in ReturnRowImpl because
     * schema evolution is not allowed on JSON collection tables
     */
    void init(TableAPIImpl impl,
              ReturnValueVersion rvv,
              RowSerializer key,
              long prevExpirationTime,
              long prevModificationTime,
              ValueReader<?> reader) {
        if (returnChoice == Choice.VALUE || returnChoice == Choice.ALL) {
            if (rvv.getValue() != null) {
                table.readKeyFields(reader, key);
                impl.getRowFromValueVersion(rvv, key, prevExpirationTime,
                                            prevModificationTime, false,
                                            false, reader);
            }
        }

        /*
         * Version and expiration time are either set or not. Setting
         * expiration may be redundant with respect to code above, although
         * that code is extremely rare (table version problem). An unconditional
         * set is simpler than a complex conditional.
         */
        reader.setExpirationTime(prevExpirationTime);
        reader.setModificationTime(prevModificationTime);
        reader.setVersion(rvv.getVersion());
     }

    /**
     * Set version to null if choice is VALUE because a dummy version is
     * used to transmit expiration time and dummy version must not leak to
     * user code.
     */
    @Override
    public void setVersion(Version version) {
        super.setVersion(returnChoice == Choice.VALUE ? null : version);
    }
}
