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
public class ReturnRowImpl extends RowImpl implements ReturnRow {
    private static final long serialVersionUID = 1L;
    private final Choice returnChoice;

    ReturnRowImpl(RecordDef field, TableImpl table,
                  Choice returnChoice) {
        super(field, table);
        this.returnChoice = returnChoice;
    }

    private ReturnRowImpl(ReturnRowImpl other) {
        super(other);
        returnChoice = other.returnChoice;
    }

    @Override
    public Choice getReturnChoice() {
        return returnChoice;
    }

    @Override
    public ReturnRowImpl clone() {
        return new ReturnRowImpl(this);
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            if (other instanceof ReturnRowImpl) {
                ReturnRowImpl otherImpl = (ReturnRowImpl) other;
                return returnChoice == otherImpl.returnChoice;
            }
        }
        return false;
    }

    /**
     * Map ReturnRow.Choice to ReturnValueVersion.Choice.  Allow null to
     * mean NONE.
     */
    static ReturnValueVersion.Choice mapChoice(ReturnRow.Choice choice) {
        if (choice == null) {
            return ReturnValueVersion.Choice.NONE;
        }
        switch (choice) {
        case VALUE:
            return ReturnValueVersion.Choice.VALUE;
        case VERSION:
            return ReturnValueVersion.Choice.VERSION;
        case ALL:
            return ReturnValueVersion.Choice.ALL;
        case NONE:
            return ReturnValueVersion.Choice.NONE;
        default:
            throw new IllegalStateException("Unknown choice: " + choice);
        }
    }

    static ReturnValueVersion makeReturnValueVersion(ReturnRow.Choice choice) {
        return new ReturnValueVersion(mapChoice(choice));
    }

    /**
     * Initialize this object from a ReturnValueVersion returned
     * from a get, put, or delete operation.
     *
     * If the previous row is from a later table version than this object's
     * table is aware of, deserialize that version using the correct table and
     * copy the fields to "this."  In other paths a new RowImpl object would be
     * created from the appropriate table but this object was created by the
     * caller.  The other alternative is to make the TableImpl in RowImpl
     * settable, and reset it. TBD.
     */
    void init(TableAPIImpl impl,
              ReturnValueVersion rvv,
              RowSerializer key,
              long prevExpirationTime,
              long creationTime,
              long prevModificationTime,
              ValueReader<?> reader) {
        if (returnChoice == Choice.VALUE || returnChoice == Choice.ALL) {
            if (rvv.getValue() != null) {
                table.readKeyFields(reader, key);
                impl.getRowFromValueVersion(rvv, key, prevExpirationTime,
                    creationTime, prevModificationTime,
                    false, false, reader);
                if (reader instanceof RowReaderImpl) {
                    RowImpl newRow = (RowImpl)reader.getValue();
                    if (newRow.getTableVersion() != getTableVersion()) {
                        /*
                         * Copy the fields appropriate to this row and set the
                         * correct table version.
                         */
                        copyFrom(newRow, true);
                        setTableVersion(newRow.getTableVersion());
                    }
                }
            }
        }

        /*
         * Version and expiration time are either set or not. Setting
         * expiration may be redundant with respect to code above, although
         * that code is extremely rare (table version problem). An unconditional
         * set is simpler than a complex conditional.
         */
        reader.setExpirationTime(prevExpirationTime);
        reader.setCreationTime(creationTime);
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
