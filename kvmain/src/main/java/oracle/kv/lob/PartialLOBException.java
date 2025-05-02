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

package oracle.kv.lob;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

import oracle.kv.FaultException;
import oracle.kv.lob.KVLargeObject.LOBState;

/**
 * Thrown when {@link KVLargeObject#getLOB} is invoked on a partial LOB. A
 * partial LOB is typically the result of an incomplete <code>deleteLOB</code>
 * or <code>putLOB</code> operation.
 *
 * <p>
 * The application can handle this exception and resume the incomplete
 * operation. For example, it can invoke <code>deleteLOB</code>, to delete a
 * LOB in the {@link LOBState#PARTIAL_DELETE} state or it can resume the failed
 * putLOB operation for a LOB in the {@link LOBState#PARTIAL_PUT} state.
 *
 * @see KVLargeObject
 *
 * @since 2.0
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class PartialLOBException extends FaultException {

    private static final long serialVersionUID = 1L;

    private final LOBState partialState;

    /**
     * @hidden
     */
    public PartialLOBException(String msg,
                               LOBState partialState,
                               boolean isRemote) {
        super(msg, isRemote);

        checkPartialStateValue(partialState);
        this.partialState = partialState;
    }

    private static void checkPartialStateValue(LOBState partialState) {
        checkNull("partialState", partialState);

        if (partialState == LOBState.COMPLETE) {
            final String emsg = "The value of partialState must denote a " +
                "partial state not " + partialState;
            throw new IllegalArgumentException(emsg);
        }
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public PartialLOBException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        partialState = LOBState.readFastExternal(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FaultException}) {@code super}
     * <li> ({@link LOBState}) {@link #getPartialState partialState}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        partialState.writeFastExternal(out, serialVersion);
    }

    /**
     * Returns the state associated with the LOB. The state returned is one of
     * the partial states: {@link LOBState#PARTIAL_PUT},
     * {@link LOBState#PARTIAL_DELETE} or
     * {@link LOBState#PARTIAL_APPEND}.
     *
     * @since 2.1.55
     */
    public LOBState getPartialState() {
        return partialState;
    }

    /**
     * Returns true only if the exception resulted from a partially deleted LOB.
     *
     * @deprecated Use the getPartialStateMethod() instead
     */
    @Deprecated
    public boolean isPartiallyDeleted() {
        return LOBState.PARTIAL_DELETE == partialState;
    }

    /** Check for valid fields. */
    private void readObject(ObjectInputStream in)
        throws ClassNotFoundException, IOException {

        in.defaultReadObject();
        checkPartialStateValue(partialState);
    }
}
