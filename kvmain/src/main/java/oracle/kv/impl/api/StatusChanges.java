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

package oracle.kv.impl.api;

import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * The status updates that are returned as part of a response.
 *
 * The status update information is expected to be a small number of bytes
 * in the typical case and cheap to compute so that it does not add
 * significantly to the overhead of a request/response.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class StatusChanges implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final State[] STATE_VALUES = State.values();

    /* The state at the *responding node* as contained in the Response */
    private final State state;

    /* The masterId, it's non-null if the node is currently active, that is
     * it's a master or replica.
     */
    private final RepNodeId masterId;

    /*
     * Note that we are using time order events. Revisit if this turns out to
     * be an issue and we should be using heavier weight group-wise sequenced
     * election proposal numbers instead.
     */
    private final long statusTime;

    public StatusChanges(State state,
                         RepNodeId masterId,
                         long sequenceNum) {
        super();
        this.state = state;
        this.masterId = masterId;
        this.statusTime = sequenceNum;
    }

    public StatusChanges(DataInput in, short serialVersion)
        throws IOException {

        final int value = in.readByte();
        if (value == -1) {
            state = null;
        } else {
            try {
                state = STATE_VALUES[value];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException(
                    "Wrong value for ReplicatedEnvironment.State: " + value);
            }
        }
        if (in.readBoolean()) {
            final ResourceId rId =
                ResourceId.readFastExternal(in, serialVersion);
            if (!(rId instanceof RepNodeId)) {
                throw new IOException("Expected RepNodeId: " + rId);
            }
            masterId = (RepNodeId) rId;
        } else {
            masterId = null;
        }
        statusTime = in.readLong();
    }

    /* Verify enum ordinal values for this JE enum. */
    static {
        assert State.DETACHED.ordinal() == 0;
        assert State.UNKNOWN.ordinal() == 1;
        assert State.MASTER.ordinal() == 2;
        assert State.REPLICA.ordinal() == 3;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) {@link #getState state} // {@link State#DETACHED}=0,
     *      {@link State#UNKNOWN}=1, {@link State#MASTER}=2, {@link
     *      State#REPLICA}=3, or -1 if not present
     * <li> ({@link SerializationUtil#writeFastExternalOrNull RepNodeId or
     *      null}) {@link #getCurrentMaster masterId}
     * <li> ({@link DataOutput#writeLong long}) {@link #getStatusTime
     *      statusTime}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeByte((state != null) ? state.ordinal() : -1);
        writeFastExternalOrNull(out, serialVersion, masterId);
        out.writeLong(statusTime);
    }

    public ReplicatedEnvironment.State getState() {
        return state;
    }

    /**
     * Returns the master RN as known to the responding RN
     *
     * @return the unique id associated with the master RN
     */
    public RepNodeId getCurrentMaster() {
        return masterId;
    }

    /**
     * Returns the time associated with the status transition at the responding
     * node.
     */
    public long getStatusTime() {
        return statusTime;
    }
}
