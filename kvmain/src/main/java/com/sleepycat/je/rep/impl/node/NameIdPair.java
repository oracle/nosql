/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.impl.node;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.utilint.BinaryProtocol;

/**
 * The public name and internal id pair used to uniquely identify a node
 * within a replication group.
 */
public class NameIdPair implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;

    /**
     * The unique internal id associated with a node within its group.
     *
     * The node id space, is local to a replication group and is utilized as
     * follows:
     *
     * The value -1 is reserved to represent the null node id value.
     *
     * Electable and Arbiter node ids are allocated starting at 1 and are
     * allocated in increasing order. They are not reused.
     *
     * Secondary node ids allocated from Integer.MAXINT on downwards. They are
     * transient, that is, the same node can acquire different client ids when
     * it reconnects with the master.
     *
     * During a Network restore operation on a node with a null node id, the
     * node is assigned a temporary -ve random number as its node id. This id
     * is used solely to communicate with the network restore server and is
     * discarded, upon completion of the network restore operation.
     */
    private int id;

    /* Constant to denote an unknown NODE_ID */
    public final static int NULL_NODE_ID = -1;

    /* The node ID used to bypass group membership checks. */
    public static final int NOCHECK_NODE_ID = Integer.MIN_VALUE;

    public static final NameIdPair NULL =
        new ReadOnlyNameIdPair("NullNode", NameIdPair.NULL_NODE_ID);

    public static final NameIdPair NOCHECK =
        new ReadOnlyNameIdPair("NoCheckNode", NOCHECK_NODE_ID);

    public NameIdPair(String name, int id) {
        if (name == null) {
            throw EnvironmentFailureException.unexpectedState
                ("name argument was null");
        }
        this.name = name;
        this.id = id;
    }

    /**
     * Constructor for a pair where the node ID is as yet unknown.
     */
    public NameIdPair(String name) {
        this(name, NULL.getId());
    }

    /** Serializes from a ByteBuffer for a given protocol. */
    public static NameIdPair deserialize(ByteBuffer buffer,
                                         BinaryProtocol protocol) {
        return new NameIdPair(protocol.getString(buffer),
                              LogUtils.readInt(buffer));
    }

    /** Serializes from a TupleInput after retrieving from storage. */
    public static NameIdPair deserialize(TupleInput buffer) {
        return new NameIdPair(buffer.readString(), buffer.readInt());
    }

    /** Serializes into a ByteBuffer for a given protocol. */
    public void serialize(ByteBuffer buffer, BinaryProtocol protocol) {
        protocol.putString(name, buffer);
        LogUtils.writeInt(buffer, id);
    }

    /** Serializes into a TupleOutput before storing. */
    public void serialize(TupleOutput buffer) {
        buffer.writeString(name);
        buffer.writeInt(id);
    }

    /** Returns serialized for a given protocol. */
    public int serializedSize(BinaryProtocol protocol) {
        return protocol.stringSize(name) + 4;
    }

    /**
     * Returns the application assigned name
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name + "(" + id + ")";
    }

    /**
     * Gets the node name from a String representation of the NameIdPair.  For
     * example, "Node2(2)" would return "Node2".
     */
    public static String getNameFromString(final String nameIdPair) {
        final int index = nameIdPair.lastIndexOf("(");
        if (index == -1) {
            return null;
        }
        return nameIdPair.substring(0, index);
    }

    /**
     * Returns the internally generated compact id.
     */
    public int getId() {
        return id;
    }

    public boolean hasNullId() {
        return this.id == NameIdPair.NULL_NODE_ID;
    }

    public void setId(int id) {
        setId(id, true);
    }

    public void setId(int id, boolean checkId) {
        if (checkId && (id != this.id) && ! hasNullId()) {
            throw EnvironmentFailureException.unexpectedState
                ("Id was already not null: " + this.id);
        }
        this.id = id;
    }

    public void revertToNull() {
        this.id = NameIdPair.NULL_NODE_ID;
    }

    public void update(NameIdPair other) {
        if (!name.equals(other.getName())) {
            throw EnvironmentFailureException.unexpectedState
                ("Pair name mismatch: " + name + " <> " + other.getName());
        }
        setId(other.getId());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        if (!(obj instanceof NameIdPair)) {
            return false;
        }
        NameIdPair other = (NameIdPair) obj;
        if (id != other.id) {
            return false;
        }
        if (!name.equals(other.name)) {
            throw EnvironmentFailureException.unexpectedState
                ("Ids: " + id + " were equal." + " But names: " + name + ", " +
                 other.name + " weren't!");
        }
        return true;
    }

    private static class ReadOnlyNameIdPair extends NameIdPair {
        private static final long serialVersionUID = 1L;

        public ReadOnlyNameIdPair(String name, int id) {
            super(name, id);
        }

        @Override
        public void setId(int id) {
            throw EnvironmentFailureException.unexpectedState
                ("Read only NameIdPair");
        }
    }
}
