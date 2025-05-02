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

package com.sleepycat.je.tree;

class EntryStates {

    static final byte KNOWN_DELETED_BIT = 0x1;
    static final byte CLEAR_KNOWN_DELETED_BIT = ~0x1;
    static final byte DIRTY_BIT = 0x2;
    static final byte CLEAR_DIRTY_BIT = ~0x2;
    static final byte PENDING_DELETED_BIT = 0x8;
    static final byte CLEAR_PENDING_DELETED_BIT = ~0x8;
    static final byte EMBEDDED_LN_BIT = 0x10;
    static final byte CLEAR_EMBEDDED_LN_BIT = ~0x10;
    static final byte NO_DATA_LN_BIT = 0x20;
    static final byte CLEAR_NO_DATA_LN_BIT = ~0x20;
    static final byte UPDATE_KEY_WHEN_LOGGED = 0x40;
    static final byte CLEAR_UPDATE_KEY_WHEN_LOGGED = ~0x40;
    static final byte TOMBSTONE_BIT = (byte) 0x80;
    static final byte CLEAR_TOMBSTONE_BIT = ~((byte) 0x80);

    /*
     * When transient bits are defined, they should be OR'd into TRANSIENT_BITS
     * below so they will be cleared before writing.
     *
     * In the past, the old MIGRATE_BIT (0x4) was accidentally persisted. It is
     * always defined here as a transient bit so it will be cleared on read.
     * Because of this, 0x4 may not be defined above as a persistent bit, it
     * must forever remain a transient bit.
     */
    static final byte TRANSIENT_BITS = 0x4 | UPDATE_KEY_WHEN_LOGGED;
    static final byte CLEAR_TRANSIENT_BITS = ~TRANSIENT_BITS;
}
