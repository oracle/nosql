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

package oracle.kv.impl.async.dialog;

import java.util.function.Supplier;

import oracle.kv.impl.async.NetworkAddress;

/**
 * A channel description. Currently, the channel description will be supplied
 * to the message when a connection related exception is thrown to the dialogs.
 * The implementation should at least include local and remote addresses.
 * Complementing the exception with both local and remote addresses seems to be
 * quite useful when filtering for tcpdump results. The current implementations
 * simply use the string representation of the nio and netty socket channels.
 * In the future, if necessary, we could include more details in our own
 * DataChannel implementation such as handshake status.
 */
public interface ChannelDescription extends Supplier<String> {

    /**
     * A channel description where the channel is not connected, due to, for
     * example, already shutdown or not established.
     */
    public static class NoChannelDescription implements ChannelDescription {

        private final String remoteAddress;

        public NoChannelDescription(NetworkAddress remoteAddress) {
            this.remoteAddress = remoteAddress.toString();
        }

        @Override
        public String get() {
            return String.format("%s(not connected)", remoteAddress);
        }
    }

}

