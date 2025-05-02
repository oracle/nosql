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

package oracle.kv.impl.async;

import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A handler for when the socket is prepared for a sync connection.
 */
public interface SocketPrepared extends ListeningChannelErrorHandler {

    /**
     * Called after pre-read determines the channel is of sync and the channel
     * is configured non-blocking and unregistered from the async executor.
     *
     * @param preReadData the pre-read data
     * @param socket the blocking socket for further operation
     */
    void onPrepared(ByteBuffer preReadData, Socket socket);

}
