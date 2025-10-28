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

package oracle.kv.impl.async.dialog.nio;

import java.nio.channels.SocketChannel;

import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.async.AbstractResponderEndpoint;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;

/**
 * Nio responder endpoint.
 */
class NioResponderEndpoint extends AbstractResponderEndpoint {

    NioResponderEndpoint(AbstractEndpointGroup endpointGroup,
                         NetworkAddress remoteAddress,
                         ListenerConfig listenerConfig,
                         NioEndpointGroup.NioListener listener,
                         EndpointConfig endpointConfig,
                         NioChannelExecutor executor,
                         SocketChannel socketChannel) {
        super(endpointGroup, remoteAddress, listenerConfig, listener);
        this.handler = new PreReadWrappedEndpointHandler(
            this, endpointConfig, remoteAddress,
            executor,
            getEndpointGroup().getBackupExecService(),
            getEndpointGroup().getBackupSchedExecService(),
            listener, socketChannel);
    }

    @Override
    public String toString() {
        return String.format("NioResponderEndpoint[%s, %s]",
                getRemoteAddress(), getListenerConfig());
    }

    public PreReadWrappedEndpointHandler getHandler() {
        return (PreReadWrappedEndpointHandler) handler;
    }
}
