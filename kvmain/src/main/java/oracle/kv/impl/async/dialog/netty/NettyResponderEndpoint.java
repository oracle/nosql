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

package oracle.kv.impl.async.dialog.netty;

import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.async.AbstractResponderEndpoint;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;

/**
 * Nio responder endpoint.
 */
class NettyResponderEndpoint extends AbstractResponderEndpoint {

    NettyResponderEndpoint(AbstractEndpointGroup endpointGroup,
                           NetworkAddress remoteAddress,
                           ListenerConfig listenerConfig,
                           NettyEndpointGroup.NettyListener listener,
                           EndpointConfig endpointConfig) {
        super(endpointGroup, remoteAddress, listenerConfig, listener);
        this.handler = new PreReadWrappedEndpointHandler(
                this, endpointConfig, remoteAddress, listener);
    }

    @Override
    public String toString() {
        return String.format("NettyResponderEndpoint[%s, %s]",
                getRemoteAddress(), getListenerConfig());
    }

    public PreReadWrappedEndpointHandler getHandler() {
        return (PreReadWrappedEndpointHandler) handler;
    }
}
