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

package com.sleepycat.je.rep.utilint.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.utilint.RepUtils;

/**
 * A factory class for generating SimpleDataChannel instances based on
 * SocketChannel instances.
 */
public class SimpleChannelFactory implements DataChannelFactory {

    public SimpleChannelFactory() {
    }

    /**
     * Included for compatibility with the standard DataChannelFactory.Builder
     * construction model.
     */
    public SimpleChannelFactory(InstanceParams unusedParams) {
    }

    @Override
    public DataChannel acceptChannel(SocketChannel socketChannel) {
        return new SimpleDataChannel(socketChannel);
    }

    @Override
    public DataChannel connect(InetSocketAddress addr,
                               InetSocketAddress localAddr,
                               ConnectOptions connectOptions)
        throws IOException {

        final SocketChannel socketChannel =
            RepUtils.openSocketChannel(addr, localAddr, connectOptions);
        return new SimpleDataChannel(socketChannel);
    }
}
