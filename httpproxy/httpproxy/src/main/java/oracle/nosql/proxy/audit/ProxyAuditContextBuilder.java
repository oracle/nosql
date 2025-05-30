/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.proxy.audit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import oracle.nosql.audit.AuditContext.AuditContextBuilder;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.util.tmi.TableInfo;

/**
 * An interface used for building audit context for Proxy.
 */
public interface ProxyAuditContextBuilder extends AuditContextBuilder {

    /**
     * Record request to audit context builder.
     */
    ProxyAuditContextBuilder setRequest(FullHttpRequest request,
                                        ChannelHandlerContext ctx,
                                        String requestId,
                                        String tableName,
                                        AccessContext ac);

    /**
     * Record response to audit context builder.
     */
    ProxyAuditContextBuilder setResponse(FullHttpResponse response);

    /**
     * Record event name to audit context builder.
     * @param opCode TABLE_REQUEST is invalid. It need be changed to
     * CREATE_TABLE, ALTER_TABLE, DROP_TABLE, CREATE_INDEX or DROP_INDEX
     */
    ProxyAuditContextBuilder setOperation(OpCode opCode, TableInfo tableInfo);
}
