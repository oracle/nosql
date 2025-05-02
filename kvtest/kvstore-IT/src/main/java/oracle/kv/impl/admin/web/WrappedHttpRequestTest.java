/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web;

import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class WrappedHttpRequestTest {

    @Test
    public void testMakePathList() throws Exception {
        ChannelHandlerContext ctx =
            AdminWebServiceTest.createExpectationAndMock("localhost");
        FullHttpRequest msg =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                           HttpMethod.POST,
                                           "-!@#$%");
        try {
            WrappedHttpRequest req = new WrappedHttpRequest(msg, ctx);
            req.getPaths();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         "/test/");
        try {
            WrappedHttpRequest req = new WrappedHttpRequest(msg, ctx);
            req.getPaths();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         "V/nosq/amin");
        try {
            WrappedHttpRequest req = new WrappedHttpRequest(msg, ctx);
            req.getPaths();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         "V0/nosq/amin");
        try {
            WrappedHttpRequest req = new WrappedHttpRequest(msg, ctx);
            req.getPaths();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.POST,
                                         "V0/nosql/amin");
        try {
            WrappedHttpRequest req = new WrappedHttpRequest(msg, ctx);
            req.getPaths();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }
    }
}
