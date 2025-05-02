/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.sklogger.SkLogger;

import org.junit.Test;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ResponseHandlerTest extends TestBase {

    private static final String testMessage = "testMessage";
    private static final SkLogger sklogger =
        new SkLogger(LoggerUtils.getLogger(ResponseHandlerTest.class, "Test"));
    @Test
    public void testCreateResponse() throws Exception {
        final ResponseHandler handler = new ResponseHandler();
        handler.clear();
        assertNull(handler.getHeaders());
        assertNull(handler.getPayload());
        assertNull(handler.getReponseStatus());
        handler.setDefaults();
        assertEquals(handler.getReponseStatus(),
                     HttpResponseStatus.OK);
        HttpHeaders headers = handler.getHeaders();
        assertEquals(headers.get(HttpHeaderNames.CONTENT_TYPE),
                     HttpHeaderValues.TEXT_PLAIN.toString());
        assertEquals(headers.get(HttpHeaderNames.CONTENT_LENGTH),
                     "0");
        assertEquals(headers.get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN),
                     "*");
        assertNotNull(handler.getPayload());
        handler.setPlainTextHeader();
        handler.setResponseStatus(HttpResponseStatus.ACCEPTED);
        handler.getHeaders().set(testMessage, testMessage);
        handler.print(testMessage);
        FullHttpResponse response = handler.createResponse();
        assertEquals(new String(response.content().array()), testMessage);
        assertEquals(response.headers().get(testMessage), testMessage);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE),
                     HttpHeaderValues.TEXT_PLAIN.toString());
        assertEquals(response.status(), HttpResponseStatus.ACCEPTED);
        handler.clear();
        handler.setDefaults();
        handler.setAppJsonHeader();
        handler.setResponseStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
        ShellCommandResult scr =
            ShellCommandResult.getDefault(testMessage);
        scr.setReturnCode(ErrorMessage.NOSQL_5500.getValue());
        scr.setDescription(testMessage);
        handler.print(scr.convertToJson());
        response = handler.createResponse();
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE),
                     HttpHeaderValues.APPLICATION_JSON.toString());
        assertEquals(response.status(),
                     HttpResponseStatus.SERVICE_UNAVAILABLE);
        scr = JsonUtils.readValue(response.content().array(),
                                  ShellCommandResult.class);
        assertEquals(scr.getOperation(), testMessage);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5500.getValue());
        assertEquals(scr.getDescription(), testMessage);
    }

    @Test
    public void testHandleErrorAndException() throws Exception {
        final ResponseHandler handler = new ResponseHandler();
        final ShellException shellException =
            new ShellException(testMessage,
                               ErrorMessage.NOSQL_5100,
                               CommandResult.NO_CLEANUP_JOBS);
        handler.handleShellException(testMessage, shellException);
        FullHttpResponse response = handler.createResponse();
        assertEquals(response.status(),
                     HttpResponseStatus.OK);
        ShellCommandResult scr =
            JsonUtils.readValue(response.content().array(),
                               ShellCommandResult.class);
        assertEquals(scr.getOperation(), testMessage);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5100.getValue());
        handler.clear();
        handler.setDefaults();
        final IllegalStateException ise =
            new IllegalStateException(testMessage);
        handler.handleUnknownException(testMessage, ise, sklogger);
        response = handler.createResponse();
        assertEquals(response.status(),
                     HttpResponseStatus.OK);
        scr = JsonUtils.readValue(response.content().array(),
                                  ShellCommandResult.class);

        assertEquals(scr.getOperation(), testMessage);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5500.getValue());
        assertEquals(scr.getDescription(), testMessage);
        final AuthenticationRequiredException are =
            new AuthenticationRequiredException(testMessage, false);
        handler.clear();
        handler.setDefaults();
        handler.handleKVSecurityException(testMessage, are);
        response = handler.createResponse();
        assertEquals(response.status(),
                     HttpResponseStatus.OK);
        scr = JsonUtils.readValue(response.content().array(),
                                  ShellCommandResult.class);
        assertEquals(scr.getOperation(), testMessage);
        assertEquals(scr.getReturnCode(), ErrorMessage.NOSQL_5100.getValue());
        assertEquals(scr.getDescription(), testMessage);
    }
}
