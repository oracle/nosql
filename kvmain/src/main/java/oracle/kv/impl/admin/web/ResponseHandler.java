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

package oracle.kv.impl.admin.web;

import static io.netty.buffer.Unpooled.copiedBuffer;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Handle the Http response sent back to client.
 * Functionality of this class:
 * - Handle the set/get for Http response header.
 * - Print string based results to buffer for Http response content.
 * - Handle Http response status code. Handle command error code map to Http
 *   status
 * - Create Http response object to be sent to the channel.
 * - Handle exception logic during executing web service.
 */
public class ResponseHandler {

    /* Http response status code */
    private HttpResponseStatus responseStatus;

    /* Buffer for Http response headers */
    private HttpHeaders headers;

    /* Buffer for Http response content */
    private StringBuilder payload;

    public ResponseHandler() {
        setDefaults();
    }

    /* Clear up the buffer fields and set with default values */
    public void setDefaults() {
        responseStatus = HttpResponseStatus.OK;
        headers = new DefaultHttpHeaders();
        headers.set(HttpHeaderNames.CONTENT_TYPE,
                    HttpHeaderValues.TEXT_PLAIN).
            setInt(HttpHeaderNames.CONTENT_LENGTH, 0).
            set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        payload = new StringBuilder();
    }

    /* Clear all buffer fields */
    public void clear() {
        responseStatus = null;
        headers = null;
        payload = null;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    /*
     * For unit test
     */
    public StringBuilder getPayload() {
        return payload;
    }

    /*
     * For unit test
     */
    public HttpResponseStatus getReponseStatus() {
        return responseStatus;
    }

    /* Set plain text content type */
    public void setPlainTextHeader() {
        headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
    }

    /* Set application JSON content type */
    public void setAppJsonHeader() {
        headers.set(HttpHeaderNames.CONTENT_TYPE,
                    HttpHeaderValues.APPLICATION_JSON);
    }

    /* Print response result to Http content buffer */
    public void print(String result) {
        payload.append(result);
    }

    /* Print response result to Http content buffer, with additional return */
    public void println(String result) {
        print(result + "\n");
    }

    /*
     * Create Http response object for channel writing. Set the content
     * length automatically.
     */
    public FullHttpResponse createResponse() {
        final FullHttpResponse response =
            new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                responseStatus,
                copiedBuffer(payload.toString().getBytes()));
        response.headers().add(headers).
            setInt(HttpHeaderNames.CONTENT_LENGTH,
                   response.content().readableBytes());
        return response;
    }

    public void setResponseStatus(HttpResponseStatus responseStatus) {
        this.responseStatus = responseStatus;
    }

    /* Handle the response result when we have security exception */
    public void handleKVSecurityException(String line,
                                           KVSecurityException kvse) {
      final CommandResult cmdResult =
          new CommandResult.CommandFails(kvse.getMessage(),
                                         ErrorMessage.NOSQL_5100,
                                         CommandResult.NO_CLEANUP_JOBS);
      setAppJsonHeader();
      println(ShellCommandResult.toJsonReport(line, cmdResult));
   }

    /* Handle the response result when we have shell exception */
    public void handleShellException(String line, ShellException se) {
        final CommandResult cmdResult = se.getCommandResult();
        setAppJsonHeader();
        println(ShellCommandResult.toJsonReport(line, cmdResult));
    }

    /* Handle the response result when we have general exception */
    public void handleUnknownException(String line,
                                       Exception e,
                                       SkLogger logger) {
        if (e instanceof AdminFaultException) {
            AdminFaultException afe = (AdminFaultException) e;
            setAppJsonHeader();
            println(ShellCommandResult.toJsonReport(
                line, afe.getCommandResult()));
        } else if (e instanceof AdminNotReadyException) {
            AdminNotReadyException anre = (AdminNotReadyException) e;
            final CommandResult cmdResult =
                new CommandResult.CommandFails(anre.getMessage(),
                                               anre.getErrorMessage(),
                                               CommandResult.NO_CLEANUP_JOBS);
            setAppJsonHeader();
            println(ShellCommandResult.toJsonReport(line, cmdResult));
        } else if (e instanceof KVSecurityException) {
            handleKVSecurityException(line, (KVSecurityException) e);
        } else if (e instanceof IllegalArgumentException) {
            final CommandResult cmdResult =
                new CommandResult.CommandFails(e.getMessage(),
                                               ErrorMessage.NOSQL_5100,
                                               CommandResult.NO_CLEANUP_JOBS);
            setAppJsonHeader();
            println(ShellCommandResult.toJsonReport(line, cmdResult));
        } else {
            logger.warning("Unexpected error:" + LoggerUtils.getStackTrace(e));
            final CommandResult cmdResult =
                new CommandResult.CommandFails(e.getMessage(),
                                               ErrorMessage.NOSQL_5500,
                                               CommandResult.NO_CLEANUP_JOBS);
            setAppJsonHeader();
            println(ShellCommandResult.toJsonReport(line, cmdResult));
        }
    }
}
