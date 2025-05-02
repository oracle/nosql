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

import static oracle.nosql.common.http.Constants.ADMIN_PATH_NAME;
import static oracle.nosql.common.http.Constants.NOSQL_PATH_NAME;
import static oracle.nosql.common.http.Constants.NOSQL_VERSION;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import oracle.nosql.common.json.JsonUtils;

import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * Wrapper class of the FullHttpRequest. Parsing the Http request path and
 * Http JSON payload during initialization. This class stores the processed
 * information of Http request for further re-use. Specifically store the Http
 * path and Http JSON payload.
 */
public class WrappedHttpRequest {

    private final FullHttpRequest request;

    /* Store the processed Http path */
    private final List<String> paths;

    /* Store the processed Http JSON payload */
    private final CommandInputs inputs;

    /* Client host name */
    private final String clientHostName;

    public WrappedHttpRequest(FullHttpRequest request,
                              ChannelHandlerContext ctx) {
        this.request = request;
        this.paths = makePathList(request.uri());
        this.inputs = readCommandInputs(request);
        this.clientHostName = parseClientHostName(ctx);
    }

    /* Retrieve clientHostName */
    public String getClientHostName() {
        return clientHostName;
    }

    /* Retrieve the original request */
    public FullHttpRequest getRequest() {
        return request;
    }

    /* Retrieve the Http path */
    public List<String> getPaths() {
        return paths;
    }

    /* Retrieve the Http JSON payload */
    public CommandInputs getInputs() {
        return inputs;
    }

    /*
     * parse client host name from ChannelHandlerContext
     */
    private String parseClientHostName(ChannelHandlerContext ctx) {
        String hostName = null;
        final InetSocketAddress socketAddress =
            (InetSocketAddress) ctx.channel().remoteAddress();
        if (socketAddress != null && socketAddress.getAddress() != null) {
            hostName = socketAddress.getAddress().getHostName();
        }
        if (hostName == null) {
            throw new IllegalArgumentException("Invalid ClientHostName:" +
                                               hostName);
        }
        return hostName;
    }

    /*
     * Generate a list of request path components based on request URI.
     */
    private List<String> makePathList(String uri) {

        String path;
        try {
            path = new URI(uri).normalize().getPath();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                "Fail to parse request URI: " + uri);
        }

        if (path == null) {
            throw new IllegalArgumentException(
                "Undefined path: " + uri);
        }

        /* filter out the "/" from beginning */
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        /* filter out the "/" at the end */
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        final String[] splitPath = path.split("/");

        /* start with /VERSION/NOSQL/ADMIN */
        if (splitPath.length < 3 ||
            !splitPath[0].equals(NOSQL_VERSION) ||
            !splitPath[1].equals(NOSQL_PATH_NAME) ||
            !splitPath[2].equals(ADMIN_PATH_NAME)) {
            throw new IllegalArgumentException(
                "Request path is not start with /" + NOSQL_VERSION +
                "/" + NOSQL_PATH_NAME + "/" + ADMIN_PATH_NAME);
        }

        /* filter out /VERSION/NOSQL/ADMIN */
        final List<String> result = new ArrayList<String>();
        for (int i = 3; i < splitPath.length; i++) {
            result.add(splitPath[i]);
        }
        return result;
    }

    /*
     * Read JSON payload from Http request and return the POJO instance of the
     * conversion. If there is not JSON payload, return null. If the JSON
     * payload's format does not meet CommandInputs, throw exception and error
     * code to indicate the problem.
     */
    private CommandInputs readCommandInputs(FullHttpRequest req) {
        if (req.content().readableBytes() == 0) {
            return null;
        }
        try (InputStream inStream =
            new ByteBufInputStream(req.content())) {
            try (InputStreamReader reader = new InputStreamReader(inStream)) {
                return JsonUtils.fromJson(reader, CommandInputs.class);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Fail to convert JSON payload", e);
        }
    }
}
