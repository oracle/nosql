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

package oracle.nosql.proxy.rest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonPayload;

/**
 * Represents the request information including:
 *  o Original information: url, headers, payload.
 *  o Parsed parameters in path and query string.
 */
public class RequestParams {
    private final FullHttpRequest request;
    private final String root;
    private final String[] paths;
    private final Map<String, Integer> pathParams;
    private final Map<String, List<String>> queryParams;

    public RequestParams(FullHttpRequest request) {
        this.request = request;

        String url = request.uri();
        int pos = url.indexOf('/', 1);
        root = url.substring(1, pos);

        String path = request.uri().substring(pos + 1);
        QueryStringDecoder qsd = new QueryStringDecoder(path);
        paths = qsd.path().split("/");
        pathParams = new HashMap<String, Integer>();
        queryParams = qsd.parameters();
    }

    public FullHttpRequest getRawRequest() {
        return request;
    }

    public String getUri() {
        return request.uri();
    }

    public String getRoot() {
        return root;
    }

    public String[] getPaths() {
        return paths;
    }

    public void addPathParam(String key, Integer indexOfPaths) {
        pathParams.put(key, indexOfPaths);
    }

    public String getPathParam(String key) {
        if (pathParams.containsKey(key)) {
            return paths[pathParams.get(key)];
        }
        return null;
    }

    public int getPathParamsNum() {
        return pathParams.size();
    }

    public List<String> getQueryParam(String key) {
        if (queryParams.containsKey(key)) {
            return queryParams.get(key);
        }
        return null;
    }

    public String getQueryParamAsString(String key) {
        List<String> values = getQueryParam(key);
        if (values != null) {
            if (values.size() == 1) {
                return values.get(0);
            }
            throw new IllegalArgumentException("The value of parameter '" +
                key + "' is not string but a list: " + values);
        }
        return null;
    }

    public boolean getQueryParamAsBoolean(String key, boolean defVal) {
        String value = getQueryParamAsString(key);
        if (value != null) {
            return Boolean.valueOf(value);
        }
        return defVal;
    }

    public int getQueryParamAsInt(String key) {
        String value = getQueryParamAsString(key);
        if (value != null) {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(
                    "The value of parameter '" + key +
                    "'Not a valid integer: " + value);
            }
        }
        return 0;
    }

    public String getHeaderAsString(String key) {
        return request.headers().getAsString(key);
    }

    public ByteBuf getPayload() {
        return request.content();
    }

    public JsonPayload parsePayload() throws IOException {
        ByteBuf payload = getPayload();
        if (payload != null) {
            payload.resetReaderIndex();
            return new JsonPayload(new ByteInputStream(payload));
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(request.uri());
        sb.append("\n\troot=");
        sb.append(root);
        sb.append("\n\tpaths=");
        sb.append(Arrays.toString(paths));
        if (pathParams != null) {
            sb.append("\n\tpathParams=");
            sb.append(pathParams);
        }
        if (queryParams != null) {
            sb.append("\n\tqueryParams=");
            sb.append(queryParams);
        }
        if (request.headers() != null) {
            sb.append("\n\theaders=");
            for (Map.Entry<String, String> e : request.headers().entries()) {
                sb.append("\n\t");
                sb.append(e.getKey());
                sb.append(": ");
                sb.append(e.getValue());
            }
        }
        ByteBuf payload = getPayload();
        if (payload != null) {
            sb.append("\n\tpayload=[length=");
            payload.resetReaderIndex();
            sb.append(payload.readableBytes());
            sb.append("] ");
        }
        sb.append("\n");
        return sb.toString();
    }
}
