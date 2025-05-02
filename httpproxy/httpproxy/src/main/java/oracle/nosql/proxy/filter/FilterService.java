/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.filter;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.CONTENT_TYPE;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.proxy.protocol.HttpConstants.APPLICATION_JSON_NOCHARSET;
import static oracle.nosql.proxy.protocol.HttpConstants.FILTERS_PATH;
import static oracle.nosql.proxy.protocol.HttpConstants.pathInURIAllVersions;
import static oracle.nosql.proxy.protocol.Protocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.Protocol.RESOURCE_EXISTS;
import static oracle.nosql.proxy.protocol.Protocol.UNKNOWN_OPERATION;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotEmpty;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotNull;
import static oracle.nosql.proxy.protocol.JsonProtocol.checkNotNullEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import oracle.nosql.common.JsonBuilder;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.Service;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.filter.FilterHandler.PullRulesResult;
import oracle.nosql.proxy.filter.FilterHandler.RuleWrapper;
import oracle.nosql.util.filter.Rule;
import oracle.nosql.util.filter.Rule.ActionType;
import oracle.nosql.util.filter.Rule.OpType;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonPayload;
import oracle.nosql.proxy.rest.RequestParams;
import oracle.nosql.proxy.rest.UrlInfo;
import oracle.nosql.proxy.protocol.JsonProtocol.JsonArray;

/**
 * Filter service provides rest API to manage the rules:
 *
 *  POST /V0/tools/filters
 *    add or update a rule.
 *
 *  GET /V0/tools/filters/{ruleName}
 *    get a rule with given name.
 *
 *  DELETE /V0/tools/filters/{ruleName}
 *    delete a rule with given name.
 *
 *  GET /V0/tools/filters[?all=true]
 *    list all rules.
 *
 *  PUT /V0/tools/filters/reload
 *    reload persistent rules to cache
 */
public class FilterService implements Service {
    static final String NAME = "name";
    static final String ACTION = "action";
    static final String OPERATIONS = "operations";
    static final String TENANT = "tenant";
    static final String USER = "user";
    static final String TABLE = "table";

    /*
     * Response buffer size, minimum. Consider adjusting this per-request,
     * or other mechanism to reduce the frequency of resizing the buffer.
     */
    private static final int RESPONSE_BUFFER_SIZE = 1024;

    private enum RuleOp {
        PUT_RULE,
        GET_RULE,
        DELETE_RULE,
        LIST_RULES,
        RELOAD_RULES
    }

    private final Map<HttpMethod, Map<UrlInfo, RuleOp>> methods =
                                                        new HashMap<>();
    private void initMethods() {
        /* Add rule */
        initMethod(HttpMethod.POST, FILTERS_PATH, RuleOp.PUT_RULE);

        /* Get rule */
        initMethod(HttpMethod.GET, FILTERS_PATH + "/{" + NAME + "}",
                   RuleOp.GET_RULE);

        /* Delete rule */
        initMethod(HttpMethod.DELETE, FILTERS_PATH + "/{" + NAME + "}",
                   RuleOp.DELETE_RULE);

        /* List rules */
        initMethod(HttpMethod.GET, FILTERS_PATH, RuleOp.LIST_RULES);

        /* Reload persistent rules */
        initMethod(HttpMethod.PUT, FILTERS_PATH + "/reload",
                   RuleOp.RELOAD_RULES);
    }

    private final Map<RuleOp, RuleOperation> operations = new HashMap<>();
    private void initOperations() {
        operations.put(RuleOp.PUT_RULE, this::handleAddRule);
        operations.put(RuleOp.GET_RULE, this::handleGetRule);
        operations.put(RuleOp.DELETE_RULE, this::handleDeleteRule);
        operations.put(RuleOp.LIST_RULES, this::handleListRules);
        operations.put(RuleOp.RELOAD_RULES, this::handleReloadRules);
    }

    private Map<Integer, HttpResponseStatus> errorCodes = new HashMap<>();
    private void initErrorCodes() {
        errorCodes.put(ILLEGAL_ARGUMENT,  HttpResponseStatus.BAD_REQUEST);
        errorCodes.put(UNKNOWN_OPERATION,  HttpResponseStatus.NOT_FOUND);
        errorCodes.put(RESOURCE_NOT_FOUND,  HttpResponseStatus.NOT_FOUND);
        errorCodes.put(RESOURCE_EXISTS,  HttpResponseStatus.CONFLICT);
    }

    private final FilterHandler handler;
    private final SkLogger logger;

    public FilterService(FilterHandler handler,
                         SkLogger logger) {
        this.handler = handler;
        this.logger = logger;

        initMethods();
        initOperations();
        initErrorCodes();
    }

    @Override
    public boolean lookupService(String uri) {
        return pathInURIAllVersions(uri, FILTERS_PATH);
    }

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc) {

        /*
         * Allocate the output buffer and stream here to simplify reference
         * counting and ensure that the stream is closed.
         */
        ByteBuf payload = ctx.alloc().directBuffer(RESPONSE_BUFFER_SIZE);

        FullHttpResponse resp;
        try {
            HttpMethod method = request.method();

            /* Create response */
            resp = createResponse(payload);

            try {
                RequestParams params = new RequestParams(request);
                RuleOp opCode = findOp(method, params);
                RuleOperation ruleOp = operations.get(opCode);
                if (ruleOp == null) {
                    throw new RequestException(UNKNOWN_OPERATION,
                            "Unknown rule opertion: " + opCode);
                }

                ruleOp.handle(resp, params, opCode, lc);
            } catch (IllegalArgumentException iae) {
                throw new RequestException(ILLEGAL_ARGUMENT, iae.getMessage());
            }
        } catch (RequestException ex) {
            logger.warning("Handle " + request.uri() + " failed: " +
                           ex.getErrorCode() + ", " + ex.getMessage());
            resp = createErrorResponse(payload,
                                       mapStatusCode(ex),
                                       ex.getMessage());
        } catch (Throwable ex) {
            /*
             * include stack trace here because this path should be
             * infrequent and may represent a bug in the filter service
             */
            logger.logEvent("Filters" /* category */, Level.WARNING,
                            "Request exception" /* subject */,
                            ex.getMessage() /* message */, ex);
            resp = createErrorResponse(payload,
                                       HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                       ex.getMessage());
        }
        return resp;
    }

    private void handleAddRule(FullHttpResponse response,
                               RequestParams request,
                               RuleOp op,
                               LogContext lc) {

        JsonPayload pl = null;
        try {
            pl = request.parsePayload();
            if (pl == null) {
                throw new RequestException(ILLEGAL_ARGUMENT,
                    "The payload of " + op.name() +
                    " request must not be empty");
            }

            Rule rule = parseRule(pl);
            RuleWrapper ret = handler.addRule(rule);

            /* build response */
            buildRuleResponse(response, ret);
        } catch (IOException ioe) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "Invalid payload of " + op.name() + ": " + ioe.getMessage());
        } finally {
            if (pl != null) {
                pl.close();
            }
        }
    }

    /* Parses a rule object from JSON payload */
    private Rule parseRule(JsonPayload pl) throws IOException {
        ActionType action = null;
        String name = null;
        String tenant = null;
        String user = null;
        String table = null;
        List<String> ops = null;

        while (pl.hasNext()) {
            if (pl.isField(NAME)) {
                name = pl.readString();
                checkNotEmpty(NAME, name);
            } else if (pl.isField(ACTION)) {
                String value = pl.readString();
                checkNotEmpty(ACTION, value);
                if (value != null) {
                    action = ActionType.valueOf(value);
                }
            } else if (pl.isField(TENANT)) {
                tenant = pl.readString();
                checkNotEmpty(TENANT, tenant);
            } else if (pl.isField(USER)) {
                user = pl.readString();
                checkNotEmpty(USER, user);
            } else if (pl.isField(TABLE)) {
                table = pl.readString();
                checkNotEmpty(TABLE, table);
            } else if (pl.isField(OPERATIONS)) {
                ops = new ArrayList<>();
                JsonArray ja = pl.readArray();
                while(ja.hasNext()) {
                    String op = ja.readString();
                    checkNotNullEmpty("element of " + OPERATIONS, op);
                    ops.add(op);
                }
            } else {
                throw new IllegalArgumentException("Invalid field of Rule: " +
                    pl.getCurrentField());
            }
        }

        checkNotNull(NAME, name);

        if (tenant == null && user == null && table == null && ops == null) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                "One of properties must be specified: " + TENANT + ", " + USER +
                ", " + TABLE + ", " + OPERATIONS);
        }

        return Rule.createRule(name, action, tenant, user, table,
                (ops != null ? ops.toArray(new String[ops.size()]) : null),
                System.currentTimeMillis());
    }

    /* Gets a rule */
    private void handleGetRule(FullHttpResponse response,
                               RequestParams request,
                               RuleOp op,
                               LogContext lc) {
        final String name = request.getPathParam(NAME);
        checkNotNullEmpty(NAME, name);

        RuleWrapper ret = handler.getRule(name);

        /* build response */
        buildRuleResponse(response, ret);
    }

    /*
     * Lists all the rules
     *
     *  GET /V0/tools/filters[?all=true]
     *
     * If all is false or not specified, return all transient rules. Otherwise
     * return all rules in cache.
     */
    private void handleListRules(FullHttpResponse response,
                                 RequestParams request,
                                 RuleOp op,
                                 LogContext lc) {

        final boolean all = request.getQueryParamAsBoolean("all", false);
        final Iterator<RuleWrapper> iter = handler.rulesIterator(all);

        /* Build response */
        JsonBuilder jb = JsonBuilder.create(false);
        while (iter.hasNext()) {
            jb.startObject(null);
            appendRule(jb, iter.next(), all);
            jb.endObject();
        }
        buildResponse(response, jb.toString());
    }

    /* Delete a rule */
    private void handleDeleteRule(FullHttpResponse response,
                                  RequestParams request,
                                  RuleOp op,
                                  LogContext lc) {

        final String name = request.getPathParam(NAME);
        checkNotNullEmpty(NAME, name);

        handler.deleteRule(name);

        /* build response */
        buildResponse(response,
                      JsonBuilder.create().append(name, "deleted").toString());
    }

    /* Reload persistent rules */
    private void handleReloadRules(FullHttpResponse response,
                                   RequestParams request,
                                   RuleOp op,
                                   LogContext lc) {

        final PullRulesResult result = handler.reloadPersistentRules(lc);

        JsonBuilder jb = JsonBuilder.create();
        jb.append("numLoaded", result.getNumLoaded())
          .append("numDeleted", result.getNumDeleted())
          .append("time", result.getTimeMs() + "ms");

        /* build response */
        buildResponse(response, jb.toString());
    }

    /**
     * Finds the http method to handle the given request.
     *
     * It compares the http method URL path with the given request's URL in
     * case sensitive manner.
     */
    private RuleOp findOp(HttpMethod method, RequestParams request) {
        final Map<UrlInfo, RuleOp> urlOps = methods.get(method);
        if (urlOps != null) {
            for (Entry<UrlInfo, RuleOp> e : urlOps.entrySet()) {
                UrlInfo url = e.getKey();
                if (url.match(request)) {
                    /*
                     * Found the method, parse the parameters from request path
                     * and store to RequestParams.pathParam.
                     */
                    for (int index : url.getIndexParams()) {
                        request.addPathParam(url.getParamName(index), index);
                    }
                    return e.getValue();
                }
            }
        }
        throw new RequestException(ILLEGAL_ARGUMENT,
                                   "Unsupported request " + request.getUri());
    }

    private void initMethod(HttpMethod method, String url, RuleOp opCode) {
        Map<UrlInfo, RuleOp> urlOps = methods.get(method);
        if (urlOps == null) {
            urlOps = new HashMap<>();
            methods.put(method, urlOps);
        }
        urlOps.put(new UrlInfo(url), opCode);
    }

    /* Creates http response */
    private static FullHttpResponse createResponse(ByteBuf payload) {

        FullHttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1,
            HttpResponseStatus.OK, payload);
        /*
        * It's necessary to add the header below to allow script-based
        * access from browsers.
        */
        resp.headers()
            .set(CONTENT_TYPE, APPLICATION_JSON)
            .setInt(CONTENT_LENGTH, 0);
        return resp;
    }

    /* Creates error response */
    private static FullHttpResponse createErrorResponse(
            ByteBuf payload,
            HttpResponseStatus status,
            String message) {

        FullHttpResponse resp =
            new DefaultFullHttpResponse(HTTP_1_1, status, payload);

        String body = JsonBuilder.create()
                        .append("code", status)
                        .append("message", message)
                        .toString();
        payload.writeCharSequence(body, UTF_8);

        HttpHeaders headers = resp.headers();
        headers.set(CONTENT_TYPE, APPLICATION_JSON_NOCHARSET)
            .setInt(CONTENT_LENGTH, payload.readableBytes());
        return resp;
    }

    private HttpResponseStatus mapStatusCode(RequestException ex) {
        HttpResponseStatus errorCode = errorCodes.get(ex.getErrorCode());
        if (errorCode != null) {
            return errorCode;
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

    private static void buildResponse(FullHttpResponse resp, String info) {
        ByteBuf payload = resp.content();
        if (info != null) {
            payload.writeCharSequence(info, UTF_8);
        }
        resp.headers().setInt(CONTENT_LENGTH, payload.readableBytes());
    }

    private static void buildRuleResponse(FullHttpResponse response,
                                          RuleWrapper rw) {
        final JsonBuilder jb = JsonBuilder.create();
        appendRule(jb, rw, false);
        buildResponse(response, jb.toString());
    }

    private static void appendRule(JsonBuilder jb,
                                   RuleWrapper rw,
                                   boolean showType) {
        Rule r = rw.getRule();
        String name = rw.getRuleName();
        jb.append("name", name);
        if (showType) {
            jb.append("type", (rw.isTransient() ? "TRANSIENT" : "PERSISTENT"));
        }
        jb.append("action", r.getAction().name());
        if (r.getTenant() != null) {
            jb.append("tenant", r.getTenant());
        }
        if (r.getUser() != null) {
            jb.append("user", r.getUser());
        }
        if (r.getTable() != null) {
            jb.append("table", r.getTable());
        }
        jb.startArray("operations");
        for (OpType e : r.getOpTypes()) {
            jb.append(e.name());
        }
        jb.endArray();

        if (r.getCreateTimeMs() > 0) {
            jb.append("createTime", r.getCreateTime());
        }
    }

    /*
     * Interface for handling operation requests.
     */
    @FunctionalInterface
    private interface RuleOperation {
        void handle(FullHttpResponse response,
                    RequestParams request,
                    RuleOp op,
                    LogContext lc);
    }
}
