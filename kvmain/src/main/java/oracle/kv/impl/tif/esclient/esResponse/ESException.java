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

package oracle.kv.impl.tif.esclient.esResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * ESException wraps what is sent inside the structure of error. That is the
 * structure under the field name - "error" : { ESException Structure}
 *
 * While parsing the error json response, it skips many details like: -
 * rootCauses - stack_trace Reason is not much detail is required in the
 * Exception details. Currently FTS code does not use these details.
 *
 * see buildFromJson method.
 */
public final class ESException extends Exception
        implements JsonResponseObjectMapper<ESException> {

    private static final long serialVersionUID = 1L;

    private static final String STATUS = "status";
    private static final String ERROR = "error";
    private static final String TYPE = "type";
    private static final String REASON = "reason";
    private static final String CAUSED_BY = "caused_by";
    private static final String HEADER = "header";
    private static final String ROOT_CAUSE = "root_cause";

    private RestStatus errorStatus;
    private RestResponse restResponse;
    private String errorType;
    private String reason;
    private String header;
    private ESException cause;
    private List<ESException> rootCauses = new ArrayList<ESException>();
    private Map<String, List<String>> headerMap =
        new HashMap<String, List<String>>();
    private Map<String, List<String>> otherFields =
        new HashMap<String, List<String>>();

    public ESException() {
        super();
    }

    public ESException(RestResponse restResponse) {
        /* Make the httpEntity repeatable. */
        super(createMessage(restResponse));
        this.restResponse = restResponse;
        parseResponseException(restResponse);
    }

    public ESException(String msg) {
        super(msg);
    }

    public ESException(String msg, RestResponse restResp) {
        super(msg);
        this.restResponse = restResp;
    }

    public void setResponse(RestResponse response) {
        this.restResponse = response;
    }

    public String errorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String reason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public ESException cause() {
        return cause;
    }

    public void cause(ESException cause1) {
        this.cause = cause1;
    }

    public List<ESException> rootCauses() {
        return rootCauses;
    }

    public String header() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public Map<String, List<String>> headerMap() {
        return headerMap;
    }

    public void setHeaderMap(Map<String, List<String>> headerMap) {
        this.headerMap = headerMap;
    }

    public Map<String, List<String>> otherFields() {
        return otherFields;
    }

    public void setOtherFields(Map<String, List<String>> otherFields) {
        this.otherFields = otherFields;
    }

    public RestStatus errorStatus() {
        return errorStatus;
    }

    public void setErrorStatus(RestStatus errorStatus) {
        this.errorStatus = errorStatus;
    }

    private static String createMessage(RestResponse response) {
        String message = response.httpResponse().request().method() + " " +
                         response.host() + 
                         response.httpResponse().request().uri().toString() 
                         + ": " + String.valueOf(response.getStatusCode());

        if (response.httpResponse().body() != null) {
            message += "\n" + response.httpResponse().body();
        }
        return message;
    }

    /**
     * Returns the RestResponse for this exception.
     */
    public RestResponse response() {
        return restResponse;
    }

    /**
     * The parser is positioned at the start curly braces for error structure.
     */
    @Override
    public ESException buildFromJson(JsonParser parser) throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);

        // Go through every field in the error response.
        // Skip inner structures and keep the scalar value responses.
        String currentFieldName = null;
        while (token == JsonToken.FIELD_NAME) {
            currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isScalarValue()) {
                if (TYPE.equals(currentFieldName)) {
                    this.errorType = parser.getText();
                } else if (REASON.equals(currentFieldName)) {
                    this.reason = parser.getText();
                } else if (HEADER.equals(currentFieldName)) {
                    this.header = parser.getText();
                } else {
                    putOtherFieldEntry(currentFieldName, parser.getText());
                }
            } else if (token == JsonToken.START_OBJECT) {
                if (CAUSED_BY.equals(currentFieldName)) {
                    cause = new ESException();
                    cause.buildFromJson(parser);
                } else if (HEADER.equals(currentFieldName)) {
                    while ((token =
                        parser.nextToken()) != JsonToken.END_OBJECT) {
                        if (token == JsonToken.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == JsonToken.VALUE_STRING) {
                            putHeaderMapEntry(currentFieldName,
                                              parser.getText());
                        } else if (token == JsonToken.START_ARRAY) {
                            parser.skipChildren();
                        } else if (token == JsonToken.START_OBJECT) {
                            parser.skipChildren();
                        }

                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == JsonToken.START_ARRAY) {
                if (ROOT_CAUSE.equals(currentFieldName)) {
                    while ((token =
                        parser.nextToken()) != JsonToken.END_ARRAY) {
                        final ESException rootCause = new ESException();
                        rootCauses.add(rootCause.buildFromJson(parser));
                    }

                } else {
                    parser.skipChildren();
                }
            }

            token = parser.nextToken();
        }
        return this;
    }

    private void parseResponseException(RestResponse errorResponse) {

        String entity = errorResponse.httpResponse().body();
        if (entity == null) {
            this.errorStatus =
                RestStatus.fromCode(errorResponse.getStatusCode());
        } else {
            try {
                final JsonParser parser =
                    ESJsonUtil.createParser(entity);
                parseErrorResponseEntity(parser);
            } catch (Exception e) /* CHECKSTYLE:OFF */ {
                /* Could not parse the error response. */
            } /* CHECKSTYLE:ON */
        }

    }

    private void parseErrorResponseEntity(JsonParser parser)
        throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        String currentFieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if (STATUS.equals(currentFieldName)) {
                if (token == JsonToken.VALUE_NUMBER_INT) {
                    errorStatus = RestStatus.fromCode(parser.getIntValue());
                }
            } else if (ERROR.equals(currentFieldName)) {
                if (token == JsonToken.FIELD_NAME) {
                    // position parser at the start_object.
                    parser.nextToken();
                }
                buildFromJson(parser);
            }
        }

    }

    @Override
    public ESException buildErrorReponse(ESException e) {

        return e;
    }

    @Override
    public ESException buildFromRestResponse(RestResponse restResponse1) {
        return new ESException("Non Parseable Exception Response from ES",
                               restResponse1);
    }

    private void putHeaderMapEntry(String key, String value) {
        upsertKeyValue(key, value, headerMap);
    }

    private void putOtherFieldEntry(String key, String value) {
        upsertKeyValue(key, value, otherFields);
    }

    private void upsertKeyValue(
                                String key,
                                String value,
                                Map<String, List<String>> map) {
        if (map.get(key) == null) {
            final List<String> headerValues = new ArrayList<String>();
            headerValues.add(value);
            map.put(key, headerValues);
        } else {
            map.get(key).add(value);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ESException[type:");
        sb.append(errorType);
        sb.append(", reason:");
        sb.append("'" + reason + "'");
        sb.append(", errorStatus:").append(errorStatus);
        sb.append(", rootCauses:").append(rootCauses);
        return sb.toString();

    }
}
