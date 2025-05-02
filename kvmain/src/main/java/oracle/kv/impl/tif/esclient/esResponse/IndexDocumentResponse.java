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

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class IndexDocumentResponse extends ESWriteResponse implements
        JsonResponseObjectMapper<IndexDocumentResponse> {

    private static final String CREATED = "created";
    private static final String BULK_INDEX_ERROR = "error";

    private boolean created;

    public IndexDocumentResponse() {

    }

    public IndexDocumentResponse(String index,
            String type,
            String id,
            long seqNo,
            long primaryTerm,
            long version,
            boolean created,
            RestStatus restStatus) {
        super(index, type, id, seqNo, primaryTerm, version,
                created ? Result.CREATED: Result.UPDATED, restStatus);
    }

    /**
     * Returns the current version of the doc indexed.
     */
    public IndexDocumentResponse created(boolean created1) {
        this.created = created1;
        result(Result.CREATED);
        return this;
    }

    /**
     * Returns true if the document was created, false if updated.
     */
    public boolean isCreated() {
        return created;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexDocumentResponse[");
        builder.append("Successful=").append(isSuccessfulResponse());
        builder.append(",index=").append(index());
        builder.append(",type=").append(type());
        builder.append(",id=").append(id());
        builder.append(",version=").append(version());
        builder.append(",created=").append(isCreated());
        builder.append(",shards=").append(shardInfo());
        return builder.append("]").toString();
    }

    @Override
    public IndexDocumentResponse buildFromJson(JsonParser parser)
        throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            String currentFieldName = parser.currentName();
            if (RESULT.equals(currentFieldName)) {
                if (token.isScalarValue()) {
                    if (parser.getValueAsString().equals(CREATED)) {
                        created(true);
                        //other values we get in result are "updated" or "noop"
                    } 
                }
            } else {
                ESWriteResponse.buildFromJson(parser, this);
            }
        }
        parsed(true);
        return this;
    }

    /**
     * Error Info - Informs about the error type and reason only for Bulk index
     * other errors are handled by the ESException
     *
     */
    public static class ErrorInfo
        implements JsonResponseObjectMapper<ErrorInfo> {

        private static final String TYPE = "type";
        private static final String REASON = "reason";
        private static final String CAUSED_BY = "caused_by";

        private String errorType;
        private String errorReason;

        public ErrorInfo() {
        }

        public ErrorInfo(String errorType, String errorReason) { 
            this.errorType = errorType;
            this.errorReason = errorReason;
        }

        public String getErrorType() {
            return errorType;
        }

        public String getErrorReason() {
            return errorReason;
        }

        /**
         * Builds the structure "error" in the ES Response.
         */

        @Override
        public ErrorInfo buildFromJson(JsonParser parser)
            throws IOException {
            JsonToken token = parser.currentToken();
            ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
            String currentFieldName = null;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isScalarValue()) {
                    if (TYPE.equals(currentFieldName)) {
                        errorType = parser.getText();
                    } else if (REASON.equals(currentFieldName)) {
                        errorReason = parser.getText();
                    } else {
                        throw new IOException(new InvalidResponseException
                                ("Unknown Field: " +
                                 currentFieldName + " at:" +
                                 parser.currentTokenLocation()));
                    }
                } else {
                    if (CAUSED_BY.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) 
                                != JsonToken.END_OBJECT) {
                            //do nothing
                        }
                    } else {
                        throw new IOException(new InvalidResponseException
                                ("Unknown Field: " +
                                 currentFieldName + " at:" +
                                 parser.currentTokenLocation()));


                    }
                }
            }
            return this;
        }

        @Override
        public String toString() {
            return "ErrorInfo {" + "type= " + errorType + ", reason= " + 
                errorReason + " }";
        }

        @Override
        public ErrorInfo buildErrorReponse(ESException e) {
            return null;
        }

        @Override
        public ErrorInfo buildFromRestResponse(RestResponse restResp) {
            return new ErrorInfo();
        }
    }


    public IndexDocumentResponse buildFromJsonForBulkIndex(JsonParser parser)
        throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

            token = parser.currentToken();
            ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);

            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isScalarValue()) {
                if (_INDEX.equals(currentFieldName)) {
                    index(parser.getText());
                } else if (_TYPE.equals(currentFieldName)) {
                    type(parser.getText());
                } else if (_ID.equals(currentFieldName)) {
                    id(parser.getText());
                } else if (_VERSION.equals(currentFieldName)) {
                    version(parser.getLongValue());
                } else if (RESULT.equals(currentFieldName)) {
                    String res = parser.getText();
                    for (Result r : Result.values()) {
                        if (r.getLowercase().equals(res)) {
                            result(r);
                            if (r == Result.CREATED || r == Result.DELETED ||
                                    r == Result.UPDATED) {
                                successfulResponse = true;
                            }
                            break;
                        }
                    }
                } else if (FORCED_REFRESH.equals(currentFieldName)) {
                    forcedRefresh(parser.getBooleanValue());
                } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                    primaryTerm(parser.getLongValue());
                } else if (_SEQ_NO.equals(currentFieldName)) {
                    seqNo(parser.getLongValue());
                } else if (STATUS.equals(currentFieldName)) {

                } else {
                    throw new IOException(new InvalidResponseException(
                                "Unknown Field: " + currentFieldName + " at:" +
                                parser.currentTokenLocation()));
                }
            } else {
                if (BULK_INDEX_ERROR.equals(currentFieldName)) {
                    ErrorInfo eInfo = new ErrorInfo();
                    eInfo.buildFromJson(parser);
                } else if (_SHARDS.equals(currentFieldName)) {
                    ShardInfo shardInfo = new ShardInfo();
                    shardInfo(shardInfo.buildFromJson(parser));
                } else {
                 /*   throw new IOException(new InvalidResponseException(
                                "Unknown Field: " + currentFieldName + " at:" +
                                parser.currentTokenLocation()));*/
                }
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public IndexDocumentResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public IndexDocumentResponse buildFromRestResponse(RestResponse restResp) {
        switch (restResp.getStatusCode()) {
            case 200:
            case 201:
            case 202:
                this.successfulResponse = true;
                break;
            default:
                this.successfulResponse = false;
        }

        return this;
    }

}
