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
import java.util.Locale;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public abstract class ESWriteResponse extends ESResponse {

    protected static final String _SHARDS = "_shards";
    protected static final String _SEQ_NO = "_seq_no";
    protected static final String _PRIMARY_TERM = "_primary_term";
    protected static final String RESULT = "result";
    protected static final String FORCED_REFRESH = "forced_refresh";
    protected static final String STATUS = "status";

    public enum Result {
        CREATED(0), UPDATED(1), DELETED(2), NOT_FOUND(3), NOOP(4);

        private final byte code;
        private final String lowercase;

        Result(int code) {
            this.code = (byte) code;
            this.lowercase = this.toString().toLowerCase(Locale.ENGLISH);
        }

        public byte getCode() {
            return code;
        }

        public String getLowercase() {
            return lowercase;
        }

        public static Result get(Byte code) {
            switch (code) {
                case 0:
                    return CREATED;
                case 1:
                    return UPDATED;
                case 2:
                    return DELETED;
                case 3:
                    return NOT_FOUND;
                case 4:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown result code: " +
                            code);
            }
        }

    }

    private String index;
    private String id;
    private String type;
    private long version;
    private long seqNo;
    private long primaryTerm;
    private boolean forcedRefresh;
    private ShardInfo shardInfo;
    protected Result result;
    protected RestStatus restStatus;

    protected boolean successfulResponse;

    public ESWriteResponse() {

    }

    public ESWriteResponse(String index,
            String type,
            String id,
            long seqNo,
            long primaryTerm,
            long version,
            Result result,
            RestStatus restStatus) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.result = result;
        this.restStatus = restStatus;
    }

    public ESWriteResponse index(String index1) {
        this.index = index1;
        return this;
    }

    public ESWriteResponse id(String id1) {
        this.id = id1;
        return this;
    }

    public ESWriteResponse type(String type1) {
        this.type = type1;
        return this;
    }

    public ESWriteResponse version(long version1) {
        this.version = version1;
        return this;
    }

    public ESWriteResponse seqNo(long seqNo1) {
        this.seqNo = seqNo1;
        return this;
    }

    public ESWriteResponse primaryTerm(long primaryTerm1) {
        this.primaryTerm = primaryTerm1;
        return this;
    }

    public ESWriteResponse result(Result result1) {
        this.result = result1;
        return this;
    }

    public ESWriteResponse shardInfo(ShardInfo shardInfo1) {
        this.shardInfo = shardInfo1;
        return this;
    }

    public ESWriteResponse forcedRefresh(boolean forcedRefresh1) {
        this.forcedRefresh = forcedRefresh1;
        return this;
    }

    public ESWriteResponse restStatus(RestStatus restStatus1) {
        this.restStatus = restStatus1;
        return this;
    }

    /**
     * The index the document was indexed into.
     */
    public String index() {
        return this.index;
    }

    /**
     * The type of the document indexed.
     */
    public String type() {
        return this.type;
    }

    public ShardInfo shardInfo() {
        return shardInfo;
    }

    /**
     * The change that occurred to the document.
     */
    public Result result() {
        return result;
    }

    /**
     * The id of the document changed.
     */
    public String id() {
        return this.id;
    }

    /**
     * Returns the current version of the doc.
     */
    public long version() {
        return this.version;
    }

    /**
     * Returns the sequence number assigned for this change.
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term assigned for this change.
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Did this request force a refresh?
     */
    public boolean isForcedRefresh() {
        return forcedRefresh;
    }

    /**
     * This is restStatus of this request. TODO: see if rest status of a
     * request is equivalent to http response status code. Otherwise this needs
     * to be evaluated from Shard's rest status.
     * 
     */
    public RestStatus restStatus() {
        return restStatus;
    }

    /**
     * This will return OK only when all replicas were successful. This status
     * needs to be used for consistency constrained indexing operation. For
     * operations that do not care about consistency, use restStatus which is
     * set directly by ESRestClient from the rest response.
     * 
     * @return shard status
     */
    public RestStatus shardStatus() {
        return shardInfo().shardStatus();
    }

    /*
     * This method is made static, so that sub classes can use it.
     * 
     */

    public boolean isSuccessfulResponse() {
        return successfulResponse;
    }

    public void successfulResponse(boolean successfulResponse1) {
        this.successfulResponse = successfulResponse1;
    }

    /*
       write response example in elasticsearch in 8.8.1
    {
      "index": {
        "_index": "ondb.mystore.fulltext.textindex0",
        "_id": "100",
        "_version": 1,
        "result": "created",
        "_shards": {
          "total": 3,
          "successful": 1,
          "failed": 0
        },
        "_seq_no": 4864,
        "_primary_term": 1,
        "status": 201
      }
    */

    protected static void
            buildFromJson(JsonParser parser, ESWriteResponse responseObject)
                throws IOException {
        JsonToken token = parser.currentToken();
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        if (token.isScalarValue()) {
            if (_INDEX.equals(currentFieldName)) {
                // index uuid and shard id are unknown and can't be parsed back
                // for now.
                responseObject.index(parser.getText());
            } else if (_TYPE.equals(currentFieldName)) {
                responseObject.type(parser.getText());
            } else if (_ID.equals(currentFieldName)) {
                responseObject.id(parser.getText());
            } else if (_VERSION.equals(currentFieldName)) {
                responseObject.version(parser.getLongValue());
            } else if (RESULT.equals(currentFieldName)) {
                String result = parser.getText();
                for (Result r : Result.values()) {
                    if (r.getLowercase().equals(result)) {
                        responseObject.result(r);
                        if (r == Result.CREATED || r == Result.DELETED ||
                                r == Result.UPDATED) {
                            responseObject.successfulResponse = true;
                        }
                        break;
                    }
                }
            } else if (STATUS.equals(currentFieldName)) {
                // put to es cluster includes the status of response also   
            } else if (FORCED_REFRESH.equals(currentFieldName)) {
                responseObject.forcedRefresh(parser.getBooleanValue());
            } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                responseObject.primaryTerm(parser.getLongValue());
            } else if (_SEQ_NO.equals(currentFieldName)) {
                responseObject.seqNo(parser.getLongValue());
            } else {
                throw new IOException(
                        new InvalidResponseException("Unknown Field: " +
                            currentFieldName + " at:" +
                            parser.currentTokenLocation()));
            }
        } else if (token == JsonToken.START_OBJECT) {
            if (_SHARDS.equals(currentFieldName)) {
                ShardInfo shardInfo = new ShardInfo();
                responseObject.shardInfo(shardInfo.buildFromJson(parser));
            } else {
                throw new IOException(
                        new InvalidResponseException("Unknown Field: " +
                            currentFieldName + " at:" +
                            parser.currentTokenLocation()));
            }
        } else {
            throw new IOException(
                    new InvalidResponseException("Unknown Token: " +
                        token + " at:" + parser.currentTokenLocation()));
        }
    }
}
