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

import oracle.kv.impl.tif.esclient.esRequest.ESRequest.RequestType;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class BulkItemResponse extends ESResponse implements
        JsonResponseObjectMapper<BulkItemResponse> {

    private final int itemSequenceNo;
    private ESWriteResponse itemResponse;
    private RestStatus itemStatus;
    private boolean error;
    private String index;
    private String type;
    private String id;
    private RequestType itemType;
    private ESException esException;

    public BulkItemResponse(int itemSequenceNo) {
        this.itemSequenceNo = itemSequenceNo;
    }

    public BulkItemResponse(int itemSequenceNo,
            ESWriteResponse itemResponse,
            RestStatus itemStatus,
            boolean error,
            String index,
            String type,
            String id,
            ESException esException,
            RequestType itemType) {
        this.itemSequenceNo = itemSequenceNo;
        this.itemResponse = itemResponse;
        this.itemStatus = itemStatus;
        this.error = error;
        this.index = index;
        this.type = type;
        this.id = id;
        this.itemType = itemType;
        this.esException = esException;

    }

    public ESWriteResponse itemResponse() {
        return itemResponse;
    }

    public int getItemSequenceNo() {
        return itemSequenceNo;
    }

    public void setItemResponse(ESWriteResponse itemResponse) {
        this.itemResponse = itemResponse;
    }

    public RestStatus itemStatus() {
        return itemStatus;
    }

    public void setItemStatus(RestStatus itemStatus) {
        this.itemStatus = itemStatus;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public RequestType itemType() {
        return itemType;
    }

    public void setRequestType(RequestType requestType) {
        this.itemType = requestType;
    }

    public ESException getEsException() {
        return esException;
    }

    public void setEsException(ESException esException) {
        this.esException = esException;
    }

    /**
     * Parse a particular bulk item structure. It is of the form: 
     * 
     * {
     * 
     * "requestType": {
     * 
     * "ItemResponse"
     * 
     * }
     * 
     * }
     * 
     * 
     * Different possible Bulk Item Response structure look as follows:
     * 
     * {
     * 
     * "index": {
     * 
     * IndexResponse structure.
     * 
     * },
     * 
     * "index": {
     * 
     * "_index": "index_name",
     * 
     * "_type": "index_type",
     * 
     * "_id": "document_id",
     * 
     * "error": {
     * 
     * ESException structure.i.e error
     * 
     * },
     * 
     * "status": INT_VALUE
     * 
     * },
     * 
     * "delete": {
     * 
     * Delete Response structure
     * 
     * },
     * 
     * "delete": {
     * 
     * "_index": "index_name",
     * 
     * "_type": "index_type",
     * 
     * "_id": "document_id",
     * 
     * "error": {
     * 
     * ESException structure.i.e error
     * 
     * },
     * 
     * "status": INT_VALUE
     * 
     * }
     * 
     * }
     * 
     * Bulk Response buildFromJson positions parser at Start of Bulk Item
     * Response Structure.
     */

    @Override
    public BulkItemResponse buildFromJson(JsonParser parser)
        throws IOException {
        // The parser is positioned at Bulk Item Start Structure.
        // While loop below runs till Bulk Item End Structure.
        ESJsonUtil.validateToken(JsonToken.START_OBJECT,
                                 parser.currentToken(), parser);

        JsonToken token = parser.nextToken(); // The fieldName is the
                                              // requestType -"index" or
                                              // "delete"
        ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();

        final RequestType reqType = RequestType.get(currentFieldName);
        if (reqType == null) {
            throw new JsonParseException(parser,
                                         "Unknown RequestType" +
                                         " - only index,delete,update," +
                                         "create is allowed");
        }
        this.itemType = reqType;

        /*
         * An item in error has a field name called error and status inside the
         * item response. It also has fields common to ESWriteResponse. So the
         * logic of parsing a bulk item would be: first see if the nextToken is
         * field name called error or status. If this is true, then parse error
         * structure or status field, Else, it is a partial response from
         * ESWriteResponse or full response for request type. Both partial or
         * full response will parsed by the JsonResponseConverter corresponding
         * to the request type.
         * 
         * Note that ESWriteReponse buildFromJson is not aware of error or
         * status so it will only parse index,id and type in error item
         * response.
         */
        while (token != JsonToken.END_OBJECT) {
            if (reqType == RequestType.INDEX || reqType 
                    == RequestType.CREATE) {
                IndexDocumentResponse indexResponse 
                = new IndexDocumentResponse();
                // Parser is before Start of Index Structure.
                try {
                    indexResponse.buildFromJsonForBulkIndex(parser);
                } catch (IOException ioe) {
                    if (ioe.getCause() instanceof InvalidResponseException) {
                        throw new JsonParseException(parser, ioe.getMessage());
                    }
                }
                // Parser is at End of Index Structure.
                this.itemResponse = indexResponse;

            } else if (reqType == RequestType.DELETE) {
                DeleteResponse deleteResponse = new DeleteResponse();
                try {
                    deleteResponse.buildFromJson(parser);
                } catch (IOException ioe) {
                    if (ioe.getCause() instanceof InvalidResponseException) {
                        throw new JsonParseException(parser, ioe.getMessage());
                    }
                }
                this.itemResponse = deleteResponse;

            }
            /*Index Response does not have status field,
             * but bulk item of reqType index has status.
             * This will cause InvalidResponseException.
             * For now it works because status is last field
             * in LinkedHashMap of the response.
             * May be coming out of InvalidResponseException.
             * Get the field Name again.
             * And if field is status,move the token.
             */
            token = parser.nextToken(); // Parser is at end of this Bulk Item.

        }

        /* Parser should be at end of Bulk Item structure */
        ESJsonUtil.validateToken(JsonToken.END_OBJECT, token, parser);
        this.index = itemResponse.index();
        this.type = itemResponse.type();
        this.id = itemResponse.id();

        parsed(true);
        return this;
    }

    @Override
    public BulkItemResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public BulkItemResponse buildFromRestResponse(RestResponse restResp) {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BulkItem: {");
        builder.append(" itemSeqNo:" + getItemSequenceNo());
        builder.append(" requestType:" + itemType);
        builder.append(" index:" + index);
        builder.append(" type:" + type);
        builder.append(" id:" + id);
        builder.append(" }");
        return builder.toString();

    }

}
