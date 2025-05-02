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

package oracle.kv.impl.tif.esclient.esRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Bulk Request is a collection of different types of ESWriteRequests. The
 * format for Rest Based Bulk Request is: (from elasticsearch documentation
 * webapge:
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs
 * -bulk.html)
 * 
 * 
 * action_and_meta_data\n
 * 
 * optional_source\n
 * 
 * action_and_meta_data\n
 * 
 * optional_source\n
 * 
 * ....
 * 
 * action_and_meta_data\n
 * 
 * optional_source\n
 * 
 * 
 * "\n" is a required separator. Moreover, the content-type for this request is
 * application/x-ndjson
 * 
 * Note that for BulkRequest properties index and type only make sense when all
 * of the individual request are to same index and type.
 * 
 *
 */
public class BulkRequest extends ESRequest<BulkRequest> implements
        ESRestRequestGenerator {

    private long byteSize;

    public static int PER_REQUEST_BYTE_OVERHEAD = 50;

    List<ESWriteRequest<?>> writeRequests = new ArrayList<ESWriteRequest<?>>();

    public BulkRequest() {

    }

    public <T extends ESRequest<T>> BulkRequest add(ESWriteRequest<T> req) {
        Objects.requireNonNull(req);
        if (req instanceof IndexDocumentRequest) {
            IndexDocumentRequest indexReq = (IndexDocumentRequest) req;
            writeRequests.add(indexReq);
            byteSize += (indexReq.source() != null ? indexReq.source().length
                    : 0) + PER_REQUEST_BYTE_OVERHEAD;
        } else if (req instanceof DeleteRequest) {
            DeleteRequest delReq = (DeleteRequest) req;
            writeRequests.add(delReq);
            byteSize += PER_REQUEST_BYTE_OVERHEAD;
        }

        return this;

    }

    public long byteSize() {
        return byteSize;
    }

    public int nRequestItems() {
        return writeRequests.size();
    }

    /*
     * Need to build a json as follows for BulkRequest. It is of the form :
     * Header Data /n Request Data /n { "index" : { "_index" : "test", "_type"
     * : "type1", "_id" : "1" } } { "field1" : "value1" } { "delete" : {
     * "_index" : "test", "_type" : "type1", "_id" : "2" } } { "create" : {
     * "_index" : "test", "_type" : "type1", "_id" : "3" } } { "field1" :
     * "value3" } { "update" : {"_id" : "1", "_type" : "type1", "_index" :
     * "test"} } { "doc" : {"field2" : "value2"} }
     */
    @Override
    public RestRequest generateRestRequest() {

        ByteArrayOutputStream bulkRequestContent = new ByteArrayOutputStream();
        for (ESWriteRequest<?> reqItem : writeRequests) {

            try {
                ByteArrayOutputStream headerByteStream =
                        new ByteArrayOutputStream();
                JsonGenerator headerBuilder = ESJsonUtil
                        .createGenerator(headerByteStream);
                headerBuilder.writeStartObject(); // Header Data start
                headerBuilder.writeFieldName(reqItem.requestType().toString()
                                                    .toLowerCase());
                headerBuilder.writeStartObject(); // requestType Data Start
                headerBuilder.writeStringField("_index", reqItem.index());
                headerBuilder.writeStringField("_id", reqItem.id());
                headerBuilder.writeEndObject(); // requestType Data End
                headerBuilder.writeEndObject(); // Header Data End
                headerBuilder.flush();
                // Now need to put the JsonContentSeperator before putting
                // the request data.
                bulkRequestContent.write(headerByteStream.toByteArray());
                headerBuilder.close();
                bulkRequestContent.write(ESWriteRequest.JSONItemSeperator);

                if (reqItem.requestType() == RequestType.INDEX) {
                    IndexDocumentRequest indexReq =
                            (IndexDocumentRequest) reqItem;
                    byte[] source = indexReq.source();
                    if (source != null && source.length > 0) {
                        bulkRequestContent.write(source);
                        bulkRequestContent.write(ESWriteRequest
                                                 .JSONItemSeperator);
                    }
                }
            } catch (IOException e) {
                // Can not move ahead without bulk request.
                throw new RuntimeException(e);
            }
        }

        RequestParams parameters = new RequestParams();
        /*
         *TODO: For Debugging Purposes-Leave it for a while.
         * 
         *try {
         *   System.out.println(new String(bulkRequestContent.toByteArray(),
         *                                 StandardCharsets.UTF_8.name()));
         * } catch (UnsupportedEncodingException e) {
         */
        
        return new RestRequest(ESHttpMethods.HttpPost, "/_bulk",
                               bulkRequestContent.toByteArray(),
                               parameters.getParams(),
                               new RequestParams().putParam("Content-Type",
                                      getContentType()).getParams());
    }

    public static String getContentType() {
        return "application/x-ndjson; charset=UTF-8";
    }

    @Override
    public RequestType requestType() {
        return RequestType.BULK_INDEX;
    }

}
