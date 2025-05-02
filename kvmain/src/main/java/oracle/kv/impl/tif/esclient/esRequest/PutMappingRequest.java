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

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * 
 * Puts a new mapping for an index. 
 * This does not merge any existing old mapping which exists for this index and type.
 * This does not take care of reserved words in mapping spec.
 * So Caller should take care that mapping spec does not have reserved words.
 * 
 * However, adding new fields is possible by just passing the new fields schema.
 * 
 * Uses indexType as part of the endpoint.
 * 
 * PUT twitter/_mapping/user
 * {
 * 
 *   "properties": {
 *   
 *       "name" : {
 *       
 *          "type" : "text"
 *          
 *       }
 *    
 *    }
 *    
 * }
 * 
 * This class depends on this Json to be passed as JsonGenerator.
 * 
 * 
 *
 */

public class PutMappingRequest extends ESRequest<PutMappingRequest> implements
        ESRestRequestGenerator {

    private byte[] source;

    public PutMappingRequest() {

    }

    public PutMappingRequest(String index, byte[] source) {
        super(index);
        this.source = source;
    }

    public PutMappingRequest(String index,
            JsonGenerator mappingSpec) throws IOException {
        super(index);
        mappingSpec.flush();
        this.source =
            ((ByteArrayOutputStream) mappingSpec.getOutputTarget())
                                                .toByteArray();
    }

    byte[] source() {
        return source;
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
               new InvalidRequestException("index name is not provided");
        }
        if (source == null || source.length <= 0) {
            exception =
               new InvalidRequestException("Index Settings are not provided");
        }
        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = ESHttpMethods.HttpPut;
        String endpoint = endpoint(index(), "_mapping");
        RequestParams parameters = new RequestParams();
        return new RestRequest(method, endpoint, source(),
                               parameters.getParams(),
                               new RequestParams().putParam("Content-Type",
                                      getContentType()).getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.PUT_MAPPING;
    }

    public static String getContentType() {
        return "application/json; charset=UTF-8";
    }
}
