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
import java.util.List;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class BulkResponse extends ESResponse implements
        JsonResponseObjectMapper<BulkResponse> {

    private static final String TOOK = "took";
    private static final String ERRORS = "errors";
    private static final String ITEMS = "items";

    private List<BulkItemResponse> itemResponses =
        new ArrayList<BulkItemResponse>();
    private long tookInMillis;
    /*
     * Bulk Response has errors field in the header part. However, it is not
     * there in older versions of ES. So, there is another method called
     * getErrorFromItems to know wether there were any error in any item.
     */
    private Boolean errors = null;

    public BulkResponse() {
    }

    public BulkResponse(List<BulkItemResponse> itemResponses,
            long tookInMillis,
            boolean errors) {
        this.itemResponses = itemResponses;
        this.tookInMillis = tookInMillis;
        this.errors = errors;
    }

    /**
     * How long the bulk execution took. Excluding ingest preprocessing.
     */
    public long tookInMillis() {
        return tookInMillis;
    }

    public BulkResponse tookInMillis(long tookInMillis1) {
        this.tookInMillis = tookInMillis1;
        return this;
    }

    public List<BulkItemResponse> itemResponses() {
        return itemResponses;
    }

    public BulkResponse itemResponses(List<BulkItemResponse> itemResponses1) {
        this.itemResponses = itemResponses1;
        return this;
    }

    public Boolean errors() {
        return errors;
    }

    public BulkResponse errors(Boolean errors1) {
        this.errors = errors1;
        return this;
    }

    public boolean getErrorFromItems() {
        for (BulkItemResponse item : itemResponses()) {
            if (item.isError()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Bulk Response structure is as follows: 
     * 
     * {  // Bulk Response Structure Start - This is parser.nextToken()
     * 
     * "took" : 6,
     * 
     * "errors" : true,
     * 
     * "items" : [  // Bulk Items Array Start
     * 
     *              {  // Bulk Item Structure Start
     * 
     *                "index" : {    // Index Response Structure Start
     *                
     *                               Index Response Structure 
     *                               
     *                          }
     *                          
     *              },
     *              
     *              
     *              {
     *              
     *                "delete" : { Delete Response Structure }
     *                
     *              },
     *              
     *              
     *              ...
     *              {
     *              
     *               } // Bulk Item Structure End
     *               
     *             ] // Items Array End
     *             
     *    }  // Bulk Response Structure End
     *            
     *            
     */

    /* TODO: Check if this method needs to be thread safe. 
     * Since bulk request uses Synchronous response listener -
     * the onComplete method will only be invoked in one thread. */
    
    @Override
    public BulkResponse buildFromJson(JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        // Bulk Response Structure Start
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        List<BulkItemResponse> items = new ArrayList<>();

        String currentFieldName = null;
        // Parse header fields : took, errors and items.
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isScalarValue()) {
                /*
                 * Parse tookInMillis and Boolean errors value. "errors" may
                 * not exist in old ES version BulkResponses.
                 */
                if (TOOK.equals(currentFieldName)) {
                    this.tookInMillis = parser.getLongValue();
                } else if (ERRORS.equals(currentFieldName)) {
                    this.errors = (Boolean.valueOf(parser.getValueAsBoolean()));
                    /*
                     * TODO: May be need to break off here. No point parsing
                     * rest of items if FTS fails whole of BulkRequest. Or
                     * provide another implementation for a quicker response.
                     */
                }
            } else if (ITEMS.equals(currentFieldName)) {
                /*
                 * Parse all Items. TODO: Make a quicker version where bulk
                 * items are not parsed.
                 */
                //Bulk Items Array Start.
                if ((token == JsonToken.START_ARRAY)) {
                    while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                        BulkItemResponse item = new BulkItemResponse(items.size());
                        item.buildFromJson(parser);
                        items.add(item);
                    }
                }
            }
            itemResponses.addAll(items);
        }
        parsed(true);
        return this;
    }

    @Override
    public BulkResponse buildErrorReponse(ESException e) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BulkResponse buildFromRestResponse(RestResponse restResp) {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * Only print first 100 Bulk Items;
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
                builder.append("BulkResponse: {");
                builder.append(" took:" + tookInMillis);
                builder.append(" errors:" + errors);
                int toIndex = itemResponses.size();
                if(itemResponses.size() > 100) {
                    toIndex=100;
                }
                builder.append(" items:[" +
                itemResponses.subList(0, toIndex) + "]" );
        return builder.toString();
                
    }

}
