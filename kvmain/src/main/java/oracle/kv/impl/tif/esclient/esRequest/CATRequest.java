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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;

/**
 * This class can be used to get information about several indices or nodes. It
 * is more of a free form usage and success depends on caller passing right
 * parameter values.
 * 
 *
 */

public class CATRequest extends ESRequest<CATRequest>
        implements ESRestRequestGenerator {

    private API api;
    private String indexPrefix;
    private String indexSuffix;
    private Map<String, String> params;

    /**
     * 
     * @param type         Request Type for cat. for eg: indices,count etc.
     * @param index        name of the index, if info for one particular index
     *                     is required.
     * @param indexPrefix  information for all indices starting with this
     *                     prefix.
     * @param indexSuffix  information for all indices ending with this
     *                     prefix.
     * @param params       parameters for Cat API. Can be found on
     *                     Elasticsearch docs.
     */
    public CATRequest(API type,
            String index,
            String indexPrefix,
            String indexSuffix,
            Map<String, String> params) {
        this.api = type;
        this.index = index;
        this.indexPrefix = indexPrefix;
        this.indexSuffix = indexSuffix;
        this.params = params;
        if (params != null) {
            params.put("format", "json");
        }
    }

    /**
     * If most of the other parameters are null, use this constructor. And
     * other non-null params
     * 
     * @param type  Type of the CAT request.
     * 
     */
    public CATRequest(API type) {
        this.api = type;
    }

    public API api() {
        return api;
    }

    public CATRequest api(API api1) {
        this.api = api1;
        return this;
    }

    public String indexPrefix() {
        return indexPrefix;
    }

    public CATRequest indexPrefix(String indexPrefix1) {
        if (!ESRestClientUtil.isEmpty(indexPrefix1)) {
            this.indexPrefix = indexPrefix1;
        }
        return this;
    }

    public String indexSuffix() {
        return indexSuffix;
    }

    public CATRequest indexSuffix(String indexSuffix1) {
        if (!ESRestClientUtil.isEmpty(indexSuffix1)) {
            this.indexSuffix = indexSuffix1;
        }
        return this;
    }

    public Map<String, String> params() {
        return params;
    }

    public CATRequest params(Map<String, String> params1) {
        this.params = params1;
        if (params1 != null) {
            params1.put("format", "json");
        } else {
            this.params = new HashMap<String, String>();
            this.params.put("format", "json");
        }
        return this;
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (api == null) {
            exception = new InvalidRequestException("No CAT API is provided");
        }

        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String wildchar = "*";
        String indexName = "";
        if (!ESRestClientUtil.isEmpty(index())) {
            indexName = index();
        } else if (!ESRestClientUtil.isEmpty(indexPrefix())) {
            indexName = indexPrefix() + wildchar;
        }
        if (!ESRestClientUtil.isEmpty(indexSuffix())) {
            if (indexName.endsWith(wildchar)) {
                indexName = indexName + indexSuffix();
            } else {
                indexName = indexName + wildchar + indexSuffix();
            }
        }

        String endpoint;
        if (ESRestClientUtil.isEmpty(indexName)) {
            endpoint =
                endpoint("_cat", api().name().toLowerCase(Locale.ENGLISH));
        } else {
            endpoint =
                endpoint("_cat", api().name().toLowerCase(Locale.ENGLISH),
                         indexName);
        }

        if (params() == null) {
            Map<String, String> formatParam = new HashMap<String, String>();
            formatParam.put("format", "json");
            params(formatParam);
        }

        return new RestRequest(ESHttpMethods.HttpGet, endpoint, null, params(),
                               null);
    }

    @Override
    public oracle.kv.impl.tif.esclient.esRequest.ESRequest.RequestType
           requestType() {
        return RequestType.CAT;
    }

    public static enum API {
        INDICES, COUNT, NODES, HEALTH;

        public static RequestType get(String reqTypeStr) {
            for (RequestType reqType : RequestType.values()) {
                if (reqTypeStr.toLowerCase()
                              .equals(reqType.toString().toLowerCase())) {
                    return reqType;
                }
            }
            return null;
        }
    }

}
