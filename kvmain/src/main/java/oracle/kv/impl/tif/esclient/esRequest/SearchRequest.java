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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

/**
 * NoSQL FTS does not support search and this class is meant only for unit
 * tests.
 * 
 * This is an URI Search class with QueryStringQuery query specified in URL
 * with parameter "q".
 * 
 *
 */
public class SearchRequest extends ESRequest<SearchRequest>
        implements ESRestRequestGenerator {

    /* Parameters */
    private List<String> storedFields = null;
    private boolean _source = false; // Mostly search results only need id.
    private String sort = null;
    /*
     * TODO: can id be a deafult sort param? "id:asc"; Typically search results
     * should be sorted by score.
     */
    private int pageSize = 100; // page size for search results.
    private int fromIndex = 0; // Get results from index 0.
    private String queryString = null; // The query parameter. Html Encoded
                                       // String.
    private String routing = null; // Search subset of shards with routing
                                   // param.
    private QueryBuilder queryBuilder = null;

    public SearchRequest() {

    }

    public SearchRequest(String index) {
        super(index);
    }

    public SearchRequest(String index, String queryString) {
        super(index);
        Objects.requireNonNull(queryString);
        this.queryString = queryString;
    }

    public List<String> storedFields() {
        return storedFields;
    }

    public SearchRequest storedFields(List<String> storedFields1) {
        this.storedFields = storedFields1;
        return this;
    }

    public boolean is_source() {
        return _source;
    }

    public SearchRequest source(boolean _source1) {
        this._source = _source1;
        return this;
    }

    public String getSort() {
        return sort;
    }

    public SearchRequest sort(String sort1) {
        this.sort = sort1;
        return this;
    }

    public int pageSize() {
        return pageSize;
    }

    public SearchRequest pageSize(int pageSize1) {
        this.pageSize = pageSize1;
        return this;
    }

    public int from() {
        return fromIndex;
    }

    public SearchRequest from(int from) {
        this.fromIndex = from;
        return this;
    }

    public String queryString() {
        return queryString;
    }

    public SearchRequest queryString(String queryString1) {
        this.queryString = queryString1;
        return this;
    }

    public SearchRequest queryBuilder(QueryBuilder queryBuilder1) {
        this.queryBuilder = queryBuilder1;
        return this;
    }

    public InvalidRequestException validate() {

        /*
         * Should be able to search all indices and all types TODO: Decide
         * whether to put this validation constraint or not.
         */
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            /*
             * Should be able to search all indices and all types TODO: Decide
             * whether to put this validation constraint or not.
             */
            // throw validate();
        }
        String endpoint = "";
        if (index() != null) {
            endpoint = endpoint(index(), "_search");
        } else {
            endpoint = endpoint("_search");
        }

        RequestParams params = new RequestParams();
        if (queryString != null && queryString.length() > 0)
            params.queryString(queryString);
        if (routing != null && routing.length() > 0)
            params.routing(routing);
        if (storedFields != null && storedFields.size() > 0)
            params.storedFields(storedFields);
        params.fetchSource(_source);
        if (pageSize > 0)
            params.pageSize(pageSize);
        if (fromIndex > 0)
            params.fromIndex(fromIndex);
        /*
         * No default sort provided If required should be passed from the
         * caller side.
         */

        if (sort != null && sort.length() > 0)
            params.sort(sort);

        byte[] entity = null;
        if (queryBuilder != null) {
            try {
                entity = queryBuilder.querySource();
            } catch (IOException e) {
                /*
                 * Currently request validation is not being done. Will be
                 * added soon.
                 */
                throw new RuntimeException(new InvalidRequestException("query format is wrong"));
            }
        }

        return new RestRequest(ESHttpMethods.HttpGet, endpoint, entity,
                               params.getParams(),
                               new RequestParams().putParam("Content-Type",
                                   getContentType()).getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.SEARCH;
    }

    public static String getContentType() {
        return "application/json; charset=UTF-8";
    }
}
