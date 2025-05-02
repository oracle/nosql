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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

public interface ESRestRequestGenerator {
    public static final String DELIMITER = "/";

    public RestRequest generateRestRequest() throws InvalidRequestException;

    /**
     * A class for request's parameters map.
     */
    static class RequestParams {
        private final Map<String, String> params = new HashMap<>();

        RequestParams() {
        }

        RequestParams putParam(String key, String value) {
            if (value != null && value.length() > 0) {
                if (params.putIfAbsent(
                                       key, value) != null) {
                    throw new IllegalArgumentException("Request parameter {" +
                            key + "} already exists");
                }
            }
            return this;
        }

        RequestParams upsert(boolean upsert) {
            if (upsert) {
                return putParam(
                                "doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

        RequestParams realtime(boolean realtime) {
            if (realtime == false) {
                return putParam(
                                "realtime", Boolean.FALSE.toString());
            }
            return this;
        }

        RequestParams refresh(boolean refresh) {
            if (refresh) {
                return refreshType(
                                   "true");
            }
            return this;
        }

        RequestParams consistency(String consistency) {
            if (consistency != null && consistency.length() >= 3) {
                return putParam(
                                "consistency", consistency.toLowerCase(
                                                                       Locale.ENGLISH));
            }
            return putParam(
                            "consistency", "one");

        }

        /**
         * By default refreshType is false in ES. That is index is not
         * refreshed after the request (actions of the request remain in
         * translog). true : means immediate refresh wait_for : means wait
         * until the refresh time period.
         * 
         * @param refreshType : either of "false","true" or "wait_for"
         * @return this Param Object
         */
        RequestParams refreshType(String refreshType) {
            if (!refreshType.toLowerCase().equals(
                                                  "false")) {
                return putParam(
                                "refresh", refreshType.toLowerCase());
            }
            return this;
        }

        RequestParams retryOnConflict(int retryOnConflict) {
            if (retryOnConflict > 0) {
                return putParam(
                                "retry_on_conflict", String.valueOf(
                                                                    retryOnConflict));
            }
            return this;
        }

        RequestParams routing(String routing) {
            return putParam(
                            "routing", routing);
        }

        RequestParams version(long version) {
            return putParam(
                            "version", Long.toString(
                                                     version));
        }

        RequestParams fetchSource(Boolean fetchSource) {
            putParam(
                     "_source", fetchSource.toString());
            return this;
        }

        RequestParams storedFields(List<String> storedFields) {
            if (storedFields != null && storedFields.size() > 0) {
                return putParam(
                                "fields", String.join(
                                                      ",", storedFields));
            }
            return this;
        }

        RequestParams queryString(String queryString) {
            Objects.requireNonNull(
                                   queryString);
            /*
             * URL encoding has moved to ESHttpClient putParam("q",
             * URLEncoder.encode(queryString, StandardCharsets.UTF_8.name()));
             */
            putParam(
                     "q", queryString);

            return this;
        }

        RequestParams sort(String sort) {
            if (sort != null && sort.length() > 0) {
                try {
                    putParam(
                             "sort",
                             URLEncoder.encode(
                                               sort,
                                               StandardCharsets.UTF_8.name()));
                } catch (UnsupportedEncodingException e) {
                    putParam(
                             "sort", sort);
                }
            }
            return this;
        }

        RequestParams pageSize(int pageSize) {
            if (pageSize > 0) {
                putParam(
                         "size", Integer.toString(
                                                  pageSize));
            }
            return this;
        }

        RequestParams fromIndex(int fromIndex) {
            if (fromIndex > 0) {
                putParam(
                         "from", Integer.toString(
                                                  fromIndex));
            }
            return this;
        }

        /*
         * Values for level can be: cluster,indices or shards. Only cluster
         * level is supported in Response.
         */
        RequestParams clusterHealthLevel(String level) {
            return putParam(
                            "level", level);
        }

        Map<String, String> getParams() {
            return Collections.unmodifiableMap(
                                               params);
        }
    }

    default RestRequest info() {
        return new RestRequest(ESHttpMethods.HttpGet, "/", null,
                               Collections.emptyMap(), Collections.emptyMap());
    }

    default String endpoint(String... parts) {
        if (parts == null || parts.length == 0) {
            return DELIMITER;
        }

        StringJoiner joiner = new StringJoiner(DELIMITER, DELIMITER, "");
        for (String part : parts) {
            if (part != null) {
                joiner.add(
                           part);
            }
        }
        return joiner.toString();
    }

}
