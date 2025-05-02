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

package oracle.kv.impl.tif.esclient.restClient.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;

import oracle.kv.impl.tif.esclient.esResponse.InvalidResponseException;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.nosql.common.json.JsonUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

public class ESRestClientUtil {

    public static JsonParser initParser(RestResponse resp)
        throws InvalidResponseException {
        String entity = resp.getHeader("Content-Type");
        if (entity == null) {
            throw new InvalidResponseException("Response has no httpEntity");
        } else if (!entity.toLowerCase()
                          .startsWith("application/json")) {
            throw new InvalidResponseException("ContentType is" +
                                               " not application/json");
        } else {
            try {
                return ESJsonUtil.createParser(resp.httpResponse().body());
            } catch (Exception pe) {
                throw new InvalidResponseException("ParseException: " +
                                                   "Response JSON could" +
                                                   " not be parsed");
            }
        }
    }

    /*
     * Call this method only after checking if mapping for the given
     * esIndexName and esIndexType exists.
     *
     * There are no null checks while getting the relevant mapping spec from
     * mapping response.
     */
    @SuppressWarnings("unchecked")
    public static boolean isMappingResponseEqual(
                                                 String mappingResponse,
                                                 JsonGenerator mappingSpec,
                                                 String esIndexName)
        throws IOException {
        try {
            if (ESRestClientUtil.isEmpty(mappingResponse) ||
                    mappingSpec == null) {
                return false;
            }
            mappingSpec.flush();
            String mappicSpecStr =
                new String(((ByteArrayOutputStream) mappingSpec
                                                    .getOutputTarget())
                                                    .toByteArray(),
                           "UTF-8");
            Map<String, Object> mappingSpecMap =
                JsonUtils.fromJson(mappicSpecStr, HashMap.class);
            Map<String, Object> existingMappingMap =
                JsonUtils.fromJson(mappingResponse, HashMap.class);

            /*
             * Note that this method is called after checking that mapping
             * exists. hence no null checks done here.
             */
            Map<String, Object> existingMappingExtract = null;

            try {
                existingMappingExtract =
                    (Map<String, Object>)
                     (((Map<String, Object>) (existingMappingMap
                                              .get(esIndexName)))
                      .get("mappings"));
            } catch (NullPointerException npe) {
                return false;
            }

            return areJsonMapsEqual(mappingSpecMap, existingMappingExtract);

        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static boolean areJsonMapsEqual(Map<?, ?> a, Map<?, ?> b) {
        if (a == b)
            return true;

        if (a.size() != b.size())
            return false;

        try {
            Iterator<?> i = a.entrySet().iterator();
            while (i.hasNext()) {
                Entry<?, ?> e = (Entry<?, ?>) i.next();
                Object key = e.getKey();
                Object value = e.getValue();
                if (value == null) {
                    if (!(b.get(key) == null && b.containsKey(key)))
                        return false;
                } else {
                    if (value instanceof Map) {
                        if (b.get(key) instanceof Map) {
                            if (!areJsonMapsEqual((Map<?, ?>) value,
                                                  (Map<?, ?>) b.get(key))) {
                                return false;
                            }
                        } else
                            return false;
                    } else if (value instanceof Boolean ^
                            b.get(key) instanceof Boolean) {
                        if (!value.toString().equals(b.get(key).toString())) {
                            return false;
                        }
                    } else if (!value.equals(b.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }

        return true;
    }

    public static boolean isEmpty(String string) {
        if (string == null || string.length() == 0) {
            return true;
        }
        return false;

    }

}
