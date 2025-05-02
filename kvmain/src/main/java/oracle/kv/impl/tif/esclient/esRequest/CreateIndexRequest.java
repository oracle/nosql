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
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

public class CreateIndexRequest extends ESRequest<CreateIndexRequest>
        implements ESRestRequestGenerator {

    private byte[] source;

    public CreateIndexRequest() {

    }

    public CreateIndexRequest(String index, byte[] source) {
        this.index = index;
        this.source = source;
    }

    public CreateIndexRequest(String index) {
        this.index = index;
    }

    /**
     * Create Index Post Body in UTF-8 Json.
     */
    public CreateIndexRequest source(byte[] source1) {
        this.source = Objects.requireNonNull(
                                             source1);
        return this;
    }

    public CreateIndexRequest source(String jsonString) throws IOException {
        Objects.requireNonNull(
                               jsonString);
        JsonParser parser = null;
        JsonGenerator jsonGen = null;
        try {
            parser = ESJsonUtil.createParser(
                                             jsonString);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            jsonGen = ESJsonUtil.createGenerator(
                                                 baos);
            jsonGen.copyCurrentStructure(
                                         parser);
            jsonGen.flush();
            this.source = baos.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            if (parser != null) {
                parser.close();
            }
            if (jsonGen != null) {
                jsonGen.close();
            }
        }

        return this;
    }

    public CreateIndexRequest settings(Map<String, Object> settings)
        throws IOException {
        Objects.requireNonNull(settings);
        JsonGenerator jsonGen = null;
        try {
            jsonGen = ESJsonUtil.mapToJson(settings);
            this.source = ((ByteArrayOutputStream)jsonGen.getOutputTarget())
                                                         .toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            if (jsonGen != null) {
                jsonGen.close();
            }
        }
        return this;
    }

    public byte[] source() {
        return source;
    }

    /*
     * TODO : CreateIndexRequest should work with null source. Default settings
     * should take effect.
     */
    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        /*
         * Index creation with null source should take default settings for
         * index.
         */
        /*
         * if (source == null || source.length <= 0) { exception = new
         * InvalidRequestException( "Index Settings are not provided"); }
         */
        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = ESHttpMethods.HttpPut;
        String endpoint = endpoint(index());
        RequestParams parameters = new RequestParams();
        return new RestRequest(method, endpoint, source,
                               parameters.getParams(),
                               new RequestParams().putParam("Content-Type",
                                      getContentType()).getParams());
    }

    public static String getContentType() {
        return "application/json; charset=UTF-8";
    }

    @Override
    public RequestType requestType() {
        return RequestType.CREATE_INDEX;
    }

}
