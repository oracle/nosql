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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;

import com.fasterxml.jackson.core.JsonGenerator;

public class IndexDocumentRequest
        extends ESWriteRequest<IndexDocumentRequest> {

    private byte[] source;

    private boolean isRetry = false;

    public IndexDocumentRequest() {

    }

    /**
     * Construct a IndexDocumentRequest. An IndexDocumentRequest should have
     * source to index. Also, currently ESHandler uses indexing operation as an
     * idempotent operation. So "id" field is required. However, if not given,
     * id is generated but then it is not an idempotent operation for
     * logEntries resulting from a restart of a stream from the checkpointed
     * VLSN.
     *
     * @param index  The index to index into
     * @param id     The id of document
     */
    public IndexDocumentRequest(String index, String id) {
        super(index, id);
    }

    /*
     * This is JSON source and String has UTF-8 encoding
     */
    public IndexDocumentRequest source(String source1) {
        return source(
                      source1.getBytes(
                                       StandardCharsets.UTF_8));
    }

    /**
     * Sets the content source to index.
     */
    public IndexDocumentRequest source(JsonGenerator jsonGen) {
        return source(
                      ((ByteArrayOutputStream) jsonGen.getOutputTarget()).toByteArray());
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexDocumentRequest source(byte[] source1) {
        this.source = Objects.requireNonNull(
                                             source1);
        return this;
    }

    public IndexDocumentRequest source(Map<String, Object> source1)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ESJsonBuilder jsonBuilder = new ESJsonBuilder(baos);
        jsonBuilder.value(
                          source1);
        jsonBuilder.flushJsonGenerator();
        return source(
                      baos.toByteArray());
    }

    /**
     * The source of the document to index, recopied to a new array if it is
     * unsafe.
     */
    public byte[] source() {
        return source;
    }

    @Override
    public String toString() {
        return "{ index:" + index + " ,id:" + id +
                " ,source:" + ESJsonUtil.toStringUTF8Bytes(
                                                           source) +
                " }";
    }

    /**
     * Returns <code>true</code> if this request has been sent to a shard copy
     * more than once.
     */
    public boolean isRetry() {
        return isRetry;
    }

    public void onRetry() {
        isRetry = true;
    }

    @Override
    public IndexDocumentRequest routing(String routing1) {
        if (routing1 != null && routing1.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing1;
        }
        return this;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = (id() != null && id().length() > 0)
                ? ESHttpMethods.HttpPut : ESHttpMethods.HttpPost;

        String id1 = null;
        try {
            id1 = URLEncoder.encode(
                                    id(), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            id1 = id();
        }
        // String endpoint = endpoint(index(), type(), id(), "_create" );
        String endpoint = endpoint(index() + "/_doc", id1);

        RequestParams parameters = new RequestParams();
        // ES Default is quorum. But ES Shards may not be active all the time.
        // So we can not fail these index opertaions.
        // parameters.consistency("one");
        // parameters.routing(routing());
        // parameters.version(version());
        // parameters.refreshType(refreshType());

        return new RestRequest(method, endpoint, source,
                               parameters.getParams(),
                               new RequestParams().putParam("Content-Type",
                               getContentType()).getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.INDEX;
    }

    public static String getContentType() {
        return "application/json; charset=UTF-8";
    }
}
