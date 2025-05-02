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

package oracle.kv.impl.tif.esclient.restClient;

import java.io.IOException;

import oracle.kv.impl.tif.esclient.esResponse.ESException;

import com.fasterxml.jackson.core.JsonParser;

/**
 * Parses the ES UTF-8 JSON response into corresponding Object Structures.
 * 
 *
 */
public interface JsonResponseObjectMapper<T> {

    /**
     * This method is called to build a response object from the Json Response
     * in the HttpEntity sent by the ES Server.
     * 
     * This method should be called only for successful Response Status.
     * 
     * Typically this method will be implemented as an instance method of T.
     * The caller should make sure that parser is initialized appropriately.
     * Parser needs to be positioned appropriately by the caller, and the
     * position is implementation dependent. For eg, if the structure this
     * method will build is the top level structure object, the parser needs to
     * be typically positioned before the START_OBJECT, for inner structure,
     * the parser will typically be positioned at the JsonToken.START_OBJECT
     * and the currentFieldName is the structure name which it will build.
     * 
     * 
     *
     * 
     * @param parser - JsonParser positioned correctly at the start of the
     * structure being built.
     * 
     * @return - instance of the concrete ESResponse implementation.
     * @throws IOException - an instance of JsonParseException.
     */

    T buildFromJson(JsonParser parser) throws IOException;

    /**
     * Some Response Object may need to get built even in case of unsuccessful
     * response.
     * 
     * This method is called only for unsuccessful response.
     */

    T buildErrorReponse(ESException e);

    /**
     * Some Response types need not be parsed and can be used directly. For eg
     * GetMappingResponse only contains the mapping and need not be parsed for
     * more details.
     * 
     * @param restResp
     * @return instance of the concrete ESResponse implementation.
     * @throws IOException
     */
    T buildFromRestResponse(RestResponse restResp) throws IOException;

}
