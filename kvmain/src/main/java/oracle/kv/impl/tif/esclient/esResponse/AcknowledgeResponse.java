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

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class AcknowledgeResponse extends ESResponse {

    private static final String ACKNOWLEDGED = "acknowledged";

    private boolean acknowledged = false;

    public AcknowledgeResponse() {

    }

    public AcknowledgeResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public AcknowledgeResponse acknowledged(boolean acknowledged1) {
        this.acknowledged = acknowledged1;
        return this;
    }

    /**
     * Builds the acknowlege response. The parser should be positioned before
     * the start of curly braces.
     * 
     */
    public void buildAcknowledgeResponse(JsonParser parser)
        throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            }

            if (ACKNOWLEDGED.equals(currentFieldName)) {
                if (token.isScalarValue()) {
                    this.acknowledged = parser.getBooleanValue();
                }
            } else if (token.isStructStart()) {
                parser.skipChildren();
            }
        }

        parsed(true);
    }

}
