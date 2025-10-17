/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.sc;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.KmsKeyInfo;

/**
 * Response to a TenantManager getKmsKey operation.
 */
public class GetKmsKeyInfoResponse extends CommonResponse {

    private final KmsKeyInfo keyInfo;

    GetKmsKeyInfoResponse(KmsKeyInfo keyInfo, int httpResponse) {
        super(httpResponse);
        this.keyInfo = keyInfo;
    }

    public GetKmsKeyInfoResponse(ErrorResponse err) {
        super(err);
        keyInfo = null;
    }

    public KmsKeyInfo getKeyInfo() {
        return keyInfo;
    }

    @Override
    public String successPayload() {
        try {
            return JsonUtils.print(keyInfo);
        } catch (IllegalArgumentException iae) {
            return ("Error serializing payload: " + iae.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ConfigurationResponse [keyinfo=" + keyInfo + ", toString()="
                + super.toString() + "]";
    }
}
