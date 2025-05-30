/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

import java.util.Arrays;

import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.TableRequestLimits;

/**
 * Response to a TenantManager getStore operation. It returns
 * information sufficient to allow the proxy to connect to a store. This
 * includes:
 *  storeName
 *  helperHosts (host:port[,host:port]*)
 */
public class GetStoreResponse extends CommonResponse {
    private final String storeName;
    private final String[] helperHosts;
    private final RequestLimits requestLimits;
    private final boolean isMultiRegion;
    private final boolean isInitialized;

    public GetStoreResponse(int httpResponse,
                            String storeName,
                            String[] helperHosts,
                            TableRequestLimits limits,
                            boolean isMultiRegion,
                            boolean isInitialized) {
        super(httpResponse);
        this.storeName = storeName;
        this.helperHosts = helperHosts;
        this.requestLimits = makeRequestLimits(limits);
        this.isMultiRegion = isMultiRegion;
        this.isInitialized = isInitialized;
    }

    public GetStoreResponse(int httpResponse,
                            String storeName,
                            String[] helperHosts) {
        this(httpResponse, storeName, helperHosts, null,
             false /* isMultiRegion */, true /* isInitialized */);
    }

    public GetStoreResponse(ErrorResponse err) {
        super(err);
        storeName = null;
        helperHosts = null;
        requestLimits = null;
        isMultiRegion = false;
        isInitialized = false;
    }

    /**
     * Returns the store name
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Returns the helper hosts string
     */
    public String[] getHelperHosts() {
        return helperHosts;
    }

    public RequestLimits getRequestLimits() {
        return requestLimits;
    }

    public boolean isMultiRegion() {
        return isMultiRegion;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    /**
     * NOTE: this will never be used as it's not part of the REST
     * interface.
     */
    @Override
    public String successPayload() {
        return toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "GetStoreResponse [storeName=" + storeName + ", helperHosts="
                + Arrays.toString(helperHosts) + ", getHttpResponse()="
                + getHttpResponse() + ", getSuccess()=" + getSuccess()
                + ", getErrorCode()=" + getErrorCode() + ", getErrorString()="
                + getErrorString() + "]";
    }

    private RequestLimits makeRequestLimits(TableRequestLimits trl) {
        if (trl == null) {
            return null;
        }
        return new RequestLimits(trl.getPrimaryKeySizeLimit(),
                                 trl.getRowSizeLimit(),
                                 trl.getRequestSizeLimit(),
                                 trl.getRequestReadKBLimit(),
                                 trl.getRequestWriteKBLimit(),
                                 trl.getQueryStringSizeLimit(),
                                 trl.getBatchOpNumberLimit(),
                                 trl.getBatchRequestSizeLimit());
    }
}
