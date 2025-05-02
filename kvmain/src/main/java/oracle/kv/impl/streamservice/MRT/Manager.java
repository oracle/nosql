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

package oracle.kv.impl.streamservice.MRT;

import static oracle.kv.impl.streamservice.MRT.Response.VERSION_REQUEST_ID;
import static oracle.kv.impl.streamservice.ServiceMessage.ServiceType.MRT;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.streamservice.MRT.Request.StoreAndMinAgentVersions;
import oracle.kv.impl.streamservice.ServiceManager;
import oracle.kv.impl.streamservice.ServiceMessage;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.Row;

/**
 * Base class for MRT service manager.
 */
public abstract class Manager extends ServiceManager<Request, Response> {

    protected Manager(Logger logger) {
        super(logger);
    }

    @Override
    protected ServiceMessage.ServiceType getServiceType() {
        return MRT;
    }

    @Override
    protected Response getResponseFromRow(Row row) throws IOException {
        return Response.getFromRow(row);
    }

    @Override
    protected Request getRequestFromRow(Row row) throws IOException {
        return Request.getFromRow(row);
    }

    public KVVersion getAgentVersion() {
        final Response res = super.getResponse(VERSION_REQUEST_ID);
        return res == null ? null : res.getMinVersion();
    }

    public void setAgentVersion(NoSQLSubscriberId agentId, KVVersion version) {
        final Response resp = Response.createVerResp(agentId.getTotal());
        resp.addAgentVersion(agentId, version);
        postResponse(resp, true);
    }

    protected void setStoreAndMinAgentVersions(KVVersion storeVersion,
                                               KVVersion minAgentVersion) {
        postRequest(new StoreAndMinAgentVersions(storeVersion,
                                                 minAgentVersion),
                    true);
    }

    public KVVersion getStoreVersion() {
        final Request req = getRequest(StoreAndMinAgentVersions.REQUEST_ID);
        return req == null ? null :
                            ((StoreAndMinAgentVersions)req).getStoreVersion();
    }

    protected KVVersion getMinAgentVersion() {
        final Request req = getRequest(StoreAndMinAgentVersions.REQUEST_ID);
        return req == null ? null :
                           ((StoreAndMinAgentVersions)req).getMinAgentVersion();
    }

    /**
     * Gets an iterator over all requests.
     */
    public RequestIterator getRequestIterator(long timeout,
                                              TimeUnit timeoutUnit) {
        return new RequestIterator(0, timeout, timeoutUnit);
    }

    public class RequestIterator extends MessageIterator<Request> {

        private RequestIterator(int startId, long timeout, TimeUnit timeoutUnit) {
            super(getRequestTable(), startId, timeout, timeoutUnit);
        }

        @Override
        protected Request getMessage(Row row) throws IOException {
            return getRequestFromRow(row);
        }
    }

    /**
     * Gets an iterator over the all responses.
     */
    public ResponseIterator getResponseIterator(int startId,
                                                long timeout,
                                                TimeUnit timeoutUnit) {
        return new ResponseIterator(startId, timeout, timeoutUnit);
    }

    public class ResponseIterator extends MessageIterator<Response> {

        private ResponseIterator(int startId, long timeout, TimeUnit timeoutUnit) {
            super(getResponseTable(), startId, timeout, timeoutUnit);
        }

        @Override
        protected Response getMessage(Row row) throws IOException {
            return getResponseFromRow(row);
        }
    }
}

