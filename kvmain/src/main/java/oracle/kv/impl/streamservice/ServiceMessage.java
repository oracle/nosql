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

package oracle.kv.impl.streamservice;

import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_PAYLOAD;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_REQUEST_ID;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_SERVICE_TYPE;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_TIMESTAMP;

import java.io.IOException;

import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Base class for all stream service messages.
 *
 * Each message is a row in either the request or response tables.
 *
 * The primary key fields are the service type and the request ID. The first
 * component of the key is the service type. This allows iterations of a
 * specific service in order of ID.
 *
 * The service type is defined by the enum ServiceType.
 *
 * Request-response service messages use request IDs greater than 0. Request
 * ID 0 is reserved. Request IDs less than 0 are used for non-request-response
 * messages.
 *
 * The timestamp set at message construction.
 */
public class ServiceMessage {

    /**
     * Streaming service types. New services must be added to the end of this
     * enum to maintain compatibility.
     */
    public enum ServiceType {
        MRT,        /* Multi region table */
        PITR;       /* Point in time recovery */

        private static final ServiceType[] VALUES = values();

        /**
         * Gets the service type from the specified Row.
         */
        private static ServiceType getServiceType(Row row) {
            final int ord = row.get(COL_SERVICE_TYPE).asInteger().get();
            return VALUES[ord];
        }
    }

    /**
     * Fields common to all service messages.
     */
    private final ServiceType serviceType;
    /**
     * ID of the request
     */
    private final int requestId;
    /**
     * Timestamp of the message is created or last modified
     */
    private long timestamp;

    /**
     * Constructs a service message. The timestamp is set at the time of
     * construction.
     */
    protected ServiceMessage(ServiceType service, int requestId) {
        assert service != null;
        this.requestId = requestId;
        this.serviceType = service;
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Sets the timestamp of the response
     */
    public void setTimestamp(long ts) {
        timestamp = ts;
    }

    /**
     * Constructs a serviceType message from the specified Row.
     */
    public ServiceMessage(Row row) {
        requestId = row.get(COL_REQUEST_ID).asInteger().get();
        serviceType = ServiceType.getServiceType(row);
        timestamp = row.get(COL_TIMESTAMP).asLong().get();
    }

    /**
     * Gets the service type of this message.
     */
    public final ServiceType getServiceType() {
        return serviceType;
    }

    /**
     * Gets the request ID of this message. The request ID is an opaque value
     * and does not represent the order in which the messages are posted.
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Gets the timestamp of this message. The timestamp is set at time
     * of construction and is for debug purpose only as it does not represent
     * the order in which messages are posted.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets the payload from the row as a byte array.
     */
    protected final byte[] getPayloadFromRow(Row row) {
        return row.get(COL_PAYLOAD).asBinary().get();
    }

    /**
     * Returns a row from this message for the specified table. Subclasses
     * should override this method, call super.toRow(), and then complete
     * populating the row with the message specific data.
     */
    protected Row toRow(Table serviceTable, short maxSerialVersion)
            throws IOException {
        final Row row = serviceTable.createRow();
        row.put(COL_REQUEST_ID, requestId);
        row.put(COL_SERVICE_TYPE, serviceType.ordinal());
        row.put(COL_TIMESTAMP, timestamp);
        row.put(COL_PAYLOAD, getPayload(maxSerialVersion));
        return row;
    }

    /**
     * Gets the payload from this message as a byte array. The default
     * implementation returns an empty array. Subclasses can override this
     * method to return the message specific payload.
     */
    @SuppressWarnings("unused")
    protected byte[] getPayload(short maxSerialVersion) throws IOException {
        return new byte[0];
    }

    @Override
    public final String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[").append(requestId);
        sb.append(", ").append(serviceType);
        sb.append(", ").append(timestamp);
        getToString(sb).append("]");
        return sb.toString();
    }

    /**
     * Adds additional information to the value returned by toString().
     * Subclasses can override this method to return message specific
     * information. The general format is:
     *      MessageClassName[requestId, serviceType, timestamp getToString()]
     *
     * If the added field is not obvious, adding the field name may be useful.
     * For example:
     *      ..., seqNumber=###, ...
     */
    protected StringBuilder getToString(StringBuilder sb) {
        return sb;
    }
}
