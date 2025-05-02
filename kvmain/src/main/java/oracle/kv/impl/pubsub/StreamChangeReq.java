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


package oracle.kv.impl.pubsub;

import java.io.Serializable;
import java.util.logging.Logger;

import com.sleepycat.je.rep.stream.FeederFilterChange;

/**
 * Object represents the base class of all filter change requests used
 * by Streams API client to change the feeder filter at runtime.
 */
public abstract class StreamChangeReq
    implements FeederFilterChange, Serializable {

    /* Types of request  */
    public enum Type {
        /* add a table to stream */
        ADD,
        /* remove a table from stream */
        REMOVE
    }

    protected static final long serialVersionUID = 1L;

    /* request id */
    private final String reqId;

    /* type of request */
    private final Type reqType;

    /* name of table in request */
    private final String tableName;

    /* root table id of the table, same as table id if a root table */
    private final String rootTableId;

    /* id string of the table */
    private final String tableId;

    /* private logger */
    protected final transient Logger logger;

    StreamChangeReq(String reqId, Type reqType, String tableName,
                    String rootTableId, String tableId, Logger logger) {
        this.reqId = reqId;
        this.reqType = reqType;
        this.tableName = tableName;
        this.rootTableId = rootTableId;
        this.tableId = tableId;
        this.logger = logger;
    }

    /**
     * Returns the request id
     *
     * @return the request id
     */
    @Override
    public String getReqId() {
        return reqId;
    }

    /**
     * Returns the request type
     *
     * @return the request type
     */
    public Type getReqType() {
        return reqType;
    }

    /**
     * Returns the name of table in request
     *
     * @return the name of table in request
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the root table id string of table in request
     *
     * @return the root table id string of table in request
     */
    public String getRootTableId() {
        return rootTableId;
    }

    /**
     * Returns the id string of table in request
     *
     * @return the id string of table in request
     */
    public String getTableId() {
        return tableId;
    }

    /**
     * Converts this request to a {@link StreamChangeSubscribeReq}.
     *
     * @return this request as a StreamChangeSubscribeReq
     *
     * @throws IllegalArgumentException if this request is not a
     * StreamChangeSubscribeReq
     */
    abstract StreamChangeSubscribeReq asSubscribeReq();

    /**
     * Converts this request to a {@link StreamChangeUnsubscribeReq}.
     *
     * @return this request as a StreamChangeUnsubscribeReq
     *
     * @throws IllegalArgumentException if this request is not a
     * StreamChangeUnsubscribeReq
     */
    abstract StreamChangeUnsubscribeReq asUnsubscribeReq();
}
