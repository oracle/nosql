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

package oracle.kv.impl.xregion.service;

import java.util.Set;
import java.util.stream.Collectors;

import oracle.kv.table.Table;

/**
 * Object represents the request for cross-region service
 */
public class XRegionRequest {

    /* for request without request id */
    public static final int NULL_REQUEST_ID = 0;

    /**
     * Cross-region request type
     */
    public enum RequestType {

        /*--------------------*/
        /* request form Admin */
        /*--------------------*/

        /* shut down the service */
        SHUTDOWN,

        /* add a region to multi-region table or create a multi-region table */
        MRT_ADD,

        /*
         * multi-region table update without region change, e.g, schema
         * evolution, update ttl, etc
         */
        MRT_UPDATE,

        /* remove a region from multi-region table */
        MRT_REMOVE,

        /* drop a multi-region table */
        MRT_DROP,

        /* add a PITR table */
        PITR_ADD,

        /* remove a PITR table */
        PITR_REMOVE,

        /* add a region */
        REGION_ADD,

        /* drop a region */
        REGION_DROP,

        /* change parameter */
        CHANGE_PARAM;

        /**
         * Returns true if request is a multi-region table request, false
         * otherwise.
         *
         * @return true if request is a multi-region table request, false
         * otherwise.
         */
        public boolean isMRTRequest() {
            return this.equals(MRT_ADD) || this.equals(MRT_REMOVE) ||
                   this.equals(MRT_DROP) || this.equals(MRT_UPDATE);
        }

        /**
         * Returns true if request is a region request, false otherwise.
         *
         * @return true if request is a region request, false otherwise.
         */
        public boolean isRegionRequest() {
            return this.equals(REGION_ADD) || this.equals(REGION_DROP);
        }

        /**
         * Returns true if request is a PITR request, false otherwise.
         *
         * @return true if request is a PITR request, false otherwise.
         */
        public boolean isPITRRequest() {
            return this.equals(PITR_ADD) || this.equals(PITR_REMOVE);
        }

        @Override
        public String toString() {
            switch(this) {
                case REGION_ADD: return "add region";
                case REGION_DROP: return "drop region";
                case SHUTDOWN: return "stop";
                case MRT_ADD: return "create or add region to table";
                case MRT_UPDATE: return "update table metadata";
                case MRT_REMOVE: return "remove region from table";
                case MRT_DROP: return "drop table";
                case PITR_ADD: return "add pitr table";
                case PITR_REMOVE: return "remove pitr table";
                case CHANGE_PARAM: return "change parameter";
                default: throw new IllegalArgumentException("Unsupported " +
                                                            this.name());
            }
        }
    }

    /* message type to represent the action to take */
    private final RequestType reqType;

    /* call back to process the response */
    private final XRegionRespHandlerThread resp;

    /**
     * Builds a request
     *
     * @param reqType    type of request
     * @param resp       response handler
     */
    private XRegionRequest(RequestType reqType,
                           XRegionRespHandlerThread resp) {
        this.reqType = reqType;
        this.resp = resp;
    }

    /**
     * Returns a request to add a MRT
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getAddMRTReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.MRT_ADD, resp);
    }

    /**
     * Returns a request to update table instance
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getUpdateMRTReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.MRT_UPDATE, resp);
    }

    /**
     * Returns a request to shut down the service
     *
     * @return request
     */
    public static XRegionRequest getShutdownReq() {
        return new XRegionRequest(RequestType.SHUTDOWN, null);
    }

    /**
     * Returns a request to remove a MRT
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getRemoveMRTReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.MRT_REMOVE, resp);
    }

    /**
     * Returns a request to remove a MRT
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getDropMRTReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.MRT_DROP, resp);
    }

    /**
     * Returns a request to add PITR tables from source region
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getAddPITRReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.PITR_ADD, resp);
    }

    /**
     * Returns a request to remove PITR tables from source region
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getRemovePITRReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.PITR_REMOVE, resp);
    }

    /**
     * Returns a request to change parameter for a table
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getChangeParamReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.CHANGE_PARAM, resp);
    }

    /**
     * Returns a request to add a region
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getAddRegionReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.REGION_ADD, resp);
    }


    /**
     * Returns a request to drop a region
     *
     * @param resp    response handler
     *
     * @return request
     */
    static XRegionRequest getDropRegionReq(XRegionRespHandlerThread resp) {
        return new XRegionRequest(RequestType.REGION_DROP, resp);
    }

    /**
     * Returns the request id
     *
     * @return the request id
     */
    long getReqId() {
        return (resp == null ? NULL_REQUEST_ID : resp.getReqId());
    }

    /**
     * Returns the request type
     *
     * @return the request type
     */
    RequestType getReqType() {
        return reqType;
    }

    /**
     * Returns the source region of the request
     *
     * @return the source region of the request
     */
    Set<RegionInfo> getSrcRegions() {
        if (resp == null) {
            return null;
        }
        return resp.getRegions();
    }

    Set<Table> getTableMD() {
        if (resp == null) {
            return null;
        }
        return resp.getTableMD();
    }

    /**
     * Returns the tables in request
     *
     * @return the tables in request
     */
    Set<String> getTables() {
        if (resp == null) {
            return null;
        }

        return resp.getTables();
    }

    /**
     * Returns the response callback
     *
     * @return the response callback
     */
    XRegionRespHandlerThread getResp() {
        return resp;
    }

    @Override
    public String toString() {
        return getReqId() + ": type=" + reqType + ", tables=" + getTables() +
               " from region=" + getSrcRegions().stream()
                                                .map(RegionInfo::getName)
                                                .collect(Collectors.toSet());
    }
}
