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

/**
 * Base Parametrized class for Request objects.
 * 
 * All Request objects also implement the Builder pattern, so the base class
 * has builder setter methods for two common properties, in all request objects
 * - index and type. They return the parameter type.
 * 
 * Currently FTS models an index on a table. And consraints iteself to one
 * search request on one index only.
 * 
 * When search over indices is added, same can be changed to indices[] Also,
 * null value for indices[] would mean search all indices and all types.
 * 
 * One interface to be enforced semantically is that every concrete ESRequest
 * subtype is also a builder, so all set methods should return the "this"
 * object instance.
 * 
 *
 */
public abstract class ESRequest<T> {

    protected String index;

    public ESRequest() {
    }

    public ESRequest(String index) {
        this.index = index;
    }

    public String index() {
        return index;
    }

    @SuppressWarnings("unchecked")
    public T index(String index1) {
        this.index = index1;
        return (T) this;
    }

    public abstract RequestType requestType();

    public static enum RequestType {
        /* Document Operations */
        INDEX, DELETE,
        /* For now FTS does not use CREATE/UPDATE operation */
        CREATE, UPDATE, BULK_INDEX, GET, SEARCH,
        /* Commit Documents */
        REFRESH,

        /* MetaData/Admin Operations */
        CREATE_INDEX, PUT_MAPPING, GET_MAPPING, EXIST_INDEX, MAPPING_EXIST, GET_INDEX, DELETE_INDEX, ES_VERSION,

        /* Monitoring Operations */
        CLUSTER_HEALTH, GET_NODES, CAT;

        public static RequestType get(String reqTypeStr) {
            for (RequestType reqType : RequestType.values()) {
                if (reqTypeStr.toLowerCase().equals(
                                                    reqType.toString()
                                                           .toLowerCase())) {
                    return reqType;
                }
            }
            return null;
        }
    }
}
