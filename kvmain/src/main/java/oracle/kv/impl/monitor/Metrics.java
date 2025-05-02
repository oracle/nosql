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

package oracle.kv.impl.monitor;

import oracle.kv.impl.measurement.MeasurementType;

/**
 * Provides a catalog of KVStore monitoring measurement types.
 *
 * The id field of the MeasurementType is used as the identifier for
 * metrics transmitted on the network. Because of that, any change or
 * addition of id values constitutes a network protocol change. Existing id
 * values should never be changed for any metric that has been used in an
 * production system.
 *
 * The Metrics catalog will need not be transmitted, and is not Serializable.
 */
public class Metrics {

    /*
     * This defines the types of monitoring packets sent within the KVS.
     */
    public static MeasurementType LOG_MSG = new MeasurementType
        (1, "Info", "Logging output from kvstore components.");
    public static MeasurementType SERVICE_STATUS = new MeasurementType
        (2, "ServiceStatus", "Service status for kvstore components.");
    public static MeasurementType PLAN_STATE = new MeasurementType
        (3, "PlanState", "Plan execution state changes.");
    public static MeasurementType PRUNED = new MeasurementType
        (4, "PrunedMeasurements",
         "Record of measurements that have aged out of the monitoring " +
         "repository before being viewed");

    /*
     * Note that this type is currently also used for StorageNodes, Admins, and
     * ArbNodes.
     */
    public static MeasurementType RNSTATS = new MeasurementType
        (10, "RepNodeStats", "Collection of stats for a single RepNode");
}
