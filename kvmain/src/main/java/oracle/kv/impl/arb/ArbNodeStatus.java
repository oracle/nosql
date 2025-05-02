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

package oracle.kv.impl.arb;

import java.io.Serializable;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * ArbNodeStatus represents the current status of a running ArbNodeService.  It
 * includes ServiceStatus as well as additional state specific to a ArbNode.
 */
public class ArbNodeStatus implements Serializable {

    private static final long serialVersionUID = 1L;
    private final ServiceStatus status;
    private final State arbState;
    private final long vlsn;
    private final String haHostPort;

    /**
     * Time when the ArbNode instance was created, or 0 if not available.
     * @since 21.2
     */
    private final long serviceStartTime;

    public ArbNodeStatus(ServiceStatus status, long vlsn,
                         State arbiterState, String haHostPort,
                         long serviceStartTime) {
        this.status = status;
        this.vlsn = vlsn;
        this.haHostPort = haHostPort;
        this.arbState = arbiterState;
        this.serviceStartTime = serviceStartTime;
    }

    /**
     * Returns the time when the ArbNode instance was created, or 0 if not
     * available.
     *
     * @return the service start time or 0
     */
    public long getServiceStartTime() {
        return serviceStartTime;
    }

    public ServiceStatus getServiceStatus() {
        return status;
    }

    public State getArbiterState() {
        return arbState;
    }

    public long getVlsn() {
        return vlsn;
    }

    /**
     * Returns the HA host and port string.
     *
     * @return the HA host and port string or null
     */
    public String getHAHostPort() {
        return haHostPort;
    }

    @Override
    public String toString() {
        return status.toString() ;
    }
}
