/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.health;

import java.net.HttpURLConnection;

import oracle.nosql.common.json.JsonUtils;

/**
 * HealthStatus indicates the health of a service in a simple format of a
 * Beacon, which be useful to get a quick health status.
 *
 * The HealthStatus can be used by a load balancer to check whether a service
 * instance can be used to service requests. For this purpose, the beacon
 * maps to an Http status code so that the load balancer can ascertain if the
 * web service instance is alive.
 *
 * Typically, most Beacon values will map to HTTP_OK which implies that the
 * instance should be used to service requests. Beacon values that map to
 * HTTP_UNAVAILABLE communicate that the instance is not functioning correctly
 * and should not be sent requests.
 */
public class HealthStatus {

    public static enum Beacon {

        /* Service is running correctly. */
        GREEN(HttpURLConnection.HTTP_OK),

        /* Service may be bootstrapping or has a temporary issue that
         * is expected to be resolved.
         */
        YELLOW(HttpURLConnection.HTTP_OK),

        /* Service is not functioning correctly and should not be
         * sent requests. */
        RED(HttpURLConnection.HTTP_UNAVAILABLE);

        /* The http status code associated with the beacon. */
        private final int httpStatusCode;

        Beacon(int httpStatusCode) {
            this.httpStatusCode = httpStatusCode;
        }

        public int getHttpStatusCode() {
            return httpStatusCode;
        }

        @Override
        public String toString() {
            return name();
        }
    }

    /* The Health Beacon. */
    private Beacon beacon;

    /*
     * Optional field describing the state of the service including any known
     * errors.
     */
    private String info;

    public HealthStatus() {
    }

    public HealthStatus(Beacon beacon) {
        this.beacon = beacon;
        this.info = "";
    }

    public HealthStatus(Beacon beacon, String info) {
        this.beacon = beacon;
        this.info = info;
    }

    public Beacon getBeacon() {
        return beacon;
    }

    public String getInfo() {
        return info;
    }

    public String toJsonString() {
        return JsonUtils.toJson(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((beacon == null) ? 0 : beacon.hashCode());
        result = prime * result
                + ((info == null) ? 0 : info.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof HealthStatus)) {
            return false;
        }
        HealthStatus other = (HealthStatus) obj;
        if (beacon == null) {
            if (other.beacon != null) {
                return false;
            }
        } else if (!beacon.equals(other.beacon)) {
            return false;
        }
        if (info == null) {
            if (other.info != null) {
                return false;
            }
        } else if (!info.equals(other.info)) {
            return false;
        }
        return true;
    }
}
