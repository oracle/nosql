/*-
 * Copyright (C) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.util;

/**
 * Service Directory is used by spartakv and httpproxy. It's responsible for
 * providing a guaranteed unique service name for each pod. This unique name is
 * needed to support multi-region tables, where services replicate data to one
 * another, and must have a notion of their own identities, and a minimal
 * understanding of the identities of their peers.
 *
 * Concepts:
 *   - the service name is a human-readable unique name for the service, i.e
 *       IADPP, or PHXPROD
 *   - the service integer is an encoding of the service name, for persistence.
 *       It is a non-negative value, because kvstore on-prem attaches semantic
 *       meaning to a negative region id when processing CRDT fields.
 *   - the customer name is an OCI region name, which is well known and part
 *       of the public OCI api, i.e us-ashburn-1.
 */
public interface ServiceDirectory {

    /**
     * @return the service name for this pod
     */
    public String getLocalServiceName();

    /**
     * @return the integer version of the local service id, for use in
     * data persistence
     */
    public int getLocalServiceInteger();

    /**
     * Given a service name, find the customer facing region name
     */
    public String translateToRegionName(String serviceName);

    /**
     * Validate that the OCI region name passed in by an API call is known to
     * this pod and exists in its service directory.
     *
     * Note that this method doesn't validate whether this specific API call is
     * permitted to replicate to that target region, it only checks that the
     * argument is a valid, known region. Checking whether it's permissible to
     * target that region is done at another layer of the cloud.
     *
     * @param target region name. This is the customer facing, well known
     *        oci region name.
     * @return service name of this target region.
     */
    public String validateRemoteReplica(String targetRegionName);
}
