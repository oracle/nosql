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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.util.HostPort;

/**
 * Object that represents a region.
 */
public class RegionInfo implements Serializable, Comparable<RegionInfo> {

    private static final long serialVersionUID = 1L;

    /* immutable region name, case-insensitive */
    private final String name;

    /* immutable store name */
    private final String store;

    /* path to security file, null if non-secure store */
    private String security;

    /*  helper host and port */
    private final Set<String> helpers;

    /**
     * Create a region object with helper hosts
     *
     * @param name      region name
     * @param store     store name
     * @param helpers   list of access points
     */
    public RegionInfo(String name, String store, String[] helpers) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid region name");
        }
        if (store == null || store.isEmpty()) {
            throw new IllegalArgumentException("Invalid store name");
        }

        this.name = name;
        this.store = store;
        this.helpers = new HashSet<>();
        security = null;
        if (helpers != null && helpers.length > 0) {
            Arrays.stream(helpers).forEach(this::addHelper);
        }
        validate();
    }

    /**
     * Returns the name of the region
     *
     * @return the region name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the security file or null if not secure
     *
     * @return the security file or null
     */
    public String getSecurity() {
        return security;
    }

    /**
     * Returns the name of the store
     *
     * @return the store name
     */
    public String getStore() {
        return store;
    }

    /**
     * Returns helper hosts of the region. The returned array will be empty
     * if helpers are not set
     *
     * @return helper hosts of the region
     */
    public String[] getHelpers() {
        return helpers.toArray(new String[0]);
    }

    /**
     * Sets the path to security file, null for non-secure store
     *
     * @param security the path to security file or null
     */
    public void setSecurity(String security) {
        this.security = security;
    }

    /**
     * Adds a helper host for the region
     *
     * @param hostPort helper host
     * @return true if the new access point is added to the set, false
     * otherwise.
     */
    boolean addHelper(String hostPort) {
        HostPort.parse(hostPort);
        return helpers.add(hostPort);
    }

    /**
     * Returns true if the region is a secure store, false otherwise
     *
     * @return true if the region is a secure store, false otherwise
     */
    boolean isSecureStore() {
        return security != null;
    }

    /**
     * Removes a helper host from the list
     *
     * @param hostPort helper host to remove
     * @return true if successfully removes the host, false otherwise.
     */
    boolean removeHelper(String hostPort) {
        HostPort.parse(hostPort);
        return helpers.remove(hostPort);
    }

    @Override
    public int compareTo(RegionInfo obj) {
        return name.compareToIgnoreCase(obj.name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final RegionInfo other = (RegionInfo) obj;
        if (isSecureStore() != other.isSecureStore()) {
            return false;
        }
        if (isSecureStore() && !security.equals(other.security)) {
            return false;
        }
        /*
         * Two regions considered to be equal as long as both store id and
         * store name match, they do not need to share the same set of helper
         * hosts.
         */
        return name.equalsIgnoreCase(other.getName()) &&
               store.equals(other.getStore());
    }

    @Override
    public int hashCode() {
        return name.hashCode() + store.hashCode() +
               (isSecureStore() ? security.hashCode() : 0);
    }

    @Override
    public String toString() {
        return name + "(" +
               "store=" + store +
               ", security file=" +
               (isSecureStore() ? security : "non-secure store") +
               ", helpers=" + helpers + ")";
    }


    /* check required parameters */
    void validate() {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Missing region name");
        }
        JsonConfig.validateRegionName(name);
        if (store == null || store.isEmpty()) {
            throw new IllegalArgumentException("Missing store name in " +
                                               "region=" + name);
        }
        if (helpers == null || helpers.isEmpty()) {
            throw new IllegalArgumentException("Missing helper hosts in " +
                                               "region=" + name);
        }
    }
}
