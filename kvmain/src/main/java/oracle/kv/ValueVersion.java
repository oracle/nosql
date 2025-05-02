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

package oracle.kv;

/**
 * Holds a Value and Version that are associated with a given Key.
 *
 * <p>A ValueVersion instance is returned by methods such as {@link KVStore#get
 * get} and {@link KVStore#multiGet multiGet} as the current value and version
 * associated with a given key.  The version and value properties will always
 * be non-null.</p>
 */
public class ValueVersion {

    private Value value;
    private Version version;

    /**
     * Used internally to create an object with null value and version.
     */
    public ValueVersion() {
    }

    /**
     * Used internally to create an object with a value and version.
     */
    public ValueVersion(Value value, Version version) {
        this.value = value;
        this.version = version;
    }

    /**
     * Returns the Value part of the KV pair.
     */
    public Value getValue() {
        return value;
    }

    /**
     * Returns the Version of the KV pair.
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Used internally to initialize the Value part of the KV pair.
     */
    public ValueVersion setValue(Value value) {
        this.value = value;
        return this;
    }

    /**
     * Used internally to initialize the Version of the KV pair.
     */
    public ValueVersion setVersion(Version version) {
        this.version = version;
        return this;
    }

    @Override
    public String toString() {
        return "<ValueVersion " + value + ' ' + version + '>';
    }
}
