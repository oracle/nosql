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

package oracle.kv.impl.api;

import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.Version;

/**
 * Adds an expiration time to KeyValueVersion, making its
 * getExpirationTime() interface return a valid value.
 */
public class KeyValueVersionInternal extends KeyValueVersion {

    private final long expirationTime;

    /**
     * Creates a KeyValueVersion with non-null properties, extending it
     * to include an expiration time.
     */
    public KeyValueVersionInternal(final Key key,
                                   final Value value,
                                   final Version version,
                                   final long expirationTime,
                                   final long modificationTime) {
        super(key, value, version, modificationTime);
        this.expirationTime = expirationTime;
    }

    /**
     * Creates a KeyValueVersion with non-null properties for key and value,
     * extending it to include an expiration time.
     */
    public KeyValueVersionInternal(final Key key,
                                   final Value value,
                                   final long expirationTime) {
        super(key, value);
        this.expirationTime = expirationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public String toString() {
        return super.toString() + ' ' + expirationTime;
    }
}
