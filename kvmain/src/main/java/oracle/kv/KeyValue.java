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
 * Represents a key/value pair.
 *
 * <p>The KeyValue is used as element of input argument
 * EntryStream&lt;KeyValue&gt; for {@link KVStore#put put}, the key and value
 * properties will always be non-null.</p>
 *
 * @since 4.0
 */
public class KeyValue {

    private final Key key;
    private final Value value;

    /**
     * Creates a KeyValue, key and value should be non-null.
     */
    public KeyValue(final Key key, final Value value) {

        if (key == null) {
            throw new IllegalArgumentException("key argument must not be null");
        }

        if (value == null) {
            throw new IllegalArgumentException("value argument must not be " +
            		                           "null");
        }

        this.key = key;
        this.value = value;
    }

    /**
     * Returns the Key part of the KV pair.
     */
    public Key getKey() {
        return key;
    }

    /**
     * Returns the Value part of the KV pair.
     */
    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return key.toString() + ' ' + value + ' ';
    }
}
