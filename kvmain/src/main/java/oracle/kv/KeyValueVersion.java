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
 * Represents a key/value pair along with its version.
 *
 * <p>A KeyValueVersion instance is returned by methods such as {@link
 * KVStore#storeIterator(Direction, int)} and {@link
 * KVStore#multiGetIterator(Direction, int, Key, KeyRange, Depth)}.  The key,
 * version and value properties will always be non-null.</p>
 */
public class KeyValueVersion {

    private final Key key;
    private final Value value;
    private final Version version;
    private final long modificationTime;

    /**
     * Internal use only
     * @hidden
     * Creates a KeyValueVersion with non-null properties.
     */
    public KeyValueVersion(final Key key,
                           final Value value,
                           final Version version) {
        assert key != null;
        assert value != null;
        assert version != null;
        this.key = key;
        this.value = value;
        this.version = version;
        this.modificationTime = 0;
    }

    /**
     * Internal use only
     * @hidden
     * Creates a KeyValueVersion with non-null properties and
     * a modification time.
     */
    public KeyValueVersion(final Key key,
                           final Value value,
                           final Version version,
                           final long modificationTime) {
        assert key != null;
        assert value != null;
        assert version != null;
        this.key = key;
        this.value = value;
        this.version = version;
        this.modificationTime = modificationTime;
    }

    /**
     * Internal use only
     * @hidden
     * Creates a KeyValueVersion with non-null values for key and value
     */
    public KeyValueVersion(final Key key,
                           final Value value) {
        assert key != null;
        assert value != null;
        this.key = key;
        this.value = value;
        this.version = null;
        this.modificationTime = 0;
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

    /**
     * Returns the Version of the KV pair.
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Internal use only
     * @hidden
     * Returns the expiration time of the record.This default method always
     * returns 0 (no expiration). A subclass may return a non-zero value.
     * See impl/api/KeyValueVersionInternal.
     * @since 4.0
     */
    public long getExpirationTime() {
        return 0L;
    }

    /**
     * Returns the modification time of the record. This method throws an
     * {@link UnsupportedOperationException} for records created when the store
     * was running at a version earlier than version 19.5, or if the
     * modification time is not available because the return value was not
     * requested.
     *
     * @return the modification time
     * @throws UnsupportedOperationException if the modification time is not
     * available
     * @hidden make it public when this method is added to other non-java
     * drivers.
     */
    public long getModificationTime() {
        if (modificationTime <= 0) {
            throw new UnsupportedOperationException("Modification " +
                "time is not available.");
        }
        return modificationTime;
    }

    @Override
    public String toString() {
        return key.toString() + ' ' + value + ' ' + version;
    }
}
