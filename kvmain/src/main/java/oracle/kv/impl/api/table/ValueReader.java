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

package oracle.kv.impl.api.table;

import oracle.kv.Version;
import oracle.kv.table.Table;

/**
 * The interface of row deserializer.
 */
public interface ValueReader<T> extends AvroRowReader {
    /*
     * These set* methods allow implementations to acquire metadata that is
     * usually associated only with a Row. They should arguably be
     * the same as the data methods and use the "read" prefix. They should
     * also perhaps be methods added by RowReaderImpl vs ValueReader, but
     * that's a thought for a future refactor.
     */
    void setTableVersion(int tableVersion);

    void setExpirationTime(long expirationTime);

    @SuppressWarnings(value = "unused")
    default void setCreationTime(long creationTime) {}

    @SuppressWarnings("unused")
    default void setModificationTime(long modificationTime) {}

    @SuppressWarnings("unused")
    default void setRowMetadata(String rowMetadata) {}

    void setVersion(Version version);

    @SuppressWarnings("unused")
    default void setRegionId(int regionId) {}

    @SuppressWarnings("unused")
    default void setTombstone(boolean isTombstone) {}

    T getValue();

    Table getTable();

    void reset();

    /* this doesn't belong here at all, but is kept for compat for now */
    void setValue(T value);
}
