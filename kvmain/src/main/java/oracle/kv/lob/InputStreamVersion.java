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

package oracle.kv.lob;

import java.io.InputStream;

import oracle.kv.Version;

/**
 * Holds a Stream and Version that are associated with a LOB.
 *
 * <p>
 * An InputStreamVersion instance is returned by {@link KVLargeObject#getLOB}
 * as the current value (represented by the stream) and version associated with
 * a given LOB. The version and inputStream properties will always be non-null.
 * </p>
 * IOExceptions thrown by this stream may wrap KVStore exceptions as described
 * in the documentation for the {@link KVLargeObject#getLOB} method.
 *
 * @since 2.0
 */
public class InputStreamVersion {

    private final InputStream inputStream;
    private final Version version;

    /**
     * Used internally to create an object with an inputStream and version.
     */
    public InputStreamVersion(InputStream inputStream, Version version) {
        this.inputStream = inputStream;
        this.version = version;
    }

    /**
     * Returns the InputStream part of the InputStream and Version pair.
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Returns the Version of the InputStream and Version pair.
     */
    public Version getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "<InputStreamVersion " + inputStream + ' ' + version + '>';
    }
}