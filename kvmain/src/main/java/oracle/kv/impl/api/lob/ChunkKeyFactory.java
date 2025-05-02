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

package oracle.kv.impl.api.lob;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.Key;

/**
 * Utility methods associated with the manipulation of chunk keys
 */
public class ChunkKeyFactory {

    /**
     * Produce a key that's compatible with this metadata version.
     */
    private final int metadataVersion;

    /**
     * The radix used to encode the chunk key
     */
    static final int KEY_RADIX = 32;

    ChunkKeyFactory(int metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    /**
     * Creates a chunk key suitable for "getting" the chunk.
     */
    Key create(Key internalLobKey,
                      long superChunkId,
                      long chunkId) {

        if (! (superChunkId > 0 && chunkId > 0)) {
            throw new IllegalArgumentException("super chunk id:" +
                                                superChunkId +
                                                "chunk id:" + chunkId);
        }
        final List<String> majorPath =
            new ArrayList<String>(internalLobKey.getMajorPath());
        majorPath.add(getIdString(superChunkId));

        final List<String> minorPath =
                new ArrayList<String>(internalLobKey.getMinorPath());
        minorPath.add(getIdString(chunkId));

        return Key.createKey(majorPath, minorPath);
    }

    /**
     * Create a super chunk key that can be used as the basis for getting the
     * chunk keys associated with it.
     */
    Key createSuperChunkKey(Key internalLobKey,
                                   long superChunkId) {

        if (superChunkId <= 0) {
            throw new IllegalArgumentException("Invalid super chunk id:" +
                                                superChunkId);
        }

        final List<String> majorPath =
            new ArrayList<String>(internalLobKey.getMajorPath());
        majorPath.add(getIdString(superChunkId));
        return Key.createKey(majorPath);
    }

    /**
     * Parses a chunk key to obtain the chunk id.
     */
    int getChunkId(Key chunkKey) {
       return Integer.parseInt(chunkKey.getMinorPath().get(0), KEY_RADIX);
    }

    /**
     * Used to format the super chunk and chunk ids into string key components.
     */
    private String getIdString(long i) {
        if (metadataVersion == 1) {
            /* For compatibility with version 1 */
            final String s = Long.toString(i, KEY_RADIX);
            /*
             * Note bug in version 1, substring expression should have been:
             *  "0000000".substring(0, 7 - s.length())
             */
            return "0000000".substring(7 - s.length()) + s;
        }
        return Long.toString(i, KEY_RADIX);
    }

    int getMetadataVersion() {
        return metadataVersion;
    }
}
