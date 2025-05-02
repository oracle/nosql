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

package oracle.kv.impl.metadata;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.metadata.MetadataKey.MetadataKeyType;
import oracle.kv.impl.metadata.MetadataKey.MetadataKeyTypeFinder;
import oracle.kv.impl.metadata.MetadataKey.StdMetadataKeyType;

/** Class for registering and using MetadataKeyTypeFinders */
public class MetadataKeyTypeFinders {

    private static final List<MetadataKeyTypeFinder> finders =
        new ArrayList<>();
    static {
        addFinder(StdMetadataKeyType::valueOf);
    }

    /** Registers a MetadataKeyTypeFinder. */
    public static void addFinder(MetadataKeyTypeFinder finder) {
        finders.add(finder);
    }

    /**
     * Uses all registered MetadataKeyTypeFinders to find the MetadataKeyType
     * associated with the specified integer value, throwing an exception if not
     * found.
     */
    static MetadataKeyType findKeyType(int intValue) {
        for (final MetadataKeyTypeFinder finder : finders) {
            final MetadataKeyType keyType =
                finder.getMetadataKeyType(intValue);
            if (keyType != null) {
                return keyType;
            }
        }
        throw new IllegalArgumentException(
            "Wrong integer value for MetadataKeyType: " + intValue);
    }
}
