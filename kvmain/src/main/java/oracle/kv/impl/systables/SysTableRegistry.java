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

package oracle.kv.impl.systables;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.api.table.TableImpl;

/**
 * The registry for system tables. Every system table should have a
 * descriptor in the descriptors array.
 */
public class SysTableRegistry {

    /**
     * The system tables. (keep in alphabetical order for readability)
     */
    public static final SysTableDescriptor[] descriptors = {
                                            new IndexStatsLeaseDesc(),
                                            new MRTableAgentStatDesc(),
                                            new MRTableInfoDesc(),
                                            new MRTableInitCkptDesc(),
                                            new PartitionStatsLeaseDesc(),
                                            new SGAttributesTableDesc(),
                                            new StreamRequestDesc(),
                                            new StreamResponseDesc(),
                                            new TableMetadataDesc(),
                                            new TableStatsIndexDesc(),
                                            new TableStatsPartitionDesc(),
                                            new TopologyHistoryDesc()
                                            };

    private static final Map<String, SysTableDescriptor> tableNameMap =
            new HashMap<>();
    static {
        for (SysTableDescriptor descriptor : descriptors) {
            tableNameMap.put(descriptor.getTableName(), descriptor);
        }
    }

    private SysTableRegistry() { }

    /**
     * Gets the descriptor instance for the specified class.
     *
     * For unit tests.
     */
    public static SysTableDescriptor getDescriptor(Class<?> c) {
        for (SysTableDescriptor desc : descriptors) {
            if (c.isInstance(desc)) {
                return desc;
            }
        }
        throw new IllegalStateException(
                                "Requesting unknown system table descriptor: " +
                                c.getSimpleName());
    }

    /**
     * Returns the descriptor for the associated system table.
     *
     * @param table the system table
     * @return the descriptor
     * @throws IllegalArgumentException if table is not a system table
     */
    public static SysTableDescriptor getDescriptor(TableImpl table) {
        if (!table.isSystemTable()) {
            throw new IllegalArgumentException(
                    "Argument is not a system table");
        }
        final SysTableDescriptor descriptor = tableNameMap.get(table.getName());
        if (descriptor == null) {
            throw new IllegalStateException(
                    "No descriptor found for table " + table.getName());
        }
        return descriptor;
    }
}
