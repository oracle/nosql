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

package oracle.kv.impl.xregion.resolver;

/**
 * The ConflictResolver is used by the MR table to determine the winner from
 * two conflicting write ops. The resolver can be implemented by user and
 * transmitted to the data links during the creation of MR table.
 */
public interface ConflictResolver {

    /**
     * Resolves the conflict writes in local and remote region
     *
     * @param isExternalMultiRegion true if the operation is an external
     * multi-region operation
     * @param r1  row 1
     * @param r2  row 2
     *
     * @return the winner of the two conflicting write rows
     */
    KeyMetadata resolve(boolean isExternalMultiRegion,
                        final KeyMetadata r1,
                        final KeyMetadata r2);
}
