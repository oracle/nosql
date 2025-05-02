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

package oracle.kv.impl.api.ops;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.api.table.TargetTables;

/**
 * An index iteration that returns keys.  The index is specified by a
 * combination of the indexName and tableName.  The index scan range and
 * additional parameters are specified by the IndexRange.
 * <p>
 * Both primary and secondary keys are required for resumption of batched
 * operations because a single index match may match a large number of
 * primary records. In order to honor batch size constraints a single
 * request/reply operation may need to resume within a set of duplicates.
 * <p>
 * Unless ancestor return values are requested this operation returns only that
 * information which is available in the index itself avoiding extra fetches.
 * When an ancestor table is requested the operation will minimally verify that
 * the ancestor key exists in order to add it to the returned list.  Ancestor
 * keys are returned before the corresponding target table key.  This is true
 * even if the iteration is in reverse order.  In the event of multiple index
 * entries matching the same primary entry and/or ancestor entry, duplicate keys
 * will be returned.
 * <p>
 * The childTables parameter is a list of child tables to return. These are not
 * supported in R3 and will be null.
 * <p>
 * The ancestorTables parameter, if not null, specifies the ancestor tables to
 * return in addition to the target table, which is the table containing the
 * index.
 * <p>
 * The resumeSecondaryKey parameter is used for batching of results and will be
 * null on the first call
 * <p>
 * The resumePrimaryKey is used for batching of results and will be null
 * on the first call
 * <p>
 * The batchSize parameter is the batch size to to use
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class IndexKeysIterate extends IndexOperation {

    public IndexKeysIterate(String indexName,
                            TargetTables targetTables,
                            IndexRange range,
                            byte[] resumeSecondaryKey,
                            byte[] resumePrimaryKey,
                            int batchSize,
                            int maxReadKB,
                            int emptyReadFactor) {
        super(OpCode.INDEX_KEYS_ITERATE, indexName, targetTables, range,
              resumeSecondaryKey, resumePrimaryKey, batchSize,
              maxReadKB, emptyReadFactor);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     *
     * For subclasses, allows passing OpCode.
     */
    IndexKeysIterate(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.INDEX_KEYS_ITERATE, in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link IndexOperation}) {@code super}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    @Override
    public String toString() {
        return super.toString(); //TODO
    }
}
