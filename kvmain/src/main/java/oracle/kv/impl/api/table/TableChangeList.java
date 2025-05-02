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

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

/**
 * Container for table change instances. A sequence of changes can be
 * applied to a TableMetadata instance, via {@link TableMetadata#apply}
 * to make it more current.
 */
public class TableChangeList implements MetadataInfo,
                                        Iterable<TableChange>,
                                        Serializable {
    private static final long serialVersionUID = 1L;

    public static final MetadataInfo EMPTY_TABLE_INFO =
            new TableChangeList(Metadata.EMPTY_SEQUENCE_NUMBER, null);

    private final int sourceSeqNum;
    private final List<TableChange> changes;

    TableChangeList(int sourceSeqNum, List<TableChange> changes) {
        this.sourceSeqNum = sourceSeqNum;
        this.changes = changes;
    }

    public TableChangeList(DataInput in, short serialVersion)
        throws IOException
    {
        sourceSeqNum = readPackedInt(in);
        changes = readCollection(in, serialVersion, ArrayList::new,
                                 TableChange::readTableChange);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writePackedInt(out, sourceSeqNum);
        writeCollection(out, serialVersion, changes,
                        TableChange::writeTableChange);
    }

    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public MetadataInfoType getMetadataInfoType() {
        return MetadataInfoType.TABLE_CHANGE_LIST;
    }

    @Override
    public int getSequenceNumber() {
        return sourceSeqNum;
    }

    @Override
    public boolean isEmpty() {
        return (changes == null) ? true : changes.isEmpty();
    }

    @Override
    public Iterator<TableChange> iterator() {
        assert changes != null; // TODO- maybe make this an interator.?
        return changes.iterator();
    }

    @Override
    public String toString() {
        return "TableChangeList[" + sourceSeqNum + ", " +
               (isEmpty() ? "-" : changes.size()) + "]";
    }
}
