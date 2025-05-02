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

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.WriteFastExternal;

/**
 * Container class for a list of metadata related objects.
 */
public abstract class InfoList<T> implements Iterable<T>,
                                             MetadataInfo,
                                             Serializable {
    private static final long serialVersionUID = 1L;

    protected final List<T> elements;
    protected final int seqNum;

    /**
     * Constructs an empty list.
     */
    protected InfoList(int seqNum) {
        this(seqNum, null);
    }

    /**
     * Constructs a list initialized with elements from the specified collection.
     */
    protected InfoList(int seqNum, Collection<? extends T> c) {
        this.seqNum = seqNum;
        elements = (c == null) ? new ArrayList<>() : new ArrayList<>(c);
    }

    protected InfoList(DataInput in,
                       short serialVersion,
                       ReadFastExternal<T> elementReader)
        throws IOException
    {
        seqNum = readPackedInt(in);
        elements = readCollection(in, serialVersion, ArrayList::new,
                                  elementReader);
    }

    protected void writeFastExternal(DataOutput out,
                                     short serialVersion,
                                     WriteFastExternal<T> elementWriter)
        throws IOException
    {
        writePackedInt(out, seqNum);
        writeCollection(out, serialVersion, elements, elementWriter);
    }

    /**
     * Returns the elements in this list.
     */
    public List<T> get() {
        return elements;
    }

    public void add(T e) {
        elements.add(e);
    }

    /* -- From Iterable -- */

    @Override
    public Iterator<T> iterator() {
        return elements.iterator();
    }

    /* -- From MetadataInfo -- */

    @Override
    public int getSequenceNumber() {
        return seqNum;
    }

    @Override
    public boolean isEmpty() {
        return elements.isEmpty();
    }
}
