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

import java.io.Serializable;
import java.util.Comparator;

/**
 * FieldComparator is a simple implementation of Comparator&lt;String&gt; that
 * is used for case-insensitive String comparisons.  This is used to
 * implement case-insensitive, but case-preserving names for fields, tables,
 * and indexes.
 *
 * IMPORTANT: technically this class should be declared @Persistent and
 * stored with JE instances of TreeMap that use it, but JE does not
 * currently store Comparator instances.  As a result the code that
 * uses previously-stored JE entities will not have the comparator set.
 * Fortunately that list is restricted to persistent plans and tasks
 * for creation and evolution of tables, and further, that code indirectly
 * uses FieldMap to encapsulate the relevant maps and that class *always*
 * deep-copies the source maps so the Comparator will always be set in
 * TableMetadata and related objects.
 *
 * This is public so it can be accessed by .../rep/table/SecondaryInfoMap
 */
public class FieldComparator implements Comparator<String>, Serializable {
    public static final FieldComparator instance = new FieldComparator();
    private static final long serialVersionUID = 1L;

    /**
     * Comparator&lt;String&gt;
     */
    @Override
    public int compare(String s1, String s2) {
        return s1.compareToIgnoreCase(s2);
    }
}
