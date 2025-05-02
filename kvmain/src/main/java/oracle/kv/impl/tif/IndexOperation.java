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

package oracle.kv.impl.tif;


/**
 * Object representing an ES index operation for the text index service. The
 * operation can be either a PUT or DEL, defined in OperationType. For PUT, it
 * carries a JSON document for ES indexing. For both PUT and DEL it carries
 * an encoded primary key that identifies the original record.
 */

class IndexOperation {

    private final String esIndexName;                     /* name of ES index */
    private final OperationType operation;           /* operation: PUT or DEL */
    private final String pkPath;                /* encoded PK of the document */
    private final byte[] document;           /* document content, can be null */

    public IndexOperation(String esIndexName,
                          String pkPath,
                          byte[] document,
                          OperationType operation) {
        this.esIndexName = esIndexName;
        this.pkPath = pkPath;
        this.document = document;
        this.operation = operation;
    }

    public String getESIndexName() {
        return esIndexName;
    }

    public String getPkPath() {
        return pkPath;
    }

    public byte[] getDocument() {
        return document;
    }

    public OperationType getOperation() {
        return operation;
    }

    /**
     * Return a number of bytes this index operation represents, for the
     * purpose of calculating this operation's contribution to the size of a
     * bulk request.
     *
     * We take as given that ES uses UTF-8, therefore the number of bytes is
     * equal to the number of chars in the strings.
     *
     * We follow ES's BulkProcessor class's precedent for determining this
     * value, which is simply the length of an IndexRequest's payload "source"
     * field (represented in this class by "document") plus a fudge factor of
     * 50, which is referred to as REQUEST_OVERHEAD.
     *
     * It isn't clear what this REQUEST_OVERHEAD represents.  Possibly it
     * compensates for the lengths of the other fields in the IndexRequest: the
     * index name, type and id.  If that were known to be true then we could be
     * more precise, since we have those fields here in this object.  But then,
     * BulkProcessor also has this information and yet chooses to use a
     * constant fudge factor.
     *
     * This number does not need to be perfectly precise, so I am not
     * going to research this any further!
     */
    private static int FUDGE_FACTOR = 50;
    public int size() {
        final int s = (document == null ? 0 : document.length);
        return s + FUDGE_FACTOR;
    }

    @Override
    public String toString() {
        String fullPath = "/" + esIndexName + "/" + pkPath;

        return fullPath + ": " + document;
    }

    /* type of the operation to be performed on the search index. */
    public enum OperationType {

        /* put a record to ES index */
        PUT,
        /* delete a record from ES index */
        DEL;

        @Override
        public String toString() {
            switch (this) {
                case PUT:
                    return "PUT";
                case DEL:
                    return "DEL";
                default:
                    throw new IllegalStateException();
            }
        }
    }
}
