/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.nio.ByteBuffer;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 * This class is also used within the proxy to ferry table related info.
 */
public class IndexInfo {
    private static final int CURRENT_VERSION = 1;
    private int version;

    public enum IndexState {
        ACTIVE,
        CREATING,
        DROPPING,
        DROPPED,
    }

    private String indexName;
    private IndexField[] indexFields;
    private IndexState state;
    private long createTime;

    /* Needed for serialization */
    public IndexInfo() {
    }

    public IndexInfo(String indexName,
                     IndexField[] indexFields,
                     IndexState state,
                     long createdTime) {
        super();
        this.version = CURRENT_VERSION;
        this.indexName = indexName;
        this.indexFields = indexFields;
        this.state = state;
        this.createTime = createdTime;
    }

    public int getVersion() {
        return version;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexField[] getIndexFields() {
        return indexFields;
    }

    public void setState(IndexState state) {
        this.state = state;
    }

    public IndexState getState() {
        return state;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    /* Generates ETag */
    public byte[] getETag() {
        if (getState() != IndexState.ACTIVE) {
            return null;
        }

        /* Use "createTime" as ETag of Index */
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(createTime);
        return buffer.array();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{name=").append(indexName);
        sb.append(", fields=[");
        boolean first = true;
        for (IndexField field : indexFields) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(field);
        }
        sb.append("], state=").append(state.name())
          .append(", createTime=").append(createTime)
          .append("}");
        return sb.toString();
    }

    /**
     * Represents for single Index Field, the "type" field is non-null if
     * the Index Field is JSON type.
     */
    public static class IndexField {
        private String path;
        private String type;

        /* Needed for serialization */
        public IndexField() {
        }

        public IndexField(String path) {
            this(path, null);
        }

        public IndexField(String path, String type) {
            this.path = path;
            this.type = type;
        }

        public String getPath() {
            return path;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            if (type != null) {
                return path + " as " + type;
            }
            return path;
        }
    }
}
