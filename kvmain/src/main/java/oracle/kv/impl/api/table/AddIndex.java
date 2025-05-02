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
import static oracle.kv.impl.util.SerializationUtil.readMap;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeMap;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;
import oracle.kv.table.FieldDef;

/**
 * A TableChange to add an index.
 */
class AddIndex extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String description;
    private final String tableName;
    private final String namespace;
    private final List<String> fields;
    private final List<FieldDef.Type> types;
    private final Map<String,String> annotations;
    private final Map<String,String> properties;
    private final boolean skipNulls;
    private final boolean isUnique;

    AddIndex(String namespace,
             String indexName,
             String tableName,
             List<String> fields,
             List<FieldDef.Type> types,
             boolean indexNulls,
             boolean isUnique,
             String description,
             int seqNum) {

        this(namespace, indexName, tableName, fields, types, indexNulls,
             isUnique, null, null, description, seqNum);
    }

    AddIndex(String namespace,
             String indexName,
             String tableName,
             List<String> fields,
             Map<String,String> annotations,
             Map<String,String> properties,
             String description,
             int seqNum) {

        this(namespace, indexName, tableName, fields, null, true, false,
             annotations, properties, description, seqNum);
    }

    private AddIndex(String namespace,
                     String indexName,
                     String tableName,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     boolean indexNulls,
                     boolean isUnique,
                     Map<String,String> annotations,
                     Map<String,String> properties,
                     String description,
                     int seqNum) {

        super(seqNum);
        name = indexName;
        this.description = description;
        this.tableName = tableName;
        this.namespace = namespace;
        this.fields = fields;
        this.types = types;
        this.skipNulls = !indexNulls;
        this.isUnique = isUnique;
        this.annotations = annotations;
        this.properties = properties;
    }

    AddIndex(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        name = readString(in, serialVersion);
        description = readString(in, serialVersion);
        tableName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
        fields = readCollection(in, serialVersion, ArrayList::new,
                                SerializationUtil::readString);
        types = readCollection(in, serialVersion, ArrayList::new,
                               FieldDef.Type::readFastExternal);
        skipNulls = in.readBoolean();
        isUnique = in.readBoolean();
        annotations = readMap(in, serialVersion, HashMap::new,
                              SerializationUtil::readString,
                              SerializationUtil::readString);
        properties = readMap(in, serialVersion, HashMap::new,
                             SerializationUtil::readString,
                             SerializationUtil::readString);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writeString(out, serialVersion, description);
        writeString(out, serialVersion, tableName);
        writeString(out, serialVersion, namespace);
        writeCollection(out, serialVersion, fields,
                        WriteFastExternal::writeString);
        writeCollection(out, serialVersion, types);
        out.writeBoolean(skipNulls);
        out.writeBoolean(isUnique);
        writeMap(out, serialVersion, annotations,
                 WriteFastExternal::writeString,
                 WriteFastExternal::writeString);
        writeMap(out, serialVersion, properties,
                 WriteFastExternal::writeString,
                 WriteFastExternal::writeString);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        final IndexImpl index= (annotations == null) ?
                    md.insertIndex(namespace, name, tableName,
                                   fields, types, !skipNulls, isUnique,
                                   description) :
                    md.insertTextIndex (namespace, name, tableName, fields,
                                        annotations, properties, description);
        return index.getTable();
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.ADD_INDEX;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AddIndex)) {
            return false;
        }
        final AddIndex other = (AddIndex) obj;
        return Objects.equals(name, other.name) &&
            Objects.equals(description, other.description) &&
            Objects.equals(tableName, other.tableName) &&
            Objects.equals(namespace, other.namespace) &&
            Objects.equals(fields, other.fields) &&
            Objects.equals(types, other.types) &&
            Objects.equals(annotations, other.annotations) &&
            Objects.equals(properties, other.properties) &&
            (skipNulls == other.skipNulls) &&
            (isUnique == other.isUnique);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            name,
                            description,
                            tableName,
                            namespace,
                            fields,
                            types,
                            annotations,
                            properties,
                            skipNulls,
                            isUnique);
    }
}
