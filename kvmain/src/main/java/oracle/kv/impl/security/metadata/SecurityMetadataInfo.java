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

package oracle.kv.impl.security.metadata;

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

public class SecurityMetadataInfo implements MetadataInfo, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SecurityMDChange> changeList;
    private final String securityMetadataId;
    private final int sequenceNum;

    public static final SecurityMetadataInfo EMPTY_SECURITYMD_INFO =
        new SecurityMetadataInfo(null, -1, null);

    public SecurityMetadataInfo(final SecurityMetadata secMD,
                                final List<SecurityMDChange> changes) {
        this(secMD.getId(), secMD.getSequenceNumber(), changes);
    }

    public SecurityMetadataInfo(final String secMDId,
                                final int latestSeqNum,
                                final List<SecurityMDChange> changes) {
        this.securityMetadataId = secMDId;
        this.sequenceNum = latestSeqNum;
        this.changeList = changes;
    }

    public SecurityMetadataInfo(final DataInput in, final short serialVersion)
        throws IOException
    {
        securityMetadataId = readString(in, serialVersion);
        sequenceNum = readPackedInt(in);
        changeList = readCollection(in, serialVersion, LinkedList::new,
                                    SecurityMDChange::readSecurityMDChange);
    }

    @Override
    public void writeFastExternal(final DataOutput out,
                                  final short serialVersion)
        throws IOException
    {
        writeString(out, serialVersion, securityMetadataId);
        writePackedInt(out, sequenceNum);
        writeCollection(out, serialVersion, changeList,
                        SecurityMDChange::writeSecurityMDChange);
    }

    @Override
    public MetadataType getType() {
        return MetadataType.SECURITY;
    }

    @Override
    public MetadataInfoType getMetadataInfoType() {
        return MetadataInfoType.SECURITY_METADATA_INFO;
    }

    @Override
    public int getSequenceNumber() {
        return sequenceNum;
    }

    @Override
    public boolean isEmpty() {
        return (changeList == null) || (changeList.isEmpty());
    }

    public String getSecurityMetadataId() {
        return securityMetadataId;
    }

    public List<SecurityMDChange> getChanges() {
        return changeList;
    }
}
