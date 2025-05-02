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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElement;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * Class for recording a change of SecurityMetadata on its elements. The change
 * includes three types of operation so far: ADD, UPDATE and REMOVE. The type
 * of element operated on will also be recorded.
 */
public abstract class SecurityMDChange
        implements FastExternalizable, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public enum SecurityMDChangeType implements FastExternalizable {
        ADD(0, Add::new),
        UPDATE(1, Update::new),
        REMOVE(2, Remove::new);

        private static final SecurityMDChangeType[] VALUES = values();
        private final ReadFastExternal<SecurityMDChange> reader;

        SecurityMDChangeType(final int ordinal,
                             final ReadFastExternal<SecurityMDChange> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        static
        SecurityMDChangeType readFastExternal(DataInput in,
                                              @SuppressWarnings("unused")
                                              short serialVersion)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown SecurityMDChangeType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        SecurityMDChange readSecurityMDChange(DataInput in,
                                              short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    protected int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    SecurityMDChange(final int seqNum) {
        this.seqNum = seqNum;
    }

    /*
     * Ctor with deferred sequence number setting.
     */
    private SecurityMDChange() {
    }

    SecurityMDChange(DataInput in,
                     @SuppressWarnings("unused") short serialVersion)
        throws IOException
    {
        seqNum = readPackedInt(in);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writePackedInt(out, seqNum);
    }

    /**
     * Get the sequence number of the security metadata change.
     */
    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(final int seqNum) {
        this.seqNum = seqNum;
    }

    /**
     * Get the type of element involved in the change
     */
    public abstract SecurityElementType getElementType();

    public abstract SecurityMDChangeType getChangeType();

    public abstract SecurityElement getElement();

    abstract String getElementId();

    @Override
    public abstract SecurityMDChange clone();

    static SecurityMDChange readSecurityMDChange(final DataInput in,
                                                 final short sv)
        throws IOException
    {
        return SecurityMDChangeType.readFastExternal(in, sv)
            .readSecurityMDChange(in, sv);
    }

    void writeSecurityMDChange(final DataOutput out, final short serialVersion)
        throws IOException
    {
         getChangeType().writeFastExternal(out, serialVersion);
         writeFastExternal(out, serialVersion);
    }

    /**
     * The change of UPDATE.
     */
    public static class Update extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        SecurityElement element;

        public Update(final SecurityElement element) {
            this.element = element;
        }

        private Update(final int seqNum, final SecurityElement element) {
            super(seqNum);
            this.element = element;
        }

        private Update(final DataInput in, final short serialVersion)
            throws IOException
        {
            super(in, serialVersion);
            element = SecurityElement.readSecurityElement(in, serialVersion);
        }

        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            super.writeFastExternal(out, sv);
            element.writeSecurityElement(out, sv);
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.UPDATE;
        }

        @Override
        public SecurityElement getElement() {
            return element;
        }

        @Override
        String getElementId() {
            return element.getElementId();
        }

        @Override
        public SecurityElementType getElementType() {
            return element.getElementType();
        }

        @Override
        public Update clone() {
            return new Update(seqNum, element.clone());
        }
    }

    /**
     * The change of ADD. It extends the CHANGE class since it shares almost
     * the same behavior and codes of CHANGE except the ChangeType.
     */
    public static class Add extends Update {

        private static final long serialVersionUID = 1L;

        public Add(final SecurityElement element) {
            super(element);
        }

        private Add(final int seqNum, final SecurityElement element) {
            super(seqNum, element);
        }

        private Add(final DataInput in, final short serialVersion)
            throws IOException
        {
            super(in, serialVersion);
        }

        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            super.writeFastExternal(out, sv);
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.ADD;
        }

        @Override
        public Add clone() {
            return new Add(seqNum, element.clone());
        }
    }

    /**
     * The change of REMOVE.
     */
    public static class Remove extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        private String elementId;
        private SecurityElement element;
        private SecurityElementType elementType;

        public Remove(final String removedId,
                      final SecurityElementType eType,
                      final SecurityElement element) {
            this.elementId = removedId;
            this.elementType = eType;
            this.element = element;
        }

        private Remove(final int seqNum,
                       final String removedId,
                       final SecurityElementType eType,
                       final SecurityElement element) {
            super(seqNum);
            this.elementId = removedId;
            this.elementType = eType;
            this.element = element;
        }

        private Remove(final DataInput in, final short sv) throws IOException {
            super(in, sv);
            elementId = readString(in, sv);
            elementType = SecurityElementType.readFastExternal(in, sv);
            element = SecurityElement.readSecurityElement(in, sv);
        }

        @Override
        public void writeFastExternal(final DataOutput out,
                                      final short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
            writeString(out, serialVersion, elementId);
            elementType.writeFastExternal(out, serialVersion);
            element.writeSecurityElement(out, serialVersion);
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.REMOVE;
        }

        @Override
        public SecurityElement getElement() {
            return element;
        }

        @Override
        String getElementId() {
            return elementId;
        }

        @Override
        public SecurityElementType getElementType() {
            return elementType;
        }

        @Override
        public Remove clone() {
            return new Remove(seqNum, elementId, elementType, element);
        }
    }
}
