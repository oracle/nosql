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

import java.io.DataOutput;
import java.util.List;

import oracle.kv.Version;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

/**
 * TupleValue instances are created internally only during query execution
 * and are never returned to the application.
 *
 * See the "Records vs tuples" section in the javadoc of PlanIter (in
 * impl/query/runtime/PlanIter.java) for more details.
 *
 * This class could have been placed in the query.runtime package. However
 * doing so would require making many of the package-scoped methds of
 * FieldValueImpl public or protected methods. This is the reason we chose
 * to put TupleValue in the api.table package.
 */
public class TupleValue extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    final FieldValueImpl[] theRegisters;

    final int[] theTupleRegs;

    final RecordDefImpl theDef;

    TableImpl theTable; 

    IndexImpl theIndex;

    boolean theIsIndexEntry;

    long theExpirationTime;

    long theCreationTime;

    long theModificationTime;

    int thePartition;

    int theStorageSize = -1;

    int theIndexStorageSize = -1;

    Version theRowVersion;

    String rowMetadata;


    public TupleValue(
        RecordDefImpl def,
        FieldValueImpl[] regs,
        int[] regIds) {

        super();
        theRegisters = regs;
        theTupleRegs = regIds;
        theDef = def;
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.TUPLE_VALUE;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        /*
         * This class is only used during query return and is not serialized
         * with the table metadata otherwise. So there is no need to support
         * FastExternal.
         */
        fastExternalNotSupported();
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public FieldValueImpl clone() {
        throw new IllegalStateException(
            "TupleValue does not implement clone");
    }

    @Override
    public String toString() {
        return toJsonString(false);
    }

    @Override
    public long sizeof() {
        throw new IllegalStateException("Unexpected call");
    }

    @Override
    public int hashCode() {
        int code = size();
        for (int i = 0; i < size(); ++i) {
            String fname = theDef.getFieldName(i);
            code += fname.hashCode();
            code += get(i).hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object otherObj) {

        if (this == otherObj) {
            return true;
        }

        if (otherObj instanceof RecordValueImpl) {
            RecordValueImpl other = (RecordValueImpl) otherObj;

            /* field-by-field comparison */
            if (size() == other.size() &&
                getDefinition().equals(other.getDefinition())) {

                for (int i = 0; i < size(); ++i) {

                    FieldValue val1 = get(i);
                    FieldValue val2 = other.get(i);
                    if (!val1.equals(val2)) {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        if (otherObj instanceof TupleValue) {
            TupleValue other = (TupleValue) otherObj;

            if (size() == other.size() &&
                getDefinition().equals(other.getDefinition())) {

                for (int i = 0; i < size(); ++i) {
                    FieldValue val1 = get(i);
                    FieldValue val2 = other.get(i);
                    if (!val1.equals(val2)) {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        return false;
    }

    @Override
    public int compareTo(FieldValue o) {
        throw new IllegalStateException(
            "TupleValue is not comparable to any other value");
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.RECORD;
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public RecordDefImpl getDefinition() {
        return theDef;
    }

    /*
     * Public api methods from RecordValue and RowImpl
     */

    @Override
    public int size() {
        return theTupleRegs.length;
    }

    public List<String> getFieldNames() {
        return getDefinition().getFieldNames();
    }

    public List<String> getFieldNamesInternal() {
        return getDefinition().getFieldNamesInternal();
    }

    public String getFieldName(int pos) {
        return getDefinition().getFieldName(pos);
    }

    public int getFieldPos(String fieldName) {
        return getDefinition().getFieldPos(fieldName);
    }

    public FieldValueImpl get(String fieldName) {
        int pos = getDefinition().getFieldPos(fieldName);
        return theRegisters[theTupleRegs[pos]];
    }

    public FieldValueImpl get(int pos) {
        return theRegisters[theTupleRegs[pos]];
    }

    @Override
    public FieldValueImpl getElement(int pos) {
        return theRegisters[theTupleRegs[pos]];
    }

    public void setTableAndIndex(
        TableImpl tbl,
        IndexImpl idx,
        boolean isIndexEntry) {

        theTable = tbl;
        theIndex = idx;
        theIsIndexEntry = isIndexEntry;
    }

    public TableImpl getTable() {
        return theTable;
    }

    public IndexImpl getIndex() {
        return theIndex;
    }

    public boolean isIndexEntry() {
        return theIsIndexEntry;
    }

    public void setExpirationTime(long t) {
        theExpirationTime = t;
    }

    public long getExpirationTime() {
        return theExpirationTime;
    }

    public void setCreationTime(long t) {
        theCreationTime = t;
    }

    public long getCreationTime() {
        return theCreationTime;
    }

    public void setModificationTime(long t) {
        theModificationTime = t;
    }

    public long getModificationTime() {
        return theModificationTime;
    }

    public void setPartition(int p) {
        thePartition = p;
    }

    public int getPartition() {
        return thePartition;
    }

    public void setStorageSize(int sz) {
        theStorageSize = sz;
    }

    public int getStorageSize() {
        return theStorageSize;
    }

    public void setIndexStorageSize(int sz) {
        theIndexStorageSize = sz;
    }

    public int getIndexStorageSize() {
        return theIndexStorageSize;
    }

    public void setVersion(Version v) {
        theRowVersion = v;
    }

    public Version getVersion() {
        return theRowVersion;
    }

    public void setRowMetadata(String rowMetadata) {
        this.rowMetadata = rowMetadata;
    }

    public String getRowMetadata() {
        return rowMetadata;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public boolean isTuple() {
        return true;
    }

    @Override
    public void toStringBuilder(StringBuilder sb,
                                DisplayFormatter formatter) {
        if (formatter == null) {
            throw new IllegalArgumentException(
                "DisplayFormatter must be non-null");
        }
        formatter.startObject();
        sb.append('{');
        for (int i = 0; i < theTupleRegs.length; ++i) {
            FieldValueImpl val = get(i);
            formatter.newPair(sb, (i > 0));
            sb.append('\"');
            sb.append(getDefinition().getFieldName(i));
            sb.append('\"');
            formatter.separator(sb);

            /*
             * The field value may be null if "this" is the value of a
             * variable referenced in a filtering pred over a secondary index.
             */
            if (val == null) {
                sb.append("java-null");
            } else {
                val.toStringBuilder(sb, formatter);
            }
        }
        formatter.endObject(sb, theTupleRegs.length);
        sb.append('}');
    }

    /*
     * Local methods
     */

    public int getNumFields() {
        return theTupleRegs.length;
    }

    FieldDefImpl getFieldDef(String fieldName) {
        return getDefinition().getFieldDef(fieldName);
    }

    FieldDefImpl getFieldDef(int pos) {
        return getDefinition().getFieldDef(pos);
    }

    void putFieldValue(int fieldPos, FieldValueImpl value) {
        theRegisters[theTupleRegs[fieldPos]] = value;
    }

    public RecordValueImpl toRecord() {

        RecordValueImpl rec = theDef.createRecord();

        for (int i = 0; i < size(); ++i) {

            if (get(i) == null) {
                throw new NullPointerException(
                    "TupleValue has null value for field " + getFieldName(i));
            }

            rec.put(i, get(i));
        }

        return rec;
    }

    public RowImpl toRow() {

        RowImpl rec = theTable.createRow();

        for (int i = 0; i < size(); ++i) {

            if (get(i) == null) {
                throw new NullPointerException(
                    "TupleValue has null value for field " + getFieldName(i));
            }

            rec.put(i, get(i));
        }

        return rec;
    }

    public void toTuple(RecordValueImpl rec, boolean doNullOnEmpty) {

        assert(theDef.equals(rec.getDefinition()));

        if (doNullOnEmpty) {
            for (int i = 0; i < size(); ++i) {
                FieldValueImpl val = rec.get(i);
                theRegisters[theTupleRegs[i]] =
                    (val.isEMPTY() ? NullValueImpl.getInstance() : val);
            }
        } else {
            for (int i = 0; i < size(); ++i) {
                theRegisters[theTupleRegs[i]] = rec.get(i);
            }
        }
    }
}
