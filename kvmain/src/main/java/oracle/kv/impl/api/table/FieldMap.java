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

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.FieldDef;

/**
 * FieldMap represents a RECORD type definition. RecordDefImpl stores an
 * instance of FieldMap, which does the bulk of the implementation work.
 * However, FieldMap is also used in TableImpl as well as the builder
 * utilities.
 * <p>
 * fieldProperties:
 * An ArrayList of FieldMapEntry instances, one for each field declared by the
 * RECORD type. The list preserves the order of field declaration with the
 * RECORD type. Each FieldMapEntry stores the properties of a field, like its
 * name, type, nullability, and default value.
 * <p>
 * fieldOrder:
 * An ArrayList storing the names of the fields defined by this FieldMap.
 * Although the field names are stored in the FieldMapEntries, storing them
 * separately in this list as well avoids the creation of a new list in the
 * getFieldNames() method. So, this field acts as a cache.
 * <p>
 * fieldPositions:
 * A TreeMap&lt;String, Integer&gt;, mapping field names to the position of the
 * associated fields within the RECORD declaration. Since fieldProperties
 * maintains the declaration order, these positions are used as indexes
 * inside the fieldProperties array. As a result, fieldPositions is used
 * to provide fast access to the field properties by field name.
 * <p>
 * Note: fieldPositions does case-insensitive comparisons of the stored field
 * names, in order to implement the semantics of case-insensitive field names.
 * <p>
 * Note: FieldMap is persistent but the comparator is not saved with the
 * object. This is not a problem because in all cases a new FieldMap is
 * constructed from the raw Map when the deserialized FieldMap is used so
 * order in that case does not matter.
 * <p>
 * Note: this.fields and this.fieldOrder represent the previous implementation
 * of FieldMap (pre 4.2). When a FieldMap undergoes java-based serialization
 * (for example when table metadata is sent from a server to client) we have
 * to serialize in this old format, because we cannot know if the receiver
 * is pre or post 4.2. As a result, we have to keep these old fields around
 * and re-define the writeObject and readObject methods to always write and
 * read the old format. Specifically: (a) All the new fields are transient so
 * that they don't participate in the default java serialization. (b) The
 * writeObject method populates the old fields from the new ones, calls
 * defaultWriteObject, and then clears out the old fields, and (c) The
 * readObject method calls defaultReadObject to populate the old fields,
 * then populates the new fields from the old ones, and finally clears out
 * the old fields.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class FieldMap implements Cloneable, Serializable, FastExternalizable {

    private static final long serialVersionUID = 1L;

    /*
     * This is only used for serialization compatibility. The map is normally
     * empty.
     */
    private final Map<String, FieldMapEntry> fields;

    /* List of field names (name is a bit misleading) */
    private final List<String> fieldOrder;

    private transient List<FieldMapEntry> fieldProperties;

    private transient Map<String, Integer> fieldPositions;

    public FieldMap() {
        fieldOrder = new ArrayList<>();
        fieldProperties = new ArrayList<>();
        fieldPositions = new TreeMap<>(FieldComparator.instance);

        fields = new TreeMap<>(FieldComparator.instance);
    }

    private FieldMap(FieldMap other) {
        assert(other.fieldPositions != null);
        assert(other.fieldProperties != null);

        int numFields = other.fieldOrder.size();

        /* Do a deep copy of the fieldProperties list */
        fieldProperties = new ArrayList<>(numFields);

        for (FieldMapEntry fme : other.fieldProperties) {
            fieldProperties.add(fme.clone());
        }

        /* Copy the fieldPositions map */
        fieldPositions = new TreeMap<>(FieldComparator.instance);
        fieldPositions.putAll(other.fieldPositions);

        /* Copy of the fieldOrder list */
        fieldOrder = new ArrayList<>(other.fieldOrder);

        fields = new TreeMap<>(FieldComparator.instance);
    }

    /**
     * Constructor for FastExternalizable
     */
    FieldMap(DataInput in, short serialVersion) throws IOException {
        final int nNames = readNonNullSequenceLength(in);

        /*
         * Populate fieldOrder.
         */
        fieldOrder = new ArrayList<>(nNames);
        for (int i = 0; i < nNames; i++) {
            fieldOrder.add(i, readNonNullString(in, serialVersion));
        }
        fields = new TreeMap<>(FieldComparator.instance);

        /* Populate field map for use by populateTransient. */
        for (int i = 0; i < nNames; i++) {

            final FieldMapEntry fme = new FieldMapEntry(in, serialVersion);
            fields.put(fme.getFieldName(), fme);
        }
        populateTransient();
        assert fields.isEmpty();
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@code fieldOrder} <i>length</i>
     * <li> For each element of {@code fieldOrder}:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeString
     *         non-null String}) <i>field name</i>
     *    </ol>
     * <li> For each element of {@code fieldProperties}:
     *    <ol type="a">
     *    <li> ({@code FieldMapEntry}) <i>field map entry</i>
     *    </ol>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        assert fieldOrder.size() == fieldProperties.size();

        writeNonNullSequenceLength(out, fieldOrder.size());
        for (String name : fieldOrder) {
           writeNonNullString(out, serialVersion, name);
        }
        for (final FieldMapEntry fme : fieldProperties) {
            fme.writeFastExternal(out, serialVersion);
        }
    }

    @Override
    public FieldMap clone() {
        return new FieldMap(this);
    }

    /*
     * Multiple threads may try to serialize the same FieldMap at the same
     * time (for example there is a shared MetaData object at a server, which
     * is requested by multiple clients at the same time). Because the method
     * modifies the FieldMap object, it has to be synchronized.
     */
    private synchronized void writeObject(java.io.ObjectOutputStream out)
        throws IOException {

        /* Populate fields map from the transient collections. */
        fields.clear();
        final int numFields = fieldProperties.size();
        for (int i = 0; i < numFields; ++i) {
            FieldMapEntry fme = fieldProperties.get(i);
            fields.put(fme.getFieldName(), fme);
        }
        out.defaultWriteObject();
        fields.clear();
    }

    /*
     * Override readObject() to handle deserialization of a binary FieldMap
     * created by an older-version server that used the old FieldMap impl.
     */
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        /* Convert old format to new format */
        populateTransient();
    }

    /*
     * Populates the transient collections from the fields map. The map
     * is cleared upon return.
     */
    private void populateTransient() {

        fieldProperties = new ArrayList<>(fieldOrder.size());

        fieldPositions = new TreeMap<>(FieldComparator.instance);

        for (String fname : fieldOrder) {

            FieldMapEntry oldFME = fields.get(fname);

            FieldValueImpl defVal = oldFME.getDefaultValueInternal();

            FieldMapEntry newFME = new FieldMapEntry(fname,
                                                     oldFME.getFieldDef(),
                                                     oldFME.isNullable(),
                                                     defVal);

            fieldProperties.add(newFME);
            fieldPositions.put(fname, fieldProperties.size() - 1);
        }

        fields.clear();
    }

    Map<String, Integer> getFieldPositions() {
        return fieldPositions;
    }

    List<FieldMapEntry> getFieldProperties() {
        return fieldProperties;
    }

    List<String> getFieldNames() {
        return fieldOrder;
    }

    FieldMapEntry getFieldMapEntry(String name) {
        Integer pos = fieldPositions.get(name);
        return (pos != null ? fieldProperties.get(pos) : null);
    }

    public FieldMapEntry getFieldMapEntry(int pos) {
        return fieldProperties.get(pos);
    }

    public String getFieldName(int pos) {
        return fieldOrder.get(pos);
    }

    boolean exists(String name) {
        return fieldPositions.containsKey(name);
    }

    int getFieldPos(String name) {
        Integer pos = fieldPositions.get(name);
        if (pos == null) {
            throw new IllegalArgumentException(
                "There is no field with name " + name);
        }
        return pos.intValue();
    }

    public FieldDefImpl getFieldDef(int pos) {
        return fieldProperties.get(pos).getFieldDef();
    }

    FieldDefImpl getFieldDef(String name) {
        FieldMapEntry fme = getFieldMapEntry(name);
        return (fme != null ? fme.getFieldDef() :  null);
    }

    FieldValueImpl getDefaultValue(int pos) {
        return fieldProperties.get(pos).getDefaultValue();
    }

    boolean isNullable(int pos) {
        return fieldProperties.get(pos).isNullable();
    }

    /**
     * Utility method used by the query translator.
     */
    public void reverseFieldOrder() {
        Collections.reverse(fieldOrder);
        Collections.reverse(fieldProperties);
        for (int i = 0; i < fieldProperties.size(); ++i) {
            fieldPositions.put(fieldProperties.get(i).getFieldName(), i);
        }
    }

    void put(FieldMapEntry fme) {

        if (fieldPositions.put(fme.getFieldName(),
                               fieldPositions.size()) != null) {
            throw new IllegalArgumentException(
              "Field " + fme.getFieldName() + " exists already");
        }

        fieldProperties.add(fme);
        fieldOrder.add(fme.getFieldName());
    }

    public void put(
        String name,
        FieldDefImpl type,
        boolean nullable,
        FieldValueImpl defaultValue) {

        put(new FieldMapEntry(name, type, nullable, defaultValue));
    }

    boolean remove(String name) {

        Integer pos = fieldPositions.remove(name);
        if (pos == null) {
            return false;
        }

        FieldMapEntry fme = fieldProperties.remove(pos.intValue());
        assert(fme != null);

        fieldOrder.remove(pos.intValue());

        /*
         * Adjust the positions stored in fieldPositions: subtract 1 from all
         * positions after "pos".
         */
        for (Map.Entry<String, Integer> entry : fieldPositions.entrySet()) {
            String fname = entry.getKey();
            Integer fpos = entry.getValue();
            if (fpos > pos) {
                fieldPositions.put(fname, fpos - 1);
            }
        }

        return true;
    }

    public boolean isEmpty() {
        return fieldProperties.isEmpty();
    }

    public int size() {
        return fieldProperties.size();
    }

    /**
     * Compare equality.  Field names are case-insensitive, so ignore case.
     * Order of declaration does not matter for equality.
     */
    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof FieldMap)) {
            return false;
        }

        FieldMap other = (FieldMap) obj;

        if (fieldProperties.size() != other.fieldProperties.size()) {
            return false;
        }

        for (int i = 0; i < fieldProperties.size(); ++i) {

            FieldMapEntry fme1 = fieldProperties.get(i);
            FieldMapEntry fme2 = other.fieldProperties.get(i);
            if (fme1.equals(fme2)) {
                continue;
            }
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FieldMap[");
        for (FieldMapEntry fme : fieldProperties) {
            sb.append(fme).append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    public boolean isPrecise() {

        for (FieldMapEntry fme : fieldProperties) {
            if (!fme.isPrecise()) {
                return false;
            }
        }
        return true;
    }

    public boolean isSubtype(FieldMap superType) {

        if (fieldProperties.size() != superType.fieldProperties.size()) {
            return false;
        }

        for (int i = 0; i < fieldProperties.size(); ++i) {

            FieldMapEntry fme1 = fieldProperties.get(i);
            FieldMapEntry fme2 = superType.fieldProperties.get(i);
            if (fme1.isSubtype(fme2)) {
                continue;
            }
            return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        int code = fieldProperties.size();
        for (FieldMapEntry fme : fieldProperties) {
            code += fme.hashCode();
        }
        return code;
    }

    FieldMapEntry getFieldMapEntry(TablePath tablePath) {
        FieldMap containingMap = findContainingMap(tablePath);
        String lastStep = tablePath.getLastStep();
        return containingMap.getFieldMapEntry(lastStep);
    }

    /*
     * Find the FieldMap containing the entry for a record field declared in
     * a table or record schema. The field to lookup may be deeply nested
     * inside the type hierarchy, so it is specified as a path consisting of
     * field-name and [] steps.
     *
     * @throws IllegalArgumentException for param values that don't work in
     * the current state
     */
    public FieldMap findContainingMap(TablePath tablePath) {

        int numSteps = tablePath.numSteps();

        if (numSteps == 0) {
            throw new IllegalArgumentException(" Field path is empty.");
        }

        if (tablePath.getLastStep() == TableImpl.BRACKETS ||
            tablePath.getLastStep().equalsIgnoreCase(TableImpl.VALUES)) {
            throw new IllegalArgumentException(
                "Path " + tablePath.getPathName() +
                " does not lead to a record field");
        }

        String currentStep = tablePath.getStep(0);
        FieldDef def = getFieldDef(currentStep);
        if (def == null) {
            throw new IllegalArgumentException(
                "Field \"" + currentStep + "\" in path " +
                currentStep + " does not exist.");
        }

        if (numSteps == 1) {
            return this;
        }

        int stepIndex = 1;

        while (stepIndex < numSteps - 1) {

            currentStep = tablePath.getStep(stepIndex);
            ++stepIndex;

            if (currentStep == TableImpl.BRACKETS) {
                if (def.isArray()) {
                    def = def.asArray().getElement();
                    assert def != null;
                } else {
                    throw new IllegalArgumentException(
                        "Step \"[]\" in path " + tablePath.getPathName() +
                        " does not have an array as input");
                }
            } else if (currentStep.equalsIgnoreCase(TableImpl.VALUES)) {
                if (def.isMap()) {
                    def = def.asMap().getElement();
                    assert def != null;
                } else {
                    throw new IllegalArgumentException(
                        "Step \".values()\" in path " + tablePath.getPathName() +
                        " does not have a map as input");
                }
            } else {
                // def must be a record type
                if (def.isRecord()) {
                    def = def.asRecord().getFieldDef(currentStep);

                    if (def == null) {
                        throw new IllegalArgumentException(
                            "Field \"" + currentStep + "\" in path " +
                             tablePath.getPathName() + " does not exist.");
                    }
                    //} else if (def.isArray() || def.isMap()) {
                    // TODO: allow for the [] steps to be optional.
                } else if (def.isAnyRecord()) {
                    throw new IllegalStateException(
                        "AnyRecord cannot be used in table schema.");
                } else {
                    throw new IllegalArgumentException(
                        "Step \"" + currentStep + "\" in path " +
                        tablePath.getPathName() +
                        " does not have a record as input");
                }
            }
        }

        currentStep = tablePath.getStep(stepIndex);

        if (!def.isRecord()) {
            throw new IllegalArgumentException(
                "Step \"" + currentStep + "\" in path " +
                tablePath.getPathName() +
                " does not have a record as input");
        }

        if (def.asRecord().getFieldDef(currentStep) == null) {
            throw new IllegalArgumentException(
                "Field \"" + currentStep + "\" in path " +
                tablePath.getPathName() + " does not exist.");
        }

        return ((RecordDefImpl)def).getFieldMap();
    }

    /**
     * Remove a field from the schema of a table or record. The field to
     * remove may be deeply nested inside the type hierarchy, so it is
     * specified as a path consisting of field-name and [] steps.
     *
     *  @throws IllegalArgumentException for param values that don't work in
     *  the current state
     */
    public void removeField(List<String> stepsList) {
        TablePath tablePath = new TablePath(this, stepsList);
        removeField(tablePath);
    }

    void removeField(TablePath tablePath) {
        FieldMap containingMap = findContainingMap(tablePath);
        containingMap.remove(tablePath.getLastStep());
    }

    /**
     * Returns the minimum version of the server that can execute this
     * fieldMap. This is the maximum serialVersion of the fields
     * contained in this FieldMap.
     */
    public short getRequiredSerialVersion() {
        short requiredSerialVersion = SerialVersion.MINIMUM;
        for (int i = size() - 1; i >= 0; i--) {
            requiredSerialVersion = (short)Math.max(
                requiredSerialVersion,
                getFieldDef(i).getRequiredSerialVersion());
        }
        return requiredSerialVersion;
    }
}
