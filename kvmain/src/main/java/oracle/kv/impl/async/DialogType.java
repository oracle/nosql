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

package oracle.kv.impl.async;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The dialog type of an asynchronous service interface.
 *
 * <p>A dialog type is represent by an integer dialog type ID.  The low 100
 * values represent the dialog type family -- a collection of dialog types that
 * have the same methods and serialization, but represent different instances
 * of the same interface.  Values above 100 are used to represent instances of
 * a given family.  The value 0 is reserved for the single service registry
 * dialog type, which needs to be well known so that both clients and servers
 * can use the same dialog type.  Note that we do not typically expect to use
 * multiple instances of the same dialog type family in the same Java VM since
 * we currently only deploy a single instance of each service type in each
 * JVM.  Support for multiple instances is provided because it is used in
 * testing, and to provide flexibility in the future.
 *
 * <p>Note that different service instances need to have different dialog type
 * numbers to prevent an old service endpoint from referring to the wrong
 * object -- See [KVSTORE-2383] for details. Use the ServiceResourceId and port
 * to generate a unique type number.
 */
public class DialogType implements FastExternalizable {

    /**
     * The maximum number of dialog type families.
     */
    public static final int MAX_TYPE_FAMILIES = 100;

    /**
     * Maps dialog type family IDs to dialog type families.
     */
    private static final Map<Integer, DialogTypeFamily> familyMap =
        Collections.synchronizedMap(new HashMap<Integer, DialogTypeFamily>());

    private final int dialogTypeId;
    private final DialogTypeFamily dialogTypeFamily;

    /**
     * Creates an instance with the specified dialog type ID.
     *
     * @param dialogTypeId the dialog type ID
     * @throws IllegalArgumentException if the ID is negative or does not
     * specify a known dialog type family
     */
    public DialogType(int dialogTypeId) {
        if (dialogTypeId < 0) {
            throw new IllegalArgumentException(
                "DialogTypeId must not be less than 0");
        }
        this.dialogTypeId = dialogTypeId;
        final int familyId = dialogTypeId % MAX_TYPE_FAMILIES;
        final DialogTypeFamily family;
        synchronized (familyMap) {
            family = familyMap.get(familyId);
        }
        if (family == null) {
            throw new IllegalArgumentException(
                "Dialog type family not found for dialogTypeId=" +
                dialogTypeId + " familyId=" + familyId);
        }
        assert family.getFamilyId() == familyId;
        dialogTypeFamily = family;
    }

    /**
     * Creates an instance with a new dialog type ID that is associated with
     * the specified dialog type family.
     *
     * @param dialogTypeFamily the dialog type family
     * @param typeNumber the dialog type number
     * @throws IllegalArgumentException if the family has not been registered
     * in a call to {@link #registerTypeFamily}, or if typeNumber is less than
     * 0 or greater than Integer.MAX_VALUE/MAX_TYPE_FAMILIES - 1
     */
    public DialogType(DialogTypeFamily dialogTypeFamily, int typeNumber) {
        this.dialogTypeFamily =
            checkNull("dialogTypeFamily", dialogTypeFamily);
        final int familyId = dialogTypeFamily.getFamilyId();
        if (!familyMap.containsKey(familyId)) {
            throw new IllegalArgumentException(
                "Dialog type family was not registered: " + dialogTypeFamily);
        }
        if ((typeNumber < 0) ||
            (typeNumber >= (Integer.MAX_VALUE / MAX_TYPE_FAMILIES))) {
            throw new IllegalArgumentException(
                "Dialog type number out of range: " + typeNumber);
        }
        dialogTypeId = (typeNumber * MAX_TYPE_FAMILIES) + familyId;
    }

    /** Constructor to create the bootstrap service registry dialog type. */
    DialogType(int dialogTypeId, DialogTypeFamily dialogTypeFamily) {
        if (dialogTypeId < 0) {
            throw new IllegalArgumentException(
                "DialogTypeId must not be less than 0");
        }
        this.dialogTypeId = dialogTypeId;
        this.dialogTypeFamily =
            checkNull("dialogTypeFamily", dialogTypeFamily);
    }

    /**
     * Registers a new dialog type family
     *
     * @param dialogTypeFamily the dialog type family
     * @throws IllegalArgumentException if there is an existing dialog type
     * family with the same family ID, or if the family ID is illegal
     */
    public static void registerTypeFamily(DialogTypeFamily dialogTypeFamily) {
        checkNull("dialogTypeFamily", dialogTypeFamily);
        final Integer familyId = dialogTypeFamily.getFamilyId();
        if ((familyId < 0) || (familyId >= MAX_TYPE_FAMILIES)) {
            throw new IllegalArgumentException(
                "Illegal dialog type family ID: " + familyId);
        }
        synchronized (familyMap) {
            if (familyMap.containsKey(familyId)) {
                throw new IllegalArgumentException(
                    "Existing entry for " + familyId + ": " +
                    familyMap.get(familyId));
            }
            familyMap.put(familyId, dialogTypeFamily);
        }
    }

    /**
     * Returns the dialog type ID of this dialog type.
     *
     * @return the dialog type ID
     */
    public int getDialogTypeId() {
        return dialogTypeId;
    }

    /**
     * Returns the dialog type family of this dialog type.
     *
     * @return the dialog type family of this dialog type
     */
    public DialogTypeFamily getDialogTypeFamily() {
        return dialogTypeFamily;
    }

    /**
     * Writes a dialog type to the output stream.  Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      <i>intValue</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writePackedInt(out, dialogTypeId);
    }

    /**
     * Reads an instance from the input stream.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialized form
     * @return the dialog type
     * @throws IOException if an I/O error occurs or if the format of the input
     * data is invalid
     */
    public static DialogType readFastExternal(DataInput in,
                                              short serialVersion)
        throws IOException {

        return new DialogType(readPackedInt(in));
    }

    @Override
    public String toString() {
        return String.format("%s(%d)",
                             dialogTypeFamily.getFamilyName(),
                             dialogTypeId);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DialogType)) {
            return false;
        }
        final DialogType other = (DialogType) o;
        return dialogTypeId == other.dialogTypeId;
    }

    @Override
    public int hashCode() {
        return dialogTypeId;
    }
}
