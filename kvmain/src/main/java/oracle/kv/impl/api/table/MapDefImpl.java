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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.table.FieldDef;
import oracle.kv.table.MapDef;

/**
 * MapDefImpl implements the MapDef interface.
 */
public class MapDefImpl extends FieldDefImpl implements MapDef {

    private static final long serialVersionUID = 1L;

    private final FieldDefImpl element;

    MapDefImpl(FieldDefImpl element, String description) {

        super(FieldDef.Type.MAP, description);
        if (element == null) {
            throw new IllegalArgumentException
                ("Map has no field and cannot be built");
        }
        this.element = element;
    }

   MapDefImpl(FieldDefImpl element) {
       this(element, null);
   }

    private MapDefImpl(MapDefImpl impl) {
        super(impl);
        element = impl.element.clone();
    }

    /**
     * Constructor for FastExternalizable
     */
    MapDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.MAP);
        element = FieldDefImpl.readFastExternal(in, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link FieldDefImpl}) {@code element}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        element.writeFastExternal(out, serialVersion);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public MapDefImpl clone() {

        if (this == FieldDefImpl.Constants.mapAnyDef ||
            this == FieldDefImpl.Constants.mapJsonDef) {
            return this;
        }

        return new MapDefImpl(this);
    }

    @Override
    public int hashCode() {
        return element.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof MapDefImpl) {
            return element.equals(((MapDefImpl)other).getElement());
        }
        return false;
    }

    @Override
    public MapDef asMap() {
        return this;
    }

    @Override
    public MapValueImpl createMap() {
        return new MapValueImpl(this);
    }

    /*
     * Public api methods from MapDef
     */

    @Override
    public FieldDefImpl getElement() {
        return element;
    }

    @Override
    public FieldDef getKeyDefinition() {
        return FieldDefImpl.Constants.stringDef;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return element.isPrecise();
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (this == superType) {
            return true;
        }

        if (superType.isMap()) {
            MapDefImpl supMap = (MapDefImpl)superType;
            return element.isSubtype(supMap.element);
        }

        if (superType.isJson()) {
            return element.isSubtype(Constants.jsonDef);
        }

        if (superType.isAny()) {
             return true;
        }

        return false;
    }

    /**
     * If called for a map the fieldName applies to the key that is being
     * indexed in the map, so the target is the map's element.
     */
    @Override
    FieldDefImpl findField(String fieldName) {
        return element;
    }

    @Override
    public short getRequiredSerialVersion() {
        return element.getRequiredSerialVersion();
    }

    @Override
    int countTypes() {
        return element.countTypes() + 1; /* +1 for this field */
    }
}
