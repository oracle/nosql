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

package oracle.kv.shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellException;

class CommandUtils {

    /**
     * Create a Key from a URI-formatted string
     */
    static Key createKeyFromURI(String uri) {
        return Key.fromString(uri);
    }

    /**
     * Create a URI from a Key
     */
    static String createURI(Key key) {
        return key.toString();
    }

    /**
     * Translate the specified Base64 string into a byte array.
     * @throws ShellException
     */
    static String encodeBase64(byte[] buf)
        throws ShellException {
        try {
            return TableJsonUtils.encodeBase64(buf);
        } catch (IllegalArgumentException iae) {
            throw new ShellException("Get Exception in Base64 encoding:" + iae);
        }
    }

    /**
     * Decode the specified Base64 string into a byte array.
     * @throws ShellException
     */
    static byte[] decodeBase64(String str)
        throws ShellException {
        try {
            return TableJsonUtils.decodeBase64(str);
        } catch (IllegalArgumentException iae) {
            throw new ShellException("Get Exception in Base64 decoding:" + iae);
        }
    }

    abstract static class RunTableAPIOperation {
        abstract void doOperation() throws ShellException;

        void run() throws ShellException {
            try {
                doOperation();
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            } catch (IllegalCommandException ice) {
                throw new ShellException(ice.getMessage(), ice);
            } catch (DurabilityException de) {
                throw new ShellException(de.getMessage(), de);
            } catch (RequestTimeoutException rte) {
                throw new ShellException(rte.getMessage(), rte);
            } catch (KVSecurityException kvse) {
                throw new ShellException(kvse.getMessage(), kvse);
            } catch (FaultException fe) {
                throw new ShellException(fe.getMessage(), fe);
            }
        }
    }

    static Table findTable(TableAPI tableImpl,
                           String namespace,
                           String tableName)
        throws ShellException {

        Table table = tableImpl.getTable(namespace, tableName);
        if (table != null) {
            return table;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Table not found: ").append(tableName);
        if (namespace != null) {
            sb.append(" in namespace: ").append(namespace);
        }
        throw new ShellException(sb.toString());
    }

    static Index findIndex(Table table, String indexName)
        throws ShellException {

        Index index = table.getIndex(indexName);
        if (index != null) {
            return index;
        }
        throw new ShellException("Index does not exist: " + indexName +
                                 " on table: " + table.getFullName());
    }

    static boolean validateIndexField(String fieldName, Index index)
        throws ShellException {

        for (String fname: index.getFields()) {
            if (fieldName.equals(fname)) {
                return true;
            }
        }
        throw new ShellException
            ("Invalid index field: " + fieldName +
             " for index: " + index.getName() +
             " on table: " + index.getTable().getFullName());
    }

    static void putIndexKeyValues(RecordValue key,
                                  String fieldName,
                                  String sValue)
        throws ShellException {

        if (!(key.isIndexKey() || key.isPrimaryKey())) {
            throw new IllegalArgumentException
                ("Invalid record type passed to putIndexKeyValues");
        }

        try {
            if (sValue == null) {
                key.putNull(fieldName);
            } else {
                FieldDef def = key.getDefinition().getFieldDef(fieldName);
                if (def == null) {
                    throw new ShellException("Not a valid " +
                        (key.isIndexKey() ? "index " : "primary ") +
                        "key field: " + fieldName);
                }
                FieldValue fdValue = createFieldValue(def, sValue);
                key.put(fieldName, fdValue);
            }
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
    }

    static RecordValue createKeyFromJson(Table table,
                                         String indexName,
                                         String jsonString)
        throws ShellException {

        try {
            RecordValue key = null;
            if (indexName == null) {
                key = table.createPrimaryKeyFromJson(jsonString, false);
            } else {
                key = findIndex(table, indexName).
                    createIndexKeyFromJson(jsonString, false);
            }
            return key;
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
    }

    static FieldValue createFieldValue(FieldDef def, String sValue,
                                       boolean isFile)
        throws ShellException {

        try {
            FieldValue fdVal = null;
            switch (def.getType()) {
            case INTEGER:
                try {
                    fdVal = def.createInteger(Integer.parseInt(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellArgumentException(
                        "Invalid integer value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case LONG:
                try {
                    fdVal = def.createLong(Long.parseLong(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellArgumentException(
                        "Invalid long value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case DOUBLE:
                try {
                    fdVal = def.createDouble(Double.parseDouble(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellArgumentException(
                        "Invalid double value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case FLOAT:
                try {
                    fdVal = def.createFloat(Float.parseFloat(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellArgumentException(
                        "Invalid float value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case NUMBER:
                try {
                    fdVal = def.createNumber(new BigDecimal(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellException("Invalid decimal value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case STRING:
                try {
                    fdVal = def.createString(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case ENUM:
                try {
                    fdVal = def.createEnum(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case BOOLEAN:
                try {
                    fdVal = def.createBoolean(Boolean.parseBoolean(sValue));
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case TIMESTAMP:
                try {
                    fdVal = def.asTimestamp().fromString(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case FIXED_BINARY:
            case BINARY:
                byte[] data = null;
                if (isFile) {
                    data = readFromFile(sValue);
                } else {
                    data = decodeBase64(sValue);
                }
                try {
                    if (def.getType() == Type.BINARY) {
                        fdVal = def.createBinary(data);
                    } else {
                        fdVal = def.createFixedBinary(data);
                    }
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case JSON:
                try {
                    fdVal = FieldValueFactory.createValueFromJson(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case ARRAY:
                try {
                    fdVal = def.createArray();
                    FieldDef elementDef = getFieldDef(fdVal, null);
                    FieldValue fv = createFieldValue(elementDef, sValue);
                    fdVal.asArray().add(fv);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case MAP:

                /*
                 * If the field is a map the string value may be either a simple
                 * string key (key-only) or a key.value pair.
                 */
                try {
                    fdVal = def.createMap();
                    FieldDef elementDef = getFieldDef(fdVal, null);
                    TablePath path = new TablePath(null, sValue);
                    if (path.numSteps() == 1) {
                        throw new IllegalArgumentException(
                            "Invalid map key/value: " + sValue);
                    }
                    /*
                     * Grab the map key from the first field in the
                     * components and use the remainder of the components
                     * to create a name used to create the field value to
                     * be used as the map element's value.
                     */
                    String mapKey = path.remove(0);
                    String val = path.getPathName();
                    FieldValue fv = createFieldValue(elementDef, val);
                    fdVal.asMap().put(mapKey, fv);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            case RECORD:

                /*
                 * If the field is a record the string value must be a key.value
                 * pair valid for the record.
                 */
                try {
                    fdVal = def.createRecord();
                    TablePath path = new TablePath(null, sValue);
                    if (path.numSteps() == 1) {
                        throw new IllegalArgumentException
                            ("Illegal path to value in record: " + sValue);
                    }
                    String fieldName = path.remove(0);
                    FieldDef elementDef = getFieldDef(fdVal, fieldName);
                    String val = path.getPathName();
                    FieldValue fv = createFieldValue(elementDef, val);
                    fdVal.asRecord().put(fieldName, fv);
                } catch (IllegalArgumentException iae) {
                    throw new ShellArgumentException(iae.getMessage());
                }
                break;
            default:
                throw new ShellArgumentException("Can't create a field value " +
                    "for " + def.getType() + " field from a string: " + sValue);
            }
            return fdVal;
        } catch (IllegalArgumentException iae) {
            throw new ShellArgumentException(iae.getMessage());
        }
    }

    private static FieldValue createFieldValue(FieldDef def, String sValue)
        throws ShellException {

        return createFieldValue(def, sValue, false);
    }

    static MultiRowOptions createMultiRowOptions(TableAPI tableImpl,
                                                 Table table,
                                                 RecordValue key,
                                                 List<String> ancestors,
                                                 List<String> children,
                                                 String frFieldName,
                                                 String rgStart,
                                                 String rgEnd)
        throws ShellException {

        FieldRange fr = null;
        List<Table> lstAncestor = null;
        List<Table> lstChild = null;
        if (frFieldName != null) {
            fr = createFieldRange(table, key, frFieldName, rgStart, rgEnd);
        }

        if (ancestors != null && !ancestors.isEmpty()) {
            lstAncestor = new ArrayList<Table>(ancestors.size());
            final String targetNs =
                NameUtils.switchToInternalUse(table.getNamespace());
            for (String tname : ancestors) {
                final String ancestorNs =
                    NameUtils.getNamespaceFromQualifiedName(tname);
                /**
                 * If ancestor table namespace is specified explicitly, check
                 * whether it matches the target table namespace.
                 */
                if (ancestorNs != null && !ancestorNs.isEmpty() &&
                    !NameUtils.namespaceEquals(targetNs,
                        NameUtils.switchToInternalUse(ancestorNs))) {
                    throw new ShellException("The ancestor table namespace" +
                        " does not match target table namespace.");
                }
                final String ancestorTableName =
                    NameUtils.getFullNameFromQualifiedName(tname);
                lstAncestor.add(findTable(tableImpl,
                                          targetNs,
                                          ancestorTableName));
            }
        }
        if (children != null && !children.isEmpty()) {
            lstChild = new ArrayList<Table>(children.size());
            final String targetNs =
                NameUtils.switchToInternalUse(table.getNamespace());
            for (String tname : children) {
                final String childNs =
                    NameUtils.getNamespaceFromQualifiedName(tname);
                /**
                 * If child table namespace is specified explicitly, check
                 * whether it matches the target table namespace.
                 */
                if (childNs != null && !childNs.isEmpty() &&
                    !NameUtils.namespaceEquals(targetNs,
                        NameUtils.switchToInternalUse(childNs))) {
                    throw new ShellException("The child table namespace" +
                        " does not match target table namespace.");
                }
                final String childTable =
                    NameUtils.getFullNameFromQualifiedName(tname);
                lstChild.add(findTable(tableImpl,
                                       targetNs,
                                       childTable));
            }
        }
        if (fr != null || lstAncestor != null || lstChild != null) {
            return new MultiRowOptions(fr, lstAncestor, lstChild);
        }
        return null;
    }

    static boolean matchFullMajorKey(PrimaryKey key) {
        Table table = key.getTable();
        for (String fieldName: table.getShardKey()) {
            if (key.get(fieldName) == null) {
                return false;
            }
        }
        return true;
    }

    static boolean matchFullPrimaryKey(PrimaryKey key) {
        Table table = key.getTable();
        for (String fieldName: table.getPrimaryKey()) {
            if (key.get(fieldName) == null) {
                return false;
            }
        }
        return true;
    }

    private static FieldRange createFieldRange(Table table, RecordValue key,
                                               String fieldName, String start,
                                               String end)
        throws ShellException{

        try {
            FieldRange range = null;
            if (key.isPrimaryKey()) {
                range = table.createFieldRange(fieldName);
            } else {
                range = ((IndexKey)key).getIndex().createFieldRange(fieldName);
            }
            if (start != null) {
                range.setStart(createFieldValue(getRangeFieldDef(range), start),
                               true);
            }
            if (end != null) {
                range.setEnd(createFieldValue(getRangeFieldDef(range), end),
                             true);
            }
            return range;
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
    }

    private static FieldDef getRangeFieldDef(FieldRange range) {
        if (range.getDefinition().isArray()) {
            return ((ArrayDef)range.getDefinition()).getElement();
        }
        return range.getDefinition();
    }

    static FieldDef getFieldDef(FieldValue fieldValue, String fieldName)
        throws ShellException {

        try {
            if (fieldValue.isRecord()) {
                FieldDef def = fieldValue.asRecord().getDefinition()
                    .getFieldDef(fieldName);
                if (def == null) {
                    throw new ShellException("No such field: " + fieldName);
                }
                return def;
            } else if (fieldValue.isArray()) {
                return fieldValue.asArray().getDefinition().getElement();
            } else if (fieldValue.isMap()) {
                return fieldValue.asMap().getDefinition().getElement();
            }
            return null;
        } catch (ClassCastException cce) {
            throw new ShellException(cce.getMessage(), cce);
        }
    }

    static byte[] readFromFile(String fileName)
        throws ShellException {

        File file = new File(fileName);
        int len = (int)file.length();
        if (len == 0) {
            throw new ShellException(
                                     "Input file not found or empty: " + fileName);
        }
        byte[] buffer = new byte[len];

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            fis.read(buffer);
        } catch (IOException ioe) {
            throw new ShellException("Read file error: " + fileName, ioe);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignored) {
                }
            }
        }
        return buffer;
    }
}
