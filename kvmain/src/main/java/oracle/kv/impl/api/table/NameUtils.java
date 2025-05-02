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

import oracle.kv.table.TableAPI;

/**
 * Contains various functions related to names and namespaces.
 *
 * A qualified name can have 4 parts: [tenant@][namespace:][parent.]name
 * A full name is: [parent.]name.<br/>
 *
 * <b>Note</b>: SYSDEFAULT namespace is the namespace of tables created without
 * an explicit namespace or before namespaces were implemented. This namespace
 * is represented internally as Java null or as tenant@ if tenant is not null.
 */
public class NameUtils {

    /* Namespaces with this prefix are reserved */
    public static final String SYSTEM_NAMESPACE_PREFIX = "sys";

    /*
     * String separator used to generate a globally unique name for a table.
     */
    public static final char CHILD_SEPARATOR = '.';
    public static final char NAMESPACE_SEPARATOR = ':';

    /**
     * Creates a qualified fullName from namespace and full name.
     * Format is: [namespace:]fullName
     */
    public static String makeQualifiedName(String namespace, String fullName) {
        if (namespace == null || namespace.isEmpty()) {
            return fullName;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(namespace).append(NAMESPACE_SEPARATOR).append(fullName);
        return sb.toString();
    }

    /**
     * Create a string that uniquely identifies a table for use in error
     * messages.  Format is [namespace:][parentName.]name
     */
    public static String makeQualifiedName(String namespace, String name,
        String parentName) {

        final StringBuilder sb = new StringBuilder();
        if (namespace != null && !namespace.isEmpty()) {
            sb.append(namespace);
        }

        if (parentName != null && !parentName.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(NAMESPACE_SEPARATOR);
            }
            sb.append(parentName);
            if (name != null && !name.isEmpty()) {
                sb.append(CHILD_SEPARATOR).append(name);
            }
            return sb.toString();
        }

        if (name != null && !name.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(NAMESPACE_SEPARATOR);
            }
            sb.append(name);
        }
        return sb.toString();
    }

    /**
     * Extracts full name (i.e. [parent_name.]name) from a namespace qualified
     * name: [tenant@][namespace:][parent.]name.
     */
    public static String getFullNameFromQualifiedName(String qualifiedName) {
        if (qualifiedName == null) {
            return null;
        }
        final int nsSeparator = qualifiedName.indexOf(NAMESPACE_SEPARATOR);
        if (nsSeparator < 0) {
            return qualifiedName;
        }
        return qualifiedName.substring(nsSeparator + 1);
    }

    /**
     * Extracts internal namespace part from a qualified name.
     */
    public static String getNamespaceFromQualifiedName(String qualifiedName) {
        if (qualifiedName == null) {
            return null;
        }
        final int nsSeparator = qualifiedName.indexOf(NAMESPACE_SEPARATOR);
        if (nsSeparator < 0) {
            return null;
        }
        return qualifiedName.substring(0, nsSeparator);
    }

    /**
     * Returns true if internal namespace is the SYSDEFAULT one. Checks if it is
     * null or only tenant@ if there is a tenant part.
     */
    public static boolean isInternalInitialNamespace(String namespace) {
        if( namespace == null) {
            return true;
        }
        return false;
    }


    /**
     * Switches to the internal representation of namespaces.
     * The "sysdefault" namespace is represented internally as null - for
     * compatibility reasons, while externally it is presented as
     * {@link TableAPI#SYSDEFAULT_NAMESPACE_NAME} */
    public static String switchToInternalUse(String namespace) {
        if (TableAPI.SYSDEFAULT_NAMESPACE_NAME.equalsIgnoreCase(namespace)) {
            return null;
        }
        return namespace;
    }

    /**
     * Switches to the external representation of namespaces.
     * The "sysdefault" namespace is represented internally as null - for
     * compatibility reasons, while externally it is presented as
     * {@link TableAPI#SYSDEFAULT_NAMESPACE_NAME} */
    public static String switchToExternalUse(String namespace) {
        if (namespace == null) {
            return TableAPI.SYSDEFAULT_NAMESPACE_NAME;
        }
        return namespace;
    }

    public static boolean namespaceEquals(String ns1, String ns2) {
        if (ns1 == null) {
            return ns2 == null;
        }
        return ns1.equalsIgnoreCase(ns2);
    }
}
