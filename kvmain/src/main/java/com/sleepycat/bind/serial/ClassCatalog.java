/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.bind.serial;

import java.io.Closeable;
import java.io.ObjectStreamClass;

import com.sleepycat.je.DatabaseException;

/**
 * A catalog of class description information for use during object
 * serialization.
 *
 * <p>A catalog is used to store class descriptions separately from serialized
 * objects, to avoid redundantly stored information with each object.
 * When serialized objects are stored in a database, a {@link
 * StoredClassCatalog} should be used.</p>
 *
 * <p>This information is used for serialization of class descriptors or
 * java.io.ObjectStreamClass objects, each of which represents a unique class
 * format.  For each unique format, a unique class ID is assigned by the
 * catalog.  The class ID can then be used in the serialization stream in place
 * of the full class information.  When used with {@link SerialInput} and
 * {@link SerialOutput} or any of the serial bindings, the use of the catalog
 * is transparent to the application.</p>
 *
 * @see <a href="SerialBinding.html#evolution">Class Evolution</a>
 *
 * @author Mark Hayes
 */
public interface ClassCatalog
    /* <!-- begin JE only --> */
    extends Closeable
    /* <!-- end JE only --> */
    {

    /**
     * Close a catalog database and release any cached resources.
     *
     * @throws DatabaseException if an error occurs closing the catalog
     * database.
     */
    public void close()
        throws DatabaseException;

    /**
     * Return the class ID for the current version of the given class
     * description.
     * This is used for storing in serialization streams in place of a full
     * class descriptor, since it is much more compact.  To get back the
     * ObjectStreamClass for a class ID, call {@link #getClassFormat(byte[])}.
     * This function causes a new class ID to be assigned if the class
     * description has changed.
     *
     * @param classDesc The class description for which to return the
     * class ID.
     *
     * @return The class ID for the current version of the class.
     *
     * @throws DatabaseException if an error occurs accessing the catalog
     * database.
     *
     * @throws ClassNotFoundException if the class does not exist.
     */
    public byte[] getClassID(ObjectStreamClass classDesc)
        throws DatabaseException, ClassNotFoundException;

    /**
     * Return the ObjectStreamClass for the given class ID.  This may or may
     * not be the current class format, depending on whether the class has
     * changed since the class ID was generated.
     *
     * @param classID The class ID for which to return the class format.
     *
     * @return The class format for the given class ID, which may or may not
     * represent the current version of the class.
     *
     * @throws DatabaseException if an error occurs accessing the catalog
     * database.
     *
     * @throws ClassNotFoundException if the class does not exist.
     */
    public ObjectStreamClass getClassFormat(byte[] classID)
        throws DatabaseException, ClassNotFoundException;

    /**
     * Returns the ClassLoader to be used by bindings that use this catalog, or
     * null if a default class loader should be used. The ClassLoader is used
     * by {@link SerialBinding} to load classes whose description is stored in
     * the catalog.
     *
     * <p>In BDB JE, the implementation of this method in {@link
     * StoredClassCatalog} returns the ClassLoader property of the catalog
     * database Environment.  This ensures that the Environment's ClassLoader
     * property is used for loading all user-supplied classes.</p>
     *
     * @return the ClassLoader or null.
     */
    public ClassLoader getClassLoader();
}
