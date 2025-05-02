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

package com.sleepycat.je;

/**
 * Specifies the attributes of database cursor.  An instance created with the
 * default constructor is initialized with the system's default settings.
 */
public class CursorConfig implements Cloneable {

    /**
     * Default configuration used if null is passed to methods that create a
     * cursor.
     */
    public static final CursorConfig DEFAULT = new CursorConfig();

    /**
     * A convenience instance to configure read operations performed by the
     * cursor to return modified but not yet committed data.
     */
    public static final CursorConfig READ_UNCOMMITTED =
        new CursorConfig().setReadUncommitted(true);

    private boolean readUncommitted = false;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public CursorConfig() {
    }

    /**
     * Configures read operations performed by the cursor to return modified
     * but not yet committed data.
     *
     * @param readUncommitted If true, configure read operations performed by
     * the cursor to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     *
     * @return this
     */
    public CursorConfig setReadUncommitted(boolean readUncommitted) {
        this.readUncommitted = readUncommitted;
        return this;
    }

    /**
     * Returns true if read operations performed by the cursor are configured
     * to return modified but not yet committed data.
     *
     * @return true if read operations performed by the cursor are configured
     * to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     */
    public boolean getReadUncommitted() {
        return readUncommitted;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public CursorConfig clone() {
        try {
            return (CursorConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "readUncommitted=" + readUncommitted;
    }
}
