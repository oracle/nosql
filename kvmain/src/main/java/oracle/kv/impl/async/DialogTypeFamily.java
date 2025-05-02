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

/**
 * The family of dialog types for an asynchronous service interface.  Instances
 * need to be registered with {@link DialogType#registerTypeFamily}.
 */
public interface DialogTypeFamily {

    /**
     * Returns the integer value of this dialog type family, which must not be
     * negative, and must be less than {@link DialogType#MAX_TYPE_FAMILIES}.
     *
     * @return the integer value of this dialog type family
     */
    int getFamilyId();

    /**
     * Returns the name of the family, not including the ID.
     *
     * @return the family name
     */
    String getFamilyName();
}
