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

package oracle.kv.table;

import java.math.BigDecimal;

/**
 * NumberValue extends {@link FieldValue} to represent a BigDecimal.
 *
 * @since 4.4
 */
public interface NumberValue extends FieldValue {

    /**
     * Get the BigDecimal value of this object.
     *
     * @return the BigDecimal value of this object
     */
    BigDecimal get();

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public NumberValue clone();
}
