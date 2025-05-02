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

/**
 * AnyDef is the most generic data type; it contains all kinds of values.
 * AnyDef cannot be used when defining the schema of a table (i.e., in the
 * CREATE TABLE statement). Instead, it is used to describe the result of
 * a query expression, when the query processor cannot infer a more precise
 * type for the expression.
 *
 * @since 4.0
 */
public interface AnyDef extends FieldDef {
}
