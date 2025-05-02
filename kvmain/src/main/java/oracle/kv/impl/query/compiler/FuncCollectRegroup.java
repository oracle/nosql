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

package oracle.kv.impl.query.compiler;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

class FuncCollectRegroup extends FuncCollect {

    FuncCollectRegroup(boolean distinct) {
        super(distinct);
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {

        FieldDefImpl inType = caller.getInput().getType().getDef();
        assert(inType.isArray());
        return TypeManager.createType(inType, Quantifier.ONE);
    }
}
