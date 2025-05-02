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

import oracle.kv.impl.query.compiler.Expr.ExprIter;

/**
 * ExprWalker traverses the expression tree in a depth-first fashion, calling
 * the enter() and exit() methods of a given visitor on each node of the tree.
 */
class ExprWalker {

    ExprVisitor theVisitor;
    boolean theAllocateChildrenIter;

    ExprWalker(ExprVisitor visitor, boolean allocateChildrenIter) {
        theVisitor = visitor;
        theAllocateChildrenIter = allocateChildrenIter;
    }

    void walk(Expr e) {

        switch (e.getKind()) {

        case CONST:
            theVisitor.enter((ExprConst)e);
            theVisitor.exit((ExprConst)e);
            break;

       case BASE_TABLE:
            if (theVisitor.enter((ExprBaseTable)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprBaseTable)e);
            break;

        case FUNC_CALL:
            if (theVisitor.enter((ExprFuncCall)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprFuncCall)e);
            break;

        case ARRAY_CONSTR:
            if (theVisitor.enter((ExprArrayConstr)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprArrayConstr)e);
            break;

        case MAP_CONSTR:
            if (theVisitor.enter((ExprMapConstr)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprMapConstr)e);
            break;

        case REC_CONSTR:
            if (theVisitor.enter((ExprRecConstr)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprRecConstr)e);
            break;

        case IN:
            if (theVisitor.enter((ExprInOp)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprInOp)e);
            break;

        case PROMOTE:
            if (theVisitor.enter((ExprPromote)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprPromote)e);
            break;

        case IS_OF_TYPE:
            if (theVisitor.enter((ExprIsOfType)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprIsOfType)e);
            break;

        case CAST:
            if (theVisitor.enter((ExprCast)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprCast)e);
            break;

        case FIELD_STEP:
            if (theVisitor.enter((ExprFieldStep)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprFieldStep)e);
            break;

        case MAP_FILTER:
            if (theVisitor.enter((ExprMapFilter)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprMapFilter)e);
            break;

        case ARRAY_FILTER:
            if (theVisitor.enter((ExprArrayFilter)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprArrayFilter)e);
            break;

        case ARRAY_SLICE:
            if (theVisitor.enter((ExprArraySlice)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprArraySlice)e);
            break;

        case SEQ_MAP:
            if (theVisitor.enter((ExprSeqMap)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprSeqMap)e);
            break;

        case CASE:
            if (theVisitor.enter((ExprCase)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprCase)e);
            break;

        case VAR:
            theVisitor.enter((ExprVar)e);
            theVisitor.exit((ExprVar)e);
            break;

        case SFW:
            if (theVisitor.enter((ExprSFW)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprSFW)e);
            break;

        case JOIN:
            if (theVisitor.enter((ExprJoin)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprJoin)e);
            break;

        case RECEIVE:
            if (theVisitor.enter((ExprReceive)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprReceive)e);
            break;

        case SORT:
            if (theVisitor.enter((ExprSort)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprSort)e);
            break;

        case GROUP:
            if (theVisitor.enter((ExprGroup)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprGroup)e);
            break;

        case INSERT_ROW:
            if (theVisitor.enter((ExprInsertRow)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprInsertRow)e);
            break;

        case DELETE_ROW:
            if (theVisitor.enter((ExprDeleteRow)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprDeleteRow)e);
            break;

        case UPDATE_ROW:
            if (theVisitor.enter((ExprUpdateRow)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprUpdateRow)e);
            break;

        case UPDATE_FIELD:
            if (theVisitor.enter((ExprUpdateField)e)) {
                walkChildren(e);
            }
            theVisitor.exit((ExprUpdateField)e);
            break;
        }
    }

    void walkChildren(Expr e) {

        ExprIter children = 
            (theAllocateChildrenIter ? e.getChildrenIter() : e.getChildren());

        while (children.hasNext()) {
            Expr child = children.next();
            walk(child);
        }
        children.reset();
    }
}
