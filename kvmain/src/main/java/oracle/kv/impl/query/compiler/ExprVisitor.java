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


/*
 *
 */
class ExprVisitor {

    @SuppressWarnings("unused")
    boolean enter(ExprConst e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprConst e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprBaseTable e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprBaseTable e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprFuncCall e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprFuncCall e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprArrayConstr e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprArrayConstr e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprMapConstr e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprMapConstr e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprRecConstr e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprRecConstr e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprInOp e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprInOp e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprPromote e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprPromote e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprIsOfType e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprIsOfType e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprCast e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprCast e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprFieldStep e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprFieldStep e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprMapFilter e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprMapFilter e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprArrayFilter e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprArrayFilter e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprArraySlice e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprArraySlice e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprSeqMap e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprSeqMap e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprCase e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprCase e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprVar e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprVar e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprSFW e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprSFW e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprJoin e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprJoin e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprReceive e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprReceive e) {
    }

    @SuppressWarnings("unused")

    boolean enter(ExprSort e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprSort e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprGroup e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprGroup e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprInsertRow e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprInsertRow e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprDeleteRow e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprDeleteRow e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprUpdateRow e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprUpdateRow e) {
    }

    @SuppressWarnings("unused")
    boolean enter(ExprUpdateField e) {
        return true;
    }

    @SuppressWarnings("unused")
    void exit(ExprUpdateField e) {
    }
}
