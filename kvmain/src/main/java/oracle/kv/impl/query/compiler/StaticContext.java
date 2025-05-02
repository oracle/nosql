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


import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;

/**
 * StaticContext stores various pieces of info needed during compilation.
 * The reason why we use StaticContext, rather than the QueryControlBlock,
 * is that the relevan info may be scoped. So, multiple StaticContext
 * instances may be created during compilation, each defining a nested
 * scope.
 */
public class StaticContext {

    private final StaticContext theParent;

    private HashMap<String, Function> theFunctions;

    private HashMap<String, ExprVar> theVariables;


    StaticContext(StaticContext parent) {
        theParent = parent;
    }

    boolean isRoot() {
        return theParent == null;
    }

    /*
     * VarInfo is used to store info about an external var. This info is
     * collected, for all external vars, after compilation is done and is
     * then stored in the PreparedStatement. The collected info is needed
     * during query execution, and by extracting it from the ExprVars that
     * represent the external vars during compilation and storing it into
     * the PreparedStatement we can throw away the expressions graph and
     * the static context after query compilation is done.
     */
    public static class VarInfo {

        private final int theId;

        private final ExprType theType;

        private final boolean theAllowJsonNull;

        VarInfo(ExprVar var) {
            theId = var.getVarId();
            theType = var.getDeclaredType();
            theAllowJsonNull = var.allowJsonNull();
        }

        private VarInfo(
            int id,
            ExprType type,
            boolean allowJsonNull) {
            theId = id;
            theType = type;
            theAllowJsonNull = allowJsonNull;
        }

        /* for use by deserialization -- see PreparedStatementImpl */
        public static VarInfo createVarInfo(
            int id,
            ExprType type,
            boolean allowJsonNull) {
            return new VarInfo(id, type, allowJsonNull);
        }

        public int getId() {
            return theId;
        }

        public ExprType getType() {
            return theType;
        }

        public boolean allowJsonNull() {
            return theAllowJsonNull;
        }
    }

    void addVariable(ExprVar var) {
        addVariable(var.getName(), var);
    }

    void addVariable(String varName, ExprVar var) {

        if (theVariables == null) {
            theVariables = new HashMap<String, ExprVar>();
        }

        if (theVariables.put(varName, var) != null) {
            throw new QueryException(
                "Duplicate declaration for variable " + varName,
                var.getLocation());
        }
    }

    void removeVariable(ExprVar var) {
        theVariables.remove(var.getName());
    }

    ExprVar findVariable(String name) {
        return findVariable(name, false);
    }

    ExprVar findVariable(String name, boolean local) {

        if (local) {
            if (theVariables == null) {
                return null;
            }
            ExprVar var = theVariables.get(name);
            return var;
        }

        ExprVar var = null;
        StaticContext sctx = this;

        while (var == null && sctx != null) {
           if (sctx.theVariables != null) {
               var = sctx.theVariables.get(name);
           }
            sctx = sctx.theParent;
        }

        return var;
    }

    public Map<String, VarInfo> getExternalVars() {

        Map<String, VarInfo> varsMap = new HashMap<String, VarInfo>();

        if (theVariables == null) {
            return varsMap;
        }

        for (Map.Entry<String, ExprVar> entry : theVariables.entrySet()) {
            String varName = entry.getKey();
            ExprVar var = entry.getValue();
            if (var.isExternal()) {
                varsMap.put(varName, new VarInfo(var));
            }
        }

        return varsMap;
    }

    void addFunction(Function func) {

        if (theFunctions == null) {
            theFunctions = new HashMap<String, Function>();
        }

        if (theFunctions.put(func.getName(), func) != null) {
            throw new QueryException(
                "Duplicate declaration for function " + func.getName(), null);
        }
    }

    Function findFunction(String name) {
        return findFunction(name, -1, false);
    }

    Function findFunction(String name, int arity) {
        return findFunction(name, arity, false);
    }

    Function findFunction(String name, int arity, boolean local) {

        Function func = null;

        if (local) {
            if (theFunctions == null) {
                return null;
            }

            func = theFunctions.get(name);

        } else {
            StaticContext sctx = this;

            while (func == null && sctx != null) {
                if (sctx.theFunctions != null) {
                    func = sctx.theFunctions.get(name);
                }
                sctx = sctx.theParent;
            }
        }

        if (func != null &&
            !func.isVariadic() &&
            arity >= 0 &&
            arity != func.getArity()) {
            return null;
        }

        return func;
    }
}
