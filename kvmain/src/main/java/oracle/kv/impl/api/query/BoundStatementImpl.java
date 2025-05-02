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

package oracle.kv.impl.api.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.query.compiler.StaticContext.VarInfo;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;

/**
 * Implementation of BoundStatement
 */
public class BoundStatementImpl
    implements BoundStatement,
               InternalStatement {

    private final PreparedStatementImpl preparedStatement;

    private final Map<String, FieldValue> bindVariables;

    BoundStatementImpl(PreparedStatementImpl preparedStatement) {
        this.preparedStatement = preparedStatement;
        bindVariables = new HashMap<String, FieldValue>();
    }

    PreparedStatementImpl getPreparedStmt() {
        return preparedStatement;
    }

    @Override
    public String toString() {
        return preparedStatement.toString();
    }

    @Override
    public RecordDef getResultDef() {
        return preparedStatement.getResultDef();
    }

    @Override
    public Map<String, FieldDef> getVariableTypes() {
        return preparedStatement.getExternalVarsTypes();
    }

    @Override
    public FieldDef getVariableType(String name) {
        return preparedStatement.getVariableType(name);
    }

    @Override
    public Map<String, FieldValue> getVariables() {
        return bindVariables;
    }

    @Override
    public BoundStatement createBoundStatement() {
        return preparedStatement.createBoundStatement();
    }

    @Override
    public BoundStatement setVariable(String name, FieldValue value) {
        validate(name, value);
        bindVariables.put(name, value);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, FieldValue value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, int value) {
        FieldValue val =
            FieldDefImpl.Constants.integerDef.createInteger(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, int value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, boolean value) {
        FieldValue val =
            FieldDefImpl.Constants.booleanDef.createBoolean(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, boolean value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, double value) {
        FieldValue val = FieldDefImpl.Constants.doubleDef.createDouble(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, double value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, float value) {
        FieldValue val = FieldDefImpl.Constants.floatDef.createFloat(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, float value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, long value) {
        FieldValue val = FieldDefImpl.Constants.longDef.createLong(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, long value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, String value) {
        FieldValue val = FieldDefImpl.Constants.stringDef.createString(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, String value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    @Override
    public BoundStatement setVariable(String name, byte[] value) {
        FieldValue val = FieldDefImpl.Constants.binaryDef.createBinary(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(int pos, byte[] value) {
        String name = preparedStatement.varPosToName(pos);
        return setVariable(name, value);
    }

    private void validate(String varName, FieldValue value) {

        if (value.isNull()) {
            return;
        }

        VarInfo vi = preparedStatement.getExternalVarInfo(varName);

        if (vi == null) {
            throw new IllegalArgumentException(
                "Variable " + varName + " has not been declared in the query");
        }

        if (value.isJsonNull() && vi.allowJsonNull()) {
            return;
        }

        FieldDefImpl def = vi.getType().getDef();
        FieldDefImpl valdef = (FieldDefImpl)value.getDefinition();
        if (!valdef.isSubtype(def)) {
            throw new IllegalArgumentException(
                "Variable " + varName + " does not have an expected type. " +
                "Expected " + def.getDDLString() + " or subtype, got " +
                valdef.getDDLString());
        }

        checkRecordsContainAllFields(varName, value);
    }

    /*
     * Check if record values have all the fields defined in the type.
     */
    private void checkRecordsContainAllFields(
        String varName,
        FieldValue value) {

        if (value.isNull() ) {
            return;
        }

        FieldDef def = value.getDefinition();

        if (def.isRecord()) {

            RecordValueImpl rec = (RecordValueImpl)value.asRecord();

            /*
             * The various RecordValue.put() methods forbid adding a field
             * whose name or type does not comform to the record def.
             */
            for (int i = 0; i < rec.getNumFields(); ++i) {

                FieldValue fval = rec.get(i);

                if (fval == null) {
                    String fname = rec.getFieldName(i);
                    throw new IllegalArgumentException(
                        "Value for variable " + varName +
                            " not conforming to type definition: there is no" +
                            " value for field: '" + fname + "'.");
                }

                checkRecordsContainAllFields(varName, fval);
            }
        } else if (def.isArray()) {
            for (FieldValue v : value.asArray().toList()) {
                checkRecordsContainAllFields(varName, v);
            }
        } else if (def.isMap()) {
            for (FieldValue v :
                ((MapValueImpl)value.asMap()).getFieldsInternal().values()) {
                checkRecordsContainAllFields(varName, v);
            }
        }
    }

    @Override
    public StatementResult executeSync(
        KVStoreImpl store,
        ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this);
    }

    @Override
    public StatementResult executeSyncShards(
        KVStoreImpl store,
        ExecuteOptions options,
        Set<RepGroupId> shards) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this,
            null /* QueryPublisher */, null, shards);
    }
}
