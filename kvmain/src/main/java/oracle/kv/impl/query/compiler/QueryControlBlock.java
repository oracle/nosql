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

import java.util.ArrayList;
import java.util.HashSet;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.parser.KVParser;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;

/**
 * The query control block.
 *
 * theTableMetaHelper:
 *
 * theInitSctx:
 * The top-level static context for the query (for now, there is actually no
 * nesting of static contexts (no need for scoping within a query)), so
 * theInitSctx is the only sctx obj used by the query.
 *
 * theException:
 *
 * generatedNames:
 * A set of names generated internally for use in otherwise unnamed maps and
 * arrays that need them (for Avro schema generation). This set guarantees
 * uniqueness, which is also required by Avro.
 */
public class QueryControlBlock {

    private final KVStoreImpl theStore;

    private final ExecuteOptions theOptions;

    private final char[] theQueryString;

    private final TableMetadataHelper theTableMetaHelper;

    private final StatementFactory theStatementFactory;

    private final StaticContext theInitSctx;

    private final String theNamespace;

    private final PrepareCallback thePrepareCallback;

    private boolean theStrictMode;

    private int theInternalVarCounter = 0;

    private Translator theTranslator;

    private CodeGenerator theCodegen;

    private FieldDefImpl theResultDef;

    private String theWrapRecordFieldName;

    private Expr theRootExpr;

    private PlanIter theRootPlanIter;

    private boolean theHasReceiveIter;

    private RuntimeException theException = null;

    private final HashSet<String> generatedNames = new HashSet<String>();

    PartitionId thePartitionId;

    DistributionKind theDistributionKind;

    ArrayList<TableImpl> theTables;

    boolean theHaveJsonConstructors;

    boolean theHasSort;

    boolean theIsFunctionalIndexPath;

    public QueryControlBlock(
        TableAPIImpl tableAPI,
        ExecuteOptions options,
        char[] queryString,
        StaticContext sctx,
        String namespace,
        PrepareCallback prepareCallback) {

        theStore = (tableAPI != null ?
                    tableAPI.getStore() :
                    null);

        theOptions = options;
        theQueryString = queryString;
        theTableMetaHelper =
            (tableAPI != null ?
             tableAPI.getTableMetadataHelper() :
             (prepareCallback != null ?
              prepareCallback.getMetadataHelper() :
              null));
        theInitSctx = sctx;
        theStatementFactory = null;
        theNamespace = namespace;
        thePrepareCallback = prepareCallback;
    }

    public QueryControlBlock(
        TableMetadataHelper metadataHelper,
        StatementFactory statementFactory,
        char[] queryString,
        ExecuteOptions options,
        StaticContext sctx,
        String namespace,
        PrepareCallback prepareCallback) {

        theStore = null;
        theOptions = (options != null ? options : new ExecuteOptions());
        theQueryString = queryString;
        theTableMetaHelper = metadataHelper;
        theInitSctx = (sctx == null ?
                       new StaticContext(CompilerAPI.getRootSctx()) :
                       sctx);
        theStatementFactory = statementFactory;
        theNamespace = namespace;
        thePrepareCallback = prepareCallback;
    }

    public KVStoreImpl getStore() {
        return theStore;
    }

    ExecuteOptions getOptions() {
        return theOptions;
    }

    TableMetadataHelper getTableMetaHelper() {
        return theTableMetaHelper;
    }

    public StaticContext getInitSctx() {
        return theInitSctx;
    }

    public String getNamespace() {
        return theNamespace;
    }

    public PrepareCallback getPrepareCallback() {
        return thePrepareCallback;
    }

    boolean strictMode() {
        return theStrictMode;
    }

    StatementFactory getStatementFactory() {
        return theStatementFactory;
    }

    public RuntimeException getException() {
        return theException;
    }

    public boolean succeeded() {
        return theException == null;
    }

    public String getErrorMessage() {
        return theException == null ? "" :
            theException.getMessage();
    }

    public Expr getRootExpr() {
        return theRootExpr;
    }

    void setRootExpr(Expr e) {
        theRootExpr = e;
    }

    void setPartitionId(PartitionId p) {
        thePartitionId = p;
    }

    public PartitionId getPartitionId() {
        return thePartitionId;
    }

    public DistributionKind getDistributionKind() {
        return theDistributionKind;
    }

    public void setDistributionKind(DistributionKind kind) {
        theDistributionKind = kind;
    }

    void setTables(ArrayList<TableImpl> tables) {
        theTables = tables;
    }

    public long getRootTableId() {
        return theTables.get(0).getTopLevelTable().getId();
    }
 
    public int getFirstTableVersion() {
        return theTables.get(0).getTableVersion();
    }

    public long getJsonTableId() {
        if (theTables.size() == 1 && theTables.get(0).isJsonCollection()) {
            return theTables.get(0).getId();
        }
        return 0;
    }

    public TableImpl getJsonTable() {
        assert(theTables.size() == 1 && theTables.get(0).isJsonCollection()); 
        return theTables.get(0);
    }

    public FieldDefImpl getResultDef() {
        return theResultDef;
    }

    void setWrapResultInRecord(String name) {
        theWrapRecordFieldName = name;
    }

    public String getResultColumnName() {
        return theWrapRecordFieldName;
    }

    public char[] getQueryString() {
        return theQueryString;
    }

    void setHasSort(boolean value) {
        theHasSort = value;
    }

    public boolean hasSort() {
        return theHasSort;
    }

    public void setIsFunctionalIndexPath() {
        theIsFunctionalIndexPath = true;
    }

    /**
     * The caller is responsible for determining success or failure by
     * calling QueryControlBlock.succeeded(). On failure there may be
     * an exception which can be obtained using
     * QueryControlBlock.getException().
     */
    public void compile() {

        KVParser parser = new KVParser();
        parser.parse(theQueryString);

        if (!parser.succeeded()) {
            theException = parser.getParseException();
            return;
        }

        theTranslator = new Translator(this);
        theTranslator.translate(parser.getParseTree());
        theException = theTranslator.getException();

        if (theException != null) {
            return;
        }

        if (theTranslator.isQuery()) {
            theRootExpr = theTranslator.getRootExpr();
            FieldDefImpl resDef1 = theRootExpr.getType().getDef();

            if (!theIsFunctionalIndexPath) {
                OptRulePushIndexPreds rule = new OptRulePushIndexPreds();
                rule.apply(theRootExpr);
                theException = rule.getException();

                if (theException != null) {
                    return;
                }

                Distributer distributer = new Distributer(this);
                distributer.distributeQuery();
            }

            theCodegen = new CodeGenerator(this);
            theCodegen.generatePlan(theRootExpr);
            theException = theCodegen.getException();

            if (theException == null) {
                theRootPlanIter = theCodegen.getRootIter();

                /*
                 * The type of the root expr may be EMPTY, if the optimizer
                 * found out that the query will return nothing. In this case,
                 * use the result type after transalation, because the users
                 * expect a record type as the result type.
                 */
                FieldDefImpl resDef2 = theRootExpr.getType().getDef();

                if (theIsFunctionalIndexPath) {
                    theResultDef = null;
                } else if (theTables.size() == 1 &&
                    theTables.get(0).isJsonCollection()) {
                    if (!resDef2.isEmpty()) {
                        theResultDef = resDef2;
                    } else {
                        theResultDef = resDef1;
                    }
                } else if (resDef2.isRecord()) {
                    theResultDef = resDef2;
                } else if (!resDef2.isEmpty()) {
                    theException = new QueryStateException(
                        "Result does not have a record type");
                } else if (!resDef1.isRecord()) {
                    String fname = getResultColumnName();
                    if (fname == null) {
                        fname = "Column_1";
                    }

                    FieldMap fieldMap = new FieldMap();
                    fieldMap.put(fname,
                                 resDef1,
                                 theRootExpr.mayReturnNULL(),
                                 null/*defaultValue*/);

                    theResultDef = FieldDefFactory.createRecordDef(fieldMap, null);
                } else {
                    theResultDef = resDef1;
                }
            }
        }
    }

    void parse() {

        KVParser parser = new KVParser();
        parser.parse(theQueryString);

        if (!parser.succeeded()) {
            theException = parser.getParseException();
            return;
        }

        theTranslator = new Translator(this);
        theTranslator.translate(parser.getParseTree());
        theException = theTranslator.getException();

        if (theException != null) {
            return;
        }

        if (theTranslator.isQuery()) {
            theRootExpr = theTranslator.getRootExpr();

            OptRulePushIndexPreds rule = new OptRulePushIndexPreds();
            rule.apply(theRootExpr);
            theException = rule.getException();
        }

        return;
    }

    String createInternalVarName(String prefix) {

        if (prefix == null) {
            return "$internVar-" + theInternalVarCounter++;
        }

        return "$" + prefix + "-" + theInternalVarCounter++;
    }

    int createExternalVarId() {
        return theTranslator.createExternalVarId();
    }

    public void setCodegen(CodeGenerator v) {
        theCodegen = v;
    }

    public int getNumRegs() {
        return theCodegen.getNumRegs();
    }

    public int incNumPlanIters() {
        return theCodegen.incNumPlanIters();
    }

    public int getNumIterators() {
        return theCodegen.getNumIterators();
    }

    public PlanIter getQueryPlan() {
        return theRootPlanIter;
    }

    void setHasReceiveIter() {
        theHasReceiveIter = true;
    }

    public boolean hasReceiveIter() {
        return theHasReceiveIter;
    }

    public String displayExprTree() {
        return theRootExpr.display();
    }

    public String displayQueryPlan() {
        return theRootPlanIter.display();
    }

    /**
     * Use the generatedNames set to generate a unique name based on the
     * prefix, which is unique per-type (record, enum, binary).  Avro
     * requires generated names for some data types that otherwise do not
     * need them in the DDL.
     */
    String generateFieldName(String prefix) {
        final String gen = "_gen";
        int num = 0;
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(gen);
        String name = sb.toString();
        while (generatedNames.contains(name)) {
            sb.append(num++);
            name = sb.toString();
        }
        generatedNames.add(name);
        return name;
    }
}
