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

import static java.util.Locale.ENGLISH;
import static oracle.kv.table.TableAPI.SYSDEFAULT_NAMESPACE_NAME;
import static oracle.kv.impl.query.compiler.FuncExtractFromTimestamp.Unit;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.NullJsonValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.SequenceDefImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableBuilderBase;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ExprIter;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.Expr.UpdateKind;
import oracle.kv.impl.query.compiler.ExprMapFilter.FilterKind;
import oracle.kv.impl.query.compiler.ExprSFW.FromClause;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.parser.KVQLBaseListener;
import oracle.kv.impl.query.compiler.parser.KVQLParser;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Alter_defContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Es_propertiesContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Freeze_defContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Frozen_defContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Unfreeze_defContext;
import oracle.kv.impl.query.runtime.ArithUnaryOpIter;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PrepareCallback.QueryOperation;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Index;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

/**
 * This class works with the Abstract Syntax Tree (AST) generated from KVSQL.g
 * by Antlr V4. It implements a parse tree visitor (extends KVQLBaseListener)
 * to walk the AST and generate an expression tree (in case of a DML statement)
 * or a single DdlOperation (in case of a DDL statement).
 *
 * Antlr parses the entire DDL/DML statement into the parse tree before any of
 * the TableListener methods are called. This means that the implementation
 * can rely on state that is guaranteed by the successful parse.
 *
 * If there was an error in processing of the AST, the Translator instance will
 * return false from its succeeded() method. In this case, there is no useful
 * state in the Translator object other than the exception returned by
 * getException().
 *
 * Use of TableMetadata for DDL.
 *
 * It would be possible to implement the translation without
 * access to TableMetadata.  In fact, the current code does not need metadata
 * for create/drop index and drop table.  The rationale for requiring
 * TableMetadata for some operations is just that it simplifies interactions
 * with TableBuilder and related classes in the case of table evolution and
 * child table creation.  This connection could be changed to have the
 * TableMetadata accessed only by callers.
 *
 * If so, it'd have the following implications: o creation of child tables
 * would need to add, and validate parent primary key information after the
 * fact.  o alter table would have to save its individual modifications for
 * application after the parse
 *
 * This may be desirable, but for now, this class uses TableMetadata directly.
 *
 * Usage warnings:
 * The syntax error messages are currently fairly
 * cryptic. oracle.kv.shell.ExecuteCmd implements a getUsage() method which
 * attempts to augment those messages. This should be moved into TableDdl.
 */
public class Translator extends KVQLBaseListener {

    private final static String ALL_PRIVS = "ALL";
    public final static String SYS_PREFIX = "SYS$";

    /* The minimum cost to charge for a query. */
    private final static int MIN_QUERY_COST = 2;

    private final ParseTreeWalker theWalker = new ParseTreeWalker();

    private final TableMetadataHelper theMetadataHelper;

    /*
     * The query control block.
     */
    private final QueryControlBlock theQCB;

    /*
     * The library of build-in functions
     */
    private final FunctionLib theFuncLib;

    /*
     * The initial (root) static context of the query.
     */
    private final StaticContext theInitSctx;

    /*
     * The current static context.
     */
    private StaticContext theSctx;

    /*
     * A stack of static contexts to implement nested scopes.
     */
    private final Stack<StaticContext> theScopes = new Stack<StaticContext>();

    private boolean theAllowUndeclaredVars;

    /*
     * For storing the sub-exprs of each expr. Sometimes, the parent expr is
     * created and placed in the stack before its subexpr. Other times, all of
     * the subexprs are translated first and then replaced in the stack by
     * their parent expr.
     */
    private final Stack<Expr> theExprs = new Stack<Expr>();

    /*
     * theSFWExprs together with theInSelectClause are used to know whether
     * the translator is translating a SELECT clause. This is the case if
     * the sizes of the 2 stacks are the same. theInOrderByClause is used
     * the same way to check whether the translator is translating an
     * ORDER BY clause. These 2 checks are needed to make sure that aggregate
     * functions appear in the SELECT and ORDER BY clauses only.
     *
     * theInWhereClause is used the same way to check whether the translator
     * is translating a WHERE clause. This is needed to make sure that the
     * geo_near function appears in the WHERE clause only.
     */
    private final Stack<ExprSFW> theSFWExprs = new Stack<ExprSFW>();

    private final Stack<Boolean> theInFromClause = new Stack<Boolean>();

    private final Stack<Boolean> theInWhereClause = new Stack<Boolean>();

    private final Stack<Boolean> theInSelectClause = new Stack<Boolean>();

    private final Stack<Boolean> theInOrderByClause = new Stack<Boolean>();

    private boolean theInUpdateClause;

    private final Stack<Expr> theNearExprs = new Stack<Expr>();

    private final Stack<Expr> theSeqTransformExprs = new Stack<Expr>();

    /*
     * Used to make sure that aggregate functions are not nested.
     */
    private final Stack<Function> theAggrFunctions = new Stack<Function>();

    /*
     * For storing the column names in a select clause.
     */
    private final Stack<String> theColNames = new Stack<String>();

    /*
     * For storing the sort specs in an order by clause.
     */
    private final Stack<SortSpec> theSortSpecs = new Stack<SortSpec>();

    private final Stack<FieldDefImpl> theTypes = new Stack<FieldDefImpl>();

    private final Stack<Quantifier> theQuantifiers = new Stack<Quantifier>();

    private final Stack<FieldDefHelper> theFields = new Stack<FieldDefHelper>();

    /*
     * For collecting identity information of a table.
     */
    private Set<String> theSequenceOptions = null;

    private IdentityDefHelper theIdentityHelper = null;

    /*
     * Helper class to handle JSON fragments (used by full-text indexes)
     */
    private final JsonCollector jsonCollector = new JsonCollector();

    private final ArrayList<TableImpl> theTables = new ArrayList<TableImpl>();

    private final ArrayList<String> theTableAliases = new ArrayList<String>();

    /*
     * Counts the number of external variables in the query and serves to
     * assign a unique numeric id to each such var.
     */
    private int theExternalVarsCounter;

    private Expr theRootExpr = null;

    private RuntimeException theException;

    private TableBuilderBase theTableBuilder;

    /**
     * A flag that is set only inside DDL statements.
     */
    private boolean theInDDL = false;

    private final ArrayList<ExprVar> theQuestionVars = new ArrayList<ExprVar>();

    private int theNumQuestionVarsInSelect;

    /* Collect JSON MRCounter fields. */
    private Map<String, Type> jsonMRcounterFields;

    public Translator(QueryControlBlock qcb) {
        theQCB = qcb;
        theMetadataHelper = qcb.getTableMetaHelper();
        theInitSctx = qcb.getInitSctx();
        theSctx = theInitSctx;
        theFuncLib = CompilerAPI.getFuncLib();
        theScopes.push(theInitSctx);
    }

    Expr getRootExpr() {
        return theRootExpr;
    }

    /**
     * Returns the CompilerException if an exception occurred.
     */
    public RuntimeException getException() {
        return theException;
    }

    public void setException(RuntimeException de) {
        theException = de;
    }

    public boolean succeeded() {
        return theException == null;
    }

    public boolean isQuery() {
        return theRootExpr != null;
    }

    int createExternalVarId() {
        return theExternalVarsCounter++;
    }

    /*
     * Implementation of the translator.
     *
     * TODO: look at using the BailErrorStrategy and setSLL(true) to do faster
     * parsing with a bailout.
     */
    public void translate(ParseTree tree) {

        try {
            /*
             * Walks the parse tree, acting on the rules encountered.
             */
            theWalker.walk(this, tree);

            //if (theRootExpr != null) {
            //    System.out.println("Expr tree:\n" + theRootExpr.display());
            //}

        } catch (DdlException e) {
            /*
             * DdlException is used to notify the caller that this is a DDL
             * statement that should be sent to the server without any further
             * processing from the compiler
             */
            throw e;
        } catch (StopWalkException swe) {
            /* ignore and stop */
        } catch (RuntimeException e) {
            setException(e);
        }
    }

    void pushScope() {
        StaticContext sctx = new StaticContext(theScopes.peek());
        theScopes.push(sctx);
        theSctx = sctx;
    }

    void popScope() {
        theScopes.pop();
        theSctx = theScopes.peek();
    }

    /**
     * statement :
     *      (
     *      query
     *      | index_function_path
     *      | insert_statement
     *      | update_statement
     *      | delete_statement
     *      | create_table_statement
     *      | create_index_statement
     *      | create_user_statement
     *      | create_role_statement
     *      | create_namespace_statement
     *      | drop_index_statement
     *      | drop_namespace_statement
     *      | create_text_index_statement
     *      | drop_role_statement
     *      | drop_user_statement
     *      | alter_table_statement
     *      | alter_user_statement
     *      | drop_table_statement
     *      | grant_statement
     *      | revoke_statement
     *      | describe_statement
     *      | show_statement) ;
     */
    @Override
    public void enterStatement(KVQLParser.StatementContext ctx) {
        if (ctx.query() == null &&
            ctx.index_function_path() == null &&
            ctx.update_statement() == null &&
            ctx.insert_statement() == null &&
            ctx.delete_statement() == null) {
            theInDDL = true;
        }
    }

    @Override
    public void exitStatement(KVQLParser.StatementContext ctx) {
        theInDDL = false;
    }

    @Override
    public void exitIndex_function_path(
        KVQLParser.Index_function_pathContext ctx) {

        theRootExpr = theExprs.pop();

        assert(theRootExpr != null);
        assert(theExprs.isEmpty());
        assert(theColNames.isEmpty());
        assert(theTypes.isEmpty());
    }

    /*
     * query : prolog? sfw_expr ;
     *
     * prolog : DECLARE var_decl (var_decl)* SEMI;
     *
     * var_decl : var_name type_def;
     *
     * var_name : DOLLAR id;
     */
    @Override
    public void exitQuery(KVQLParser.QueryContext ctx) {

        theRootExpr = theExprs.pop();

        assert(theRootExpr != null);
        assert(theExprs.isEmpty());
        assert(theColNames.isEmpty());
        assert(theTypes.isEmpty());
    }

    @Override
    public void exitVar_decl(KVQLParser.Var_declContext ctx) {

        Location loc = getLocation(ctx);

        String varName = ctx.VARNAME().getText();

        FieldDefImpl varType = theTypes.pop();

        if (varName.equals(ExprVar.theElementVarName) ||
            varName.equals(ExprVar.theElementPosVarName) ||
            varName.equals(ExprVar.theKeyVarName) ||
            varName.equals(ExprVar.theValueVarName)) {
            throw new QueryException(
                varName +
                " cannot be used as the name of an external variable");
        }

        ExprVar varExpr = new ExprVar(theQCB, theInitSctx, loc,
                                      varName, varType,
                                      theExternalVarsCounter++,
                                      false);

        theInitSctx.addVariable(varExpr);
    }

    /*
     * sfw_expr : select_clause
     *            from_clause
     *            where_clause?
     *            groupby_clause?
     *            orderby_clause?
     *            limit_clause?
     *            offset_clause? ;
     */
    @Override
    public void enterSfw_expr(KVQLParser.Sfw_exprContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.SELECT);
        }

        Location loc = getLocation(ctx);

        ExprSFW sfw = new ExprSFW(theQCB, theInitSctx, loc);
        theExprs.push(sfw);
        theSFWExprs.push(sfw);

        theWalker.walk(this, ctx.from_clause());

        if (ctx.where_clause() != null) {
            theWalker.walk(this, ctx.where_clause());
        }

        if (ctx.groupby_clause() != null) {
            theWalker.walk(this, ctx.groupby_clause());
        }

        if (ctx.orderby_clause() != null) {
            theWalker.walk(this, ctx.orderby_clause());
        }

        if (ctx.limit_clause() != null) {
            theWalker.walk(this, ctx.limit_clause());
        }

        if (ctx.offset_clause() != null) {
            theWalker.walk(this, ctx.offset_clause());
        }

        theWalker.walk(this, ctx.select_clause());

        /*
         * The children of this Sfw_clauseContext node have been processed
         * already. To avoid reprocessing them, we remove all the children
         * here.
         */
        int numParseChildren = ctx.getChildCount();

        while (numParseChildren > 0) {
            ctx.removeLastChild();
            --numParseChildren;
        }
    }

    @Override
    public void exitSfw_expr(KVQLParser.Sfw_exprContext ctx) {

        ExprSFW sfw = (ExprSFW)theExprs.peek();

        theSFWExprs.pop();

        sfw.rewriteNearPred();

        Expr topExpr = sfw;

        if (sfw.hasJoin()) {
            sfw.rewriteJoin();
        }

        if (sfw.hasGroupBy()) {
            topExpr = sfw.rewriteGroupBy(false);
        }

        if (sfw.isSelectDistinct()) {
            topExpr = sfw.rewriteSelectDistinct(topExpr);
        }

        if (topExpr != sfw) {
            theExprs.pop();
            theExprs.push(topExpr);
        }

        /* If there are any '?' in select clause, adjust the ids and names
         * of the '?' variables. This is needed because the SELECT clause
         * is processed last, so the ids assigned to '?'s in the SELECT
         * clause are, at this point, higher than the ids of '?'s in the
         * other clauses. */
        if (theNumQuestionVarsInSelect > 0 &&
            theQuestionVars.size() > theNumQuestionVarsInSelect) {

            int numNonSelectVars = theQuestionVars.size() -
                                   theNumQuestionVarsInSelect;

            for (int i = 0; i < numNonSelectVars; ++i) {
                ExprVar var = theQuestionVars.get(i);
                theInitSctx.removeVariable(var);
                var.setVarId(var.getVarId() + theNumQuestionVarsInSelect);
                var.setName("$$" + var.getVarId());
            }

            for (int i = 0; i < theNumQuestionVarsInSelect; ++i) {
                ExprVar var = theQuestionVars.get(numNonSelectVars + i);
                theInitSctx.removeVariable(var);
                var.setVarId(var.getVarId() - numNonSelectVars);
                var.setName("$$" + var.getVarId());
            }

            for (ExprVar var : theQuestionVars) {
                theInitSctx.addVariable(var);
            }
        }
    }

    /*
     * from_clause :
     *   FROM table_spec (COMMA table_spec)*
     *        (COMMA ( (expr (AS? VARNAME)) | unnest_clause ) )*;
     *
     * table_spec : from_table | nested_tables | left_outer_join_tables ;
     *
     * nested_tables :
     *     NESTED TABLES
     *     LP
     *     from_table
     *     (ANCESTORS LP ancestor_tables RP) ?
     *     (DESCENDANTS LP descendant_tables RP) ?
     *     RP ;
     *
     * ancestor_tables : from_table (COMMA from_table)* ;
     *
     * descendant_tables : from_table (COMMA from_tsble)* ;
     *
     * left_outer_join_tables :
     *     from_table left_outer_join_table (left_outer_join_table)* ;
     *
     * left_outer_join_table : LEFT_OUTER_JOIN from_table ;
     *
     * from_table : aliased_table_name (ON or_expr)? ;
     *
     * aliased_table_name : (table_name | SYSTEM_TABLE_NAME) (AS? tab_alias)? ;
     *
     * table_name : (namespace ':' )? id_path ;
     *
     * tab_alias : DOLLAR? id ;
     *
     * This method is called explicitly from enterSfw_expr, and then it is
     * also called from the antlr tree walker. The 2nd invocation should be
     * a noop. This is done by removing all the children of the FROM parse
     * node at tha end of this method.
     */
    @Override
    public void enterFrom_clause(KVQLParser.From_clauseContext ctx) {

        int numParseChildren = ctx.getChildCount();

        pushScope();

        theInFromClause.push(true);

        ExprSFW sfw = (ExprSFW)theExprs.peek();

        List<KVQLParser.Table_specContext> tableSpecs = ctx.table_spec();

        boolean shouldStopWalk = false;
        for (KVQLParser.Table_specContext tableSpec : tableSpecs) {
            try {
                theWalker.walk(this, tableSpec);
            } catch (StopWalkException swe) {
                shouldStopWalk = true;
            }
        }
        if (shouldStopWalk) {
            throw new StopWalkException();
        }

        List<KVQLParser.ExprContext> exprCtxs =
            new ArrayList<KVQLParser.ExprContext>(numParseChildren);

        List<TerminalNode> varCtxs = ctx.VARNAME();

        KVQLParser.Unnest_clauseContext unnestCtx = null;
        int unnestCtxPos = -1;

        for (int i = 0; i < numParseChildren; ++i) {

            ParseTree child = ctx.getChild(i);

            if (child instanceof KVQLParser.ExprContext) {
                exprCtxs.add((KVQLParser.ExprContext)child);

            } else if (child instanceof KVQLParser.Unnest_clauseContext) {
                if (unnestCtx != null) {
                    throw new QueryException(
                        "Only one UNNEST clause is allowed in FROM clause",
                        getLocation((KVQLParser.Unnest_clauseContext)child));
                }
                unnestCtx = (KVQLParser.Unnest_clauseContext)child;
                unnestCtxPos = exprCtxs.size()-1;
            }
        }

        assert(exprCtxs.size() == varCtxs.size());

        if (unnestCtx != null && unnestCtxPos == -1) {
            theWalker.walk(this, unnestCtx);
        }

        for (int i = 0; i < exprCtxs.size(); ++i) {

            KVQLParser.ExprContext exprCtx = exprCtxs.get(i);
            String varName = varCtxs.get(i).getText();

            theWalker.walk(this, exprCtx);

            Expr domainExpr = theExprs.pop();
            ExprVar var = sfw.createFromVar(domainExpr, varName);
            theSctx.addVariable(var);

            if (i == unnestCtxPos) {
                theWalker.walk(this, unnestCtx);
            }
        }

        for (int i = 1; i < sfw.getNumFroms(); ++i) {
            FromClause fc = sfw.getFromClause(i);
            if (fc.getNumVars() == 1) {
                ExprVar var = fc.getVar();
                if (var.isUnnestVar()) {
                    theSctx.addVariable(var);
                }
            }
        }

        /*
         * The children of this From_clauseContext node have been processed
         * already. To avoid reprocessing them, we remove all the children
         * here.
         */
        while (numParseChildren > 0) {
            ctx.removeLastChild();
            --numParseChildren;
        }
    }

    @Override
    public void exitFrom_clause(KVQLParser.From_clauseContext ctx) {

        theInFromClause.pop();
        theQCB.setTables(theTables);
        // TODO: make sure all tables are jsonCollection or none are
    }

    @Override
    public void enterTable_spec(KVQLParser.Table_specContext ctx) {

        int numParseChildren = ctx.getChildCount();

        ExprSFW sfw = (ExprSFW)theExprs.peek();
        ExprBaseTable tableExpr;

        KVQLParser.Nested_tablesContext nestedTablesCtx =
            ctx.nested_tables();

        KVQLParser.Left_outer_join_tablesContext lojTablesCtx =
            ctx.left_outer_join_tables();

        KVQLParser.From_tableContext targetTableCtx = null;
        List<KVQLParser.From_tableContext> ancCtxs = null;
        List<KVQLParser.From_tableContext> descCtxs = null;
        List<KVQLParser.From_tableContext> rightCtxs = null;

        if (nestedTablesCtx != null) {

            Location loc = getLocation(nestedTablesCtx);

            tableExpr = new ExprBaseTable(theQCB, theInitSctx, loc);

            targetTableCtx = nestedTablesCtx.from_table();

            KVQLParser.Ancestor_tablesContext ancCtx =
                nestedTablesCtx.ancestor_tables();

            KVQLParser.Descendant_tablesContext descCtx =
                nestedTablesCtx.descendant_tables();

            translateTable(tableExpr, targetTableCtx, false, false, false,
                           false /* stopWalkWithException */);

            if (ancCtx != null) {
                ancCtxs = ancCtx.from_table();

                for (KVQLParser.From_tableContext anc : ancCtxs) {
                    translateTable(tableExpr, anc, true, false, false,
                                   false /* stopWalkWithException */);
                }
            }

            if (descCtx != null) {
                descCtxs = descCtx.from_table();

                for (KVQLParser.From_tableContext desc : descCtxs) {
                    translateTable(tableExpr, desc, false, true, false,
                                   false /* stopWalkWithException */);
                }
            }

            if (theQCB.getPrepareCallback() != null &&
                !theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        } else if (lojTablesCtx != null) {
            /* left outer join */
            Location loc = getLocation(lojTablesCtx);

            tableExpr = new ExprBaseTable(theQCB, theInitSctx, loc);

            targetTableCtx = lojTablesCtx.from_table();

            /* target table */
            translateTable(tableExpr, targetTableCtx, false, false, false,
                           false /* stopWalkWithException */);

            rightCtxs = new ArrayList<>();

            for (KVQLParser.Left_outer_join_tableContext tctx :
                 lojTablesCtx.left_outer_join_table()) {

                KVQLParser.From_tableContext rightCtx = tctx.from_table();

                /* right table must have ON clause */
                if (rightCtx.ON() == null) {
                    throw new QueryException(
                       "ON clause was missing on the right table '" +
                        rightCtx.aliased_table_name().table_name().getText() +
                        "' of LEFT OUTER JOIN", getLocation(rightCtx));
                }

                translateTable(tableExpr, rightCtx, false /* isAncestor */,
                               false /* isDescendant */, true,
                               false /* stopWalkWithException */);

                rightCtxs.add(rightCtx);
            }

            if (theQCB.getPrepareCallback() != null &&
                !theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        } else {
            targetTableCtx = ctx.from_table();

            Location loc = getLocation(targetTableCtx);

            tableExpr = new ExprBaseTable(theQCB, theInitSctx, loc);

            translateTable(tableExpr, targetTableCtx, false, false, false,
                           true /* stopWalkWithException*/);
        }

        tableExpr.finalizeTables();

        orderTables(sfw, tableExpr, targetTableCtx, ancCtxs, descCtxs, rightCtxs);

        while (numParseChildren > 0) {
            ctx.removeLastChild();
            --numParseChildren;
        }
    }

    /**
     * Extract table name and alias, get the metadata for the table, and store
     * them in the tableExpr.
     *
     * The flag stopWalkWithException indicates if throw StopWalkException to
     * stop parsing or return silently when theQCB.thePrepareCallback specified
     * and thePrepareCallback.preparedNeeded() returns false.
     */
    private void translateTable(
        ExprBaseTable tableExpr,
        KVQLParser.From_tableContext tableCtx,
        boolean isAncestor,
        boolean isDescendant,
        boolean isRightTable,
        boolean stopWalkWithException) {

        boolean isTarget = (!isAncestor && !isDescendant && !isRightTable);

        Location loc;
        String[] pathName;
        String[] mappedPathName;
        String namespace = null;

        KVQLParser.Aliased_table_nameContext aliasedTableName =
            tableCtx.aliased_table_name();

        namespace = computeNamespace(aliasedTableName.table_name());
        pathName = getNamePath(aliasedTableName.table_name().table_id_path());
        loc = getLocation(aliasedTableName);

        /*
         * Callback with all table names, not just the target
         */
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(pathName));
        }

        if (isTarget) {
            if (tableCtx.ON() != null) {
                throw new QueryException(
                    "ON clause can not be used on a target table in the " +
                    "FROM clause", getLocation(tableCtx.ON()));
            }

            if (theQCB.getPrepareCallback() != null) {
                if (!theQCB.getPrepareCallback().prepareNeeded()) {
                    if (stopWalkWithException) {
                        throw new StopWalkException();
                    }
                    return;
                }
            }
        }

        mappedPathName = pathName;

        if (theQCB.getPrepareCallback() != null) {
            mappedPathName = mapTableNames(theQCB.getPrepareCallback(), pathName);
            namespace = theQCB.getPrepareCallback().mapNamespaceName(namespace);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                if (stopWalkWithException) {
                    throw new StopWalkException();
                }
                return;
            }
        }

        TableImpl table = getTable(namespace, mappedPathName, loc);

        if (table == null) {
            throw new QueryException(
               "Table " + NameUtils.makeQualifiedName(namespace,
                   concatPathName(pathName)) + " does not exist", loc);
        }

        String alias = (aliasedTableName.tab_alias() == null ?
                        concatPathName(pathName, '_') :
                        aliasedTableName.tab_alias().getText());

        loc = (aliasedTableName.tab_alias() == null ?
               getLocation(tableCtx) :
               getLocation(aliasedTableName.tab_alias()));

        if (theTableAliases.contains(alias)) {
            throw new QueryException(
                "Table alias " + alias + " is not unique", loc);
        }

        /* left outer join */
        if (isRightTable) {
            TableImpl targetTable = tableExpr.getTargetTable();
            isAncestor = targetTable.isAncestor(table);
            isDescendant = table.isAncestor(targetTable);

            if (!isAncestor && !isDescendant) {
                throw new QueryException("Table " +
                    aliasedTableName.table_name().getText() +
                    " is neither ancestor nor descendant of the target table " +
                    targetTable.getFullName(), loc);
            }

            int numTables = tableExpr.getNumTables();

            if (numTables > 1) {
                /*
                 * All right tables should be in linear order, ancestors first
                 * then descendants.
                 *
                 * e.g.
                 *   A.B.C c LOJ A a
                 *           LOJ A.B b
                 *           LOJ A.B.C.D d
                 *           LOJ A.B.C.D.E.F f
                 */
                TableImpl last = tableExpr.getTable(numTables - 1);
                if (!table.isAncestor(last)) {
                    throw new QueryException(
                        "Table " +
                        aliasedTableName.table_name().getText() +
                        " is not descendant of table " +
                        last.getFullName() +
                        ". Tables in left-outer-joins must appear in " +
                        "top-down order after the target table",
                        loc);
                }
            }
        }

        theTables.add(table);
        theTableAliases.add(alias);

        tableExpr.addTable(table, alias, isAncestor, isDescendant, loc);
    }

    /**
     * Sort the tables in the order that they are encountered during a depth-
     * first traversal of the table hierarchy. For each table, create an
     * associated table variable and translate the associated ON predicate,
     * if any. The variables must be created in this order in order to enforce
     * the rule that an ON pred may access variables over its associated table
     * or its ancestors in the join tree.
     */
    private void orderTables(
        ExprSFW sfw,
        ExprBaseTable tableExpr,
        KVQLParser.From_tableContext targetTableCtx,
        List<KVQLParser.From_tableContext> ancCtxs,
        List<KVQLParser.From_tableContext> descCtxs,
        List<KVQLParser.From_tableContext> rightCtxs) {

        ArrayList<TableImpl> tables = tableExpr.getTables();
        int numTables = tables.size();

        if (numTables == 1) {

            String vname = ExprVar.createVarNameFromTableAlias(
                tableExpr.getAliases().get(0));

            ExprVar var = sfw.createTableVar(tableExpr, tables.get(0), vname);
            theSctx.addVariable(var);

            return;
        }

        ArrayList<TableImpl> sortedTables = new ArrayList<TableImpl>(numTables);
        ArrayList<String> sortedAliases = new ArrayList<String>(numTables);
        ArrayList<ExprVar> sortedVars = new ArrayList<ExprVar>(numTables);

        if (ancCtxs != null || descCtxs != null) {
            TableImpl topTable = tables.get(0).getTopLevelTable();
            traverseTables(sfw, tableExpr, targetTableCtx, ancCtxs, descCtxs,
                           topTable, sortedTables, sortedAliases, sortedVars);
        } else {
            /* For left outer join */
            assert(rightCtxs != null);

            ArrayList<Expr> sortedPreds = new ArrayList<Expr>(numTables);
            traverseTables(sfw, tableExpr, targetTableCtx, rightCtxs, 0,
                           sortedTables, sortedAliases, sortedVars,
                           sortedPreds);

            /* Set table predicates */
            int pos = 0;
            for (Expr pred : sortedPreds) {
                if (pred != null) {
                    tableExpr.setTablePred(pos, pred, false);
                }
                pos++;
            }
        }

        tableExpr.setSortedTables(sortedTables, sortedAliases);

        /*
         * The vars are inserted in temp scopes, which are created and destroyed
         * during the traverseTables() method. So, here we insert all the vars
         * in the SFW scope, so they are visible during the transaltion of the
         * other SFW clauses.
         */
        for (ExprVar var : sortedVars) {
            theSctx.addVariable(var);
        }
    }

    private void traverseTables(
        ExprSFW sfw,
        ExprBaseTable tableExpr,
        KVQLParser.From_tableContext targetTableCtx,
        List<KVQLParser.From_tableContext> ancCtxs,
        List<KVQLParser.From_tableContext> descCtxs,
        TableImpl table,
        ArrayList<TableImpl> sortedTables,
        ArrayList<String> sortedAliases,
        ArrayList<ExprVar> sortedVars) {

        ArrayList<TableImpl> tables = tableExpr.getTables();
        ArrayList<String> aliases = tableExpr.getAliases();
        int numAncestors = tableExpr.getNumAncestors();

        int i = tables.indexOf(table);

        if (i >= 0) {

            pushScope();

            sortedTables.add(table);
            sortedAliases.add(aliases.get(i));
            int sortedPos = sortedTables.size() -1;

            String vname = ExprVar.createVarNameFromTableAlias(aliases.get(i));
            ExprVar var = sfw.createTableVar(tableExpr, table, vname);
            sortedVars.add(var);
            theSctx.addVariable(var);

            if (i == 0) {
                if (targetTableCtx.ON() != null) {
                    throw new QueryException(
                        "ON clause is not allowed on the target " +
                        "table of a NESTED TABLES clause");
                }
            } else if (i <= numAncestors) {
                KVQLParser.From_tableContext anc = ancCtxs.get(i - 1);
                if (anc.ON() != null) {
                    theWalker.walk(this, anc.or_expr());
                    Expr onExpr = theExprs.pop();
                    tableExpr.setTablePred(sortedPos, onExpr, false);
                }
            } else {
                KVQLParser.From_tableContext desc =
                    descCtxs.get(i - numAncestors - 1);
                if (desc.ON() != null) {
                    theWalker.walk(this, desc.or_expr());
                    tableExpr.setTablePred(sortedPos, theExprs.pop(), false);
                }
            }
        }

        for (Map.Entry<String, Table> entry :
                 table.getChildTables().entrySet()) {

            traverseTables(sfw, tableExpr, targetTableCtx, ancCtxs, descCtxs,
                           (TableImpl)entry.getValue(),
                           sortedTables, sortedAliases, sortedVars);
        }

        if (i >= 0) {
            popScope();
        }
    }

    /*
     * Used for left outer join
     *
     * Traverses the right tables, do below things:
     *
     *  1. Validate the ON expressions must contain the join predicates
     *     left.pk = right.pk and exclude them from table predicate.
     *
     *  2. Sort the tables and its associated information in below order
     *       ancestors
     *       target table
     *       descendants
     *  For now, only linear tables supported in left outer join, descendant
     *  tables are already sorted in the tableExpr.getTables(), the ancestors
     *  must be ahead of target table, the order between ancestors doesn't
     *  matter.
     */
    private void traverseTables(
        ExprSFW sfw,
        ExprBaseTable tableExpr,
        KVQLParser.From_tableContext targetTableCtx,
        List<KVQLParser.From_tableContext> rightCtxs,
        int index,
        ArrayList<TableImpl> sortedTables,
        ArrayList<String> sortedAliases,
        ArrayList<ExprVar> sortedVars,
        ArrayList<Expr> sortedPreds) {

        ArrayList<TableImpl> tables = tableExpr.getTables();
        ArrayList<String> aliases = tableExpr.getAliases();
        TableImpl targetTable = tableExpr.getTargetTable();

        assert(index <= tables.size());
        TableImpl table = tables.get(index);

        pushScope();

        boolean isAncestor = targetTable.isAncestor(table);
        int pos = isAncestor ? sortedTables.indexOf(targetTable) : -1;

        String alias = aliases.get(index);
        String vname = ExprVar.createVarNameFromTableAlias(alias);
        ExprVar var = sfw.createTableVar(tableExpr, table, vname, pos);

        theSctx.addVariable(var);

        Expr pred = null;
        if (index == 0) {
            if (targetTableCtx.ON() != null) {
                throw new QueryException(
                    "ON clause is not allowed on the target " +
                    "table of a NESTED TABLES clause");
            }
        } else {
            KVQLParser.Or_exprContext orExpr =
                rightCtxs.get(index - 1).or_expr();
            if (orExpr != null) {
                theWalker.walk(this, orExpr);
                pred = theExprs.pop();
                pred = validateOnExpr(tableExpr, tables, aliases, targetTable,
                                      table, index, pred);
            }
        }

        if (pos >= 0) {
            sortedTables.add(pos, table);
            sortedAliases.add(pos, alias);
            sortedVars.add(pos, var);
            sortedPreds.add(pos, pred);
        } else  {
            sortedTables.add(table);
            sortedAliases.add(alias);
            sortedVars.add(var);
            sortedPreds.add(pred);
        }

        if (++index < tables.size()) {
            traverseTables(sfw, tableExpr, targetTableCtx, rightCtxs, index,
                           sortedTables, sortedAliases, sortedVars,
                           sortedPreds);
        }

        popScope();
    }

    /*
     * Validates ON expression:
     *   1. Check if all the required join predicates left.pk = right.pk exists
     *      in ON expression of the specified right table
     *   2. Remove them and return non-join predicates.
     *   3. For LOJ ancestor table, target table can only be referenced in
     *      join predicates.
     */
    private Expr validateOnExpr(
        ExprBaseTable tableExpr,
        ArrayList<TableImpl> tables,
        ArrayList<String> aliases,
        TableImpl targetTable,
        TableImpl table,
        int pos,
        Expr pred) {

        /* The top expression of ON clause must be AND or EQ func call*/
        if (pred.getKind() != ExprKind.FUNC_CALL) {
            throw new QueryException(
                "Invalid expression in ON clause. The ON clause must be a " +
                "simple \'=\' condition or an number and-ed conditions",
                 pred.getLocation());

        }
        ExprFuncCall top = (ExprFuncCall)pred;
        if (top.getFuncCode() != FuncCode.OP_AND &&
            top.getFuncCode() != FuncCode.OP_EQ) {
            throw new QueryException(
                "Invalid expression in ON clause. The ON clause must be a " +
                "simple \'=\' condition or an number and-ed conditions",
                 pred.getLocation());
        }

        /*
         * Check if the required join equal predicates on primary key columns
         * exist in the on expression and remove them from on expression.
         *
         * If the right table is ancestor table, the left table is target table.
         * If the right table is descendant table, the left table is last
         * descendant table or target table if this is 1st descendant table.
         */
        TableImpl left = targetTable;
        int lpos = 0;
        boolean isDescendant = table.isAncestor(targetTable);
        if (isDescendant) {
            TableImpl last = tables.get(pos - 1);
            if (last.isAncestor(targetTable)) {
                lpos = pos - 1;
                left = last;
            }
        }

        /* Used to store the found pk columns with join predicates */


        /* The expected pk columns to join the table with its left table */
        int numLojPKCols = getLOJPKSize(left, table);
        boolean[] foundPkCols = new boolean[numLojPKCols];

        if (top.getFuncCode() == FuncCode.OP_EQ) {
            if (isPrimaryKeyEQExpr(tableExpr, left, table, 0, top)) {
                foundPkCols[0] = true;
            }
        } else  {
            /* traverses the expression tree, find and remove join predicates. */
            findAndRemoveJoinPreds(tableExpr, left, table, numLojPKCols,
                                   foundPkCols, top);
        }

        for (int pkCol = 0; pkCol < numLojPKCols; pkCol++) {
            if (!foundPkCols[pkCol]) {
                String pkField = left.getPrimaryKeyColumnName(pkCol);
                throw new QueryException(
                    "A join predicate is missing from ON clause of table " +
                    table.getFullName() + " : " +
                    aliases.get(lpos) + "." + pkField + " = " +
                    aliases.get(pos) + "." + pkField);
            }
        }

        if (top.getFuncCode() == FuncCode.OP_EQ) {
            /*
             * Return null if only single join predicate in ON clause, no other
             * predicate for this table
             */
            return null;
        }

        if (top.getNumChildren() > 0) {
            /*
             * The target table was in the scope to resolve join predicates,
             * but it can not be used in the expression other than join
             * predicates if the right table is ancestor, because ancestor
             * tables data will be retrieved firstly then target table on
             * server side.
             */
            if (!isDescendant) {
                checkInvalidColumnRef(top, targetTable);
            }
            return top;
        }

        return null;
    }

    /* Find and remove the predicates used to join left and right tables */
    private void findAndRemoveJoinPreds(
        ExprBaseTable tableExpr,
        TableImpl left,
        TableImpl right,
        int numLojPkCols,
        boolean[] retPkPos,
        ExprFuncCall top) {

        List<Expr> joinPreds= new ArrayList<Expr>();
        ExprIter iter = top.getChildrenIter();

        while (iter.hasNext()) {
            Expr exp = iter.next();

            if (exp.getFunction(FuncCode.OP_EQ) != null) {
                ExprFuncCall fc = (ExprFuncCall)exp;
                for (int pkCol = 0; pkCol < numLojPkCols; pkCol++) {
                    if (isPrimaryKeyEQExpr(tableExpr, left, right, pkCol, fc)) {
                        joinPreds.add(fc);
                        retPkPos[pkCol] = true;
                        break;
                    }
                }
            }
        }
        iter.reset();

        for (Expr exp : joinPreds) {
            top.removeChild(exp, true);
        }
    }

    /*
     * Returns true if the given expr is left.pk = right.pk, the pk is the
     * primary key column specified by pkCol.
     */
    private boolean isPrimaryKeyEQExpr(
        ExprBaseTable tableExpr,
        TableImpl left,
        TableImpl right,
        int pkCol,
        ExprFuncCall expr) {

        if (expr.getFuncCode() != FuncCode.OP_EQ) {
            return false;
        }

        boolean foundLeft = false;
        boolean foundRight = false;

        ExprIter iter = expr.getChildrenIter();
        while (iter.hasNext()) {
            Expr e = iter.next();
            if (!foundLeft) {
                foundLeft = ExprUtils.
                            isPrimKeyColumnRef(tableExpr, left, pkCol, e);
                if (foundLeft) {
                    continue;
                }
            }
            if (!foundRight) {
                foundRight = ExprUtils.
                             isPrimKeyColumnRef(tableExpr, right, pkCol, e);
            }
        }
        iter.reset();

        return foundLeft && foundRight;
    }

    /**
     * Checks if given expression references to any column of the specified
     * table which is not allowed.
     */
    private void checkInvalidColumnRef(Expr top, TableImpl table) {

        ExprIter iter = top.getChildrenIter();

        while (iter.hasNext()) {
            Expr expr = iter.next();
            if (expr.getKind() == ExprKind.FIELD_STEP) {
                ExprFieldStep stepExpr = (ExprFieldStep)expr;
                if (stepExpr.getInput().getKind() == ExprKind.VAR) {
                    ExprVar var = (ExprVar)stepExpr.getInput();
                    TableImpl table2 = var.getTable();
                    if (table2 != null && table2.getId() == table.getId()) {
                        throw new QueryException(
                            "Table alias " + var.display() +
                            " cannot be referenced at this location",
                            expr.getLocation());
                    }
                }
            } else {
                checkInvalidColumnRef(expr, table);
            }
        }
        iter.reset();
    }

    /*
     * Returns the number of required primary key fields to join left and
     * right table.
     *   o join ancestor
     *      return ancestor primary key columns
     *   o join descendant
     *      return its parent's primary key columns
     */
    private int getLOJPKSize(TableImpl left, TableImpl right) {
        if (right.isAncestor(left)) {
            return left.getPrimaryKeySize();
        }
        return right.getPrimaryKeySize();
    }

    @Override
    public void enterUnnest_clause(KVQLParser.Unnest_clauseContext ctx) {

        ExprSFW sfw = (ExprSFW)theExprs.peek();
        int numParseChildren = ctx.getChildCount();

        List<KVQLParser.Path_exprContext> exprCtxs =
            new ArrayList<KVQLParser.Path_exprContext>(numParseChildren);

        List<TerminalNode> varCtxs = ctx.VARNAME();

        for (int i = 0; i < numParseChildren; ++i) {

            ParseTree child = ctx.getChild(i);

            if (child instanceof KVQLParser.Path_exprContext) {
                exprCtxs.add((KVQLParser.Path_exprContext)child);
            }
        }

        assert(exprCtxs.size() == varCtxs.size());

        pushScope();

        ArrayList<ExprVar> unnestVars = new  ArrayList<ExprVar>(4);

        for (int i = 0; i < exprCtxs.size(); ++i) {

            KVQLParser.Path_exprContext exprCtx = exprCtxs.get(i);
            String varName = varCtxs.get(i).getText();

            theWalker.walk(this, exprCtx);

            Expr domainExpr = theExprs.pop();
            ExprVar var = sfw.createFromVar(domainExpr, varName);
            var.setIsUnnestVar();
            unnestVars.add(var);
            theSctx.addVariable(var);
        }

        popScope();

        ExprVar tableVar = validateUnnestClause(unnestVars);
        assert(tableVar != null);
        TableImpl table = tableVar.getTable();
        assert(table != null);
        ExprBaseTable tableExpr = sfw.findTableExprForVar(tableVar);
        assert(tableExpr != null);

        if (tableExpr.getTargetTable() != table) {
            throw new QueryException(
                "UNNEST clause cannot be applied to an ancestor or " +
                "descendant table", getLocation(ctx));
        }

        Map<String, Index> indexes = table.getIndexes();

        for (Index index : indexes.values()) {

            IndexImpl idx = (IndexImpl)index;

            if (!idx.isMultiKey() || !idx.isUnique()) {
                continue;
            }

            IndexExpr.analyzeUnnestClause((IndexImpl)index, unnestVars);
        }

        /*
         * The children of this Unnest_clauseContext node have been processed
         * already. To avoid reprocessing them, we remove all the children
         * here.
         */
        while (numParseChildren > 0) {
            ctx.removeLastChild();
            --numParseChildren;
        }
    }

    private ExprVar validateUnnestClause(
        ArrayList<ExprVar> unnestVars) {

        ExprVar invar = null;
        ExprVar tableVar = null;

        for (ExprVar unnestVar : unnestVars) {

            ExprVar var = checkUnnestVar(unnestVar, invar);
            if (var != null) {
                tableVar = var;
            }
            invar = unnestVar;
        }

        return tableVar;
    }

    private ExprVar checkUnnestVar(ExprVar var, ExprVar invar) {

        Expr expr = var.getDomainExpr();

        ExprKind kind = expr.getKind();

        if (kind != ExprKind.ARRAY_FILTER && kind != ExprKind.MAP_FILTER) {
            throw new QueryException(
                "Path expression in UNNEST clause does not end with " +
                "[] or values() step.", expr.getLocation());
        }

        if (kind == ExprKind.MAP_FILTER) {
            ExprMapFilter stepExpr = (ExprMapFilter)expr;
            if (stepExpr.getFilterKind() != FilterKind.VALUES) {
                throw new QueryException(
                    "Path expression in UNNEST clause does not end with " +
                    "[] or values() step.", expr.getLocation());
            }
        }

        while (expr != null) {

            switch (expr.getKind()) {
            case FIELD_STEP:
                expr = expr.getInput();
                break;
            case MAP_FILTER: {
                ExprMapFilter stepExpr = (ExprMapFilter)expr;
                if (stepExpr.getPredExpr() != null) {
                    throw new QueryException(
                        "Path expression in UNNEST clause contains a " +
                        "values() step with a filtering predicate.",
                        expr.getLocation());
                }
                expr = expr.getInput();
                break;
            }
            case ARRAY_FILTER: {
                ExprArrayFilter stepExpr = (ExprArrayFilter)expr;
                expr = expr.getInput();
                if (stepExpr.getPredExpr() != null) {
                    throw new QueryException(
                        "Path expression in UNNEST clause contains a " +
                        "[] step with a filtering predicate.",
                        expr.getLocation());
                }
                expr = expr.getInput();
                break;
            }
            case VAR: {
                if (invar == null) {
                    ExprVar tableVar = (ExprVar)expr;
                    if (tableVar.getTable() == null) {
                        throw new QueryException(
                            "The first Path expression in UNNEST clause does not " +
                            "start with table variable", expr.getLocation());
                    }
                    return tableVar;
                }

                if (expr != invar) {
                    throw new QueryException(
                        "Path expression in UNNEST clause does not " +
                        "start with previous variable", expr.getLocation());
                }
                return null;
            }
            default:
                throw new QueryException(
                    "Invalid expression in UNNEST clause",
                    expr.getLocation());
            }
        }

        return null;
    }

    /*
     * where_clause : WHERE expr ;
     */
    @Override
    public void enterWhere_clause(KVQLParser.Where_clauseContext ctx) {

        theInWhereClause.push(true);
        theNearExprs.push(null);
    }

    @Override
    public void exitWhere_clause(KVQLParser.Where_clauseContext ctx) {

        theInWhereClause.pop();

        Expr condExpr = theExprs.pop();
        ExprSFW sfwExpr = (ExprSFW)theExprs.peek();

        sfwExpr.addWhereClause(condExpr);

        Expr near = theNearExprs.pop();

        /*
         * Check that geo_near function, if present, appear as a top-level
         * predicate, and if so, save a ref to it in the SFW.
         */
        if (near != null) {

            if (near != condExpr) {
                if (condExpr.getFunction(FuncCode.OP_AND) == null) {
                    throw new QueryException(
                        "The geo_near function must be a top-level predicate " +
                        "in WHERE clause",
                        near.getLocation());
                }

                ExprIter children = condExpr.getChildren();
                boolean found = false;

                while (children.hasNext()) {
                    Expr child = children.next();
                    if (child == near) {
                        found = true;
                        break;
                    }
                }

                children.reset();

                if (!found) {
                    throw new QueryException(
                        "The geo_near function must be a top-level predicate " +
                        "in WHERE clause",
                        near.getLocation());
                }
            }

            sfwExpr.setNearPred((ExprFuncCall)near);
        }
    }

    /*
     * groupby_clause : GROUP BY expr (COMMA expr)* ;
     */
    @Override
    public void enterGroupby_clause(
        KVQLParser.Groupby_clauseContext ctx) {
        /* Push a null sentinel in the stacks */
        theExprs.push(null);
    }

    @Override
    public void exitGroupby_clause(
        KVQLParser.Groupby_clauseContext ctx) {

        ArrayList<Expr> gbExprs = new ArrayList<Expr>();

        Expr expr = theExprs.pop();

        while (expr != null) {
            gbExprs.add(expr);
            expr = theExprs.pop();
        }

        Collections.reverse(gbExprs);

        ExprSFW sfw = (ExprSFW)theExprs.peek();
        sfw.addGroupByClause(gbExprs);
    }

    /*
     * select_clause :
     *     SELECT hint? (STAR | (expr col_alias (COMMA expr col_alias)*)) ;
     *
     * col_alias : (AS id)?
     */
    @Override
    public void enterSelect_list(KVQLParser.Select_listContext ctx) {

        theInSelectClause.push(true);

        ExprSFW sfw = (ExprSFW)theExprs.peek();

        if (ctx.DISTINCT() != null) {
            sfw.setIsSelectDistinct();
        }

        /*
         * If it's a "select *", add in the SELECT clause an expr for each
         * of the variables declared in the FROM clause.
         */
        if (ctx.STAR() != null) {

            if (sfw.hasGroupBy()) {
                throw new QueryException(
                    "select * is not allowed together with group by");
            }

            int numFroms = sfw.getNumFroms();

            ArrayList<Expr> colExprs = new ArrayList<Expr>(numFroms);
            ArrayList<String> colNames = new ArrayList<String>(numFroms);

            for (int i = 0; i < numFroms; ++i) {
                for (ExprVar var : sfw.getFromClause(i).getVars()) {
                    colExprs.add(var);
                    String varname = var.getName();
                    if (varname.startsWith("$$")) {
                        colNames.add(var.getName().substring(2));
                    } else {
                        colNames.add(var.getName().substring(1));
                    }
                }
            }

            sfw.addSelectClause(colNames, colExprs);
            sfw.setIsSelectStar(true);

        } else {
            /* Push a null sentinel in the stacks */
            theExprs.push(null);
            theColNames.push(null);
        }
    }

    @Override
    public void exitSelect_list(KVQLParser.Select_listContext ctx) {

        if (ctx.STAR() != null) {
            return;
        }

        ArrayList<Expr> colExprs = new ArrayList<Expr>();
        ArrayList<String> colNames = new ArrayList<String>();

        Expr expr = theExprs.pop();
        String name = theColNames.pop();
        boolean hasASclauses = true;

        HashSet<String> uniqueColNames = new HashSet<String>(colNames.size());

        while (expr != null) {

            if (name == null) {
                hasASclauses = false;
                if (expr.getKind() == ExprKind.FIELD_STEP) {
                    name = ((ExprFieldStep)expr).getFieldName();
                } else if (expr.getKind() == ExprKind.VAR) {
                    name = ((ExprVar)expr).getName().substring(1);
                }
            } else if (!uniqueColNames.add(name)) {
                throw new QueryException(
                    "Duplicate column name in SELECT clause: " + name,
                    expr.getLocation());
            }

            colExprs.add(expr);
            colNames.add(name);

            expr = theExprs.pop();
            name = theColNames.pop();
        }

        Collections.reverse(colExprs);
        Collections.reverse(colNames);
        uniqueColNames.clear();

        ExprSFW sfw = (ExprSFW)theExprs.peek();

        for (int i = 0; i < colNames.size(); ++i) {
            String colName = colNames.get(i);
            if (colName == null || (!uniqueColNames.add(colName))) {
                colNames.set(i, ("Column_" + (i+1)));
            }
        }

        if (colNames.size() == 1 && !hasASclauses) {
            theQCB.setWrapResultInRecord(colNames.get(0));
        }

        sfw.addSelectClause(colNames, colExprs);
        theInSelectClause.pop();
    }

    @Override
    public void enterCol_alias(KVQLParser.Col_aliasContext ctx) {

        if (ctx.id() == null) {
            theColNames.push(null);
        } else {
            theColNames.push(ctx.id().getText());
        }
    }

    /*
     *  hint : ( (PREFER_INDEXES LP table_name index_name* RP) |
     *           (FORCE_INDEX    LP table_name index_name  RP) |
     *           (PREFER_PRIMARY_INDEX LP table_name RP)       |
     *           (FORCE_PRIMARY_INDEX  LP table_name RP) ) STRING?;
     */
    @Override
    public void exitHint(KVQLParser.HintContext ctx) {

        ExprSFW sfwExpr = (ExprSFW)theExprs.peek();
        if (sfwExpr == null) {
            // skipping the null sentinel inserted by enterSelect_clause()
            sfwExpr = (ExprSFW)theExprs.get( theExprs.size() - 2);
            if (sfwExpr == null) {
                throw new QueryStateException("SFW expr not found.");
            }
        }

        assert ctx.table_name().table_id_path() != null :
               "Table name missing from " +
               " hint at: " + getLocation(ctx.table_name());

        ExprBaseTable exprBaseTable = sfwExpr.getFirstFrom().getTableExpr();
        String tableName = ctx.table_name().getText();
        /*
         * tableName comes directly from the parser, while the exprBaseTable
         * below has a potentially mapped name. Map the tableName if needed.
         */
        if (theQCB.getPrepareCallback() != null) {
            tableName = theQCB.getPrepareCallback().mapTableName(tableName);
        }

        if (!exprBaseTable.getTargetTable().getFullName().
            equalsIgnoreCase(tableName)) {
                throw new QueryException(
                    "Table name specified in " +
                    "hint doesn't match the table in the FROM statement: " +
                    exprBaseTable.getTargetTable().getFullName(),
                    getLocation(ctx.table_name()));
        }

        if (ctx.PREFER_INDEXES() != null) {

            for (KVQLParser.Index_nameContext indxCtx : ctx.index_name()) {
                String indexName = indxCtx.getText();
                IndexImpl indx = (IndexImpl)exprBaseTable.getTargetTable().
                    getIndex(indexName);

                if (indx == null) {
                    /*
                     * Ignore the hint if the specified index does not actually
                     * exist. The index could have existed when the query was
                     * written, but it was dropped some time later. In this case
                     * we don't want existing queries to start throwing errors
                     * (of course, if there are any saved PreparedStatements,
                     * those should also be recompiled, but that a bigger TODO).
                     */
                    continue;
                }
                exprBaseTable.addIndexHint(indx, false, getLocation(indxCtx));
            }

        } else if (ctx.FORCE_INDEX() != null) {

            for (KVQLParser.Index_nameContext indxCtx : ctx.index_name()) {
                String indexName = indxCtx.getText();
                IndexImpl indx = (IndexImpl)exprBaseTable.getTargetTable().
                    getIndex(indexName);

                if (indx == null) {
                    continue;
                }

                exprBaseTable.addIndexHint(indx, true, getLocation(indxCtx));
            }

        } else if (ctx.PREFER_PRIMARY_INDEX() != null) {

            exprBaseTable.addIndexHint(null, false, getLocation(ctx));

        } else if (ctx.FORCE_PRIMARY_INDEX() != null) {

            exprBaseTable.addIndexHint(null, true, getLocation(ctx));

        }
    }

    /*
     * orderby_clause : ORDER BY expr sort_spec (COMMA expr sort_spec)* ;
     *
     * sort_spec : (ASC | DESC)? (NULLS (FIRST | LAST))? ;
     */
    @Override
    public void enterOrderby_clause(KVQLParser.Orderby_clauseContext ctx) {

        theInOrderByClause.push(true);

        /* Push a null sentinel in the stacks */
        theExprs.push(null);
        theSortSpecs.push(null);
    }

    @Override
    public void exitOrderby_clause(
        KVQLParser.Orderby_clauseContext ctx) {

        ArrayList<Expr> sortExprs = new ArrayList<Expr>();
        ArrayList<SortSpec> sortSpecs = new ArrayList<SortSpec>();

        Expr expr = theExprs.pop();
        SortSpec spec = theSortSpecs.pop();

        while (expr != null) {
            sortExprs.add(expr);
            sortSpecs.add(spec);
            expr = theExprs.pop();
            spec = theSortSpecs.pop();
        }

        Collections.reverse(sortExprs);
        Collections.reverse(sortSpecs);

        ExprSFW sfw = (ExprSFW)theExprs.peek();
        sfw.addSortClause(sortExprs, sortSpecs);
        theInOrderByClause.pop();
    }

    @Override
    public void enterSort_spec(KVQLParser.Sort_specContext ctx) {

        boolean desc = false;
        boolean nullsFirst = false;

        if (ctx.DESC() != null) {
            desc = true;
        }

        if (ctx.NULLS() == null) {
            nullsFirst = desc;
        } else if (ctx.FIRST() != null) {
            nullsFirst = true;
        }

        theSortSpecs.push(new SortSpec(desc, nullsFirst));
    }

    /*
     * limit_clause : LIMIT add_expr
     */
    @Override
    public void exitLimit_clause(
        KVQLParser.Limit_clauseContext ctx) {

        Expr limitExpr = null;

        limitExpr = theExprs.pop();

        ExprSFW sfwExpr = (ExprSFW)theExprs.peek();

        sfwExpr.addLimit(limitExpr);
    }

    /*
     * offset_clause : OFFSET add_expr
     */
    @Override
    public void exitOffset_clause(
        KVQLParser.Offset_clauseContext ctx) {

        Expr offsetExpr = null;

        offsetExpr = theExprs.pop();

        ExprSFW sfwExpr = (ExprSFW)theExprs.peek();

        sfwExpr.addOffset(offsetExpr);
    }

    /*
     * case_expr : CASE WHEN expr THEN expr (WHEN expr THEN expr)* ELSE expr END;
     */
    @Override
    public void enterCase_expr(KVQLParser.Case_exprContext ctx) {

        theExprs.push(null);
    }

    @Override
    public void exitCase_expr(KVQLParser.Case_exprContext ctx) {

        Location loc = getLocation(ctx);

        ArrayList<Expr> exprs = new ArrayList<Expr>();

        if (ctx.ELSE() != null) {
            exprs.add(theExprs.pop());
        }

        Expr expr = theExprs.pop();

        while (expr != null) {
            exprs.add(expr);
            expr = theExprs.pop();
        }

        Collections.reverse(exprs);

        Expr caseExpr = new ExprCase(theQCB, theInitSctx, loc, exprs);
        theExprs.push(caseExpr);
    }

    /*
     * or_expr : and_expr | or_expr OR and_expr ;
     */
    @Override
    public void exitOr_expr(KVQLParser.Or_exprContext ctx) {

        if (ctx.OR() == null) {
            return;
        }

        Location loc = getLocation(ctx);
        Expr op2 = theExprs.pop();
        Expr op1 = theExprs.pop();

        ArrayList<Expr> args = new ArrayList<Expr>(2);

        if (op1.getKind() == ExprKind.FUNC_CALL) {

            ExprFuncCall fnCall = (ExprFuncCall)op1;

            if (fnCall.getFunction().getCode() == FuncCode.OP_OR) {
                flattenAndOrArgs(fnCall.getChildren(), args);
            } else {
                args.add(op1);
            }
        } else {
            args.add(op1);
        }

        if (op2.getKind() == ExprKind.FUNC_CALL) {

            ExprFuncCall fnCall = (ExprFuncCall)op2;

            if (fnCall.getFunction().getCode() == FuncCode.OP_OR) {
                flattenAndOrArgs(fnCall.getChildren(), args);
            } else {
                args.add(op2);
            }
        } else {
            args.add(op2);
        }

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                        theFuncLib.getFunc(FuncCode.OP_OR),
                                        args);
        theExprs.push(expr);
    }

    /*
     * and_expr : not_expr | and_expr AND not_expr ;
     */
    @Override
    public void exitAnd_expr(KVQLParser.And_exprContext ctx) {

        if (ctx.AND() == null) {
            return;
        }

        Location loc = getLocation(ctx);
        Expr op2 = theExprs.pop();
        Expr op1 = theExprs.pop();

        ArrayList<Expr> args = new ArrayList<Expr>(2);

        if (op1.getKind() == ExprKind.FUNC_CALL) {

            ExprFuncCall fnCall = (ExprFuncCall)op1;

            if (fnCall.getFunction().getCode() == FuncCode.OP_AND) {
                flattenAndOrArgs(fnCall.getChildren(), args);
            } else {
                args.add(op1);
            }
        } else {
            args.add(op1);
        }

        if (op2.getKind() == ExprKind.FUNC_CALL) {

            ExprFuncCall fnCall = (ExprFuncCall)op2;

            if (fnCall.getFunction().getCode() == FuncCode.OP_AND) {
                flattenAndOrArgs(fnCall.getChildren(), args);
            } else {
                args.add(op2);
            }
        } else {
            args.add(op2);
        }

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                        theFuncLib.getFunc(FuncCode.OP_AND),
                                        args);
        theExprs.push(expr);
    }

    private void flattenAndOrArgs(ExprIter children, List<Expr> args) {

        while (children.hasNext()) {
           Expr arg = children.next();
           children.remove(false/*destroy*/);
           args.add(arg);
        }
        children.reset();
    }

    /*
     * not_expr : NOT ? is_null_expr
     */
    @Override
    public void exitNot_expr(KVQLParser.Not_exprContext ctx) {

        if (ctx.NOT() == null) {
            return;
        }

        Location loc = getLocation(ctx);
        Expr input = theExprs.pop();

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(input);

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                        theFuncLib.getFunc(FuncCode.OP_NOT),
                                        args);
        theExprs.push(expr);
    }

    /*
     * is_null_expr : cond_expr IS NOT? NULL;
     *
     * cond_expr := between_expr | comp_expr | exists_expr | is_of_type_expr | in_expr;
     */
    @Override
    public void exitIs_null_expr(KVQLParser.Is_null_exprContext ctx) {

        Location loc = getLocation(ctx);

        if (ctx.NULL() == null) {
            return;
        }

        Function func = (ctx.NOT() != null ?
                         theFuncLib.getFunc(FuncCode.OP_IS_NOT_NULL) :
                         theFuncLib.getFunc(FuncCode.OP_IS_NULL));

        Expr input = theExprs.pop();

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(input);

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }

    /*
     * exists_expr : EXISTS concatenate_expr ;
     */
    @Override
    public void exitExists_expr(KVQLParser.Exists_exprContext ctx) {

        Location loc = getLocation(ctx);

        Function func = theFuncLib.getFunc(FuncCode.OP_EXISTS);

        Expr input = theExprs.pop();

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(input);

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }

    /*
     * in1_expr : in1_left_op IN LP in1_expr_list (COMMA in1_expr_list)+ RP ;
     *
     * in1_left_op : LP concatenate_expr (COMMA concatenate_expr)* RP ;
     *
     * in1_expr_list : LP expr (COMMA expr)* RP ;
     */
    @Override
    public void enterIn1_expr(KVQLParser.In1_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr inExpr = new ExprInOp(theQCB, theInitSctx, loc, false);

        theExprs.push(inExpr);
        theExprs.push(null);
    }

    @Override
    public void exitIn1_left_op(KVQLParser.In1_left_opContext ctx) {

        ArrayList<Expr> args = new ArrayList<Expr>(4);

        Expr e = theExprs.pop();

        while (e != null) {
            args.add(e);
            e = theExprs.pop();
        }

        Collections.reverse(args);

        ExprInOp inExpr = (ExprInOp)theExprs.peek();
        inExpr.addDataArgs(args);
        theExprs.push(null);
    }

    @Override
    public void exitIn1_expr_list(KVQLParser.In1_expr_listContext ctx) {

        ArrayList<Expr> args = new ArrayList<Expr>(32);

        Expr e = theExprs.pop();

        while (e != null) {
            args.add(e);
            e = theExprs.pop();
        }

        Collections.reverse(args);

        ExprInOp inExpr = (ExprInOp)theExprs.peek();
        inExpr.addKeyArgs(args);
        theExprs.push(null);
    }

    @Override
    public void exitIn1_expr(KVQLParser.In1_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr e = theExprs.pop();
        assert(e == null);

        ExprInOp inExpr = (ExprInOp)theExprs.peek();

        if (inExpr.getNumKeys() == 0) {
            theExprs.pop();
            theExprs.push(new ExprConst(theQCB, theInitSctx, loc, false));
        }
    }

    /*
     * in2_expr : concatenate_expr IN LP expr (COMMA expr)+ RP ;
     */
    @Override
    public void enterIn2_expr(KVQLParser.In2_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr inExpr = new ExprInOp(theQCB, theInitSctx, loc, false);

        theExprs.push(inExpr);
        theExprs.push(null);
    }

    @Override
    public void exitIn2_expr(KVQLParser.In2_exprContext ctx) {

        Location loc = getLocation(ctx);

        ArrayList<Expr> args = new ArrayList<Expr>(32);

        Expr e = theExprs.pop();

        while (e != null) {
            args.add(e);
            e = theExprs.pop();
        }

        Collections.reverse(args);

        ExprInOp inExpr = (ExprInOp)theExprs.peek();
        inExpr.addArgs(args);

        if (inExpr.getNumKeys() == 0) {
            theExprs.pop();
            theExprs.push(new ExprConst(theQCB, theInitSctx, loc, false));
        }
    }

    /*
     * in3_expr :
     *     (concatenate_expr |
     *      (LP concatenate_expr (COMMA concatenate_expr)* RP)) IN path_expr ;
     */
    @Override
    public void enterIn3_expr(KVQLParser.In3_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr inExpr = new ExprInOp(theQCB, theInitSctx, loc, true);

        theExprs.push(inExpr);
        theExprs.push(null);
    }

    @Override
    public void exitIn3_expr(KVQLParser.In3_exprContext ctx) {

        ArrayList<Expr> args = new ArrayList<Expr>(32);

        Expr e = theExprs.pop();

        while (e != null) {
            args.add(e);
            e = theExprs.pop();
        }

        Collections.reverse(args);

        ExprInOp inExpr = (ExprInOp)theExprs.peek();
        inExpr.addArgs(args);
    }

    /*
     * between_expr : concatenate_expr BETWEEN concatenate_expr AND concatenate_expr ;
     */
    @Override
    public void exitBetween_expr(KVQLParser.Between_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr highop = theExprs.pop();
        Expr lowop = theExprs.pop();
        Expr op = theExprs.pop();

        Expr expr1 = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                         FuncCode.OP_LE, lowop, op);

        Expr expr2 = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                         FuncCode.OP_LE, op, highop);

        Expr andExpr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                         FuncCode.OP_AND, expr1, expr2);
        theExprs.push(andExpr);
    }

    /*
     * comp_expr : (concatenate_expr ((comp_op | any_op) concatenate_expr)?) ;
     *
     * comp_op : EQ | NEQ | GT | GTE | LT | LTE ;
     *
     * any_op : EQ_ANY | NEQ_ANY | GT_ANY | GTE_ANY | LT_ANY | LTE_ANY;
     */
    @Override
    public void exitComp_expr(KVQLParser.Comp_exprContext ctx) {

        Location loc = getLocation(ctx);
        KVQLParser.Comp_opContext cmpctx = ctx.comp_op();
        KVQLParser.Any_opContext anyctx = ctx.any_op();

        if (cmpctx == null && anyctx == null) {
            return;
        }

        Expr op2 = theExprs.pop();
        Expr op1 = theExprs.pop();
        Function func;

        if (cmpctx != null) {
            if (cmpctx.EQ() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_EQ);
            } else if (cmpctx.NEQ() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_NEQ);
            } else if (cmpctx.GT() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_GT);
            } else if (cmpctx.GTE() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_GE);
            } else if (cmpctx.LT() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_LT);
            } else if (cmpctx.LTE() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_LE);
            } else {
                throw new QueryException(
                    "Unexpected comparison operator: " + cmpctx.getText(), loc);
            }
        } else {
            if (anyctx.EQ_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_EQ_ANY);
            } else if (anyctx.NEQ_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_NEQ_ANY);
            } else if (anyctx.GT_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_GT_ANY);
            } else if (anyctx.GTE_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_GE_ANY);
            } else if (anyctx.LT_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_LT_ANY);
            } else if (anyctx.LTE_ANY() != null) {
                func = theFuncLib.getFunc(FuncCode.OP_LE_ANY);
            } else {
            throw new QueryException(
                "Unexpected comparison operator: " + anyctx.getText(), loc);
            }
        }

        ArrayList<Expr> args = new ArrayList<Expr>(2);
        args.add(op1);
        args.add(op2);

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }

    /*
     * quantified_type_def : type_def ( STAR | PLUS | QUESTION_MARK)? ;
     */
    @Override
    public void enterQuantified_type_def(KVQLParser.Quantified_type_defContext ctx) {
        if (ctx.PLUS() != null) {
            theQuantifiers.push(Quantifier.PLUS);
        } else if (ctx.STAR() != null) {
            theQuantifiers.push(Quantifier.STAR);
        } else if (ctx.QUESTION_MARK() != null) {
            theQuantifiers.push(Quantifier.QSTN);
        } else {
            theQuantifiers.push(Quantifier.ONE);
        }
    }

    /*
     * is_of_type_expr :
     *     concatenate_expr IS NOT? OF TYPE?
     *     LP ONLY? quantified_type_def ( COMMA ONLY? quantified_type_def )* RP;
     */
    @Override
    public void exitIs_of_type_expr(KVQLParser.Is_of_type_exprContext ctx) {

        Location loc = getLocation(ctx);
        Expr input = theExprs.pop();
        boolean notFlag = ctx.NOT() != null;

        int numTypes = ctx.quantified_type_def().size();

        List<FieldDef> types = new ArrayList<FieldDef>(numTypes);
        List<Quantifier> quantifiers = new ArrayList<Quantifier>(numTypes);
        List<Boolean> onlyFlags = new ArrayList<Boolean>(numTypes);

        for (int i = 0; i < numTypes; i++) {
            FieldDefImpl typeDef = theTypes.pop();
            Quantifier typeQuantifier = theQuantifiers.pop();

            types.add(typeDef);
            quantifiers.add(typeQuantifier);
            onlyFlags.add(ctx.ONLY(i) != null);
        }

        ExprIsOfType expr = new ExprIsOfType(theQCB, theInitSctx, loc,
                                             input, notFlag, types,
                                             quantifiers, onlyFlags);
        theExprs.push(expr);
    }

    /*
     * cast_expr : CAST LP expr AS quantified_type_def RP ;
     */
    @Override
    public void exitCast_expr(KVQLParser.Cast_exprContext ctx) {
        Expr input = theExprs.pop();

        FieldDefImpl typeDefImpl = theTypes.pop();
        Quantifier typeQuantifier = theQuantifiers.pop();

        Expr expr = ExprCast.create(
            theQCB, theInitSctx, getLocation(ctx), input, typeDefImpl,
            typeQuantifier);

        theExprs.push(expr);
    }

    /*
     * extract_expr : EXTRACT LP id FROM expr RP ;
     */
    @Override
    public void exitExtract_expr(KVQLParser.Extract_exprContext ctx) {
        Location loc = getLocation(ctx);

        String text = ctx.id().getText();
        Unit unit;
        try {
            unit = Unit.valueOf(text.toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new QueryException("Unrecognized unit: " + text, loc);
        }

        FuncCode funcCode;
        if (unit.ordinal() <= Unit.ISOWEEK.ordinal()) {
            funcCode = FuncCode.valueOf(FuncCode.FN_YEAR.ordinal() +
                                        unit.ordinal());
        } else {
            int offset = unit.ordinal() - Unit.QUARTER.ordinal();
            funcCode = FuncCode.valueOf(FuncCode.FN_QUARTER.ordinal() + offset);
        }

        Function func = theFuncLib.getFunc(funcCode);

        Expr input = theExprs.pop();

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, input);
        theExprs.push(expr);
    }


    /*
     * concatenate_expr : add_expr ( CONCAT add_expr)* ;
     */
    @Override
    public void enterConcatenate_expr(KVQLParser.Concatenate_exprContext ctx) {
        if (ctx.CONCAT().isEmpty()) {
            return;
        }

        theExprs.push(null);
    }

    /*
     * concatenate_expr : add_expr ( CONCAT add_expr)* ;
     */
    @Override
    public void exitConcatenate_expr(KVQLParser.Concatenate_exprContext ctx) {
        if (ctx.CONCAT().isEmpty()) {
            return;
        }

        ArrayList<Expr> inputs = new ArrayList<Expr>();

        Expr input = theExprs.pop();
        while (input != null) {
            inputs.add(input);
            input = theExprs.pop();
        }

        Collections.reverse(inputs);

        Location loc = getLocation(ctx);
        Function func = theFuncLib.getFunc(FuncCode.OP_CONCATENATE_STRINGS);
        Expr concatExpr = ExprFuncCall.create(theQCB, theInitSctx, loc, func,
            inputs);

        theExprs.push(concatExpr);
    }

    /*
     * add_expr : multiply_expr ((PLUS | MINUS) multiply_expr)* ;
     */
    @Override
    public void exitAdd_expr(KVQLParser.Add_exprContext ctx) {

        if (ctx.PLUS().isEmpty() && ctx.MINUS().isEmpty()) {
            return;
        }

        Location loc = getLocation(ctx);

        Function func = theFuncLib.getFunc(FuncCode.OP_ADD_SUB);

        /*
         * operations stores, as a "+" or "-" char, each operator that appears
         * in the additive expr processed. The ops are stored in the order of
         * their appearance in the expr. This state will get saved in a
         * ConstExpr, which is added as an arg to the ExprFuncCall created here,
         * and will eventually be passed to the ArithOpIter.
         */
        String operations = "";
        /*
         * args contain the operands in reverse query order because this is how
         * they are on the stack. After the for-loop is done, the order will be
         * reversed to follow the order of the specified ops in the query. The
         * last arg contains the ConstExpr holding the operations string.
         */
        ArrayList<Expr> args = new ArrayList<Expr>(ctx.getChildCount()/2 + 2);

        for (int c = ctx.getChildCount() - 1; c >= 0; c = c - 2) {

            ParseTree pt = ctx.getChild(c);

            if (pt instanceof RuleNode) {

                Expr op = theExprs.pop();

                if (c > 0) {
                    ParseTree ptOp = ctx.getChild(c - 1);

                    if (ptOp instanceof TerminalNode) {
                        int tokenId =
                            ((TerminalNode)ptOp).getSymbol().getType();

                        if ( tokenId == KVQLParser.PLUS ) {
                            operations = "+" + operations;
                            args.add(op);
                        } else if (tokenId == KVQLParser.MINUS) {
                            operations = "-" + operations;
                            args.add(op);
                        } else {
                            throw new QueryStateException(
                                "Unexpected arithmetic operator in: " +
                                ctx.getText());
                        }
                    } else {
                        throw new QueryStateException(
                            "Unexpected arithmetic parse tree in: " +
                            ctx.getText());
                    }
                } else {
                    // This is the 0 + operation.
                    operations = "+" + operations;
                    args.add(op);
                }

            } else {
                throw new QueryStateException(
                    "Unexpected arithmetic parse tree in: " + ctx.getText());
            }
        }

        /* reverse order of arguments */
        Collections.reverse(args);

        FieldValueImpl ops =
            FieldDefImpl.Constants.stringDef.createString(operations);

        args.add(new ExprConst(theQCB, theInitSctx, loc, ops));

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }


    /*
     * multiply_expr : unary_expr ((STAR | DIV | RDIV) unary_expr)* ;
     */
    @Override
    public void exitMultiply_expr(KVQLParser.Multiply_exprContext ctx) {

        if (ctx.STAR().isEmpty() &&
            ctx.IDIV().isEmpty() &&
            ctx.RDIV().isEmpty()) {
            return;
        }

        Location loc = getLocation(ctx);

        Function func = theFuncLib.getFunc(FuncCode.OP_MULT_DIV);

        /*
         * operations stores, as a "*", "/", or "d" char, each operator that
         * appears in the multiply expr processed. The ops are stored in the
         * order of their appearance in the expr. This state will get saved in a
         * ConstExpr, which is added as an arg to the ExprFuncCall created here,
         * and will eventually be passed to the ArithOpIter.
         */
        String operations = "";
        /*
         * args contain the operands in reverse query order because this is how
         * they are on the stack. After the for-loop is done, the order will be
         * reversed to follow the order of the specified ops in the query. The
         * last arg contains the ConstExpr holding the operations string.
         */
        ArrayList<Expr> args = new ArrayList<Expr>(ctx.getChildCount()/2 + 2);

        for (int c = ctx.getChildCount() - 1; c >= 0; c = c - 2) {

            ParseTree pt = ctx.getChild(c);

            if (pt instanceof RuleNode) {

                Expr op = theExprs.pop();

                if (c > 0) {
                    ParseTree ptOp = ctx.getChild(c - 1);

                    if (ptOp instanceof TerminalNode) {
                        int tokenId =
                            ((TerminalNode)ptOp).getSymbol().getType();

                        if ( tokenId == KVQLParser.STAR) {
                            operations = "*" + operations;
                            args.add(op);
                        } else if (tokenId == KVQLParser.IDIV) {
                            operations = "/" + operations;
                            args.add(op);
                        } else if (tokenId == KVQLParser.RDIV) {
                            operations = "d" + operations;
                            args.add(op);
                        } else {
                            throw new QueryStateException(
                                "Unexpected arithmetic operator in: " +
                                ctx.getText());
                        }
                    } else {
                        throw new QueryStateException(
                            "Unexpected arithmetic parse tree in: " +
                            ctx.getText());
                    }
                } else {
                    // This is the 1 * operation.
                    operations = "*" + operations;
                    args.add(op);
                }

            } else {
                throw new QueryStateException(
                    "Unexpected arithmetic parse tree in: " + ctx.getText());
            }
        }

        /* reverse order of arguments to be in the query order */
        Collections.reverse(args);

        FieldValueImpl ops =
            FieldDefImpl.Constants.stringDef.createString(operations);

        args.add(new ExprConst(theQCB, theInitSctx, loc, ops));

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }

    /*
     * unary_expr : path_expr |
     *    (PLUS | MINUS) unary_expr ;
     */
    @Override
    public void exitUnary_expr(KVQLParser.Unary_exprContext ctx) {

        if ( ctx.MINUS() == null ) {
            return;
        }

        Location loc = getLocation(ctx);

        Function func = theFuncLib.getFunc(FuncCode.OP_ARITH_NEGATE);

        Expr op = theExprs.pop();

        if (op.getKind() == ExprKind.CONST) {

            FieldValueImpl val = ((ExprConst)op).getValue();

            if (!val.isNull()) {
                FieldValueImpl negVal =
                    ArithUnaryOpIter.getNegativeOfValue(val, getLocation(ctx));

                Expr expr = new ExprConst(theQCB, theInitSctx, loc, negVal);
                theExprs.push(expr);
                return;
            }
        }

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(op);

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, args);
        theExprs.push(expr);
    }

    /*
     * path_expr : primary_expr (map_step | array_step)* ;
     *
     * map_step : DOT ( map_filter_step | map_field_step );
     *
     * array_step : array_filter_step | array_slice_step;
     */
    @Override
    public void exitPath_expr(KVQLParser.Path_exprContext ctx) {

        Expr expr = theExprs.peek();

        if (!expr.isStepExpr()) {
            return;
        }

        if ((!ctx.map_step().isEmpty() || !ctx.array_step().isEmpty()) &&
            ctx.primary_expr().column_ref() != null &&
            ctx.primary_expr().column_ref().DOT() == null) {
            throw new QueryException(
                "The input to a path expression cannot be a direct column " +
                "reference. Use a table alias to access the column",
                getLocation(ctx));
        }

        /*
         * We must call IndexExpr.create() here so that when the type of the
         * path exprs is computed taking into account type declarations of
         * json index paths. This is needed for order-by: if the path expr
         * is an order-by expr, we must prevent wraping it with a promote
         * expr, so that the order-by expr will be matched against an index.
         */
        expr.getIndexExpr();
    }

    /*
     * map_field_step :
     *     ( id | string | var_ref | parenthesized_expr | func_call );
     */
    @Override
    public void enterMap_field_step(KVQLParser.Map_field_stepContext ctx) {

        Location loc = getLocation(ctx);

        Expr inputExpr = theExprs.pop();

        ExprVar ctxItemVar = null;

        ExprFieldStep step = new ExprFieldStep(theQCB, theInitSctx, loc,
                                               inputExpr);

        if (ctx.id() == null && ctx.string() == null) {

            ctxItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                     ExprVar.theCtxVarName, step);
            step.addCtxVars(ctxItemVar);

            pushScope();
            theSctx.addVariable(ctxItemVar);
        }

        theExprs.push(step);
    }

    @Override
    public void exitMap_field_step(KVQLParser.Map_field_stepContext ctx) {

        String fieldName = null;
        Expr fieldNameExpr = null;

        if (ctx.id() != null) {
            fieldName = ctx.id().getText();
        } else if (ctx.string() != null) {
            fieldName = stripFirstLast(ctx.string().getText());
            fieldName = EscapeUtil.inlineEscapedChars(fieldName);
        } else {
            fieldNameExpr = theExprs.pop();
        }

        ExprFieldStep step = (ExprFieldStep)theExprs.peek();

        step.addFieldNameExpr(fieldName, fieldNameExpr);

        if (ctx.id() == null && ctx.string() == null) {
            popScope();
        }
    }

    /*
     * map_filter_step : (KEYS | VALUES) LP expr? RP ;
     */
    @Override
    public void enterMap_filter_step(KVQLParser.Map_filter_stepContext ctx) {

        Location loc = getLocation(ctx);

        FilterKind kind = (ctx.KEYS() != null ?
                           FilterKind.KEYS : FilterKind.VALUES);

        if (kind ==  FilterKind.KEYS && theInUpdateClause) {
            throw new QueryException("keys() step is not allowed in the " +
                                     "target expression of a SET, ADD, or " +
                                     "PUT clause", loc);
        }

        Expr inputExpr = theExprs.pop();

        ExprMapFilter step = new ExprMapFilter(
            theQCB, theInitSctx, loc, kind, inputExpr);

        if (ctx.expr() != null) {
            ExprVar ctxItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                             ExprVar.theCtxVarName, step);

            ExprVar ctxElemVar = new ExprVar(theQCB, theInitSctx, loc,
                                             ExprVar.theValueVarName, step);

            ExprVar ctxKeyVar = new ExprVar(theQCB, theInitSctx, loc,
                                            ExprVar.theKeyVarName, step);

            step.addCtxVars(ctxItemVar, ctxElemVar, ctxKeyVar);

            pushScope();

            theSctx.addVariable(ctxItemVar);
            theSctx.addVariable(ctxElemVar);
            theSctx.addVariable(ctxKeyVar);
        }

        theExprs.push(step);
    }

    @Override
    public void exitMap_filter_step(KVQLParser.Map_filter_stepContext ctx) {

        ExprMapFilter filterStep;
        Expr predExpr = null;

        if (ctx.expr() != null) {
            predExpr = theExprs.pop();
            filterStep = (ExprMapFilter)theExprs.peek();
            filterStep.addPredExpr(predExpr);
            popScope();
        }
    }

    /*
     * array_slice_step : LBRACK expr? COLON expr? RBRACK ;
     */
    @Override
    public void enterArray_slice_step(KVQLParser.Array_slice_stepContext ctx) {

        Location loc = getLocation(ctx);

        Expr inputExpr = theExprs.pop();

        ExprVar ctxItemVar = null;

        ExprArraySlice step = new ExprArraySlice(theQCB, theInitSctx, loc,
                                               inputExpr);

        assert(ctx.COLON() != null);
        ctxItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                 ExprVar.theCtxVarName, step);
        step.addCtxVars(ctxItemVar);

        pushScope();
        theSctx.addVariable(ctxItemVar);

        theExprs.push(step);
    }

    @Override
    public void exitArray_slice_step(KVQLParser.Array_slice_stepContext ctx) {

        Expr lowExpr = null;
        Expr highExpr = null;

        List<KVQLParser.ExprContext> args = ctx.expr();

        if (args.size() == 2) {
            highExpr = theExprs.pop();
            lowExpr = theExprs.pop();

        } else  if (args.size() == 1) {

            if (ctx.getChild(1) instanceof KVQLParser.ExprContext) {
                lowExpr = theExprs.pop();
            } else {
                highExpr = theExprs.pop();
            }
        }

        ExprArraySlice step = (ExprArraySlice)theExprs.peek();

        step.addBoundaryExprs(lowExpr, highExpr);

        popScope();
    }

    /*
     * arrayt_filter_step : LBRACK expr RBRACK ;
     */
    @Override
    public void enterArray_filter_step(KVQLParser.Array_filter_stepContext ctx) {

        Location loc = getLocation(ctx);

        Expr inputExpr = theExprs.pop();

        ExprArrayFilter step = new ExprArrayFilter(
            theQCB, theInitSctx, loc, inputExpr);

        if (ctx.expr() != null) {
            ExprVar ctxItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                             ExprVar.theCtxVarName, step);

            ExprVar ctxElemVar = new ExprVar(theQCB, theInitSctx, loc,
                                             ExprVar.theElementVarName, step);

            ExprVar ctxElemPosVar = new ExprVar(theQCB, theInitSctx, loc,
                                                ExprVar.theElementPosVarName,
                                                step);

            step.addCtxVars(ctxItemVar, ctxElemVar, ctxElemPosVar);

            pushScope();

            theSctx.addVariable(ctxItemVar);
            theSctx.addVariable(ctxElemVar);
            theSctx.addVariable(ctxElemPosVar);
        }

        theExprs.push(step);
    }

    @Override
    public void exitArray_filter_step(KVQLParser.Array_filter_stepContext ctx) {

        ExprArrayFilter filterStep;
        Expr predExpr = null;

        if (ctx.expr() != null) {
            predExpr = theExprs.pop();
            filterStep = (ExprArrayFilter)theExprs.pop();
            filterStep.addPredExpr(predExpr);
            popScope();
        } else {
            filterStep = (ExprArrayFilter)theExprs.pop();
        }

        /* Convert to slice step, if possible */
        Expr expr = filterStep.convertToSliceStep();
        theExprs.push(expr);
    }

    /*
     * primary_expr : const_expr |
     *                column_ref |
     *                var_ref |
     *                array_constructor |
     *                map_constructor |
     *                transform_expr |
     *                func_call |
     *                count_star |
     *                count_distinct |
     *                collect |
     *                case_expr |
     *                parenthesized_expr ;
     */

    /*
     * array_constructor : LBRACK expr? (COMMA expr)* RBRACK ;
     */
    @Override
    public void enterArray_constructor(
        KVQLParser.Array_constructorContext ctx) {

        theExprs.push(null);
    }

    @Override
    public void exitArray_constructor(
        KVQLParser.Array_constructorContext ctx) {

        Location loc = getLocation(ctx);

        ArrayList<Expr> inputs = new ArrayList<Expr>();

        Expr input = theExprs.pop();
        while (input != null) {
            inputs.add(input);
            input = theExprs.pop();
        }

        Collections.reverse(inputs);

        Expr arrayConstr = new ExprArrayConstr(theQCB, theInitSctx, loc,
                                               inputs, false/*conditional*/);

        theExprs.push(arrayConstr);
    }

    /*
     * map_constructor :
     *     (LBRACE expr COLON expr (COMMA expr COLON expr)* RBRACE) |
     *     (LBRACE RBRACE) ;
     */
    @Override
    public void enterMap_constructor(
        KVQLParser.Map_constructorContext ctx) {

        theExprs.push(null);
    }

    @Override
    public void exitMap_constructor(
        KVQLParser.Map_constructorContext ctx) {

        Location loc = getLocation(ctx);

        ArrayList<Expr> inputs = new ArrayList<Expr>();

        Expr input = theExprs.pop();
        while (input != null) {
            inputs.add(input);
            input = theExprs.pop();
        }

        Collections.reverse(inputs);

        Expr mapConstr = new ExprMapConstr(theQCB, theInitSctx, loc, inputs);

        theExprs.push(mapConstr);
    }

    /*
     * transform_expr : seq_transform LP transform_input_expr COMMA expr RP ;
     *
     * transform_input_expr : expr ;
     *
     * The transform_input_expr rule may seem redundant, but we use it because
     * it gives us the opportunity (in the exitTransform_input_expr method
     * below) to create the context item var after the input expr has been
     * transalted and before the maper expr is translated.
     */
    @Override
    public void enterTransform_expr(KVQLParser.Transform_exprContext ctx) {

        Location loc = getLocation(ctx);

        ExprSeqMap seqMap = new ExprSeqMap(theQCB, theInitSctx, loc);
        theExprs.push(seqMap);
        theSeqTransformExprs.push(seqMap);
    }

    @Override
    public void exitTransform_input_expr(
        KVQLParser.Transform_input_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr inExpr = theExprs.pop();

        ExprSeqMap seqMap = (ExprSeqMap)theExprs.peek();
        seqMap.addInputExpr(inExpr);

        pushScope();

        String varName = "$sq" + theSeqTransformExprs.size();
        ExprVar ctxItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                         varName, seqMap);
        seqMap.addCtxVar(ctxItemVar);
        theSctx.addVariable(ctxItemVar);

        /* For backward compatibility, add the "$" var in the scope as well
         * and map it to ctxItemVar */
        theSctx.addVariable(ExprVar.theCtxVarName, ctxItemVar);
    }

    @Override
    public void exitTransform_expr(KVQLParser.Transform_exprContext ctx) {

        Expr mapExpr = theExprs.pop();
        ExprSeqMap seqMap = (ExprSeqMap)theExprs.peek();
        seqMap.addMapExpr(mapExpr);
        theSeqTransformExprs.pop();
        popScope();
    }

    /*
     * func_call : id LP (expr (COMMA expr)*)? RP ;
     */
    @Override
    public void enterFunc_call(KVQLParser.Func_callContext ctx) {

        Location loc = getLocation(ctx);

        Function func = theSctx.findFunction(ctx.id().getText());

        if (func == null) {
            throw new QueryException(
                "Could not find function with name " + ctx.id().getText());
        }

        if (func.getCode() == FuncCode.FN_SEQ_SORT) {

            int numParseChildren = ctx.getChildCount();
            List<KVQLParser.ExprContext> argCtxs = ctx.expr();
            ArrayList<Expr> args = new ArrayList<Expr>(4);

            for (int i = 0; i < argCtxs.size(); ++i) {

                KVQLParser.ExprContext argCtx = argCtxs.get(i);
                theWalker.walk(this, argCtx);
                Expr arg = theExprs.pop();
                args.add(arg);

                if (i == 0 && argCtxs.size() > 1) {
                    ExprVar ctxItemVar = new ExprVar(theQCB, theSctx, loc,
                                                     ExprVar.theCtxVarName, arg);
                    args.add(ctxItemVar);
                    pushScope();
                    theSctx.addVariable(ctxItemVar);
                }
            }

            popScope();
            Expr expr = ExprFuncCall.create(theQCB, theSctx, loc, func, args);
            theExprs.push(expr);

            /* The children of this parse node have been processed already. To
             * avoid reprocessing them, we remove all the children here. */
            while (numParseChildren > 0) {
                ctx.removeLastChild();
                --numParseChildren;
            }

            return;
        }

        /*
         * If function is aggregate, push it into theAggrFunctions. This is
         * done to check that aggregate functions are not nested.
         */
        if (func.isAggregate()) {

            if (!theAggrFunctions.isEmpty()) {
                throw new QueryException(
                    "Aggregate functions cannot be nested", loc);
            }

            if (theSFWExprs.isEmpty() ||
                (theSFWExprs.size() != theInSelectClause.size() &&
                 theSFWExprs.size() != theInOrderByClause.size())) {
                throw new QueryException(
                    "Aggregate functions can appear in SELECT or " +
                    "ORDER BY clauses only", loc);
            }

            theAggrFunctions.push(func);
        }

        theExprs.push(null);
    }

    @Override
    public void exitFunc_call(KVQLParser.Func_callContext ctx) {

        if (ctx.id() == null) {
            return;
        }

        Location loc = getLocation(ctx);

        ArrayList<Expr> inputs = new ArrayList<Expr>();

        Expr input = theExprs.pop();

        while (input != null) {
            inputs.add(input);
            input = theExprs.pop();
        }

        Collections.reverse(inputs);

        Function func = theSctx.findFunction(ctx.id().getText(), inputs.size());

        if (func == null) {
            throw new QueryException(
                "Could not find function with name " + ctx.id().getText() +
                " and arity " + inputs.size(), loc);
        }

        if (func.isAggregate()) {

            theAggrFunctions.pop();

            ExprSFW sfw = theSFWExprs.peek();
            sfw.addEmptyGroupBy();
        }

        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func, inputs);

        if (func.getCode() == FuncCode.FN_GEO_NEAR) {

            if (theSFWExprs.size() != theInWhereClause.size()) {
                throw new QueryException(
                    "The geo_near function can appear in WHERE clause only",
                    loc);
            }

            Expr prevNear = theNearExprs.peek();
            if (prevNear != null) {
                throw new QueryException(
                    "There can be at most one geo_near function " +
                    "in the WHERE clause",
                    loc);
            }

            theNearExprs.set(theNearExprs.size() - 1, expr);
        }

        theExprs.push(expr);
    }

    @Override
    public void enterCollect(KVQLParser.CollectContext ctx) {

        Location loc = getLocation(ctx);

        if (!theAggrFunctions.isEmpty()) {
            throw new QueryException(
                "Aggregate functions cannot be nested", loc);
        }

        if (theSFWExprs.isEmpty() ||
            theSFWExprs.size() != theInSelectClause.size()) {
            throw new QueryException(
                "The collect aggregate function can appear in SELECT clause only",
                loc);
        }

        Function func;
        if (ctx.DISTINCT() != null) {
            func = theSctx.findFunction("array_collect_distinct");
        } else {
            func = theSctx.findFunction("array_collect");
        }
        theAggrFunctions.push(func);
    }

    @Override
    public void exitCollect(KVQLParser.CollectContext ctx) {

        Location loc = getLocation(ctx);

        Function func = theAggrFunctions.pop();
        Expr input = theExprs.pop();
        Expr collectExpr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                               func, input);
        ExprSFW sfw = theSFWExprs.peek();
        sfw.addEmptyGroupBy();

        theExprs.push(collectExpr);
    }

    @Override
    public void enterCount_distinct(KVQLParser.Count_distinctContext ctx) {

        Location loc = getLocation(ctx);

        if (!theAggrFunctions.isEmpty()) {
            throw new QueryException(
                "Aggregate functions cannot be nested", loc);
        }

        if (theSFWExprs.isEmpty() ||
            (theSFWExprs.size() != theInSelectClause.size() &&
             theSFWExprs.size() != theInOrderByClause.size())) {
            throw new QueryException(
                "Aggregate functions can appear in the SELECT or ORDER BY clauses only",
                loc);
        }

        Function func = theSctx.findFunction("array_collect_distinct");
        theAggrFunctions.push(func);
    }

    @Override
    public void exitCount_distinct(KVQLParser.Count_distinctContext ctx) {

       Location loc = getLocation(ctx);

       Function func = theAggrFunctions.pop();
       Expr input = theExprs.pop();
       Expr collectExpr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                              func, input);

       Function sizeFunc = theSctx.findFunction("size", 1);
       Expr sizeExpr = ExprFuncCall.create(theQCB, theInitSctx, loc,
                                           sizeFunc, collectExpr);
       ExprSFW sfw = theSFWExprs.peek();
       sfw.addEmptyGroupBy();

       theExprs.push(sizeExpr);
    }

    @Override
    public void enterCount_star(KVQLParser.Count_starContext ctx) {

        Location loc = getLocation(ctx);

        if (!theAggrFunctions.isEmpty()) {
            throw new QueryException(
                "Aggregate functions cannot be nested", loc);
        }

        if (theSFWExprs.isEmpty() ||
            (theSFWExprs.size() != theInSelectClause.size() &&
             theSFWExprs.size() != theInOrderByClause.size())) {
            throw new QueryException(
                "Aggregate functions can appear in the SELECT or ORDER BY clauses only",
                loc);
        }

        ExprSFW sfw = theSFWExprs.peek();
        sfw.addEmptyGroupBy();

        Function func = theSctx.findFunction("count(*)", 0);
        Expr expr = ExprFuncCall.create(theQCB, theInitSctx, loc, func);
        theExprs.push(expr);
    }

    /*
     * var_ref : (DOLLAR DOLLAR? id) | (DOLLAR DOLLAR) ;
     */
    @Override
    public void exitVar_ref(KVQLParser.Var_refContext ctx) {

        Location loc = getLocation(ctx);
        ExprVar varExpr;
        String varName;

        if (ctx.QUESTION_MARK() != null) {

            varName = "$$" + theExternalVarsCounter;

            varExpr = new ExprVar(theQCB, theInitSctx, loc,
                                  varName, FieldDefImpl.Constants.anyDef,
                                  theExternalVarsCounter++,
                                  false);

            theInitSctx.addVariable(varExpr);
            theExprs.push(varExpr);
            theQuestionVars.add(varExpr);

            if (theSFWExprs.size() == theInSelectClause.size()) {
                ++theNumQuestionVarsInSelect;
            }

            return;
        }

        varName = ctx.getText();
        varExpr = theScopes.peek().findVariable(varName);

        if (varExpr == null) {

            if (!theAllowUndeclaredVars) {
                throw new QueryException(
                    " Unknown variable " + varName, getLocation(ctx));
            }

            varExpr = new ExprVar(theQCB, theInitSctx, loc,
                                  varName, FieldDefImpl.Constants.anyDef,
                                  theExternalVarsCounter++,
                                  false);
            theInitSctx.addVariable(varExpr);
        }

        theExprs.push(varExpr);
    }

    /*
     * column_ref : id (DOT (id | string))? ;
     *
     * If there are 2 ids, the first one refers to a table alias and the
     * second to a column in that table. A single id refers to a column in some
     * of the tables in the FROM clause. If more than one table has a column of
     * that name, an error is thrown. In this case, the user has to rewrite the
     * query to use table aliases to resolve the ambiguity.
     */
    @Override
    public void exitColumn_ref(KVQLParser.Column_refContext ctx) {

        Location loc = getLocation(ctx);

        List<KVQLParser.IdContext> ids = ctx.id();

        String colName = null;
        String tableAlias = null;
        String tableName = null;
        boolean found = false;
        int collectionTablePos = -1;
        int numCollectionTables = 0;

        if (ctx.DOT() == null) {

            /* there's a single id */

            colName = ids.get(0).getText();

            for (int i = 0; i < theTables.size(); ++i) {

                TableImpl table = theTables.get(i);

                if (table.isJsonCollection()) {
                    ++numCollectionTables;
                    collectionTablePos = i;
                }

                if (table.getField(colName) != null) {
                    if (found) {
                        throw new QueryException(
                            "The reference to column " + colName +
                            " is ambiguous because more than one table has " +
                            "a column with this name", loc);
                    }

                    found = true;
                    tableAlias = theTableAliases.get(i);
                    tableName = table.getFullName();
                }
            }

            if (!found && numCollectionTables > 0) {
                if (numCollectionTables > 1) {
                    throw new QueryException(
                        "The reference to column " + colName +
                        " is ambiguous because more than one table may " +
                        "have a column with this name", loc);
                }

                tableAlias = theTableAliases.get(collectionTablePos);
                tableName = theTables.get(collectionTablePos).getFullName();
            }
        } else {
            assert(ids.size() == 2 || (ids.size() == 1 && ctx.string() != null));

            if (ctx.string() != null) {
                colName = EscapeUtil.inlineEscapedChars(
                    stripFirstLast(ctx.string().getText()));
            }

            if (ids.size() == 2) {
                assert(colName == null);
                colName = ids.get(1).getText();
            }

            tableAlias = ids.get(0).getText();
            boolean foundAlias = false;

            for (int i = 0; i < theTables.size(); ++i) {

                TableImpl table = theTables.get(i);

                if (table.isJsonCollection()) {
                    ++numCollectionTables;
                    collectionTablePos = i;
                }

                if (theTableAliases.get(i).equals(tableAlias)) {
                    foundAlias = true;
                    tableName = table.getFullName();
                    if (table.getField(colName) != null) {
                        found = true;
                        break;
                    }
                }
            }

            if (!foundAlias) {
                throw new QueryException(
                    "No table in the FROM clause has an alias named " +
                    tableAlias, loc);
            }
        }

        if (!found && numCollectionTables == 0) {
            if (tableName != null) {
                throw new QueryException(
                    "Table " + tableName + " has no column named " +
                    colName, loc);
            }
            throw new QueryException(
                "No table in the FROM clause has a column named " +
                colName, loc);
        }

        /*
         * tableAlias cannot be null; this check is here to eliminate a
         * compiler warning
         */
        if (tableAlias == null) {
            throw new QueryStateException("No tables in FROM clause!");
        }

        String varName = (tableAlias.charAt(0) == '$' ?
                          tableAlias :
                          ("$$" + tableAlias));

        ExprVar varExpr = theScopes.peek().findVariable(varName);

        if (varExpr == null) {
            throw new QueryException(
                "Table alias " + tableAlias + " cannot be referenced at " +
                "this location", loc);
        }

        ExprFieldStep expr = new ExprFieldStep(theQCB, theInitSctx, loc,
                                               varExpr,
                                               colName);

        theExprs.push(expr);
    }

    /*
     * const_expr : INT | FLOAT | string | TRUE | FALSE | NULL;
     */
    @Override
    public void exitConst_expr(KVQLParser.Const_exprContext ctx) {

        Location loc = getLocation(ctx);

        FieldValue value = null;

        try {
            if (ctx.number() != null ) {
                String numberStr = ctx.number().getText();
                BigDecimal bdval = new BigDecimal(stripNumericLetter(numberStr));
                if (ctx.number().INT() != null) {
                    try {
                        long lval = bdval.longValueExact();
                        if (Integer.MIN_VALUE <= lval &&
                            lval <= Integer.MAX_VALUE) {
                            value = FieldDefImpl.Constants.integerDef
                                        .createInteger((int)lval);
                        } else {
                            value = FieldDefImpl.Constants.longDef
                                        .createLong(lval);
                        }
                    } catch (ArithmeticException ae) {
                        /* overflow, will create NumberValue later */
                    }
                } else if (ctx.number().FLOAT() != null) {
                    /* Create double value if no precision loss */
                    double dval = bdval.doubleValue();
                    if (Double.isFinite(dval) &&
                        bdval.compareTo(BigDecimal.valueOf(dval)) == 0) {
                        value = FieldDefImpl.Constants
                                    .doubleDef.createDouble(dval);
                    }
                }

                /* Create NumberValue if not fit long or double */
                if (value == null) {
                    value = FieldDefImpl.Constants.numberDef.createNumber(bdval);
                }
            } else if (ctx.TRUE() != null) {
                Boolean val = Boolean.parseBoolean(ctx.TRUE().getText());
                value = FieldDefImpl.Constants.booleanDef.createBoolean(val);
            } else if (ctx.FALSE() != null) {
                Boolean val = Boolean.parseBoolean(ctx.FALSE().getText());
                value = FieldDefImpl.Constants.booleanDef.createBoolean(val);
            } else if (ctx.NULL() != null) {
                value = NullJsonValueImpl.getInstance();
            } else {
                String val = stripFirstLast(ctx.string().getText());
                val = EscapeUtil.inlineEscapedChars(val);
                value = FieldDefImpl.Constants.stringDef.createString(val);
            }
        } catch (NumberFormatException nfe) {
            throw new QueryException(
                "Invalid numeric literal: " + ctx.getText(), loc);
        }

        ExprConst constExpr = new ExprConst(
            theQCB, theInitSctx, loc, (FieldValueImpl)value);

        theExprs.push(constExpr);
    }

    /* For numbers that end with 'n' or 'N' letter strip it out.
     */
    private static String stripNumericLetter(String numberString) {
        // this can end with 'n'
        char lastChar = numberString.charAt(numberString.length() - 1);
        if (lastChar == 'n' || lastChar == 'N') {
            numberString = numberString.substring(0, numberString
                .length() - 1);
        }
        return numberString;
    }

    /*
     * insert_statement :
     *     prolog?
     *     INSERT INTO table_name (LP id (COMMA id)* RP)?
     *     VALUES insert_clause (COMMA insert_clause)*
     *     returning_clause? ;
     *
     * insert_clause : expr | DEFAULT;
     *
     * returning_clause : RETURNING select_list ;
     */
    @Override
    public void enterInsert_statement(KVQLParser.Insert_statementContext ctx) {

        pushScope();

        theAllowUndeclaredVars = true;

        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String[] mappedTableName = tableName;
        Location loc = getLocation(ctx);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.INSERT);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        if (theQCB.getPrepareCallback() != null) {
            mappedTableName = mapTableNames(theQCB.getPrepareCallback(), tableName);
            namespace = theQCB.getPrepareCallback().mapNamespaceName(namespace);
        }

        TableImpl table = getTable(namespace, mappedTableName, loc);

        if (table == null) {
            throw new QueryException(
               "Table " + NameUtils.makeQualifiedName(namespace,
                   concatPathName(tableName)) + " does not exist", loc);
        }

        String tableAlias = (ctx.tab_alias() == null ?
                             concatPathName(tableName) :
                             ctx.tab_alias().getText());

        theTables.add(table);
        theTableAliases.add(tableAlias);

        theQCB.setTables(theTables);

        RecordDefImpl rowDef = table.getRowDef();

        ArrayList<Integer> colPositions = null;
        ArrayList<String> topFieldNames = null;
        boolean jsonCollection = table.isJsonCollection();

        List<KVQLParser.Insert_labelContext> labelCtxList = ctx.insert_label();

        if (labelCtxList != null && !labelCtxList.isEmpty()) {

            colPositions = new ArrayList<Integer>(labelCtxList.size());

            for (int i = 0; i < labelCtxList.size(); ++i) {

                KVQLParser.Insert_labelContext ilctx = labelCtxList.get(i);

                String name = (ilctx.id() != null ? ilctx.id().getText() :
                               EscapeUtil.inlineEscapedChars(
                                   stripFirstLast(ilctx.string().getText())));

                if (jsonCollection) {
                    if (topFieldNames == null) {
                        topFieldNames = new ArrayList<String>(labelCtxList.size());
                    }
                    if (table.isKeyComponent(name)) {
                        colPositions.add(rowDef.getFieldPos(name));
                    } else {
                       colPositions.add(-1);
                    }
                    topFieldNames.add(name);
                } else {
                    try {
                        colPositions.add(rowDef.getFieldPos(name));
                    } catch (IllegalArgumentException e) {
                        throw new QueryException(
                            "Table " + table.getFullName() +
                            " does not have a column named " + name,
                            getLocation(ilctx));
                    }
                }
            }
        }

        Expr ins = new ExprInsertRow(theQCB, theInitSctx, loc,
                                     table, colPositions, topFieldNames,
                                     ctx.UPSERT() != null,
                                     ctx.insert_returning_clause() != null);
        theExprs.push(ins);
    }

    @Override
    public void exitInsert_statement(KVQLParser.Insert_statementContext ctx) {

        theRootExpr = theExprs.pop();

        if (theRootExpr.getKind() == ExprKind.INSERT_ROW) {
            ((ExprInsertRow)theRootExpr).validate();
        }

        assert(theRootExpr != null);
        assert(theExprs.isEmpty());
        assert(theColNames.isEmpty());
        assert(theTypes.isEmpty());

        theAllowUndeclaredVars = false;
        popScope();
    }

    @Override
    public void exitInsert_clause(KVQLParser.Insert_clauseContext ctx) {

        if (ctx.DEFAULT() == null) {
            Expr valExpr = theExprs.pop();
            ExprInsertRow ins = (ExprInsertRow)theExprs.peek();
            ins.addInsertClause(valExpr, getLocation(ctx));
        } else  {
            ExprInsertRow ins = (ExprInsertRow)theExprs.peek();
            ins.addInsertClause(null, getLocation(ctx));
        }
    }

    @Override
    public void exitInsert_ttl_clause(KVQLParser.Insert_ttl_clauseContext ctx) {

        Expr ttlExpr = null;
        UpdateKind ttlKind;
        ExprInsertRow ins;

        if (ctx.USING() != null) {
            ins = (ExprInsertRow)theExprs.peek();
            ttlKind = UpdateKind.TTL_TABLE;
        } else {
            ttlExpr = theExprs.pop();
            ttlExpr = ExprPromote.create(null, ttlExpr, TypeManager.NUMBER_QSTN());

            ins = (ExprInsertRow)theExprs.peek();

            if (ctx.HOURS() != null) {
                ttlKind = UpdateKind.TTL_HOURS;
            } else {
                ttlKind = UpdateKind.TTL_DAYS;
            }
        }

        ins.addTTLClause(ttlExpr, ttlKind);
    }

    @Override
    public void enterInsert_returning_clause(
        KVQLParser.Insert_returning_clauseContext ctx) {

        Location loc = getLocation(ctx);

        ExprInsertRow ins = (ExprInsertRow)theExprs.pop();

        ins.validate();

        ExprSFW returningSFW = new ExprSFW(theQCB, theInitSctx, loc);

        TableImpl table = ins.getTable();

        String varName =
            ExprVar.createVarNameFromTableAlias(theTableAliases.get(0));

        ExprVar var = returningSFW.createTableVar(ins, table, varName);

        theSctx.addVariable(var);
        theExprs.push(returningSFW);
    }

    /*
     * delete_statement :
     *     prolog?
     *     DELETE FROM table_name (AS? tab_alias)? (WHERE expr)?
     *     delete_returning_clause? ;
     *
     * delete_returning_clause : RETURNING select_list ;
     */
    @Override
    public void enterDelete_statement(KVQLParser.Delete_statementContext ctx) {

        pushScope();

        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String[] mappedTableName = tableName;
        Location loc = getLocation(ctx.table_name());

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DELETE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        if (theQCB.getPrepareCallback() != null) {
            mappedTableName = mapTableNames(theQCB.getPrepareCallback(), tableName);
            namespace = theQCB.getPrepareCallback().mapNamespaceName(namespace);
        }

        TableImpl table = getTable(namespace, mappedTableName, loc);

        if (table == null) {
            throw new QueryException(
               "Table " + NameUtils.makeQualifiedName(namespace,
                   concatPathName(tableName)) + " does not exist", loc);
        }

        String tableAlias = (ctx.tab_alias() == null ?
                             concatPathName(tableName) :
                             ctx.tab_alias().getText());

        theTables.add(table);
        theTableAliases.add(tableAlias);

        theQCB.setTables(theTables);

        /*
         * Create an SFW expr to scan the table, apply the WHERE clause, and
         * return the primary key columns of the qualifying rows, as well as
         * any additional rows required by the RETURNING clause, if any.
         */
        String varName = ExprVar.createVarNameFromTableAlias(tableAlias);

        ExprBaseTable tableExpr = new ExprBaseTable(theQCB, theInitSctx, loc);
        tableExpr.addTable(table, tableAlias, false, false, loc);
        tableExpr.finalizeTables();

        /* Make sure that the row will be write-locked when accessed */
        tableExpr.setIsDelete();

        ExprSFW sfw = new ExprSFW(theQCB, theInitSctx, loc);
        ExprVar var = sfw.createTableVar(tableExpr, table, varName);

        theSctx.addVariable(var);

        /*
         * If there is no RETURNING clause, add the primary key columns to the
         * select list of the above SFW, and create the update-row expr, with
         * this SFW as its input.
         */
        if (ctx.delete_returning_clause() == null) {

            sfw.addPrimKeyToSelect(null, true);

            Expr del = new ExprDeleteRow(theQCB, theInitSctx, loc,sfw,
                                         false/*hasReturningCaluse*/);

            theExprs.push(del);
        } else {
            theExprs.push(sfw);
        }
    }

    @Override
    public void exitDelete_statement(KVQLParser.Delete_statementContext ctx) {

        popScope();

        if (ctx.delete_returning_clause() == null) {

            Expr whereCond = null;
            if (ctx.WHERE() != null) {
                whereCond = theExprs.pop();
            }

            ExprDeleteRow del = (ExprDeleteRow)theExprs.pop();
            ExprSFW inputSFW = (ExprSFW)del.getInput();

            if (whereCond != null) {
                inputSFW.addWhereClause(whereCond);
            }

            theRootExpr = del;
            return;
        }

        Location loc = getLocation(ctx);

        ExprSFW inputSFW = (ExprSFW)theExprs.pop();

        int numFields = inputSFW.getNumFields();

        int[] primKeyPositions = inputSFW.addPrimKeyToSelect(null, true);

        ExprDeleteRow del = new ExprDeleteRow(theQCB, theInitSctx, loc,
                                              inputSFW,
                                              true/*hasReturningClause*/);

        del.addPrimKeyPositions(primKeyPositions);

        if (numFields == inputSFW.getNumFields()) {
            theRootExpr = del;
            return;
        }

        /*
         * Create a new SFW on top of the delete-row expr. This SFW will
         * simply propagate the records produced by the input SFW after
         * projecting out any prim-key columns added by the addPrimKeyToSelect()
         * call above.
         */
        pushScope();

        ExprSFW returningSFW = new ExprSFW(theQCB, theInitSctx, loc);

        ExprVar tableVar = inputSFW.getFromClause(0).getTargetTableVar();
        String varName = tableVar.getName();
        ExprVar var = returningSFW.createFromVar(del, varName);

        theSctx.addVariable(var);

        for (int i = 0; i < numFields; ++i) {

            Expr inField = inputSFW.getFieldExpr(i);
            Expr outField = new ExprFieldStep(theQCB, theInitSctx,
                                              inField.getLocation(),
                                              var, i);
            returningSFW.addField(inputSFW.getFieldName(i), outField);
        }

        returningSFW.setIsSelectStar(inputSFW.isSelectStar());

        theExprs.push(returningSFW);
        theRootExpr = returningSFW;

        popScope();
    }

    @Override
    public void enterDelete_returning_clause(
        KVQLParser.Delete_returning_clauseContext ctx) {

        Expr e = theExprs.peek();

        if (e.getKind() == ExprKind.SFW) {
            return;
        }

        Expr whereCond = theExprs.pop();
        ExprSFW sfw = (ExprSFW)theExprs.peek();

        sfw.addWhereClause(whereCond);
    }

    /*
     * update_statement :
     *     prolog?
     *     UPDATE table_name AS? tab_alias? update_clause (COMMA update_clause)*
     *     WHERE expr
     *     returning_clause? ;
     *
     * returning_clause : RETURNING select_list
     *
     * update_clause :
     *     (SET set_clause (COMMA set_clause)*) |
     *     (ADD add_clause (COMMA add_clause)*) |
     *     (PUT put_clause (COMMA put_clause)*) |
     *     (REMOVE remove_clause (COMMA remove_clause)*) |
     *     (JSON MERGE json_merge_path_clause (COMMA (update_clause |
     *                                               json_merge_path_clause))*)
     *     (SET TTL ttl_clause (COMMA update_clause)*) ;
     *
     * set_clause : target_expr EQ expr ;
     *
     * add_clause : target_expr pos_expr? expr ;
     *
     * put_clause : target_expr expr ;
     *
     * remove_clause : target_expr ;
     *
     * json_merge_path_clause : target_expr WITH PATCH json_patch_expr ;
     *
     * json_patch_expr : map_constructor | array_constructor | const_expr | var_ref ;
     *
     * ttl_clause : (add_expr (HOURS | DAYS)) | (USING TABLE TTL) ;
     *
     * target_expr : path_expr ;
     *
     * pos_expr : add_expr ;
     *
     * See javadoc for ExprUpdateRow for implementation details.
     */
    @Override
    public void enterUpdate_statement(KVQLParser.Update_statementContext ctx) {

        pushScope();
        theAllowUndeclaredVars = true;

        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String[] mappedTableName = tableName;
        Location loc = getLocation(ctx.table_name());

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.UPDATE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        if (theQCB.getPrepareCallback() != null) {
            mappedTableName = mapTableNames(theQCB.getPrepareCallback(), tableName);
            namespace = theQCB.getPrepareCallback().mapNamespaceName(namespace);
        }

        TableImpl table = getTable(namespace, mappedTableName, loc);

        if (table == null) {
            throw new QueryException(
               "Table " + NameUtils.makeQualifiedName(namespace,
                   concatPathName(tableName)) + " does not exist", loc);
        }

        String tableAlias = (ctx.tab_alias() == null ?
                             concatPathName(tableName) :
                             ctx.tab_alias().getText());

        theTables.add(table);
        theTableAliases.add(tableAlias);

        theQCB.setTables(theTables);

        /*
         * Create an SFW expr to scan the table, apply the WHERE clause, and
         * return the qualifying rows.
         */
        String varName = ExprVar.createVarNameFromTableAlias(tableAlias);

        ExprBaseTable tableExpr = new ExprBaseTable(theQCB, theInitSctx, loc);
        tableExpr.addTable(table, tableAlias, false, false, loc);
        tableExpr.finalizeTables();

        /* Force access by primary index */
        tableExpr.addIndexHint(null, true, loc);

        /* Make sure that the row will be write-locked when accessed */
        tableExpr.setIsUpdate();

        ExprSFW sfw = new ExprSFW(theQCB, theInitSctx, loc);
        ExprVar var = sfw.createTableVar(tableExpr, table, varName);

        theSctx.addVariable(var);

        ArrayList<Expr> colExprs = new ArrayList<Expr>(1);
        ArrayList<String> colNames = new ArrayList<String>(1);
        colExprs.add(var);
        String varname = var.getName();
        if (varname.startsWith("$$")) {
            colNames.add(var.getName().substring(2));
        } else {
            colNames.add(var.getName().substring(1));
        }

        sfw.addSelectClause(colNames, colExprs);
        sfw.setIsSelectStar(true);

        /*
         * Now create the update-row expr, with the SFW created above as input.
         */
        Expr upd = new ExprUpdateRow(theQCB, theInitSctx, loc, sfw, table,
                                     ctx.update_returning_clause() != null);

        theExprs.push(upd);
        theExprs.push(null);
    }

    @Override
    public void exitUpdate_statement(KVQLParser.Update_statementContext ctx) {

        if (ctx.update_returning_clause() == null) {
            addWhereAndUpdateClauses();
        }

        popScope();
        theAllowUndeclaredVars = false;

        theRootExpr = theExprs.pop();

        assert(theRootExpr != null);
        assert(theExprs.isEmpty());
        assert(theColNames.isEmpty());
        assert(theTypes.isEmpty());
    }

    @Override
    public void enterUpdate_returning_clause(
        KVQLParser.Update_returning_clauseContext ctx) {

        Location loc = getLocation(ctx);

        /*
         * Fill-up the update-row expr with the update clauses, add the
         * WHERE clause to the input SFW, and pop the scope to make the
         * FROM var(s) of the input SFW invisible.
         */
        addWhereAndUpdateClauses();
        popScope();

        /*
         * Create a new SFW on top of the update-row expr. This SFW will
         * evaluate exprs in the RETURNING clause the construct the final
         * query resutls.
         */
        pushScope();

        ExprUpdateRow upd = (ExprUpdateRow)theExprs.pop();

        ExprSFW inputSFW = (ExprSFW)upd.getInput();
        ExprSFW returningSFW = new ExprSFW(theQCB, theInitSctx, loc);

        String varName = inputSFW.getFromClause(0).getTargetTableVar().getName();
        TableImpl table = upd.getTable();
        ExprVar var = returningSFW.createTableVar(upd, table, varName);

        theSctx.addVariable(var);
        theExprs.push(returningSFW);
    }

    private void addWhereAndUpdateClauses() {

        ArrayList<Expr> clauses = new ArrayList<Expr>();
        Expr ttlClause = null;
        Expr whereCond = theExprs.pop();

        ExprUpdateField clause = (ExprUpdateField)theExprs.pop();

        while (clause != null) {
            if (clause.isTTLUpdate()) {
                if (ttlClause == null) {
                    ttlClause = clause;
                }
            } else {
                clauses.add(clause);
            }

            clause = (ExprUpdateField)theExprs.pop();
        }

        Collections.reverse(clauses);
        if (ttlClause != null) {
            clauses.add(ttlClause);
        }

        ExprUpdateRow updExpr = (ExprUpdateRow)theExprs.peek();

        updExpr.addUpdateClauses(clauses, (ttlClause != null));

        ExprSFW inputSFW = (ExprSFW)updExpr.getInput();
        inputSFW.addWhereClause(whereCond);
    }

    @Override
    public void exitTtl_clause(KVQLParser.Ttl_clauseContext ctx) {

        Location loc = getLocation(ctx);
        Expr ttlExpr = null;

        ExprUpdateField upd;

        if (ctx.USING() != null) {
            upd = new ExprUpdateField(theQCB, theInitSctx, loc, ttlExpr, -1);
            upd.setUpdateKind(UpdateKind.TTL_TABLE);
        } else {
            ttlExpr = theExprs.pop();
            ttlExpr = ExprPromote.create(null, ttlExpr, TypeManager.NUMBER_QSTN());
            upd = new ExprUpdateField(theQCB, theInitSctx, loc, ttlExpr, -1);

            if (ctx.HOURS() != null) {
                upd.setUpdateKind(UpdateKind.TTL_HOURS);
            } else {
                upd.setUpdateKind(UpdateKind.TTL_DAYS);
            }
        }

        theExprs.push(upd);
    }

    @Override
    public void enterSet_clause(KVQLParser.Set_clauseContext ctx) {

        /* For the $ var that may be referenced by the new-value expr */
        pushScope();
        theInUpdateClause = true;
    }

    @Override
    public void exitSet_clause(KVQLParser.Set_clauseContext ctx) {

        Expr valueExpr = theExprs.pop();
        ExprUpdateField upd = (ExprUpdateField)theExprs.peek();
        upd.setUpdateKind(UpdateKind.SET);
        upd.addNewValueExpr(valueExpr);
        upd.removeTargetItemVar();

        popScope();
        theInUpdateClause = false;
    }

    @Override
    public void enterAdd_clause(KVQLParser.Add_clauseContext ctx) {

        /* For the $ var that may be referenced by the new-value expr */
        pushScope();
        theInUpdateClause = true;
    }

    @Override
    public void exitAdd_clause(KVQLParser.Add_clauseContext ctx) {

        Expr valueExpr = theExprs.pop();
        Expr posExpr = null;

        if (ctx.pos_expr() != null) {
            posExpr = theExprs.pop();
        }

        ExprUpdateField upd = (ExprUpdateField)theExprs.peek();

        upd.setUpdateKind(UpdateKind.ADD);
        upd.addNewValueExpr(valueExpr);
        if (posExpr != null) {
            upd.addPosExpr(posExpr);
        }
        upd.removeTargetItemVar();

        popScope();
        theInUpdateClause = false;
    }

    @Override
    public void enterPut_clause(KVQLParser.Put_clauseContext ctx) {

        /* For the $ var that may be referenced by the new-value expr */
        pushScope();
        theInUpdateClause = true;
    }

    @Override
    public void exitPut_clause(KVQLParser.Put_clauseContext ctx) {

        Expr valueExpr = theExprs.pop();
        ExprUpdateField upd = (ExprUpdateField)theExprs.peek();

        upd.setUpdateKind(UpdateKind.PUT);
        upd.addNewValueExpr(valueExpr);
        upd.removeTargetItemVar();

        popScope();
        theInUpdateClause = false;
    }

    @Override
    public void enterRemove_clause(KVQLParser.Remove_clauseContext ctx) {

        /*
         * There is no $ var in this case, but exitTarget_expr() will create
         * the var anyway, because it doesn't know what kind of update clause
         * is being translated. So, we have to add a scope to avoid any conflict
         * with another $ var in the current scope.
         */
        pushScope();
    }

    @Override
    public void exitRemove_clause(KVQLParser.Remove_clauseContext ctx) {

        ExprUpdateField upd = (ExprUpdateField)theExprs.peek();
        upd.setUpdateKind(UpdateKind.REMOVE);
        upd.removeTargetItemVar();

        popScope();
    }

    @Override
    public void enterJson_merge_patch_clause(
        KVQLParser.Json_merge_patch_clauseContext ctx) {

        /*
         * There is no $ var in this case, but exitTarget_expr() will create
         * the var anyway, because it doesn't know what kind of update clause
         * is being translated. So, we have to add a scope to avoid any conflict
         * with another $ var in the current scope.
         */
        pushScope();
    }

    @Override
    public void exitJson_merge_patch_clause(
        KVQLParser.Json_merge_patch_clauseContext ctx) {

        Expr patchExpr = theExprs.pop();
        ExprUpdateField upd = (ExprUpdateField)theExprs.peek();

        upd.setUpdateKind(UpdateKind.JSON_MERGE_PATCH);
        upd.addNewValueExpr(patchExpr);
        upd.removeTargetItemVar();

        popScope();
    }

    @Override
    public void exitTarget_expr(KVQLParser.Target_exprContext ctx) {

        Location loc = getLocation(ctx);

        Expr path = theExprs.pop();
        Expr step = path;
        Expr firstStep = null;
        int numSteps = 0;

        TableImpl targetTable = theTables.get(0);

        while (step != null) {

            if (step.isStepExpr()) {
                firstStep = step;
                step = step.getInput();
                ++numSteps;
                continue;
            }

            if (step.getKind() == ExprKind.VAR) {
                ExprVar var = (ExprVar)step;
                TableImpl table = var.getTable();

                if (table != null && table.getId() == targetTable.getId()) {
                    break;
                }
            }

            throw new QueryException(
                "Target expression in update clause is not a path " +
                "expression over a row of table " +
                targetTable.getFullName(), loc);
        }

        if (targetTable.isJsonCollection()) {
            if (firstStep != null &&
                firstStep.getKind() != ExprKind.FIELD_STEP) {
                throw new QueryException(
                    "Target expression in update clause does not start " +
                    "with a top-level field", loc);
            }
        } else {
            if (firstStep == null ||
                firstStep.getKind() != ExprKind.FIELD_STEP ||
                ((ExprFieldStep)firstStep).getFieldPos() < 0) {
                throw new QueryException(
                    "Target expression in update clause does not start " +
                    "with a table column", loc);
            }
        }

        int colPos = -1;

        if (firstStep != null) {
            ExprFieldStep colRef = (ExprFieldStep)firstStep;
            colPos = colRef.getFieldPos();
        }

        if (numSteps == 1 && colPos >= 0 && targetTable.isPrimKeyAtPos(colPos)) {
            throw new QueryException(
                "Cannot update a primary key column", loc);
        }

        int jsonMRCounterColPos = -1;
        if (targetTable.hasSchemaMRCounters()) {
            FieldDefImpl colDef = targetTable.getFieldDef(colPos);
            if (colDef.hasJsonMRCounter()) {
                jsonMRCounterColPos = colPos;
            }
        }

        ExprUpdateField upd = new ExprUpdateField(theQCB, theInitSctx, loc,
                                                  path, jsonMRCounterColPos);

        ExprVar targetItemVar = new ExprVar(theQCB, theInitSctx, loc,
                                            ExprVar.theCtxVarName, upd);
        theSctx.addVariable(targetItemVar);
        upd.addTargetItemVar(targetItemVar);

        theExprs.push(upd);
    }

    /*
     * This is a helper class used during the translation of field definitions.
     * It serves as a temporary place holder for the properties of the field
     * (its data type, nullability, default value, and associated comment).
     */
    private static class FieldDefHelper {

        final String name;

        String comment ;

        final QueryException.Location location;

        FieldDefImpl type = null;

        FieldValueImpl defaultValue = null;

        boolean nullable = true;

        FieldDefHelper(String name, String comment,
                       QueryException.Location location) {
            this.name = name;
            this.comment = comment;
            this.location = location;
        }

        String getName() {
            return name;
        }

        void setType(FieldDefImpl t) {
            type = t;
        }

        FieldDefImpl getType() {
            return type;
        }

        void setNullable(boolean v,
                         KVQLParser.Not_nullContext ctx) {
            nullable = v;

            /* only atomic, non-binary types can be not nullable */
            if (!nullable) {
                if (type.isAtomic()) {
                    return;
                }
                throw new QueryException(
                    "Fields of type: " + type.getType() +
                    " cannot be created as not-nullable",
                    getLocation(ctx));
            }
        }

        boolean getNullable() {
            return nullable;
        }

        void setDefault(
            String strval,
            KVQLParser.Default_valueContext ctx) {

            if (type == null) {
                throw new QueryStateException("Type must be set before " +
                    "setting a default value.");
            }

            /* only atomic, non-binary types can have a default value */
            if (!type.isAtomic()) {
                throw new QueryException(
                    "Fields of type: " + type.getType() +
                    " cannot have default values",
                    getLocation(ctx));
            }

            if (ctx.string() != null) {
                if (!type.isString() && !type.isTimestamp() &&
                    !type.isBinary() && !type.isFixedBinary()) {
                    throw new QueryException(
                        "Quoted default value for a non-string field. " +
                        "Field = " + name + " Value = " + strval,
                        getLocation(ctx));
                }

                strval = stripFirstLast(strval);
                strval = EscapeUtil.inlineEscapedChars(strval);
            }

            if (ctx.number() != null) {
                if (!type.isInteger() &&
                    !type.isLong() &&
                    !type.isFloat() &&
                    !type.isNumber() &&
                    !type.isDouble() &&
                    !type.isTimestamp()) {
                    throw new QueryException(
                        "Numeric default value for a non-numeric field. " +
                        "Field = " + name + " Value = " + strval,
                        getLocation(ctx));
                }

                strval = stripNumericLetter(strval);
            }

            if (ctx.id() != null) {
                if (!type.isEnum()) {
                    throw new QueryException(
                        "id as default value for a non-enum field. " +
                        "Field = " + name + " Value = " + strval,
                        getLocation(ctx));
                }
            }

            if (ctx.TRUE() != null || ctx.FALSE() != null) {
                if (!type.isBoolean()) {
                    throw new QueryException(
                        "Boolean default value for a non-boolean field. " +
                        "Field = " + name + " Value = " + strval,
                        getLocation(ctx));
                }
            }
            try {
                switch (type.getType()) {
                case INTEGER:
                    defaultValue = (FieldValueImpl)
                        type.createInteger(Integer.parseInt(strval));
                    break;
                case LONG:
                    defaultValue = (FieldValueImpl)
                        type.createLong(Long.parseLong(strval));
                    break;
                case FLOAT:
                    defaultValue = (FieldValueImpl)
                        type.createFloat(Float.parseFloat(strval));
                    break;
                case DOUBLE:
                    defaultValue = (FieldValueImpl)
                        type.createDouble(Double.parseDouble(strval));
                    break;
                case NUMBER:
                    defaultValue = (FieldValueImpl)
                        type.createNumber(new BigDecimal(strval));
                    break;
                case STRING:
                    defaultValue = (FieldValueImpl) type.createString(strval);
                    break;
                case ENUM:
                    defaultValue = type.createEnum(strval);
                    break;
                case BOOLEAN:
                    defaultValue = (FieldValueImpl)
                        type.createBoolean(Boolean.parseBoolean(strval));
                    break;
                case TIMESTAMP:
                    if (ctx.string() != null) {
                        defaultValue = (FieldValueImpl)
                                type.asTimestamp().fromString(strval);
                    } else {
                        assert (ctx.number().INT() != null);
                        defaultValue = ((TimestampDefImpl)type)
                               .createTimestamp(new Timestamp
                                                (Long.parseLong(strval)));
                    }

                    break;
                case BINARY:
                    defaultValue = (FieldValueImpl)
                        type.asBinary().fromString(strval);
                    break;
                case FIXED_BINARY:
                    defaultValue = (FieldValueImpl)
                        type.asFixedBinary().fromString(strval);
                    break;
                default:
                    throw new QueryException(
                        "Unexpected type for default value. Field = " + name +
                        " Type = " + type + " Value = " + strval,
                        getLocation(ctx));
                }
            } catch (IllegalArgumentException iae) {
                throw new QueryException(iae.getMessage(), getLocation(ctx));
            }
        }

        FieldValueImpl getDefault() {
            return defaultValue;
        }

        /*
         * This method is called at the end of the parsing of a field
         * definition, just before the field definition is added to its
         * containing record or table definition.
         */
        void validate() {
            if (defaultValue == null && !nullable) {
                throw new QueryException(
                    "Non-nullable field without a default value. " +
                    " Field = " + name, location);
            }

            type.setDescription(comment);
        }
    }

    /*
     * This is a helper class used during the translation of identity
     * definitions. It serves as a temporary place holder for the properties of
     * the identity (its always, on null, start, increment, max, min, cache and
     * cycle).
     */
    public static class IdentityDefHelper {
        private boolean isSetAlways = false;
        private boolean always = false;
        private boolean isSetOnNull = false;
        private boolean onNull = false;
        private boolean isSetStart = false;
        private String start = null;
        private boolean isSetIncrement = false;
        private String increment = null;
        private boolean isSetMax = false;
        private String max = null;
        private boolean isSetMin = false;
        private String min = null;
        private boolean isSetCache = false;
        private String cache = null;
        private boolean isSetCycle = false;
        private boolean cycle = false;

        public boolean isSetAlways() {
            return isSetAlways;
        }

        public boolean getAlways() {
            return always;
        }

        public void setAlways(boolean always) {
            this.always = always;
            isSetAlways = true;
        }

        public boolean isSetOnNull() {
            return isSetOnNull;
        }

        public boolean getOnNull() {
            return onNull;
        }

        public void setOnNull(boolean onNull) {
            this.onNull = onNull;
            isSetOnNull = true;
        }

        public boolean isSetStart() {
            return isSetStart;
        }

        public String getStart() {
            return start;
        }

        public void setStart(String start) {

            this.start = start;
            isSetStart = true;
        }

        public boolean isSetIncrement() {
            return isSetIncrement;
        }

        public String getIncrement() {
            return increment;
        }

        public void setIncrement(String increment) {
            this.increment = increment;
            isSetIncrement = true;
        }

        public boolean isSetMax() {
            return isSetMax;
        }

        public String getMax() {
            return max;
        }

        public void setMax(String max) {
            this.max = max;
            isSetMax = true;
        }

        public boolean isSetMin() {
            return isSetMin;
        }

        public String getMin() {
            return min;
        }

        public void setMin(String min) {
            this.min = min;
            isSetMin = true;
        }

        public boolean isSetCache() {
            return isSetCache;
        }

        public String getCache() {
            return cache;
        }

        public void setCache(String cache) {
            this.cache = cache;
            isSetCache = true;
        }

        public boolean isSetCycle() {
            return isSetCycle;
        }

        public boolean getCycle() {
            return cycle;
        }

        public void setCycle(boolean cycle) {
            this.cycle = cycle;
            isSetCycle = true;
        }
    }

    /*
     * type_def :
     *     binary_def         # Binary
     *   | array_def          # Array
     *   | boolean_def        # Boolean
     *   | enum_def           # Enum
     *   | float_def          # Float        // float, double, number
     *   | integer_def        # Int          // int, long
     *   | json_def           # JSON
     *   | map_def            # Map
     *   | record_def         # Record
     *   | string_def         # StringT
     *   | timestamp_def      # Timestamp
     *   | any_def            # Any           // not allowed in DDL
     *   | anyAtomic_def      # AnyAtomic     // not allowed in DDL
     *   | anyJsonAtomic_def  # AnyJsonAtomic // not allowed in DDL
     *   | anyRecord_def      # AnyRecord     // not allowed in DDL
     *   ;
     */

    /*
     * any_def : ANY_T ;
     */
    @Override public void enterAny(KVQLParser.AnyContext ctx) {
        if (theInDDL) {
            throw new QueryException("Type Any not allowed in DDL statements.",
                getLocation(ctx));
        }

        theTypes.push(FieldDefFactory.createAnyDef());
    }

    /*
     * anyAtomic_def : ANYATOMIC_T;
     */
    @Override public void enterAnyAtomic(KVQLParser.AnyAtomicContext ctx) {
        if (theInDDL) {
            throw new QueryException("Type AnyAtomic not allowed in DDL " +
                "statements.", getLocation(ctx));
        }

        theTypes.push(FieldDefFactory.createAnyAtomicDef());
    }

    /*
     * anyJsonAtomic_def : ANYJSONATOMIC_T;
     */
    @Override public void enterAnyJsonAtomic(
        KVQLParser.AnyJsonAtomicContext ctx) {
        if (theInDDL) {
            throw new QueryException("Type AnyJsonAtomic not allowed in DDL" +
                " statements.", getLocation(ctx));
        }

        theTypes.push(FieldDefFactory.createAnyJsonAtomicDef());
    }

    /*
     * json_def : JSON_T
     */
    @Override
    public void enterJSON(KVQLParser.JSONContext ctx) {
        jsonMRcounterFields = new HashMap<>();
        theTypes.push(FieldDefFactory.createJsonDef());
    }

    /*
     * anyRecord_def : ANYRECORD_T;
     */
    @Override public void enterAnyRecord(KVQLParser.AnyRecordContext ctx) {
        if (theInDDL) {
            throw new QueryException("Type AnyRecord not allowed in DDL" +
                " statements.", getLocation(ctx));
        }

        theTypes.push(FieldDefFactory.createAnyRecordDef());
    }

    /*
     * record_def : RECORD_T LP field_def (COMMA field_def)* RP
     */
    @Override
    public void enterRecord(KVQLParser.RecordContext ctx) {

        /*
         * Push a null as a sentinel for the unknown number of field
         * definitions that will follow.
         */
        theFields.push(null);
    }

    @Override
    public void exitRecord(KVQLParser.RecordContext ctx) {

        FieldMap fieldMap = new FieldMap();

        FieldDefHelper field = theFields.pop();
        assert(field != null);

        while (field != null) {

            /* Records, enums, and fixed binaries require a name in Avro */
            setNameForNamedType(field.getName(), field.getType());

            field.validate();

            /* fieldMap.put() checks for duplicate fields */
            fieldMap.put(
                field.getName(), field.getType(),
                field.getNullable(), field.getDefault());

            field = theFields.pop();
        }

        fieldMap.reverseFieldOrder();

        RecordDefImpl type = FieldDefFactory.createRecordDef(
            fieldMap, null/*description*/);

        theTypes.push(type);
    }

    /*
     * field_def : id type_def default_def? comment?
     *
     * default_def : (default_value not_null?) | (not_null? default_value) ;
     *
     * comment : COMMENT string
     */
    @Override
    public void enterField_def(KVQLParser.Field_defContext ctx) {

        String name = ctx.id().getText();

        String comment = null;
        if (ctx.comment() != null) {
            comment = stripFirstLast(ctx.comment().string().getText());
            comment = EscapeUtil.inlineEscapedChars(comment);
        }

        FieldDefHelper field = new FieldDefHelper(name, comment,
                                                  getLocation(ctx));
        theFields.push(field);
    }

    @Override
    public void exitField_def(KVQLParser.Field_defContext ctx) {

        assert(!theFields.empty());
        assert(!theTypes.empty());

        FieldDefHelper field = theFields.peek();

        field.setType(theTypes.pop());

        assert(theTypes.empty());
    }

    /*
     * default_value : DEFAULT (number | string | BOOLEAN_VALUE | id)
     *
     * not_null : NOT_NULL ;
     */
    @Override
    public void enterDefault_value(
        KVQLParser.Default_valueContext ctx) {

        assert(!theFields.empty());
        assert(!theTypes.empty());

        FieldDefHelper fieldDefHelper = theFields.peek();
        FieldDefImpl type = theTypes.peek();
        fieldDefHelper.setType(type);

        String strval = ctx.getChild(1).getText();

        /* validate and set the default value for the current field */
        fieldDefHelper.setDefault(strval, ctx);
    }

    /*
     * not_null : NOT_NULL;
     */
    @Override
    public void enterNot_null(KVQLParser.Not_nullContext ctx) {

        assert(!theFields.empty());
        FieldDefHelper field = theFields.peek();
        FieldDefImpl type = theTypes.peek();
        field.setType(type);
        field.setNullable(false, ctx);
    }

    /*
     * array_def : ARRAY_T LP type_def RP
     */
    @Override
    public void exitArray(KVQLParser.ArrayContext ctx) {

        FieldDefImpl elemType = theTypes.pop();

        /* Record, enum, and fixed binary types require a name in Avro */
        setNameForNamedType(null/*name*/, elemType);

        FieldDefImpl type = FieldDefFactory.createArrayDef(elemType);
        theTypes.push(type);
    }

    /*
     * map_def : MAP_T LP type_def RP
     */
    @Override
    public void exitMap(KVQLParser.MapContext ctx) {

        FieldDefImpl elemType = theTypes.pop();

        /* Records enum, and fixed binary types require a name in Avro */
        setNameForNamedType(null/*name*/, elemType);

        FieldDefImpl type = FieldDefFactory.createMapDef(elemType);
        theTypes.push(type);
    }

    /*
     * integer_def : (INTEGER_T | LONG_T)
     */
    @Override
    public void enterInt(KVQLParser.IntContext ctx) {

        boolean isLong = ctx.integer_def().LONG_T() != null;

        FieldDefImpl type = (isLong ?
                             FieldDefFactory.createLongDef() :
                             FieldDefFactory.createIntegerDef());
        theTypes.push(type);
    }

    /*
     * float_def : (FLOAT_T | DOUBLE_T | NUMBER_T)
     */
    @Override
    public void enterFloat(KVQLParser.FloatContext ctx) {

        boolean isDouble = ctx.float_def().DOUBLE_T() != null;
        boolean isNumber = ctx.float_def().NUMBER_T() != null;

        FieldDefImpl type = (isDouble ?
                             FieldDefFactory.createDoubleDef() :
                             (isNumber?
                              FieldDefFactory.createNumberDef() :
                              FieldDefFactory.createFloatDef()));

        theTypes.push(type);
    }

    /*
     * string_def : STRING_T
     */
    @Override
    public void enterStringT(KVQLParser.StringTContext ctx) {

        FieldDefImpl type = FieldDefFactory.createStringDef();
        theTypes.push(type);
    }

    /*
     * uuid_def :
     *     AS UUID (GENERATED BY DEFAULT)
     */
    @Override
    public void enterUuid_def(KVQLParser.Uuid_defContext ctx) {
        FieldDefImpl type = theTypes.pop();
        if (!type.isString()) {
            throw new IllegalArgumentException(
                "AS UUID option can only be used for STRING");
        }
        FieldDefImpl UUIDtype;
        if (ctx.GENERATED() != null) {
            UUIDtype = FieldDefFactory.createDefaultUUIDStrDef();
        }
        else {
            UUIDtype = FieldDefFactory.createUUIDStringDef();
        }
        theTypes.push(UUIDtype);
    }

    /*
     * enum_def : ENUM_T id_list_with_paren
     *
     * id_list_with_paren : LP id_list RP
     *
     * id_list : id (COMMA id)*
     */
    @Override
    public void enterEnum(KVQLParser.EnumContext ctx) {

        String[] values = makeIdArray(ctx.enum_def().id_list().id());

        try {
            FieldDefImpl type = FieldDefFactory.createEnumDef(values);
            theTypes.push(type);
        } catch (IllegalArgumentException iae) {
            String msg = "Invalid ENUM type '" + ctx.enum_def().getText() +
                "': " + iae.getMessage();
            throw new QueryException(msg, getLocation(ctx));
        }
    }

    /*
     * boolean_def : BOOLEAN_T
     */
    @Override
    public void enterBoolean(KVQLParser.BooleanContext ctx) {

        FieldDefImpl type = FieldDefFactory.createBooleanDef();
        theTypes.push(type);
    }

    /*
     * binary_def : BINARY_T (LP INT RP)?
     */
    @Override
    public void enterBinary(KVQLParser.BinaryContext ctx) {

        int size = 0;
        if (ctx.binary_def().INT() != null) {
            size = Integer.parseInt(ctx.binary_def().INT().getText());
        }

        FieldDefImpl type = (size == 0 ?
                             FieldDefFactory.createBinaryDef() :
                             FieldDefFactory.createFixedBinaryDef(size));
        theTypes.push(type);
    }

    /*
     * timestamp_def : TIMESTAMP_T (LP INT RP)?
     */
    @Override
    public void enterTimestamp(KVQLParser.TimestampContext ctx) {

        int precision = -1;

        if (ctx.timestamp_def().INT() != null) {

            precision = Integer.parseInt(ctx.timestamp_def().INT().getText());

            if (precision > TimestampDefImpl.MAX_PRECISION) {
                throw new QueryException(
                    "Timestamp precision exceeds the maximum allowed " +
                    "precision (" + TimestampDefImpl.MAX_PRECISION + ")");
            }

            if (precision < 0) {
                throw new QueryException(
                    "Timestamp precision cannot be a negative number");
            }
        }

        if (theInDDL && precision < 0) {
            throw new QueryException(
                "In DDL statements there is no default precision for the " +
                "Timestamp type. The precision must be explicitly specified.",
                getLocation(ctx));
        }

        if (precision < 0) {
            precision = TimestampDefImpl.DEF_PRECISION;
        }

        FieldDefImpl type = FieldDefFactory.createTimestampDef(precision);
        theTypes.push(type);
    }

    /*
     * identity_def :
     *     GENERATED (ALWAYS | (BY DEFAULT (ON NULL)?)) AS IDENTITY
     *     (LP sequence_options+ RP)?
     */
    @Override
    public void enterIdentity_def(KVQLParser.Identity_defContext ctx) {

        theSequenceOptions = new HashSet<String>();
        theIdentityHelper = new IdentityDefHelper();

        if (ctx.ALWAYS() != null) {
            theIdentityHelper.setAlways(true);
        }

        if (ctx.NULL() != null) {
            theIdentityHelper.setOnNull(true);
        }
    }

    /*
     * sequence_options :
     *     (START WITH signed_int) |
     *     (INCREMENT BY signed_int) |
     *     (MAXVALUE signed_int) | (NO MAXVALUE) |
     *     (MINVALUE signed_int) | (NO MINVALUE) |
     *     (CACHE INT) | (NO CACHE) |
     *     CYCLE | (NO CYCLE)
     * Note: These options can appear in any order but only once. Also MAXVALUE
     * precludes NO MAXVALUE etc.
     */
    @Override
    public void enterSequence_options(KVQLParser.Sequence_optionsContext ctx) {

        if (ctx.signed_int() != null) {

            final String stringValue = ctx.signed_int().getText();

            if (ctx.START() != null) {
                if (theSequenceOptions.contains("START")) {
                    throw new QueryException(
                        "START WITH can be specified for only one time",
                        getLocation(ctx));
                }
                theIdentityHelper.setStart(stringValue);
                theSequenceOptions.add("START");

            } else if (ctx.INCREMENT() != null) {
                if (theSequenceOptions.contains("INCREMENT")) {
                    throw new QueryException(
                        "INCREMENT BY can be specified for only one time",
                        getLocation(ctx));
                }
                theIdentityHelper.setIncrement(stringValue);
                theSequenceOptions.add("INCREMENT");

            } else if (ctx.MAXVALUE() != null) {
                if (theSequenceOptions.contains("MAXVALUE")) {
                    throw new QueryException(
                        "MAXVALUE can be specified for only one time and " +
                        "cannot be specified when NO MAXVALUE is specified.",
                        getLocation(ctx));
                }
                theIdentityHelper.setMax(stringValue);
                theSequenceOptions.add("MAXVALUE");

            } else if (ctx.MINVALUE() != null) {
                if (theSequenceOptions.contains("MINVALUE")) {
                    throw new QueryException(
                        "MINVALUE can be specified for only one time and " +
                        "cannot be specified when NO MINVALUE is specified.",
                        getLocation(ctx));
                }
                theIdentityHelper.setMin(stringValue);
                theSequenceOptions.add("MINVALUE");
            }

        } else if (ctx.NO() != null && ctx.MAXVALUE() != null) {
            if (theSequenceOptions.contains("MAXVALUE")) {
                throw new QueryException(
                    "NO MAXVALUE can be specified for only one time and " +
                    "cannot be specified when MAXVALUE is specified.",
                    getLocation(ctx));
            }
            theIdentityHelper.setMax("MAXVALUE");
            theSequenceOptions.add("MAXVALUE");

        } else if (ctx.NO() != null && ctx.MINVALUE() != null) {
            if (theSequenceOptions.contains("MINVALUE")) {
                throw new QueryException(
                    "NO MINVALUE can be specified for only one time and " +
                    "cannot be specified when MINVALUE is specified.",
                    getLocation(ctx));
            }
            theIdentityHelper.setMax("MINVALUE");
            theSequenceOptions.add("MINVALUE");

        } else if (ctx.NO() == null && ctx.CACHE() != null) {
            final String stringValue = ctx.INT().getText();
            if (theSequenceOptions.contains("CACHE")) {
                throw new QueryException(
                    "CACHE can be specified for only one time and cannot " +
                    "be specified when NO CACHE is specified.",
                    getLocation(ctx));
            }
            if ( Integer.parseInt(stringValue) == 0) {
                throw new QueryException("CACHE value cannot be 0.",
                                         getLocation(ctx));
            }

            theIdentityHelper.setCache(stringValue);
            theSequenceOptions.add("CACHE");
        } else if (ctx.NO() != null && ctx.CACHE() != null) {
            if (theSequenceOptions.contains("CACHE")) {
                throw new QueryException(
                    "NO CACHE can be specified for only one time and cannot " +
                    "be specified when CACHE is specified.",
                    getLocation(ctx));
            }
            theIdentityHelper.setCache(null);
            theSequenceOptions.add("CACHE");
        } else if (ctx.NO() == null && ctx.CYCLE() != null) {
            if (theSequenceOptions.contains("CYCLE")) {
                throw new QueryException(
                    "CYCLE can be specified for only one time and cannot " +
                    "be specified when NO CYCLE is specified.",
                    getLocation(ctx));
            }
            theIdentityHelper.setCycle(true);
            theSequenceOptions.add("CYCLE");
        } else if (ctx.NO() != null && ctx.CYCLE() != null) {
            if (theSequenceOptions.contains("CYCLE")) {
                throw new QueryException(
                    "NO CYCLE can be specified for only one time and cannot " +
                    "be specified when CYCLE is specified.",
                    getLocation(ctx));
            }
            theIdentityHelper.setCycle(false);
            theSequenceOptions.add("CYCLE");
        }
    }

    /*
     * ???? Can we use the name of the associated field (if any) as the type
     * name? What is the scope of the uniqueness requirement?
     */
    private void setNameForNamedType(String name, FieldDef type) {

       if (type.isRecord()) {
           ((RecordDefImpl)type).
               setName(name != null ?
                       name :
                       theQCB.generateFieldName(getNamePrefix("RECORD")));
        } else if (type.isEnum()) {
            ((EnumDefImpl)type).
                setName(name != null ?
                        name :
                        theQCB.generateFieldName(getNamePrefix("ENUM")));
        } else if (type.isFixedBinary()) {
            ((FixedBinaryDefImpl)type).
                setName(name != null ?
                        name :
                        theQCB.generateFieldName(getNamePrefix("FIXEDBINARY")));
        }
    }

    private String getNamePrefix(String name) {
        if (theTableBuilder instanceof TableEvolver) {
            return name + ((TableEvolver)theTableBuilder).getTableVersion();
        }
        return name;
    }

    /*
     * create_namespace_statement :
     *     CREATE NAMESPACE (IF NOT EXISTS)? namespace;
     */
    @Override
    public void enterCreate_namespace_statement(
        KVQLParser.Create_namespace_statementContext ctx) {
        String namespace = ctx.namespace().getText();
        TableImpl.validateNamespace(namespace);
        boolean ifNotExists = (ctx.EXISTS() != null);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(
                QueryOperation.CREATE_NAMESPACE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("CREATE NAMESPACE");

        theQCB.getStatementFactory().createNamespace(namespace, ifNotExists);
    }

    /*
     * drop_namespace_statement :
     *     DROP NAMESPACE (IF EXISTS)? namespace;
     */
    @Override
    public void enterDrop_namespace_statement( KVQLParser
        .Drop_namespace_statementContext ctx) {
        String namespace = ctx.namespace().getText();
        TableImpl.validateNamespace(namespace);
        boolean ifExists = (ctx.EXISTS() != null);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(
                QueryOperation.DROP_NAMESPACE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("DROP NAMESPACE");

        boolean cascade = ctx.CASCADE() != null;
        theQCB.getStatementFactory().dropNamespace(namespace, ifExists,
            cascade);
    }

    /**
     * create_region_statement :
     *    CREATE REGION region_name;
     */
    @Override
    public void enterCreate_region_statement(
                                KVQLParser.Create_region_statementContext ctx) {
        final String regionName = ctx.region_name().getText();
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().
                                     queryOperation(QueryOperation.CREATE_REGION);
            theQCB.getPrepareCallback().regionName(regionName);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }
        checkServerStatement("CREATE REGION");
        theQCB.getStatementFactory().createRegion(regionName);
    }

    /**
     * drop_region_statement :
     *    DROP REGION region_name;
     */
    @Override
    public void enterDrop_region_statement(
                                KVQLParser.Drop_region_statementContext ctx) {
        final String regionName = ctx.region_name().getText();
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().
                                       queryOperation(QueryOperation.DROP_REGION);
            theQCB.getPrepareCallback().regionName(regionName);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }
        checkServerStatement("DROP REGION");
        theQCB.getStatementFactory().dropRegion(regionName);
    }

    @Override
    public void enterSet_local_region_statement(
                            KVQLParser.Set_local_region_statementContext ctx) {
        final String regionName = ctx.region_name().getText();
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().
                                  queryOperation(QueryOperation.SET_LOCAL_REGION);
            theQCB.getPrepareCallback().regionName(regionName);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }
        checkServerStatement("SET LOCAL REGION");
        theQCB.getStatementFactory().setLocalRegionName(regionName);
    }

    /*
     * JSON Collection (schemaless) tables
     */
    @Override public void enterJson_collection_def(
        KVQLParser.Json_collection_defContext ctx) {
        Location loc = getLocation(ctx);
        try {
            theTableBuilder.setJsonCollection();
            theTableBuilder.setMRCounters(jsonMRcounterFields);
            if (theQCB.getPrepareCallback() != null) {
                theQCB.getPrepareCallback().jsonCollectionFound();
            }
        } catch (IllegalArgumentException iae) {
            String msg = "Attempt to set jsonCollection failed";
            throw new QueryException(msg, loc);
        }
    }

    @Override public void exitJson_collection_def(
        KVQLParser.Json_collection_defContext ctx) {
    }

    /*
     * create_table_statement :
     *     CREATE TABLE (IF NOT EXISTS)?
     *     table_name comment? LP table_def RP ttl_def?;
     *
     * table_def : (column_def | key_def) (COMMA (column_def | key_def))* ;
     */
    @Override
    public void enterCreate_table_statement(
        KVQLParser.Create_table_statementContext ctx) {

        String namespace = computeNamespace(ctx.table_name());
        /* only get the last component of the table path */
        String name = getPathLeaf(ctx.table_name().table_id_path());
        String[] parentPath = getParentPath(ctx.table_name().table_id_path());

        if ((name != null && name.startsWith(SYS_PREFIX)) ||
            (parentPath != null && parentPath.length > 0 &&
             parentPath[parentPath.length - 1] != null &&
             parentPath[parentPath.length - 1].startsWith(SYS_PREFIX)) ) {
            throw new QueryException(
                "Can not create table with name prefix '" + SYS_PREFIX + "': " +
                ctx.table_name().table_id_path().getText(),
                getLocation(ctx.table_name()));
        }

        /*
         * Do callback for name including parent path before requiring metadata
         * to allow the caller to allow, or not, child tables without a metadata
         * helper.
         */
        if (theQCB.getPrepareCallback() != null && parentPath != null) {
            theQCB.getPrepareCallback().queryOperation(
                QueryOperation.CREATE_TABLE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(parentPath) +
                                                  "." + name);
            if (ctx.EXISTS() != null) {
                theQCB.getPrepareCallback().ifNotExistsFound();
            }

            Frozen_defContext frozen_def = ctx.table_options().frozen_def();
            if (frozen_def != null) {
                boolean force = (frozen_def.FORCE() != null);
                theQCB.getPrepareCallback().freezeFound();
                theQCB.getPrepareCallback().freezeFound(force);
            }

            if (!theQCB.getPrepareCallback().prepareNeeded() &&
                theMetadataHelper == null) {
                throw new StopWalkException();
            }
        }

        TableImpl parentTable = getParentTable(ctx.table_name());

        KVQLParser.Table_defContext table_def = ctx.table_def();

        /* Validate the number of primary keys. */
        if (table_def.key_def() == null ||
            table_def.key_def().isEmpty() ||
            table_def.key_def().size() > 1) {
            throw new QueryException(
                "Table definition must contain a single primary " +
                "key definition", getLocation(table_def));
        }

        String comment = null;
        if (ctx.comment() != null) {
            comment = stripFirstLast(ctx.comment().string().getText());
            EscapeUtil.inlineEscapedChars(comment);
        }

        /*
         * Push a null as a sentinel for the unknown number of field
         * definitions that will follow.
         */
        theFields.push(null);

        /*
         * The TableBuilder constructor adds the columns of the parent's key to
         * its local FieldMap and primaryKey members.
         */
        theTableBuilder = TableBuilder.createTableBuilder(
                namespace, name, comment, parentTable,
                theMetadataHelper == null ? null :
                                            theMetadataHelper.getRegionMapper());
    }

    /**
     * Computes the namespace based on the namespace specified in the query:
     * queryNs and the namespace specified in the execute options: optionsNs.
     * Note: optionsNs can contain a tenant name for the cloud implementation.
     * Tenant name is the string before '@'.
     * Full name format is:  [tenant_name@][namespace_name:][parent.]item_name;
     *
     * Rules:
     *  If tenant is specified in optionsNs, it is passed on the result, else
     *  no tenant is specified.
     *  If queryNs is specified, it takes precedence over the optionsNs, else
     *  optionsNs namespace is used if specified, else INITIAL
     *  {@link oracle.kv.table.TableAPI#SYSDEFAULT_NAMESPACE_NAME} is used.
     *
     * Note: INITIAL namespace is translated here as null String when no
     * tenant is specified or only a "tenant@" string to remain compatible
     * with previous versions.
     */
    private String computeNamespace(KVQLParser.Table_nameContext ctx) {

        String ns = null;
        if ( ctx.namespace() != null ) {
            ns = ctx.namespace().getText();
        }

        if (ns != null) {
            // when specified in the query

            if (SYSDEFAULT_NAMESPACE_NAME.equalsIgnoreCase(ns)) {
                ns = null;
            }
        } else {
            // when not specified in the query
            ns = NameUtils.switchToInternalUse(theQCB.getNamespace());
        }

        return ns;
    }

    @Override
    public void exitCreate_table_statement(
        KVQLParser.Create_table_statementContext ctx) {

        assert(theTableBuilder != null);
        assert(!theFields.isEmpty());
        assert(theTypes.isEmpty());

        ArrayList<FieldDefHelper> fields = new ArrayList<FieldDefHelper>();

        FieldDefHelper field = theFields.pop();
        assert(field != null);
        while (field != null) {

            /* Record, enum, and fixed binary types require a name in Avro */
            setNameForNamedType(field.getName(), field.getType());

            field.validate();
            fields.add(field);
            field = theFields.pop();
        }

        assert(theFields.isEmpty());

        Collections.reverse(fields);

        for (FieldDefHelper field1 : fields) {
            theTableBuilder.addField(
                field1.getName(), field1.getType(), field1.getNullable(),
                field1.getDefault());
        }

        TableImpl table;
        SequenceDef sequenceDef = null;

        try {

            /*
             * Some semantic errors are only caught when building the table.
             * Validation of primary key size is one of them. Re-throw as a
             * QueryException.
             *
             * Validate the primary key fields separately to avoid potential
             * issues with upgraded stores
             */
            theTableBuilder.validatePrimaryKeyFields();
            table = theTableBuilder.buildTable();

            checkIfAllowCRDT(table);

            if (table.hasIdentityColumn()) {
                sequenceDef = theTableBuilder.getSequenceDef();
            }
            theTableBuilder = null;
        } catch (Exception e) {
            throw new QueryException("Error found when creating the table: " +
                e.getMessage());
        }

        boolean ifNotExists = (ctx.EXISTS() != null);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(
                QueryOperation.CREATE_TABLE);
            theQCB.getPrepareCallback().namespaceName(
                table.getInternalNamespace());
            theQCB.getPrepareCallback().tableName(table.getFullName());
            if (ifNotExists) {
                theQCB.getPrepareCallback().ifNotExistsFound();
            }
            theQCB.getPrepareCallback().newTable(table);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("CREATE TABLE");

        theQCB.getStatementFactory().createTable(table, ifNotExists,
                                                 sequenceDef);
    }


    /**
     * column_def : id type_def (default_def | identity_def | mr_counter_def)?
     *              comment? ;
     */
    @Override
    public void enterColumn_def(KVQLParser.Column_defContext ctx) {
        String name = ctx.id().getText();

        String comment = null;
        if (ctx.comment() != null) {
            comment = stripFirstLast(ctx.comment().string().getText());
            comment = EscapeUtil.inlineEscapedChars(comment);
        }

        FieldDefHelper field = new FieldDefHelper(name, comment,
            getLocation(ctx));

        /* Setup for enterDefault_value and exitCreate_table_statement */
        theFields.push(field);
    }

    @Override
    public void exitColumn_def(KVQLParser.Column_defContext ctx) {
        FieldDefHelper field = theFields.peek();

        field.setType(theTypes.pop());
        assert(theTypes.empty());

        if (theIdentityHelper != null) {
            if (theTableBuilder.getSequenceDef() != null) {
                throw new QueryException(
                    "Only one IDENTITY field is allowed in a table.",
                    getLocation(ctx.identity_def()));
            }

            theTableBuilder.setIdentity(
                field.getName(),
                theIdentityHelper.getAlways(),
                theIdentityHelper.getOnNull(),
                new SequenceDefImpl(field.getType(), theIdentityHelper));
            theIdentityHelper = null;
        }
    }

    /**
     * mr_counter_def : AS MR_COUNTER ;
     */
    @Override
    public void exitMr_counter_def(KVQLParser.Mr_counter_defContext ctx) {
        FieldDefImpl type = theTypes.pop();
        FieldDefImpl CRDTDef = FieldDefImpl.getCRDTDef(type.getType());
        theTypes.push(CRDTDef);
    }

    @Override
    public void exitJson_mrcounter_fields(
        KVQLParser.Json_mrcounter_fieldsContext ctx) {
        if (jsonMRcounterFields.size() != 0) {
            theTypes.pop();
            theTypes.push(FieldDefFactory.createJsonDef(jsonMRcounterFields));
        }
    }

    @Override
    public void exitJson_mrcounter_def(
        KVQLParser.Json_mrcounter_defContext ctx) {
        if (jsonMRcounterFields == null) {
            /*
             * Allow this for all tables because we don't yet know if it's
             * a JSON Collection. Validation happens in the TableImpl
             * constructor
             */
            jsonMRcounterFields = new HashMap<>();
        }

        String fieldName = ctx.json_mrcounter_path().getText();
        if (jsonMRcounterFields.containsKey(fieldName)) {
            throw new QueryException("Duplicate JSON MR_Counter field: " +
                fieldName);
        }
        if (ctx.INTEGER_T() != null) {
            jsonMRcounterFields.put(fieldName, Type.INTEGER);
        } else if (ctx.LONG_T() != null) {
            jsonMRcounterFields.put(fieldName, Type.LONG);
        } else if (ctx.NUMBER_T() != null) {
            jsonMRcounterFields.put(fieldName, Type.NUMBER);
        } else {
            throw new QueryException("Unexpected type for JSON " +
                "MR_Counter. Only INTEGER, LONG or NUMBER are allowed. " +
                getLocation(ctx));
        }
    }

    /**
     * key_def : PRIMARY_KEY LP (shard_key_def COMMA?)? id_list_with_size? RP ;
     *
     * id_list_with_size : id_with_size (COMMA id_with_size)* ;
     *
     * id_with_size : id ( LP 1..5 RP )?
     *
     * This is the Primary Key definition, which includes optional
     * specification of the shard key.
     *
     * The optional size specifier allows this:
     * 1. id field of primary key may take no more than 2 bytes of storage
     * when serialized:
     *   ... primary key (id(2))...
     * 2. id1 field of primary key may take no more than 2 bytes of storage
     *   ... primary key (shard(id1(2)), id2) ...
     */
    @Override
    public void enterKey_def(
        KVQLParser.Key_defContext ctx) {

        assert(theTableBuilder != null);

        try {

            /*
             * If it is a simple id list then there is no shard key
             * specified.
             */
            if (ctx.shard_key_def() == null) {
                /*
                 * Handle empty primary key (primary key()).
                 */
                if (ctx.id_list_with_size() == null) {
                    throw new QueryException(
                        "PRIMARY KEY must contain a list of fields",
                        getLocation(ctx));
                }

                /*
                 * tableBuilder.primaryKey() checks that there no duplicate
                 * column names in the list, but does not check that the key
                 * columns have been declared. This is done by the TableImpl
                 * constructor.
                 */
                makePrimaryKey(ctx.id_list_with_size().id_with_size());
                return;
            }

            /*
             * There is shard key specified. Create a list from that, then add
             * the additional primary key fields.
             */
            List<KVQLParser.Id_with_sizeContext> shardKeyList =
                ctx.shard_key_def().id_list_with_size().id_with_size();

            /*
             * tableBuilder.shardKey() Checks that there no duplicate column
             * names in the list, but does not check that the key columns have
             * been declared. This is done by the TableImpl constructor.
             */
            theTableBuilder.shardKey(makeKeyIdArray(shardKeyList));

            List<KVQLParser.Id_with_sizeContext> pkey =
                new ArrayList<KVQLParser.Id_with_sizeContext>(shardKeyList);

            /*
             * Handle case where primary key == shard key and the user
             * specified shard(), even though it is redundant.  It's allowed,
             * just not needed.  E.g. create table foo (id integer, primary
             * key (shard(id))).
             */
            if (ctx.id_list_with_size() != null) {
                pkey.addAll(ctx.id_list_with_size().id_with_size());
            }

            makePrimaryKey(pkey);

        } catch (IllegalArgumentException iae) {
            throw new QueryException(iae.getMessage(), getLocation(ctx));
        }
    }

    @Override
    public void enterTtl_def(KVQLParser.Ttl_defContext ctx) {
        KVQLParser.DurationContext duration = ctx.duration();
        Location loc = getLocation(ctx);
        try {
            theTableBuilder.setDefaultTTL(
                TimeToLive.createTimeToLive(
                    Integer.parseInt(duration.INT().getText()),
                    convertToTimeUnit(duration.time_unit())));
        } catch (NumberFormatException nfex) {
            String msg = "Invalid TTL value: "
                    + duration.INT().getText()
                    + " in " + duration.INT().getText()
                    + " " + duration.time_unit().getText();
            throw new QueryException(msg, loc);
        } catch (IllegalArgumentException iae) {
            String msg = "Invalid TTL Unit: "
                    + convertToTimeUnit(duration.time_unit())
                    + " in " + duration.INT().getText()
                    + " " + duration.time_unit().getText();
            throw new QueryException(msg, loc);
        }
    }

    @Override
    public void enterRegions_def(KVQLParser.Regions_defContext ctx) {
        final String[] regionNames =
                makeIdArray(ctx.region_names().id_list().id());

        PrepareCallback pc = theQCB.getPrepareCallback();
        if (pc != null) {
            for (String regionName : regionNames) {
                pc.regionName(regionName);
            }
        }

        /*
         * If the RegionMapper is available add regions so that the
         * table can be validated
         */
        if (theTableBuilder.getRegionMapper() != null) {
            for (String regionName : regionNames) {
                theTableBuilder.addRegion(regionName);
            }
        }
    }

    @Override
    public void enterAdd_region_def(KVQLParser.Add_region_defContext ctx) {
        final String[] regionNames =
                makeIdArray(ctx.region_names().id_list().id());

        PrepareCallback pc = theQCB.getPrepareCallback();
        if (pc != null) {
            for (String regionName : regionNames) {
                pc.regionName(regionName);
            }
        }

        /*
         * If the RegionMapper is available add regions so that the
         * table can be validated
         */
        if (theTableBuilder.getRegionMapper() != null) {
            for (String regionName : regionNames) {
                theTableBuilder.addRegion(regionName);
            }
        }
    }

    @Override
    public void enterDrop_region_def(KVQLParser.Drop_region_defContext ctx) {
        final String[] regionNames =
                makeIdArray(ctx.region_names().id_list().id());

        PrepareCallback pc = theQCB.getPrepareCallback();
        if (pc != null) {
            for (String regionName : regionNames) {
                pc.regionName(regionName);
            }
        }

        /*
         * If the prepare doesn't need to be completed, there may not be
         * metadata available to map the regions, so don't add them. This case
         * is a syntax check only, so if any regions don't exist the operation
         * will fail later.
         */
        if ((pc == null) || pc.prepareNeeded()) {
            for (String regionName : regionNames) {
                theTableBuilder.dropRegion(regionName);
            }
        }
    }

    /*
     * alter_table_statement : ALTER TABLE table_name alter_field_statement ;
     *
     * alter_field_statement :
     * LP
     * (add_field_statement | drop_field_statement | modify_field_statement)
     * (COMMA
     * (add_field_statement | drop_field_statement | modify_field_statement))*
     * RP ;
     *
     * add_field_statement :
     *     ADD schema_path type_def (default_def | identity_def |
     *                               mr_counter_def)? comment? ;
     *
     * drop_field_statement : DROP schema_path ;
     *
     * modify_field_statement :
     *     MODIFY schema_path ((type_def default_def? comment?) |
     *                         identity_def |
                               DROP IDENTITY);
     *
     * schema_path : init_schema_path_step (DOT schema_path_step)*;
     *
     * init_schema_path_step : id (LBRACK RBRACK)* ;
     *
     * schema_path_step : id (LBRACK RBRACK)* | VALUES LP RP ;
     */
    @Override
    public void enterAlter_table_statement(
        KVQLParser.Alter_table_statementContext ctx) {

        String namespace = computeNamespace(ctx.table_name());
        String[] pathName = getNamePath(ctx.table_name().table_id_path());

        if (pathName != null && pathName.length > 0 && (
            pathName[0].startsWith(SYS_PREFIX) ||
            pathName[pathName.length - 1].startsWith(SYS_PREFIX))) {
            throw new QueryException("Can not ALTER system table: " +
                ctx.table_name().table_id_path().getText(),
                getLocation(ctx.table_name().table_id_path()));
        }

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.ALTER_TABLE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(pathName));
            /*
             * Prepare may not have metadata available, in which case no
             * further callbacks will be made to let the caller know if
             * this is a freeze or unfreeze statement.
             * Check for freeze and unfreeze in the context and make
             * callbacks as needed
             */
            Alter_defContext adf = ctx.alter_def();
            Freeze_defContext fdf = adf.freeze_def();
            Unfreeze_defContext udf = adf.unfreeze_def();
            if (fdf != null || udf != null) {
                if (fdf != null) {
                    boolean force = (fdf.FORCE() != null);
                    theQCB.getPrepareCallback().freezeFound();
                    theQCB.getPrepareCallback().freezeFound(force);
                } else {
                    theQCB.getPrepareCallback().unfreezeFound();
                }
            }

            if (adf.ttl_def() != null) {
                theQCB.getPrepareCallback().alterTtlFound();
            }

            if (!theQCB.getPrepareCallback().prepareNeeded() &&
                theMetadataHelper == null) {
                /* keep going if there is a MD helper to get the table */
                throw new StopWalkException();
            }
        }

        TableImpl currentTable = getTable(namespace, pathName, getLocation(ctx));

        if (currentTable == null) {
            noTable(namespace, pathName, getLocation(ctx));
        }

        theTableBuilder =
            TableEvolver.createTableEvolver(currentTable,
                theMetadataHelper == null ? null :
                                            theMetadataHelper.getRegionMapper());
    }

    @Override
    public void exitAlter_table_statement(
        KVQLParser.Alter_table_statementContext ctx) {

        final TableEvolver evolver = (TableEvolver) theTableBuilder;
        final int tableVersion = evolver.getTableVersion();
        final TableImpl table;

        try {
            table = evolver.evolveTable();
            checkIfAllowCRDT(table);
        } catch (IllegalArgumentException iae) {
            throw new QueryException(iae.getMessage(), getLocation(ctx));
        }

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().newTable(table);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("ALTER TABLE");

        theTableBuilder = null;
        theQCB.getStatementFactory().evolveTable(table, tableVersion);
    }

    /*
     * freeze_def
     *  ALTER TABLE FREEZE SCHEMA
     */
    @Override
    public void enterFreeze_def(KVQLParser.Freeze_defContext ctx) {
        if (theQCB.getPrepareCallback() != null) {
            boolean force = (ctx.FORCE() != null);
            theQCB.getPrepareCallback().freezeFound();
            theQCB.getPrepareCallback().freezeFound(force);
        }
    }

    /*
     * unfreeze_def
     *  ALTER TABLE UNFREEZE SCHEMA
     */
    @Override
    public void enterUnfreeze_def(KVQLParser.Unfreeze_defContext ctx) {
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().unfreezeFound();
        }
    }

    /*
     * create table ... WITH SCHEMA FROZEN ...
     */
    @Override
    public void enterFrozen_def(KVQLParser.Frozen_defContext ctx) {
        if (theQCB.getPrepareCallback() != null) {
            boolean force = (ctx.FORCE() != null);
            theQCB.getPrepareCallback().freezeFound();
            theQCB.getPrepareCallback().freezeFound(force);
        }
    }

    /*
     * add_field_statement :
     *     ADD schema_path type_def (default_def | identity_def )? comment? ;
     *
     * default_def : (default_value not_null?) | (not_null? default_value) ;
     *
     * comment : COMMENT string
     */
    @Override
    public void enterAdd_field_statement(
        KVQLParser.Add_field_statementContext ctx) {

        String comment = null;
        if (ctx.comment() != null) {
            comment = stripFirstLast(ctx.comment().string().getText());
            comment = EscapeUtil.inlineEscapedChars(comment);
        }

        /*
         * This FieldDefHelper is used primarily for the translation of
         * the default_def.
        */
        FieldDefHelper fieldDefHelper = new FieldDefHelper("", comment,
                                                           getLocation(ctx));
        theFields.push(fieldDefHelper);
    }

    @Override
    public void exitAdd_field_statement(
        KVQLParser.Add_field_statementContext ctx) {

        assert(!theFields.empty());
        assert(!theTypes.empty());
        assert(theTableBuilder != null);

        TableEvolver evolver = (TableEvolver) theTableBuilder;

        FieldDefHelper field = theFields.pop();

        field.setType(theTypes.pop());
        // the default value is set in enterDefault_value() method

        assert(theTypes.empty());

        List<String> stepsList = getStepsList(ctx.schema_path());

        /* Record, enum, and fixed binary types require a name in Avro */
        /* Use the last component of the path name as the type name */
        String newFieldName = stepsList.get(stepsList.size() - 1);
        setNameForNamedType(newFieldName, field.getType());

        field.validate();

        try {
            evolver.addField(new TablePath(evolver.getFieldMap(), stepsList),
                             field.getType(), field.getNullable(),
                             field.getDefault());

            if (theIdentityHelper != null) {

                if (stepsList.size() > 1) {
                    throw new IllegalArgumentException(
                        "Invalid identity column, it can't be nested field: " +
                        ctx.schema_path().getText());
                }

                evolver.setIdentity(
                    stepsList.get(0),
                    theIdentityHelper.getAlways(),
                    theIdentityHelper.getOnNull(),
                    new SequenceDefImpl(field.getType(), theIdentityHelper));

                theIdentityHelper = null;
            }

        } catch (IllegalArgumentException iae) {
            throw new QueryException(iae.getMessage(),
                getLocation(ctx.schema_path()));
        }
    }

    /*
     * drop_field_statement : DROP schema_path ;
     */
    @Override
    public void exitDrop_field_statement(
        KVQLParser.Drop_field_statementContext ctx) {

        List<String> stepsList = getStepsList(ctx.schema_path());

        try {
            theTableBuilder.removeField(
                new TablePath(theTableBuilder.getFieldMap(), stepsList));
        } catch (IllegalArgumentException iae) {
            throw new QueryException(iae.getMessage(), iae,
                                     getLocation(ctx.schema_path()));
        }
    }

    /*
     * schema_path : init_schema_path_step (DOT schema_path_step)*;
     *
     * init_schema_path_step : id (LBRACK RBRACK)* ;
     *
     * schema_path_step : id (LBRACK RBRACK)* | VALUES LP RP ;
     */
    @SuppressWarnings("unused")
    static List<String> getStepsList(
        KVQLParser.Schema_pathContext schemaPathCtx) {

        KVQLParser.Init_schema_path_stepContext initStep =
            schemaPathCtx.init_schema_path_step();

        List<KVQLParser.Schema_path_stepContext> steps =
            schemaPathCtx.schema_path_step();

        List<String> stepsList = new ArrayList<String>(steps.size() + 1);

        stepsList.add(initStep.id().getText());

        if (initStep.LBRACK() != null) {
            assert initStep.RBRACK() != null;
            for (TerminalNode t : initStep.LBRACK()) {
                stepsList.add(TableImpl.BRACKETS);
            }
        }

        for (KVQLParser.Schema_path_stepContext step : steps) {

            if (step.id() != null) {
                stepsList.add(step.id().getText());

                if (step.LBRACK() != null) {
                    assert step.RBRACK() != null;
                    for (TerminalNode t : step.LBRACK()) {
                        stepsList.add(TableImpl.BRACKETS);
                    }
                }
            } else {
               stepsList.add(TableImpl.VALUES);
            }
        }
        return stepsList;
    }

    /**
     * modify_field_statement :
     *     MODIFY schema_path ((type_def default_def? comment?) |
     *            identity_def |
     *            DROP IDENTITY |
     *            uuid_def);
     */
    @Override
    public void enterModify_field_statement(
        KVQLParser.Modify_field_statementContext ctx) {

        if (ctx.type_def() != null || ctx.uuid_def() != null) {
            /*
             * In the current TableBuilder/TableEvolver model a new field cannot
             * be added over top of an existing field, so remove the field first
             * so the add later works.  The actual modification is validated in
             * TableImpl.evolve().
             */
            throw new QueryException("MODIFY " +
                    ((ctx.uuid_def() != null) ? "AS UUID" : "<type>")  +
                    " is not supported at this time",
                    getLocation(ctx));
        }
    }

    /**
     * modify_field_statement :
     *     MODIFY schema_path ((type_def default_def? comment?) |
     *            identity_def |
     *            DROP IDENTITY |
     *            uuid_def);
     */
    @Override
    public void exitModify_field_statement(
        KVQLParser.Modify_field_statementContext ctx) {

        List<String> stepsList = getStepsList(ctx.schema_path());
        TablePath tablePath = new TablePath(theTableBuilder.getFieldMap(),
                                            stepsList);

        if (ctx.DROP() != null) {
            assert ctx.IDENTITY() != null;

            try {
                theTableBuilder.setIdentity(null, false, false, null);
            } catch (IllegalArgumentException iae) {
                throw new QueryException(iae.getMessage(), iae,
                                         getLocation(ctx.schema_path()));
            }

        } else if (ctx.identity_def() != null) {
            assert theIdentityHelper != null;
            assert theTableBuilder != null;
            FieldDefImpl field = (FieldDefImpl) theTableBuilder.
                getField(tablePath);

            theTableBuilder.setIdentity(
                tablePath.getPathName(),
                ctx.identity_def().ALWAYS() != null,
                ctx.identity_def().NULL() != null,
                new SequenceDefImpl(field, theIdentityHelper));

            theIdentityHelper = null;
        }
    }

    /*
     * drop_table_statement : DROP TABLE (IF_EXISTS)? table_name ;
     */
    @Override
    public void enterDrop_table_statement(
        KVQLParser.Drop_table_statementContext ctx) {

        boolean ifExists = (ctx.EXISTS() != null);

        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        TableImpl table = getTableSilently(namespace, tableName);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DROP_TABLE);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            if (ifExists) {
                theQCB.getPrepareCallback().ifExistsFound();
            }
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("DROP TABLE");

        theQCB.getStatementFactory().dropTable(namespace,
            concatPathName(tableName),table, ifExists);
    }

    /*
     * create_index_statement :
     *     CREATE INDEX (IF NOT EXISTS)? index_name ON table_name
     *     LP index_field_list RP [WITH [NO] NULLS] WITH UNIQUE KEYS PER ROW
     *     comment?;
     *
     * index_name : id ;
     *
     * index_field_list : index_field (COMMA index_field)* ;
     *
     * index_field : (index_path path_type?) | index_function  ;
     *
     * index_function : id LP index_path? path_type? index_function_args? RP ;
     *
     * index_function_args : (COMMA const_expr)+ ;
     *
     * index_path :
     *     name_path |
     *     multikey_path_prefix multikey_path_suffix? |
     *     ELEMENTOF LP name_path RP |
     *     KEYOF LP name_path RP |
     *     KEYS LP name_path RP ;
     *
     * multikey_path_prefix :
     *    field_name
     *    ((DOT field_name) | (DOT VALUES LP RP) | (LBRACK RBRACK))*
     *    ((LBRACK RBRACK) | (DOT VALUES LP RP) | (DOT KEYS LP RP));
     *
     * multikey_path_suffix : (DOT name_path) ;
     */
    @Override
    public void enterCreate_index_statement(
        KVQLParser.Create_index_statementContext ctx) {

        boolean ifNotExists = (ctx.EXISTS() != null);
        boolean indexNulls = (ctx.NO() == null);
        boolean isUnique = (ctx.UNIQUE() != null);
        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String indexName = ctx.index_name().id().getText();

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.CREATE_INDEX);
            if (ifNotExists) {
                theQCB.getPrepareCallback().ifNotExistsFound();
            }
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            theQCB.getPrepareCallback().indexName(indexName);
        }

        List<KVQLParser.Index_fieldContext> pathCtxs =
            ctx.index_field_list().index_field();

        String[] fieldNames = getIndexFieldNames(pathCtxs);
        FieldDef.Type[] typeArray = makeTypeArray(pathCtxs);
        Map<String, String> geoProps = getGeoProperties(pathCtxs);

        String indexComment = null;
        if (ctx.comment() != null) {
            indexComment = stripFirstLast(ctx.comment().string().getText());
            indexComment = EscapeUtil.inlineEscapedChars(indexComment);
        }

        /*
         * If the callback is set and doesn't need the prepared
         * query return. If it continues, errors are likely in this path.
         */
        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().indexFields(fieldNames);
            theQCB.getPrepareCallback().indexFieldTypes(typeArray);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        TableImpl table = getTable(namespace, tableName, getLocation(ctx));

        checkServerStatement("CREATE INDEX");

        theQCB.getStatementFactory().createIndex(namespace,
            concatPathName(tableName), table, indexName, fieldNames,
            typeArray, indexNulls, isUnique, null /* annotatedFields */, geoProps,
            indexComment, ifNotExists, false /* override */);
    }

    @Override
    public void exitCreate_index_statement(
        KVQLParser.Create_index_statementContext ctx) {

        theExprs.clear();
    }

    private String[] getIndexFieldNames(
        List<KVQLParser.Index_fieldContext> list) {

        String[] names = new String[list.size()];
        int i = 0;

        for (KVQLParser.Index_fieldContext field : list) {

            KVQLParser.Index_pathContext path;
            boolean isFunctionalField = field.index_function() != null;
            String funcName = null;
            String functionArgs = null;
            int arity = 1;

            if (isFunctionalField) {
                funcName = field.index_function().id().getText();
                path = field.index_function().index_path();
                if (field.index_function().index_function_args() != null) {
                    arity += field.index_function().index_function_args().
                            const_expr().size();
                    functionArgs = field.index_function().
                            index_function_args().getText();
                }
                Function func = theSctx.findFunction(funcName, arity);
                if (func == null) {
                    throw new QueryException(
                        "Could not find function with name " + funcName +
                        " and arity 1", getLocation(field));
                }

                if (!func.isIndexable()) {
                    throw new QueryException(
                        "Function " + funcName +
                        " cannot be used in index field definition ",
                        getLocation(field));
                }
            } else {
                path = field.index_path();
            }

            if (path == null) {
                assert(isFunctionalField);
                names[i] = (funcName + "#");
               ++i;
                continue;
            }

            if (funcName != null) {
                names[i] = funcName + "#";
            } else {
                names[i] = "";
            }

            /* if there is no type expression, just get the text.
             * Otherwise we need to split out the type_expr. */
            if (field.path_type() == null) {
                names[i] += path.getText();
            } else {
                if (path.multikey_path_prefix() != null) {
                    names[i] += path.multikey_path_prefix().getText();
                    if (path.multikey_path_suffix() != null) {
                        names[i] += path.multikey_path_suffix().getText();
                    }
                } else if (path.name_path() != null) {
                    names[i] += path.name_path().getText();
                } else {
                    throw new IllegalStateException(
                        "Unexpected path in index statement: " +
                        path.getText());
                }
            }

            if (functionArgs != null) {
                names[i] += ("@" + functionArgs);
            }

            /*
             * Check for old-stype syntax and handle it
             */
            if (path.name_path() == null) {
                ++i;
                continue;
            }

            String indexPath = names[i];
            String lower = indexPath.toLowerCase();

            boolean isValues = lower.startsWith(TableImpl.FN_ELEMENTOF);
            boolean isKeys = (lower.startsWith(TableImpl.FN_KEYS) ||
                              lower.startsWith(TableImpl.FN_KEYOF));

            if (isValues || isKeys) {
                StringBuilder sb = new StringBuilder();
                int lp = lower.indexOf('(');
                int rp = lower.indexOf(')', lp);

                sb.append(indexPath.substring(lp + 1, rp));
                sb.append(NameUtils.CHILD_SEPARATOR);
                sb.append((isValues ? TableImpl.VALUES : TableImpl.KEYS));

                if (indexPath.length() > rp + 1) {
                    /* use the separator from the field */
                    sb.append(indexPath.substring(rp + 1, indexPath.length()));
                }

                names[i] = sb.toString();
            }

            ++i;
        }

        return names;
    }

    /**
     * Determines if any of the fields in an index declaration are typed.
     * This is only legal for JSON fields at this time.
     * Valid types are the valid JSON scalars (integer/long, float/double,
     * boolean, string) plus SCALAR (mapped to any atomic), indicating that
     * the field can be any of the atomic types, and ARRAY (mapped
     * to ARRAY) indicating that the field must be an array.
     *
     * If there are no declared types, return null.
     */
    static private FieldDef.Type[] makeTypeArray(
        List<KVQLParser.Index_fieldContext> list) {

        FieldDef.Type[] types = new FieldDef.Type[list.size()];
        boolean returnTypes = false;
        boolean isGeom = false;
        int i = 0;

        for (KVQLParser.Index_fieldContext field : list) {

            KVQLParser.Path_typeContext typeCtx;
            boolean isFunctionalField = field.index_function() != null;

            if (isFunctionalField) {
                typeCtx = field.index_function().path_type();
            } else {
                typeCtx = field.path_type();
            }

            if (typeCtx == null) {
                types[i] = null;
            } else {
                returnTypes = true;
                if (typeCtx.INTEGER_T() != null) {
                    types[i] = FieldDef.Type.INTEGER;
                } else if (typeCtx.LONG_T() != null) {
                    types[i] = FieldDef.Type.LONG;
                } else if (typeCtx.DOUBLE_T() != null) {
                    types[i] = FieldDef.Type.DOUBLE;
                } else if (typeCtx.NUMBER_T() != null) {
                    types[i] = FieldDef.Type.NUMBER;
                } else if (typeCtx.STRING_T() != null) {
                    types[i] = FieldDef.Type.STRING;
                } else if (typeCtx.BOOLEAN_T() != null) {
                    types[i] = FieldDef.Type.BOOLEAN;
                } else if (typeCtx.GEOMETRY_T() != null) {
                    types[i] = FieldDef.Type.GEOMETRY;
                    isGeom = true;
                } else if (typeCtx.POINT_T() != null) {
                    types[i] = FieldDef.Type.POINT;
                    isGeom = true;
                } else if (typeCtx.ANYATOMIC_T() != null) {
                    types[i] = FieldDef.Type.ANY_ATOMIC;
                }
            }
            i++;
        }

        /*
         * If it is a geometry index call CompilerAPI.getGeomUtils(). It will
         * try to load the GeometryUtilsImpl class, which is available in the.
         * in EE distribution only. So in CE, this call will raise an error and
         * cause index creation to fail.
         */
        if (isGeom) {
            CompilerAPI.getGeoUtils();
        }

        if (returnTypes) {
            return types;
        }

        return null;
    }

    static private Map<String, String> getGeoProperties(
        List<KVQLParser.Index_fieldContext> list) {

        Map<String, String> properties = null;

        for (KVQLParser.Index_fieldContext field : list) {

            KVQLParser.Path_typeContext typeCtx = field.path_type();

            if (typeCtx == null ||
                typeCtx.GEOMETRY_T() == null ||
                typeCtx.jsobject() == null) {
                continue;
            }

            if (properties == null) {
                properties = new HashMap<String,String>();
            }

            String jsontext = typeCtx.jsobject().getText();
            MapValueImpl json = (MapValueImpl)
                FieldValueFactory.createValueFromJson(jsontext);
            FieldValueImpl maxCellsVal = json.get("max_covering_cells");
            FieldValueImpl minCellsVal = json.get("min_covering_cells");
            FieldValueImpl maxSplitsVal = json.get("max_splits");
            FieldValueImpl splitRatioVal = json.get("split_ratio");

            if (maxCellsVal != null) {
                if (maxCellsVal.getDefinition().getType() != Type.INTEGER) {
                    throw new IllegalArgumentException(
                        "Value of max_covering_cells field is not an integer");
                }
                properties.put("max_covering_cells", maxCellsVal.toString());
            }

            if (minCellsVal != null) {
                if (minCellsVal.getDefinition().getType() != Type.INTEGER) {
                    throw new IllegalArgumentException(
                        "Value of min_covering_cells field is not an integer");
                }
                properties.put("min_covering_cells", minCellsVal.toString());
            }

            if (maxSplitsVal != null) {
                if (maxSplitsVal.getDefinition().getType() != Type.INTEGER) {
                    throw new IllegalArgumentException(
                        "Value of max_splits field is not an integer");
                }
                properties.put("max_splits", maxSplitsVal.toString());
            }

            if (splitRatioVal != null) {
                if (splitRatioVal.getDefinition().getType() != Type.DOUBLE) {
                    throw new IllegalArgumentException(
                        "Value of split_ratio field is not a double");
                }
                properties.put("split_ratio", splitRatioVal.toString());
            }

        }

        return null;
    }

    @Override
    public void enterDrop_index_statement(
        KVQLParser.Drop_index_statementContext ctx) {

        boolean ifExists = (ctx.EXISTS() != null);
        boolean override = (ctx.OVERRIDE() != null);

        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String indexName = ctx.index_name().id().getText();

        TableImpl table = getTableSilently(namespace, tableName);

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DROP_INDEX);
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            if (ifExists) {
                theQCB.getPrepareCallback().ifExistsFound();
            }
            theQCB.getPrepareCallback().indexName(indexName);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        checkServerStatement("DROP INDEX");

        theQCB.getStatementFactory().dropIndex(namespace,
                                               concatPathName(tableName),
                                               table,
                                               indexName,
                                               ifExists,
                                               override);
    }

    @Override
    public void exitCreate_text_index_statement
        (KVQLParser.Create_text_index_statementContext ctx) {

        boolean ifNotExists = false;
        if (ctx.EXISTS() != null) {
            ifNotExists = true;
        }
        boolean override = (ctx.OVERRIDE() != null);
        String namespace = computeNamespace(ctx.table_name());
        String[] tableName = getNamePath(ctx.table_name().table_id_path());
        String indexName = ctx.index_name().id().getText();

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.CREATE_INDEX);
            if (ifNotExists) {
                theQCB.getPrepareCallback().ifNotExistsFound();
            }
            theQCB.getPrepareCallback().namespaceName(namespace);
            theQCB.getPrepareCallback().tableName(concatPathName(tableName));
            theQCB.getPrepareCallback().indexName(indexName);
            theQCB.getPrepareCallback().isTextIndex();
        }

        AnnotatedField[] ftsFieldArray =
            makeFtsFieldArray(ctx.fts_field_list().fts_path_list().fts_path());

        Map<String, String> properties = new HashMap<String,String>();

        Es_propertiesContext propCtx = ctx.es_properties();
        if (propCtx != null) {
            for (KVQLParser.Es_property_assignmentContext prop :
                     propCtx.es_property_assignment()) {

                if (prop.ES_SHARDS() != null) {
                    String shards = prop.INT().toString();
                    if (Integer.parseInt(shards) < 1) {
                        throw new DdlException
                        ("The " + prop.ES_SHARDS() + " value of " + shards +
                         " is not allowed.");
                    }
                    properties.put(prop.ES_SHARDS().toString(), shards);
                } else if (prop.ES_REPLICAS() != null) {
                    String replicas = prop.INT().toString();
                    if (Integer.parseInt(replicas) < 0) {
                        throw new DdlException
                        ("The " + prop.ES_REPLICAS() + " value of " + replicas +
                         " is not allowed.");
                    }
                    properties.put(prop.ES_REPLICAS().toString(), replicas);
                }
            }
        }

        /* Don't carry an empty map around if we don't need it. */
        if (properties.isEmpty()) {
            properties = null;
        }

        String indexComment = null;
        if (ctx.comment() != null) {
            indexComment = stripFirstLast(ctx.comment().string().getText());
            indexComment = EscapeUtil.inlineEscapedChars(indexComment);
        }

        /*
         * If the callback is set and doesn't need the prepared
         * query return. If it continues, errors are likely in this path.
         */
        if (theQCB.getPrepareCallback() != null &&
            !theQCB.getPrepareCallback().prepareNeeded()) {
            throw new StopWalkException();
        }

        TableImpl table = getTable(namespace, tableName, getLocation(ctx));

        checkServerStatement("CREATE FULLTEXT INDEX");

        theQCB.getStatementFactory().createIndex(namespace,
            concatPathName(tableName), table, indexName, null, null, true,
            false, ftsFieldArray, properties, indexComment, ifNotExists,
            override);
    }

    @Override
    public void enterDescribe_statement(
        KVQLParser.Describe_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DESCRIBE);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        String namespace = null;
        String[] tableName = null;
        String indexName = null;
        List<List<String>> schemaPaths = null;

        namespace = computeNamespace(ctx.table_name());
        tableName = getNamePath(ctx.table_name().table_id_path());
        if (getTable(namespace, tableName, getLocation(ctx.table_name())) == null) {
            noTable(namespace, tableName, getLocation(ctx.table_name()));
        }

        if (ctx.index_name() != null) {
            indexName = ctx.index_name().id().getText();
        }

        if (ctx.schema_path_list() != null) {

            List<KVQLParser.Schema_pathContext> pathCtxList =
                ctx.schema_path_list().schema_path();

            schemaPaths = new ArrayList<List<String>>(pathCtxList.size());

            for (KVQLParser.Schema_pathContext spctx : pathCtxList) {
                schemaPaths.add(getStepsList(spctx));
            }
        }

        boolean describeAsJson = (ctx.JSON() != null);

        checkServerStatement("DESCRIBE TABLE");

        theQCB.getStatementFactory().describeTable( namespace,
            concatPathName(tableName), indexName, schemaPaths, describeAsJson);
    }

    /**
     * Very similar to DESCRIBE, with other options
     * show_statment: SHOW AS_JSON?
     *      (TABLES |
     *      ROLES |
     *      USERS |
     *      ROLE role_name |
     *      USER user_name |
     *      NAMESPACES |
     *      REGIONS |
     *      INDEXES ON table_name |
     *      TABLE table_name) ;
     */
    @Override
    public void enterShow_statement(
        KVQLParser.Show_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.SHOW);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        /* Try to identify as a Show User or Show Role operation */
        if (getShowUserOrRoleOp(ctx)) {
            return;
        }

        boolean asJson = (ctx.JSON() != null);

        if (ctx.NAMESPACES() != null) {
            checkServerStatement("SHOW NAMESPACES");

            theQCB.getStatementFactory().showNamespaces(asJson);
            return;
        }

        if (ctx.REGIONS() != null) {
            checkServerStatement("SHOW REGIONS");
            theQCB.getStatementFactory().showRegions(asJson);
            return;
        }

        String namespace = null;
        String[] tableName = null;
        boolean showTables = false;
        boolean showIndexes = false;

        /* Try to identify as a Show Table or Show Index operation */
        if (ctx.table_name() != null) {
            namespace = computeNamespace(ctx.table_name());
            tableName = getNamePath(ctx.table_name().table_id_path());
            if (getTable(namespace, tableName, getLocation(ctx.table_name())) == null) {
                noTable(namespace, tableName, getLocation(ctx.table_name()));
            }
            if (ctx.INDEXES() != null) {
                showIndexes = true;
            }
        } else {
            /*
             * The grammar does not allow table name and TABLES in the same
             * statement.
             */
            assert ctx.TABLES() != null;
            namespace = NameUtils.switchToInternalUse(theQCB.getNamespace());
            showTables = true;
        }

        checkServerStatement("SHOW TABLE|INDEX");

        theQCB.getStatementFactory().showTableOrIndex( namespace,
            concatPathName(tableName), showTables, showIndexes, asJson);
    }

    /*
     * For security related commands
     */

    /*
     * create_user_statement :
     *     CREATE USER create_user_identified_clause account_lock? ADMIN? ;
     *
     * create_user_identified_clause :
     *    id identified_clause (PASSWORD EXPIRE)? password_lifetime? |
     *    string IDENTIFIED EXTERNALLY ;
     */
    @Override
    public void exitCreate_user_statement(
        KVQLParser.Create_user_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.CREATE_USER);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final String userName =
            getIdentifierName(ctx.create_user_identified_clause(), "user");

        final boolean isExternal =
            ctx.create_user_identified_clause().IDENTIFIED_EXTERNALLY() !=
                null ? true : false;

        final boolean isAdmin = (ctx.ADMIN() != null);
        final boolean passExpired =
           (ctx.create_user_identified_clause().PASSWORD_EXPIRE() != null);

        final boolean isEnabled =
            ctx.account_lock() != null ?
            !isAccountLocked(ctx.account_lock()) :
            true;

        checkServerStatement("CREATE USER");

        if (!isExternal) {
            Long pwdLifetimeInMillis =
                ctx.create_user_identified_clause().
                    password_lifetime() == null ? null : resolvePassLifeTime(
                        ctx.create_user_identified_clause().
                            password_lifetime());

            final String plainPass =
                resolvePlainPassword(
                    ctx.create_user_identified_clause().identified_clause());

            if (passExpired) {
                pwdLifetimeInMillis = -1L;
            }

            theQCB.getStatementFactory().createUser(
                userName, isEnabled, isAdmin, plainPass, pwdLifetimeInMillis);
        } else {
            theQCB.getStatementFactory().createExternalUser(userName,
                                                            isEnabled,
                                                            isAdmin);
        }
    }

    @Override
    public void exitCreate_role_statement(
        KVQLParser.Create_role_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.CREATE_ROLE);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final String roleName = getIdentifierName(ctx.id(), "role");

        checkServerStatement("CREATE ROLE");

        theQCB.getStatementFactory().createRole(roleName);
    }

    @Override
    public void exitAlter_user_statement(
        KVQLParser.Alter_user_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.ALTER_USER);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final String userName =
            getIdentifierName(ctx.identifier_or_string(), "user");
        boolean retainPassword = false;
        String newPass = null;

        final KVQLParser.Reset_password_clauseContext resetPassCtx =
            ctx.reset_password_clause();

        if (resetPassCtx != null) {
            newPass = resolvePlainPassword(resetPassCtx.identified_clause());
            retainPassword = (resetPassCtx.RETAIN_CURRENT_PASSWORD() != null);
        }

        final boolean clearRetainedPassword =
            (ctx.CLEAR_RETAINED_PASSWORD() != null);
        final boolean passwordExpire = (ctx.PASSWORD_EXPIRE() != null);

        Long pwdLifetimeInMillis =
            ctx.password_lifetime() == null ?
            null :
            resolvePassLifeTime(ctx.password_lifetime());

        final Boolean isEnabled =
            ctx.account_lock() != null ?
            !isAccountLocked(ctx.account_lock()) :
            null;

        if (passwordExpire) {
            pwdLifetimeInMillis = -1L;
        }

        checkServerStatement("ALTER USER");

        theQCB.getStatementFactory().alterUser(
            userName, isEnabled, newPass, retainPassword,
            clearRetainedPassword, pwdLifetimeInMillis);
    }

    @Override
    public void exitDrop_user_statement(
        KVQLParser.Drop_user_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DROP_USER);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final String userName =
            getIdentifierName(ctx.identifier_or_string(), "user");
        final boolean cascade = (ctx.CASCADE() != null);

        checkServerStatement("DROP USER");

        theQCB.getStatementFactory().dropUser(userName, cascade);
    }

    @Override
    public void exitDrop_role_statement(
        KVQLParser.Drop_role_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.DROP_ROLE);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final String roleName =
            getIdentifierName(ctx.id(), "role");

        checkServerStatement("DROP ROLE");

        theQCB.getStatementFactory().dropRole(roleName);
    }

    @Override
    public void exitGrant_statement(
        KVQLParser.Grant_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.GRANT);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final Set<String> privSet = new HashSet<String>();
        final List<KVQLParser.Priv_itemContext> privItemList;
        final String roleName;

        checkServerStatement("GRANT");

        /* The GRANT roles TO user/role case */
        if (ctx.grant_roles() != null) {
            String[] roleNames =
                makeIdArray(ctx.grant_roles().id_list().id());
            final String grantee;
            if (ctx.grant_roles().principal().USER() != null) {
                assert (ctx.grant_roles().principal().ROLE() == null);
                grantee = getIdentifierName(
                    ctx.grant_roles().principal().identifier_or_string(),
                    "user");

                theQCB.getStatementFactory().grantRolesToUser(
                    grantee, roleNames);
            } else {
                grantee = getIdentifierName(
                    ctx.grant_roles().principal().id(), "role");

                theQCB.getStatementFactory().grantRolesToRole(
                    grantee, roleNames);
            }
            return;
        }

        /* The GRANT system_privileges TO role case */
        if (ctx.grant_system_privileges() != null) {
            privItemList =
                ctx.grant_system_privileges().sys_priv_list().priv_item();
            getPrivSet(privItemList, privSet);

            roleName = getIdentifierName(
                ctx.grant_system_privileges().id(), "role");

            theQCB.getStatementFactory().grantPrivileges(roleName,
                                                         null, // namespace
                                                         null, // tableName
                                                         privSet);
            return;
        }

        /*
         * The GRANT object_privilege ON (object | NAMESPACE namespace)
         * TO role case
         */
        if (ctx.grant_object_privileges() != null) {
            if (!ctx.grant_object_privileges().obj_priv_list().
                    ALL().isEmpty()) {
                privSet.add(ALL_PRIVS);
            } else {
                privItemList =
                    ctx.grant_object_privileges().obj_priv_list().priv_item();
                getPrivSet(privItemList, privSet);
            }
            roleName = getIdentifierName(
                ctx.grant_object_privileges().id(), "role");

            if (ctx.grant_object_privileges().object() != null) {
                final String[] onTable = getNamePath(
                    ctx.grant_object_privileges().object().table_name()
                        .table_id_path());
                String namespace = computeNamespace(
                    ctx.grant_object_privileges().object().table_name());

                theQCB.getStatementFactory().grantPrivileges(
                    roleName, namespace, concatPathName(onTable), privSet);
                return;
            }

            if (ctx.grant_object_privileges().namespace() != null) {
                String namespace =
                    ctx.grant_object_privileges().namespace().getText();
                theQCB.getStatementFactory().grantNamespacePrivileges(roleName,
                    NameUtils.switchToInternalUse(namespace), privSet);
                return;
            }
        }
    }

    @Override
    public void exitRevoke_statement(
        KVQLParser.Revoke_statementContext ctx) {

        if (theQCB.getPrepareCallback() != null) {
            theQCB.getPrepareCallback().queryOperation(QueryOperation.REVOKE);
            if (!theQCB.getPrepareCallback().prepareNeeded()) {
                throw new StopWalkException();
            }
        }

        final Set<String> privSet = new HashSet<String>();
        final List<KVQLParser.Priv_itemContext> privItemList;
        final String roleName;

        checkServerStatement("REVOKE");

        /* The REVOKE roles FROM user/role case */
        if (ctx.revoke_roles() != null) {
            String[] roleNames =
                makeIdArray(ctx.revoke_roles().id_list().id());
            final String revokee;
            if (ctx.revoke_roles().principal().USER() != null) {
                assert (ctx.revoke_roles().principal().ROLE() == null);
                revokee = getIdentifierName(
                    ctx.revoke_roles().principal().identifier_or_string(),
                    "user");

                theQCB.getStatementFactory().revokeRolesFromUser(
                    revokee, roleNames);
            } else {
                revokee = getIdentifierName(
                    ctx.revoke_roles().principal().id(), "role");

                theQCB.getStatementFactory().revokeRolesFromRole(
                    revokee, roleNames);
            }
            return;
        }

        /* The REVOKE system_privileges FROM role case */
        if (ctx.revoke_system_privileges() != null) {
            privItemList =
                ctx.revoke_system_privileges().sys_priv_list().priv_item();
            getPrivSet(privItemList, privSet);

            roleName = getIdentifierName(
                ctx.revoke_system_privileges().id(), "role");

            theQCB.getStatementFactory().revokePrivileges(roleName,
                null, // namespace
                null, // tableName
                privSet);
            return;
        }

        /*
         * The REVOKE object_privilege ON (object | NAMESPACE namespace)
         * FROM role case
         */
        if (ctx.revoke_object_privileges() != null) {
            if (!ctx.revoke_object_privileges().obj_priv_list().
                    ALL().isEmpty()) {
                privSet.add(ALL_PRIVS);
            } else {
                privItemList =
                    ctx.revoke_object_privileges().obj_priv_list().priv_item();
                getPrivSet(privItemList, privSet);
            }
            roleName = getIdentifierName(
                ctx.revoke_object_privileges().id(), "role");

            /* ON object */
            if (ctx.revoke_object_privileges().object() != null) {
                final String[] onTable = getNamePath(
                    ctx.revoke_object_privileges().object().table_name().
                        table_id_path());
                String namespace = computeNamespace(
                    ctx.revoke_object_privileges().object().table_name());

                theQCB.getStatementFactory().revokePrivileges(
                    roleName, namespace, concatPathName(onTable), privSet);
                return;
            }

            /* ON NAMESPACE namespace */
            if (ctx.revoke_object_privileges().namespace() != null) {
                String namespace = ctx.revoke_object_privileges().namespace().
                    getText();
                theQCB.getStatementFactory().revokeNamespacePrivileges(roleName,
                    NameUtils.switchToInternalUse(namespace), privSet);
                return;
            }
        }
    }

    /* Callbacks for embedded JSON parsing. */
    @Override
    public void exitJsonAtom(KVQLParser.JsonAtomContext ctx) {
        jsonCollector.exitJsonAtom(ctx);
    }

    @Override
    public void exitJsonArrayValue
        (KVQLParser.JsonArrayValueContext ctx) {

        jsonCollector.exitJsonArrayValue(ctx);
    }

    @Override
    public void exitJsonObjectValue
        (KVQLParser.JsonObjectValueContext ctx) {

        jsonCollector.exitJsonObjectValue(ctx);
    }

    @Override
    public void exitJsonPair(KVQLParser.JsonPairContext ctx) {
        jsonCollector.exitJsonPair(ctx);
    }

    @Override
    public void exitArrayOfJsonValues
        (KVQLParser.ArrayOfJsonValuesContext ctx) {

        jsonCollector.exitArrayOfJsonValues(ctx);
    }

    @Override
    public void exitEmptyJsonArray
        (KVQLParser.EmptyJsonArrayContext ctx) {

        jsonCollector.exitEmptyJsonArray(ctx);
    }

    @Override
    public void exitJsonObject(KVQLParser.JsonObjectContext ctx) {
        jsonCollector.exitJsonObject(ctx);
    }

    @Override
    public void exitEmptyJsonObject
        (KVQLParser.EmptyJsonObjectContext ctx) {

        jsonCollector.exitEmptyJsonObject(ctx);
    }

    @Override
    public void exitJson_text(KVQLParser.Json_textContext ctx) {
        jsonCollector.exitJson_text(ctx);
    }

    /*
     * Internal functions and classes
     */

    private boolean getShowUserOrRoleOp(
        KVQLParser.Show_statementContext ctx) {

        final boolean asJson = (ctx.JSON() != null);

        checkServerStatement("SHOW");

        if (ctx.identifier_or_string() != null && ctx.USER() != null) {
            final String name = getIdentifierName(ctx.identifier_or_string(),
                                                  "user");
            theQCB.getStatementFactory().showUser(name, asJson);
            return true;
        }
        if (ctx.id() != null && ctx.ROLE() != null) {
            final String name = getIdentifierName(ctx.id(), "role");
            theQCB.getStatementFactory().showRole(name, asJson);
            return true;
        }
        if (ctx.USERS() != null) {
            theQCB.getStatementFactory().showUser(null, asJson);
            return true;
        } else if (ctx.ROLES() != null) {
            theQCB.getStatementFactory().showRole(null, asJson);
            return true;
        }
        return false;
    }

    private static boolean isAccountLocked(
        KVQLParser.Account_lockContext ctx) {

        if (ctx.LOCK() != null) {
            assert (ctx.UNLOCK() == null);
            return true;
        }
        return false;
    }

    private static String getIdentifierName(
        KVQLParser.IdContext ctx,
        String idType) {

        if (ctx != null) {
            return ctx.getText();
        }
        throw new QueryException("Invalid empty name of " + idType,
                                    getLocation(ctx));
    }

    private static String getIdentifierName(
        KVQLParser.Identifier_or_stringContext ctx,
        String idType) {

        if (ctx.id() != null) {
            return getIdentifierName(ctx.id(), idType);
        }
        if (ctx.string() != null) {
            final String result = EscapeUtil.inlineEscapedChars(
                stripFirstLast(ctx.string().getText()));
            if (!result.equals("")) {
                return result;
            }
        }
        throw new QueryException("Invalid empty name of " + idType,
                                    getLocation(ctx));
    }

    private static String getIdentifierName(
        KVQLParser.Create_user_identified_clauseContext ctx,
        String idType) {

        if (ctx.identified_clause() != null && ctx.id() != null) {
            return getIdentifierName(ctx.id(), idType);
        }
        if (ctx.IDENTIFIED_EXTERNALLY() != null && ctx.string() != null) {
            final String result = EscapeUtil.inlineEscapedChars(
                stripFirstLast(ctx.string().getText()));
            if (!result.equals("")) {
                return result;
            }
        }
        throw new QueryException("Invalid empty name of " + idType,
                                    getLocation(ctx));
    }

    private static String resolvePlainPassword(
        KVQLParser.Identified_clauseContext ctx) {

        final String passStr = ctx.by_password().string().getText();
        if (passStr.isEmpty() || passStr.length() <= 2) {
            throw new QueryException("Invalid empty password",
                                        getLocation(ctx));
        }
        return passStr;
    }

    private static long resolvePassLifeTime(
        KVQLParser.Password_lifetimeContext ctx) {

        final long timeValue;
        final TimeUnit timeUnit;
        try {
            timeValue = Integer.parseInt(ctx.duration().INT().getText());
            if (timeValue < 0) {
                throw new QueryException(
                    "Time value must not be negative",
                    getLocation(ctx));
            }
        } catch (NumberFormatException nfe) {
            throw new QueryException("Invalid numeric value for time value",
                                        getLocation(ctx));
        }

        timeUnit = convertToTimeUnit(ctx.duration().time_unit());
        return TimeUnit.MILLISECONDS.convert(timeValue, timeUnit);
    }

    enum DDLTimeUnit {
        S() {
            @Override
            TimeUnit getUnit() {
                return TimeUnit.SECONDS;
            }
        },

        M() {
            @Override
            TimeUnit getUnit() {
                return TimeUnit.MINUTES;
            }
        },

        H() {
            @Override
            TimeUnit getUnit() {
                return TimeUnit.HOURS;
            }
        },

        D() {
            @Override
            TimeUnit getUnit() {
                return TimeUnit.DAYS;
            }
        };

        abstract TimeUnit getUnit();
    }

    private static TimeUnit convertToTimeUnit(KVQLParser.Time_unitContext ctx) {
        String unitStr = ctx.getText();
        try {
            return TimeUnit.valueOf(unitStr.toUpperCase(ENGLISH));
        } catch (IllegalArgumentException iae) {
            try {
                return DDLTimeUnit.valueOf(
                    unitStr.toUpperCase(ENGLISH)).getUnit();
            } catch (IllegalArgumentException iae2) {
                /* Fall through */
            }
        }
        throw new QueryException("Unrecognized time unit " + unitStr,
                                 getLocation(ctx));
    }

    /**
     * Returns all the components of a path name as an array of strings.
     */
    static private String[] getNamePath(
        KVQLParser.Table_id_pathContext ctx) {

        List<KVQLParser.Table_idContext> steps = ctx.table_id();

        String[] result = new String[steps.size()];

        int i = 0;
        for (KVQLParser.Table_idContext step : steps) {
            result[i] = step.getText();
            ++i;
        }

        return result;
    }

    /*
     * Returns all the components of a path name, except from the last one,
     * as an array of strings.
     */
    static private String[] getParentPath(
        KVQLParser.Table_id_pathContext ctx) {

        List<KVQLParser.Table_idContext> steps = ctx.table_id();

        if (steps.size() == 1) {
            return null;
        }

        String[] result = new String[steps.size() - 1];

        int i = 0;
        for (KVQLParser.Table_idContext step : steps) {
            result[i] = step.getText();
            ++i;
            if (i == steps.size() - 1) {
                break;
            }
        }

        return result;
    }

    static private String getPathLeaf(
        KVQLParser.Table_id_pathContext ctx) {

        List<KVQLParser.Table_idContext> steps = ctx.table_id();

        return steps.get(steps.size() - 1).getText();
    }

    static private String concatPathName(String[] pathName) {
        return concatPathName(pathName, '.');
    }

    static private String concatPathName(String[] pathName, char separator) {

        if (pathName == null) {
            return null;
        }

        int numSteps = pathName.length;
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < numSteps; ++i) {
            name.append(pathName[i]);
            if (i < numSteps - 1) {
                name.append(separator);
            }
        }
        return name.toString();
    }

    /**
     * Given a full id_path for a table, return the parent table, if any.
     */
    private TableImpl getParentTable(KVQLParser.Table_nameContext ctx) {

        String namespace = computeNamespace(ctx);
        String[] parentPath = getParentPath(ctx.table_id_path());

        if (parentPath == null) {
            return null;
        }

        TableImpl parent = getTable(namespace, parentPath, getLocation(ctx));
        if (parent == null) {
            String fullPath = NameUtils.makeQualifiedName(namespace,
                concatPathName(getNamePath(ctx.table_id_path())));
            String parentName = NameUtils.makeQualifiedName(namespace,
                concatPathName(parentPath));
            noParentTable(parentName, fullPath,
                          getLocation(ctx));
        }
        return parent;
    }

    /**
     * Returns the named table if it exists in the table metadata.
     *
     * @return the table if it exists, null if not
     * @throws QueryException if TableMetadata is null
     */
    private TableImpl getTable(
        String namespace,
        String[] pathName,
        QueryException.Location location) {

        if (theMetadataHelper == null) {
            throw new QueryException(
                "No metadata found for table " +
                NameUtils.makeQualifiedName(namespace,
                    concatPathName(pathName)),
                location);
        }
        return theMetadataHelper.getTable(namespace, pathName,
                                          theInDDL ? 0: MIN_QUERY_COST);
    }

    /**
     * Returns the named table if it exists in table metadata.  Null will be
     * returned if either the table metadata is null, or the table does not
     * exist.
     */
    private TableImpl getTableSilently(String namespace, String[] pathName) {
        return theMetadataHelper == null ? null :
            theMetadataHelper.getTable(namespace, pathName,
                                       theInDDL ? 0: MIN_QUERY_COST);
    }

    static private String[] makeIdArray(
        List<KVQLParser.IdContext> list) {

        String[] ids = new String[list.size()];
        int i = 0;
        for (KVQLParser.IdContext idCtx : list) {
            ids[i++] = idCtx.getText();
        }
        return ids;
    }

    static private String[] makeKeyIdArray(
        List<KVQLParser.Id_with_sizeContext> list) {
        String[] ids = new String[list.size()];
        int i = 0;
        for (KVQLParser.Id_with_sizeContext idCtx : list) {
            ids[i++] = idCtx.id().getText();
        }
        return ids;
    }

    private void makePrimaryKey(List<KVQLParser.Id_with_sizeContext> list) {
        for (KVQLParser.Id_with_sizeContext idCtx : list) {
            String keyField = idCtx.id().getText();
            theTableBuilder.primaryKey(keyField);
            if (idCtx.storage_size() != null) {
                int size = Integer.parseInt(
                    idCtx.storage_size().INT().getText());
                theTableBuilder.primaryKeySize(keyField, size);
            }
        }
    }

    static private void getPrivSet(
        List<KVQLParser.Priv_itemContext> pCtxList,
        Set<String> privSet) {

        for (KVQLParser.Priv_itemContext privItem : pCtxList) {
            if (privItem.ALL_PRIVILEGES() != null) {
                privSet.add(ALL_PRIVS);
            } else {
                privSet.add(getIdentifierName(privItem.id(), "privilege"));
            }
        }
    }

    private AnnotatedField[] makeFtsFieldArray(
        List<KVQLParser.Fts_pathContext>list) {

    	final AnnotatedField[] fieldspecs =
    			new AnnotatedField[list.size()];

    	int i = 0;
    	for (KVQLParser.Fts_pathContext pctx: list) {
            KVQLParser.Index_pathContext path = pctx.index_path();
            String fieldName = path.getText();
            String jsonStr = jsonCollector.get(pctx.jsobject());
            fieldspecs[i++] = new AnnotatedField(fieldName, jsonStr);
    	}
    	return fieldspecs;
    }

    static private String stripFirstLast(String s) {
        return s.substring(1, s.length() - 1);
    }

    static private void noTable(String namespace, String[] pathName,
        QueryException.Location location) {
        throw new QueryException(
            "Table does not exist: " + NameUtils.
                makeQualifiedName(namespace, concatPathName(pathName)),
            location);
    }

    static private void noParentTable(String parentName, String fullName,
        QueryException.Location location) {
        throw new QueryException(
            "Parent table does not exist (" + parentName +
            ") in table path " + fullName, location);
    }

    private static QueryException.Location getLocation(ParserRuleContext ctx) {
        int startLine = -1;
        int startColumn = -1;
        int endLine = -1;
        int endColumn = -1;

        if (ctx.getStart() != null) {
            startLine = ctx.getStart().getLine();
            startColumn = ctx.getStart().getCharPositionInLine();
        }

        if (ctx.getStop() != null) {
            endLine = ctx.getStop().getLine();
            endColumn = ctx.getStop().getCharPositionInLine();
        }

        return new QueryException.Location(startLine, startColumn, endLine, endColumn);
    }

    private static QueryException.Location getLocation(TerminalNode node) {
        int startLine = -1;
        int startColumn = -1;
        int endLine = -1;
        int endColumn = -1;

        if (node != null && node.getSymbol() != null) {
            startLine = node.getSymbol().getLine();
            startColumn = node.getSymbol().getCharPositionInLine();
            endLine = node.getSymbol().getLine();
            endColumn = node.getSymbol().getCharPositionInLine();
        }

        return new QueryException.Location(startLine, startColumn, endLine, endColumn);
    }

    private static String[] mapTableNames(PrepareCallback cb, String[] names) {
        String tableName = concatPathName(names, '.');
        String mappedName = cb.mapTableName(tableName);
        return mappedName.split("\\.");
    }

    /**
     * Throws DdlException if not executing on the server.
     */
    private void checkServerStatement(String op) {
        if (theQCB.getStatementFactory() == null) {
            throw new DdlException(op + " must execute on a server");
        }
    }

    /*
     * Checks if CRDT fields are allowed in the table.
     *
     * The CRDT fields are allowed for multi-region table or
     * ExecuteOptions.allowCRDT is set to true, throw IAE if not.
     */
    private void checkIfAllowCRDT(TableImpl table) {
        if (table.hasSchemaMRCounters()) {
            if (!table.isMultiRegion() && !theQCB.getOptions().allowCRDT()){
                throw new IllegalArgumentException(
                    "MR_counters are not allowed in the table");
            }
        }
    }

    /**
     * A local exception to indicate that the walking should stop early.
     * This is used when a PrepareCallback is involved.
     */
    @SuppressWarnings("serial")
    private static class StopWalkException extends RuntimeException {}
}
