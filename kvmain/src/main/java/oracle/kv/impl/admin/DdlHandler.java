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

package oracle.kv.impl.admin;

import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.compiler.StatementFactory;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.table.FieldDef;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;

/**
 * This class encapsulates DDL requests to the admin. The requests are
 * presented as DDL statements, which are strings. The statements are parsed
 * and results handled in this class, which provides success/failure state as
 * well as additional information required to return information to callers,
 * which are remote clients for the most part.
 *
 * This class does not throw any exceptions.  Upon completion a successful
 * operation results in this state:
 * 1. getSuccess() returns true
 * 2. One of the following:
 *  2a. A plan was created and is running.  getPlanId() returns a non-zero value
 *  2b. A statement with "if exists" or "if not exists" was presented and
 *  resulted in a no-op.  getPlanId() returns 0, getResultString() returns null
 *  2c. A statement that returns a String was run.  getResultString() returns a
 *  non-null value, getPlanId() returns 0
 *
 * On failure:
 * 1.  getSuccess() returns false
 * 2.  getErrorMessage() has a non-null value
 * 3.  if the error is transient, in that the operation can be retried,
 * canRetry() returns true.  This will be the case for most plan execution
 * failures.  Failures that occur before plan execution are not transient and
 * if retried will still fail.
 *
 * TODO:
 * o finish describe
 */
public class DdlHandler {

    private QueryControlBlock query;
    private final Admin admin;
    private boolean success;
    private String errorString;
    private String resultString;
    private int planId;
    private boolean hasPlan;
    private boolean canRetry;
    private DdlOperation ddlOperation;
    private final String statement;
    private final DdlOperationExecutor ddlOpExecutor;
    private final StatementFactory statementFactory;
    private final TableLimits limits;
    private final TableMetadata tableMetadata;

    /**
     * Validates, if necessary, that the namespace used in create table
     * statement exists.
     */
    public static class ValidateNsPrepareCallback implements PrepareCallback {
        private final String namespace;
        private final boolean validateNamespace;
        private final TableMetadata tableMetadata;
        private PrepareCallback.QueryOperation queryOp;

        ValidateNsPrepareCallback(String namespace, boolean validateNamespace,
            TableMetadata tableMetadata) {
            this.namespace = namespace;
            this.validateNamespace = validateNamespace;
            this.tableMetadata = tableMetadata;
        }

        @Override
        public void tableName(String tableName) {
        }

        @Override
        public void indexName(String indexName) {
        }

        @Override
        public void namespaceName(String namespaceName) {
            if (validateNamespace &&
                PrepareCallback.QueryOperation.CREATE_TABLE == queryOp) {

                String ns;
                if (namespaceName != null) {
                    ns = namespaceName;
                } else {
                    ns = namespace;
                }

                if (tableMetadata != null && ns != null &&
                    !tableMetadata.hasNamespace(ns) ) {
                    throw new IllegalArgumentException
                        ("Cannot create table. Namespace does not exist: " +
                            NameUtils.switchToExternalUse(ns));
                }
            }
        }

        @Override
        public void queryOperation(PrepareCallback.QueryOperation
            queryOperation) {
            this.queryOp = queryOperation;
        }

        @Override
        public void ifNotExistsFound() {
        }

        @Override
        public void ifExistsFound() {
        }

        @Override
        public boolean prepareNeeded() {
            return true;
        }

        @Override
        public void isTextIndex() {
        }

        @Override
        public void newTable(Table table) {
        }

        @Override
        public TableMetadataHelper getMetadataHelper() {
            return null;
        }

        @Override
        public void regionName(String regionName) {
        }
    }


    /**
     * Constructs a DdlHandler and executes the statement.
     */
    DdlHandler(String statement, Admin admin, AccessChecker accessChecker) {
        this(statement, admin, null, true, null, accessChecker);
    }

    DdlHandler(String statement, Admin admin,
               String namespace, boolean validateNamespace, TableLimits limits,
               AccessChecker accessChecker) {
        this.admin = admin;
        this.statement = statement;
        this.statementFactory = new DdlStatementFactory();
        this.ddlOpExecutor = new DdlOperationExecutor(this, accessChecker);
        this.limits = limits;
        this.tableMetadata = admin.getTableMetadata();

        /*
         * This "compiles" the statement. The result of the compilation is the
         * creation of a DdlOperation, which is pointed-to by this.ddlOperation.
         */
        try {
            PrepareCallback pc =
                new ValidateNsPrepareCallback(namespace, validateNamespace,
                    tableMetadata);

            /*
             * Always allow CRDT in executing ddl on server side, whether allow
             * CRDT has been checked in compiling ddl on client side.
             */
            ExecuteOptions options = new ExecuteOptions().setAllowCRDT(true);

            query = CompilerAPI.compile(statement.toCharArray(), options,
                tableMetadata, statementFactory, namespace, pc);
            success = query.succeeded();
        } catch (IllegalArgumentException iae) {
            errorString = iae.getMessage();
            success = false;
            query = null;
        }

        /*
         * If compilation was successful, execute the DDL operation.
         */
        handleResults();
    }

    /**
     * A constructor used only by the DdlSyntaxTest and QueryTest, in
     * the table/query package.
     */
    public DdlHandler(String statement,
                      String namespace,
                      TableMetadata tableMetadata) {

        this.admin = null;
        this.statement = statement;
        this.statementFactory = new DdlStatementFactory();
        this.ddlOpExecutor = null;
        this.limits = null;
        this.tableMetadata = tableMetadata;

        /*
         * This parses and executes the statement.
         */
        PrepareCallback pc = (namespace == null ? null :
            new ValidateNsPrepareCallback(namespace, true,
                tableMetadata));
        query = CompilerAPI.compile(statement.toCharArray(),
            null /* ExecuteOptions */, tableMetadata,
            statementFactory, namespace, pc);
        success = query.succeeded();

        if (!success) {
            errorString = query.getErrorMessage();
        }
    }

    /**
     * Returns if the operation succeeded or not.
     */
    public boolean getSuccess() {
        return success;
    }

    /**
     *  Get the exception, if any, from the compilation of the statement.
     */
    public RuntimeException getException() {
        return query.getException();
    }

    /**
     * Returns an error String if an error occurred (!success), null if not.
     */
    String getErrorMessage() {
        return errorString;
    }

    /**
     * Returns a result string if the operation is synchronous and returns
     * a result, and it was successful, otherwise null.
     */
    String getResultString() {
        return resultString;
    }

    void setResultString(String resultStr) {
        this.resultString = resultStr;
    }

    /**
     * Returns a plan ID if the operation resulted in an executed plan
     * (hasPlan == true), otherwise 0 (undefined).
     */
    int getPlanId() {
        return planId;
    }

    /**
     * Returns whether the operation can be retried.  This value is only valid
     * on errors.
     */
    boolean canRetry() {
        return canRetry;
    }

    /**
     * Returns true if the operation resulted in plan execution.
     */
    boolean hasPlan() {
        return hasPlan;
    }

    /**
     * Return the TableImpl object associated with a TableDdlOperation, or null
     * if the ddlOperation is not a TableDdlOperation.
     *
     * This is public so it can be used by tests.
     */
    public TableImpl getTable() {
        return (ddlOperation instanceof TableDdlOperation ?
                ((TableDdlOperation)ddlOperation).getTable() :
                null);
    }
    /**
     * Returns true if this is a table create
     */
    boolean isTableCreate() {
        return (ddlOperation instanceof TableDdlOperation.CreateTable);
    }

    /**
     * Returns true if this is a table evolve
     */
    boolean isTableEvolve() {
        return (ddlOperation instanceof TableDdlOperation.EvolveTable);
    }

    /**
     * Returns true if this is a table drop statement.
     */
    boolean isTableDrop() {
        return (ddlOperation instanceof TableDdlOperation.DropTable);
    }

    /**
     * Returns true if this is an index add statement.
     */
    boolean isIndexAdd() {
        return (ddlOperation instanceof TableDdlOperation.CreateIndex);
    }

    /**
     * Returns true if this is an index drop statement.
     */
    boolean isIndexDrop() {
        return (ddlOperation instanceof TableDdlOperation.DropIndex);
    }

    /**
     * Returns true if the operation was a describe.
     */
    boolean isDescribe() {
        return (ddlOperation instanceof TableDdlOperation.DescribeTable);
    }

    /**
     * Returns true if the operation was a show
     */
    boolean isShow() {
        return (ddlOperation instanceof TableDdlOperation.ShowTableOrIndex ||
                ddlOperation instanceof SecurityDdlOperation.ShowUser ||
                ddlOperation instanceof SecurityDdlOperation.ShowRole);
    }

    Admin getAdmin() {
        return admin;
    }

    /**
     * Tell the DDLHandler that the statement has finished successfully
     */
    void operationSucceeds() {
        success = true;
    }

    /**
     * Tell the DDLHandler that the statement has failed, and record failure
     * information
     * @param errMsg error message to set in ddlhandler
     */
    void operationFails(String errMsg) {
        success = false;
        errorString = errMsg;
    }

    /**
     * Handles the result of the parse.  If it was successful a plan may need
     * to be created and executed.  If so, do that.  If the operation is
     * synchronous then there is nothing to do other than create the
     * resultString.  If the operation failed, the errorString is set.  Note
     * that in a secure kvstore, a security check will be done before the real
     * execution of operations.
     */
    private void handleResults() {

        /*
         * Handle parse and parse tree processing errors
         */
        if (!success) {
            if (errorString == null && query != null) {
                errorString = query.getErrorMessage();
            }
            return;
        }

        if (ddlOperation == null) {
            throw new QueryStateException("Problem parsing " + statement +
                                          ": " + errorString);
        }
        ddlOpExecutor.execute(ddlOperation);
    }

    void approveAndExecute(int planId1) {
        this.planId = planId1;
        approveAndExecute();
    }

    /**
     * Approves and executes the plan.  If execution fails, cancel the plan.
     *
     * A hole exists in plan execution, where concurrent statement execution
     * can cause a spurious exception for the IF NOT EXISTS statement.
     *
     * But the lack of idempotency in table plans causes these
     * problems. Specifically, ddl execution consists of two steps:
     *
     * 1) parsing/metadata checks
     * 2) plan execution
     *
     * When creating a table or index, step 1 complains if the table or index
     * is already in the metadata. But because plan execution isn't fully
     * idempotent, the following interleaving can happen with concurrent
     * statements:
     *
     * Statement A does parsing/metadata check
     * Statement A' does parsing/metadata check
     * Statement A executes plan, completes
     * Statement A' executes plan, but gets error because the plan isn't
     * idempotent -- i.e. the index exists, etc.
     *
     * This is akin to the problem if a index or table creation is cancelled
     * before the index or table becomes READY. In that case, the plan must be
     * re-executed, but because there's been no cleanup, and also because the
     * plan is not idempotent, there's no easy way to make progress.
     */
    void approveAndExecute() {
        assert planId != 0;
        try {
            admin.approvePlan(planId);
            planId = admin.executePlanOrFindMatch(planId);
            hasPlan = true;
        } catch (IllegalCommandException ice) {
            cleanFailedDdlPlan(ice);
            canRetry = false; /* this error not to be tried again */
        } catch (PlanLocksHeldException plhe) {
            cleanFailedDdlPlan(plhe);
            canRetry = true; /* this error can be tried again */
        }
    }

    /* Clean up the failed DDL plan */
    private void cleanFailedDdlPlan(Exception e) {
        /*
         * Plan execution usually fails because there's a running plan.
         * If this happens, cancel the current plan.
         */
        errorString = "Failed to execute plan: " + e.getMessage();
        /* don't let this throw past here */
        try {
            admin.cancelPlan(planId);
        } catch (Exception ignore) {
            /* ignore */
        }
        planId = 0;
        success = false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Success: ");
        sb.append(success);
        if (success) {
            if (hasPlan) {
                sb.append(", plan succeeded: ");
                sb.append(planId);
            }
            if (resultString != null) {
                sb.append(", success, result string: ");
                sb.append(resultString);
            }
        } else {
            sb.append(", operation failed: ");
            sb.append(errorString);
        }
        return sb.toString();
    }

    private class DdlStatementFactory implements StatementFactory {

        @Override
        public void createTable(TableImpl table,
                                boolean ifNotExists,
                                SequenceDef sequenceDef) {
            ddlOperation =
                new TableDdlOperation.CreateTable(table,
                                                  limits,
                                                  ifNotExists,
                                                  sequenceDef);
        }

        @Override
        public void dropTable(String namespace,
                              String tableName,
                              TableImpl table,
                              boolean ifExists) {
            ddlOperation = new TableDdlOperation.DropTable(namespace,
                                                           tableName,
                                                           table,
                                                           ifExists);
        }

        @Override
        public void createIndex(String namespace,
                                String tableName,
                                TableImpl table,
                                String indexName,
                                String[] fieldArray,
                                FieldDef.Type[] typeArray,
                                boolean indexNulls,
                                boolean isUnique,
                                AnnotatedField[] annotatedFields,
                                Map<String,String> properties,
                                String indexComment,
                                boolean ifNotExists,
                                boolean override) {

            ddlOperation = new TableDdlOperation.CreateIndex(
                table, namespace, tableName, indexName, fieldArray,
                typeArray, indexNulls, isUnique,
                annotatedFields, properties, indexComment,
                ifNotExists, override);
        }

        @Override
        public void dropIndex(String namespace,
                              String tableName,
                              TableImpl table,
                              String indexName,
                              boolean ifExists,
                              boolean override) {

            ddlOperation = new TableDdlOperation.DropIndex(
                namespace, tableName, table, indexName, ifExists, override);
        }

        @Override
        public void evolveTable(TableImpl table, int tableVersion) {
            ddlOperation = new TableDdlOperation.EvolveTable(table, tableVersion);
        }

        @Override
        public void describeTable(String namespace,
                                  String tableName,
                                  String indexName,
                                  List<List<String>> schemaPaths,
                                  boolean describeAsJson) {

            ddlOperation = new TableDdlOperation.DescribeTable(
                namespace, tableName, indexName, schemaPaths, describeAsJson);
        }

        @Override
        public void showTableOrIndex(String namespace,
                                     String tableName,
                                     boolean showTables,
                                     boolean showIndexes,
                                     boolean asJson) {
            ddlOperation =
                new TableDdlOperation.ShowTableOrIndex(namespace,
                                                       tableName,
                                                       showTables,
                                                       showIndexes,
                                                       asJson);
        }

        @Override
        public void showNamespaces(boolean asJson) {
            ddlOperation =
                new NamespaceDdlOperation.ShowNamespaces(asJson);
        }

        @Override
        public void showRegions(boolean asJson) {
            ddlOperation = new RegionDdlOperation.ShowRegions(asJson);
        }

        /*
         * Security methods that read state
         */
        @Override
        public void showUser(String userName,
                             boolean asJson) {

            ddlOperation = new SecurityDdlOperation.ShowUser(userName, asJson);
        }

        @Override
        public void showRole(String role,
                             boolean asJson) {

            ddlOperation = new SecurityDdlOperation.ShowRole(role, asJson);
        }

        /*
         * Security methods that modify state
         */
        @Override
        public void createUser(String userName,
                               boolean isEnabled,
                               boolean isAdmin,
                               final String pass,
                               Long passLifetimeMillis) {

            final char[] passBytes = resolvePlainPassword(pass);
            ddlOperation =
                new SecurityDdlOperation.CreateUser(userName, isEnabled,
                                                    isAdmin, passBytes,
                                                    passLifetimeMillis);
            SecurityUtils.clearPassword(passBytes);
        }

        /*
         * Security methods that modify state
         */
        @Override
        public void createExternalUser(String userName,
                                       boolean isEnabled,
                                       boolean isAdmin) {

            ddlOperation =
                new SecurityDdlOperation.CreateExternalUser(userName,
                                                            isEnabled,
                                                            isAdmin);
        }

        @Override
        public void alterUser(String userName,
                              Boolean isEnabled,
                              final String pass,
                              boolean retainPassword,
                              boolean clearRetainedPassword,
                              Long passLifetimeMillis) {

            final char[] passBytes =
                (pass != null ? resolvePlainPassword(pass) : null);

            ddlOperation =
                new SecurityDdlOperation.AlterUser(userName,
                                                   isEnabled,
                                                   passBytes,
                                                   retainPassword,
                                                   clearRetainedPassword,
                                                   passLifetimeMillis);

            SecurityUtils.clearPassword(passBytes);
        }

        @Override
        public void dropUser(String userName, boolean cascade) {

            ddlOperation =
                new SecurityDdlOperation.DropUser(userName, cascade);
        }

        @Override
        public void createRole(String role) {

            ddlOperation = new SecurityDdlOperation.CreateRole(role);
        }

        @Override
        public void dropRole(String role) {

            ddlOperation = new SecurityDdlOperation.DropRole(role);
        }

        @Override
        public void grantRolesToUser(String userName,
                                     String[] roles) {
                ddlOperation =
                    new SecurityDdlOperation.GrantRoles(userName,
                                                        roles);

        }

        @Override
        public void grantRolesToRole(String roleName,
                                     String[] roles) {

            ddlOperation = new SecurityDdlOperation.GrantRolesToRole(roleName,
                                                                     roles);
        }

        @Override
        public void revokeRolesFromUser(String userName,
                                        String[] roles) {

                ddlOperation = new SecurityDdlOperation.RevokeRoles(userName,
                                                                    roles);
        }

        @Override
        public void revokeRolesFromRole(String roleName,
                                        String[] roles) {

            ddlOperation =
                new SecurityDdlOperation.RevokeRolesFromRole(roleName,
                                                             roles);
        }

        @Override
        public void grantPrivileges(String roleName,
                                    String namespace,
                                    String tableName,
                                    Set<String> privSet) {

            ddlOperation = new SecurityDdlOperation.GrantPrivileges(roleName,
                                                                    namespace,
                                                                    tableName,
                                                                    privSet);
        }

        @Override
        public void revokePrivileges(String roleName,
                                     String namespace,
                                     String tableName,
                                     Set<String> privSet) {

            ddlOperation = new SecurityDdlOperation.RevokePrivileges(roleName,
                                                                     namespace,
                                                                     tableName,
                                                                     privSet);
        }

        @Override
        public void createNamespace(String namespace, boolean ifNotExists) {
            ddlOperation = new NamespaceDdlOperation.CreateNamespaceOp
                (namespace, ifNotExists);
        }

        @Override
        public void dropNamespace(String namespace, boolean ifExists,
            boolean cascade) {
            ddlOperation = new NamespaceDdlOperation.RemoveNamespaceOp
                (namespace, ifExists, cascade);
        }

        @Override
        public void grantNamespacePrivileges(String roleName,
            String namespace,
            Set<String> privSet) {

            ddlOperation = new SecurityDdlOperation.
                GrantNamespacePrivilegesDdlOp(roleName, namespace, privSet);
        }

        @Override
        public void revokeNamespacePrivileges(String roleName,
            String namespace,
            Set<String> privSet) {

            ddlOperation = new SecurityDdlOperation.
                RevokeNamespacePrivilegesDdlOp(roleName, namespace, privSet);
        }

        @Override
        public void createRegion(String regionName) {
            ddlOperation = new RegionDdlOperation.CreateRegion(regionName);
        }

        @Override
        public void dropRegion(String regionName) {
            ddlOperation = new RegionDdlOperation.DropRegion(regionName);
        }

        @Override
        public void setLocalRegionName(String regionName) {
            ddlOperation = new RegionDdlOperation.SetLocalRegionName(regionName);
        }

        /*
         * TODO: Will be extended to parse other types of authentication. For now
         * we only parse a password by default.
         */
        private char[] resolvePlainPassword(String passStr) {
            /* Tears down the surrounding '"' */
            final char[] result = new char[passStr.length() - 2];
            passStr.getChars(1, passStr.length() - 1, result, 0);
            return result;
        }
    }

    /**
     * Return the ddl operation object.  For testing only.
     */
    DdlOperation getDdlOp() {
        return ddlOperation;
    }

    TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    /**
     * Runs a ddl operation.  In a secured kvstore, the permission for the
     * specific ddl operation will be checked first.
     */
    static class DdlOperationExecutor {
        private final AccessChecker accessChecker;
        private final DdlHandler ddlHandler;

        DdlOperationExecutor(DdlHandler ddlHandler,
                             AccessChecker accessChecker) {
            this.accessChecker = accessChecker;
            this.ddlHandler = ddlHandler;
        }

        void execute(DdlOperation ddlOp)
            throws SessionAccessException, ClientAccessException {

            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx != null && accessChecker != null) {
                try {
                    accessChecker.checkAccess(exeCtx, ddlOp.getOperationCtx());
                } catch (KVSecurityException kvse) {
                    throw new ClientAccessException(kvse);
                }
            }
            try {
                ddlOp.perform(ddlHandler);
            } catch (IllegalArgumentException ex) {
                throw new WrappedClientException(ex);
            }
        }
    }

    /**
     * A class wrapping a ddl operation with the proper OperationContext which
     * will be checked in a secure kvstore.
     */
    public interface DdlOperation {

        /**
         * Returns the operation context used for security check of this ddl
         * operation.
         */
        OperationContext getOperationCtx();

        /**
         * Performs the operation.
         */
        void perform(DdlHandler ddlHandler);
    }
}
