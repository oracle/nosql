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

import static oracle.kv.impl.admin.CommandJsonUtils.formatList;

import java.util.List;
import java.util.Set;

import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SystemPrivilege;

/**
 * This class represents namespaces related operations
 */
public abstract class NamespaceDdlOperation implements DdlHandler.DdlOperation {
    private final String opName;
    private final String namespace;

    protected NamespaceDdlOperation(String opName, String namespace) {
        this.opName = opName;
        this.namespace = namespace;
    }

    String opName() {
        return opName;
    }

    public String getNamespace() {
        return namespace;
    }

    /**
     * Operation of creating a namespace, needing CREATE_ANY_NAMESPACE
     * privilege.
     */
    public static class CreateNamespaceOp extends NamespaceDdlOperation {
        private final boolean ifNotExists;

        public CreateNamespaceOp(String namespace, boolean ifNotExists) {
            super("CREATE NAMESPACE", namespace);
            this.ifNotExists = ifNotExists;
        }

        @Override
        public OperationContext getOperationCtx() {
            return new OperationContext() {
                @Override
                public String describe() {
                    return opName() + (ifNotExists ? " IF NOT EXISTS " : " ") +
                        getNamespace();
                }

                @Override
                public List<? extends KVStorePrivilege>
                getRequiredPrivileges() {
                    return SystemPrivilege.namespaceCreatePrivList;
                }
            };
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            String exceptionMsg;
            try {
                final Admin admin = ddlHandler.getAdmin();

                final TableMetadata md = ddlHandler.getTableMetadata();
                if (ifNotExists && md.hasNamespace(getNamespace())) {
                    ddlHandler.operationSucceeds();
                    return;
                }

                final int planId = admin.getPlanner().createAddNamespacePlan
                    ("CreateNamespace", getNamespace());
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (NamespaceAlreadyExistsException naee) {
                if (ifNotExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = naee.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed for namespace '" +
                getNamespace() + "': " + exceptionMsg);
        }
    }

    /**
     * Operation of dropping a namespace, needing DROP_ANY_NAMESPACE system
     * privilege.
     */
    public static class RemoveNamespaceOp extends NamespaceDdlOperation {
        private final OperationContext opCtx;
        private final boolean ifExists;
        private final boolean cascade;

        public RemoveNamespaceOp(String namespace,
                                 boolean ifExists,
                                 boolean cascade) {

            super("DROP NAMESPACE", namespace);
            this.ifExists = ifExists;
            this.cascade = cascade;

            opCtx = new OperationContext() {
                @Override
                public String describe() {
                    return opName() + (ifExists ? " IF EXISTS " : " ") +
                        getNamespace();
                }

                @Override
                public List<? extends KVStorePrivilege>
                getRequiredPrivileges() {
                    return SystemPrivilege.namespaceDropPrivList;
                }
            };
        }

        @Override
        public OperationContext getOperationCtx() {
            return opCtx;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            String exceptionMsg;
            try {
                final int planId = admin.getPlanner().createRemoveNamespacePlan
                    ("DropNamespace", getNamespace(), cascade);
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (NamespaceNotFoundException nnfe) {
                if (ifExists) {
                    ddlHandler.operationSucceeds();
                    return;
                }
                exceptionMsg = nnfe.getMessage();
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed for namespace '" +
                getNamespace() + "': " + exceptionMsg);
        }
    }


    /**
     * Operation of showing all available namespaces, needing DBVIEW privilege.
     */
    public static class ShowNamespaces extends
        oracle.kv.impl.admin.NamespaceDdlOperation {

        private final boolean asJson;

        public ShowNamespaces(
            boolean asJson) {
            super("ShowNamespaces", null /* no namespace */);
            this.asJson = asJson;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {

            String resultString = null;
            final TableMetadata md = ddlHandler.getTableMetadata();

            /* show tables */
            Set<String> namespaceList = md.listNamespaces();
            resultString = formatList("namespaces", namespaceList, asJson);
            ddlHandler.operationSucceeds();
            ddlHandler.setResultString(resultString);
        }

        @Override
        public OperationContext getOperationCtx() {
            return new OperationContext() {
                @Override
                public String describe() {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("SHOW");
                    if (asJson) {
                        sb.append(" AS JSON");
                    }
                    sb.append(" NAMESPACES");
                    return sb.toString();
                }
                @Override
                public List<? extends KVStorePrivilege>
                getRequiredPrivileges() {
                    return SystemPrivilege.dbviewPrivList;
                }
            };
        }
    }
}

