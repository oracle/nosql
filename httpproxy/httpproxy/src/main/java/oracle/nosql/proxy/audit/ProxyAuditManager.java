/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.proxy.audit;

import java.util.logging.Level;

import oracle.nosql.audit.AuditContext;
import oracle.nosql.audit.Auditor;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.protocol.Protocol.OpCode;

/**
 * To manage OCI audit on Proxy.
 */
public abstract class ProxyAuditManager {
    private static final String PROXY_OCI_AUDIT_MANAGER_CLASS =
        "oracle.nosql.proxy.audit.oci.ProxyOCIAuditManager";

    private final Auditor auditor;
    protected SkLogger logger;

    public ProxyAuditManager() {
        auditor = getAuditor();
    }

    /**
     * @return ProxyAuditContextBuilder ready for new request.
     */
    public ProxyAuditContextBuilder startAudit() {
        return newContextBuilder();
    }

    /**
     * Audit the context.
     * @param context to be audited.
     * @return true if context is audited successfully.
     */
    public boolean endAudit(ProxyAuditContextBuilder builder) {
        if (builder == null) {
            return false;
        }
        try {
            final AuditContext[] contexts = builder.build();
            if (contexts == null) {
                return false;
            }
            for (AuditContext context : contexts) {
                if (context != null) {
                    auditor.audit(context);
                }
            }
            return true;
        } catch (Throwable e) {
            if (logger != null) {
                logger.log(Level.SEVERE, "Unexpected audit error", e);
            }
            return false;
        }
    }

    /**
     * Updates the logger used by this instance.
     */
    public void setLogger(SkLogger logger) {
        this.logger = logger;
    }

    /**
     * @return true if the operation is allowed to be audited.
     */
    public abstract boolean isAllowed(OpCode op);

    /**
     * @return Auditor will be used by ProxyAuditManager.
     */
    protected abstract Auditor getAuditor();

    /**
     * @return ProxyAuditContextBuilder will be used by ProxyAuditManager.
     */
    protected abstract ProxyAuditContextBuilder newContextBuilder();

    /**
     * Create proxy audit manager.
     */
    public static ProxyAuditManager createProxyOCIAuditManager() {
        Class<?> auditClass;
        try {
            auditClass = Class.forName(PROXY_OCI_AUDIT_MANAGER_CLASS);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                "Unable to find ProxyOCIAuditManager class", cnfe);
        }

        try {
            return (ProxyAuditManager) auditClass.getDeclaredConstructor().
                                                  newInstance();
        } catch (Throwable t) {
            throw new IllegalArgumentException(
                "Unable to create ProxyOCIAuditManager instance", t);
        }
    }
}
