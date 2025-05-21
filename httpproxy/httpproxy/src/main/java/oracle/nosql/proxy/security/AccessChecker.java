/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.security;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.util.ph.HealthStatus;

import java.util.List;

import org.checkerframework.checker.nullness.qual.Nullable;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

/**
 * An interface used for checking access rights at request basis on proxy.
 */
public interface AccessChecker {

    /**
     * Given HTTP method and headers extracted from a HTTP request, checks that
     * the invocation of the operation against resources is valid. This method
     * should be called before the actual operation execution. The
     * implementation must be re-entrant.
     *
     * @param httpMethod HTTP method
     * @param requestUri request URI
     * @param httpHeaders HTTP headers
     * @param opCode a valid operation code that identifies the operation.
     * @param compartmentId compartmentId
     * @param tableNameOrId the table associated with the operation, it may be
     * null for operations that are not table-specific.
     * @param callerActx the AccessContext passed in by caller
     * @return AccessContext the object contains requesting subject information,
     * which contains tenant id, principal id and permissions associated.
     * @throws RequestException the errors occurred during access checking.
     * All errors are thrown as this type of exception but may with different
     * error code.<p>
     * <li>{@link BinaryProtocol#INVALID_AUTHORIZATION} that indicates
     * security token in authorization header is invalid cannot be verified.
     * </li><li>
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} indicates security
     * token is valid but the subject represented by this token doesn't have
     * the permission to perform the operation.</li><li>
     * {@link BinaryProtocol#SECURITY_INFO_UNAVAILABLE} indicates errors
     * occurred during interactions with IDCS authorization service. They
     * should be handled as retryable error.</li>
     */
    public AccessContext checkAccess(HttpMethod httpMethod,
                                     String requestUri,
                                     HttpHeaders headers,
                                     OpCode opCode,
                                     @Nullable String compartmentId,
                                     @Nullable String tableNameOrId,
                                     @Nullable AccessContext callerActx,
                                     Filter filter,
                                     LogContext lc)
        throws RequestException;

    /**
     * Given HTTP method, headers and paylod extracted from a HTTP request,
     * checks that the invocation of the operation against resources is valid.
     * This method should be called before the actual operation execution. The
     * implementation must be re-entrant. This method also must return the check
     * results immediately without throwing a retryable exception.
     *
     * @param httpMethod HTTP method
     * @param requestUri request URI
     * @param httpHeaders HTTP headers
     * @param opCode a valid operation code that identifies the operation.
     * @param compartmentId compartmentId
     * @param tableNameOrId the table associated with the operation, it may be
     * null for operations that are not table-specific.
     * @param payload the request payload
     * @param callerActx the AccessContext passed in by caller
     * @return AccessContext the object contains requesting subject information,
     * which contains tenant id, principal id and permissions associated.
     * @throws RequestException the errors occurred during access checking.
     * All errors are thrown as this type of exception but may with different
     * error code.<p>
     * <li>{@link BinaryProtocol#INVALID_AUTHORIZATION} that indicates
     * security token in authorization header is invalid cannot be verified.
     * </li><li>
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} indicates security
     * token is valid but the subject represented by this token doesn't have
     * the permission to perform the operation.</li><li>
     */
    public AccessContext checkAccess(HttpMethod httpMethod,
                                     String uri,
                                     HttpHeaders headers,
                                     OpCode op,
                                     @Nullable String compartmentId,
                                     @Nullable String tableNameOrId,
                                     @Nullable byte[] payload,
                                     @Nullable AccessContext callerActx,
                                     Filter filter,
                                     LogContext lc)
        throws RequestException;

    /**
     * Given HTTP method, headers and paylod extracted from a HTTP request,
     * checks that the invocation of the work request operation is valid.
     * This method should be called before the actual operation execution. The
     * implementation must be re-entrant. This method also must return the check
     * results immediately without throwing a retryable exception.
     *
     * @param httpMethod HTTP method
     * @param requestUri request URI
     * @param httpHeaders HTTP headers
     * @param opCode a work request operation code
     * @param authorizeOps the sub operations to be authorized. If not provided,
     * check the authorization of {@code opCode}. The sub operations must be
     * valid sub operation of {@code opCode}.
     * @param shouldAuthorizeAllOps set to true if requires all the sub
     * operations specified in authorizeOps must be authorized, if any of
     * sub operations is not authorized,
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} error will be returned.
     * If false, the authorized sub operations will be returned with
     * {@link AccessContext#getAuthorizedOps}, if none of sub operations is
     * authorized, {@link BinaryProtocol#INSUFFICIENT_PERMISSION} error will
     * be returned.
     * This flag will be ignored when {@code authorizeOps} is not provided, in
     * which case {@code opCode} will always be checked for authorization.
     * @param compartmentId compartmentId
     * @param workRequestId work request id
     * @param payload the request payload
     * @return AccessContext the object contains requesting subject information,
     * which contains tenant id, principal id and permissions associated.
     * @throws RequestException the errors occurred during access checking.
     * All errors are thrown as this type of exception but may with different
     * error code.<p>
     * <li>{@link BinaryProtocol#INVALID_AUTHORIZATION} that indicates
     * authorization header is invalid cannot be verified.
     * </li><li>
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} indicates authorization
     * header is valid but the caller subject doesn't have the permission to
     * perform the operation.</li><li>
     */
    public default AccessContext checkWorkRequestAccess(
        HttpMethod httpMethod,
        String uri,
        HttpHeaders headers,
        OpCode opCode,
        @Nullable OpCode[] authorizeOps,
        boolean shouldAuthorizeAllOps,
        @Nullable String compartmentId,
        @Nullable String workRequestId,
        @Nullable byte[] payload,
        Filter filter,
        LogContext lc)
        throws RequestException {

        return null;
    }

    /**
     * Given HTTP method, headers and paylod extracted from a HTTP request,
     * checks that the invocation of the configuration operation is valid.
     * This method should be called before the actual operation execution. The
     * implementation must be re-entrant. This method also must return the check
     * results immediately without throwing a retryable exception.
     *
     * @param httpMethod HTTP method
     * @param requestUri request URI
     * @param httpHeaders HTTP headers
     * @param opCode a work request operation code
     * @param authorizeOps the sub operations to be authorized. If not provided,
     * check the authorization of {@code opCode}. The sub operations must be
     * valid sub operations of {@code opCode}. If any of sub operations is not
     * authorized, {@link BinaryProtocol#INSUFFICIENT_PERMISSION} error will
     * be returned.
     * @param compartmentId compartmentId
     * @param payload the request payload
     * @param filter the filter interface to block request if needed
     * @param lc the log context object
     * @return AccessContext the object contains requesting subject information,
     * which contains tenant id, principal id and permissions associated.
     * @throws RequestException the errors occurred during access checking.
     * All errors are thrown as this type of exception but may with different
     * error code.<p>
     * <li>{@link BinaryProtocol#INVALID_AUTHORIZATION} that indicates
     * authorization header is invalid cannot be verified.
     * </li><li>
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} indicates authorization
     * header is valid but the caller subject doesn't have the permission to
     * perform the operation.</li><li>
     */
    public default AccessContext checkConfigurationAccess(
            HttpMethod httpMethod,
            String uri,
            HttpHeaders headers,
            OpCode opCode,
            @Nullable OpCode[] authorizeOps,
            String compartmentId,
            @Nullable byte[] payload,
            Filter filter,
            LogContext lc) {
        return null;
    }

    /**
     * Close access checker, stop threads and release resources used
     * by access checker.
     */
    public default void close() {
        return;
    }

    /**
     * Configure logger to this access checker.
     * @param logger logger
     */
    public default void setLogger(SkLogger logger) {
        return;
    }

    /**
     * Set tenant manager.
     * @param tm tenant manager
     */
    public default void setTenantManager(TenantManager tm) {
        return;
    }

    /**
     * Reset all table name mapping in cache.
     * @param fqtn compartmentId/table name
     */
    public default void resetTableNameMapping(String fqtn) {
        return;
    }

    /**
     * Check the network connectivity to all components it rely on.
     * @param errors to add connectivity error if have.
     */
    public default HealthStatus checkConnectivity(List<String> errors,
                                                  SkLogger logger) {
        if (logger != null) {
            logger.fine("Return default status GREEN");
        }
        return HealthStatus.GREEN;
    }

    public default void setLogControl(LogControl lc) {
        return;
    }

    /**
     * Given HTTP method and headers extracted from a HTTP request, checks that
     * the invocation of the operation is from NoSQL service. This method should
     * be called before the internal cross region operations execution. The
     * implementation must be re-entrant.
     *
     * @param httpMethod HTTP method
     * @param requestUri request URI
     * @param httpHeaders HTTP headers
     * @param opCode a valid operation code that identifies the operation
     * @param lc log context
     *
     * @throws RequestException the errors occurred during access checking.
     * All errors are thrown as this type of exception but may with different
     * error code.<p>
     * <li>
     * {@link BinaryProtocol#INSUFFICIENT_PERMISSION} indicates the operation is
     * not from NoSQL service.</li>
     */
    public default void checkInternalAccess(HttpMethod httpMethod,
                                            String requestUri,
                                            HttpHeaders headers,
                                            OpCode opCode,
                                            LogContext lc) {
        return;
    }

}
