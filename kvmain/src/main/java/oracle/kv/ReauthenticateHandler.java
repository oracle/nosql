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

package oracle.kv;

/**
 * A callback interface used when KVStore authentication has expired and
 * requires renewal. When an application calls the KVStoreFactory.getStore()
 * method and passes in a LoginCredentials object, it may also pass in an
 * instance of an object that implements this interface. The object will be
 * used to re-establish a login without interrupting the sequence of 
 * KVStore calls.
 *
 * @since 3.0
 */
public interface ReauthenticateHandler {
    /**
     * Attempts to re-authenticate a kvstore handle.  When an instance of this
     * interface is provided in a call to KVStoreFactory.getStore(), this
     * method is called by the KVStore client to perform the re-authentication
     * needed to continue an operation on the store that would otherwise
     * result in an AuthenticationRequiredException being thrown.  The
     * implementation should call KVStore.login() with valid credentials that
     * match the user identity that was provided when the store was opened.
     * If this method returns without throwing an exception but without
     * successfully performing a re-authentication, the original
     * AuthenticationRequiredException is re-thrown, bypassing this
     * ReauthenticationHandler instance.
     *
     * <p>This method should typically handle any {@link FaultException}s
     * thrown by any calls made by its implementation.  Any runtime exceptions
     * thrown by this method will cause the original operation to fail by
     * rethrowing an exception that will be seen by the application.  If the
     * FaultException thrown by the  reauthenticate method is a
     * KVSecurityException then the exception re-thrown to the application is
     * the original AuthenticationRequiredException, otherwise the exception
     * thrown by the reauthenticate method is re-thrown to the application.
     *
     * @param kvstore The KVStore instance that requires reauthentication
     * @throws RuntimeException if the reauthentication failed
     */

    void reauthenticate(KVStore kvstore)
        throws RuntimeException;
}
