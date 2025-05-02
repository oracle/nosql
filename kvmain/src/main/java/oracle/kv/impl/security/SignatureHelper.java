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
package oracle.kv.impl.security;

/**
 * A common interface implemented by object signature signer and verifier.
 *
 * @param <T> type of object to be performed with signature operations
 */
public interface SignatureHelper<T> {

    /**
     * Returns an array of bytes representing the signature of an object
     *
     * @param object
     * @throws SignatureFaultException if any issue happens in generating the
     * signature
     */
    byte[] sign(T object) throws SignatureFaultException;

    /**
     * Verifies the integrity of the specified object using the given
     * signature.
     *
     * @param object
     * @param sigBytes signature to be verified
     * @return true if the signature was verified, false if not.
     * @throws SignatureFaultException f a configuration problem prevented
     * signature verification from being performed
     */
    boolean verify(T object, byte[] sigBytes) throws SignatureFaultException;
}
