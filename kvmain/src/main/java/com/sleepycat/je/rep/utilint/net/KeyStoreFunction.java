/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.utilint.net;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

/**
 * A function that processes an input stream associated with a keystore, with
 * the ability to throw {@link IOException} and {@link
 * GeneralSecurityException}.
 *
 * @param <R> the return type
 */
@FunctionalInterface
public interface KeyStoreFunction<R> {

    /**
     * Processes the specified input stream associated with a keystore and
     * returns a result.
     *
     * @param inputStream the input stream associated with a keystore
     * @return the result
     * @throws IOException if there is a problem reading data from the input
     * stream
     * @throws GeneralSecurityException if there is a problem with the keystore
     */
    R apply(InputStream inputStream)
        throws IOException, GeneralSecurityException;
}
