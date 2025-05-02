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

package oracle.kv.impl.security.util;

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.impl.security.KVStorePrincipal;

/**
 * Utilities for managing Subjects uses by KVStore.
 */
public class SubjectUtils {

    /**
     * Creates a read-only subject that contains the specified set of
     * principals that implement KVStorePrincipal. The resulting subject will
     * not have any public or private credentials.
     *
     * @param principals the principals
     * @return the subject
     */
    public static Subject createSubject(Set<KVStorePrincipal> principals) {
        return new Subject(true /* readOnly */,
                           new HashSet<>(principals),
                           new HashSet<>() /* publicCreds */,
                           new HashSet<>() /* privateCreds */);
    }

    /**
     * Reads a read-only subject that contains principals that implement
     * KVStorePrincipal and no public or private credentials from the input
     * stream.
     *
     * @param in the input stream
     * @param serialVersion the serial version
     * @return the subject
     * @throws IOException if an I/O error occurs
     */
    public static Subject readSubject(DataInput in, short serialVersion)
        throws IOException
    {
        return createSubject(
            readCollection(in, serialVersion,
                           HashSet::new,
                           KVStorePrincipal::readPrincipal));
    }

    /**
     * Writes a subject to the output stream. Throws an exception if the
     * subject contains principals that do not implement KVStorePrincipal or if
     * it contains public or private credentials.
     *
     * @param subject the subject
     * @param out the output stream
     * @param sv the serial version
     * @throws IOException if an I/O error occurs
     */
    public static void writeSubject(Subject subject, DataOutput out, short sv)
        throws IOException
    {
        if (!subject.getPublicCredentials().isEmpty() ||
            !subject.getPrivateCredentials().isEmpty()) {
            throw new IllegalStateException(
                "Attempt to write a subject that contains credentials: " +
                subject);
        }
        final Set<Principal> principals = subject.getPrincipals();
        for (final Principal principal : principals) {
            if (!(principal instanceof KVStorePrincipal)) {
                throw new IllegalStateException(
                    "Attempt to write a subject containing a principal that" +
                    " does not implement KVStorePrincipal: " + subject);
            }
        }
        writeCollection(out, sv, principals,
                        (e, outp, sver) ->
                        ((KVStorePrincipal) e).writePrincipal(outp, sver));
    }
}
