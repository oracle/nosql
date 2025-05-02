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
package oracle.kv.impl.security.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import oracle.kv.impl.security.KVStorePrivilegeLabel;

/**
 * Specifies that the dynamic proxy should use a required AuthContext
 * argument along with the privilege or privileges annotation to validate
 * access. The "Auto" portion of the name is intended to convey that security
 * checking is automatically applied base on the statically declared privilege
 * requirements. For methods that have differing privilege requirements
 * depending on the specific input arguments, this annotation type will not be
 * sufficient, and the method must provide custom checks within the code.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SecureAutoMethod {
    /**
     * A list of privileges that must all be granted to the accessing user if
     * security is in effect in order to call this method.  Although
     * syntactically allowed, the SecureProxy class requires that this list
     * be not null and not empty.
     */
    KVStorePrivilegeLabel[] privileges();
}
