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

/**
 * This annotations marks an class whose declared methods are not, in general,
 * freely accessible, and for which the AuthContext argument will typically
 * need to be examined.  The scope of this annotation applies only to the
 * declared methods of this class.  A superclass or derived class may choose
 * an alternate annotation for its declared methods.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SecureAPI {
    /**
     * If trusted=true, this interface is not permitted to have method-level
     * annotations as the developer is asserting that the interface will be
     * authenticated and authorized at a higher level.  If trusted=false,
     * each interface method must be individually annotated.
     */
    boolean trusted() default false;
}
