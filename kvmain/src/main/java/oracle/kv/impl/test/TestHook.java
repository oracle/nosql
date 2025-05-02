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

package oracle.kv.impl.test;

/**
 * TestHook is used induce testing behavior that can't be provoked externally.
 * For example, unit tests may use hooks to throw IOExceptions, or to cause
 * waiting behavior.
 *
 * To use this, a unit test should extend TestHook with a class that overrides
 * the desired method. The desired code will have a method that allows the unit
 * test to specify a hook, and will execute the hook if it is non-null.
 * This should be done within an assert like so:
 *
 *    assert TestHookExecute.doHookIfSet(myTestHook, hookArg);
 *
 * See StorewideLoggingView for an example.
 */
public interface TestHook<T> {

    public void doHook(T arg);
}
