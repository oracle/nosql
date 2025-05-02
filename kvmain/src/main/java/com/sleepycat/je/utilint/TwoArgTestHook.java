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

package com.sleepycat.je.utilint;

import com.sleepycat.je.rep.impl.node.Feeder.ExitException;

import java.io.IOException;

/**
 * Similar to TestHook except that it takes two arguments as test hook input.
 *
 * @see TestHook
 */
public interface TwoArgTestHook<T,S> {
    public void doHook(T arg1, S arg2);
    public void doExitandIOHook(T arg1, S arg2) throws ExitException,
                                                       IOException;
}

