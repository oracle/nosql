/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt;

import qt.framework.QTDefaultImpl;

/**
 * Setup implementation for cases that depend on //data2.
 */
public class Data2Setup extends QTDefaultImpl {
    @Override
    public void before() {
        opts.verbose("Before:  Data2");
    }

    @Override
    public void after() {
        opts.verbose("After: Data2");
    }
}