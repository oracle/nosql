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

package com.sleepycat.je.tree;

import com.sleepycat.je.utilint.NotSerializable;

/**
 * Indicates that we need to return to the top of the tree in order to
 * do a forced splitting pass.  A checked exception is used to ensure that it
 * is handled internally and not propagated through the API.
 */
@SuppressWarnings("serial")
class SplitRequiredException extends Exception implements NotSerializable {
    public SplitRequiredException() {
    }
}
