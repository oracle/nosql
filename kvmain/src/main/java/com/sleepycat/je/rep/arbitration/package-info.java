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

/**
 * INTERNAL: Manages the designated primary of a two-node group -- this is
 * NOT the {@link com.sleepycat.je.rep.arbiter arbiter node}.
 */
package com.sleepycat.je.rep.arbitration;