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

package oracle.kv.util;

import java.util.logging.Level;

import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * KVStore instances of java.util.logging.Logger are configured to use this
 * implementation of java.util.logging.ConsoleHandler. By default, the
 * handler's level is {@link Level#OFF}. To enable the console output for a

 * KVStore component, as it would be seen on the console of the local node, use
 * the standard java.util.logging.LogManager configuration to set the desired
 * level:
 * <pre>
 * oracle.kv.util.ConsoleHandler.level=ALL
 * </pre>
 * See oracle.kv.util.StoreConsoleHandler for the store wide consolidated
 * logging output as it would be seen on the AdminService.
 */
public class ConsoleHandler extends java.util.logging.ConsoleHandler {

    /*
     * Using a KV specific handler lets us enable and disable output for all
     * kvstore component.
     */
    public ConsoleHandler() {
        super();

        Level level = null;
        String propertyName = getClass().getName() + ".level";

        String levelProperty =
            CommonLoggerUtils.getLoggerProperty(propertyName);
        if (levelProperty == null) {
            level = Level.OFF;
        } else {
            level = Level.parse(levelProperty);
        }

        setLevel(level);
    }
}

