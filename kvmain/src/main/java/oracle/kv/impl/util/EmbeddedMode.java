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

package oracle.kv.impl.util;

import java.io.PrintStream;
import java.io.OutputStream;

/**
 * Manages variables related to embedded mode.
 *
 * The KVLite can be started either from command line or by user application.
 *
 * If KVLite is started from command line, it lives in it's own JVM.
 * This KVLite is in non-embedded mode.
 * If KVLite is started by user application, it lives in the same JVM as the
 * application that starts it. This KVLite is in embedded mode.
 */
/*
 * TODO: Remove this class and revert changes related to it now that KVLocal
 * works in process mode. [KVSTORE-1298]
 */
public class EmbeddedMode {
     /**
      * A PrintStream that can be used to consume output silently.
      */
     public static final PrintStream NULL_PRINTSTREAM =
        new PrintStream(new OutputStream() {
            @Override
            public void write(int b) { }
        });

    /* The default mode is non-embedded. */
    private static volatile boolean embedded = false;

    private EmbeddedMode() { }

    /**
     * Returns whether the KVLite is started in embedded mode.
     */
    public static boolean isEmbedded() {
        return embedded;
    }

     public static void setEmbedded(boolean isEmbedded) {
        embedded = isEmbedded;
    }
}
