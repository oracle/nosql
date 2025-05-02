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

package oracle.kv.impl.fault;

import oracle.kv.KVVersion;

/**
 * UnknownVersionException is thrown when a Topology with an unknown future
 * version is encountered during the upgrade of an object. The caller can take
 * appropriate action based upon the version information that's returned.
 */
public class UnknownVersionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final KVVersion kvVersion;
    private final int supportedVersion;
    private final int unknownVersion;
    private final String className;

    public UnknownVersionException(String message,
                            String className,
                            int supportedVersion,
                            int unknownVersion) {
        super(message);

        this.kvVersion = KVVersion.CURRENT_VERSION;
        this.supportedVersion = supportedVersion;
        this.unknownVersion = unknownVersion;
        this.className = className;
    }

    @Override
    public String getMessage() {
        return super.getMessage() +
               " Class: " + className +
               " KVVersion: " + kvVersion.toString() +
               " Supported version:" + supportedVersion +
               " Unknown version:" + unknownVersion;
    }

    public KVVersion getKVVersion() {
        return kvVersion;
    }

    public int getUnknownVersion() {
        return unknownVersion;
    }

    public int getSupportedVersion() {
        return supportedVersion;
    }

    public String getClassName() {
        return className;
    }
}