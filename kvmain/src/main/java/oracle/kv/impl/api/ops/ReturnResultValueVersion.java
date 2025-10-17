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

package oracle.kv.impl.api.ops;

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Version;

/**
 * Holds ReturnValueVersion.Choice and ResultValueVersion during result
 * processing.  Initialized with the Choice specified in the request, and the
 * ResultValueVersion is filled in when the operation is complete.
 */
class ReturnResultValueVersion {

    private final Choice returnChoice;
    private ResultValueVersion valueVersion;

    ReturnResultValueVersion(Choice returnChoice) {
        this.returnChoice = returnChoice;
    }

    Choice getReturnChoice() {
        return returnChoice;
    }

    void setValueVersion(byte[] valueBytes,
                         Version version,
                         long expirationTime,
                         long creationTime,
                         long modificationTime,
                         int storageSize) {
        this.valueVersion = new ResultValueVersion(valueBytes,
                                                   version,
                                                   expirationTime,
                                                   creationTime,
                                                   modificationTime,
                                                   storageSize);
    }

    ResultValueVersion getValueVersion() {
        return valueVersion;
    }
}
