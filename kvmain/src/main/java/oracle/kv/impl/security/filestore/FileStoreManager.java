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
package oracle.kv.impl.security.filestore;

import java.io.File;

import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;

/**
 * Manager for FileStore implementation of PasswordStore.
 */
public class FileStoreManager extends PasswordManager {

    @Override
    public PasswordStore getStoreHandle(File storeLocation) {
        return new FileStore(storeLocation);
    }

    @Override
    public boolean storeLocationIsDirectory() {
        return false;
    }
}
