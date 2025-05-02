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
package oracle.kv.impl.security.wallet;

import java.io.File;

import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;

/**
 * Provides an implementation of PasswordManager that uses the Oracle wallet
 * mechanism.
 */
public class WalletManager extends PasswordManager {

    @Override
    public PasswordStore getStoreHandle(File storeLocation) {
        return new WalletStore(storeLocation);
    }

    @Override
    public boolean storeLocationIsDirectory() {
        return true;
    }
}
