/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

import oracle.kv.KVStore;
import oracle.nosql.common.qtf.QTOptions;

public class QTOptionsKV extends QTOptions {

    KVStore store;

    public KVStore getStore() {
        return store;
    }

    public void setStore(KVStore store) {
        this.store = store;
    }
}
