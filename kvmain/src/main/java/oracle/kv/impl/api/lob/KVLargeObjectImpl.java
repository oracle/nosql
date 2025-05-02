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

package oracle.kv.impl.api.lob;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Key;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.ObjectUtil;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.lob.KVLargeObject;

/**
 * Implements the large object interfaces.
 */
public class KVLargeObjectImpl implements KVLargeObject, LOBMetadataKeys {

    /** The KVS handle that owns this component. */
    private volatile KVStoreImpl kvsImpl;

    /**
     * Contains the bytes representing the LOB suffix in a format that's
     * consistent with the key byte representation format. A LOB key will have
     * these bytes as the trailing bytes in the encoded key. It's a non null
     * value of length > 0.
     */
    private volatile byte[] lobSuffixBytes;

    /**
     * Create a placeholder impl object; it's created as a component of the
     * KVSImpl object in its constructor. Users of the constructor must
     * subsequently call setKVSImpl() to establish a bi-directional relationship
     * between the two components.
     */
    public KVLargeObjectImpl() {
        super();
    }

    /**
     * Set to establish a bi-directional relationship between KVSImpl and
     * KVLargeObjectImpl. It must be invoked immediately after the construction
     * of the KVSImpl object. This initializiation, due to its bi-directional
     * nature cannot be done in the above constructor, since it will result in
     * a null KVSImpl.largeObjectImpl iv in the internal handle created by this
     * method.
     */
    public void setKVSImpl(KVStoreImpl kvsImpl) {
        /* Create a handle that allows access to the internal LOB keyspace. */
        this.kvsImpl = (KVStoreImpl) KVStoreImpl.makeInternalHandle(kvsImpl);

        try {
            lobSuffixBytes = kvsImpl.getDefaultLOBSuffix().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Renew KVStore handle login manager.
     */
    public void renewLoginMgr(LoginManager loginMgr) {
        this.kvsImpl.renewLoginManager(loginMgr);
    }

    /**
     * Returns the LOB suffix bytes in effect.
     *
     * @return a non null value of length &gt; 0
     */
    public byte[] getLOBSuffixBytes() {
        return lobSuffixBytes;
    }

    @Override
    public Version putLOB(Key appLobKey,
                          InputStream lobStream,
                          Durability durability,
                          long chunkTimeout,
                          TimeUnit timeoutUnit)
        throws IOException {

        ObjectUtil.checkNull("lobStream", lobStream);

        return new PutOperation(kvsImpl, appLobKey, lobStream, durability,
                                chunkTimeout, timeoutUnit).execute(false, false);
    }

    @Override
    public InputStreamVersion getLOB(Key appLobKey,
                                     Consistency consistency,
                                     long chunkTimeout,
                                     TimeUnit timeoutUnit) {

        return new GetOperation(kvsImpl, appLobKey,
                                consistency,
                                chunkTimeout,
                                timeoutUnit).execute();
    }

    @Override
    public boolean deleteLOB(Key appLobKey,
                             Durability durability,
                             long timeout,
                             TimeUnit timeoutUnit) {

        return new DeleteOperation(kvsImpl, appLobKey, durability,
                                   timeout, timeoutUnit).execute(false);
    }

    @Override
    public Version putLOBIfAbsent(Key appLobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long chunkTimeout,
                                  TimeUnit timeoutUnit)
        throws IOException {

        ObjectUtil.checkNull("lobStream", lobStream);

        return new PutOperation(kvsImpl, appLobKey, lobStream, durability,
                                chunkTimeout, timeoutUnit).execute(false, true);
    }

    @Override
    public Version putLOBIfPresent(Key appLobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long chunkTimeout,
                                   TimeUnit timeoutUnit)
        throws IOException {

        ObjectUtil.checkNull("lobStream", lobStream);

        return new PutOperation(kvsImpl, appLobKey, lobStream, durability,
                                chunkTimeout, timeoutUnit).execute(true, false);
    }

    @Override
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws IOException {

        ObjectUtil.checkNull("lobAppendStream", lobAppendStream);

        return new AppendOperation(kvsImpl, lobKey, lobAppendStream,
                                   durability, lobTimeout,
                                   timeoutUnit).execute();
    }

    /**
     * Returns true if the key bytes have the specified LOB suffix.
     *
     * @param lobSuffixBytes is null when an R1 client sends a MultiDelete.
     */
    public static boolean hasLOBSuffix(byte[] keyBytes,
                                       byte[] lobSuffixBytes) {
        if (lobSuffixBytes == null) {
            return false;
        }
        int i = lobSuffixBytes.length;
        int j = keyBytes.length;

        if (j < i) {
            return false;
        }

        while (i > 0) {
            if (lobSuffixBytes[--i] != keyBytes[--j]) {
                return false;
            }
        }

        return true;
    }
}
