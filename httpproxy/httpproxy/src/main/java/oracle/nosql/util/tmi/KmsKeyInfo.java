/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import java.nio.ByteBuffer;

import oracle.nosql.common.json.JsonUtils;

/**
 * Used in defining the response payload for the REST API get-kms-key from SC
 * to proxy.
 */
public class KmsKeyInfo {

    public enum KeyState{
        UPDATING,
        REVERTING,
        ACTIVE,
        DELETED,
        FAILED,
        DISABLED
    }

    private final Boolean isHostedEnv;
    private final String dedicatedTenantId;
    private final String keyId;
    private final String vaultId;
    private final KeyState state;
    private final long createTime;
    private final long updateTime;

    public KmsKeyInfo(Boolean isHostedEnv,
                      String dedicatedTenantId,
                      String keyId,
                      String vaultId,
                      KeyState state,
                      long createTime,
                      long updateTime) {
        this.isHostedEnv = isHostedEnv;
        this.dedicatedTenantId = dedicatedTenantId;
        this.keyId = keyId;
        this.vaultId = vaultId;
        this.state = state;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public KmsKeyInfo(Boolean isHostedEnv,
                      String dedicatedTenantId,
                      KeyState state) {
        this(isHostedEnv, dedicatedTenantId, null /* keyId */,
             null /* vaultId */, state, 0 /* createTime */,
             0 /* updateTime */);
    }

    public String getDedicatedTenantId() {
        return dedicatedTenantId;
    }

    public Boolean isHostedEnv() {
        return isHostedEnv;
    }

    public String getKeyId() {
        return keyId;
    }

    public String getVaultId() {
        return vaultId;
    }

    public KeyState getState() {
        return state;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public byte[] getETag() {
        /*
         * The "updateTime" reflects the last change to the KmsKeyInfo, use
         * it as ETag of KmsKeyInfo.
         */
        final ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(updateTime > 0 ? updateTime : createTime);
        return buffer.array();
    }

    @Override
    public String toString() {
        return JsonUtils.print(this);
    }
}
