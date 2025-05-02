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

package oracle.kv.impl.admin.param;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.param.AuthMethodsParameter;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;

/**
 * GlobalParams store system-wide operational parameters for a KVS instance.
 */
public class GlobalParams implements Serializable {

    private static final long serialVersionUID = 1L;
    private final ParameterMap map;

    /**
     * The RMI service name for all SNAs.
     */
    public static final String SNA_SERVICE_NAME = "snaService";

    /**
     * The RMI command service name for AdminService and its test interface
     */
    public static final String COMMAND_SERVICE_NAME = "commandService";
    public static final String COMMAND_SERVICE_TEST_NAME = "commandServiceTest";

    /**
     * The RMI service name for the Admin login interface
     */
    public static final String ADMIN_LOGIN_SERVICE_NAME = "admin:LOGIN";

    /**
     * The RMI service name for the SNA trusted login interface
     */
    public static final String SNA_LOGIN_SERVICE_NAME = "SNA:TRUSTED_LOGIN";

    /**
     * The RMI service name for the admin statement service, which handles DDL.
     */
    public static final String CLIENT_ADMIN_SERVICE_NAME = "admin:CLIENT_ADMIN";

    public GlobalParams(ParameterMap map) {
        this.map = map;
    }

    public GlobalParams(String kvsName) {

        map = new ParameterMap(ParameterState.GLOBAL_TYPE,
                               ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.COMMON_STORENAME, kvsName);
    }

    /**
     * Gets the global security parameters. If the parameters are not explictly
     * set in globalParams, default values will be used.
     */
    public ParameterMap getGlobalSecurityPolicies() {
        final EnumSet<ParameterState.Info> set =
            EnumSet.of(ParameterState.Info.GLOBAL,
                       ParameterState.Info.SECURITY);
        final ParameterMap pmap = new ParameterMap();
        for (final ParameterState ps : ParameterState.getMap().values()) {
            if (ps.containsAll(set)) {
                final String pName =
                    DefaultParameter.getDefaultParameter(ps).getName();
                pmap.put(map.getOrDefault(pName));
            }
        }
        return pmap;
    }

    /**
     * Gets the global components parameters. If the parameters are not
     * explictly set in globalParams, default values will be used.
     */
    public ParameterMap getGlobalComponentsPolicies() {
        final ParameterMap pmap = new ParameterMap();
        for (final ParameterState ps : ParameterState.getMap().values()) {
            if (ps.appliesTo(ParameterState.Info.GLOBAL) &&
                !ps.appliesTo(ParameterState.Info.SECURITY)) {
                final String pName =
                    DefaultParameter.getDefaultParameter(ps).getName();
                pmap.put(map.getOrDefault(pName));
            }
        }
        return pmap;
    }

    public ParameterMap getMap() {
        return map;
    }

    /**
     * During bootstrap the Admin needs to set the store name.
     */
    public void setKVStoreName(String kvsName) {
        map.setParameter(ParameterState.COMMON_STORENAME, kvsName);
    }

    public String getKVStoreName() {
        return map.get(ParameterState.COMMON_STORENAME).asString();
    }

    public String getStoreVersion() {
        return map.get(ParameterState.GP_STORE_VERSION).asString();
    }

    public void setStoreVersion(String storeVersion) {
        if (storeVersion != null) {
            map.setParameter(ParameterState.GP_STORE_VERSION, storeVersion);
        }
    }

    /**
     * The value of isLoopback is not valid unless isLoopbackSet() is true.
     */
    public boolean isLoopbackSet() {
        return map.exists(ParameterState.GP_ISLOOPBACK);
    }

    public boolean isLoopback() {
        return map.get(ParameterState.GP_ISLOOPBACK).asBoolean();
    }

    public void setIsLoopback(boolean value) {
        map.setParameter(ParameterState.GP_ISLOOPBACK,
                         Boolean.toString(value));
    }

    /**
     * Accessors for session and login parameters
     */

    public void setSessionTimeout(String value) {
        map.setParameter(ParameterState.GP_SESSION_TIMEOUT, value);
    }

    public long getSessionTimeout() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_SESSION_TIMEOUT);
        return dp.getAmount();
    }

    public TimeUnit getSessionTimeoutUnit() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_SESSION_TIMEOUT);
        return dp.getUnit();
    }

    public long getPasswordDefaultLifeTime() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_PASSWORD_LIFETIME);
        return dp.getAmount();
    }

    public TimeUnit getPasswordDefaultLifeTimeUnit() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_PASSWORD_LIFETIME);
        return dp.getUnit();
    }

    public void setSessionExtendAllow(String value) {
        map.setParameter(ParameterState.GP_SESSION_EXTEND_ALLOW, value);
    }

    public boolean getSessionExtendAllow() {
        return map.getOrDefault(ParameterState.GP_SESSION_EXTEND_ALLOW).
            asBoolean();
    }

    public void setAcctErrLockoutThrCnt(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT,
                         value);
    }

    public int getAcctErrLockoutThrCount() {
        return map.getOrDefault(
            ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT).asInt();
    }

    public void setAcctErrLockoutThrInt(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL,
                         value);
    }

    public long getAcctErrLockoutThrInt() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL);
        return dp.getAmount();
    }

    public TimeUnit getAcctErrLockoutThrIntUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL);
        return dp.getUnit();
    }

    public void setAcctErrLockoutTimeout(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT, value);
    }

    public long getAcctErrLockoutTimeout() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT);
            return dp.getAmount();
        }

    public TimeUnit getAcctErrLockoutTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT);
        return dp.getUnit();
    }

    public void setLoginCacheTimeout(String value) {
        map.setParameter(ParameterState.GP_LOGIN_CACHE_TIMEOUT, value);
    }

    public long getLoginCacheTimeout() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_LOGIN_CACHE_TIMEOUT);
            return dp.getAmount();
        }

    public TimeUnit getLoginCacheTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_LOGIN_CACHE_TIMEOUT);
        return dp.getUnit();
    }

    public String[] getUserExternalAuthMethods() {
        final AuthMethodsParameter amp = ((AuthMethodsParameter) map.
            getOrDefault(ParameterState.GP_USER_EXTERNAL_AUTH));
        return amp.asAuthMethods();
    }

    public String getUserExternalAuthValue() {
        return map.getOrDefault(
            ParameterState.GP_USER_EXTERNAL_AUTH).asString();
    }

    public void setUserExternalAuthMethods(String value) {
        map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, value);
    }

    public String getIDCSOAuthAudienceValue() {
        return map.getOrDefault(
            ParameterState.GP_IDCS_OAUTH_AUDIENCE).asString();
    }

    public void setIDCSOAuthAudienceValue(String value) {
        map.setParameter(ParameterState.GP_IDCS_OAUTH_AUDIENCE, value);
    }

    public String getIDCSOAuthPublicKeyAlias() {
        return map.getOrDefault(
            ParameterState.GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS).asString();
    }

    public void setIDCSOAuthPublicKeyAlias(String pkAlias) {
        map.setParameter(
            ParameterState.GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS, pkAlias);
    }

    public String getIDCSOAuthSignatureVerifyAlg() {
        return map.getOrDefault(
            ParameterState.GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME).asString();
    }

    public void setIDCSOAuthSignatureVerifyAlg(String sigAlg) {
        map.setParameter(
            ParameterState.GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME, sigAlg);
    }

    public boolean getCollectorEnabled() {
        return map.getOrDefault(
            ParameterState.GP_COLLECTOR_ENABLED).asBoolean();
    }

    public long getCollectorStoragePerComponent() {
        return map.getOrDefault(
            ParameterState.GP_COLLECTOR_STORAGE_PER_COMPONENT).asLong();
    }

    public long getCollectorInterval() {
        return ParameterUtils.getDurationMillis(
            map, ParameterState.GP_COLLECTOR_INTERVAL);
    }

    public String getCollectorRecorder() {
        return map.get(ParameterState.GP_COLLECTOR_RECORDER).asString();
    }

    public boolean equals(GlobalParams other) {
        return map.equals(other.getMap());
    }
}
