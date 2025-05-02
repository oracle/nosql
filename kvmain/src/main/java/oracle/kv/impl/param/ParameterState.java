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

package oracle.kv.impl.param;

import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.rep.RNTaskCoordinator;
import oracle.kv.impl.rep.stats.StatsScan;
import oracle.kv.impl.rep.table.MaintenanceThread;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.StorageTypeDetector;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * See header comments in Parameter.java for an overview of Parameters.
 * <p>
 * This class acts as a type catalog for Parameter state. It defines all of the
 * allowed parameters along with their types, scope and other meta-data,
 * including min/max values and legal string values. All new parameters must be
 * added here. To add a new parameter do 2 things:
 *<ol>
 * <li>Add appropriate constants to this file.</li>
 *
 * <li>Add the parameter to the statically initialized map, pstate. </li>
 *</ol>
 * <p>
 * Adding a parameter should be a cut/paste exercise using another as an
 * example. Be sure to add a default String value as well as ensuring that any
 * parameter that should be defaulted is specified as a POLICY parameter.
 * <p>
 * In order to be properly validated the static createParameter() should be
 * used. If a parameter is either not defined or has an invalid value that
 * method will throw IllegalStateException.
 * <p>
 * Any changes to parameters that require a "restart" merit special attention:
 * Please be sure to bring such changes to doc group's attention, so that they
 * can make concomitant changes to the Admin documentation. Note that
 * parameters default to requiring restarts, so it takes extra vigilance to
 * when introducing new parameters.
 */
public class ParameterState {

    private static final int MB = 1024 * 1024;

    private final Type type;
    private final Object defaultParam;
    private final Scope scope;
    private final EnumSet<Info> info;
    private final long min;
    private final long max;
    private final String[] validValues;

    /**
     * Parameters to skip in comparisons.
     */
    public static final HashSet<String> skipParams = new HashSet<String>() {
        private static final long serialVersionUID = 1L;
        {
            add(COMMON_DISABLED);
            add(SN_SOFTWARE_VERSION);
            add(GP_STORE_VERSION);

        }
    };

    /**
     * Parameters to skip in comparisons. Used by the admin parameter
     * consistency feature.
     */
    public static final HashSet<String> skipParamsPCon = new HashSet<String>() {
        private static final long serialVersionUID = 1L;
        {
            add(COMMON_DISABLED);
            add(SN_SOFTWARE_VERSION);
        }
    };

    /**
     * These are the parameters from StorageNodeParams that are used by other
     * services (Admin and RepNode) for their own configuration.  They tend to
     * be used only on startup.  They are listed so that utilities such as
     * VerifyConfiguration can filter on them for validation.
     */
    public static final HashSet<String> serviceParams = new HashSet<String>() {
        private static final long serialVersionUID = 1L;
        {
            add(SN_ROOT_DIR_PATH);
            add(COMMON_SN_ID);
            add(COMMON_HOSTNAME);
            add(COMMON_REGISTRY_PORT);
            add(SN_LOG_FILE_COUNT);
            add(SN_LOG_FILE_LIMIT);
        }
    };

    /**
     * Types of Parameter instances.
     */
    public enum Type {
        NONE,                           /* For unknown parameters */
        INT, LONG, BOOLEAN, STRING,     /* Standard Java types */

        /* Values of the com.sleepycat.je.CacheMode enum */
        CACHEMODE,

        /*
         * A long value, separator, and TimeUnit enum -- see DurationParameter
         */
        DURATION,

        /* One or more comma-separated Strings -- see AuthMethodsParameter */
        AUTHMETHODS,

        /*
         * A string containing one or more permitted special characters -- see
         * SpecialCharsParameter
         */
        SPECIALCHARS,

        /*
         * A long value, separator, and size unit (KB, MB, GB, TB) -- see
         * SizeParameter
         */
        SIZE,

        /**
         * Sub-type of DURATION for time-to-live values -- see
         * TimeToLiveParameter
         */
        TIME_TO_LIVE
    }

    /**
     * Scope of a parameter.  This can be used for validation of parameter
     * changes.
     * <ul>
     * <li>SERVICE -- the parameter applies per service instance.  E.g. the
     * registry port.
     * <li>REPGROUP -- the parameter can apply across a single RepGroup. E.g
     * Helper hosts.
     * <li>STORE -- the parameter can apply across all instances of the service
     * throughout the store.  E.g. JE cache size for RepNodes.
     * Note: modifiable global or security parameter must be set to STORE scope.
     * </ul>
     */
    public enum Scope {SERVICE, REPGROUP, STORE}

    /**
     * Meta-info about a parameter.
     * <p>
     * One or more of ADMIN, REPNODE, SNA, ARBNODE, and TIF can be specified to
     * signify that the parameter can apply to one or more of those services.
     * Otherwise, the node can be marked with GLOBAL, SECURITY, or TRANSPORT to
     * mark a separate set of parameters not directly related to a particular
     * service.  GLOBAL without SECURITY means global component parameters, it
     * can be changed by "plan change-parameters -global" and be shown by
     * "show parameter -global".  GLOBAL with SECURITY means global security
     * parameters, it can be changed by "plan change-parameters -security" and
     * be shown by "show parameter -security". If SECURITY without GLOBAL
     * parameter, it is usually a read-only parameter(with RONLY) saved in
     * security directory configuration files or a policy
     * parameter(with POLICY).
     * <p>
     * The absence of RONLY implies read-write, absence of NORESTART implies
     * restart required.  HIDDEN is special in that it is internal-only and
     * only displayed conditionally in the UI.  HIDDEN parameters, if not
     * read-only, are only visible and settable via the CLI if the hidden
     * command or -hidden flag are used.
     * <p>
     * BOOT signifies parameters that appear in the bootstrap configuration
     * file generated by makebootconfig.
     * <p>
     * POLICY means that the parameter is permitted to be specified in the
     * policy that sets the default for newly created services. POLICY may
     * not be specified on global parameters (with GLOBAL).
     */
    public enum Info {
            ADMIN,      /* Applies to Admin */
            REPNODE,    /* Applies to RepNode */
            SNA,        /* Applies to SNA */
            BOOT,       /* Is a bootstrap parameter */
            GLOBAL,     /* Applies to GlobalParams */
            RONLY,      /* Not directly modifiable by a user */
            NORESTART,  /* Does not require service restart */
            POLICY,     /* Is settable as a policy parameter */
            HIDDEN,     /* Not displayed or settable by default */
            SECURITY,   /* Applies to security */
            TRANSPORT,  /* Applies to transports */
            ARBNODE,    /* Applies to ArbNode */
            TIF         /* Applies to TextIndexFeeder */
    }

    /**
     * Possible values for Boolean type.
     */
    private static String booleanVals[] = new String [] {"false", "true"};

    /*
     * This is the "schema" for the parameters, potentially modified between
     * releases to indicate the need to handle changes.
     */
    public static final int PARAMETER_VERSION = 1;
    public static final int BOOTSTRAP_PARAMETER_VERSION = 3;
    public static final int BOOTSTRAP_PARAMETER_R2_VERSION = 2;
    public static final int BOOTSTRAP_PARAMETER_R1_VERSION = 1;

    /*
     * Top-level parameter groupings.  These go into configuration files.
     * String is used vs enum because of serialization.  Changing these
     * changes the persistent representation and should not be done lightly.
     */
    public static final String REPNODE_TYPE = "repNodeParams";
    public static final String ARBNODE_TYPE = "arbNodeParams";
    public static final String ADMIN_TYPE = "adminParams";
    public static final String GLOBAL_TYPE = "globalParams";
    public static final String SNA_TYPE = "storageNodeParams";
    public static final String BOOTSTRAP_TYPE = "bootstrapParams";
    public static final String BOOTSTRAP_PARAMS = "params";
    public static final String BOOTSTRAP_MOUNT_POINTS = "mountPoints";
    public static final String BOOTSTRAP_ADMIN_MOUNT_POINTS = "adminMountPoints";
    public static final String BOOTSTRAP_RNLOG_MOUNT_POINTS = "rnLogMountPoints";

    /*
     * Top-level security parameter groupings.
     */
    public static final String SECURITY_TYPE = "securityParams";
    public static final String SECURITY_PARAMS = "params";
    public static final String SECURITY_TRANSPORT_TYPE = "transportParams";
    public static final String SECURITY_TRANSPORT_INTERNAL = "internal";
    public static final String SECURITY_TRANSPORT_JE_HA = "ha";
    public static final String SECURITY_TRANSPORT_CLIENT = "client";

    /**
     * This is the static map that holds all of the required information.
     */
    static final Map<String, ParameterState> pstate = new HashMap<>();

    /*
     * Parameter definitions
     */

    /*
     * Security parameters
     */

    /*
     * This first parameter is annotated with comments to explain how the
     * information for each parameter should be used to produce user
     * documentation.  Each parameter is defined by a String field that holds
     * the parameter name, an optional default value field, and a static block
     * that defines additional information about the parameter.
     */

    /*
     * Begin parameter: the parameter name field, as well as any documentation,
     * which may not be available for hidden parameters.
     */
    /**
     * Whether security is enabled for the store.
     */
    public static final String SEC_SECURITY_ENABLED =

        /* The parameter name */
        "securityEnabled";

    /*
     * Typically there is a second field that provides the parameter default
     * value, but not for boolean parameters or other parameters with trivial
     * default values.
     */

    /* Additional information about the parameter */
    static {
        putState(

            /* The parameter name */
            SEC_SECURITY_ENABLED,

            /* The parameter default value */
            "false",

            /* The parameter type */
            Type.BOOLEAN,

            /* Meta-data about the parameter */
            EnumSet.of(

                /*
                 * By convention, the first one or several items say whether
                 * the parameter should be documented for one or more services
                 * (ADMIN, REPNODE, SNA, ARBNODE, TIF) or belongs on another
                 * list (GLOBAL, SECURITY, TRANSPORT).
                 */
                Info.SECURITY,

                /*
                 * The remaining items provide other information.  If HIDDEN
                 * appears, then the parameter should not be documented.  The
                 * documentation should mention if the parameter is read-only
                 * (RONLY) and if changing it requires a restart (NORESTART is
                 * absent).
                 */
                 Info.RONLY)

            /*
             * Some parameters have additional information about the parameter
             * scope, minimum, maximum, and valid values.  There are additional
             * comments about each of these pieces of information at their
             * first appearance.
             */
            ); }
    /* End parameter */

    public static final String SEC_KEYSTORE_FILE = "keystore";
    static { putState(SEC_KEYSTORE_FILE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KEYSTORE_TYPE = "keystoreType";
    static { putState(SEC_KEYSTORE_TYPE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KEYSTORE_SIG_PRIVATE_KEY_ALIAS =
        "keystoreSigPrivateKeyAlias";
    static { putState(SEC_KEYSTORE_SIG_PRIVATE_KEY_ALIAS, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KEYSTORE_PWD_ALIAS = "keystorePasswordAlias";
    static { putState(SEC_KEYSTORE_PWD_ALIAS, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_TRUSTSTORE_FILE = "truststore";
    static { putState(SEC_TRUSTSTORE_FILE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_TRUSTSTORE_TYPE = "truststoreType";
    static { putState(SEC_TRUSTSTORE_TYPE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_TRUSTSTORE_SIG_PUBLIC_KEY_ALIAS =
        "truststoreSigPublicKeyAlias";
    static { putState(SEC_TRUSTSTORE_SIG_PUBLIC_KEY_ALIAS, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_PASSWORD_FILE = "passwordFile";
    static { putState(SEC_PASSWORD_FILE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_PASSWORD_CLASS = "passwordClass";
    static { putState(SEC_PASSWORD_CLASS, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_WALLET_DIR = "walletDir";
    static { putState(SEC_WALLET_DIR, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_INTERNAL_AUTH = "internalAuth";
    static { putState(SEC_INTERNAL_AUTH, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_CERT_MODE = "certMode";
    static { putState(SEC_CERT_MODE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_SIGNATURE_ALGO = "signatureAlgorithm";
    static { putState(SEC_SIGNATURE_ALGO, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    /*
     * Security Transport parameters
     */

    public static final String SEC_TRANS_TYPE = "transportType";
    static { putState(SEC_TRANS_TYPE, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_FACTORY = "transportFactory";
    static { putState(SEC_TRANS_FACTORY, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_SERVER_KEY_ALIAS = "serverKeyAlias";
    static { putState(SEC_TRANS_SERVER_KEY_ALIAS, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_CLIENT_KEY_ALIAS = "clientKeyAlias";
    static { putState(SEC_TRANS_CLIENT_KEY_ALIAS, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_CLIENT_AUTH_REQUIRED =
        "clientAuthRequired";
    static { putState(SEC_TRANS_CLIENT_AUTH_REQUIRED, "false",
                      Type.BOOLEAN,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_CLIENT_IDENT_ALLOW =
        "clientIdentityAllowed";
    static { putState(SEC_TRANS_CLIENT_IDENT_ALLOW, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_SERVER_IDENT_ALLOW =
        "serverIdentityAllowed";
    static { putState(SEC_TRANS_SERVER_IDENT_ALLOW, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_ALLOW_CIPHER_SUITES =
        "allowCipherSuites";
    static { putState(SEC_TRANS_ALLOW_CIPHER_SUITES, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES =
        "clientAllowCipherSuites";
    static { putState(SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    /**
     * The allowed SSL protocols as enforced on the server side. If no
     * parameter setting is found, the default will be used. Stores whose
     * security configuration does not specify a value will use the default
     * provided by the current software version whenever the server is
     * upgraded. That behavior provides a convenient way for users without
     * specific security requirements to use the latest defaults.
     */
    public static final String SEC_TRANS_ALLOW_PROTOCOLS =
        "allowProtocols";
    public static final String SEC_TRANS_ALLOW_PROTOCOLS_DEFAULT =
        SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT;
    static { putState(SEC_TRANS_ALLOW_PROTOCOLS,
                      SEC_TRANS_ALLOW_PROTOCOLS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    public static final String SEC_TRANS_CLIENT_ALLOW_PROTOCOLS =
        "clientAllowProtocols";
    static { putState(SEC_TRANS_CLIENT_ALLOW_PROTOCOLS, "",
                      Type.STRING,
                      EnumSet.of(Info.TRANSPORT, Info.RONLY)); }

    /*
     * Security Kerberos parameters
     */

    public static final String SEC_KERBEROS_CONFIG_FILE = "krbConf";
    static { putState(SEC_KERBEROS_CONFIG_FILE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KERBEROS_SERVICE_NAME = "krbServiceName";
    public static final String SEC_KERBEROS_SERVICE_NAME_DEFAULT =
        "oraclenosql";
    static { putState(SEC_KERBEROS_SERVICE_NAME,
                      SEC_KERBEROS_SERVICE_NAME_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KERBEROS_INSTANCE_NAME = "krbInstanceName";
    static { putState(SEC_KERBEROS_INSTANCE_NAME, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KERBEROS_REALM_NAME = "krbRealmName";
    static { putState(SEC_KERBEROS_REALM_NAME, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    public static final String SEC_KERBEROS_KEYTAB_FILE = "krbServiceKeytab";
    static { putState(SEC_KERBEROS_KEYTAB_FILE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.RONLY)); }

    /*
     * Security password complexity check policy
     */

    /**
     * Whether to enable the password complexity check policy.
     */
    public static final String SEC_PASSWORD_COMPLEXITY_CHECK =
        "passwordComplexityCheck";
    public static final String SEC_PASSWORD_COMPLEXITY_CHECK_DEFAULT = "true";
    static { putState(SEC_PASSWORD_COMPLEXITY_CHECK,
                      SEC_PASSWORD_COMPLEXITY_CHECK_DEFAULT,
                      Type.BOOLEAN,

                      /*
                       * This is a security parameter that only can be changed
                       * and shown by the policy command.  This restriction was
                       * put in place because this parameter specifies
                       * information that should not be stored in the clear in
                       * the SN's config.xml file.  The parameter can't be
                       * changed by "plan change-parameter -security" command,
                       * nor be shown by "show parameter -security" command.
                       * Note that changing this parameter in the policy will
                       * have the effect of changing the value used by current
                       * services.
                       */
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),

                      /*
                       * The scope for this parameter; in this case the value
                       * applies to a store-wide.
                       */
                      Scope.STORE); }

    /**
     * The minimum length required for the user password.
     */
    public static final String SEC_PASSWORD_MIN_LENGTH = "passwordMinLength";
    public static final String SEC_PASSWORD_MIN_LENGTH_DEFAULT = "9";
    static { putState(SEC_PASSWORD_MIN_LENGTH,
                      SEC_PASSWORD_MIN_LENGTH_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,

                      /* The minimum and maximum values */
                      1, 2048,

                      /*
                       * The valid values, where null means no values are
                       * specified.  Valid values are only needed when they
                       * should be chosen from a fixed set of strings.
                       */
                      null); }

    /**
     * The maximum length allowed for the user password.
     */
    public static final String SEC_PASSWORD_MAX_LENGTH = "passwordMaxLength";
    public static final String SEC_PASSWORD_MAX_LENGTH_DEFAULT = "256";
    static { putState(SEC_PASSWORD_MAX_LENGTH,
                      SEC_PASSWORD_MAX_LENGTH_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      1, 2048,
                      null); }

    /**
     * The minimum upper case letters required in the user password.
     */
    public static final String SEC_PASSWORD_MIN_UPPER = "passwordMinUpper";
    public static final String SEC_PASSWORD_MIN_UPPER_DEFAULT = "2";
    static { putState(SEC_PASSWORD_MIN_UPPER,
                      SEC_PASSWORD_MIN_UPPER_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      0, 2048,
                      null); }

    /**
     * The minimum lower case letters required in the user password.
     */
    public static final String SEC_PASSWORD_MIN_LOWER = "passwordMinLower";
    public static final String SEC_PASSWORD_MIN_LOWER_DEFAULT = "2";
    static { putState(SEC_PASSWORD_MIN_LOWER,
                      SEC_PASSWORD_MIN_LOWER_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      0, 2048,
                      null); }

    /**
     * The minimum digit numbers required in the user password.
     */
    public static final String SEC_PASSWORD_MIN_DIGIT = "passwordMinDigit";
    public static final String SEC_PASSWORD_MIN_DIGIT_DEFAULT = "2";
    static { putState(SEC_PASSWORD_MIN_DIGIT,
                      SEC_PASSWORD_MIN_DIGIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      0, 2048,
                      null); }

    /**
     * The minimum special characters required in the user password.
     */
    public static final String SEC_PASSWORD_MIN_SPECIAL = "passwordMinSpecial";
    public static final String SEC_PASSWORD_MIN_SPECIAL_DEFAULT = "2";
    static { putState(SEC_PASSWORD_MIN_SPECIAL,
                      SEC_PASSWORD_MIN_SPECIAL_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      0, 2048,
                      null); }

    /**
     * The special characters are allowed to use.
     *
     * Note that only a sub-set or full-set of characters listed in the default
     * value are permitted to use when setup this parameter. Setting this to
     * empty string means bypass the special character check.
     *
     * Letters, numbers, and non-printing characters are not permitted here.
     */
    public static final String SEC_PASSWORD_ALLOWED_SPECIAL=
        "passwordAllowedSpecial";
    /**
     * Default allowed special characters list.
     *
     * The following special characters are not permitted to be included in
     * the list:
     * Double quote - Special character used by shell to parse string
     *                value.
     * Back slash - Escape keyword for java.
     */
    public static final String SEC_PASSWORD_ALLOWED_SPECIAL_DEFAULT =
        "!#$%&'()*+,-./:; <=>?@[]^_`{|}~";
    static { putState(SEC_PASSWORD_ALLOWED_SPECIAL,
                      SEC_PASSWORD_ALLOWED_SPECIAL_DEFAULT,
                      Type.SPECIALCHARS,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * The number of previous remembered passwords not allow to use.
     *
     * Setting value of this parameter to 0 will disable the previous
     * remembered password checking.
     */
    public static final String SEC_PASSWORD_REMEMBER = "passwordRemember";
    public static final String SEC_PASSWORD_REMEMBER_DEFAULT = "3";
    static { putState(SEC_PASSWORD_REMEMBER,
                      SEC_PASSWORD_REMEMBER_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE,
                      0, 256,
                      null); }

    /**
     * Password should not be the same as user name, the reversed user name, or
     * the user name with a number from 1-100 appended.
     * @see oracle.kv.impl.security.pwchecker.PasswordCheckerRule.PasswordNotUserName
     */
    public static final String SEC_PASSWORD_NOT_USER_NAME =
        "passwordNotUserName";
    public static final String SEC_PASSWORD_NOT_USER_NAME_DEFAULT = "true";
    static { putState(SEC_PASSWORD_NOT_USER_NAME,
                      SEC_PASSWORD_NOT_USER_NAME_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * Password should not be the same as store name, the reversed store name,
     * or the store name with a number from 1-100 appended.
     * @see oracle.kv.impl.security.pwchecker.PasswordCheckerRule.PasswordNotStoreName
     */
    public static final String SEC_PASSWORD_NOT_STORE_NAME =
        "passwordNotStoreName";
    public static final String SEC_PASSWORD_NOT_STORE_NAME_DEFAULT = "true";
    static { putState(SEC_PASSWORD_NOT_STORE_NAME,
                      SEC_PASSWORD_NOT_STORE_NAME_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * List of words that are prohibited to use as user password.
     */
    public static final String SEC_PASSWORD_PROHIBITED =
        "passwordProhibited";
    public static final String SEC_PASSWORD_PROHIBITED_DEFAULT =
        "oracle,password,user,nosql";
    static { putState(SEC_PASSWORD_PROHIBITED,
                      SEC_PASSWORD_PROHIBITED_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /*
     * Global security parameters
     */

    public static final String GP_SESSION_TIMEOUT = "sessionTimeout";
    public static final String GP_SESSION_TIMEOUT_DEFAULT = "24 h";
    static { putState(GP_SESSION_TIMEOUT,
                      GP_SESSION_TIMEOUT_DEFAULT,
                      Type.DURATION,

                      /*
                       * This is a modifiable global security parameter, and it
                       * doesn't require a restart.
                       */
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_SESSION_EXTEND_ALLOW =
        "sessionExtendAllowed";
    public static final String GP_SESSION_EXTEND_ALLOW_DEFAULT = "true";
    static { putState(GP_SESSION_EXTEND_ALLOW,
                      GP_SESSION_EXTEND_ALLOW_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT =
        "accountErrorLockoutThresholdCount";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT_DEFAULT =
        "10";
    static { putState(GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT,
                      GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL =
        "accountErrorLockoutThresholdInterval";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL_DEFAULT =
        "10 min";
    static { putState(GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL,
                      GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT =
        "accountErrorLockoutTimeout";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT_DEFAULT =
        "30 min";
    static { putState(GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT,
                      GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_LOGIN_CACHE_TIMEOUT = "loginCacheTimeout";
    public static final String GP_LOGIN_CACHE_TIMEOUT_DEFAULT = "5 min";
    static { putState(GP_LOGIN_CACHE_TIMEOUT,
                      GP_LOGIN_CACHE_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    /**
     * Use to define the default lifetime of user password.
     */
    public static final String GP_PASSWORD_LIFETIME = "passwordLifetime";
    public static final String GP_PASSWORD_LIFETIME_DEFAULT = "180 DAYS";
    static { putState(GP_PASSWORD_LIFETIME,
                      GP_PASSWORD_LIFETIME_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.RONLY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * Use to define the comma-separated list of user external authentication
     * methods, or "NONE" if external authentication is not supported.
     */
    public static final String GP_USER_EXTERNAL_AUTH = "userExternalAuth";
    public static final String GP_USER_EXTERNAL_AUTH_DEFAULT = "NONE";
    static { putState(GP_USER_EXTERNAL_AUTH,
                      GP_USER_EXTERNAL_AUTH_DEFAULT,
                      Type.AUTHMETHODS,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL,
                                 Info.BOOT, Info.NORESTART),
                      Scope.STORE,
                      0, 0,  /* min, max n/a */

                      /* Acceptable String values */
                      new String[] {"NONE", "KERBEROS", "IDCSOAUTH"}); }

    /*
     * Global security IDCS OAuth parameters
     */

    public static final String GP_IDCS_OAUTH_AUDIENCE = "idcsOAuthAudience";
    static { putState(GP_IDCS_OAUTH_AUDIENCE, "",
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS =
        "idcsOAuthPublicKey";
    public static final String GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS_DEFAULT =
        "tenantKey";
    static { putState(GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS,
                      GP_IDCS_OAUTH_PUBLIC_KEY_ALIAS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME =
        "idcsOAuthSignatureAlgorithm";
    public static final String GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME_DEFAULT =
        "RS256";
    static { putState(GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME,
                      GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                      Scope.STORE); }

    /*
     * Global parameters
     */

    public static final String GP_ISLOOPBACK = "isLoopback";
    public static final String GP_ISLOOPBACK_DEFAULT = "false";
    static { putState(GP_ISLOOPBACK,
                      GP_ISLOOPBACK_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.GLOBAL,
                                 Info.RONLY, Info.HIDDEN)); }

    /*
     * The store version is the minimum software version of all SN's in
     * the KVStore.
     */
    public static final String GP_STORE_VERSION = "storeSoftwareVersion";
    public static final String GP_STORE_VERSION_DEFAULT = "";
    static { putState(GP_STORE_VERSION,
                      GP_STORE_VERSION_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.GLOBAL, Info.HIDDEN,
                                 Info.NORESTART),
                      Scope.STORE); }


    /**
     * The name of the store.  The value is provided as the value specified for
     * the '-name' flag to the 'configure' command in the Admin CLI, and cannot
     * be changed.
     */
    public static final String COMMON_STORENAME = "storeName";
    public static final String COMMON_STORENAME_DEFAULT = "nostore";
    static { putState(COMMON_STORENAME, COMMON_STORENAME_DEFAULT, Type.STRING,
                      EnumSet.of(Info.GLOBAL,
                                 Info.BOOT, Info.RONLY),
                      Scope.STORE); }

    /*
     * Global collector service parameters
     */

    public static final String GP_COLLECTOR_ENABLED = "collectorEnabled";
    public static final String GP_COLLECTOR_ENABLED_DEFAULT = "false";
    static { putState(GP_COLLECTOR_ENABLED,
                      GP_COLLECTOR_ENABLED_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_COLLECTOR_STORAGE_PER_COMPONENT =
        "collectorStoragePerComponent";
    public static final String GP_COLLECTOR_STORAGE_PER_COMPONENT_DEFAULT =
        "30 MB";
    static { putState(GP_COLLECTOR_STORAGE_PER_COMPONENT,
                      GP_COLLECTOR_STORAGE_PER_COMPONENT_DEFAULT,
                      Type.SIZE,
                      EnumSet.of(Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String GP_COLLECTOR_INTERVAL =
        "collectorInterval";
    public static final String GP_COLLECTOR_INTERVAL_DEFAULT = "20 s";
    static { putState(GP_COLLECTOR_INTERVAL,
                      GP_COLLECTOR_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.GLOBAL,
                                 Info.NORESTART),
                      Scope.STORE); }

    /**
     * Specify an optional, additional collector recorder that can be
     * instantiated and added to the all collector agents of the collector
     * service. By default the collector service will create a file recorder
     * that processes collected metrics and logs the result to a file. The
     * kvstore user can define an additional class that will be dynamically
     * loaded and will act as recorders. One example is the NDCS table usage
     * stats, which aggregates per-RN table metrics into a single store-wide
     * table. The default for this param is null.  To use it, set the value to
     * the fully qualified class name of a recorders.
     * For example,
     *     collectorRecorder=oracle.nosql.sc.service.MTUsageWriter
     */
    public static final String GP_COLLECTOR_RECORDER =
        "collectorRecorder";
    static { putState(GP_COLLECTOR_RECORDER,
                      null,
                      Type.STRING,
                      EnumSet.of(Info.GLOBAL, Info.NORESTART),
                      Scope.STORE); }

    /*
     * Common to RepNode, ArbNode, Admin, and SNA
     */

    /**
     * The storage node ID of the storage node hosting the service.
     */
    public static final String COMMON_SN_ID = "storageNodeId";
    public static final String COMMON_SN_ID_DEFAULT = "-1";
    static { putState(COMMON_SN_ID,
                      COMMON_SN_ID_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.SNA, Info.RONLY)); }

    /**
     * Whether the service is disabled, either because it was shutdown
     * explicitly using the 'plan stop-service' command or because it was
     * disabled because of repeated failures on startup.  Disabled services can
     * be restarted using the 'plan start-service' command.
     */
    public static final String COMMON_DISABLED = "disabled";
    public static final String COMMON_DISABLED_DEFAULT = "false";
    static { putState(COMMON_DISABLED,
                      COMMON_DISABLED_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.RONLY)); }

    /**
     * The name of the network interface used by the HA subsystem. A valid
     * string for a hostname can be a DNS name or an IP address. The value
     * specified for this parameter controls which network interface will be
     * used for JE HA operations for Replication Nodes and Arbiters.
     * Note that Admin Nodes use all local interfaces for their JE HA
     * replication servers, so the parameter has no impact on Admin Nodes.
     * This parameter can be specified in the makebootconfig command used to
     * initialize the storage node, either by the value of the -hahost flag or,
     * if that is not specified, the value of the -host flag. This parameter is
     * read-only. If you want to change the JE HA hostname, you need to migrate
     * to a new SN.
     */
    public static final String COMMON_HA_HOSTNAME = "haHostname";
    public static final String COMMON_HA_HOSTNAME_DEFAULT = "localhost";
    static { putState(COMMON_HA_HOSTNAME,
                      COMMON_HA_HOSTNAME_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART, Info.RONLY)); }

    /**
     * The range of ports used for JE HA replication nodes running on the
     * storage node.  The initial value is provided by the value of the
     * '-harange' flag to the 'makebootconfig' command.  The value is specified
     * either the low and high port numbers separated by a comma, or '0' to
     * represent that any free port may be used. This parameter is read-only.
     * If you want to change the JE HA port range, you need to migrate to a new
     * SN.
     */
    public static final String COMMON_PORTRANGE = "haPortRange";
    public static final String COMMON_PORTRANGE_DEFAULT = "1,2";
    static { putState(COMMON_PORTRANGE,
                      COMMON_PORTRANGE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART, Info.RONLY)); }

    /**
     * Switch to control master balancing.
     */
    public static final String COMMON_MASTER_BALANCE = "masterBalance";
    public static final String COMMON_MASTER_BALANCE_DEFAULT = "true";
    static { putState(COMMON_MASTER_BALANCE,
                      COMMON_MASTER_BALANCE_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SNA, Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN)); }

    /**
     * Determine whether keys and values are displayed in exceptions and
     * error messages on the server side.
     */
    public static final String COMMON_HIDE_USERDATA = "hideUserData";
    public static final String COMMON_HIDE_USERDATA_DEFAULT = "true";
    static { putState(COMMON_HIDE_USERDATA,
                      COMMON_HIDE_USERDATA_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.ADMIN, Info.REPNODE,
                                 Info.POLICY, Info.NORESTART)); }

    /**
     * The fully qualified name of the class that implements the management
     * agent responsible for delivering monitoring results.  The default value
     * specifies no monitoring.  For JMX monitoring, use the value
     * "oracle.kv.impl.mgmt.jmx.JmxAgent".
     */
    public static final String COMMON_MGMT_CLASS = "mgmtClass";
    public static final String COMMON_MGMT_CLASS_DEFAULT =
        "oracle.kv.impl.mgmt.NoOpAgent";
    static { putState(COMMON_MGMT_CLASS,
                      COMMON_MGMT_CLASS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART)); }

    /**
     * The port used for SNMP polling.
     */
    public static final String COMMON_MGMT_POLL_PORT = "mgmtPollPort";
    public static final String COMMON_MGMT_POLL_PORT_DEFAULT = "1161";
    static { putState(COMMON_MGMT_POLL_PORT,
                      COMMON_MGMT_POLL_PORT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART)); }

    /**
     * The host used for SNMP traps.
     */
    public static final String COMMON_MGMT_TRAP_HOST = "mgmtTrapHost";
    public static final String COMMON_MGMT_TRAP_HOST_DEFAULT = null;
    static { putState(COMMON_MGMT_TRAP_HOST,
                      COMMON_MGMT_TRAP_HOST_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART)); }

    /**
     * The port used for SNMP traps.
     */
    public static final String COMMON_MGMT_TRAP_PORT = "mgmtTrapPort";
    public static final String COMMON_MGMT_TRAP_PORT_DEFAULT = "0";
    static { putState(COMMON_MGMT_TRAP_PORT,
                      COMMON_MGMT_TRAP_PORT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART)); }

    /**
     * Previously used to turn on/off the use of client socket factories, but
     * now ignored. Client socket factories are now always required. Keeping
     * the parameter for backwards compatibility.
     * @deprecated as of 23.1
     */
    @Deprecated
    public static final String COMMON_USE_CLIENT_SOCKET_FACTORIES =
            "useClientSocketFactories";
    public static final String COMMON_USE_CLIENT_SOCKET_FACTORIES_DEFAULT =
        "true";
    static { putState(COMMON_USE_CLIENT_SOCKET_FACTORIES,
                      COMMON_USE_CLIENT_SOCKET_FACTORIES_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                 Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN)); }

    /**
     * Use to define how many concurrent sessions are maintained, and how many/
     * how long the tokens are kept.
     */
    public static final String COMMON_SESSION_LIMIT = "sessionLimit";
    public static final String COMMON_SESSION_LIMIT_DEFAULT = "10000";
    static { putState(COMMON_SESSION_LIMIT,
                      COMMON_SESSION_LIMIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                 Info.NORESTART),
                      Scope.STORE); }

    public static final String COMMON_LOGIN_CACHE_SIZE = "loginCacheSize";
    public static final String COMMON_LOGIN_CACHE_SIZE_DEFAULT = "10000";
    static { putState(COMMON_LOGIN_CACHE_SIZE,
                      COMMON_LOGIN_CACHE_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                 Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /** Override the electable group size */
    public static final String COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE =
        "electableGroupSizeOverride";
    public static final String
        COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE_DEFAULT = "0";
    static { putState(COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE,
                      COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.REPNODE,
                                 Info.NORESTART, Info.HIDDEN),
                      Scope.SERVICE); }

    /**
     * Request that the JE replication group be reset to the current node.
     * This parameter is used during failover to reestablish quorum when the
     * only nodes available were formerly secondary nodes.  A true value causes
     * the service to perform the reset on restart.
     */
    public static final String COMMON_RESET_REP_GROUP = "resetRepGroup";
    public static final String COMMON_RESET_REP_GROUP_DEFAULT = "false";
    static { putState(COMMON_RESET_REP_GROUP,
                      COMMON_RESET_REP_GROUP_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.ADMIN, Info.REPNODE,
                                 Info.NORESTART, Info.HIDDEN),
                      Scope.SERVICE); }

    /**
     * Replication acknowledgment timeout.
     * TODO: Eventually this value should be used to set the JE replica
     * ACK timeout value. When this happens, remove the HIDDEN tag below.
     * Also, a limit or max value check should be made as a large value
     * could cause consistency issues.
     */
    public static final String COMMON_REPLICA_ACK_TIMEOUT = "ackTimeout";
    public static final String COMMON_REPLICA_ACK_TIMEOUT_DEFAULT = "1 s";
    static { putState(COMMON_REPLICA_ACK_TIMEOUT,
                      COMMON_REPLICA_ACK_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN, Info.REPNODE,
                                 Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Names of security properties that control the TTL for InetAddress DNS
     * resolution cache.
     *
     * See the section "Networking System Properties" in the java.net package
     * documentation for more details. The Java default value of -1 for a
     * positive name resolution is to cache forever when a security manager is
     * installed. This behavior makes the system unstable if DNS mapping can
     * change, which affects all processes of the system include SNA, RN, AN
     * and Admin services as well as the admin CLI. Therefore we expose
     * parameters to set these security properties, and we set them to
     * non-negative values by default.
     *
     * The properties can be only set once and must be set before any
     * InetAddress resolution is attempted. See sun.net.InetAddressCachePolicy
     * for more details.
     *
     * See CommandParser#getDnsCacheTTLUsage for setting the parameters from
     * the admin CLI and makebootconfig command line.
     *
     * See KVStoreMain#MakeConfigParser for setting the bootstrap parameter for
     * the properties.
     *
     * See #COMMON_DNS_CACHE_TTL and #COMMON_DNS_CACHE_NEGATIVE_TTL for the
     * parameters for the SNA and the services mornitored.
     */
    public static final String INETADDR_CACHE_POLICY_PROP =
        "networkaddress.cache.ttl";
    public static final String INETADDR_CACHE_NEGATIVE_POLICY_PROP =
        "networkaddress.cache.negative.ttl";

    /**
     * InetAddress name resolution cache ttl in seconds, with 0 meaning no
     * caching, and -1 meaning caching forever.
     *
     * <p> The parameters correspond to the #INETADDR_CACHE_POLICY_PROP and
     * #INETADDR_CACHE_NEGATIVE_POLICY_PROP.
     *
     * <p> The motivation of having these parameters instead of letting the
     * store administrators just set up their JVM environment (e.g. modify the
     * master conf/security/java.security file) is that the people deploying
     * the store may not be in a position or may be quite inconveneint to
     * change the Java installation on the host. Thus it is better for them to
     * set up a KV store admin script to change parameters.
     *
     * <p> The parameters can take effect on a JVM process only once. Therefore
     * when changing these parameters, the JVM needs to be restarted. Services
     * monitored by the SNs will be restarted automatically, but the SNs
     * themsevles need to be restarted manually.
     *
     * <p> There are hidden copies of the parameters for the monitored services
     * including Admins, RNs and ANs for keeping the values consistent between
     * service JVM security properties and the SN parameters. See
     * #COMMON_DNS_CACHE_TTL_VALUE and #COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE for
     * more detail.
     *
     * <h1>Order of Precedence</h1>
     * <p> If a store administrator changed the parameter values in
     * StorageNodeParams, the changed values are taken as the JVM security
     * property values. If not and the BootstrapParams is set through the
     * makebootconfig, then the bootstrap values are copied into the
     * StorageNodeParams and taken as the security property values. Otherwise,
     * the default values are taken. That is, when starting an SNA, the JVM
     * security property values are set to the corresponding StorageNodeParams
     * values if exist, otherwise to the corresponding BootstrapParams values
     * if exist, otherwise to the default values.
     *
     * <p> As policy parameters, if a store administrator changed the values
     * with the "change-policy" command, new SNs will have the
     * StorageNodeParams set as the policy values. The policy values can be
     * overriden by the BootstrapParams values using the makebootconfig. That
     * is, when registering an SNA, upon receiving the policy parameters from
     * the Admin, the StorageNodeParams values are set to the BootstrapParams
     * values if exist, otherwise the policy values if exist, otherwise to the
     * default values. Note that if the bootstrap values are not set, the store
     * administrator needs to restart the SN to make the policy values take
     * effect since the JVM security properties are set to the default values
     * upon SN start. In such cases we log a warning message in the
     * snaboot_*.log
     */
    public static final String COMMON_DNS_CACHE_TTL = "dnsCacheTTL";
    public static final String COMMON_DNS_CACHE_TTL_DEFAULT = "10";
    static { putState(COMMON_DNS_CACHE_TTL,
                      COMMON_DNS_CACHE_TTL_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.BOOT, Info.SNA, Info.POLICY),
                      Scope.STORE,
                      -1, Integer.MAX_VALUE,
                      null); }
    public static final String
        COMMON_DNS_CACHE_NEGATIVE_TTL = "dnsCacheNegativeTTL";
    public static final String
        COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT = "10";
    static { putState(COMMON_DNS_CACHE_NEGATIVE_TTL,
                      COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.BOOT, Info.SNA, Info.POLICY),
                      Scope.STORE,
                      -1, Integer.MAX_VALUE,
                      null); }

    /**
     * Value copies of the COMMON_DNS_CACHE_TTL and
     * COMMON_DNS_CACHE_NEGATIVE_TTL parameters for each service to detect
     * inconsistency among configuration data structures.
     *
     * <p> These parameters exist so that we could use the parameter verify
     * mechanism (see ParamConsistencyChecker) to detect inconsistency between
     * the security property value of a service JVM and the values of the
     * common dns cache ttl parameters associated with the SN in the
     * configuration file.
     *
     * <p> The in-memory parameter values always equal to the JVM security
     * property values, that is, we override the values with the JVM security
     * properties when we load the service parameters into memory. The service
     * parameters in the configuation file are configured to equal to the SN
     * one, that is, we change the following service parameters when the
     * StorageNodeParams changes.
     */
    public static final String COMMON_DNS_CACHE_TTL_VALUE = "dnsCacheTTLValue";
    static { putState(COMMON_DNS_CACHE_TTL_VALUE,
                      COMMON_DNS_CACHE_TTL_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.REPNODE, Info.ARBNODE,
                                 Info.HIDDEN),
                      Scope.STORE,
                      -1, Integer.MAX_VALUE,
                      null); }
    public static final String
        COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE = "dnsCacheNegativeTTLValue";
    static { putState(COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE,
                      COMMON_DNS_CACHE_NEGATIVE_TTL_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.REPNODE, Info.ARBNODE,
                                 Info.HIDDEN),
                      Scope.STORE,
                      -1, Integer.MAX_VALUE,
                      null); }

    /*
     * RepNode parameters
     */

    public static final String REQUEST_QUIESCE_TIME = "requestQuiesceTime";
    public static final String REQUEST_QUIESCE_TIME_DEFAULT = "60 s";
    static { putState(REQUEST_QUIESCE_TIME,
                      REQUEST_QUIESCE_TIME_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_MAX_ACTIVE_REQUESTS = "rnMaxActiveRequests";
    public static final String RN_MAX_ACTIVE_REQUESTS_DEFAULT = "100";
    static { putState(RN_MAX_ACTIVE_REQUESTS,
                      RN_MAX_ACTIVE_REQUESTS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_REQUEST_THRESHOLD_PERCENT =
        "rnRequestThresholdPercent";
    public static final String RN_REQUEST_THRESHOLD_PERCENT_DEFAULT = "90";
    static { putState(RN_REQUEST_THRESHOLD_PERCENT,
                      RN_REQUEST_THRESHOLD_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_NODE_LIMIT_PERCENT = "rnNodeLimitPercent";
    public static final String RN_NODE_LIMIT_PERCENT_DEFAULT = "85";
    static { putState(RN_NODE_LIMIT_PERCENT,
                      RN_NODE_LIMIT_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode enabled request type
     */
    public static final String RN_ENABLED_REQUEST_TYPE = "rnEnabledRequestType";
    public static final String RN_ENABLED_REQUEST_TYPE_DEFAULT = "ALL";
    static { putState(RN_ENABLED_REQUEST_TYPE,
                      RN_ENABLED_REQUEST_TYPE_DEFAULT, Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.RONLY, Info.HIDDEN),
                      Scope.STORE, 0, 0,
                      new String[] { "ALL", "NONE", "READONLY" }); }

    /**
     * Determines whether the original log files are to be retained after a
     * network restore operation on an RN.
     */
    public static final String RN_NRCONFIG_RETAIN_LOG_FILES =
        "rnNRConfigRetainLogFiles";
    public static final String RN_NRCONFIG_RETAIN_LOG_FILES_DEFAULT = "false";
    static { putState(RN_NRCONFIG_RETAIN_LOG_FILES,
                      RN_NRCONFIG_RETAIN_LOG_FILES_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Internal parameter used to control whether the new JE Authoritative
     * master semantics are to be used. By default, the new JE authoritative
     * semantics will be used:
     *
     * 1) Absolute consistency reads will wait for a quorum of replicas to be
     * active, if they aren't already.
     *
     * 2) Durable writes will be similarly delayed as necessary reducing the
     * probability of hard rollbacks.
     *
     * Note that this parameter requires a RN restart, since the underlying JE
     * parameter is not mutable; it could be made mutable but is not worth the
     * additional effort for this ephemeral parameter.
     *
     * This parameter exists primarily to facilitate A/B performance
     * comparisons, and to provide a easy way to revert to the older looser
     * auth master semantics with the above drawbacks, should it become
     * necessary to do so.
     *
     * These parameters should be removed once the new code has been in use for
     * a couple of releases and is no longer needed.
     *
     * @since 19.5
     */
    public static final String RN_DISABLE_AUTH_MASTER = "rnDisableAuthMaster";
    public static final String RN_DISABLE_AUTH_MASTER_DEFAULT = "false";

    static { putState(RN_DISABLE_AUTH_MASTER,
                      RN_DISABLE_AUTH_MASTER_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * The max number of topo changes to be retained in the topology stored
     * locally at an RN. This value applies to table metadata as well.
     */
    public static final String RN_MAX_TOPO_CHANGES = "rnMaxTopoChanges";
    public static final String RN_MAX_TOPO_CHANGES_DEFAULT = "1000";
    static { putState(RN_MAX_TOPO_CHANGES,
                      RN_MAX_TOPO_CHANGES_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * These two are not dynamically configurable at this time
     */
    public static final int RN_HEAP_MB_MIN = ParameterUtils.applyMinHeapMB(48);
    public static final int RN_CACHE_MB_MIN = 33;

    /**
     * The max number of malloc arenas used by an RN.
     * Setting this parameter to a non-zero value results in the environment
     * variable MALLOC_ARENA_MAX being set to that non-zero value for the RN
     * process.
     *
     * Use a value of "0" to leave the environment variable unset.
     */
    public static final String RN_MALLOC_ARENA_MAX = "rnMallocArenaMax";
     /**
      * Using a single arena results in a smaller, and perhaps more importantly
      * predictable, heap size, which is needed for correct management of
      * native (non-Java heap) memory. Although we do not use native memory
      * directly, it may be used by some Java facilities. With multiple arenas,
      * each arena can grow to some non-deterministic max size wasting memory,
      * and provoking page swapping activity.
      */
    public static final String RN_MALLOC_ARENA_MAX_DEFAULT = "1";
    static { putState(RN_MALLOC_ARENA_MAX,
                      RN_MALLOC_ARENA_MAX_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE,
                      0, 1024,
                      null); }

    /**
     * The portion of an RN's memory set aside for the JE environment cache.
     *
     * <p>For non-Zing JVMs, a significant amount of headroom is needed for GC.
     * With the Zing JVM, this headroom is unnecessary.
     */
    public static final String RN_CACHE_PERCENT = "rnCachePercent";
    public static final String RN_CACHE_PERCENT_DEFAULT =
        JVMSystemUtils.ZING_JVM ? "90" : "70";
    static { putState(RN_CACHE_PERCENT,
                      RN_CACHE_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.SERVICE, 1, 90, null); }

    /**
     * The maximum number of index keys generated per row.
     */
    public static final String RN_MAX_INDEX_KEYS_PER_ROW = "rnMaxIndexKeysPerRow";
    public static final String RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT = "10000";
    static { putState(RN_MAX_INDEX_KEYS_PER_ROW,
                      RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.HIDDEN,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE, 1, Integer.MAX_VALUE, null); }

    /** The RN node type. */
    public static final String RN_NODE_TYPE = "rnNodeType";
    public static final String RN_NODE_TYPE_DEFAULT = "ELECTABLE";
    static { putState(RN_NODE_TYPE,
                      RN_NODE_TYPE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.BOOT, Info.HIDDEN),
                      Scope.SERVICE); }

    /**
     * The Replication Node ID.
     */
    public static final String RP_RN_ID = "repNodeId";
    public static final String RP_RN_ID_DEFAULT = "-1";
    static { putState(RP_RN_ID,
                      RP_RN_ID_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.RONLY)); }

    /*
     * RepNode KVSessionManager internal operation parameters
     */

    /**
     * General timeout for session-related requests. This is currently only used
     * for the write timeout for persistent sessions. It's purposely short so that
     * it can fail fast if the RN is in a read-only state, allowing the transient
     * session mechanism to take over.
     */
    public static final String RN_SESS_REQUEST_TIMEOUT =
        "rnSessionRequestTimeout";
    public static final String RN_SESS_REQUEST_TIMEOUT_DEFAULT = "1 s";
    static { putState(RN_SESS_REQUEST_TIMEOUT,
                      RN_SESS_REQUEST_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Timeout for session lookup requests.
     */
    public static final String RN_SESS_LOOKUP_REQUEST_TIMEOUT =
        "rnSessionLookupRequestTimeout";
    public static final String RN_SESS_LOOKUP_REQUEST_TIMEOUT_DEFAULT =
        "200 ms";
    static { putState(RN_SESS_LOOKUP_REQUEST_TIMEOUT,
                      RN_SESS_LOOKUP_REQUEST_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Consistency requirement for session lookup.
     */
    public static final String RN_SESS_LOOKUP_CONSISTENCY_LIMIT =
        "rnSessionLookupConsistencyLimit";
    public static final String RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT =
        "30 s";
    static { putState(RN_SESS_LOOKUP_CONSISTENCY_LIMIT,
                      RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Timeout for session lookup consistency.
     */
    public static final String RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT =
        "rnSessionLookupConsistencyTimeout";
    public static final String RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT_DEFAULT =
        "3 s";
    static { putState(RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT,
                      RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Timeout for session logout requests.
     */
    public static final String RN_SESS_LOGOUT_REQUEST_TIMEOUT =
        "rnSessionLogoutRequestTimeout";
    public static final String RN_SESS_LOGOUT_REQUEST_TIMEOUT_DEFAULT =
        "20 s";
    static { putState(RN_SESS_LOGOUT_REQUEST_TIMEOUT,
                      RN_SESS_LOGOUT_REQUEST_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode mount point and size (optional) parameters
     */

    public static final String RN_MOUNT_POINT = "rnMountPoint";
    public static final String RN_MOUNT_POINT_DEFAULT = "";
    static { putState(RN_MOUNT_POINT,
                      RN_MOUNT_POINT_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.RONLY)); }

    public static final String RN_MOUNT_POINT_SIZE = "rnMountPointSize";
    public static final String RN_MOUNT_POINT_SIZE_DEFAULT = "0";
    static { putState(RN_MOUNT_POINT_SIZE,
                      RN_MOUNT_POINT_SIZE_DEFAULT,
                      Type.SIZE,
                      EnumSet.of(Info.REPNODE)); }

    /*
     * RepNode log mount point and size (optional) parameters
     */

    public static final String RNLOG_MOUNT_POINT = "rnLogMountPoint";
    public static final String RNLOG_MOUNT_POINT_DEFAULT = "";
    static { putState(RNLOG_MOUNT_POINT,
                      RNLOG_MOUNT_POINT_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.RONLY)); }

    public static final String RNLOG_MOUNT_POINT_SIZE = "rnLogMountPointSize";
    public static final String RNLOG_MOUNT_POINT_SIZE_DEFAULT = "0";
    static { putState(RNLOG_MOUNT_POINT_SIZE,
                      RNLOG_MOUNT_POINT_SIZE_DEFAULT,
                      Type.SIZE,
                      EnumSet.of(Info.REPNODE)); }

    /*
     * RepNode socket configuration parameters associated with RN's request
     * handling interface.
     */

    public static final String RN_RH_SO_BACKLOG = "rnRHSOBacklog";
    public static final String RN_RH_SO_BACKLOG_DEFAULT = "1024";
    static { putState(RN_RH_SO_BACKLOG,
                      RN_RH_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_RH_SO_READ_TIMEOUT =
            "rnRHSOReadTimeout";
    public static final String RN_RH_SO_READ_TIMEOUT_DEFAULT = "30 s";
    static { putState(RN_RH_SO_READ_TIMEOUT,
                      RN_RH_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_RH_SO_CONNECT_TIMEOUT =
            "rnRHSOConnectTimeout";
    /**
     * The default value of 3sec is chosen so that it's consistent with the
     * default request timeout value of 5 sec, leaving 2 sec for a retry at
     * some other RN when the initially targeted RN is unreachable.
     *
     * <p>It takes 66ms to send a packet diametrically across our 40K km
     * circumference planet. The tcp connect sequence involves the following
     * packet exchange: 1) syn 2) syn-ack 3) ack for a total packet exchange
     * time of 66 * 3 = 198ms. So a 3 sec open timeout should be ample even
     * after allowing for switch level overheads and a busy server.
     */
    public static final String RN_RH_SO_CONNECT_TIMEOUT_DEFAULT = "3 s";
    static { putState(RN_RH_SO_CONNECT_TIMEOUT,
                      RN_RH_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * The maximum number of threads in the thread pool the async request
     * handler uses to execute incoming requests.
     */
    public static final String RN_RH_ASYNC_EXEC_MAX_THREADS =
        "rnRHAsyncExecMaxThreads";
    // TODO: Update operations take longer than 1 millisecond, so we may need
    // to increase this value at some point in order to increase update
    // throughput unless we reduce update latency through other changes.
    /**
     * Provide only enough threads to support the maximum number of concurrent
     * requests that we need to handle. If we expect to handle a maximum of
     * roughly 40,000 operations per second, with operations taking about 1
     * millisecond, then 40 threads should be sufficient. Use a 100 to provide
     * plenty of margin.
     */
    public static final String RN_RH_ASYNC_EXEC_MAX_THREADS_DEFAULT = "100";
    static { putState(RN_RH_ASYNC_EXEC_MAX_THREADS,
                      RN_RH_ASYNC_EXEC_MAX_THREADS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE,
                      1, Integer.MAX_VALUE,
                      null); }

    /**
     * The amount of time that a thread in the thread pool the async request
     * handler uses to execute incoming requests will remain idle before
     * terminating.
     */
    public static final String RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE =
        "rnRHAsyncExecThreadKeepAlive";
    public static final String RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE_DEFAULT =
        "1 MIN";
    static { putState(RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE,
                      RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE,
                      1L, Long.MAX_VALUE,
                      null); }

    /**
     * An obsolete parameter, introduced in 19.5 and obsoleted in 20.1, and now
     * ignored, that used to specify the size of the queue used to store
     * incoming requests while they are waiting to be handled by a thread in
     * the request handler thread pool. Now the queue is sized based on the
     * difference between RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS and
     * RN_RH_ASYNC_EXEC_MAX_THREADS.
     *
     * @since 19.1
     * @deprecated as of 20.1, see #RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS
     */
    @Deprecated
    private static final String RN_RH_ASYNC_EXEC_QUEUE_SIZE =
        "rnRHAsyncExecQueueSize";
    /**
     * @deprecated as of 20.1, see #RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT
     */
    @Deprecated
    private static final String RN_RH_ASYNC_EXEC_QUEUE_SIZE_DEFAULT = "0";
    static { putState(RN_RH_ASYNC_EXEC_QUEUE_SIZE,
                      RN_RH_ASYNC_EXEC_QUEUE_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE,
                      0, 10000,
                      null); }

    /**
     * The maximum number of concurrent asynchronous requests that an RN can
     * handle before it should do throttling.
     *
     * @since 20.1
     */
    public static final String RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS =
        "rnRHAsyncMaxConcurrentRequests";
    /**
     * Provide a value enough larger than RN_RH_ASYNC_EXEC_MAX_THREADS_DEFAULT
     * to handle short bursts of requests.
     */
    public static final int RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT = 1000;
    static { putState(RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS,
                      Integer.toString(
                          RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT),
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY),
                      Scope.STORE,
                      1, Integer.MAX_VALUE,
                      null); }

    /**
     * The maximum number of active socket connections that the async request
     * handler will permit, or 0 if there is no limit. This limit will be
     * applied to all RN services.
     */
    public static final String RN_ACCEPT_MAX_ACTIVE_CONNS =
        "rnAcceptMaxActiveConns";
    /**
     * The default value is 80% of the 10240 value that we suggest in the
     * Tuning Appendix of the Administrator's Guide as the limit on the number
     * of open file descriptors that should be permitted per RN. The file
     * descriptor limit is likely the limiting factor on the number of incoming
     * socket connections that a process can support simultaneously.
     */
    public static final String RN_ACCEPT_MAX_ACTIVE_CONNS_DEFAULT =
        Integer.toString((int) (0.8 * 10240));
    static { putState(RN_ACCEPT_MAX_ACTIVE_CONNS,
                      RN_ACCEPT_MAX_ACTIVE_CONNS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY),
                      Scope.STORE,
                      0, 100000,
                      null); }

    /*
     * RepNode socket configuration parameters associated with RN's monitor
     * interface.
     */

    public static final String RN_MONITOR_SO_BACKLOG = "rnMonitorSOBacklog";
    public static final String RN_MONITOR_SO_BACKLOG_DEFAULT = "1024";
    static { putState(RN_MONITOR_SO_BACKLOG,
                      RN_MONITOR_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_MONITOR_SO_READ_TIMEOUT =
            "rnMonitorSOReadTimeout";
    public static final String RN_MONITOR_SO_READ_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_MONITOR_SO_READ_TIMEOUT,
                      RN_MONITOR_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_MONITOR_SO_CONNECT_TIMEOUT =
            "rnMonitorSOConnectTimeout";
    public static final String RN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_MONITOR_SO_CONNECT_TIMEOUT,
                      RN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * The repository size that RN MonitorAgent store instrumented data.
     * If the size less than or equal to 0, then MonitorAgent will prune all
     * instrumented data.
     */
    public static final String RN_MONITOR_AGENT_REPO_SIZE =
        "rnMonitorAgentRepoSize";
    public static final String RN_MONITOR_AGENT_REPO_SIZE_DEFAULT = "10000";
    static { putState(RN_MONITOR_AGENT_REPO_SIZE,
                      RN_MONITOR_AGENT_REPO_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode socket configuration parameters associated with the RN's/AN's
     * admin interface.
     */

    public static final String RN_ADMIN_SO_BACKLOG = "rnAdminSOBacklog";
    public static final String RN_ADMIN_SO_BACKLOG_DEFAULT = "1024";
    static { putState(RN_ADMIN_SO_BACKLOG,
                      RN_ADMIN_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_ADMIN_SO_READ_TIMEOUT =
            "rnAdminSOReadTimeout";
    public static final String RN_ADMIN_SO_READ_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_ADMIN_SO_READ_TIMEOUT,
                      RN_ADMIN_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_ADMIN_SO_CONNECT_TIMEOUT =
            "rnAdminSOConnectTimeout";
    public static final String RN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_ADMIN_SO_CONNECT_TIMEOUT,
                      RN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode socket configuration parameters associated with the RN's login
     * interface.
     */

    public static final String RN_LOGIN_SO_BACKLOG = "rnLoginSOBacklog";
    public static final String RN_LOGIN_SO_BACKLOG_DEFAULT = "1024";
    static { putState(RN_LOGIN_SO_BACKLOG,
                      RN_LOGIN_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_LOGIN_SO_READ_TIMEOUT =
            "rnLoginSOReadTimeout";
    public static final String RN_LOGIN_SO_READ_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_LOGIN_SO_READ_TIMEOUT,
                      RN_LOGIN_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_LOGIN_SO_CONNECT_TIMEOUT =
            "rnLoginSOConnectTimeout";
    public static final String RN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_LOGIN_SO_CONNECT_TIMEOUT,
                      RN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode parameters associated with partition migration.
     */

    public static final String RN_PM_CONCURRENT_SOURCE_LIMIT =
            "rnPMConcurrentSourceLimit";
    public static final String RN_PM_CONCURRENT_SOURCE_LIMIT_DEFAULT = "2";
    static { putState(RN_PM_CONCURRENT_SOURCE_LIMIT,
                      RN_PM_CONCURRENT_SOURCE_LIMIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_PM_CONCURRENT_TARGET_LIMIT =
            "rnPMConcurrentTargetLimit";
    public static final String RN_PM_CONCURRENT_TARGET_LIMIT_DEFAULT = "4";
    static { putState(RN_PM_CONCURRENT_TARGET_LIMIT,
                      RN_PM_CONCURRENT_TARGET_LIMIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_PM_SO_READ_WRITE_TIMEOUT =
            "rnPMSOReadWriteTimeout";
    public static final String RN_PM_SO_READ_WRITE_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_PM_SO_READ_WRITE_TIMEOUT,
                      RN_PM_SO_READ_WRITE_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_PM_SO_CONNECT_TIMEOUT =
            "rnPMSOConnectTimeout";
    public static final String RN_PM_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_PM_SO_CONNECT_TIMEOUT,
                      RN_PM_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_PM_WAIT_AFTER_BUSY = "rnPMWaitAfterBusy";
    public static final String RN_PM_WAIT_AFTER_BUSY_DEFAULT = "60 s";
    static { putState(RN_PM_WAIT_AFTER_BUSY,
                      RN_PM_WAIT_AFTER_BUSY_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String RN_PM_WAIT_AFTER_ERROR = "rnPMWaitAfterError";
    public static final String RN_PM_WAIT_AFTER_ERROR_DEFAULT = "30 s";
    static { putState(RN_PM_WAIT_AFTER_ERROR,
                      RN_PM_WAIT_AFTER_ERROR_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * RepNode parameters associated with statistics gathering.
     */

    /**
     * Whether RNs should gather distribution statistics.
     */
    public static final String RN_SG_ENABLED = "rnStatisticsEnabled";
    public static final String RN_SG_ENABLED_DEFAULT = "true";
    static { putState(RN_SG_ENABLED,
                      RN_SG_ENABLED_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * The time interval at which RNs should gather distribution statistics.
     */
    public static final String RN_SG_INTERVAL = "rnStatisticsGatherInterval";
    public static final String RN_SG_INTERVAL_DEFAULT = "24 h";
    static { putState(RN_SG_INTERVAL,
                      RN_SG_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * The time interval at which RNs should update table size information.
     * Updates are disabled if RN_SG_INCLUDE_STORAGE_SIZE == false, the
     * value == 0, or the value is &gt;= RN_SG_INTERVAL.
     *
     * Disabled by default.
     */
    public static final String RN_SG_SIZE_UPDATE_INTERVAL =
                                        "rnTableSizeUpdateInterval";
    public static final String RN_SG_SIZE_UPDATE_INTERVAL_DEFAULT = "0 s";
    static { putState(RN_SG_SIZE_UPDATE_INTERVAL,
                      RN_SG_SIZE_UPDATE_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * How long distribution statistics should be retained.
     */
    public static final String RN_SG_TTL = "rnStatisticsTTL";
    public static final String RN_SG_TTL_DEFAULT = "60 days";
    static { putState(RN_SG_TTL,
                      RN_SG_TTL_DEFAULT,
                      Type.TIME_TO_LIVE,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * Whether to include information on storage size when gathering
     * statistics.
     */
    public static final String RN_SG_INCLUDE_STORAGE_SIZE =
        "rnStatisticsIncludeStorageSize";
    public static final String RN_SG_INCLUDE_STORAGE_SIZE_DEFAULT = "true";
    static { putState(RN_SG_INCLUDE_STORAGE_SIZE,
                      RN_SG_INCLUDE_STORAGE_SIZE_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String RN_SG_LEASE_DURATION =
            "rnStatisticsLeaseDuration";
    public static final String RN_SG_LEASE_DURATION_DEFAULT = "10 min";
    static { putState(RN_SG_LEASE_DURATION,
                      RN_SG_LEASE_DURATION_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String RN_SG_SLEEP_WAIT =
            "rnStatisticsSleepWaitDuration";
    public static final String RN_SG_SLEEP_WAIT_DEFAULT = "60 s";
    static { putState(RN_SG_SLEEP_WAIT,
                      RN_SG_SLEEP_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * Value factoring into per-partition size check. Value of zero (default)
     * disables the check.
     */
    public static final String RN_PARTITION_SIZE_PERCENT =
            "rnPartitionSizePercent";
    public static final String RN_PARTITION_SIZE_PERCENT_DEFAULT = "0";
    static { putState(RN_PARTITION_SIZE_PERCENT,
                      RN_PARTITION_SIZE_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * RepNode ttl for tombstone of multi-region tables. This parameter is
     * currently read-only and users cannot change it. Please note in future
     * if the default value of this parameter changes, the parameter property
     * must be changed to make it modifiable. This is because the user may
     * want to choose the new or old values in different use cases.
     */
    public static final String RN_TOMBSTONE_TTL = "tombstoneTTL";
    public static final String RN_TOMBSTONE_TTL_DEFAULT = "7 days";
    static { putState(RN_TOMBSTONE_TTL,
                      RN_TOMBSTONE_TTL_DEFAULT,
                      Type.TIME_TO_LIVE,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.RONLY),
                      Scope.STORE); }

    /* RepNode GC logging configuration parameters */

    /**
     * The number of GC log files that are retained for RNs and Arbiters.
     */
    public static final String RN_NUM_GC_LOG_FILES = "rnNumGCLogFiles";
    public static final String RN_NUM_GC_LOG_FILES_DEFAULT = "10";
    static { putState(RN_NUM_GC_LOG_FILES,
                      RN_NUM_GC_LOG_FILES_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * The approximate maximum size in bytes for RN and Arbiter GC log files.
     */
    public static final String RN_GC_LOG_FILE_SIZE = "rnGCLogFileSize";
    public static final String RN_GC_LOG_FILE_SIZE_DEFAULT =
        String.valueOf(5 * MB);
    static { putState(RN_GC_LOG_FILE_SIZE,
                      RN_GC_LOG_FILE_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /* RepNode wait time in store contraction */
    public static final String RN_CONTRACT_WAIT = "rnStoreContractWait";
    public static final String RN_CONTRACT_WAIT_DEFAULT = "600 s";
    static { putState(RN_CONTRACT_WAIT,
                      RN_CONTRACT_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }


    /* RepNode GC configuration parameters */

    /** CMS GC params */
    public static final String RN_CMS_GC_PARAMS = "rnCMSGCParams";
    /**
     * These are the default CMS GC parameters that are applied if the CMS GC
     * is explicitly enabled by specifying -XX:+UseConcMarkSweepGC. They can be
     * overridden and/or enhanced by user-supplied parameters.
     *
     * A note about the rationale behind the CMSInitiatingOccupancyFraction
     * setting: We currently size the JE cache size (by default) at 70% of the
     * total heap and use a NewRatio (the ratio of old to new space) of 18. So
     * if NewRatio + 1 represents the total heap space, old space is
     *
     * (1 - ( 1 / (NewRatio + 1)) * 100 =
     *
     * (1 - (1 / (18 + 1)) * 100 = 94.7% of total heap space.
     *
     * 70% of total heap used for the JE cache represents (70/94.7)*100 =
     * 73.92% of old space. A CMS setting of 77% allows us a 77% - 73.92% =
     * 3.08% margin for error when the JE cache is full, before unproductive
     * CMS cycles kick in.
     *
     * The applications metadata requirements (topology, tables, index,
     * security, sockets, RMI handles, etc.) as well as the jvm's class level
     * metadata have grown in r3 and need to be accounted for. The metadata
     * size is particularly relevant when the heap sizes are relatively modest,
     * but the number of nodes or schema is large, e.g. the default BDA
     * configuration. So we add another 3% to the percentage calculated above,
     * to arrive at 77% + 3% = 80% as the value for the occupancy threshold for
     * the CMS. Using a constant delta percentage is not the best way to
     * account for this "fixed" (unrelated to data size) overhead. So in future
     * we will calculate a more dynamic value for the CMS threshold and the JE
     * cache size as well, to account for the actual metadata overhead which
     * can vary over the lifetime of the application, even as the the data
     * sizes and machine resources are held constant.
     *
     * SR 22779 and SR 23652 have more detailed discussions on this subject.
     */
    public static final String RN_CMS_GC_PARAMS_DEFAULT =
        "-XX:+UseConcMarkSweepGC " +
        "-XX:+UseParNewGC " +
        "-XX:+DisableExplicitGC " +
        "-XX:NewRatio=18 " +
        "-XX:SurvivorRatio=4 " +
        "-XX:MaxTenuringThreshold=10 " +
        "-XX:CMSInitiatingOccupancyFraction=80 " ;
    static { putState(RN_CMS_GC_PARAMS,
                      RN_CMS_GC_PARAMS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** G1 GC params */
    public static final String RN_G1_GC_PARAMS = "rnG1GCParams";
    /**
     * These are the G1 GC parameters that are applied by default.  They can be
     * overridden and/or enhanced by user-supplied parameters.
     *
     * The number of ConcurrentGCThreads is limited to the max number of
     * ParallelGCThreads, set by the SNA when it starts up the RN.
     *
     * Note that we are relying on the default value of the experimental
     * parameter XX:G1NewSizePercent of 5%. We don't want to set it explicitly
     * since it's experimental and could be removed from the jdk at any time.
     *
     * Please review the SR below for a discussion of the g1 settings.
     *
     * https://sleepycat-tools.us.oracle.com/trac/ticket/24038#comment:115
     */
    public static final String RN_G1_GC_PARAMS_DEFAULT =
        "-XX:+UseG1GC " +

        /**
         * Try keep the 99% at 100ms
         */
        "-XX:MaxGCPauseMillis=100 " +

        /**
         * Note that this is higher than with CMS (80%) based on JE tuning. May
         * need further tuning.
         */
        "-XX:InitiatingHeapOccupancyPercent=85 " +

        /**
         * Use max region size given our typical size of heap
         */
        "-XX:G1HeapRegionSize=32m " +

       /**
        * The target number of mixed garbage collections within which the
        * regions with at most G1MixedGCLiveThresholdPercent live data should
        * be collected.
        *
        * KVSTORE-888: Going out of cache needs more aggressive GC. Several
        * experiments have been done changing the value of this parameter
        * from the previous value of 32. 12 happens to be the least aggressive
        * setting which avoids full gc pauses.  This was with 25% out-of-cache.
        * If needed, this value can be reduced further.
        */
        "-XX:G1MixedGCCountTarget=12 " +

        /**
         * G1RSetRegionEntries determines the entries in RSet fine grain table.
         * Increasing the value decreases the incoming memory slice that must be
         * scanned for references into the region, but increases the GC storage
         * overhead since we need more entries. The default value is 768.
         *
         * The G1RSetRegionEntries flag is obsolete as of JDK version 18 and
         * the option is no longer supported as of version 19. See:
         * https://bugs.openjdk.java.net/browse/JDK-8266722
         *
         * [KVSTORE-1529]
         */
        (getJavaMajorVersion() < 18 ?
         "-XX:G1RSetRegionEntries=2560 " :
         "") +

         /**
          * Threshold over which to initiate the mixed garbage collection cycle.
          * 5% is the default but is explicitly specified here to hold this
          * value constant in case the default changes in future.
          */
        "-XX:G1HeapWastePercent=5 " +

        /**
         * Disable resizing based on JE tuning experience in multi-threaded
         * environment. It's supposed to reduce thread contention resulting from
         * resizing of PLABs (Promotion Local Allocation Buffers) during young
         * generation collections.
         */
        "-XX:-ResizePLAB " +

        /**
         * Stop RMI initiated explicit GCs
         */
        "-XX:+DisableExplicitGC ";
    static { putState(RN_G1_GC_PARAMS,
                      RN_G1_GC_PARAMS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** Zing (Azul) GC params */
    public static final String RN_ZING_GC_PARAMS = "rnZingGCParams";
    /**
     * These are the Zing GC parameters that are applied by default. They can
     * be overridden and/or enhanced by user-supplied parameters.
     *
     * Zing supports some G1 options, but which should be used needs to be
     * determined by performance testing. There are also Zing options for
     * output and debugging that we may choose to add.
     *
     * Note that -XX:+UseGenPauselessGC (Continuously Concurrent Compacting
     * Collector or C4) is always used for Zing, even when G1 or CMS is
     * specified.
     */
    public static final String RN_ZING_GC_PARAMS_DEFAULT =
        "-XX:+UseGenPauselessGC ";
    static { putState(RN_ZING_GC_PARAMS,
                      RN_ZING_GC_PARAMS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * ArbNode parameters
     */

    public static final String AP_AN_ID = "arbNodeId";
    public static final String AP_AN_ID_DEFAULT = "-1";
    static { putState(AP_AN_ID,
                      AP_AN_ID_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.ARBNODE,
                                 Info.RONLY)); }

    /*
     * Admin parameters
     */

    /**
     * The ID of the admin.
     */
    public static final String AP_ID = "adminId";
    public static final String AP_ID_DEFAULT = "-1";
    static { putState(AP_ID,
                      AP_ID_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.RONLY)); }

    public static final String AP_WAIT_TIMEOUT = "waitTimeout";
    public static final String AP_WAIT_TIMEOUT_DEFAULT = "5 MINUTES";
    static { putState(AP_WAIT_TIMEOUT,
                      AP_WAIT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * Admin mount point and size (optional) parameters
     */

    public static final String ADMIN_MOUNT_POINT = "adminMountPoint";
    public static final String ADMIN_MOUNT_POINT_DEFAULT = "";
    static { putState(ADMIN_MOUNT_POINT,
                      ADMIN_MOUNT_POINT_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.ADMIN,
                                 Info.RONLY)); }

    public static final String ADMIN_MOUNT_POINT_SIZE = "adminMountPointSize";
    public static final String ADMIN_MOUNT_POINT_SIZE_DEFAULT = "0";
    static { putState(ADMIN_MOUNT_POINT_SIZE,
                      ADMIN_MOUNT_POINT_SIZE_DEFAULT,
                      Type.SIZE,
                      EnumSet.of(Info.ADMIN)); }

    /**
     * The approximate maximum size in bytes for store-wide debug log files,
     * the log files that consolidate logging output from all services. Values
     * 0 or less represent no limit. If serviceLogFileCompression is true and
     * the value of this parameter is greater than 0, the actual file limit is
     * adjusted to a larger value that takes the space savings from compression
     * into account. After reaching the limit, the logging subsystem switches
     * to a new store-wide log file. The value of this parameter can be changed
     * to change the amount of disk space devoted to saving debug log history.
     * @see #AP_LOG_FILE_COUNT
     */
    public static final String AP_LOG_FILE_LIMIT = "adminLogFileLimit";
    public static final String AP_LOG_FILE_LIMIT_DEFAULT =
        String.valueOf(5 * MB);
    static { putState(AP_LOG_FILE_LIMIT,
                      AP_LOG_FILE_LIMIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY),
                      Scope.STORE); }

    /**
     * The number of store-wide debug log files, the log files that consolidate
     * logging output from all services, that are retained. Values less than 1
     * are converted to 1. Any compressed rotated files are included in the
     * count regardless of the setting of the serviceLogFileCompression
     * parameter. When a new store-wide log file is created because the current
     * one has reached the maximum size specified by the adminLogFileLimit
     * parameter, the oldest existing log file will be removed if creating a
     * new file results in the number of log files to exceed this limit. The
     * value of this parameter can be changed to change the amount of disk
     * space devoted to saving debug log history.
     * @see #AP_LOG_FILE_LIMIT
     */
    public static final String AP_LOG_FILE_COUNT = "adminLogFileCount";
    public static final String AP_LOG_FILE_COUNT_DEFAULT = "20";
    static { putState(AP_LOG_FILE_COUNT,
                      AP_LOG_FILE_COUNT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY),
                      Scope.STORE); }

    /**
     * The amount of time that the admin stores the history of critical events
     * displayed by the show events command.
     */
    public static final String AP_EVENT_EXPIRY_AGE = "eventExpiryAge";
    public static final String AP_EVENT_EXPIRY_AGE_DEFAULT = "30 DAYS";
    static { putState(AP_EVENT_EXPIRY_AGE,
                      AP_EVENT_EXPIRY_AGE_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }


    /**
     * The number of events that the admin stores the history of critical events
     * displayed by the show events command.
     */
    public static final String AP_EVENT_MAX = "maxEvents";
    public static final String AP_EVENT_MAX_DEFAULT = "10000";
    static { putState(AP_EVENT_MAX,
                      AP_EVENT_MAX_DEFAULT,
                      Type.LONG,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String AP_BROADCAST_METADATA_RETRY_DELAY =
            "broadcastMetadataDelay";
    public static final String AP_BROADCAST_METADATA_RETRY_DELAY_DEFAULT =
            "10 s";
    static { putState(AP_BROADCAST_METADATA_RETRY_DELAY,
                      AP_BROADCAST_METADATA_RETRY_DELAY_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Polling interval for the parameter consistency thread.
     */
    public static final String AP_PARAM_CHECK_INTERVAL = "paramCheckInterval";
    public static final String AP_PARAM_CHECK_INTERVAL_DEFAULT = "5 MINUTES";
    static { putState(AP_PARAM_CHECK_INTERVAL,
                      AP_PARAM_CHECK_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * Polling interval for the software version update thread.
     */
    public static final String AP_VERSION_CHECK_INTERVAL =
        "versionCheckInterval";
    public static final String AP_VERSION_CHECK_INTERVAL_DEFAULT = "1 HOURS";
    static { putState(AP_VERSION_CHECK_INTERVAL,
                      AP_VERSION_CHECK_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * Flag to disable the parameter consistency thread.
     */
    public static final String AP_PARAM_CHECK_ENABLED =
        "paramConsistencyCheckThreadEnabled";
    public static final String AP_CHECK_ENABLED_DEFAULT = "true";
    static { putState(AP_PARAM_CHECK_ENABLED,
                      AP_CHECK_ENABLED_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.ADMIN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * Flag to disable the software version update thread.
     */
    public static final String AP_VERSION_CHECK_ENABLED =
        "versionThreadEnabled";
    public static final String AP_VERSION_CHECK_ENABLED_DEFAULT = "true";
    static { putState(AP_VERSION_CHECK_ENABLED,
                      AP_VERSION_CHECK_ENABLED_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.ADMIN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * Maximum time to wait for a plan executed by the parameter
     * consistency thread.
     */
    public static final String AP_PARAM_MAX_PLAN_WAIT =
        "paramConsistencyCheckMaxPlanWait";
    public static final String AP_PARAM_MAX_PLAN_WAIT_DEFAULT = "5 MINUTES";
    static { putState(AP_PARAM_MAX_PLAN_WAIT,
                      AP_PARAM_MAX_PLAN_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * TODO: add more description.
     * Note this is a percent
     */
    public static final String AP_BROADCAST_METADATA_THRESHOLD =
            "broadcastMetadataThreshold";
    public static final String AP_BROADCAST_METADATA_THRESHOLD_DEFAULT = "20";
    static { putState(AP_BROADCAST_METADATA_THRESHOLD,
                      AP_BROADCAST_METADATA_THRESHOLD_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE,
                      1, 100,
                      null); } /* threshold is a % */

    public static final String AP_BROADCAST_TOPO_RETRY_DELAY =
            "broadcastTopoDelay";
    public static final String AP_BROADCAST_TOPO_RETRY_DELAY_DEFAULT = "10 s";
    static { putState(AP_BROADCAST_TOPO_RETRY_DELAY,
                      AP_BROADCAST_TOPO_RETRY_DELAY_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * TODO: add more description.
     * Note this is a percent
     */
    public static final String AP_BROADCAST_TOPO_THRESHOLD =
            "broadcastTopoThreshold";
    public static final String AP_BROADCAST_TOPO_THRESHOLD_DEFAULT = "20";
    static { putState(AP_BROADCAST_TOPO_THRESHOLD,
                      AP_BROADCAST_TOPO_THRESHOLD_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE,
                      1, 100,
                      null); } /* threshold is a % */

    public static final String AP_MAX_TOPO_CHANGES = "maxTopoChanges";
    public static final String AP_MAX_TOPO_CHANGES_DEFAULT = "1000";
    static { putState(AP_MAX_TOPO_CHANGES,
                      AP_MAX_TOPO_CHANGES_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Time period that the admin waits for a relocated RepNode to come up and
     * acquire necessary data before deleting the old instance of the RepNode.
     */
    public static final String AP_WAIT_RN_CONSISTENCY = "waitRNConsistency";
    public static final String AP_WAIT_RN_CONSISTENCY_DEFAULT = "60 MINUTES";
    static { putState(AP_WAIT_RN_CONSISTENCY,
                      AP_WAIT_RN_CONSISTENCY_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** Polling time interval when the admin has failed over */
    public static final String AP_WAIT_ADMIN_FAILOVER = "waitAdminFailover";
    public static final String AP_WAIT_ADMIN_FAILOVER_DEFAULT = "1 MINUTES";
    static { putState(AP_WAIT_ADMIN_FAILOVER,
                      AP_WAIT_ADMIN_FAILOVER_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** Polling time interval when a RN has failed over */
    public static final String AP_WAIT_RN_FAILOVER = "waitRNFailover";
    public static final String AP_WAIT_RN_FAILOVER_DEFAULT = "1 MINUTES";
    static { putState(AP_WAIT_RN_FAILOVER,
                      AP_WAIT_RN_FAILOVER_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** Polling time interval when a service can't be reached */
    public static final String AP_WAIT_UNREACHABLE_SERVICE =
        "waitUnreachableService";
    public static final String AP_WAIT_UNREACHABLE_SERVICE_DEFAULT = "3 s";
    static { putState(AP_WAIT_UNREACHABLE_SERVICE,
                      AP_WAIT_UNREACHABLE_SERVICE_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Polling time interval to check if a partition migration has finished.
     */
    public static final String AP_CHECK_PARTITION_MIGRATION =
        "checkPartitionMigration";
    public static final String AP_CHECK_PARTITION_MIGRATION_DEFAULT = "10 s";
    static { putState(AP_CHECK_PARTITION_MIGRATION,
                      AP_CHECK_PARTITION_MIGRATION_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Polling time interval to check if an index population task has finished.
     */
    public static final String AP_CHECK_ADD_INDEX = "checkAddIndex";
    public static final String AP_CHECK_ADD_INDEX_DEFAULT = "10 s";
    static { putState(AP_CHECK_ADD_INDEX,
                      AP_CHECK_ADD_INDEX_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Maximum retry time when failed deploying new RepNode.
     */
    public static final String AP_NEW_RN_RETRY_TIME = "newRNRetryTime";
    public static final String AP_NEW_RN_RETRY_TIME_DEFAULT = "10 MINUTES";
    static { putState(AP_NEW_RN_RETRY_TIME,
                      AP_NEW_RN_RETRY_TIME_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * Polling time interval to check if a network restore task has finished.
     */
    public static final String AP_CHECK_NETWORK_RESTORE = "checkNetworkRestore";
    public static final String AP_CHECK_NETWORK_RESTORE_DEFAULT = "3 s";
    static { putState(AP_CHECK_NETWORK_RESTORE,
                      AP_CHECK_NETWORK_RESTORE_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /* Parameters to control GC logging in the Admin. */

    /**
     * The number of GC log files that are retained for Admin services.
     */
    public static final String AP_NUM_GC_LOG_FILES = "numGCLogFiles";
    public static final String AP_NUM_GC_LOG_FILES_DEFAULT = "10";
    static { putState(AP_NUM_GC_LOG_FILES,
                      AP_NUM_GC_LOG_FILES_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * The approximate maximum size in bytes for Admin service GC log files.
     */
    public static final String AP_GC_LOG_FILE_SIZE= "GCLogFileSize";
    public static final String AP_GC_LOG_FILE_SIZE_DEFAULT =
        String.valueOf(5 * MB);
    static { putState(AP_GC_LOG_FILE_SIZE,
                      AP_GC_LOG_FILE_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /** The Admin type. */
    public static final String AP_TYPE = "adminType";
    public static final String AP_TYPE_DEFAULT = "PRIMARY";
    static { putState(AP_TYPE,
                      AP_TYPE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.ADMIN,
                                 Info.BOOT, Info.HIDDEN),
                      Scope.SERVICE); }


    /**
     * The polling interval that the event recorder on the master admin uses
     * when checking for service events, including status changes, new stats,
     * new parameters, plan state changes, and replication state changes.
     */
    public static final String ADMIN_EVENT_RECORDER_POLLING_INTERVAL =
        "adminEventRecorderPollingInterval";
    public static final String ADMIN_EVENT_RECORDER_POLLING_INTERVAL_DEFAULT =
        "5 s";
    static { putState(ADMIN_EVENT_RECORDER_POLLING_INTERVAL,
                      ADMIN_EVENT_RECORDER_POLLING_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * Admin socket configuration parameters
     */

    /** Configures the socket used by the Admin's command service. */
    public static final String ADMIN_COMMAND_SERVICE_SO_BACKLOG =
        "adminCommandServiceSOBacklog";
    public static final String ADMIN_COMMAND_SERVICE_SO_BACKLOG_DEFAULT = "100";
    static { putState(ADMIN_COMMAND_SERVICE_SO_BACKLOG,
                      ADMIN_COMMAND_SERVICE_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                 Scope.STORE); }

    /** Configures the sockets used by the Admin's listener. */
    public static final String ADMIN_LISTENER_SO_BACKLOG =
        "adminListenerSOBacklog";
    public static final String ADMIN_LISTENER_SO_BACKLOG_DEFAULT = "100";
    static { putState(ADMIN_LISTENER_SO_BACKLOG,
                      ADMIN_LISTENER_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                 Scope.STORE); }

    /** Configures the socket used by the Admin's loginservice. */
    public static final String ADMIN_LOGIN_SO_BACKLOG =
        "adminLoginSOBacklog";
    public static final String ADMIN_LOGIN_SO_BACKLOG_DEFAULT = "100";
    static { putState(ADMIN_LOGIN_SO_BACKLOG,
                      ADMIN_LOGIN_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                 Scope.STORE); }

    /**
     * The max number of malloc arenas used by an Admin.
     * Setting this parameter to a non-zero value results in the environment
     * variable MALLOC_ARENA_MAX being set to that non-zero value for the Admin
     * process.
     *
     * Use a value of "0" to leave the environment variable unset.
     */
    public static final String ADMIN_MALLOC_ARENA_MAX = "adminMallocArenaMax";
     /**
      * Using a single arena results in a smaller, and perhaps more importantly
      * predictable, heap size, which is needed for correct management of
      * native (non-Java heap) memory. Although we do not use native memory
      * directly, it may be used by some Java facilities. With multiple arenas,
      * each arena can grow to some non-deterministic max size wasting memory,
      * and provoking page swapping activity.
      */
    public static final String ADMIN_MALLOC_ARENA_MAX_DEFAULT = "1";
    static { putState(ADMIN_MALLOC_ARENA_MAX,
                      ADMIN_MALLOC_ARENA_MAX_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.POLICY, Info.HIDDEN),
                      Scope.STORE,
                      0, 1024,
                      null); }

    /*
     * Admin Monitor parameters
     */

    /**
     * The time interval that the monitoring system in master Admin uses to poll
     * for status updates.  This value controls how frequently statistics in
     * each services are updated to monitoring system.
     */
    public static final String MP_POLL_PERIOD = "collectorPollPeriod";
    public static final String MP_POLL_PERIOD_DEFAULT = "20 s";
    static { putState(MP_POLL_PERIOD,
                      MP_POLL_PERIOD_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String MP_CREATE_CSV = "createCSV";
    public static final String MP_CREATE_CSV_DEFAULT = "false";
    static { putState(MP_CREATE_CSV,
                      MP_CREATE_CSV_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.ADMIN,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * StorageNode parameters
     */

    /**
     * The hostname used for network connections to services running on the
     * storage node.  This value is specified when calling 'makebootconfig' and
     * can only be changed by replacing the storage node with a new storage
     * node.
     */
    public static final String COMMON_HOSTNAME = "hostname";
    public static final String COMMON_HOSTNAME_DEFAULT = "localhost";
    static { putState(COMMON_HOSTNAME,
                      COMMON_HOSTNAME_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.RONLY)); }

    /**
     * The port used to access the registry that provides network contact
     * information for services running on the storage node.
     */
    public static final String COMMON_REGISTRY_PORT = "registryPort";
    public static final String COMMON_REGISTRY_PORT_DEFAULT = "-1";
    static { putState(COMMON_REGISTRY_PORT,
                      COMMON_REGISTRY_PORT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.RONLY),

                      /*
                       * The scope for this parameter; in this case the value
                       * applies to a particular service.
                       */
                      Scope.SERVICE,
                      0, 65535,
                      null); }

    /**
     * The port used to access the admin web service that provides Http
     * access of admin command service running on the storage node.
     */
    public static final String ADMIN_WEB_SERVICE_PORT = "adminWebPort";
    public static final String ADMIN_WEB_SERVICE_PORT_DEFAULT = "-1";
    static { putState(ADMIN_WEB_SERVICE_PORT,
                      ADMIN_WEB_SERVICE_PORT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.RONLY),
                      Scope.SERVICE,
                      -1, 65535,
                      null); }

    /**
     * The range of ports used for services on the storage node.  The value is
     * specified either the low and high port numbers separated by a comma, or
     * '0' to represent that any free port may be used.
     */
    public static final String COMMON_SERVICE_PORTRANGE = "servicePortRange";
    /**
     * The default is to use any free port.
     */
    public static final String COMMON_SERVICE_PORTRANGE_DEFAULT =
        PortRange.UNCONSTRAINED;
    static { putState(COMMON_SERVICE_PORTRANGE,
                      COMMON_SERVICE_PORTRANGE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT)); }

    /**
     * The number of RNs that an SN can support. This value is used during
     * topology operations to inform decisions about where to place new
     * Replication Nodes. If the value is zero, the SN can still hosts admins
     * and arbiter nodes. This parameter can be specified by the value of the
     * "-capacity" flag in the call to the makebootconfig command used to
     * initialize the storage node.
     */
    public static final String COMMON_CAPACITY = "capacity";
    public static final String COMMON_CAPACITY_DEFAULT = "1";
    static { putState(COMMON_CAPACITY,
                      COMMON_CAPACITY_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART),
                      Scope.SERVICE,
                      0, 1000,
                      null); }

    /**
     * The number of processors available for use by the storage node. This
     * parameter can be specified by the value of the "-num_cpus" flag in the
     * call to the makebootconfig command used to initialize the storage node.
     * If the default value of 0 is used, then the system determine the number,
     * which will be at least 1.
     */
    public static final String COMMON_NUMCPUS = "numCPUs";
    public static final String COMMON_NUMCPUS_DEFAULT = "0";
    static { putState(COMMON_NUMCPUS,
                      COMMON_NUMCPUS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART),
                      Scope.SERVICE,
                      1, 65536,
                      null); }

    /**
     * The number of megabytes (1000000 bytes) of memory available for use by
     * the storage node. This parameter can be specified by the value of the
     * "-memory_mb" flag in the call to the makebootconfig command used to
     * initialize the storage node. If the value is 0, then the system will
     * attempt to determine the number.
     */
    public static final String COMMON_MEMORY_MB = "memoryMB";
    public static final String COMMON_MEMORY_MB_DEFAULT = "0";
    static { putState(COMMON_MEMORY_MB,
                      COMMON_MEMORY_MB_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART),
                      Scope.SERVICE,
                      0, 128 * 1024 * 1024 /* 128 TB */,
                      null); }

    /**
     * The subdirectory of the root directory that contains security
     * configuration information.
     */
    public static final String COMMON_SECURITY_DIR = "securityDir";
    public static final String COMMON_SECURITY_DIR_DEFAULT = null;
    static { putState(COMMON_SECURITY_DIR,
                      COMMON_SECURITY_DIR_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.BOOT, Info.NORESTART)); }

    /**
     * The maximum number of threads in the thread pool used to execute
     * incoming requests for each async service other than the request handler.
     *
     * @since 21.2
     * @deprecated as of 21.3, now no limit
     */
    @Deprecated
    private static final String COMMON_MISC_ASYNC_EXEC_MAX_THREADS =
        "miscAsyncExecMaxThreads";
    @Deprecated
    private static final String COMMON_MISC_ASYNC_EXEC_MAX_THREADS_DEFAULT =
        "0";
    static { putState(COMMON_MISC_ASYNC_EXEC_MAX_THREADS,
                      COMMON_MISC_ASYNC_EXEC_MAX_THREADS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.REPNODE, Info.SNA,
                                 Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE,
                      0, 10000,
                      null); }

    /**
     * The maximum number of concurrent asynchronous operations permitted for
     * each async service other than the request handler.
     *
     * @since 21.2
     */
    public static final String COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS =
        "miscAsyncMaxConcurrentOps";
    public static final String COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS_DEFAULT =
        "20";
    static { putState(COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS,
                      COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.ADMIN, Info.REPNODE, Info.SNA,
                                 Info.ARBNODE,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE,
                      1, Integer.MAX_VALUE,
                      null); }

    public static final String SN_ROOT_DIR_PATH = "rootDirPath";
    public static final String SN_ROOT_DIR_PATH_DEFAULT = "";
    static { putState(SN_ROOT_DIR_PATH,
                      SN_ROOT_DIR_PATH_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.RONLY)); }

    public static final String SN_ROOT_DIR_SIZE = "rootDirSize";
    public static final String SN_ROOT_DIR_SIZE_DEFAULT = "0";
    static { putState(SN_ROOT_DIR_SIZE,
                      SN_ROOT_DIR_SIZE_DEFAULT,
                      Type.SIZE,
                      EnumSet.of(Info.SNA,
                                 Info.NORESTART)); }

    /**
     * Storage Type is the type of device for the storage
     * directories used by the RNs. Storage Type is set for the
     * entire SN. It is unlike storageDir and storageDirSize, which are
     * specified for each RN.
     *
     * The string "storageDirStorageType" is misleading because it sounds
     * like it's the storage type for each storageDir, therefore each RN.
     * The term StorageType is being used instead of StorageDirStorageType
     * in most cases. However, it is being left in for now for compatibility
     * reasons. The teams plans to deprecate "storageDirStorageType" in
     * the future.
     */
    public static final String SN_STORAGE_TYPE = "storageDirStorageType";

    public static final String SN_STORAGE_TYPE_DEFAULT =
            StorageTypeDetector.StorageType.UNKNOWN.name();
    static { putState(SN_STORAGE_TYPE,
                      SN_STORAGE_TYPE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String SN_SERVICE_STOP_WAIT = "serviceStopWait";
    public static final String SN_SERVICE_STOP_WAIT_DEFAULT = "120 s";
    static { putState(SN_SERVICE_STOP_WAIT,
                      SN_SERVICE_STOP_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE); }

    public static final String SN_REPNODE_START_WAIT = "repnodeStartWait";
    public static final String SN_REPNODE_START_WAIT_DEFAULT = "120 s";
    static { putState(SN_REPNODE_START_WAIT,
                      SN_REPNODE_START_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.STORE); }

    /**
     * The number of log files that are retained for stat files, perf files,
     * and service debug logs for RNs, Arbiters, Admins, and SNs. Values less
     * than 1 are converted to 1. Any compressed rotated files are included in
     * the count regardless of the setting of the serviceLogFileCompression
     * parameter. When a new log file is created for one of these purposes
     * because the current one has reached the maximum size specified by the
     * serviceLogFileLimit parameter, the oldest existing log file in its
     * category will be removed if creating a new file results in the number of
     * log files to exceed its category log file count limit.
     * @see #SN_LOG_FILE_LIMIT
     */
    public static final String SN_LOG_FILE_COUNT = "serviceLogFileCount";
    public static final String SN_LOG_FILE_COUNT_DEFAULT = "20";
    static { putState(SN_LOG_FILE_COUNT,
                      SN_LOG_FILE_COUNT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY),
                      Scope.STORE); }

    /**
     * An approximate maximum size in bytes for stat files, perf files, and
     * service debug logs for RNs, Arbiters, Admins, and SNs. Values 0 or less
     * represent no limit. If serviceLogFileCompression is true and the value
     * of this parameter is greater than 0, the actual file limit is adjusted
     * to a larger value that takes the space savings from compression into
     * account. When a file in one of these categories reaches the limit, the
     * logging subsystem switches to a new log file.
     * @see #SN_LOG_FILE_COUNT
     */
    public static final String SN_LOG_FILE_LIMIT = "serviceLogFileLimit";
    public static final String SN_LOG_FILE_LIMIT_DEFAULT =
        String.valueOf(5 * MB);
    static { putState(SN_LOG_FILE_LIMIT,
                      SN_LOG_FILE_LIMIT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY),
                      Scope.STORE); }

    /**
     * Whether to compress rotated log files. If set to true, store-wide debug
     * log files, stat files, perf files, and service debug log files for RNs,
     * Arbiters, Admins, and SNs will be compressed using gzip when files are
     * rotated after reaching the maximum size.
     *
     * @see #SN_LOG_FILE_COUNT
     */
    public static final String SN_LOG_FILE_COMPRESSION =
        "serviceLogFileCompression";
    public static final String SN_LOG_FILE_COMPRESSION_DEFAULT = "false";
    static { putState(SN_LOG_FILE_COMPRESSION,
                      SN_LOG_FILE_COMPRESSION_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SNA, Info.POLICY),
                      Scope.STORE); }

    public static final String SN_COMMENT = "comment";
    public static final String SN_COMMENT_DEFAULT = "";
    static { putState(SN_COMMENT,
                      SN_COMMENT_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.RONLY)); }

    public static final String SN_MAX_LINK_COUNT = "maxLinkCount";
    public static final String SN_MAX_LINK_COUNT_DEFAULT = "500";
    static { putState(SN_MAX_LINK_COUNT,
                      SN_MAX_LINK_COUNT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART)); }

    public static final String SN_LINK_EXEC_WAIT = "linkExecWait";
    public static final String SN_LINK_EXEC_WAIT_DEFAULT = "200 s";
    static { putState(SN_LINK_EXEC_WAIT,
                      SN_LINK_EXEC_WAIT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART)); }

    public static final String SN_ALLOW_ARBITERS = "allowArbiters";
    public static final String SN_ALLOW_ARBITERS_DEFAULT = "true";
    static { putState(SN_ALLOW_ARBITERS,
                      SN_ALLOW_ARBITERS_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN, Info.NORESTART),
                      Scope.SERVICE); }

    /**
     * Let the user customize the command line used to start managed processes,
     * Example use cases are when the user wants to configure with the numactl
     * wrapper, or to start a RN with profiling.
     */
    public static final String SN_PROCESS_STARTUP_PREFIX =
         "processStartupPrefix";
    public static final String SN_PROCESS_STARTUP_PREFIX_DEFAULT = "";
    static  { putState(SN_PROCESS_STARTUP_PREFIX,
                       SN_PROCESS_STARTUP_PREFIX_DEFAULT,
                       Type.STRING,
                       EnumSet.of(Info.SNA,
                                  Info.POLICY, Info.HIDDEN, Info.NORESTART)); }

    /**
     * The software version of the running SN. The version number
     * is updated when the SNA starts up with the new software.
     */
    public static final String SN_SOFTWARE_VERSION = "softwareVersion";
    public static final String SN_SOFTWARE_VERSION_DEFAULT = "";
    static { putState(SN_SOFTWARE_VERSION,
                      SN_SOFTWARE_VERSION_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.BOOT, Info.HIDDEN,
                                 Info.NORESTART, Info.RONLY)); }

    /**
     * The additional memory used by the JVM as a percentage of the
     * requested Java heap size.
     */
    public static final String JVM_OVERHEAD_PERCENT = "jvmOverheadPercent";
    public static final String JVM_OVERHEAD_PERCENT_DEFAULT =
        JVMSystemUtils.ZING_JVM ?
        /*
         * Zing allocates internal memory within the Java heap rather than in
         * the C heap, so the requested heap size should be the amount of
         * memory used. But Zing has a "contingency pool", presumably only used
         * as needed, that represents a 5% overhead, so use that as the default
         * overhead for Zing. Note that 5% default setting for Zing might be
         * out of date.
         */
        "5" :
        /*
         * From talking with the JVM team, and from experience, 25% seems to be
         * a good estimate of the overhead for the Oracle JVM, but the exact
         * value might be different for different workloads.
         */
        "25";
    static { putState(JVM_OVERHEAD_PERCENT,
                      JVM_OVERHEAD_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART),
                      Scope.SERVICE,
                      0, 100,
                      null); }

    /**
     * The percentage of SN memory reserved for the requested Java heap size,
     * for all RN processes hosted on this SN. This value does not include the
     * additional memory used by the JVM as represented by jvmOverheadPercent.
     * Also, note that an individual RN is usually limited to a maximum of 32
     * GB of heap.
     *
     * <p>The standard target memory allocation is:
     * <ul>
     * <li>85% for Java heap and overhead
     *   <ul>
     *   <li>68% for requested Java heap size (rnHeapPercent)
     *   <li>25% (jvmOverheadPercent) * 68 (rnHeapPercent) = 17% for JVM
     *       overhead
     *   </ul>
     * <li>15% for the operating system
     * </ul>
     */
    public static final String SN_RN_HEAP_PERCENT = "rnHeapPercent";
    public static final String SN_RN_HEAP_PERCENT_DEFAULT =
        Integer.toString(
            85 * 100 / (Integer.valueOf(JVM_OVERHEAD_PERCENT_DEFAULT) + 100));
    static { putState(SN_RN_HEAP_PERCENT,
                      SN_RN_HEAP_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART),
                      Scope.SERVICE,
                      1, 95,
                      null); }

    /**
     * The max heap size for the RN, which in turn determines the max JE cache
     * size.
     */
    public static final String SN_RN_HEAP_MAX_MB = "rnHeapMaxMB";
    /**
     * The default value of zero actually implies the VM-specific (~ 32GB)
     * compressed OOP limit: StorageNodeParams.MAX_COMPRESSED_OOPS_HEAP_SIZE.
     *
     * <p>However, we cannot reference that constant from here due to the
     * restrictions associated with compilation of the "web" copy of this file
     * by the gwt compiler.
     */
    public static final String SN_RN_HEAP_MAX_MB_DEFAULT = "0" ;
    static { putState(SN_RN_HEAP_MAX_MB,
                      SN_RN_HEAP_MAX_MB_DEFAULT,
                      Type.LONG,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART),
                      Scope.SERVICE,
                      0, Long.MAX_VALUE,
                      null); }

    /**
     * An obsolete parameter, obsoleted in 21.2, that used to specify the
     * percentage of the SN's memory that's reserved for FS cache, page tables,
     * network connections, etc. The amount of memory used for these system
     * purposes is now just the memory left over after the memory specified by
     * rnHeapPercent and jvmOverheadPercent. This parameter is ignored.
     *
     * @deprecated as of 21.2, see #SN_RN_HEAP_PERCENT
     */
    @Deprecated
    private static final String SN_SYSTEM_PERCENT = "systemPercent";
    @Deprecated
    private static final String SN_SYSTEM_PERCENT_DEFAULT= "10";
    static { putState(SN_SYSTEM_PERCENT,
                      SN_SYSTEM_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.SERVICE,
                      5, 100,
                      null); }

    /** The datacenter id SN belong to, just for UI */
    public static final String SN_DATACENTER = "datacenterId";
    public static final String SN_DATACENTER_DEFAULT = "-1";
    static {  putState(SN_DATACENTER,
                       SN_DATACENTER_DEFAULT,
                       Type.INT,
                       EnumSet.of(Info.SNA,
                                  Info.RONLY, Info.HIDDEN)); }

    /*
     * StorageNode GC parameters associated with garbage collector threads
     * tuning policy.
     */

    public static final String SN_GC_THREAD_FLOOR = "gcThreadFloor";
    public static final String SN_GC_THREAD_FLOOR_DEFAULT = "4";
    static { putState(SN_GC_THREAD_FLOOR,
                      SN_GC_THREAD_FLOOR_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN)); }

    public static final String SN_GC_THREAD_THRESHOLD = "gcThreadThreshold";
    public static final String SN_GC_THREAD_THRESHOLD_DEFAULT = "8";
    static { putState(SN_GC_THREAD_THRESHOLD,
                      SN_GC_THREAD_THRESHOLD_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN)); }

    public static final String SN_GC_THREAD_PERCENT = "gcThreadPercent";
    public static final String SN_GC_THREAD_PERCENT_DEFAULT = "62";
    static { putState(SN_GC_THREAD_PERCENT,
                      SN_GC_THREAD_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN)); }

    /**
     * The minimum number of threads that should be used for the async endpoint
     * group in an SNA or RN.
     */
    public static final String SN_ENDPOINT_GROUP_THREADS_FLOOR =
        "endpointGroupThreadsFloor";
    public static final String SN_ENDPOINT_GROUP_THREADS_FLOOR_DEFAULT = "2";
    static { putState(SN_ENDPOINT_GROUP_THREADS_FLOOR,
                      SN_ENDPOINT_GROUP_THREADS_FLOOR_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN)); }

    /**
     * The percentage of the number of available hardware threads that should
     * be used as the number of threads for the async endpoint group in an RN.
     * The actual number of threads to use is based on this percentage and the
     * number of available hardware threads, divided by the capacity, and also
     * subject to the floor.  The default specifies two threads per available
     * hardware thread, which is intended to account for any blocking caused by
     * operations other than I/O.
     */
    public static final String SN_ENDPOINT_GROUP_THREADS_PERCENT =
        "endpointGroupThreadsPercent";
    public static final String SN_ENDPOINT_GROUP_THREADS_PERCENT_DEFAULT =
        "200";
    static { putState(SN_ENDPOINT_GROUP_THREADS_PERCENT,
                      SN_ENDPOINT_GROUP_THREADS_PERCENT_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN)); }

    /**
     * The maximum time in seconds for a endpoint group thread to be in a
     * quiescent state before it is shut down for better resource usage.
     * Threads for an endpoint group are created for new connections until a
     * maximum number of threads (see SN_ENDPOINT_GROUP_THREADS_PERCENT). Once
     * the maximum is reached, new connections share the thread pool. If a
     * thread goes into a quiescent state, e.g. no IO or execution activities
     * for the amount of time specified by this parameter, the thread is shut
     * down.
     */
    public static final String SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS =
        "endpointGroupMaxQuiescentSeconds";
    public static final int
        SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_VALUE_DEFAULT = 600;
    public static final String
        SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_DEFAULT =
        String.valueOf(SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_VALUE_DEFAULT);
    static { putState(SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS,
                      SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN)); }

    /*
     * StorageNode socket configuration parameters
     *
     * All timeouts are expressed in ms.
     *
     * The SN read timeout is large to handle the load that occurs when a store
     * is being deployed.  It can take a while to create RepNodes.
     *
     * These timeouts could be NORESTART but it'd mean re-exporting all
     * interfaces.  This is possible but a TODO.  If done, it would not
     * affect existing handles as a restart would.
     */

    public static final String SN_ADMIN_SO_BACKLOG = "snAdminSOBacklog";
    public static final String SN_ADMIN_SO_BACKLOG_DEFAULT = "1024";
    static { putState(SN_ADMIN_SO_BACKLOG,
                      SN_ADMIN_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SN_ADMIN_SO_CONNECT_TIMEOUT = "connectTimeout";
    public static final String SN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(SN_ADMIN_SO_CONNECT_TIMEOUT,
                      SN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    public static final String SN_ADMIN_SO_READ_TIMEOUT = "readTimeout";
    public static final String SN_ADMIN_SO_READ_TIMEOUT_DEFAULT = "140 s";
    static { putState(SN_ADMIN_SO_READ_TIMEOUT,
                      SN_ADMIN_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    public static final String SN_MONITOR_SO_BACKLOG = "snMonitorSOBacklog";
    public static final String SN_MONITOR_SO_BACKLOG_DEFAULT = "1024";
    static { putState(SN_MONITOR_SO_BACKLOG,
                      SN_MONITOR_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SN_MONITOR_SO_CONNECT_TIMEOUT =
            "snMonitorSOConnectTimeout";
    public static final String SN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    static { putState(SN_MONITOR_SO_CONNECT_TIMEOUT,
                      SN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    public static final String SN_MONITOR_SO_READ_TIMEOUT =
            "snMonitorSOReadTimeout";
    public static final String SN_MONITOR_SO_READ_TIMEOUT_DEFAULT = "10 s";
    static {  putState(SN_MONITOR_SO_READ_TIMEOUT,
                       SN_MONITOR_SO_READ_TIMEOUT_DEFAULT,
                       Type.DURATION,
                       EnumSet.of(Info.SNA,
                                  Info.POLICY, Info.HIDDEN)); }

    /**
     * The repository size that SN MonitorAgent store instrumented data.
     * If the size less than or equal to 0, then MonitorAgent will prune all
     * instrumented data.
     */
    public static final String SN_MONITOR_AGENT_REPO_SIZE =
        "snMonitorAgentRepoSize";
    public static final String SN_MONITOR_AGENT_REPO_SIZE_DEFAULT = "10000";
    static { putState(SN_MONITOR_AGENT_REPO_SIZE,
                      SN_MONITOR_AGENT_REPO_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SN_LOGIN_SO_BACKLOG = "snLoginSOBacklog";
    public static final String SN_LOGIN_SO_BACKLOG_DEFAULT = "1024";
    static { putState(SN_LOGIN_SO_BACKLOG,
                      SN_LOGIN_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SN_LOGIN_SO_CONNECT_TIMEOUT =
            "snLoginSOConnectTimeout";
    public static final String SN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT = "5 s";
    static { putState(SN_LOGIN_SO_CONNECT_TIMEOUT,
                      SN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    public static final String SN_LOGIN_SO_READ_TIMEOUT =
            "snLoginSOReadTimeout";
    public static final String SN_LOGIN_SO_READ_TIMEOUT_DEFAULT = "5 s";
    static { putState(SN_LOGIN_SO_READ_TIMEOUT,
                      SN_LOGIN_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    public static final String SN_REGISTRY_SO_BACKLOG = "snRegistrySOBacklog";
    public static final String SN_REGISTRY_SO_BACKLOG_DEFAULT = "1024";
    static { putState(SN_REGISTRY_SO_BACKLOG,
                      SN_REGISTRY_SO_BACKLOG_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SN_REGISTRY_SO_CONNECT_TIMEOUT =
            "snRegistrySOConnectTimeout";
    /** @see #RN_RH_SO_CONNECT_TIMEOUT_DEFAULT */
    public static final String SN_REGISTRY_SO_CONNECT_TIMEOUT_DEFAULT = "3 s";
    static { putState(SN_REGISTRY_SO_CONNECT_TIMEOUT,
                      SN_REGISTRY_SO_CONNECT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    /*
     * This should probably have been defined as "snRegistrySOReadTimeout", but
     * changing it could have upgrade compatibility issues.
     */
    public static final String SN_REGISTRY_SO_READ_TIMEOUT =
            "snMonitorSOReadTimeout";
    public static final String SN_REGISTRY_SO_READ_TIMEOUT_DEFAULT = "5 s";
    static { putState(SN_REGISTRY_SO_READ_TIMEOUT,
                      SN_REGISTRY_SO_READ_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.SNA,
                                 Info.POLICY, Info.HIDDEN)); }

    /*
     * Bootstrap parameters
     */

    public static final String BP_ROOTDIR = "rootDir";
    public static final String BP_ROOTDIR_DEFAULT = "";
    static { putState(BP_ROOTDIR,
                      BP_ROOTDIR_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.BOOT,
                                 Info.RONLY)); }

    public static final String BP_HOSTING_ADMIN = "hostingAdmin";
    public static final String BP_HOSTING_ADMIN_DEFAULT = "false";
    static { putState(BP_HOSTING_ADMIN,
                      BP_HOSTING_ADMIN_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.BOOT,
                                 Info.RONLY)); }

    /*
     * JE parameters
     */

    /*
     * TODO: maybe validate JE_MISC as a Properties list
     */
    /**
     * System property settings to override the default settings for the
     * underlying Oracle Berkeley DB Java Edition subsystem.
     * The admin service will be restarted when this parameter is changed.
     * Empty string for this parameter means that the combination of default
     * settings made by KVStore and the ones specified by Oracle Berkeley DB
     * Java Edition will be in effect. Also, note that this parameter in policy
     * won't work for admin. It will be ignored when generating a default
     * AdminParams.
     */
    public static final String JE_MISC = "configProperties";
    public static final String JE_MISC_DEFAULT = "";
    static { putState(JE_MISC,
                      JE_MISC_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String JE_HOST_PORT = "nodeHostPort";
    public static final String JE_HOST_PORT_DEFAULT = "";
    static { putState(JE_HOST_PORT,
                      JE_HOST_PORT_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.RONLY, Info.HIDDEN)); }

    public static final String JE_HELPER_HOSTS = "helperHosts";
    public static final String JE_HELPER_HOSTS_DEFAULT = "";
    static { putState(JE_HELPER_HOSTS,
                      JE_HELPER_HOSTS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.ADMIN,  Info.ARBNODE,
                                 Info.HIDDEN, Info.NORESTART),
                      Scope.REPGROUP); }

    public static final String JE_CACHE_SIZE = "cacheSize";
    public static final String JE_CACHE_SIZE_DEFAULT = "0";
    static { putState(JE_CACHE_SIZE,
                      JE_CACHE_SIZE_DEFAULT,
                      Type.LONG,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * This parameter is used to enable or disable the erasure feature for the
     * underlying BDB JE storage layer. Erasure periodically wipes the obsolete
     * data from the storage layer, by zeroing out corresponding records.
     * Erasure can be enabled or disabled without restarting the database.
     * Default value is false. JE Erasure parameters only apply to the
     * REPNODE case because Erasure is only used for processing user data.
     */
    public static final String JE_ENABLE_ERASURE = "enableErasure";
    public static final String JE_ENABLE_ERASURE_DEFAULT = "true";
    static { putState(JE_ENABLE_ERASURE,
                      JE_ENABLE_ERASURE_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /**
     * This parameter is used to specify the duration for one complete erasure
     * pass over data.  Erasure is throttled based on this value, to minimize
     * its impact on performance. By default, it is set to "6 DAYS". It is
     * recommended that erasure period should be set to less than half of
     * duration in which one expects to get rid of all obsolete data. This
     * ensures data sanitization even in the face of issues like node failures.
     * JE Erasure parameters only apply to the REPNODE case because Erasure is
     * only used for processing user data.
     */
    public static final String JE_ERASURE_PERIOD = "erasurePeriod";
    public static final String JE_ERASURE_PERIOD_DEFAULT = "6 DAYS";
    static { putState(JE_ERASURE_PERIOD,
                      JE_ERASURE_PERIOD_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /*
     * The parameter jeNodePriority is a hidden parameter, it is used by zone
     * affinity feature. It is not allowed to change by change parameter plan.
     */
    public static final String JE_NODE_PRIORITY = "jeNodePriority";
    public static final String JE_NODE_PRIORITY_DEFAULT = "1";
    static { putState(JE_NODE_PRIORITY,
                      JE_NODE_PRIORITY_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE, Info.RONLY,
                                 Info.NORESTART, Info.HIDDEN),
                      Scope.STORE); }

    /*
     * JVM parameters
     */

    /**
     * This parameter is used by the system to store generated JVM command line
     * parameters which are used to start the RepNode, ArbNode and Admin
     * services.The use of this parameter by the Administrator to override
     * or add JVM command line parameters has been deprecated.
     * The javaRnParamsOverride, javaAnParamsOverride, and
     * javaAdminParamsOverride parameters should now be used to alter
     * the JVM command line parameters for an existing service.
     * While using this parameter, make sure you enclose all values in quotes
     * if they include spaces.
     */
    public static final String JVM_MISC = "javaMiscParams";
    public static final String JVM_MISC_DEFAULT = "";
    static { putState(JVM_MISC,
                      JVM_MISC_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.POLICY),
                      Scope.STORE); }

    /**
     * JVM command line parameters that are used to overwrite existing
     * system generated or add RN JVM parameters.
     * While using this parameter, make sure you enclose all values in quotes
     * if they include spaces.
     */
    public static final String JVM_RN_OVERRIDE = "javaRnParamsOverride";
    public static final String JVM_RN_OVERRIDE_DEFAULT = "";
    static { putState(JVM_RN_OVERRIDE,
                      JVM_RN_OVERRIDE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.POLICY),
                      Scope.STORE); }

    /**
     * JVM command line parameters that are used to extend the exclude list
     * of JVM arguments.
     * While using this parameter, make sure to include the JVM arguments key
     * for java version after which the argument is unsupported. The JVM
     * argument is excluded for every java version >= specified version.
     * For Example, tha accepted value is "18=-XX:key1 18=-XX:key2 21=-XX:key3"
     */
    public static final String JVM_RN_EXCLUDE_ARGS =
        "javaExcludeParamsOverride";
    public static final String JVM_RN_EXCLUDE_ARGS_DEFAULT = "";
    static { putState(JVM_RN_EXCLUDE_ARGS,
                      JVM_RN_EXCLUDE_ARGS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    /**
     * JVM command line parameters that can be used to override existing system
     * generated parameters or to add RN JVM parameters without causing RNs to
     * be restarted. While using this parameter, make sure you enclose all
     * values in quotes if they include spaces.
     */
    public static final String JVM_RN_OVERRIDE_NO_RESTART =
        "javaRnParamsOverrideNoRestart";
    public static final String JVM_RN_OVERRIDE_NO_RESTART_DEFAULT = "";
    static { putState(JVM_RN_OVERRIDE_NO_RESTART,
                      JVM_RN_OVERRIDE_NO_RESTART_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN,
                                 Info.NORESTART),
                      Scope.STORE); }

    /**
     * JVM command line parameters that are used to overwrite existing
     * system generated or add AN JVM parameters.
     * While using this parameter, make sure you enclose all values in quotes
     * if they include spaces.
     */
    public static final String JVM_AN_OVERRIDE = "javaAnParamsOverride";
    public static final String JVM_AN_OVERRIDE_DEFAULT = "";
    static { putState(JVM_AN_OVERRIDE,
                      JVM_AN_OVERRIDE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.ARBNODE, Info.POLICY),
                      Scope.STORE); }

    /**
     * JVM command line parameters that are used to overwrite existing
     * system generated Admin or add JVM parameters.
     * While using this parameter, make sure you enclose all values in quotes
     * if they include spaces.
     */
    public static final String JVM_ADMIN_OVERRIDE = "javaAdminParamsOverride";
    public static final String JVM_ADMIN_OVERRIDE_DEFAULT = "";
    static { putState(JVM_ADMIN_OVERRIDE,
                      JVM_ADMIN_OVERRIDE_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.ADMIN, Info.POLICY),
                      Scope.STORE); }

    /* TODO: logging changes are restart-required, this may change */
    /**
     * The properties that control debug logging in RNs, Admins, and Arbiters.
     */
    public static final String JVM_LOGGING = "loggingConfigProps";
    public static final String JVM_LOGGING_DEFAULT =
        /*
         * Disable the JE file handler. The contents of the JE debug log file
         * are redundant with service debug log files. [KVSTORE-878]
         */
        "com.sleepycat.je.util.FileHandler.level=OFF";
    static { putState(JVM_LOGGING,
                      JVM_LOGGING_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.REPNODE, Info.ADMIN, Info.ARBNODE,
                                 Info.POLICY),
                      Scope.STORE); }

    /*
     * KVS parameters (store-wide)
     */

    public static final String KV_CACHE_MODE = "cacheMode";
    public static final String KV_CACHE_MODE_DEFAULT =
        JVMSystemUtils.ZING_JVM ? "DEFAULT" : "EVICT_LN";
    static { putState(KV_CACHE_MODE,
                      KV_CACHE_MODE_DEFAULT,
                      Type.CACHEMODE,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.HIDDEN),
                      Scope.STORE,
                      0, 0,  /* min, max n/a */
                      new String[] {"DEFAULT", "KEEP_HOT",
                                    "UNCHANGED", "MAKE_COLD",
                                    "EVICT_LN", "EVICT_BIN",
                                    "DYNAMIC"}); }

    /**
     * Arbiter minimum (and default) heap size in MB. Not dynamically
     * configurable at this time.
     */
    public static final int AN_HEAP_MB_MIN = ParameterUtils.applyMinHeapMB(50);

    /*
     * Stats parameters
     */

    /*
     * Stats parameters in RepNode and ArbNode
     */

    public static final String SP_COLLECT_ENV_STATS = "collectEnvStats";
    public static final String SP_COLLECT_ENV_STATS_DEFAULT = "true";
    static { putState(SP_COLLECT_ENV_STATS,
                      SP_COLLECT_ENV_STATS_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE, Info.ADMIN,
                                 Info.NORESTART, Info.POLICY),
                      Scope.STORE); }

    /**
     * Enable collect endpoint group stats from rep nodes.
     *
     * The endpoint group stats include two parts: a dialog endpoint group perf
     * stats including dialog throughput and latency stats among endpoints and
     * a nio thread pool perf stats including handler read/write counts and
     * latency and executor performance metrics.
     */
    public static final String SP_COLLECT_ENDPOINT_GROUP_STATS =
        "collectEndpointGroupStats";
    public static final String
        SP_COLLECT_ENDPOINT_GROUP_STATS_DEFAULT = "true";
    static { putState(SP_COLLECT_ENDPOINT_GROUP_STATS,
                      SP_COLLECT_ENDPOINT_GROUP_STATS_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY),
                      Scope.STORE); }

    /**
     * To be replaced by GP_COLLECTOR_INTERVAL
     * @deprecated since 4.3
     */
    @Deprecated
    public static final String SP_INTERVAL = "statsInterval";
    public static final String SP_INTERVAL_DEFAULT = "60 s";
    static { putState(SP_INTERVAL,
                      SP_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE, Info.ARBNODE,
                                 Info.NORESTART, Info.POLICY,
                                 Info.HIDDEN),
                      Scope.STORE); }

    /*
     * Stats parameters in RepNode only
     */

    public static final String SP_DUMP_INTERVAL = "threadDumpInterval";
    public static final String SP_DUMP_INTERVAL_DEFAULT = "5 MINUTES";
    static { putState(SP_DUMP_INTERVAL,
                      SP_DUMP_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SP_ACTIVE_THRESHOLD = "activeThreshold";
    public static final String SP_ACTIVE_THRESHOLD_DEFAULT =
        String.valueOf(Integer.MAX_VALUE);
    static { putState(SP_ACTIVE_THRESHOLD,
                      SP_ACTIVE_THRESHOLD_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SP_THREAD_DUMP_MAX = "threadDumpMax";
    public static final String SP_THREAD_DUMP_MAX_DEFAULT = "10";
    static { putState(SP_THREAD_DUMP_MAX,
                      SP_THREAD_DUMP_MAX_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.NORESTART, Info.POLICY, Info.HIDDEN),
                      Scope.STORE); }

    public static final String SP_LATENCY_CEILING = "latencyCeiling";
    public static final String SP_LATENCY_CEILING_DEFAULT = "0";
    static { putState(SP_LATENCY_CEILING,
                      SP_LATENCY_CEILING_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String SP_THROUGHPUT_FLOOR = "throughputFloor";
    public static final String SP_THROUGHPUT_FLOOR_DEFAULT = "0";
    static { putState(SP_THROUGHPUT_FLOOR,
                      SP_THROUGHPUT_FLOOR_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    /*
     * TextIndexFeeder parameters
     */

    /*
     * TIF parameters in StorageNodeParams
     *
     * Search cluster connection information is global and is stored
     * with SN parameters.  These parameters are set by the
     * register-es plan.  They are distributed automatically to new
     * SNAs.
     */

    public static final String SN_SEARCH_CLUSTER_NAME = "searchClusterName";
    public static final String SN_SEARCH_CLUSTER_NAME_DEFAULT = "";
    static { putState(SN_SEARCH_CLUSTER_NAME,
                      SN_SEARCH_CLUSTER_NAME_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.TIF, Info.NORESTART)); }

    public static final String SN_SEARCH_CLUSTER_MEMBERS =
        "searchClusterMembers";
    public static final String SN_SEARCH_CLUSTER_MEMBERS_DEFAULT = "";
    static { putState(SN_SEARCH_CLUSTER_MEMBERS,
                      SN_SEARCH_CLUSTER_MEMBERS_DEFAULT,
                      Type.STRING,
                      EnumSet.of(Info.SNA,
                                 Info.TIF, Info.NORESTART)); }

    public static final String SN_SEARCH_CLUSTER_SECURE =
        "searchClusterSecure";
    public static final String SN_SEARCH_CLUSTER_SECURE_DEFAULT = "false";
    static {
        putState(SN_SEARCH_CLUSTER_SECURE, SN_SEARCH_CLUSTER_SECURE_DEFAULT,
                 Type.BOOLEAN,
                 EnumSet.of(Info.SNA, Info.TIF, Info.NORESTART, Info.POLICY));
    }

    public static final String SN_SEARCH_MONITOR_DELAY =
        "searchMonitoringDelay";
    public static final String SN_SEARCH_MONITOR_DELAY_DEFAULT = "10 s";
    static {
        putState(SN_SEARCH_MONITOR_DELAY, SN_SEARCH_MONITOR_DELAY_DEFAULT,
                 Type.DURATION, EnumSet.of(Info.SNA, Info.TIF, Info.NORESTART,
                                           Info.HIDDEN, Info.POLICY));
    }

    /*
     * TIF parameters in RepNodeParams
     *
     * TIF tuning parameters are kept with RepNodeParams, to take
     * advantage of the all-repnodes parameter-changing plan, and to
     * use the newParameters path via the RepNode, to cause the
     * TIF to restart when TIF parameters change.
     */

    public static final String RN_TIF_IS_NODE_CLIENT = "tifIsNodeClient";
    public static final String RN_TIF_IS_NODE_CLIENT_DEFAULT ="false";
    static { putState(RN_TIF_IS_NODE_CLIENT,
                      RN_TIF_IS_NODE_CLIENT_DEFAULT,
                      Type.BOOLEAN,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String RN_TIF_CHECKPOINT_INTERVAL =
        "tifCheckpointInterval";
    public static final String RN_TIF_CHECKPOINT_INTERVAL_DEFAULT =
        "2 MINUTES";
    static { putState(RN_TIF_CHECKPOINT_INTERVAL,
                      RN_TIF_CHECKPOINT_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String RN_TIF_BULK_OP_INTERVAL =
        "tifBulkOpInterval";
    public static final String RN_TIF_BULK_OP_INTERVAL_DEFAULT =
        "5 SECONDS";
    static { putState(RN_TIF_BULK_OP_INTERVAL,
                      RN_TIF_BULK_OP_INTERVAL_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String RN_TIF_BULK_OP_SIZE = "tifBulkOpSize";
    public static final String RN_TIF_BULK_OP_SIZE_DEFAULT = "5242880";
    static { putState(RN_TIF_BULK_OP_SIZE,
                      RN_TIF_BULK_OP_SIZE_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String RN_TIF_COMMIT_QUEUE_CAPACITY =
        "tifCommitQueueCapacity";
    public static final String RN_TIF_COMMIT_QUEUE_CAPACITY_DEFAULT =
        "20971520";
    static { putState(RN_TIF_COMMIT_QUEUE_CAPACITY,
                      RN_TIF_COMMIT_QUEUE_CAPACITY_DEFAULT,
                      Type.INT,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                      Scope.STORE); }

    public static final String RN_TIF_METRICS_SAMPLE_PERIOD =
        "tifMetricsSamplePeriod";
    public static final String RN_TIF_METRICS_SAMPLE_PERIOD_DEFAULT =
        "5 MINUTES";
    static { putState(RN_TIF_METRICS_SAMPLE_PERIOD,
                      RN_TIF_METRICS_SAMPLE_PERIOD_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE,
                                 Info.TIF, Info.HIDDEN, Info.NORESTART,
                                 Info.POLICY),
                 Scope.STORE); }

    /*
     * The parameters associated with the task coordinator permits used to
     * for stats gathering.
     */
    public static final String RN_SG_PERMIT_TIMEOUT = RepNodeParams.
            getPermitTimeoutParamName(RNTaskCoordinator.KV_STORAGE_STATS_TASK);
    /*
     * The default 10 sec timeout effectively means that under load it will run
     * at most once every 10 seconds with a deficit permit, processing 10K
     * records each time, which should represent fairly light additional load.
     */
    public static final String RN_SG_PERMIT_TIMEOUT_DEFAULT = "10 s";
    static { putState(RN_SG_PERMIT_TIMEOUT,
                      RN_SG_PERMIT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String RN_SG_PERMIT_LEASE = RepNodeParams.
        getPermitLeaseParamName(RNTaskCoordinator.KV_STORAGE_STATS_TASK);

    /* Assume generous 100ms/read (typically 10s of us) to arrive at a lease
     * default.
     */
    public static final String RN_SG_PERMIT_LEASE_DEFAULT =
        (StatsScan.BATCH_SIZE * 100) + " ms";
    static { putState(RN_SG_PERMIT_LEASE,
                  RN_SG_PERMIT_LEASE_DEFAULT,
                  Type.DURATION,
                  EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                  Scope.STORE); }

    /*
     * The parameters associated with the task coordinator permits used to
     * for index creation
     */
    public static final String RN_ADD_INDEX_PERMIT_TIMEOUT = RepNodeParams.
        getPermitTimeoutParamName(RNTaskCoordinator.KV_INDEX_CREATION_TASK);
    public static final String RN_ADD_INDEX_PERMIT_TIMEOUT_DEFAULT = "1 min";
    static { putState(RN_ADD_INDEX_PERMIT_TIMEOUT,
                      RN_ADD_INDEX_PERMIT_TIMEOUT_DEFAULT,
                      Type.DURATION,
                      EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                      Scope.STORE); }

    public static final String RN_ADD_INDEX_PERMIT_LEASE = RepNodeParams.
        getPermitLeaseParamName(RNTaskCoordinator.KV_INDEX_CREATION_TASK);

    /*
     * Use a sec per index update as a worst case.
     */
    public static final String RN_ADD_INDEX_PERMIT_LEASE_DEFAULT =
        (MaintenanceThread.POPULATE_BATCH_SIZE * 1000) + " ms" ;
    static { putState(RN_ADD_INDEX_PERMIT_LEASE,
                  RN_ADD_INDEX_PERMIT_LEASE_DEFAULT,
                  Type.DURATION,
                  EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                  Scope.STORE); }

    /**
     * Class methods
     */
    public ParameterState(Type type,
                          Object defaultParam,
                          EnumSet<Info> info,
                          Scope scope,
                          long min,
                          long max,
                          String[] validValues) {
        this.type = type;
        this.defaultParam = defaultParam;
        this.info = info;
        this.scope = scope;
        this.min = min;
        this.max = max;
        this.validValues = validValues;
        if (validValues == null && type == Type.BOOLEAN) {
            validValues = booleanVals;
        }
    }

    public Type getType() {
        return type;
    }

    public Scope getScope() {
        return scope;
    }

    public boolean appliesTo(Info svc) {
        return info.contains(svc);
    }

    public boolean containsAll(EnumSet<Info> set) {
        return info.containsAll(set);
    }

    public boolean getReadOnly() {
        return appliesTo(Info.RONLY);
    }

    public boolean getPolicyState() {
        return appliesTo(Info.POLICY);
    }

    public boolean restartRequired() {
        return !appliesTo(Info.NORESTART);
    }

    public boolean appliesToRepNode() {
        return appliesTo(Info.REPNODE);
    }

    public boolean appliesToAdmin() {
        return appliesTo(Info.ADMIN);
    }

    public boolean appliesToStorageNode() {
        return appliesTo(Info.SNA);
    }

    public boolean appliesToBootstrap() {
        return appliesTo(Info.BOOT);
    }

    public String[] getPossibleValues() {
        return validValues;
    }

    public boolean isHidden() {
        return appliesTo(Info.HIDDEN);
    }

    /**
     * TODO: add range info for int, long types if the UI wants it.
     */
    public String getValidValues() {
        String msg = null;
        switch (type) {
        case INT:
            msg = "<Integer>";
            break;
        case LONG:
            msg = "<Long>";
            break;
        case BOOLEAN:
            msg = "<Boolean>";
            break;
        case STRING:
            msg = "<String>";
            break;
        case DURATION:
            msg = "<Long TimeUnit>";
            break;
        case CACHEMODE:
            if (validValues != null) {
                msg = Arrays.toString(validValues);
            }
            break;
        case AUTHMETHODS:
            if (validValues != null) {
                msg = Arrays.toString(validValues);
            }
            break;
        case SPECIALCHARS:
            msg = "<String>";
            break;
        case SIZE:
        case TIME_TO_LIVE:
            msg = "<Long SizeUnit>";
            break;
        case NONE:
            msg = "<INVALIDTYPE>";
            break;
        }
        return msg;
    }

    /**
     * Validate ranges for int and long values.
     */
    public boolean validate(String name, long value, boolean throwIfInvalid) {
        if (value < min) {
            if (throwIfInvalid) {
                throw new IllegalArgumentException
                    ("Value: " + value + " is less than minimum (" +
                     min + ") for parameter " + name);
            }
            return false;
        }
        if (value > max) {
            if (throwIfInvalid) {
                throw new IllegalArgumentException
                    ("Value: " + value + " is greater than maximum (" +
                     max + ") for parameter " + name);
            }
            return false;
        }
        return true;
    }

    private static void noParameter(String param) {
        throw new IllegalStateException("Invalid parameter: " + param);
    }

    public static boolean restartRequired(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.restartRequired();
        }
        noParameter(parm);
        return false;
    }

    public static Type getType(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.getType();
        }
        noParameter(parm);
        return Type.NONE;
    }

    public static boolean isPolicyParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.getPolicyState();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isAdminParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToAdmin();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isRepNodeParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToRepNode();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isStorageNodeParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToStorageNode();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isHiddenParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.isHidden();
        }
        noParameter(parm);
        return false;
    }

    public static boolean validate(String name, long value) {
        ParameterState ps = lookup(name);
        if (ps != null) {
            return ps.validate(name, value, false);
        }
        return false;
    }

    public static ParameterState lookup(String parm) {
        return pstate.get(parm);
    }

    public static Set<Map.Entry<String, ParameterState>> getAllParameters() {
        return pstate.entrySet();
    }

    public Object getDefaultParameter() {
        return defaultParam;
    }

    public static Map<String, ParameterState> getMap() {
        return pstate;
    }

    /**
     * Record information for a parameter with SERVICE scope, and default
     * minimum, maximum, and valid values.
     */
    private static void putState(String name,
                                 String defaultValue,
                                 ParameterState.Type type,
                                 EnumSet<Info> info) {
        putState(name, defaultValue, type, info, Scope.SERVICE);
    }

    /**
     * Record information for a parameter with the specified scope, and default
     * minimum, maximum, and valid values.
     */
    private static void putState(String name,
                                 String defaultValue,
                                 ParameterState.Type type,
                                 EnumSet<Info> info,
                                 Scope scope) {
        putState(name, defaultValue, type, info, scope,
                 (type == Type.INT ? Integer.MIN_VALUE :
                  (type == Type.LONG ? Long.MIN_VALUE : 0)),
                 (type == Type.INT ? Integer.MAX_VALUE :
                  (type == Type.LONG ? Long.MAX_VALUE : 0)),
                 null);
    }

    /**
     * Record information for a parameter with the specified scope, minimum,
     * maximum, and valid values.
     */
    private static void putState(String name,
                                 String defaultValue,
                                 ParameterState.Type type,
                                 EnumSet<Info> info,
                                 Scope scope,
                                 long min,
                                 long max,
                                 String[] validValues) {
        assert(!(info.contains(Info.GLOBAL) && info.contains(Info.POLICY)));
        Object p = new DefaultParameter().create(name, defaultValue, type);
        ParameterState ps = new ParameterState(type, p, info, scope, min,
                                               max, validValues);
        pstate.put(name, ps);
    }
}
