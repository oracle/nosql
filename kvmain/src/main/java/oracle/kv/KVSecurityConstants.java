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

package oracle.kv;

/**
 * The KVSecurityConstants interface defines constants used for security
 * configuration. These are most commonly use when populating a set if
 * properties to be passed to {@link KVStoreConfig#setSecurityProperties},
 * but may be used as a reference when configuring a security property file.
 *
 * @since 3.0
 */
public interface KVSecurityConstants {

    /**
     * The name of the property that identifies a security property
     * configuration file to be read when a KVStoreConfig is created, as a
     * set of overriding property definitions.
     */
    public static final String SECURITY_FILE_PROPERTY = "oracle.kv.security";

    /**
     * The name of the property used by KVStore to determine the network
     * mechanism to be used when communicating with Oracle NoSQL DB
     * servers.
     */
    public static final String TRANSPORT_PROPERTY = "oracle.kv.transport";

    /**
     * The value of the {@link #TRANSPORT_PROPERTY} setting that enables the use
     * of SSL/TLS communication.  This property has the value {@value}.
     */
    public static final String SSL_TRANSPORT_NAME = "ssl";

    /**
     * The name of the property used to control what SSL/TLS cipher suites are
     * acceptable for use. This has the value {@value}. The property value is a
     * comma-separated list of SSL/TLS cipher suite names. Refer to your Java
     * documentation for the list of valid values.
     */
    public static final String SSL_CIPHER_SUITES_PROPERTY =
        "oracle.kv.ssl.ciphersuites";

    /**
     * The name of the property used to control what SSL/TLS procotols are
     * acceptable for use. This has the value {@value}.  The property value is
     * a comma-separated list of SSL/TLS protocol names.  Refer to your Java
     * documentation for the list of valid values.
     */
    public static final String SSL_PROTOCOLS_PROPERTY =
        "oracle.kv.ssl.protocols";

    /**
     * The name of the property used to specify a verification step to
     * be performed when connecting to a NoSQL DB server when using SSL/TLS.
     * This has the value {@value}. The only verification step currently
     * supported is the "dnmatch" verifier.
     * <p>
     * The dnmatch verifier must be specified in the form
     * "dnmatch(distinguished-name)", where distinguished-name must be the
     * NoSQL DB server certificate's distinguished name. For a typical secure
     * deployment this should be "dnmatch(CN=NoSQL)".
     */
    public static final String SSL_HOSTNAME_VERIFIER_PROPERTY =
        "oracle.kv.ssl.hostnameVerifier";

    /**
     * The name of the property to identify the location of a Java
     * truststore file that validates the SSL/TLS certificates used
     * by the NoSQL DB server. This has the value {@value}. The property
     * setting must be set to an absolute path for the file. If this
     * property is not set, a system property setting of
     * javax.net.ssl.trustStore will be used.
     */
    public static final String SSL_TRUSTSTORE_FILE_PROPERTY =
        "oracle.kv.ssl.trustStore";

    /**
     * The name of the property to identify the type of Java
     * truststore that is referenced by the
     * {@link #SSL_TRUSTSTORE_FILE_PROPERTY} property.  This is only needed if
     * using a non-default truststore type, and the specified type must be a
     * type supported by your Java implementation. This has the value
     * {@value}.
     */
    public static final String SSL_TRUSTSTORE_TYPE_PROPERTY =
        "oracle.kv.ssl.trustStoreType";

    /**
     * The name of the property to identify the password of the Java
     * truststore that is referenced by the
     * {@link #SSL_TRUSTSTORE_FILE_PROPERTY} property. This property is only
     * needed if using a password-protected PKCS12 truststore. This property
     * has the value {@value}.
     */
    public static final String SSL_TRUSTSTORE_PASSWORD_PROPERTY =
        "oracle.kv.ssl.trustStorePassword";

    /**
     * The name of the property to specify the alias in the password store for
     * the password of the Java truststore that is referenced by the
     * {@link #SSL_TRUSTSTORE_FILE_PROPERTY} property. This property is only
     * needed if using a password-protected PKCS12 truststore and the password
     * is kept in the password store. This property has the value {@value}.
     */
    public static final String SSL_TRUSTSTORE_PASSWORD_ALIAS_PROPERTY =
        "oracle.kv.ssl.trustStorePasswordAlias";

    /**
     * The name of a property to specify a username for authentication.
     * This has the value {@value}.
     */
    public static final String AUTH_USERNAME_PROPERTY =
        "oracle.kv.auth.username";

    /**
     * The name of the property that identifies an Oracle Wallet directory
     * containing the password of the user to authenticate. This is only used
     * in the Enterprise Edition of the product. This has the value {@value}.
     * If the value specifies a relative pathname, the pathname will be
     * resolved relative to the directory containing the security property
     * configuration file if the security properties were specified using the
     * {@link #SECURITY_FILE_PROPERTY} system property. Otherwise, the relative
     * pathname value will be resolved relative to the current directory.
     */
    public static final String AUTH_WALLET_PROPERTY =
        "oracle.kv.auth.wallet.dir";

    /**
     * The name of the property that identifies a password store file
     * containing the password of the user to authenticate. This has the value
     * {@value}. If the value specifies a relative pathname, the pathname will
     * be resolved relative to the directory containing the security property
     * configuration file if the security properties were specified using the
     * {@link #SECURITY_FILE_PROPERTY} system property. Otherwise, the relative
     * pathname value will be resolved relative to the current directory.

     */
    public static final String AUTH_PWDFILE_PROPERTY =
        "oracle.kv.auth.pwdfile.file";

    /**
     * The name of the property to specify the external authentication
     * mechanism to use for client logins.  If this property is set, the client
     * will be authenticated using the specified external mechanism, otherwise
     * the internal login mechanism will be used.
     *
     * <p>Currently, the only supported external login mechanism is: {@value
     * oracle.kv.KVSecurityConstants#KRB_MECH_NAME}.
     */
    public static final String AUTH_EXT_MECH_PROPERTY =
        "oracle.kv.auth.external.mechanism";

    /**
     * The value of the {@link #AUTH_EXT_MECH_PROPERTY} setting that enables
     * the Kerberos login mechanism. This property has the value
     * {@value}.
     */
    public static final String KRB_MECH_NAME = "KERBEROS";

    /**
     * The name of property to specify the Kerberos principals for services
     * associated with each helper host.  Setting this property is required if,
     * as recommended, each host uses a different principal that includes its
     * own principal name.  All principals should specify the same service and
     * realm.  If this property is not set, the client will use "oraclenosql"
     * as the principal name for services on all helper hosts.
     * <p>
     * Each entry should specify the helper host name followed by the Kerberos
     * service name, and optionally an instance name and realm name.  The
     * entries are separated by commas, ignoring spaces. If any entry does not
     * specify a realm, each entry will use the default realm specified in
     * Kerberos configuration file. If any entry specifies a realm name, then
     * all entries must specify the same one.  The syntax is:
     *
     * <pre>
     * host:service[/instance[@realm]][, host:service[/instance[@realm]]]*
     * </pre>
     *
     * For example:
     *
     * <pre>
     * host37:nosql/host37@EXAMPLE.COM, host53:nosql/host53@EXAMPLE.COM
     * </pre>
     */
    public static final String AUTH_KRB_SERVICES_PROPERTY =
        "oracle.kv.auth.kerberos.services";

    /**
     * The name of property to specify the location of the keytab file for
     * Kerberos login. This property has the value {@value}.  This
     * property is used when all authentication parameters are provided
     * by security properties, and must not be set if the application
     * specifies a JAAS login configuration by setting the {@link
     * #JAAS_LOGIN_CONF_NAME} security property.
     * <p>
     * If this property is not specified when authenticating with security
     * properties, then authentication will be performed via the credentials
     * cache, if specified.  If both a keytab and a credentials cache are
     * specified, then the credentials cache is tried first. If neither a
     * keytab or a credentials cache is specified, then login will try the
     * default credential cache and then the default keytab.
     * <p>
     * The default location of the keytab file is specified by the Kerberos
     * configuration file.  If the keytab is not specified there, then the
     * system looks for the file:
     * <blockquote>
     * <i>user.home</i>/krb5.keytab
     * </blockquote>
     */
    public static final String AUTH_KRB_KEYTAB_PROPERTY =
        "oracle.kv.auth.kerberos.keytab";

    /**
     * The name of property to specify the Kerberos realm for the user
     * principal if using a short name to specify the client login principal.
     */
    public static final String AUTH_KRB_REALM_PROPERTY =
        "oracle.kv.auth.kerberos.realm";

    /**
     * The name of property to specify the location of the Kerberos credential
     * cache file. This property has the value {@value}.  This property is used
     * when all authentication parameters are provided by security properties,
     * and must not be set if the application specifies a JAAS login
     * configuration by setting the {@link #JAAS_LOGIN_CONF_NAME} security
     * property.
     * <p>
     * If this property is not specified when authenticating with security
     * properties, then authentication will be performed via the keytab, if
     * specified. If both a keytab and a credentials cache are specified, then
     * the credentials cache is tried first.  If both a keytab and a credentials
     * cache are not specified, then login will try the default credential
     * cache and then the default keytab.
     * <p>
     * The default location of the credential cache is /tmp/krb5cc_<i>uid</i>,
     * where the uid is a numeric user identifier. If the credential cache is
     * not found there, the system will look for the file:
     * <blockquote>
     * <i>user.home</i>/krb5cc_<i>user.name</i>
     * </blockquote>
     */
    public static final String AUTH_KRB_CCACHE_PROPERTY =
        "oracle.kv.auth.kerberos.ccache";

    /**
     * The name of property to specify whether to use mutual authentication for
     * Kerberos external login mechanism.  Kerberos will perform mutual
     * authentication if the property is set to true, and will not be performed
     * if it is set to false or if it is not set.
     */
    public static final String AUTH_KRB_MUTUAL_PROPERTY =
        "oracle.kv.auth.kerberos.mutualAuth";

    /**
     * The name of property to specify the configuration entry name in the JAAS
     * login configuration file when the application specifies credentials
     * using JAAS login configuration.  If not set, then all authentication
     * parameters need to be provided by security properties.
     *
     * @see KerberosCredentials
     */
    public static final String JAAS_LOGIN_CONF_NAME =
        "oracle.kv.jaas.login.conf.entryName";

    /**
     * The name of property to specify whether to automatically prompt password
     * for command line utilities. If it is set to false or is not set, command
     * line utilities will prompt for password automatically if given user name
     * and password are not specified or unable to authenticate successfully.
     * If the property is set to true, command line utilities will not prompt
     * for passwords, and the login will fail if either the user or password
     * is missing, or if the password is incorrect.
     */
    public static final String CMD_PASSWORD_NOPROMPT_PROPERTY =
        "oracle.kv.password.noPrompt";
}
