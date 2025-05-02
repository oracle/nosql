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

import static oracle.kv.KVSecurityConstants.AUTH_EXT_MECH_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_USERNAME_PROPERTY;
import static oracle.kv.KVSecurityConstants.KRB_MECH_NAME;
import static oracle.kv.impl.util.SerializationUtil.readProperties;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeProperties;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import javax.security.auth.Subject;

/**
 * Login credentials for Kerberos authentication.<p>
 *
 * This class provides a way for an application to authenticate as a particular
 * Kerberos user when accessing a KVStore instance.
 * <p>
 * There are two approaches that client applications can use to authenticate
 * using Kerberos.  Client applications that use the <a
 * href="http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html">
 * Java Authentication and Authorization Service (JAAS)</a> programming
 * framework can specify credentials by using the {@link Subject#doAs} method.
 * <p>
 * Applications that do not use the JAAS framework can use this class to specify
 * a Kerberos identity. The credentials of the specified user will be acquired
 * from the Kerberos Key Distribution Center (KDC) based on the values
 * specified for the KerberosCredentials instance.
 *
 * @since 3.5
 */
public class KerberosCredentials implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    /* User principal name */
    private final String username;

    /* Kerberos login properties */
    private Properties krbProperties;

    /**
     * Creates Kerberos user credentials. The properties passed in are used to
     * retrieve the Kerberos credentials of the specified user from the
     * Kerberos Key Distribution Center (KDC).
     * <p>
     * If, as recommended, each server host uses a different principal name
     * that includes an individual instance name, the {@link
     * KVSecurityConstants#AUTH_KRB_SERVICES_PROPERTY} should specify the
     * mappings of server hostnames to Kerberos service principal names.
     * Users may need to provide Kerberos login properties so that underlying
     * authentication system can retrieve credentials from KDC. The properties
     * currently supported:
     *
     * <ul>
     * <li>{@link KVSecurityConstants#AUTH_KRB_CCACHE_PROPERTY}
     * <li>{@link KVSecurityConstants#AUTH_KRB_KEYTAB_PROPERTY}
     * <li>{@link KVSecurityConstants#AUTH_KRB_MUTUAL_PROPERTY}
     * </ul>
     *
     * <p>When multiple properties are set, for example,
     * {@link KVSecurityConstants#AUTH_KRB_CCACHE_PROPERTY} and
     * {@link KVSecurityConstants#AUTH_KRB_KEYTAB_PROPERTY},
     * the underlying login service will retrieve credentials of this user in
     * following preference order:
     * <ol>
     * <li> credentials cache
     * <li> keytab
     * </ol>
     * Without setting credential cache and keytab property, this method will
     * attempt to retrieve ticket or key from default credential cache or
     * keytab.
     * @param username the name of the user
     * @param krbProperties the Kerberos login properties
     */
    public KerberosCredentials(String username, Properties krbProperties)
        throws IllegalArgumentException {

        if (username == null) {
            throw new IllegalArgumentException(
                "The username argument must not be null");
        }
        if (krbProperties == null) {
            throw new IllegalArgumentException(
                "The krbProperties argument must not be null");
        }
        this.username = username;
        this.krbProperties = krbProperties;
        this.krbProperties.setProperty(AUTH_USERNAME_PROPERTY, username);
        this.krbProperties.setProperty(AUTH_EXT_MECH_PROPERTY, KRB_MECH_NAME);
    }

    /**
     * Read an instance from the input stream.
     *
     * @hidden For internal use only
     */
    KerberosCredentials(DataInput in, short serialVersion)
        throws IOException
    {
        username = readNonNullString(in, serialVersion);
        krbProperties = readProperties(in, serialVersion);
    }

    /**
     * Write this instance to the output stream.
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writeNonNullString(out, serialVersion, username);
        writeProperties(out, serialVersion, krbProperties);
    }

    /**
     * @hidden For internal use only
     */
    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.KERBEROS;
    }

    /**
     * @see LoginCredentials#getUsername()
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * Returns the Kerberos login properties. These properties are used to get
     * credentials from the Kerberos Key Distribution Center (KDC).
     *
     * @return the Kerberos login properties
     */
    public Properties getKrbProperties() {
        return krbProperties;
    }
}
