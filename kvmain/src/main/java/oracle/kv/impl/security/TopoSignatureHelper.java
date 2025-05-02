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

package oracle.kv.impl.security;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;

import javax.security.auth.DestroyFailedException;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Topology;

/**
 * A class helping sign or verify a signature for a topology object.
 */
public class TopoSignatureHelper implements SignatureHelper<Topology> {

    /** Test hook called by sign with the specified topology. */
    public static volatile TestHook<Topology> signTestHook;

    /** Test hook called by verify with the specified topology. */
    public static volatile TestHook<Topology> verifyTestHook;

    /**
     * The name of the keystore alias for the private key that will be used by
     * default to sign topology entries if the keystoreSigPrivateKeyAlias
     * parameter is not set.
     */
    public final static String SIG_PRIVATE_KEY_ALIAS_DEFAULT =
        SecurityUtils.KEY_ALIAS_DEFAULT;

    /**
     * The name of the truststore alias for the certificate that will be used
     * by default to verify the signature on topology entries if the
     * truststoreSigPublicKeyAlias parameter is not set.
     */
    public final static String SIG_PUBLIC_KEY_ALIAS_DEFAULT = "mykey";

    private final static String SIG_ALGORITHM_DEFAULT = "SHA256withRSA";

    /* Properties specified to access keys from kvstore security keystore */
    private final KeyStore keyStore;
    private final KeyStore certStore;
    private final String privKeyAlias;
    private final String certAlias;
    private final KeyStorePasswordSource ksPwdSource;

    private final Signature signature;

    /* Public key can be cached in memory */
    private PublicKey publicKey;

    /**
     * Builds a TopoSignatureHelper from the specified security params.
     */
    public static TopoSignatureHelper
        buildFromSecurityParams(SecurityParams sp) {

        if (sp == null) {
            throw new IllegalArgumentException(
                "Security params must not be null");
        }

        String keyAlias = sp.getKeystoreSigPrivateKeyAlias();
        if (keyAlias == null) {
            keyAlias = SIG_PRIVATE_KEY_ALIAS_DEFAULT;
        }

        String certAlias = sp.getTruststoreSigPublicKeyAlias();
        if (certAlias == null) {
            certAlias = SIG_PUBLIC_KEY_ALIAS_DEFAULT;
        }

        final KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        if (pwdSrc == null) {
            throw new IllegalArgumentException(
                "Unable to create keystore password source");
        }

        final String keyStoreName =
            sp.getConfigDir() + File.separator + sp.getKeystoreFile();
        final String certStoreName =
            sp.getConfigDir() + File.separator + sp.getTruststoreFile();

        final KeyStore keyStore;
        final KeyStore certStore;
        char[] ksPwd = null;

        try {
            ksPwd = pwdSrc.getPassword();
            keyStore = SecurityUtils.loadKeyStore(
                keyStoreName, ksPwd, "keystore", sp.getKeystoreType());
            certStore = SecurityUtils.loadKeyStore(
                certStoreName, ksPwd, "truststore", sp.getTruststoreType());

            String sigAlgorithm = sp.getSignatureAlgorithm();
            if (sigAlgorithm == null || sigAlgorithm.isEmpty()) {
                sigAlgorithm = SIG_ALGORITHM_DEFAULT;
            }

            return new TopoSignatureHelper(sigAlgorithm, keyStore, keyAlias,
                                           certStore, certAlias, pwdSrc);

        } finally {
            SecurityUtils.clearPassword(ksPwd);
        }
    }

    private TopoSignatureHelper(String sigAlgorithm,
                                KeyStore keyStore,
                                String privKeyAlias,
                                KeyStore certStore,
                                String certAlias,
                                KeyStorePasswordSource ksPwdSource) {
        try {
            signature = Signature.getInstance(sigAlgorithm);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(
                "Unrecognized signature algorithm: " + sigAlgorithm);
        }

        this.keyStore = keyStore;
        this.privKeyAlias = privKeyAlias;
        this.certStore = certStore;
        this.certAlias = certAlias;
        this.ksPwdSource = ksPwdSource;
    }

    @Override
    public byte[] sign(Topology topo) throws SignatureFaultException {

        assert TestHookExecute.doHookIfSet(signTestHook, topo);

        final byte[] topoSerialBytes;

        try {
            topoSerialBytes = topo.toByteArrayForSignature();
        } catch (IOException ioe) {
            throw new SignatureFaultException(
                "Failed to get topology bytes",
                ioe);
        }

        synchronized(signature) {
            try {
                signature.initSign(getPrivateKey());
                signature.update(topoSerialBytes);
                return signature.sign();
            } catch (InvalidKeyException ike) {
                throw new SignatureFaultException(
                    "Private key used to generate signature is invalid",
                    ike);
            } catch (KeyAccessException kae) {
                throw new SignatureFaultException(
                    "Failed to access private key",
                    kae);
            } catch (SignatureException se) {
                throw new SignatureFaultException(
                    "Problem while attempting to sign topology",
                    se);
            }
        }
    }

    @Override
    public boolean verify(Topology topo, byte[] sigBytes)
        throws SignatureFaultException {

        assert TestHookExecute.doHookIfSet(verifyTestHook, topo);

        final byte[] topoSerialBytes;

        try {
            topoSerialBytes = topo.toByteArrayForSignature();
        } catch (IOException ioe) {
            throw new SignatureFaultException(
                "Failed to get topology bytes",
                ioe);
        }

        synchronized(signature) {
            try {
                signature.initVerify(getPublicKey());
                signature.update(topoSerialBytes);
                return signature.verify(sigBytes);
            } catch (InvalidKeyException ike) {
                throw new SignatureFaultException(
                    "Public key used to verify signature is invalid",
                    ike);
            } catch (KeyAccessException kae) {
                throw new SignatureFaultException(
                    "Failed to access public key",
                    kae);
            } catch (SignatureException se) {
                throw new SignatureFaultException(
                    "Problem while attempting to verify topology",
                    se);
            }
        }
    }

    /**
     * Returns private key
     *
     * @throws KeyAccessException if any issue happened in getting private
     * key
     */
    private PrivateKey getPrivateKey() throws KeyAccessException {
        char[] ksPassword = null;
        PasswordProtection pwdParam = null;

        try {
            /*
             * We read password each time so as not to keep an in-memory
             * copy. This sacrifices efficiency, but is safer.
             */
            ksPassword = ksPwdSource.getPassword();
            pwdParam = new PasswordProtection(ksPassword);

            final PrivateKeyEntry pkEntry =
                (PrivateKeyEntry) keyStore.getEntry(privKeyAlias,
                                                    pwdParam);
            if (pkEntry == null) {
                throw new KeyAccessException(
                    "Could not find private key entry with alias of " +
                    privKeyAlias);
            }
            return pkEntry.getPrivateKey();

        } catch (NoSuchAlgorithmException nsae) {
            throw new KeyAccessException(
                "Unable to recover private key entry from keystore",
                nsae);
        } catch (UnrecoverableEntryException uee) {
            throw new KeyAccessException(
                "Password parameter is invalid or insufficent to " +
                "recover private key entry from keystore",
                uee);
        } catch (KeyStoreException kse) {
            throw new KeyAccessException(
                "Keystore is not loaded or initialized",
                kse);
        } finally {
            SecurityUtils.clearPassword(ksPassword);
            if (pwdParam != null) {
                try {
                    pwdParam.destroy();
                } catch (DestroyFailedException e) {
                    /* Ignore */
                }
            }
        }
    }

    /**
     * Returns public key
     *
     * @throws KeyAccessException if any issue happened in getting public
     * key
     */
    private PublicKey getPublicKey() throws KeyAccessException {
        if (publicKey == null) {
            try {
                final Certificate cert =
                    certStore.getCertificate(certAlias);
                if (cert == null) {
                    throw new KeyAccessException(
                        "Could not find certificate with alias of " +
                        certAlias + " or other");
                }
                publicKey = cert.getPublicKey();
            } catch (KeyStoreException kse) {
                throw new KeyAccessException(
                    "Certificate store is not loaded or initialized",
                    kse);
            }
        }
        return publicKey;
    }

    /*
     * Exception indicating problem encountered while accessing key
     */
    private static class KeyAccessException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public KeyAccessException(String msg, Throwable cause) {
            super(msg, cause);
        }

        public KeyAccessException(String msg) {
            super(msg);
        }
    }
}
