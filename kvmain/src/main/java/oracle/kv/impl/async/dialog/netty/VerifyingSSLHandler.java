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

package oracle.kv.impl.async.dialog.netty;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.HostnameVerifier;

import com.sleepycat.je.rep.net.SSLAuthenticator;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;


/**
 * A wrapped handler that verifies the identity of host after handshake.
 */
public class VerifyingSSLHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger;

    private final String endpointId;
    private final SSLEngine sslEngine;
    private final String targetHost;
    private final HostnameVerifier hostVerifier;
    private final SSLAuthenticator authenticator;

    private final SslHandler sslHandler;

    public VerifyingSSLHandler(Logger logger,
                               String endpointId,
                               SSLEngine sslEngine,
                               String targetHost,
                               HostnameVerifier hostVerifier,
                               SSLAuthenticator authenticator) {
        this.logger = logger;
        this.endpointId = endpointId;
        this.sslEngine = sslEngine;
        this.targetHost = targetHost;
        this.hostVerifier = hostVerifier;
        this.authenticator = authenticator;
        this.sslHandler = new SslHandler(sslEngine);
    }

    public SslHandler sslHandler() {
        return sslHandler;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
        throws Exception {

        if (evt != SslHandshakeCompletionEvent.SUCCESS) {
            super.userEventTriggered(ctx, evt);
            return;
        }

        logger.log(Level.INFO, "SSL handshake done ({0})", endpointId);

        final SSLSession session = sslEngine.getSession();
        if (sslEngine.getUseClientMode()) {
            if (hostVerifier != null) {

                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE,
                            "Verifying server host, " +
                            "verifier={0}, targetHost={1}, " +
                            "principal={2}, certificates={3}",
                            new Object[] {
                                hostVerifier,
                                targetHost,
                                session.getPeerPrincipal(),
                                Arrays.toString(session.getPeerCertificates()),
                            });
                }

                /* Verifying the host. Code copied from JE SSLDataChannel. */
                final boolean peerTrusted =
                    hostVerifier.verify(targetHost, session);
                if (peerTrusted) {
                    logger.log(Level.FINE,
                            "SSL host verifier reports that " +
                            "connection target is valid");
                } else {
                    logger.log(Level.INFO,
                            "SSL host verifier reports that " +
                            "connection target is NOT valid");
                    throw new IOException(
                            "Server identity could not be verified");
                }
            }
        } else {
            if (authenticator != null) {

                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE,
                            "Authenticating client host, " +
                            "authenticator={0}, principal={1}",
                            new Object[] {
                                authenticator,
                                session.getPeerPrincipal() });
                }

                /* Verifying the host. Code copied from JE SSLDataChannel. */
                final boolean peerTrusted = authenticator.isTrusted(session);
                if (peerTrusted) {
                    logger.log(Level.FINE,
                            "SSL authenticator reports that " +
                            "channel is trusted");
                } else {
                    /*
                     * Just log the information as is done within the JE code.
                     */
                    logger.log(Level.INFO,
                            "SSL authenticator reports that " +
                            "channel is NOT trusted");
                }
            }
        }
    }
}

